package spark

import java.io.EOFException
import java.net.URL
import java.io.ObjectInputStream
import java.util.concurrent.atomic.AtomicLong
import java.util.HashSet
import java.util.Random
import java.util.Date

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.FileOutputCommitter
import org.apache.hadoop.mapred.HadoopWriter
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCommitter
import org.apache.hadoop.mapred.OutputFormat
import org.apache.hadoop.mapred.SequenceFileOutputFormat
import org.apache.hadoop.mapred.TextOutputFormat

import SparkContext._

/**
 * A Resilient Distributed Dataset (RDD), the basic abstraction in Spark. Represents an immutable, 
 * partitioned collection of elements that can be operated on in parallel.
 *
 * Each RDD is characterized by five main properties:
 * - A list of splits (partitions)                  // 一堆partitions
 * - A function for computing each split            // 计算partition的func
 * - A list of dependencies on other RDDs           // 一堆前置的rdd们
 * - Optionally, a Partitioner for key-value RDDs (e.g. to say that the RDD is hash-partitioned)
 * - Optionally, a list of preferred locations to compute each split on (e.g. block locations for
 *   HDFS)
 *
 * All the scheduling and execution in Spark is done based on these methods, allowing each RDD to 
 * implement its own way of computing itself.
 *
 * This class also contains transformation methods available on all RDDs (e.g. map and filter). In 
 * addition, PairRDDFunctions contains extra methods available on RDDs of key-value pairs, and 
 * SequenceFileRDDFunctions contains extra methods for saving RDDs to Hadoop SequenceFiles.
 */
 // 传入sparkContext
abstract class RDD[T: ClassManifest](@transient sc: SparkContext) extends Serializable {

  // Methods that must be implemented by subclasses
  def splits: Array[Split]
  // 从split 里返回一个可迭代的对象
  def compute(split: Split): Iterator[T]
  @transient val dependencies: List[Dependency[_]]
  
  // Optionally overridden by subclasses to specify how they are partitioned
  val partitioner: Option[Partitioner] = None

  // Optionally overridden by subclasses to specify placement preferences
  def preferredLocations(split: Split): Seq[String] = Nil

  // sparkContext 对象
  def context = sc
  
  // Get a unique ID for this RDD
  val id = sc.newRddId()
  
  // Variables relating to caching
  private var shouldCache = false
  
  // Change this RDD's caching
  // rdd 是否需要分片cache功能
  def cache(): RDD[T] = {
    shouldCache = true
    this
  }
  
  // Read this RDD; will read from cache if applicable, or otherwise compute
  // slave 节点上执行分片逻辑，先查看本地cache有没有，有的话，直接取出结果
  // 没有的话，就计算出来，放入cache，并告诉master，放入成功了
  // rdd 需要有根据分片 返回 iterrator的功能
  final def iterator(split: Split): Iterator[T] = {
    if (shouldCache) {
     // 能不能从cache中读到，能就从cache里用
      SparkEnv.get.cacheTracker.getOrCompute[T](this, split)
    } else {
     // 从split 里返回一个iterator
      compute(split)
    }
  }
  
  // Transformations (return a new RDD)
  
  def map[U: ClassManifest](f: T => U): RDD[U] = new MappedRDD(this, sc.clean(f))
  
  def flatMap[U: ClassManifest](f: T => TraversableOnce[U]): RDD[U] =
    new FlatMappedRDD(this, sc.clean(f))
  
  def filter(f: T => Boolean): RDD[T] = new FilteredRDD(this, sc.clean(f))

  def sample(withReplacement: Boolean, fraction: Double, seed: Int): RDD[T] =
    new SampledRDD(this, withReplacement, fraction, seed)

  def takeSample(withReplacement: Boolean, num: Int, seed: Int): Array[T] = {
    var fraction = 0.0
    var total = 0
    var multiplier = 3.0
    // 总个数是多少
    var initialCount = count()
    // 最多也只能选总个数个
    var maxSelected = 0

    if (initialCount > Integer.MAX_VALUE - 1) {
      maxSelected = Integer.MAX_VALUE - 1
    } else {
      maxSelected = initialCount.toInt
    }
    // 要取的num大于总个数
    if (num > initialCount) {
      total = maxSelected
      fraction = math.min(multiplier * (maxSelected + 1) / initialCount, 1.0)
    } else if (num < 0) {
      throw(new IllegalArgumentException("Negative number of elements requested"))
    } else {
    // 占比乘以 3
      fraction = math.min(multiplier * (num + 1) / initialCount, 1.0)
      // 要取出total 个
      total = num
    }

    val rand = new Random(seed)
    var samples = this.sample(withReplacement, fraction, rand.nextInt).collect()

    while (samples.length < total) {
    // 不断sample 知道比total个数大
      samples = this.sample(withReplacement, fraction, rand.nextInt).collect()
    }

    // 取样后随机排序，再拿total个元素出来
    Utils.randomizeInPlace(samples, rand).take(total)
  }

  def union(other: RDD[T]): RDD[T] = new UnionRDD(sc, Array(this, other))

  def ++(other: RDD[T]): RDD[T] = this.union(other)

  def glom(): RDD[Array[T]] = new GlommedRDD(this)

// 创建cartesian Rdd
  def cartesian[U: ClassManifest](other: RDD[U]): RDD[(T, U)] = new CartesianRDD(sc, this, other)

//
  def groupBy[K: ClassManifest](f: T => K, numSplits: Int): RDD[(K, Seq[T])] = {
    val cleanF = sc.clean(f)
    // 计算出key 和 value pairs
    this.map(t => (cleanF(t), t)).groupByKey(numSplits)
  }
// groupBy算子接收一个函数，这个函数返回的值作为key，然后通过这个key来对里面的元素进行分组。
  def groupBy[K: ClassManifest](f: T => K): RDD[(K, Seq[T])] = groupBy[K](f, sc.defaultParallelism)

  def pipe(command: String): RDD[String] = new PipedRDD(this, command)

  def pipe(command: Seq[String]): RDD[String] = new PipedRDD(this, command)

  def pipe(command: Seq[String], env: Map[String, String]): RDD[String] =
    new PipedRDD(this, command, env)

  def mapPartitions[U: ClassManifest](f: Iterator[T] => Iterator[U]): RDD[U] =
    new MapPartitionsRDD(this, sc.clean(f))

  // Actions (launch a job to return a value to the user program)
  
  def foreach(f: T => Unit) {
    val cleanF = sc.clean(f)
    sc.runJob(this, (iter: Iterator[T]) => iter.foreach(cleanF))
  }

  def collect(): Array[T] = {
    // 消费可迭代对象，变成array
    val results = sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
    // 把所有数组合并起来
    Array.concat(results: _*)
  }

  def reduce(f: (T, T) => T): T = {
    val cleanF = sc.clean(f)
    // 返回iterator
    val reducePartition: Iterator[T] => Option[T] = iter => {
      if (iter.hasNext) {
        Some(iter.reduceLeft(cleanF))
      }else {
        None
      }
    }

    val options = sc.runJob(this, reducePartition)
    val results = new ArrayBuffer[T]
    // 将结果先拼接起来
    for (opt <- options; elem <- opt) {
      results += elem
    }
    if (results.size == 0) {
      throw new UnsupportedOperationException("empty collection")
    } else {
      return results.reduceLeft(cleanF)
    }
  }

  /**
   * Aggregate the elements of each partition, and then the results for all the partitions, using a
   * given associative function and a neutral "zero value". The function op(t1, t2) is allowed to 
   * modify t1 and return it as its result value to avoid object allocation; however, it should not
   * modify t2.
   */
  def fold(zeroValue: T)(op: (T, T) => T): T = {
    val cleanOp = sc.clean(op)
    val results = sc.runJob(this, (iter: Iterator[T]) => iter.fold(zeroValue)(cleanOp))
    return results.fold(zeroValue)(cleanOp)
  }

  /**
   * Aggregate the elements of each partition, and then the results for all the partitions, using
   * given combine functions and a neutral "zero value". This function can return a different result
   * type, U, than the type of this RDD, T. Thus, we need one operation for merging a T into an U
   * and one operation for merging two U's, as in scala.TraversableOnce. Both of these functions are
   * allowed to modify and return their first argument instead of creating a new U to avoid memory
   * allocation.
   */
  def aggregate[U: ClassManifest](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U = {
    val cleanSeqOp = sc.clean(seqOp)
    val cleanCombOp = sc.clean(combOp)
    val results = sc.runJob(this,
        (iter: Iterator[T]) => iter.aggregate(zeroValue)(cleanSeqOp, cleanCombOp))
    return results.fold(zeroValue)(cleanCombOp)
  }
  
  def count(): Long = {
  // 将数组的每个值加起来
    sc.runJob(this, (iter: Iterator[T]) => {
      var result = 0L
      while (iter.hasNext) {
        result += 1L
        iter.next
      }
      result
    }).sum
  }

  def toArray(): Array[T] = collect()
  
  /**
   * Take the first num elements of the RDD. This currently scans the partitions *one by one*, so
   * it will be slow if a lot of partitions are required. In that case, use collect() to get the
   * whole RDD instead.
   */
  // 一个一个遍历 partition
  def take(num: Int): Array[T] = {
   // 为0就不用拿
    if (num == 0) {
      return new Array[T](0)
    }
    val buf = new ArrayBuffer[T]
    var p = 0
    while (buf.size < num && p < splits.size) {
      // 还要取多少个
      val left = num - buf.size
      // 把那个分片的 left 个取出来， 跑第p个分片上的数据
      val res = sc.runJob(this, (it: Iterator[T]) => it.take(left).toArray, Array(p), true)
      // 结果是  [ [该分片的结果]  ]
      // 所以取出第0个并拼接
      // 拼接两个list
      buf ++= res(0)
      // 得到了num个
      if (buf.size == num)
        return buf.toArray
      p += 1
    }
    return buf.toArray
  }
  // 返回第一个
  def first(): T = take(1) match {
    case Array(t) => t
    case _ => throw new UnsupportedOperationException("empty collection")
  }

  def saveAsTextFile(path: String) {
    this.map(x => (NullWritable.get(), new Text(x.toString)))
      .saveAsHadoopFile[TextOutputFormat[NullWritable, Text]](path)
  }

  def saveAsObjectFile(path: String) {
    this.mapPartitions(iter => iter.grouped(10).map(_.toArray))
      .map(x => (NullWritable.get(), new BytesWritable(Utils.serialize(x))))
      .saveAsSequenceFile(path)
  }

  /** A private method for tests, to look at the contents of each partition */
  private[spark] def collectPartitions(): Array[Array[T]] = {
    sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
  }
}

class MappedRDD[U: ClassManifest, T: ClassManifest](
    prev: RDD[T],
    f: T => U)
  extends RDD[U](prev.context) {
  // 分片保持一致
  override def splits = prev.splits
  // 一对一的依赖
  override val dependencies = List(new OneToOneDependency(prev))
  // 计算某个分片
  override def compute(split: Split) = prev.iterator(split).map(f)
}

class FlatMappedRDD[U: ClassManifest, T: ClassManifest](
    prev: RDD[T],
    f: T => TraversableOnce[U])
  extends RDD[U](prev.context) {
  // 分片保持一致
  override def splits = prev.splits
  // 一对一的依赖
  override val dependencies = List(new OneToOneDependency(prev))
  override def compute(split: Split) = prev.iterator(split).flatMap(f)
}

class FilteredRDD[T: ClassManifest](prev: RDD[T], f: T => Boolean) extends RDD[T](prev.context) {
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def compute(split: Split) = prev.iterator(split).filter(f)
}

class GlommedRDD[T: ClassManifest](prev: RDD[T]) extends RDD[Array[T]](prev.context) {
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def compute(split: Split) = Array(prev.iterator(split).toArray).iterator
}

class MapPartitionsRDD[U: ClassManifest, T: ClassManifest](
    prev: RDD[T],
    f: Iterator[T] => Iterator[U])
  extends RDD[U](prev.context) {
  
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  // func是一个输入为 Iterator 输出 也为 Iterator的东西，所以叫 map partitions
  override def compute(split: Split) = f(prev.iterator(split))
}

package spark

import java.io.EOFException
import java.net.URL
import java.io.ObjectInputStream
import java.util.concurrent.atomic.AtomicLong
import java.util.HashSet
import java.util.Random
import java.util.Date
import java.text.SimpleDateFormat

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import scala.collection.mutable.HashMap

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.FileOutputCommitter
import org.apache.hadoop.mapred.FileOutputFormat
import org.apache.hadoop.mapred.HadoopWriter
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCommitter
import org.apache.hadoop.mapred.OutputFormat
import org.apache.hadoop.mapred.SequenceFileOutputFormat
import org.apache.hadoop.mapred.TextOutputFormat

import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat => NewFileOutputFormat}
import org.apache.hadoop.mapreduce.{OutputFormat => NewOutputFormat, RecordWriter => NewRecordWriter, Job => NewAPIHadoopJob, HadoopMapReduceUtil, TaskAttemptID, TaskAttemptContext}

import SparkContext._

/**
 * Extra functions available on RDDs of (key, value) pairs through an implicit conversion.
 */
class PairRDDFunctions[K: ClassManifest, V: ClassManifest](
    self: RDD[(K, V)])
  extends Logging
  with HadoopMapReduceUtil
  with Serializable {
  
  def reduceByKeyToDriver(func: (V, V) => V): Map[K, V] = {
    def mergeMaps(m1: HashMap[K, V], m2: HashMap[K, V]): HashMap[K, V] = {
      for ((k, v) <- m2) {
        m1.get(k) match {
          case None => m1(k) = v
          case Some(w) => m1(k) = func(w, v)
        }
      }
      return m1
    }
    self.map(pair => HashMap(pair)).reduce(mergeMaps)
  }


// 根据key 来combine
  def combineByKey[C](createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      partitioner: Partitioner): RDD[(K, C)] = {
    val aggregator = new Aggregator[K, V, C](createCombiner, mergeValue, mergeCombiners)
    new ShuffledRDD(self, aggregator, partitioner)
  }

  def combineByKey[C](createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      numSplits: Int): RDD[(K, C)] = {
    combineByKey(createCombiner, mergeValue, mergeCombiners, new HashPartitioner(numSplits))
  }

  def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDD[(K, V)] = {
    combineByKey[V]((v: V) => v, func, func, partitioner)
  }

  def reduceByKey(func: (V, V) => V, numSplits: Int): RDD[(K, V)] = {
    reduceByKey(new HashPartitioner(numSplits), func)
  }

  def groupByKey(partitioner: Partitioner): RDD[(K, Seq[V])] = {
    def createCombiner(v: V) = ArrayBuffer(v)
    def mergeValue(buf: ArrayBuffer[V], v: V) = buf += v
    def mergeCombiners(b1: ArrayBuffer[V], b2: ArrayBuffer[V]) = b1 ++= b2
    val bufs = combineByKey[ArrayBuffer[V]](
      createCombiner _, mergeValue _, mergeCombiners _, partitioner)
    bufs.asInstanceOf[RDD[(K, Seq[V])]]
  }

  def groupByKey(numSplits: Int): RDD[(K, Seq[V])] = {
    groupByKey(new HashPartitioner(numSplits))
  }

  def partitionBy(partitioner: Partitioner): RDD[(K, V)] = {
     // 在分区中第一次碰到key，创建一个只有单个元素的array
    def createCombiner(v: V) = ArrayBuffer(v)
     // 在分区中第二次或第n次碰到key，将v添加进来
    def mergeValue(buf: ArrayBuffer[V], v: V) = buf += v
     // 在不同分区间合并，将两个array拼接起来
    def mergeCombiners(b1: ArrayBuffer[V], b2: ArrayBuffer[V]) = b1 ++= b2
     // bufs 是一个shuffle的rdd， shuffle rdd 会将一个分片上 key, value 对变成 key list of values 对

    val bufs = combineByKey[ArrayBuffer[V]](
      createCombiner _, mergeValue _, mergeCombiners _, partitioner)
    // 把所有的key ，values 展开为 key1， value1  key2， value2
    bufs.flatMapValues(buf => buf)
  }

// 和另一个rdd 进行 join
  def join[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (V, W))] = {
    // partitioner 使用自己的partitioner，所以other 是shuffle 类型的dependency

    // 两个rdd  cogroup 出来的东西是   { key , [ [v1,v2,v3] , [a1,a2] ] }
    // 所以需要faltMapValues 来将 values 两两组合，并打散

   // 生成 { key , [v1,a1] } , {key , [v1, a2]} , {key , [v1, a3]} 等等
    this.cogroup(other, partitioner).flatMapValues {
      case (vs, ws) =>
        for (v <- vs.iterator; w <- ws.iterator) yield (v, w)
    }
  }

  def leftOuterJoin[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (V, Option[W]))] = {
    this.cogroup(other, partitioner).flatMapValues {
      case (vs, ws) =>
        if (ws.isEmpty) {
          vs.iterator.map(v => (v, None))
        } else {
          for (v <- vs.iterator; w <- ws.iterator) yield (v, Some(w))
        }
    }
  }

  def rightOuterJoin[W](other: RDD[(K, W)], partitioner: Partitioner)
      : RDD[(K, (Option[V], W))] = {
    this.cogroup(other, partitioner).flatMapValues {
      case (vs, ws) =>
        if (vs.isEmpty) {
          ws.iterator.map(w => (None, w))
        } else {
          for (v <- vs.iterator; w <- ws.iterator) yield (Some(v), w)
        }
    }
  }

  def combineByKey[C](createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C) : RDD[(K, C)] = {
    combineByKey(createCombiner, mergeValue, mergeCombiners, defaultPartitioner(self))
  }

  def reduceByKey(func: (V, V) => V): RDD[(K, V)] = {
    reduceByKey(defaultPartitioner(self), func)
  }

  def groupByKey(): RDD[(K, Seq[V])] = {
    groupByKey(defaultPartitioner(self))
  }

  //
  def join[W](other: RDD[(K, W)]): RDD[(K, (V, W))] = {
    // 如果自己有 partitioner ，就还是用自己的partitioner，

    join(other, defaultPartitioner(self, other))
  }

  // 如果指定 分片数，就用hashPartition
  def join[W](other: RDD[(K, W)], numSplits: Int): RDD[(K, (V, W))] = {
    join(other, new HashPartitioner(numSplits))
  }

  def leftOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (V, Option[W]))] = {
    leftOuterJoin(other, defaultPartitioner(self, other))
  }

  def leftOuterJoin[W](other: RDD[(K, W)], numSplits: Int): RDD[(K, (V, Option[W]))] = {
    leftOuterJoin(other, new HashPartitioner(numSplits))
  }

  def rightOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (Option[V], W))] = {
    rightOuterJoin(other, defaultPartitioner(self, other))
  }

  def rightOuterJoin[W](other: RDD[(K, W)], numSplits: Int): RDD[(K, (Option[V], W))] = {
    rightOuterJoin(other, new HashPartitioner(numSplits))
  }

  def collectAsMap(): Map[K, V] = HashMap(self.collect(): _*)
  
  def mapValues[U](f: V => U): RDD[(K, U)] = {
    val cleanF = self.context.clean(f)
    new MappedValuesRDD(self, cleanF)
  }
  
  def flatMapValues[U](f: V => TraversableOnce[U]): RDD[(K, U)] = {
    val cleanF = self.context.clean(f)
    new FlatMappedValuesRDD(self, cleanF)
  }
  
  def cogroup[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (Seq[V], Seq[W]))] = {
    // 将自己和other 组成一个cogroup ， 自己是 窄依赖， other 是宽依赖
    val cg = new CoGroupedRDD[K](
        Seq(self.asInstanceOf[RDD[(_, _)]], other.asInstanceOf[RDD[(_, _)]]),
        partitioner)
    val prfs = new PairRDDFunctions[K, Seq[Seq[_]]](cg)(classManifest[K], Manifests.seqSeqManifest)
    prfs.mapValues {
      case Seq(vs, ws) =>
        (vs.asInstanceOf[Seq[V]], ws.asInstanceOf[Seq[W]])
    }
  }
  
  def cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)], partitioner: Partitioner)
      : RDD[(K, (Seq[V], Seq[W1], Seq[W2]))] = {
    val cg = new CoGroupedRDD[K](
        Seq(self.asInstanceOf[RDD[(_, _)]],
            other1.asInstanceOf[RDD[(_, _)]], 
            other2.asInstanceOf[RDD[(_, _)]]),
        partitioner)
    val prfs = new PairRDDFunctions[K, Seq[Seq[_]]](cg)(classManifest[K], Manifests.seqSeqManifest)
    prfs.mapValues {
      case Seq(vs, w1s, w2s) =>
        (vs.asInstanceOf[Seq[V]], w1s.asInstanceOf[Seq[W1]], w2s.asInstanceOf[Seq[W2]])
    }
  }

  def cogroup[W](other: RDD[(K, W)]): RDD[(K, (Seq[V], Seq[W]))] = {
    cogroup(other, defaultPartitioner(self, other))
  }

  def cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)])
      : RDD[(K, (Seq[V], Seq[W1], Seq[W2]))] = {
    cogroup(other1, other2, defaultPartitioner(self, other1, other2))
  }

  def cogroup[W](other: RDD[(K, W)], numSplits: Int): RDD[(K, (Seq[V], Seq[W]))] = {
    cogroup(other, new HashPartitioner(numSplits))
  }

  def cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)], numSplits: Int)
      : RDD[(K, (Seq[V], Seq[W1], Seq[W2]))] = {
    cogroup(other1, other2, new HashPartitioner(numSplits))
  }

  def groupWith[W](other: RDD[(K, W)]): RDD[(K, (Seq[V], Seq[W]))] = {
    cogroup(other, defaultPartitioner(self, other))
  }

  def groupWith[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)])
      : RDD[(K, (Seq[V], Seq[W1], Seq[W2]))] = {
    cogroup(other1, other2, defaultPartitioner(self, other1, other2))
  }

  /**
   * Choose a partitioner to use for a cogroup-like operation between a number of RDDs. If any of
   * the RDDs already has a partitioner, choose that one, otherwise use a default HashPartitioner.
   */
   // 如果遇到有partition的 rdd ，就用那个 partitioner ，不然就用 hash partitioner
  def defaultPartitioner(rdds: RDD[_]*): Partitioner = {
    for (r <- rdds if r.partitioner != None) {
      return r.partitioner.get
    }
    return new HashPartitioner(self.context.defaultParallelism)
  }

  def lookup(key: K): Seq[V] = {
    self.partitioner match {
      case Some(p) =>
        val index = p.getPartition(key)
        def process(it: Iterator[(K, V)]): Seq[V] = {
          val buf = new ArrayBuffer[V]
          for ((k, v) <- it if k == key) {
            buf += v
          }
          buf
        }
        val res = self.context.runJob(self, process _, Array(index), false)
        res(0)
      case None =>
        throw new UnsupportedOperationException("lookup() called on an RDD without a partitioner")
    }
  }

  def saveAsHadoopFile[F <: OutputFormat[K, V]](path: String)(implicit fm: ClassManifest[F]) {
    saveAsHadoopFile(path, getKeyClass, getValueClass, fm.erasure.asInstanceOf[Class[F]])
  }
  
  def saveAsNewAPIHadoopFile[F <: NewOutputFormat[K, V]](path: String)(implicit fm: ClassManifest[F]) {
    saveAsNewAPIHadoopFile(path, getKeyClass, getValueClass, fm.erasure.asInstanceOf[Class[F]])
  }

  def saveAsNewAPIHadoopFile(
      path: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[_ <: NewOutputFormat[_, _]]) {
    saveAsNewAPIHadoopFile(path, keyClass, valueClass, outputFormatClass, new Configuration)
  }

  def saveAsNewAPIHadoopFile(
      path: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[_ <: NewOutputFormat[_, _]],
      conf: Configuration) {
    val job = new NewAPIHadoopJob(conf)
    job.setOutputKeyClass(keyClass)
    job.setOutputValueClass(valueClass)
    val wrappedConf = new SerializableWritable(job.getConfiguration)
    NewFileOutputFormat.setOutputPath(job, new Path(path))
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    val jobtrackerID = formatter.format(new Date())
    val stageId = self.id
    def writeShard(context: spark.TaskContext, iter: Iterator[(K,V)]): Int = {
      /* "reduce task" <split #> <attempt # = spark task #> */
      val attemptId = new TaskAttemptID(jobtrackerID,
        stageId, false, context.splitId, context.attemptId)
      val hadoopContext = newTaskAttemptContext(wrappedConf.value, attemptId)
      val format = outputFormatClass.newInstance
      val committer = format.getOutputCommitter(hadoopContext)
      committer.setupTask(hadoopContext)
      val writer = format.getRecordWriter(hadoopContext).asInstanceOf[NewRecordWriter[K,V]]
      while (iter.hasNext) {
        val (k, v) = iter.next
        writer.write(k, v)
      }
      writer.close(hadoopContext)
      committer.commitTask(hadoopContext)
      return 1
    }
    val jobFormat = outputFormatClass.newInstance
    /* apparently we need a TaskAttemptID to construct an OutputCommitter;
     * however we're only going to use this local OutputCommitter for
     * setupJob/commitJob, so we just use a dummy "map" task.
     */
    val jobAttemptId = new TaskAttemptID(jobtrackerID, stageId, true, 0, 0)
    val jobTaskContext = newTaskAttemptContext(wrappedConf.value, jobAttemptId)
    val jobCommitter = jobFormat.getOutputCommitter(jobTaskContext)
    jobCommitter.setupJob(jobTaskContext)
    val count = self.context.runJob(self, writeShard _).sum
    jobCommitter.cleanupJob(jobTaskContext)
  }

  def saveAsHadoopFile(
      path: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[_ <: OutputFormat[_, _]],
      conf: JobConf = new JobConf) {
    conf.setOutputKeyClass(keyClass)
    conf.setOutputValueClass(valueClass)
    // conf.setOutputFormat(outputFormatClass) // Doesn't work in Scala 2.9 due to what may be a generics bug
    conf.set("mapred.output.format.class", outputFormatClass.getName)
    conf.setOutputCommitter(classOf[FileOutputCommitter])
    FileOutputFormat.setOutputPath(conf, HadoopWriter.createPathFromString(path, conf))
    saveAsHadoopDataset(conf)
  }
  
  def saveAsHadoopDataset(conf: JobConf) {
    val outputFormatClass = conf.getOutputFormat
    val keyClass = conf.getOutputKeyClass
    val valueClass = conf.getOutputValueClass
    if (outputFormatClass == null) {
      throw new SparkException("Output format class not set")
    }
    if (keyClass == null) {
      throw new SparkException("Output key class not set")
    }
    if (valueClass == null) {
      throw new SparkException("Output value class not set")
    }
    
    logInfo("Saving as hadoop file of type (" + keyClass.getSimpleName+ ", " + valueClass.getSimpleName+ ")")

    val writer = new HadoopWriter(conf)
    writer.preSetup()

    def writeToFile(context: TaskContext, iter: Iterator[(K,V)]) {
      writer.setup(context.stageId, context.splitId, context.attemptId)
      writer.open()
      
      var count = 0
      while(iter.hasNext) {
        val record = iter.next
        count += 1
        writer.write(record._1.asInstanceOf[AnyRef], record._2.asInstanceOf[AnyRef])
      }
    
      writer.close()
      writer.commit()
    }

    self.context.runJob(self, writeToFile _)
    writer.cleanup()
  }

  def getKeyClass() = implicitly[ClassManifest[K]].erasure

  def getValueClass() = implicitly[ClassManifest[V]].erasure
}

// key是一个能排序的东西
class OrderedRDDFunctions[K <% Ordered[K]: ClassManifest, V: ClassManifest](
  self: RDD[(K, V)])
  extends Logging 
  with Serializable {

 // 先根据key 的 Range 算出每个partition的分界，将老的split里的所有数据根据partitioner
 // 放到新的分片中，生成新的rdd
  def sortByKey(ascending: Boolean = true): RDD[(K,V)] = {
    val rangePartitionedRDD = self.partitionBy(new RangePartitioner(self.splits.size, self, ascending))
    // 对每个分区排序
    new SortedRDD(rangePartitionedRDD, ascending)
  }
}

class SortedRDD[K <% Ordered[K], V](prev: RDD[(K, V)], ascending: Boolean)
  extends RDD[(K, V)](prev.context) {
 // 用一样的split， 一样的partitioner，只是计算完了以后排个序
  override def splits = prev.splits
  override val partitioner = prev.partitioner
  override val dependencies = List(new OneToOneDependency(prev))

   // 对这个分片的元素按照key 排序
  override def compute(split: Split) = {
    prev.iterator(split).toArray
      .sortWith((x, y) => if (ascending) x._1 < y._1 else x._1 > y._1).iterator
  }
} 
 
class MappedValuesRDD[K, V, U](prev: RDD[(K, V)], f: V => U) extends RDD[(K, U)](prev.context) {
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override val partitioner = prev.partitioner
  override def compute(split: Split) = prev.iterator(split).map{case (k, v) => (k, f(v))}
}

class FlatMappedValuesRDD[K, V, U](prev: RDD[(K, V)], f: V => TraversableOnce[U])
  extends RDD[(K, U)](prev.context) {
  
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override val partitioner = prev.partitioner

  override def compute(split: Split) = {
    prev.iterator(split).flatMap { case (k, v) => f(v).map(x => (k, x)) }
  }
}

object Manifests {
  val seqSeqManifest = classManifest[Seq[Seq[_]]]
}

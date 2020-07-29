package spark

import java.net.URL
import java.io.EOFException
import java.io.ObjectInputStream
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

sealed trait CoGroupSplitDep extends Serializable
case class NarrowCoGroupSplitDep(rdd: RDD[_], split: Split) extends CoGroupSplitDep
case class ShuffleCoGroupSplitDep(shuffleId: Int) extends CoGroupSplitDep

class CoGroupSplit(idx: Int, val deps: Seq[CoGroupSplitDep]) extends Split with Serializable {
  override val index = idx
  override def hashCode(): Int = idx
}

// 在跑 shufflemaptask时 使用ArrayBuffer 作为容器，把元素的value 装入里面
class CoGroupAggregator
  extends Aggregator[Any, Any, ArrayBuffer[Any]](
    { x => ArrayBuffer(x) },
    { (b, x) => b += x },
    { (b1, b2) => b1 ++ b2 })
  with Serializable

// 输入为一堆rdd
class CoGroupedRDD[K](rdds: Seq[RDD[(_, _)]], part: Partitioner)
  extends RDD[(K, Seq[Seq[_]])](rdds.head.context) with Logging {

  // agg 是CoGroup的Agg
  val aggr = new CoGroupAggregator

  // dependencies 数组，记录和每个rdd的关系
  override val dependencies = {
    val deps = new ArrayBuffer[Dependency[_]]
    // 每个rdd
    for ((rdd, index) <- rdds.zipWithIndex) {
    // 与当前的partitioner一致，说明是一对一，一般就是自己， rdd1.join(rdd2) 中的rdd1
      if (rdd.partitioner == Some(part)) {
        logInfo("Adding one-to-one dependency with " + rdd)
        deps += new OneToOneDependency(rdd)
      } else {

    // 不是自己
        logInfo("Adding shuffle dependency with " + rdd)
        deps += new ShuffleDependency[Any, Any, ArrayBuffer[Any]](
            context.newShuffleId, rdd, aggr, part)
      }
    }
    deps.toList
  }
  
  @transient
  val splits_ : Array[Split] = {
    val firstRdd = rdds.head
    // 分为几片？使用part里的 partitions 数量
    val array = new Array[Split](part.numPartitions)

    for (i <- 0 until array.size) {
    // 创建一个虚拟的分片，该分片有着对所有rdd 的依赖，如果时OnetoOne，就创建NarrowCoGroupSplitDep
    // 也就是说，这个split 既有 narrow的dependency 又有shuffle 的dependency
      array(i) = new CoGroupSplit(i, rdds.zipWithIndex.map { case (r, j) =>
        dependencies(j) match {
          case s: ShuffleDependency[_, _, _] =>
          // 记录shuffle id
            new ShuffleCoGroupSplitDep(s.shuffleId): CoGroupSplitDep
          case _ =>
            // 与自己的 第i个分片对应
            new NarrowCoGroupSplitDep(r, r.splits(i)): CoGroupSplitDep
        }
      }.toList)
    }
    array
  }

  override def splits = splits_
  
  override val partitioner = Some(part)
  
  override def preferredLocations(s: Split) = Nil
  
  override def compute(s: Split): Iterator[(K, Seq[Seq[_]])] = {
    // 拿到split
    val split = s.asInstanceOf[CoGroupSplit]
    val map = new HashMap[K, Seq[ArrayBuffer[Any]]]
    // 对每个key， value 是一个数组，有rdds size 个坑位，让每个依赖的rdd填入自己的坑位
    def getSeq(k: K): Seq[ArrayBuffer[Any]] = {
      map.getOrElseUpdate(k, Array.fill(rdds.size)(new ArrayBuffer[Any]))
    }

    // split的依赖
    for ((dep, depNum) <- split.deps.zipWithIndex) dep match {
    // 依赖于自己的某个分片
      case NarrowCoGroupSplitDep(rdd, itsSplit) => {
        // Read them from the parent
        // 跑出当前分片，根据key 拿到坑位们，然后放到它自己的坑位里
        for ((k, v) <- rdd.iterator(itsSplit)) {
          getSeq(k.asInstanceOf[K])(depNum) += v
        }
      }
      case ShuffleCoGroupSplitDep(shuffleId) => {
        // Read map outputs of shuffle
        def mergePair(k: K, vs: Seq[Any]) {
        // 获得key 对应的坑位们
          val mySeq = getSeq(k)
          // 遍历数组，一个一个填坑
          for (v <- vs)
            mySeq(depNum) += v
        }
        // 拿到fetcher，让他去fetch 数据
        val fetcher = SparkEnv.get.shuffleFetcher
        // split 在rdd里的index
        fetcher.fetch[K, Seq[Any]](shuffleId, split.index, mergePair)
      }
    }
    map.iterator
  }
}

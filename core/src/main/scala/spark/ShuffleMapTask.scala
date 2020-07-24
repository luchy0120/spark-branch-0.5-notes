package spark

import java.io.BufferedOutputStream
import java.io.FileOutputStream
import java.io.ObjectOutputStream
import java.util.{HashMap => JHashMap}

import it.unimi.dsi.fastutil.io.FastBufferedOutputStream

class ShuffleMapTask(
    runId: Int,
    stageId: Int,
    rdd: RDD[_], 
    dep: ShuffleDependency[_,_,_],
    val partition: Int, 
    locs: Seq[String])
  extends DAGTask[String](runId, stageId)
  with Logging {

  // 得到当前的split
  val split = rdd.splits(partition)

  //  尝试的id
  override def run (attemptId: Int): String = {
    // 依赖的partitions
    val numOutputSplits = dep.partitioner.numPartitions
    val aggregator = dep.aggregator.asInstanceOf[Aggregator[Any, Any, Any]]
    // 通过key能拿到 partition id
    val partitioner = dep.partitioner.asInstanceOf[Partitioner]
    // 每个 partition 有一个 Hashmap
    val buckets = Array.tabulate(numOutputSplits)(_ => new JHashMap[Any, Any])
    // 遍历split
    for (elem <- rdd.iterator(split)) {
      val (k, v) = elem.asInstanceOf[(Any, Any)]
      var bucketId = partitioner.getPartition(k)
      // 落到哪个partition
      val bucket = buckets(bucketId)
      var existing = bucket.get(k)
      if (existing == null) {
        bucket.put(k, aggregator.createCombiner(v))
      } else {
        bucket.put(k, aggregator.mergeValue(existing, v))
      }
    }
    val ser = SparkEnv.get.serializer.newInstance()
    for (i <- 0 until numOutputSplits) {
      val file = SparkEnv.get.shuffleManager.getOutputFile(dep.shuffleId, partition, i)
      val out = ser.outputStream(new FastBufferedOutputStream(new FileOutputStream(file)))
      out.writeObject(buckets(i).size)
      // 每一个bucket 里的key value 写出去
      val iter = buckets(i).entrySet().iterator()
      while (iter.hasNext()) {
      // 每一个key 和 value 写出
        val entry = iter.next()
        out.writeObject((entry.getKey, entry.getValue))
      }
      // TODO: have some kind of EOF marker
      out.close()
    }
    return SparkEnv.get.shuffleManager.getServerUri
  }

  override def preferredLocations: Seq[String] = locs

  // 一个shuffle 对应多少partition
  override def toString = "ShuffleMapTask(%d, %d)".format(stageId, partition)
}

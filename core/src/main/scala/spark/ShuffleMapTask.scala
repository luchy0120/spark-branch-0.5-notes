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
    // 父级依赖的partitions，因为是窄依赖，所以分片数跟父亲的分片数一样
    val numOutputSplits = dep.partitioner.numPartitions
    val aggregator = dep.aggregator.asInstanceOf[Aggregator[Any, Any, Any]]
    // 通过key能拿到 partition id
    val partitioner = dep.partitioner.asInstanceOf[Partitioner]
    // 每个 partition 有一个 Hashmap
    val buckets = Array.tabulate(numOutputSplits)(_ => new JHashMap[Any, Any])
    // 遍历split，并运行func 以后的结果
    for (elem <- rdd.iterator(split)) {
    // 结果是key value
      val (k, v) = elem.asInstanceOf[(Any, Any)]
      // 获得落在那个bucket
      var bucketId = partitioner.getPartition(k)
      // 落到哪个bucket
      val bucket = buckets(bucketId)

      var existing = bucket.get(k)
      if (existing == null) {
        // 不存在，就先创建一个单个元素的容器
        bucket.put(k, aggregator.createCombiner(v))
      } else {
        // 如果存在，就将新的值合并进容器中
        bucket.put(k, aggregator.mergeValue(existing, v))
      }
    }
    val ser = SparkEnv.get.serializer.newInstance()
    // 遍历每个bucket
    for (i <- 0 until numOutputSplits) {
      // 准备落盘到哪个文件， 每个bucket 都会创建一个文件来写
      val file = SparkEnv.get.shuffleManager.getOutputFile(dep.shuffleId, partition, i)
      // 写出时会序列化
      val out = ser.outputStream(new FastBufferedOutputStream(new FileOutputStream(file)))
      // 先写一个bucket 的大小
      out.writeObject(buckets(i).size)
      // 再把 bucket 里的key value 写出去
      val iter = buckets(i).entrySet().iterator()
      while (iter.hasNext()) {
      // 每一个key 和 value 写出
        val entry = iter.next()
        out.writeObject((entry.getKey, entry.getValue))
      }
      // TODO: have some kind of EOF marker
      out.close()
    }
    // 公布一下 uri
    return SparkEnv.get.shuffleManager.getServerUri
  }

  override def preferredLocations: Seq[String] = locs

  // 一个shuffle 对应多少partition
  override def toString = "ShuffleMapTask(%d, %d)".format(stageId, partition)
}

package spark

abstract class Partitioner extends Serializable {
  def numPartitions: Int
  def getPartition(key: Any): Int
}

// 根据  key 的 hash 定位 partition
// hash partition
class HashPartitioner(partitions: Int) extends Partitioner {
  // 总共分成几片
  def numPartitions = partitions
  // 根据key 获取 对应的partition
  def getPartition(key: Any): Int = {
    if (key == null) {
      return 0
    } else {
    // 取模
      val mod = key.hashCode % partitions
      if (mod < 0) {
        mod + partitions
      } else {
        mod // Guard against negative hash codes
      }
    }
  }
  
  override def equals(other: Any): Boolean = other match {
    case h: HashPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }
}

// 根据key的值的范围，平均在每个partition里取20个值，取过来排序，排序完后分成partition段
// 取每一段的最大值作为partition的分界值
class RangePartitioner[K <% Ordered[K]: ClassManifest, V](
    partitions: Int,
    @transient rdd: RDD[(K,V)],
    private val ascending: Boolean = true)
  extends Partitioner {

  // An array of upper bounds for the first (partitions - 1) partitions
  // 每个partition的上限值
  private val rangeBounds: Array[K] = {
    if (partitions == 1) {
      Array()
    } else {
    // rdd 里元素数量
      val rddSize = rdd.count()
      // 每个 partition 有20个
      val maxSampleSize = partitions * 20.0
      // 采样的比例
      val frac = math.min(maxSampleSize / math.max(rddSize, 1), 1.0)
      // 采样并排序
      val rddSample = rdd.sample(true, frac, 1).map(_._1).collect().sortWith(_ < _)
      if (rddSample.length == 0) {
        Array()
      } else {
        val bounds = new Array[K](partitions - 1)
        for (i <- 0 until partitions - 1) {
          // 将采样的值平均分为 partition 段，取每段的最大值
          val index = (rddSample.length - 1) * (i + 1) / partitions
          bounds(i) = rddSample(index)
        }
        bounds
      }
    }
  }

  def numPartitions = partitions


  def getPartition(key: Any): Int = {
    // TODO: Use a binary search here if number of partitions is large
    val k = key.asInstanceOf[K]
    var partition = 0
    // 一直找，直到 k 小于 某个 rangeBound
    while (partition < rangeBounds.length && k > rangeBounds(partition)) {
      partition += 1
    }
    if (ascending) {
      partition
    } else {
      rangeBounds.length - partition
    }
  }

  override def equals(other: Any): Boolean = other match {
    case r: RangePartitioner[_,_] =>
      r.rangeBounds.sameElements(rangeBounds) && r.ascending == ascending
    case _ =>
      false
  }
}


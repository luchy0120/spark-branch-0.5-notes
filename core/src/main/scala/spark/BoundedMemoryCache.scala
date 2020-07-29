package spark

import java.util.LinkedHashMap

/**
 * An implementation of Cache that estimates the sizes of its entries and attempts to limit its
 * total memory usage to a fraction of the JVM heap. Objects' sizes are estimated using
 * SizeEstimator, which has limitations; most notably, we will overestimate total memory used if
 * some cache entries have pointers to a shared object. Nonetheless, this Cache should work well
 * when most of the space is used by arrays of primitives or of simple classes.
 */
class BoundedMemoryCache(maxBytes: Long) extends Cache with Logging {
  logInfo("BoundedMemoryCache.maxBytes = " + maxBytes)

  def this() {
    this(BoundedMemoryCache.getMaxBytes)
  }

  private var currentBytes = 0L
  // 使用LinkedHashMap 来做 lru 的cache
  private val map = new LinkedHashMap[(Any, Int), Entry](32, 0.75f, true)


// 根据rddId 和partition 值 获取 value
  override def get(datasetId: Any, partition: Int): Any = {
    synchronized {
      val entry = map.get((datasetId, partition))
      if (entry != null) {
        entry.value
      } else {
        null
      }
    }
  }

  override def put(datasetId: Any, partition: Int, value: Any): CachePutResponse = {
    val key = (datasetId, partition)
    logInfo("Asked to add key " + key)

    // 估计 value 的总大小
    val size = estimateValueSize(key, value)
    synchronized {
    // 估计单一对象的大小，如果超过了整个capacity，直接报failure
      if (size > getCapacity) {
        return CachePutFailure()
        //
      } else if (ensureFreeSpace(datasetId, size)) {
      // 添加进来
        logInfo("Adding key " + key)
        // 添加的是value 和 它的估计大小
        map.put(key, new Entry(value, size))
        // 当前 cache 大小
        currentBytes += size
        logInfo("Number of entries is now " + map.size)
        return CachePutSuccess(size)
      } else {
        logInfo("Didn't add key " + key + " because we would have evicted part of same dataset")
        return CachePutFailure()
      }
    }
  }

  override def getCapacity: Long = maxBytes

  /**
   * Estimate sizeOf 'value'
   */
  private def estimateValueSize(key: (Any, Int), value: Any) = {
    val startTime = System.currentTimeMillis
    // 估计value的大小
    val size = SizeEstimator.estimate(value.asInstanceOf[AnyRef])
    val timeTaken = System.currentTimeMillis - startTime
    logInfo("Estimated size for key %s is %d".format(key, size))
    logInfo("Size estimation for key %s took %d ms".format(key, timeTaken))
    size
  }

  /**
   * Remove least recently used entries from the map until at least space bytes are free, in order
   * to make space for a partition from the given dataset ID. If this cannot be done without
   * evicting other data from the same dataset, returns false; otherwise, returns true. Assumes
   * that a lock is held on the BoundedMemoryCache.
   */
   // 根据lru 算法移除元素，直到有足够的空间给元素，如果不能，就false
  private def ensureFreeSpace(datasetId: Any, space: Long): Boolean = {
    logInfo("ensureFreeSpace(%s, %d) called with curBytes=%d, maxBytes=%d".format(
      datasetId, space, currentBytes, maxBytes))

    val iter = map.entrySet.iterator   // Will give entries in LRU order
    // 空间不够 并且还可以看看下一个元素
    while (maxBytes - currentBytes < space && iter.hasNext) {
    // 看看下一个元素
      val mapEntry = iter.next()
      val (entryDatasetId, entryPartition) = mapEntry.getKey
    // 有同样的rdd 的数据在cache 里
      if (entryDatasetId == datasetId) {
        // Cannot make space without removing part of the same dataset, or a more recently used one
        return false
      }
    // 把它删掉 ，并记录一下
      reportEntryDropped(entryDatasetId, entryPartition, mapEntry.getValue)
    // 当前大小减掉
      currentBytes -= mapEntry.getValue.size
      iter.remove()
    }
    return true
  }

  protected def reportEntryDropped(datasetId: Any, partition: Int, entry: Entry) {
    logInfo("Dropping key (%s, %d) of size %d to make space".format(datasetId, partition, entry.size))
    // 告诉 tracker 有个元素被删掉了
    SparkEnv.get.cacheTracker.dropEntry(datasetId, partition)
  }
}

// An entry in our map; stores a cached object and its size in bytes
case class Entry(value: Any, size: Long)

object BoundedMemoryCache {
  /**
   * Get maximum cache capacity from system configuration
   */
   def getMaxBytes: Long = {
   // heap 总内存的2/3 作为cache 的最大容量
    val memoryFractionToUse = System.getProperty("spark.boundedMemoryCache.memoryFraction", "0.66").toDouble
    (Runtime.getRuntime.maxMemory * memoryFractionToUse).toLong
  }
}


package spark

import scala.actors._
import scala.actors.Actor._
import scala.actors.remote._
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

sealed trait CacheTrackerMessage
case class AddedToCache(rddId: Int, partition: Int, host: String, size: Long = 0L)
  extends CacheTrackerMessage
case class DroppedFromCache(rddId: Int, partition: Int, host: String, size: Long = 0L)
  extends CacheTrackerMessage
case class MemoryCacheLost(host: String) extends CacheTrackerMessage
case class RegisterRDD(rddId: Int, numPartitions: Int) extends CacheTrackerMessage
case class SlaveCacheStarted(host: String, size: Long) extends CacheTrackerMessage
case object GetCacheStatus extends CacheTrackerMessage
case object GetCacheLocations extends CacheTrackerMessage
case object StopCacheTracker extends CacheTrackerMessage


class CacheTrackerActor extends DaemonActor with Logging {
  private val locs = new HashMap[Int, Array[List[String]]]

  /**
   * A map from the slave's host name to its cache size.
   */
  private val slaveCapacity = new HashMap[String, Long]
  private val slaveUsage = new HashMap[String, Long]

  // TODO: Should probably store (String, CacheType) tuples

  private def getCacheUsage(host: String): Long = slaveUsage.getOrElse(host, 0L)
  private def getCacheCapacity(host: String): Long = slaveCapacity.getOrElse(host, 0L)
  private def getCacheAvailable(host: String): Long = getCacheCapacity(host) - getCacheUsage(host)
  
  def act() {
  // master 启动
    val port = System.getProperty("spark.master.port").toInt
    RemoteActor.alive(port)
    RemoteActor.register('CacheTracker, self)
    logInfo("Registered actor on port " + port)
    
    loop {
      react {
        // slave 汇报它启动了，capacity 是多少
        case SlaveCacheStarted(host: String, size: Long) =>
          logInfo("Started slave cache (size %s) on %s".format(
            Utils.memoryBytesToString(size), host))
          slaveCapacity.put(host, size)
          slaveUsage.put(host, 0)
          reply('OK)
        // rddId 和 一堆partition 的host
        case RegisterRDD(rddId: Int, numPartitions: Int) =>
          logInfo("Registering RDD " + rddId + " with " + numPartitions + " partitions")
          locs(rddId) = Array.fill[List[String]](numPartitions)(Nil)
          reply('OK)
        
        case AddedToCache(rddId, partition, host, size) =>
          if (size > 0) {
            slaveUsage.put(host, getCacheUsage(host) + size)
            logInfo("Cache entry added: (%s, %s) on %s (size added: %s, available: %s)".format(
              rddId, partition, host, Utils.memoryBytesToString(size),
              Utils.memoryBytesToString(getCacheAvailable(host))))
          } else {
            logInfo("Cache entry added: (%s, %s) on %s".format(rddId, partition, host))
          }
          locs(rddId)(partition) = host :: locs(rddId)(partition)
          reply('OK)
          
        case DroppedFromCache(rddId, partition, host, size) =>
          if (size > 0) {
            logInfo("Cache entry removed: (%s, %s) on %s (size dropped: %s, available: %s)".format(
              rddId, partition, host, Utils.memoryBytesToString(size),
              Utils.memoryBytesToString(getCacheAvailable(host))))
            slaveUsage.put(host, getCacheUsage(host) - size)

            // Do a sanity check to make sure usage is greater than 0.
            val usage = getCacheUsage(host)
            if (usage < 0) {
              logError("Cache usage on %s is negative (%d)".format(host, usage))
            }
          } else {
            logInfo("Cache entry removed: (%s, %s) on %s".format(rddId, partition, host))
          }
          locs(rddId)(partition) = locs(rddId)(partition).filterNot(_ == host)
          reply('OK)

        case MemoryCacheLost(host) =>
          logInfo("Memory cache lost on " + host)
          // TODO: Drop host from the memory locations list of all RDDs
        
        case GetCacheLocations =>
          logInfo("Asked for current cache locations")
          reply(locs.map{case (rrdId, array) => (rrdId -> array.clone())})

        case GetCacheStatus =>
          val status = slaveCapacity.map { case (host,capacity) =>
            (host, capacity, getCacheUsage(host))
          }.toSeq
          reply(status)

        case StopCacheTracker =>
          reply('OK)
          exit()
      }
    }
  }
}


class CacheTracker(isMaster: Boolean, theCache: Cache) extends Logging {
  // Tracker actor on the master, or remote reference to it on workers
  var trackerActor: AbstractActor = null

  val registeredRddIds = new HashSet[Int]

  // Stores map results for various splits locally
  val cache = theCache.newKeySpace()

  if (isMaster) {
  // master 上启一个cachtracker
    val tracker = new CacheTrackerActor
    tracker.start()
    trackerActor = tracker
  } else {
  // worker 要询问master
    val host = System.getProperty("spark.master.host")
    val port = System.getProperty("spark.master.port").toInt
    trackerActor = RemoteActor.select(Node(host, port), 'CacheTracker)
  }

  // Report the cache being started.
  trackerActor !? SlaveCacheStarted(Utils.getHost, cache.getCapacity)

  // Remembers which splits are currently being loaded (on worker nodes)
  val loading = new HashSet[(Int, Int)]
  
  // Registers an RDD (on master only)
  // 向master 发送rdd 的信息
  def registerRDD(rddId: Int, numPartitions: Int) {
    registeredRddIds.synchronized {
      if (!registeredRddIds.contains(rddId)) {
        logInfo("Registering RDD ID " + rddId + " with cache")
        registeredRddIds += rddId
        trackerActor !? RegisterRDD(rddId, numPartitions)
      }
    }
  }

  // Get a snapshot of the currently known locations
  // rdd_id , 一堆存储着该rdd的partitions的hosts机器
  def getLocationsSnapshot(): HashMap[Int, Array[List[String]]] = {
    (trackerActor !? GetCacheLocations) match {
      case h: HashMap[_, _] => h.asInstanceOf[HashMap[Int, Array[List[String]]]]

      case _ => throw new SparkException("Internal error: CacheTrackerActor did not reply with a HashMap")
    }
  }


  
  // Gets or computes an RDD split
  def getOrCompute[T](rdd: RDD[T], split: Split)(implicit m: ClassManifest[T]): Iterator[T] = {
    // 准备计算某个分片
    logInfo("Looking for RDD partition %d:%d".format(rdd.id, split.index))
    // 看看cache里由没有 ， 根据 rddid 和 split的index 获取这个值
    val cachedVal = cache.get(rdd.id, split.index)
    // 拿到了这个结果， 返回一个array的iterator
    if (cachedVal != null) {
      // Split is in cache, so just return its values
      logInfo("Found partition in cache!")
      return cachedVal.asInstanceOf[Array[T]].iterator
    } else {
      // Mark the split as loading (unless someone else marks it first)
      val key = (rdd.id, split.index)
      loading.synchronized {
        // 让出锁，让其他线程先loading
        while (loading.contains(key)) {
          // Someone else is loading it; let's wait for them
          try { loading.wait() } catch { case _ => }
        }
        // See whether someone else has successfully loaded it. The main way this would fail
        // is for the RDD-level cache eviction policy if someone else has loaded the same RDD
        // partition but we didn't want to make space for it. However, that case is unlikely
        // because it's unlikely that two threads would work on the same RDD partition. One
        // downside of the current code is that threads wait serially if this does happen.
        val cachedVal = cache.get(rdd.id, split.index)
        if (cachedVal != null) {
          return cachedVal.asInstanceOf[Array[T]].iterator
        }
        // Nobody's loading it and it's not in the cache; let's load it ourselves
        loading.add(key)
      }
      // If we got here, we have to load the split
      // Tell the master that we're doing so

      // TODO: fetch any remote copy of the split that may be available
      logInfo("Computing partition " + split)
      var array: Array[T] = null
      var putResponse: CachePutResponse = null
      try {
        array = rdd.compute(split).toArray(m)
        putResponse = cache.put(rdd.id, split.index, array)
      } finally {
        // Tell other threads that we've finished our attempt to load the key (whether or not
        // we've actually succeeded to put it in the map)
        loading.synchronized {
          loading.remove(key)
          loading.notifyAll()
        }
      }

      putResponse match {
      // 如果放入成功了
        case CachePutSuccess(size) => {
          // Tell the master that we added the entry. Don't return until it
          // replies so it can properly schedule future tasks that use this RDD.
          // 告诉master
          trackerActor !? AddedToCache(rdd.id, split.index, Utils.getHost, size)
        }
        case _ => null
      }
      return array.iterator
    }
  }

  // Called by the Cache to report that an entry has been dropped from it
  def dropEntry(datasetId: Any, partition: Int) {
    val (keySpaceId, innerId) = datasetId.asInstanceOf[(Any, Any)]
    if (keySpaceId == cache.keySpaceId) {
      // TODO - do we really want to use '!!' when nobody checks returned future? '!' seems to enough here.
      trackerActor !! DroppedFromCache(innerId.asInstanceOf[Int], partition, Utils.getHost)
    }
  }

  def stop() {
    trackerActor !? StopCacheTracker
    registeredRddIds.clear()
    trackerActor = null
  }
}

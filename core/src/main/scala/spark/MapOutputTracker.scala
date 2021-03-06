package spark

import java.util.concurrent.ConcurrentHashMap

import scala.actors._
import scala.actors.Actor._
import scala.actors.remote._
import scala.collection.mutable.HashSet

sealed trait MapOutputTrackerMessage
case class GetMapOutputLocations(shuffleId: Int) extends MapOutputTrackerMessage 
case object StopMapOutputTracker extends MapOutputTrackerMessage

class MapOutputTrackerActor(serverUris: ConcurrentHashMap[Int, Array[String]])
extends DaemonActor with Logging {
  def act() {
  // master的端口
    val port = System.getProperty("spark.master.port").toInt
    RemoteActor.alive(port)
    RemoteActor.register('MapOutputTracker, self)
    logInfo("Registered actor on port " + port)
    
    loop {
      react {
      // master 收到worker 来要shuffle 的map output
        case GetMapOutputLocations(shuffleId: Int) =>
          logInfo("Asked to get map output locations for shuffle " + shuffleId)
          // 返回相关的server uri
          reply(serverUris.get(shuffleId))
          
        case StopMapOutputTracker =>
          reply('OK)
          exit()
      }
    }
  }
}

class MapOutputTracker(isMaster: Boolean) extends Logging {
  var trackerActor: AbstractActor = null
  // shuffle id 和servers 的 mapping
  private var serverUris = new ConcurrentHashMap[Int, Array[String]]

  // Incremented every time a fetch fails so that client nodes know to clear
  // their cache of map output locations if this happens.
  private var generation: Long = 0
  private var generationLock = new java.lang.Object
  
  if (isMaster) {
  // master 上， 启动一个actor，用来接收 wroker的请求
    val tracker = new MapOutputTrackerActor(serverUris)
    tracker.start()
    trackerActor = tracker
  } else {
  // 在worker 上，需要和 master去通信
    val host = System.getProperty("spark.master.host")
    val port = System.getProperty("spark.master.port").toInt
    trackerActor = RemoteActor.select(Node(host, port), 'MapOutputTracker)
  }

  // 注册一下，为shuffleId 开一个数组
  def registerShuffle(shuffleId: Int, numMaps: Int) {
    if (serverUris.get(shuffleId) != null) {
      throw new IllegalArgumentException("Shuffle ID " + shuffleId + " registered twice")
    }
    serverUris.put(shuffleId, new Array[String](numMaps))
  }

  // 注册一个serverUri 给某个shuffleId
  def registerMapOutput(shuffleId: Int, mapId: Int, serverUri: String) {
    var array = serverUris.get(shuffleId)
    array.synchronized {
      array(mapId) = serverUri
    }
  }
  // 将一堆 locations 添加进来
  def registerMapOutputs(shuffleId: Int, locs: Array[String]) {
    serverUris.put(shuffleId, Array[String]() ++ locs)
  }

  // 将shuffleId 里 的 第mapId个serveruri 清空，并增加generation数
  def unregisterMapOutput(shuffleId: Int, mapId: Int, serverUri: String) {
    var array = serverUris.get(shuffleId)
    if (array != null) {
      array.synchronized {
        if (array(mapId) == serverUri) {
          array(mapId) = null
        }
      }
      incrementGeneration()
    } else {
      throw new SparkException("unregisterMapOutput called for nonexistent shuffle ID")
    }
  }
  
  // Remembers which map output locations are currently being fetched on a worker
  val fetching = new HashSet[Int]
  
  // Called on possibly remote nodes to get the server URIs for a given shuffle
  def getServerUris(shuffleId: Int): Array[String] = {
  // 获取某个shuffle 对应的servers
    val locs = serverUris.get(shuffleId)
    if (locs == null) {
      logInfo("Don't have map  outputs for " + shuffleId + ", fetching them")
      fetching.synchronized {
        // 其他人正在fetching吗？ 如果是就可以等在这里 ，一定有结果
        if (fetching.contains(shuffleId)) {
          // Someone else is fetching it; wait for them to be done
          while (fetching.contains(shuffleId)) {
            try {
            // 等着，有其他人在fetching
              fetching.wait()
            } catch {
              case _ =>
            }
          }
          // 其他人fetching 进来了
          return serverUris.get(shuffleId)
        } else {
         // 如果没有其他人fetching， 那么我来fetching
          fetching += shuffleId
        }
      }
      // We won the race to fetch the output locs; do so
      // 由我来fetching
      logInfo("Doing the fetch; tracker actor = " + trackerActor)
      // 向 master 发请求
      val fetched = (trackerActor !? GetMapOutputLocations(shuffleId)).asInstanceOf[Array[String]]
      // 拿过来保存好
      serverUris.put(shuffleId, fetched)
      fetching.synchronized {
        fetching -= shuffleId
        fetching.notifyAll()
      }
      return fetched
    } else {
      return locs
    }
  }
  
  def getMapOutputUri(serverUri: String, shuffleId: Int, mapId: Int, reduceId: Int): String = {
    "%s/shuffle/%s/%s/%s".format(serverUri, shuffleId, mapId, reduceId)
  }

  def stop() {
  // 向master 发送关闭请求
    trackerActor !? StopMapOutputTracker
    serverUris.clear()
    trackerActor = null
  }

  // Called on master to increment the generation number
  // 增加generation 1
  def incrementGeneration() {
    generationLock.synchronized {
      generation += 1
    }
  }

  // Called on master or workers to get current generation number
  def getGeneration: Long = {
    generationLock.synchronized {
      return generation
    }
  }

  // Called on workers to update the generation number, potentially clearing old outputs
  // because of a fetch failure. (Each Mesos task calls this with the latest generation
  // number on the master at the time it was created.)
  // workers 清理原来的 shuffleId 的 serveruri cache
  def updateGeneration(newGen: Long) {
    generationLock.synchronized {
      if (newGen > generation) {
        logInfo("Updating generation to " + newGen + " and clearing cache")
        serverUris = new ConcurrentHashMap[Int, Array[String]]
        generation = newGen
      }
    }
  }
}

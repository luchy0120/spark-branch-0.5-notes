package spark

import java.io.EOFException
import java.net.URL

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import it.unimi.dsi.fastutil.io.FastBufferedInputStream

class SimpleShuffleFetcher extends ShuffleFetcher with Logging {

    // reduce id 为分片id
  def fetch[K, V](shuffleId: Int, reduceId: Int, func: (K, V) => Unit) {
    // 一次shuffle
    logInfo("Fetching outputs for shuffle %d, reduce %d".format(shuffleId, reduceId))
    // 序列化器
    val ser = SparkEnv.get.serializer.newInstance()
    val splitsByUri = new HashMap[String, ArrayBuffer[Int]]
    // 每个server的uri
    val serverUris = SparkEnv.get.mapOutputTracker.getServerUris(shuffleId)
    // 相同uri的放在一起， uri -> 1，2，5
    for ((serverUri, index) <- serverUris.zipWithIndex) {
      splitsByUri.getOrElseUpdate(serverUri, ArrayBuffer()) += index
    }
    for ((serverUri, inputIds) <- Utils.randomize(splitsByUri)) {
        // 第i个
      for (i <- inputIds) {
        // reduce Id 是分片id
        val url = "%s/shuffle/%d/%d/%d".format(serverUri, shuffleId, i, reduceId)
        var totalRecords = -1
        var recordsProcessed = 0
        var tries = 0
        while (totalRecords == -1 || recordsProcessed < totalRecords) {
          tries += 1
          // 拿4次
          if (tries > 4) {
            // We've tried four times to get this data but we've had trouble; let's just declare
            // a failed fetch
            logError("Failed to fetch " + url + " four times; giving up")
            throw new FetchFailedException(serverUri, shuffleId, i, reduceId, null)
          }
          var recordsRead = 0
          try {
            // 反序列化流
            val inputStream = ser.inputStream(
                new FastBufferedInputStream(new URL(url).openStream()))
            try {
             // 一共多少条记录
              totalRecords = inputStream.readObject().asInstanceOf[Int]
              logDebug("Total records to read from " + url + ": " + totalRecords)
              while (true) {
                // 每次读入一个pair
                val pair = inputStream.readObject().asInstanceOf[(K, V)]
                if (recordsRead <= recordsProcessed) {
                  // 对每一个k v 做处理
                  func(pair._1, pair._2)
                  // 处理了一条
                  recordsProcessed += 1
                }
                // 读入一条
                recordsRead += 1
              }
            } finally {
              inputStream.close()
            }
          } catch {
            case e: EOFException => {
              logDebug("Reduce %s got %s records from map %s before EOF".format(
                reduceId, recordsRead, i))
              if (recordsRead < totalRecords) {
                logInfo("Reduce %s only got %s/%s records from map %s before EOF; retrying".format(
                  reduceId, recordsRead, totalRecords, i))
              }
            }
            case other: Exception => {
              logError("Fetch failed", other)
              throw new FetchFailedException(serverUri, shuffleId, i, reduceId, other)
            }
          }
        }
        logInfo("Fetched all " + totalRecords + " records successfully")
      }
    }
  }
}

package spark

import java.io.ByteArrayInputStream
import java.io.EOFException
import java.net.URL
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import it.unimi.dsi.fastutil.io.FastBufferedInputStream


class ParallelShuffleFetcher extends ShuffleFetcher with Logging {
    // 同时拿3个
  val parallelFetches = System.getProperty("spark.parallel.fetches", "3").toInt

  def fetch[K, V](shuffleId: Int, reduceId: Int, func: (K, V) => Unit) {
    // 拿某个shuffleId的数据
    logInfo("Fetching outputs for shuffle %d, reduce %d".format(shuffleId, reduceId))
    
    // Figure out a list of input IDs (mapper IDs) for each server
    // spark的序列化器
    val ser = SparkEnv.get.serializer.newInstance()

    val inputsByUri = new HashMap[String, ArrayBuffer[Int]]
    val serverUris = SparkEnv.get.mapOutputTracker.getServerUris(shuffleId)
    for ((serverUri, index) <- serverUris.zipWithIndex) {
        // 把 uri 相同的放在一个list里，记录下他们的编号
      inputsByUri.getOrElseUpdate(serverUri, ArrayBuffer()) += index
    }
    
    // Randomize them and put them in a LinkedBlockingQueue
    // 把他们放到一个queue里，作为任务
    val serverQueue = new LinkedBlockingQueue[(String, ArrayBuffer[Int])]
    for (pair <- Utils.randomize(inputsByUri)) {
      serverQueue.put(pair)
    }

    // Create a queue to hold the fetched data
    // 所有获得的数据都放在queue里
    val resultQueue = new LinkedBlockingQueue[Array[Byte]]

    // Atomic variables to communicate failures and # of fetches done
    var failure = new AtomicReference[FetchFailedException](null)

    // Start multiple threads to do the fetching (TODO: may be possible to do it asynchronously)
    // 并发fetch
    for (i <- 0 until parallelFetches) {
      new Thread("Fetch thread " + i + " for reduce " + reduceId) {
        override def run() {
          while (true) {
            val pair = serverQueue.poll()
            if (pair == null)
              return
            val (serverUri, inputIds) = pair
            //logInfo("Pulled out server URI " + serverUri)
            // 同一个server uri 的每一个inputId 都需要读取数据
            for (i <- inputIds) {
              if (failure.get != null)
                return
              // i 是inPutId
              val url = "%s/shuffle/%d/%d/%d".format(serverUri, shuffleId, i, reduceId)
              // 发请求
              logInfo("Starting HTTP request for " + url)
              try {
                val conn = new URL(url).openConnection()
                conn.connect()
                // 先拿 长度
                val len = conn.getContentLength()
                if (len == -1) {
                  throw new SparkException("Content length was not specified by server")
                }
                val buf = new Array[Byte](len)
                val in = new FastBufferedInputStream(conn.getInputStream())
                var pos = 0
                while (pos < len) {
                  // 读取len-pos 个，实际读取n个
                  val n = in.read(buf, pos, len-pos)
                  if (n == -1) {
                    throw new SparkException("EOF before reading the expected " + len + " bytes")
                  } else {
                    pos += n
                  }
                }
                // Done reading everything
                // 读完了就放入结果queue里
                resultQueue.put(buf)
                in.close()
              } catch {
                case e: Exception =>
                  logError("Fetch failed from " + url, e)
                  failure.set(new FetchFailedException(serverUri, shuffleId, i, reduceId, e))
                  return
              }
            }
            //logInfo("Done with server URI " + serverUri)
          }
        }
      }.start()
    }

    // Wait for results from the threads (either a failure or all servers done)
    var resultsDone = 0
    var totalResults = inputsByUri.map{case (uri, inputs) => inputs.size}.sum
    while (failure.get == null && resultsDone < totalResults) {
      try {

        // 等结果
        val result = resultQueue.poll(100, TimeUnit.MILLISECONDS)
        if (result != null) {
          //logInfo("Pulled out a result")
          val in = ser.inputStream(new ByteArrayInputStream(result))
            try {
            while (true) {
              val pair = in.readObject().asInstanceOf[(K, V)]
              func(pair._1, pair._2)
            }
          } catch {
            case e: EOFException => {} // TODO: cleaner way to detect EOF, such as a sentinel
          }
          resultsDone += 1
          //logInfo("Results done = " + resultsDone)
        }
      } catch { case e: InterruptedException => {} }
    }
    if (failure.get != null) {
      throw failure.get
    }
  }
}

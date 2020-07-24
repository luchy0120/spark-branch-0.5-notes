package spark

import java.io._
import java.net.InetAddress
import java.util.concurrent.{Executors, ThreadFactory, ThreadPoolExecutor}

import scala.collection.mutable.ArrayBuffer
import java.util.{Locale, UUID, Random}

/**
 * Various utility methods used by Spark.
 */
object Utils {
    // 将对象变为byte数组
  def serialize[T](o: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close
    return bos.toByteArray
  }

  // 从byte数组中恢复出对象
  def deserialize[T](bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    return ois.readObject.asInstanceOf[T]
  }

  //
  def deserialize[T](bytes: Array[Byte], loader: ClassLoader): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis) {
      override def resolveClass(desc: ObjectStreamClass) =
        Class.forName(desc.getName, false, loader)
    }
    return ois.readObject.asInstanceOf[T]
  }

  // 是否是字母
  def isAlpha(c: Char): Boolean = {
    (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
  }

  // 空格分开的 String
  def splitWords(s: String): Seq[String] = {
    val buf = new ArrayBuffer[String]
    var i = 0
    while (i < s.length) {
      var j = i
      while (j < s.length && isAlpha(s.charAt(j))) {
        j += 1
      }
      if (j > i) {
        buf += s.substring(i, j);
      }
      i = j
      while (i < s.length && !isAlpha(s.charAt(i))) {
        i += 1
      }
    }
    return buf
  }

  // Create a temporary directory inside the given parent directory
  // 再tmp 文件夹下新建文件夹spark-131234
  def createTempDir(root: String = System.getProperty("java.io.tmpdir")): File = {
    var attempts = 0
    val maxAttempts = 10
    var dir: File = null
    // 最多10次
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory after " + maxAttempts + 
            " attempts!")
      }
      try {
        dir = new File(root, "spark-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch { case e: IOException => ; }
    }
    // Add a shutdown hook to delete the temp dir when the JVM exits
    Runtime.getRuntime.addShutdownHook(new Thread("delete Spark temp dir " + dir) {
      override def run() {
        // 删除掉那个dir
        Utils.deleteRecursively(dir)
      }
    })
    return dir
  }

  // Copy all data from an InputStream to an OutputStream
  // 从 输入流到输出流， 通过 8192 byte 数组copy过去
  def copyStream(in: InputStream,
                 out: OutputStream,
                 closeStreams: Boolean = false)
  {
    val buf = new Array[Byte](8192)
    var n = 0
    while (n != -1) {
      n = in.read(buf)
      if (n != -1) {
        out.write(buf, 0, n)
      }
    }
    if (closeStreams) {
      in.close()
      out.close()
    }
  }

  /**
   * Shuffle the elements of a collection into a random order, returning the
   * result in a new collection. Unlike scala.util.Random.shuffle, this method
   * uses a local random number generator, avoiding inter-thread contention.
   */
  def randomize[T: ClassManifest](seq: TraversableOnce[T]): Seq[T] = {
    randomizeInPlace(seq.toArray)
  }

  /**
   * Shuffle the elements of an array into a random order, modifying the
   * original array. Returns the original array.
   */
   // 将数组打乱
  def randomizeInPlace[T](arr: Array[T], rand: Random = new Random): Array[T] = {
    for (i <- (arr.length - 1) to 1 by -1) {
      val j = rand.nextInt(i)
      val tmp = arr(j)
      arr(j) = arr(i)
      arr(i) = tmp
    }
    arr
  }

  /**
   * Get the local host's IP address in dotted-quad format (e.g. 1.2.3.4).
   */
   // 本地ip
  def localIpAddress(): String = InetAddress.getLocalHost.getHostAddress
  
  /**
   * Returns a standard ThreadFactory except all threads are daemons.
   */
  private def newDaemonThreadFactory: ThreadFactory = {
    new ThreadFactory {
      def newThread(r: Runnable): Thread = {
        var t = Executors.defaultThreadFactory.newThread (r)
        // daemon线程
        t.setDaemon (true)
        return t
      }
    }
  }

  /**
   * Wrapper over newCachedThreadPool.
   */
  def newDaemonCachedThreadPool(): ThreadPoolExecutor = {
    var threadPool = Executors.newCachedThreadPool.asInstanceOf[ThreadPoolExecutor]

    threadPool.setThreadFactory (newDaemonThreadFactory)

    return threadPool
  }

  /**
   * Wrapper over newFixedThreadPool.
   */
  def newDaemonFixedThreadPool(nThreads: Int): ThreadPoolExecutor = {
    var threadPool = Executors.newFixedThreadPool(nThreads).asInstanceOf[ThreadPoolExecutor]

    threadPool.setThreadFactory(newDaemonThreadFactory)

    return threadPool
  }

  /**
   * Get the local machine's hostname.
   */
  def localHostName(): String = InetAddress.getLocalHost.getHostName

  /**
   * Get current host
   */
   // 本机名称
  def getHost = System.getProperty("spark.hostname", localHostName())

  /**
   * Delete a file or directory and its contents recursively.
   */
  def deleteRecursively(file: File) {
    // file 是文件夹
    if (file.isDirectory) {
      // 删除每个文件
      for (child <- file.listFiles()) {
        deleteRecursively(child)
      }
    }
    if (!file.delete()) {
      throw new IOException("Failed to delete: " + file)
    }
  }

  /**
   * Use unit suffixes (Byte, Kilobyte, Megabyte, Gigabyte, Terabyte and
   * Petabyte) in order to reduce the number of digits to four or less. For
   * example, 4,000,000 is returned as 4MB.
   */
   // Bytes 的String表达
  def memoryBytesToString(size: Long): String = {
    // 多少Byte
    val GB = 1L << 30
    val MB = 1L << 20
    // 1<< 10 是1024
    val KB = 1L << 10

    val (value, unit) = {
      if (size >= 2*GB) {
        (size.asInstanceOf[Double] / GB, "GB")
      } else if (size >= 2*MB) {
        (size.asInstanceOf[Double] / MB, "MB")
      } else if (size >= 2*KB) {
        (size.asInstanceOf[Double] / KB, "KB")
      } else {
        (size.asInstanceOf[Double], "B")
      }
    }
    "%.1f%s".formatLocal(Locale.US, value, unit)
  }
}

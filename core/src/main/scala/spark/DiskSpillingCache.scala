package spark

import java.io.File
import java.io.{FileOutputStream,FileInputStream}
import java.io.IOException
import java.util.LinkedHashMap
import java.util.UUID

// TODO: cache into a separate directory using Utils.createTempDir
// TODO: clean up disk cache afterwards
class DiskSpillingCache extends BoundedMemoryCache {

// 使用diskMap 来存储 (datasetId, partition), file
  private val diskMap = new LinkedHashMap[(Any, Int), File](32, 0.75f, true)

  override def get(datasetId: Any, partition: Int): Any = {
    synchronized {
      val ser = SparkEnv.get.serializer.newInstance()
      super.get(datasetId, partition) match {
        case bytes: Any => // found in memory
          ser.deserialize(bytes.asInstanceOf[Array[Byte]])

        case _ => diskMap.get((datasetId, partition)) match {
          // 找到了某个文件
          case file: Any => // found on disk
            try {
              // 开始时间
              val startTime = System.currentTimeMillis
              // 根据文件长度创建bytes 数组
              val bytes = new Array[Byte](file.length.toInt)
              // 读入bytes
              new FileInputStream(file).read(bytes)
              // 读取了多久
              val timeTaken = System.currentTimeMillis - startTime
              logInfo("Reading key (%s, %d) of size %d bytes from disk took %d ms".format(
                datasetId, partition, file.length, timeTaken))
               // 再次放入内存中
              super.put(datasetId, partition, bytes)
              // 反序列化出对象来
              ser.deserialize(bytes.asInstanceOf[Array[Byte]])
            } catch {
              case e: IOException =>
                logWarning("Failed to read key (%s, %d) from disk at %s: %s".format(
                  datasetId, partition, file.getPath(), e.getMessage()))
                diskMap.remove((datasetId, partition)) // remove dead entry
                null
            }

          case _ => // not found
            null
        }
      }
    }
  }

  override def put(datasetId: Any, partition: Int, value: Any): CachePutResponse = {
    var ser = SparkEnv.get.serializer.newInstance()
    // rdd 加partiion 作为key ， 数据序列化为bytes 保存在内存中
    super.put(datasetId, partition, ser.serialize(value))
  }

  /**
   * Spill the given entry to disk. Assumes that a lock is held on the
   * DiskSpillingCache.  Assumes that entry.value is a byte array.
   */
   // 覆盖了父类的reportEntryDropped ， 这个方法不同于父类，他会把元素往磁盘上写一份，并记录下
   // 写的文件的名字
  override protected def reportEntryDropped(datasetId: Any, partition: Int, entry: Entry) {
  // 将一个value 放到文件里
    logInfo("Spilling key (%s, %d) of size %d to make space".format(
      datasetId, partition, entry.size))
      // 临时目录里
    val cacheDir = System.getProperty(
      "spark.diskSpillingCache.cacheDir",
      System.getProperty("java.io.tmpdir"))
      // 随机生成的id
    val file = new File(cacheDir, "spark-dsc-" + UUID.randomUUID.toString)
    try {
    // 写出文件的流
      val stream = new FileOutputStream(file)
      // 将entry 的数据 写出
      stream.write(entry.value.asInstanceOf[Array[Byte]])
      stream.close()
      // 在内存中保留 数据的文件句柄
      diskMap.put((datasetId, partition), file)
    } catch {
      case e: IOException =>
        logWarning("Failed to spill key (%s, %d) to disk at %s: %s".format(
          datasetId, partition, file.getPath(), e.getMessage()))
        // Do nothing and let the entry be discarded
    }
  }
}

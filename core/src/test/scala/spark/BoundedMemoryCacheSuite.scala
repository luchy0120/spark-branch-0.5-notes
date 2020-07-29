package spark

import org.scalatest.FunSuite
import org.scalatest.PrivateMethodTester
import org.scalatest.matchers.ShouldMatchers

class BoundedMemoryCacheSuite extends FunSuite with PrivateMethodTester with ShouldMatchers {
  test("constructor test") {
    val cache = new BoundedMemoryCache(60)
    expect(60)(cache.getCapacity)
  }

  test("caching") {
    // Set the arch to 64-bit and compressedOops to true to get a deterministic test-case 
    val oldArch = System.setProperty("os.arch", "amd64")
    val oldOops = System.setProperty("spark.test.useCompressedOops", "true")
    val initialize = PrivateMethod[Unit]('initialize)
    SizeEstimator invokePrivate initialize()

    val cache = new BoundedMemoryCache(60) {
      //TODO sorry about this, but there is not better way how to skip 'cacheTracker.dropEntry'
      override protected def reportEntryDropped(datasetId: Any, partition: Int, entry: Entry) {
        logInfo("Dropping key (%s, %d) of size %d to make space".format(datasetId, partition, entry.size))
      }
    }

    // NOTE: The String class definition changed in JDK 7 to exclude the int fields count and length
    // This means that the size of strings will be lesser by 8 bytes in JDK 7 compared to JDK 6.
    // http://mail.openjdk.java.net/pipermail/core-libs-dev/2012-May/010257.html
    // Work around to check for either.

    //should be OK
    // 两个值都可以，jdk 7 和 jdk 6 返回值不同
    cache.put("1", 0, "Meh") should (equal (CachePutSuccess(56)) or equal (CachePutSuccess(48)))

    //we cannot add this to cache (there is not enough space in cache) & we cannot evict the only value from
    //cache because it's from the same dataset
    // 同一个 rddId 所以不能 evict
    expect(CachePutFailure())(cache.put("1", 1, "Meh"))

    //should be OK, dataset '1' can be evicted from cache
    // 把 rddId 1 的数据给换出去
    cache.put("2", 0, "Meh") should (equal (CachePutSuccess(56)) or equal (CachePutSuccess(48)))

    //should fail, cache should obey it's capacity
    // string太大了，比cache 容量还大
    expect(CachePutFailure())(cache.put("3", 0, "Very_long_and_useless_string"))

    // 把配置换回去
    if (oldArch != null) {
      System.setProperty("os.arch", oldArch)
    } else {
      System.clearProperty("os.arch")
    }

    if (oldOops != null) {
      System.setProperty("spark.test.useCompressedOops", oldOops)
    } else {
      System.clearProperty("spark.test.useCompressedOops")
    }
  }
}

package spark

// cache 序列器

class SparkEnv (
  val cache: Cache,
  val serializer: Serializer,
  val closureSerializer: Serializer,
  val cacheTracker: CacheTracker,
  val mapOutputTracker: MapOutputTracker,
  val shuffleFetcher: ShuffleFetcher,
  val shuffleManager: ShuffleManager
)

object SparkEnv {
  private val env = new ThreadLocal[SparkEnv]

  def set(e: SparkEnv) {
    env.set(e)
  }

  def get: SparkEnv = {
    env.get()
  }

  def createFromSystemProperties(isMaster: Boolean): SparkEnv = {
     // 内存限制的cache
    val cacheClass = System.getProperty("spark.cache.class", "spark.BoundedMemoryCache")
     // cache对象
    val cache = Class.forName(cacheClass).newInstance().asInstanceOf[Cache]
     // java的序列器
    val serializerClass = System.getProperty("spark.serializer", "spark.JavaSerializer")

    val serializer = Class.forName(serializerClass, true, Thread.currentThread.getContextClassLoader).newInstance().asInstanceOf[Serializer]
       // 闭包的序列器
    val closureSerializerClass =
      System.getProperty("spark.closure.serializer", "spark.JavaSerializer")
    val closureSerializer =
      Class.forName(closureSerializerClass).newInstance().asInstanceOf[Serializer]
     // cache 记录器
    val cacheTracker = new CacheTracker(isMaster, cache)

    val mapOutputTracker = new MapOutputTracker(isMaster)
    // shuffle 文件获取器，使用simple shuffle fetcher
    val shuffleFetcherClass = 
      System.getProperty("spark.shuffle.fetcher", "spark.SimpleShuffleFetcher")
    val shuffleFetcher = 
      Class.forName(shuffleFetcherClass).newInstance().asInstanceOf[ShuffleFetcher]

    val shuffleMgr = new ShuffleManager()

    new SparkEnv(
      cache,
      serializer,
      closureSerializer,
      cacheTracker,
      mapOutputTracker,
      shuffleFetcher,
      shuffleMgr)
  }
}

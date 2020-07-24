package spark

abstract class ShuffleFetcher {
  // Fetch the shuffle outputs for a given ShuffleDependency, calling func exactly
  // once on each key-value pair obtained.
  // 把拿过来的output key value 执行一遍 func
  def fetch[K, V](shuffleId: Int, reduceId: Int, func: (K, V) => Unit)

  // Stop the fetcher
  def stop() {}
}

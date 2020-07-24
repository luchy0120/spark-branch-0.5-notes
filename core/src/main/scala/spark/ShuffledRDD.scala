package spark

import java.util.{HashMap => JHashMap}

class ShuffledRDDSplit(val idx: Int) extends Split {
  override val index = idx
  override def hashCode(): Int = idx
}

class ShuffledRDD[K, V, C](
    parent: RDD[(K, V)],
    aggregator: Aggregator[K, V, C],
    part : Partitioner) 
  extends RDD[(K, C)](parent.context) {
  //override val partitioner = Some(part)
  override val partitioner = Some(part)

  // i从0开始，一直到length - 1
  @transient
  val splits_ = Array.tabulate[Split](part.numPartitions)(i => new ShuffledRDDSplit(i))

  override def splits = splits_
  
  override def preferredLocations(split: Split) = Nil

  // 需要分配给这个dependency 一个shuffleId
  val dep = new ShuffleDependency(context.newShuffleId, parent, aggregator, part)
  override val dependencies = List(dep)

  override def compute(split: Split): Iterator[(K, C)] = {
    val combiners = new JHashMap[K, C]
    // 把同一个key的所有value 合起来，形成list of values
    def mergePair(k: K, c: C) {
    // 原来没有值就放入
      val oldC = combiners.get(k)
      if (oldC == null) {
        combiners.put(k, c)
      } else {
        combiners.put(k, aggregator.mergeCombiners(oldC, c))
      }
    }
    // 计算当前分片时需要从远处获取数据，再通过mergePair把 相同key 的 values 合并起来
    // 这两个key values 来自于不同的 partition
    val fetcher = SparkEnv.get.shuffleFetcher

    fetcher.fetch[K, C](dep.shuffleId, split.index, mergePair)

    // 访问每个key 和 value list
    return new Iterator[(K, C)] {
      var iter = combiners.entrySet().iterator()

      def hasNext(): Boolean = iter.hasNext()

      def next(): (K, C) = {
        val entry = iter.next()
        (entry.getKey, entry.getValue)
      }
    }
  }
}

package spark

// parents 父级依赖们
class Stage(
    val id: Int,
    val rdd: RDD[_],
    val shuffleDep: Option[ShuffleDependency[_,_,_]],
    val parents: List[Stage]) {
  
  val isShuffleMap = shuffleDep != None
  // 当前的rdd的分片个数
  val numPartitions = rdd.splits.size

  val outputLocs = Array.fill[List[String]](numPartitions)(Nil)
  var numAvailableOutputs = 0

  def isAvailable: Boolean = {
    // 是final stage 吗？
    if (parents.size == 0 && !isShuffleMap) {
      true
    } else {
    // 每个partition都有数据了，说明父rdd 把所有数据都跑出来了
      numAvailableOutputs == numPartitions
    }
  }

  // 增加某个partition的host
  def addOutputLoc(partition: Int, host: String) {
    val prevList = outputLocs(partition)
    outputLocs(partition) = host :: prevList
    if (prevList == Nil)
      numAvailableOutputs += 1
  }

  def removeOutputLoc(partition: Int, host: String) {
    val prevList = outputLocs(partition)
    val newList = prevList.filterNot(_ == host)
    outputLocs(partition) = newList
    if (prevList != Nil && newList == Nil) {
    // 变为空了，说明这个partition没有location 了
      numAvailableOutputs -= 1
    }
  }

  override def toString = "Stage " + id

  override def hashCode(): Int = id
}

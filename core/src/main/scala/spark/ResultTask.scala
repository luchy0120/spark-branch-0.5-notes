package spark

class ResultTask[T, U](
    runId: Int,
    stageId: Int, 
    rdd: RDD[T], 
    func: (TaskContext, Iterator[T]) => U,
    val partition: Int, 
    locs: Seq[String],
    val outputId: Int)
  extends DAGTask[U](runId, stageId) {

  // 获得那个分片
  val split = rdd.splits(partition)

  override def run(attemptId: Int): U = {
    val context = new TaskContext(stageId, partition, attemptId)
    // 运行那个分片的数据，最后运行 final rdd 的func
    func(context, rdd.iterator(split))
  }

  override def preferredLocations: Seq[String] = locs

  override def toString = "ResultTask(" + stageId + ", " + partition + ")"
}

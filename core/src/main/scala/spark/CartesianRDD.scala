package spark

class CartesianSplit(idx: Int, val s1: Split, val s2: Split) extends Split with Serializable {
  override val index = idx
}

class CartesianRDD[T: ClassManifest, U:ClassManifest](
    sc: SparkContext,
    rdd1: RDD[T],
    rdd2: RDD[U])
  extends RDD[Pair[T, U]](sc)
  with Serializable {

  // 第二个rdd的split的大小
  val numSplitsInRdd2 = rdd2.splits.size
  
  @transient
  val splits_ = {
    // create the cross product split
    // 叉积split，乘起来
    val array = new Array[Split](rdd1.splits.size * rdd2.splits.size)
    for (s1 <- rdd1.splits; s2 <- rdd2.splits) {
      // id生成规则为 s1 里的index编号乘以 rdd2里的split的个数 再加上 s2的编号
      val idx = s1.index * numSplitsInRdd2 + s2.index
      // 通过 s1 和 s2 两个分片生成一个新的split
      array(idx) = new CartesianSplit(idx, s1, s2)
    }
    array
  }

  override def splits = splits_.asInstanceOf[Array[Split]]

  override def preferredLocations(split: Split) = {
    val currSplit = split.asInstanceOf[CartesianSplit]
    rdd1.preferredLocations(currSplit.s1) ++ rdd2.preferredLocations(currSplit.s2)
  }

  override def compute(split: Split) = {
    val currSplit = split.asInstanceOf[CartesianSplit]
    // 分别遍历第一个rdd1中的分片 和rdd2中的分片，形成叉积
    for (x <- rdd1.iterator(currSplit.s1); y <- rdd2.iterator(currSplit.s2)) yield (x, y)
  }
  // 一个partition来自两个rdd的partition
  override val dependencies = List(
    new NarrowDependency(rdd1) {
      def getParents(id: Int): Seq[Int] = List(id / numSplitsInRdd2)
    },
    new NarrowDependency(rdd2) {
      def getParents(id: Int): Seq[Int] = List(id % numSplitsInRdd2)
    }
  )
}
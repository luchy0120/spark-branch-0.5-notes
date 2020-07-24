package spark

import scala.collection.mutable.ArrayBuffer

class UnionSplit[T: ClassManifest](
    idx: Int, 
    rdd: RDD[T],
    split: Split)
  extends Split
  with Serializable {
  
  def iterator() = rdd.iterator(split)
  def preferredLocations() = rdd.preferredLocations(split)
  override val index = idx
}

class UnionRDD[T: ClassManifest](
    sc: SparkContext,
    @transient rdds: Seq[RDD[T]])
  extends RDD[T](sc)
  with Serializable {
  
  @transient
  val splits_ : Array[Split] = {
  // 一共有多少个父亲分片，把每个父亲rdd的分片数目加起来
    val array = new Array[Split](rdds.map(_.splits.size).sum)
    var pos = 0
    // 遍历每个rdd的每个split
    for (rdd <- rdds; split <- rdd.splits) {
      // 把所有父亲 分片都拿过来
      // 放入数组，这个分片任然由父rdd 来掌管它的locations 和 遍历方式
      array(pos) = new UnionSplit(pos, rdd, split)
      pos += 1
    }
    array
  }

  override def splits = splits_

  // union RDD 的dependencies 有多个，dependency 描述了Rdd与Rdd之间的一种数据转移关系
  //
  @transient override val dependencies = {
    val deps = new ArrayBuffer[Dependency[_]]
    var pos = 0
    for ((rdd, index) <- rdds.zipWithIndex) {
    // 0是instart 的位置 ，每一个父亲的分片都是从0开始的，pos 是 outstart的位置，是子rdd的起始位置
    // 映射关系，把某个父rdd 的一堆分片 映射到 当前 子rdd 的某个分片区域
      deps += new RangeDependency(rdd, 0, pos, rdd.splits.size) 
      pos += rdd.splits.size
    }
    deps.toList
  }
  
  override def compute(s: Split): Iterator[T] = s.asInstanceOf[UnionSplit[T]].iterator()

  override def preferredLocations(s: Split): Seq[String] =
    s.asInstanceOf[UnionSplit[T]].preferredLocations()
}

package spark.examples

import java.lang.System
import scala.math.random
import spark._
import SparkContext._

object SparkPi {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: SparkPi <host> [<slices>]")
      System.exit(1)
    }
    // 新建context 对象
    val spark = new SparkContext(args(0), "SparkPi",  System.getenv("SPARK_HOME"), List(System.getenv("SPARK_EXAMPLES_JAR")))
    // 分片数量，默认为2
    val slices = if (args.length > 1) args(1).toInt else 2
    // 总数量，每个分片有100000个值
    val n = 100000 * slices
    // 将 1 到 n 平均分为 slice 份
    val count = spark.parallelize(1 to n, slices).map { i =>
    // 随机一个-0.5 到 0.5 的数
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    // 把总个数统计出来
    println("Pi is roughly " + 4.0 * count / n)
    System.exit(0)
  }
}

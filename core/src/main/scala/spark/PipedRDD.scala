package spark

import java.io.PrintWriter
import java.util.StringTokenizer

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * An RDD that pipes the contents of each parent partition through an external command
 * (printing them one per line) and returns the output as a collection of strings.
 */
 // 外部的command 是一个 字符串数组
class PipedRDD[T: ClassManifest](
  parent: RDD[T], command: Seq[String], envVars: Map[String, String])
  extends RDD[String](parent.context) {

  def this(parent: RDD[T], command: Seq[String]) = this(parent, command, Map())

  // Similar to Runtime.exec(), if we are given a single string, split it into words
  // using a standard StringTokenizer (i.e. by spaces)
  def this(parent: RDD[T], command: String) = this(parent, PipedRDD.tokenize(command))

  // 父亲 splits
  override def splits = parent.splits

  override val dependencies = List(new OneToOneDependency(parent))

  override def compute(split: Split): Iterator[String] = {
  // 新的进程
    val pb = new ProcessBuilder(command)
    // Add the environmental variables to the process.
    val currentEnvVars = pb.environment()
    // 添加环境变量
    envVars.foreach { case(variable, value) => currentEnvVars.put(variable, value) }
    // 进程开始
    val proc = pb.start()
    // 得到环境
    val env = SparkEnv.get

    // Start a thread to print the process's stderr to ours
    new Thread("stderr reader for " + command) {
      override def run() {
        for(line <- Source.fromInputStream(proc.getErrorStream).getLines) {
        // 打印错误输出流
          System.err.println(line)
        }
      }
    }.start()

    // Start a thread to feed the process input from our parent's iterator
    // 把parent的元素一个一个传递给process
    new Thread("stdin writer for " + command) {
      override def run() {
        SparkEnv.set(env)
        val out = new PrintWriter(proc.getOutputStream)
        for(elem <- parent.iterator(split)) {
          out.println(elem)
        }
        out.close()
      }
    }.start()

    // Return an iterator that read lines from the process's stdout
    // 获取 外部命令处理完了后的结果
    Source.fromInputStream(proc.getInputStream).getLines
  }
}

object PipedRDD {
  // Split a string into words using a standard StringTokenizer
  def tokenize(command: String): Seq[String] = {
    val buf = new ArrayBuffer[String]
    val tok = new StringTokenizer(command)
    // 把一个string 分割成token
    while(tok.hasMoreElements)
      buf += tok.nextToken()
    buf
  }
}

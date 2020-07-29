package spark.bagel

import org.scalatest.{FunSuite, Assertions, BeforeAndAfter}
import org.scalatest.prop.Checkers
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen
import org.scalacheck.Prop._

import scala.collection.mutable.ArrayBuffer

import spark._

class TestVertex(val active: Boolean, val age: Int) extends Vertex with Serializable
class TestMessage(val targetId: String) extends Message[String] with Serializable

class BagelSuite extends FunSuite with Assertions with BeforeAndAfter {
  
  var sc: SparkContext = _
  
  after {
    sc.stop()
  }
  
  test("halting by voting") {
    sc = new SparkContext("local", "test")
    val verts = sc.parallelize(Array("a", "b", "c", "d").map(id => (id, new TestVertex(true, 0))))
    val msgs = sc.parallelize(Array[(String, TestMessage)]())
    val numSupersteps = 5
    val result =
      Bagel.run(sc, verts, msgs, sc.defaultParallelism) {
        (self: TestVertex, msgs: Option[Array[TestMessage]], superstep: Int) =>
          (new TestVertex(superstep < numSupersteps - 1, self.age + 1), Array[TestMessage]())
      }
    for ((id, vert) <- result.collect)
      assert(vert.age === numSupersteps)
  }

  test("halting by message silence") {
    sc = new SparkContext("local", "test")
    val verts = sc.parallelize(Array("a", "b", "c", "d").map(id => (id, new TestVertex(false, 0))))
    val msgs = sc.parallelize(Array("a" -> new TestMessage("a")))
    val numSupersteps = 5
    val result =
      Bagel.run(sc, verts, msgs, sc.defaultParallelism) {
        (self: TestVertex, msgs: Option[Array[TestMessage]], superstep: Int) =>
          val msgsOut =
            msgs match {
              case Some(ms) if (superstep < numSupersteps - 1) =>
                ms
              case _ =>
                Array[TestMessage]()
            }
        (new TestVertex(self.active, self.age + 1), msgsOut)
      }
    for ((id, vert) <- result.collect)
      assert(vert.age === numSupersteps)
  }
}

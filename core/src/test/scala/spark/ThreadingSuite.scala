package spark

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import SparkContext._

/**
 * Holds state shared across task threads in some ThreadingSuite tests.
 */
object ThreadingSuiteState {
  val runningThreads = new AtomicInteger
  val failed = new AtomicBoolean

  def clear() {
    runningThreads.set(0)
    failed.set(false)
  }
}

class ThreadingSuite extends FunSuite with BeforeAndAfter {
  
  var sc: SparkContext = _
  
  after {
    if(sc != null) {
      sc.stop()
    }
  }
  
  // 在另一个线程里跑 reduce
  test("accessing SparkContext form a different thread") {
    sc = new SparkContext("local", "test")
    val nums = sc.parallelize(1 to 10, 2)
    val sem = new Semaphore(0)
    @volatile var answer1: Int = 0
    @volatile var answer2: Int = 0
    new Thread {
      override def run() {
        answer1 = nums.reduce(_ + _)
        answer2 = nums.first    // This will run "locally" in the current thread
        sem.release()
      }
    }.start()
    sem.acquire()
    assert(answer1 === 55)
    assert(answer2 === 1)
  }

  // 10 个 thread 同时算
  test("accessing SparkContext form multiple threads") {
    sc = new SparkContext("local", "test")
    val nums = sc.parallelize(1 to 10, 2)
    val sem = new Semaphore(0)
    @volatile var ok = true
    for (i <- 0 until 10) {
      new Thread {
        override def run() {
          val answer1 = nums.reduce(_ + _)
          if (answer1 != 55) {
            printf("In thread %d: answer1 was %d\n", i, answer1);
            ok = false;
          }
          val answer2 = nums.first    // This will run "locally" in the current thread
          if (answer2 != 1) {
            printf("In thread %d: answer2 was %d\n", i, answer2);
            ok = false;
          }
          sem.release()
        }
      }.start()
    }
    sem.acquire(10)
    if (!ok) {
      fail("One or more threads got the wrong answer from an RDD operation")
    }
  }

  test("accessing multi-threaded SparkContext form multiple threads") {
    sc = new SparkContext("local[4]", "test")
    val nums = sc.parallelize(1 to 10, 2)
    val sem = new Semaphore(0)
    @volatile var ok = true
    for (i <- 0 until 10) {
      new Thread {
        override def run() {
          val answer1 = nums.reduce(_ + _)
          if (answer1 != 55) {
            printf("In thread %d: answer1 was %d\n", i, answer1);
            ok = false;
          }
          val answer2 = nums.first    // This will run "locally" in the current thread
          if (answer2 != 1) {
            printf("In thread %d: answer2 was %d\n", i, answer2);
            ok = false;
          }
          sem.release()
        }
      }.start()
    }
    sem.acquire(10)
    if (!ok) {
      fail("One or more threads got the wrong answer from an RDD operation")
    }
  }

  test("parallel job execution") {
    // This test launches two jobs with two threads each on a 4-core local cluster. Each thread
    // waits until there are 4 threads running at once, to test that both jobs have been launched.
    sc = new SparkContext("local[4]", "test")
    val nums = sc.parallelize(1 to 2, 2)
    val sem = new Semaphore(0)
    ThreadingSuiteState.clear()
    for (i <- 0 until 2) {
      new Thread {
        override def run() {
          val ans = nums.map(number => {
            val running = ThreadingSuiteState.runningThreads
            running.getAndIncrement()
            val time = System.currentTimeMillis()
            while (running.get() != 4 && System.currentTimeMillis() < time + 1000) {
              Thread.sleep(100)
            }
            if (running.get() != 4) {
              println("Waited 1 second without seeing runningThreads = 4 (it was " +
                running.get() + "); failing test")
              ThreadingSuiteState.failed.set(true)
            }
            number
          }).collect()
          assert(ans.toList === List(1, 2))
          sem.release()
        }
      }.start()
    }
    sem.acquire(2)
    if (ThreadingSuiteState.failed.get()) {
      fail("One or more threads didn't see runningThreads = 4")
    }
  }
}

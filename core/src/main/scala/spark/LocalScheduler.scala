package spark

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

/**
 * A simple Scheduler implementation that runs tasks locally in a thread pool. Optionally the 
 * scheduler also allows each task to fail up to maxFailures times, which is useful for testing
 * fault recovery.
 */

private class LocalScheduler(threads: Int, maxFailures: Int) extends DAGScheduler with Logging {
  var attemptId = new AtomicInteger(0)
  var threadPool = Executors.newFixedThreadPool(threads, DaemonThreadFactory)

  // TODO: Need to take into account stage priority in scheduling

  override def start() {}
  
  override def waitForRegister() {}

  // 提交一堆tasks
  override def submitTasks(tasks: Seq[Task[_]], runId: Int) {
    val failCount = new Array[Int](tasks.size)

    def submitTask(task: Task[_], idInJob: Int) {
       // 尝试一次
      val myAttemptId = attemptId.getAndIncrement()
      threadPool.submit(new Runnable {
        def run() {
          // 跑一个任务, 任务的id, 尝试一次
          runTask(task, idInJob, myAttemptId)
        }
      })
    }

    def runTask(task: Task[_], idInJob: Int, attemptId: Int) {
      logInfo("Running task " + idInJob)
      // Set the Spark execution environment for the worker thread
      SparkEnv.set(env)
      try {
        // Serialize and deserialize the task so that accumulators are changed to thread-local ones;
        // this adds a bit of unnecessary overhead but matches how the Mesos Executor works.
        Accumulators.clear
        val ser = SparkEnv.get.closureSerializer.newInstance()
        // 开始时间
        val startTime = System.currentTimeMillis
        // 把一个task给序列化起来
        val bytes = ser.serialize(task)
        // 计算一下时间
        val timeTaken = System.currentTimeMillis - startTime
        // task 的大小是多少，序列化时间是多少
        logInfo("Size of task %d is %d bytes and took %d ms to serialize".format(
            idInJob, bytes.size, timeTaken))
        // 反序列化一下，模仿一下调度到节点以后反序列化的操作
        val deserializedTask = ser.deserialize[Task[_]](bytes, currentThread.getContextClassLoader)
        // 跑一下task
        val result: Any = deserializedTask.run(attemptId)

        // Serialize and deserialize the result to emulate what the mesos
        // executor does. This is useful to catch serialization errors early
        // on in development (so when users move their local Spark programs
        // to the cluster, they don't get surprised by serialization errors).

        // 模仿一下 mesos 的做法，收到结果后反序列化一下
        val resultToReturn = ser.deserialize[Any](ser.serialize(result))

        val accumUpdates = Accumulators.values
        // 完成了task, task id
        logInfo("Finished task " + idInJob)
        // resultToReturn 是返回值
        taskEnded(task, Success, resultToReturn, accumUpdates)
      } catch {
        case t: Throwable => {
          logError("Exception in task " + idInJob, t)
          failCount.synchronized {
            // 某个task的失败次数+1
            failCount(idInJob) += 1
            // 失败次数不多，再次提交
            if (failCount(idInJob) <= maxFailures) {
              submitTask(task, idInJob)
            } else {
              // TODO: Do something nicer here to return all the way to the user
              taskEnded(task, new ExceptionFailure(t), null, null)
            }
          }
        }
      }
    }

    // 对每个task，提交task，id为0到task length个
    for ((task, i) <- tasks.zipWithIndex) {
      submitTask(task, i)
    }
  }
  
  override def stop() {}

  override def defaultParallelism() = threads
}

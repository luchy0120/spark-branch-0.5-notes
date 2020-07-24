package spark

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Queue, Map}

/**
 * A task created by the DAG scheduler. Knows its stage ID and map ouput tracker generation.
 */
abstract class DAGTask[T](val runId: Int, val stageId: Int) extends Task[T] {
  val gen = SparkEnv.get.mapOutputTracker.getGeneration
  override def generation: Option[Long] = Some(gen)
}

/**
 * A completion event passed by the underlying task scheduler to the DAG scheduler.
 */
 // 从task scheduler 发送event到dag scheduler
case class CompletionEvent(
    task: DAGTask[_],
    reason: TaskEndReason,
    result: Any,
    accumUpdates: Map[Long, Any])

/**
 * Various possible reasons why a DAG task ended. The underlying scheduler is supposed to retry
 * tasks several times for "ephemeral" failures, and only report back failures that require some
 * old stages to be resubmitted, such as shuffle map fetch failures.
 */
sealed trait TaskEndReason
case object Success extends TaskEndReason
case class FetchFailed(serverUri: String, shuffleId: Int, mapId: Int, reduceId: Int) extends TaskEndReason
case class ExceptionFailure(exception: Throwable) extends TaskEndReason
case class OtherFailure(message: String) extends TaskEndReason

/**
 * A Scheduler subclass that implements stage-oriented scheduling. It computes a DAG of stages for 
 * each job, keeps track of which RDDs and stage outputs are materialized, and computes a minimal 
 * schedule to run the job. Subclasses only need to implement the code to send a task to the cluster
 * and to report fetch failures (the submitTasks method, and code to add CompletionEvents).
 */
 // 面向stage的调度 ，构建dag stage 图，以最少的调度来跑
private trait DAGScheduler extends Scheduler with Logging {
  // Must be implemented by subclasses to start running a set of tasks. The subclass should also
  // attempt to run different sets of tasks in the order given by runId (lower values first).
  def submitTasks(tasks: Seq[Task[_]], runId: Int): Unit

  // Must be called by subclasses to report task completions or failures.
  def taskEnded(task: Task[_], reason: TaskEndReason, result: Any, accumUpdates: Map[Long, Any]) {
    lock.synchronized {
      val dagTask = task.asInstanceOf[DAGTask[_]]
      eventQueues.get(dagTask.runId) match {
        case Some(queue) =>
        // 每个runId 都代表一次run，它有自己的queue记录一系列 完成的结果event
          queue += CompletionEvent(dagTask, reason, result, accumUpdates)
          lock.notifyAll()
        case None =>
          logInfo("Ignoring completion event for DAG job " + dagTask.runId + " because it's gone")
      }
    }
  }

  // The time, in millis, to wait for fetch failure events to stop coming in after one is detected;
  // this is a simplistic way to avoid resubmitting tasks in the non-fetchable map stage one by one
  // as more failure events come in
  val RESUBMIT_TIMEOUT = 2000L

  // The time, in millis, to wake up between polls of the completion queue in order to potentially
  // resubmit failed stages
  val POLL_TIMEOUT = 500L

// 整个dagscheduler的锁，当有多个任务提交要run时，需要排队
  private val lock = new Object          // Used for access to the entire DAGScheduler

  private val eventQueues = new HashMap[Int, Queue[CompletionEvent]]   // Indexed by run ID

    // 每一次run的id
  val nextRunId = new AtomicInteger(0)
    // 每一个新stage的id
  val nextStageId = new AtomicInteger(0)
    // 根据id拿stage
  val idToStage = new HashMap[Int, Stage]
    // shuffle stage的id
  val shuffleToMapStage = new HashMap[Int, Stage]
    // rddId是key
  var cacheLocs = new HashMap[Int, Array[List[String]]]

  val env = SparkEnv.get
  val cacheTracker = env.cacheTracker
  val mapOutputTracker = env.mapOutputTracker


  def getCacheLocs(rdd: RDD[_]): Array[List[String]] = {
    cacheLocs(rdd.id)
  }
  
  def updateCacheLocs() {
    cacheLocs = cacheTracker.getLocationsSnapshot()
  }

  def getShuffleMapStage(shuf: ShuffleDependency[_,_,_]): Stage = {
  // 根据shuffleId 能取到stage，就取到
    shuffleToMapStage.get(shuf.shuffleId) match {
      case Some(stage) => stage
      case None =>
      // 取不到，就新建一个shuffle map stage
        val stage = newStage(shuf.rdd, Some(shuf))
      // 保存一下id 对应的 stage
        shuffleToMapStage(shuf.shuffleId) = stage
        stage
    }
  }

  def newStage(rdd: RDD[_], shuffleDep: Option[ShuffleDependency[_,_,_]]): Stage = {
    // Kind of ugly: need to register RDDs with the cache and map output tracker here
    // since we can't do it in the RDD constructor because # of splits is unknown
    // 一边遍历，一边把看到的rdd们的split个数记录下来
    cacheTracker.registerRDD(rdd.id, rdd.splits.size)
    if (shuffleDep != None) {
      // 记录一下shuffle
      mapOutputTracker.registerShuffle(shuffleDep.get.shuffleId, rdd.splits.size)
    }
    // 新的stage的id
    val id = nextStageId.getAndIncrement()
    // 创建一个stage，创建的同时查看它的依赖，将父一级的stages 都算出来
    // 作为stage 的父级依赖
    val stage = new Stage(id, rdd, shuffleDep, getParentStages(rdd))
    idToStage(id) = stage
    stage
  }

// 在运行时也就是触发action算子开始向前回溯后，遇到宽依赖就切分成一个stage。每一个stage包含一个或多个并行的task任务
// 计算父stage
  def getParentStages(rdd: RDD[_]): List[Stage] = {
    val parents = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
    def visit(r: RDD[_]) {
      if (!visited(r)) {
      // r 被遍历过了
        visited += r
        // Kind of ugly: need to register RDDs with the cache here since
        // we can't do it in its constructor because # of splits is unknown
        // 一边遍历，一边把看到的rdd们的split个数记录下来
        cacheTracker.registerRDD(r.id, r.splits.size)
        // 遍历 r 的依赖
        for (dep <- r.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_,_,_] =>
            // 是shuffle， 需要shuffle的就给出一个stage
              parents += getShuffleMapStage(shufDep)
            case _ =>
            // 不是shuffle，沿着直线走回去
              visit(dep.rdd)
          }
        }
      }
    }
    visit(rdd)
    // 依赖的parent stages有哪些
    parents.toList
  }
  // 把未跑完整的parentStages 找出来
  def getMissingParentStages(stage: Stage): List[Stage] = {
    val missing = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
    def visit(rdd: RDD[_]) {
      if (!visited(rdd)) {
        visited += rdd
        val locs = getCacheLocs(rdd)
        // 检查每个分片
        for (p <- 0 until rdd.splits.size) {
          if (locs(p) == Nil) {
            for (dep <- rdd.dependencies) {
              dep match {
                case shufDep: ShuffleDependency[_,_,_] =>
                  val stage = getShuffleMapStage(shufDep)
                  if (!stage.isAvailable) {
                  // 本shuffle map stage缺少依赖的数据
                    missing += stage
                  }
                case narrowDep: NarrowDependency[_] =>
                  visit(narrowDep.rdd)
              }
            }
          }
        }
      }
    }
    visit(stage.rdd)
    missing.toList
  }
    // 最终的rdd，要跑在哪些分片上，是否允许在本地跑
  override def runJob[T, U](
      finalRdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      allowLocal: Boolean)
      (implicit m: ClassManifest[U]): Array[U] = {
     // 上锁，只有一个线程能跑任务
    lock.synchronized {
        // 跑下一轮的id
      val runId = nextRunId.getAndIncrement()
        // 哪些partition需要跑
      val outputParts = partitions.toArray
      // 一共多少partitions 需要跑
      val numOutputParts: Int = partitions.size
      // 最终的rdd，产生一个最终的stage， shuffle dependency 是None，所以该 stage的 shuffle
      // dependency 是None，说明不是一个map Shuffle
      // 这一步把dag stage 图构建出来了
      val finalStage = newStage(finalRdd, None)
      // 每个partition的结果
      val results = new Array[U](numOutputParts)
      // 每个partition是否都跑完了
      val finished = new Array[Boolean](numOutputParts)
      var numFinished = 0

      // 等着要跑的stages
      val waiting = new HashSet[Stage] // stages we need to run whose parents aren't done
      // 正在跑的stages
      val running = new HashSet[Stage] // stages we are running right now
      // 跑失败的stages
      val failed = new HashSet[Stage]  // stages that must be resubmitted due to fetch failures
      // 每个stage需要跑的tasks
      val pendingTasks = new HashMap[Stage, HashSet[Task[_]]] // missing tasks from each stage
      // 上一次出错时间
      var lastFetchFailureTime: Long = 0  // used to wait a bit to avoid repeated resubmits
  
      SparkEnv.set(env)
  
      updateCacheLocs()
      
      logInfo("Final stage: " + finalStage)
      logInfo("Parents of final stage: " + finalStage.parents)
      logInfo("Missing parents: " + getMissingParentStages(finalStage))
  
      // Optimization for short actions like first() and take() that can be computed locally
      // without shipping tasks to the cluster.
      // 可以在master本地跑，并且输出个数为1， 并且parents数组为0
      if (allowLocal && finalStage.parents.size == 0 && numOutputParts == 1) {
        logInfo("Computing the requested partition locally")
        // 取到那个唯一需要的分片
        val split = finalRdd.splits(outputParts(0))
        val taskContext = new TaskContext(finalStage.id, outputParts(0), 0)

        // 执行一下 func 函数，这个func 函数已经被包了一层 taskContext
        // 最后的结果是一个array
        return Array(func(taskContext, finalRdd.iterator(split)))
      }

      // Register the job ID so that we can get completion events for it
      eventQueues(runId) = new Queue[CompletionEvent]

  // 把当前所有missing parent的stage添加到waiting里，因为他们需要等到parent stage
  // 执行完了才跑自己
      def submitStage(stage: Stage) {
        // 是不是一个新来的stage
        if (!waiting(stage) && !running(stage)) {
          // 有没有没跑好的parent stages
          val missing = getMissingParentStages(stage)
          // 没有
          if (missing == Nil) {
            logInfo("Submitting " + stage + ", which has no missing parents")
            // 当前的stage可以生成task了
            submitMissingTasks(stage)
            //设置当前的stage为running，因为当前的stage没有未处理完的依赖的stage
            running += stage
          } else {
          // 没有跑好的parent stages们
            for (parent <- missing) {
            // 提交没跑的parent stage
              submitStage(parent)
            }
            // 将自己加入等待队列中
            waiting += stage
          }
        }
      }
  
      def submitMissingTasks(stage: Stage) {
        // Get our pending tasks and remember them in our pendingTasks entry
        val myPending = pendingTasks.getOrElseUpdate(stage, new HashSet)
        var tasks = ArrayBuffer[Task[_]]()
        // 如果是刚开始的stage
        if (stage == finalStage) {
        // 检查所有的分片，如果有没有完成的
          for (id <- 0 until numOutputParts if (!finished(id))) {
            // 取出分片的id
            val part = outputParts(id)
            // 取出分片所在的locations
            val locs = getPreferredLocs(finalRdd, part)
            // 添加一个result task
            tasks += new ResultTask(runId, finalStage.id, finalRdd, func, part, locs, id)
          }
        } else {
        // 如果是shuffleMapStage
          for (p <- 0 until stage.numPartitions if stage.outputLocs(p) == Nil) {
            // 分片没跑出数据，找到这个分片的locations
            val locs = getPreferredLocs(stage.rdd, p)
            // 添加一个shuffletask，将当前stage的partition 转换成一个 task
            tasks += new ShuffleMapTask(runId, stage.id, stage.rdd, stage.shuffleDep.get, p, locs)
          }
        }
        // 等待的tasks们，因为没有等到结果
        myPending ++= tasks
        // 提交这些tasks
        submitTasks(tasks, runId)
      }

      // 把最后的stage 提交上去， 它会找parent stages 去跑
      submitStage(finalStage)

        // 如果没有全部完成
      while (numFinished != numOutputParts) {
        //  等待一个 event的到来
        val eventOption = waitForEvent(runId, POLL_TIMEOUT)
        // 当前时间
        val time = System.currentTimeMillis // TODO: use a pluggable clock for testability
  
        // If we got an event off the queue, mark the task done or react to a fetch failure
        // 等到了一个event
        if (eventOption != None) {
          val evt = eventOption.get
          // task 对应的stage
          val stage = idToStage(evt.task.stageId)
          // 这个 task 不是pending状态了，因为这个task 有了结果，把他从pending里去掉
          pendingTasks(stage) -= evt.task

          // task 成功了
          if (evt.reason == Success) {
            // A task ended
            logInfo("Completed " + evt.task)
            Accumulators.add(evt.accumUpdates)
            evt.task match {
              case rt: ResultTask[_, _] =>
                results(rt.outputId) = evt.result.asInstanceOf[U]
                // 标记成功，partition[outputId] 分片成功了
                finished(rt.outputId) = true
                // 完成了++
                numFinished += 1
              case smt: ShuffleMapTask =>
                val stage = idToStage(smt.stageId)
                stage.addOutputLoc(smt.partition, evt.result.asInstanceOf[String])
                // 当前stage已经没有需要跑的task了
                if (running.contains(stage) && pendingTasks(stage).isEmpty) {
                  // 跑新的stage
                  logInfo(stage + " finished; looking for newly runnable stages")
                  running -= stage
                  if (stage.shuffleDep != None) {
                    mapOutputTracker.registerMapOutputs(
                      stage.shuffleDep.get.shuffleId,
                      stage.outputLocs.map(_.head).toArray)
                  }
                  updateCacheLocs()
                  val newlyRunnable = new ArrayBuffer[Stage]
                  // 从waiting 队列里拿出没有前置依赖的 stage
                  for (stage <- waiting if getMissingParentStages(stage) == Nil) {
                    newlyRunnable += stage
                  }
                  // 从waiting里删掉
                  waiting --= newlyRunnable
                  // 放入running 里
                  running ++= newlyRunnable
                  for (stage <- newlyRunnable) {
                  // 生成task 去run
                    submitMissingTasks(stage)
                  }
                }
            }
          } else {
            evt.reason match {
              case FetchFailed(serverUri, shuffleId, mapId, reduceId) =>
                // Mark the stage that the reducer was in as unrunnable
                // fetch失败的task对应的stage

                val failedStage = idToStage(evt.task.stageId)
                // 从running 中拿走
                running -= failedStage
                // 在failed 中添加
                failed += failedStage
                // TODO: Cancel running tasks in the stage
                logInfo("Marking " + failedStage + " for resubmision due to a fetch failure")
                // Mark the map whose fetch failed as broken in the map stage
                val mapStage = shuffleToMapStage(shuffleId)
                mapStage.removeOutputLoc(mapId, serverUri)
                mapOutputTracker.unregisterMapOutput(shuffleId, mapId, serverUri)
                logInfo("The failed fetch was from " + mapStage + "; marking it for resubmission")
                failed += mapStage
                // Remember that a fetch failed now; this is used to resubmit the broken
                // stages later, after a small wait (to give other tasks the chance to fail)
                lastFetchFailureTime = time
                // TODO: If there are a lot of fetch failures on the same node, maybe mark all
                // outputs on the node as dead.
              case _ =>
                // Non-fetch failure -- probably a bug in the job, so bail out
                eventQueues -= runId
                throw new SparkException("Task failed: " + evt.task + ", reason: " + evt.reason)
                // TODO: Cancel all tasks that are still running
            }
          }
        } // end if (evt != null)
  
        // If fetches have failed recently and we've waited for the right timeout,
        // resubmit all the failed stages
        // 如果有失败的tasks 并且我们也等了挺久了，那么就重新提交失败的stage
        if (failed.size > 0 && time > lastFetchFailureTime + RESUBMIT_TIMEOUT) {
          logInfo("Resubmitting failed stages")
          updateCacheLocs()
          for (stage <- failed) {
            submitStage(stage)
          }
          failed.clear()
        }
      }
        // 跑成功了，把queue去掉
      eventQueues -= runId
      return results
    }
  }

  def getPreferredLocs(rdd: RDD[_], partition: Int): List[String] = {
    // If the partition is cached, return the cache locations
    val cached = getCacheLocs(rdd)(partition)
    if (cached != Nil) {
      return cached
    }
    // If the RDD has some placement preferences (as is the case for input RDDs), get those

    // rdd 喜欢的locations
    val rddPrefs = rdd.preferredLocations(rdd.splits(partition)).toList
    if (rddPrefs != Nil) {
      return rddPrefs
    }
    // If the RDD has narrow dependencies, pick the first partition of the first narrow dep
    // that has any placement preferences. Ideally we would choose based on transfer sizes,
    // but this will do for now.
    // 某个依赖是narrow的
    rdd.dependencies.foreach(_ match {
      case n: NarrowDependency[_] =>
      // 本partition 来自于哪些父partition
        for (inPart <- n.getParents(partition)) {
          val locs = getPreferredLocs(n.rdd, inPart)
          // 不一定能获得到，因为这个dependency 不一顶定能关联当前这个partition
          if (locs != Nil)
            return locs;
        }
      case _ =>
    })
    return Nil
  }

  // Assumes that lock is held on entrance, but will release it to wait for the next event.
  def waitForEvent(runId: Int, timeout: Long): Option[CompletionEvent] = {
    val endTime = System.currentTimeMillis() + timeout   // TODO: Use pluggable clock for testing
    // queue 为空，就先等着，等timeout的时间，如果再这期间等到了event，就返回哪个event
    while (eventQueues(runId).isEmpty) {
      val time = System.currentTimeMillis()
      // 超时了
      if (time >= endTime) {
        return None
      } else {
      // 不超时，等着先
        lock.wait(endTime - time)
      }
    }
    return Some(eventQueues(runId).dequeue())
  }
}

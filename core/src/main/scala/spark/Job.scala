package spark

import org.apache.mesos._
import org.apache.mesos.Protos._

/**
 * Class representing a parallel job in MesosScheduler. Schedules the job by implementing various 
 * callbacks.
 */
 // 模仿mesos中的一个并行的任务
abstract class Job(val runId: Int, val jobId: Int) {
  def slaveOffer(s: Offer, availableCpus: Double): Option[TaskInfo]

  def statusUpdate(t: TaskStatus): Unit

  def error(message: String): Unit
}

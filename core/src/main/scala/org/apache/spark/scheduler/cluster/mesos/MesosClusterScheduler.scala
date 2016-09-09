/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler.cluster.mesos

import java.io.File
import java.util.concurrent.locks.ReentrantLock
import java.util.{Collections, Date, List => JList}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.mesos.Protos.Environment.Variable
import org.apache.mesos.Protos.TaskStatus.Reason
import org.apache.mesos.Protos.{TaskState => MesosTaskState, _}
import org.apache.mesos.{Scheduler, SchedulerDriver}
import org.apache.spark.deploy.mesos.MesosDriverDescription
import org.apache.spark.deploy.rest.{CreateSubmissionResponse, KillSubmissionResponse, SubmissionStatusResponse}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.util.Utils
import org.apache.spark.{SecurityManager, SparkConf, SparkException, TaskState}


/**
 * Tracks the current state of a Mesos Task that runs a Spark driver.
 * @param driverDescription Submitted driver description from
 * [[org.apache.spark.deploy.rest.mesos.MesosRestServer]]
 * @param taskId Mesos TaskID generated for the task
 * @param slaveId Slave ID that the task is assigned to
 * @param mesosTaskStatus The last known task status update.
 * @param startDate The date the task was launched
 */
private[spark] class MesosClusterSubmissionState(
    val driverDescription: MesosDriverDescription,
    val taskId: TaskID,
    val slaveId: SlaveID,
    var mesosTaskStatus: Option[TaskStatus],
    var startDate: Date,
    var finishDate: Option[Date])
  extends Serializable {

  def copy(): MesosClusterSubmissionState = {
    new MesosClusterSubmissionState(
      driverDescription, taskId, slaveId, mesosTaskStatus, startDate, finishDate)
  }
}

/**
 * Tracks the retry state of a driver, which includes the next time it should be scheduled
 * and necessary information to do exponential backoff.
 * This class is not thread-safe, and we expect the caller to handle synchronizing state.
 * @param lastFailureStatus Last Task status when it failed.
 * @param retries Number of times it has been retried.
 * @param nextRetry Time at which it should be retried next
 * @param waitTime The amount of time driver is scheduled to wait until next retry.
 */
private[spark] class MesosClusterRetryState(
    val lastFailureStatus: TaskStatus,
    val retries: Int,
    val nextRetry: Date,
    val waitTime: Int) extends Serializable {
  def copy(): MesosClusterRetryState =
    new MesosClusterRetryState(lastFailureStatus, retries, nextRetry, waitTime)
}

/**
 * The full state of the cluster scheduler, currently being used for displaying
 * information on the UI.
 * @param frameworkId Mesos Framework id for the cluster scheduler.
 * @param masterUrl The Mesos master url
 * @param queuedDrivers All drivers queued to be launched
 * @param launchedDrivers All launched or running drivers
 * @param finishedDrivers All terminated drivers
 * @param pendingRetryDrivers All drivers pending to be retried
 */
private[spark] class MesosClusterSchedulerState(
    val frameworkId: String,
    val masterUrl: Option[String],
    val queuedDrivers: Iterable[MesosDriverDescription],
    val launchedDrivers: Iterable[MesosClusterSubmissionState],
    val finishedDrivers: Iterable[MesosClusterSubmissionState],
    val pendingRetryDrivers: Iterable[MesosDriverDescription])

/**
 * The full state of a Mesos driver, that is being used to display driver information on the UI.
 */
private[spark] class MesosDriverState(
    val state: String,
    val description: MesosDriverDescription,
    val submissionState: Option[MesosClusterSubmissionState] = None)

/**
 * A Mesos scheduler that is responsible for launching submitted Spark drivers in cluster mode
 * as Mesos tasks in a Mesos cluster.
 * All drivers are launched asynchronously by the framework, which will eventually be launched
 * by one of the slaves in the cluster. The results of the driver will be stored in slave's task
 * sandbox which is accessible by visiting the Mesos UI.
 * This scheduler supports recovery by persisting all its state and performs task reconciliation
 * on recover, which gets all the latest state for all the drivers from Mesos master.
 */
private[spark] class MesosClusterScheduler(
    engineFactory: MesosClusterPersistenceEngineFactory,
    conf: SparkConf)
  extends Scheduler with MesosSchedulerUtils {
  var frameworkUrl: String = _
  private val metricsSystem =
    MetricsSystem.createMetricsSystem("mesos_cluster", conf, new SecurityManager(conf))
  private val master = conf.get("spark.master")
  private val appName = conf.get("spark.app.name")
  private val queuedCapacity = conf.getInt("spark.mesos.maxDrivers", 200)
  private val retainedDrivers = conf.getInt("spark.mesos.retainedDrivers", 200)
  private val maxRetryWaitTime = conf.getInt("spark.mesos.cluster.retry.wait.max", 60) // 1 minute
  private val schedulerState = engineFactory.createEngine("scheduler")
  private val stateLock = new ReentrantLock()
  private val finishedDrivers =
    new mutable.ArrayBuffer[MesosClusterSubmissionState](retainedDrivers)
  private var frameworkId: String = null
  // Holds all the launched drivers and current launch state, keyed by driver id.
  private val launchedDrivers = new mutable.HashMap[String, MesosClusterSubmissionState]()
  // Holds a map of driver id to expected slave id that is passed to Mesos for reconciliation.
  // All drivers that are loaded after failover are added here, as we need get the latest
  // state of the tasks from Mesos.
  private val pendingRecover = new mutable.HashMap[String, SlaveID]()
  // Stores all the submitted drivers that hasn't been launched.
  private val queuedDrivers = new ArrayBuffer[MesosDriverDescription]()
  // All supervised drivers that are waiting to retry after termination.
  private val pendingRetryDrivers = new ArrayBuffer[MesosDriverDescription]()
  private val queuedDriversState = engineFactory.createEngine("driverQueue")
  private val launchedDriversState = engineFactory.createEngine("launchedDrivers")
  private val pendingRetryDriversState = engineFactory.createEngine("retryList")
  // Flag to mark if the scheduler is ready to be called, which is until the scheduler
  // is registered with Mesos master.
  @volatile protected var ready = false
  private var masterInfo: Option[MasterInfo] = None

  def submitDriver(desc: MesosDriverDescription): CreateSubmissionResponse = {
    val c = new CreateSubmissionResponse
    if (!ready) {
      c.success = false
      c.message = "Scheduler is not ready to take requests"
      return c
    }

    stateLock.synchronized {
      if (isQueueFull()) {
        c.success = false
        c.message = "Already reached maximum submission size"
        return c
      }
      c.submissionId = desc.submissionId
      queuedDriversState.persist(desc.submissionId, desc)
      queuedDrivers += desc
      c.success = true
    }
    c
  }

  def killDriver(submissionId: String): KillSubmissionResponse = {
    val k = new KillSubmissionResponse
    if (!ready) {
      k.success = false
      k.message = "Scheduler is not ready to take requests"
      return k
    }
    k.submissionId = submissionId
    stateLock.synchronized {
      // We look for the requested driver in the following places:
      // 1. Check if submission is running or launched.
      // 2. Check if it's still queued.
      // 3. Check if it's in the retry list.
      // 4. Check if it has already completed.
      if (launchedDrivers.contains(submissionId)) {
        val task = launchedDrivers(submissionId)
        mesosDriver.killTask(task.taskId)
        k.success = true
        k.message = "Killing running driver"
      } else if (removeFromQueuedDrivers(submissionId)) {
        k.success = true
        k.message = "Removed driver while it's still pending"
      } else if (removeFromPendingRetryDrivers(submissionId)) {
        k.success = true
        k.message = "Removed driver while it's being retried"
      } else if (finishedDrivers.exists(_.driverDescription.submissionId.equals(submissionId))) {
        k.success = false
        k.message = "Driver already terminated"
      } else {
        k.success = false
        k.message = "Cannot find driver"
      }
    }
    k
  }

  def getDriverStatus(submissionId: String): SubmissionStatusResponse = {
    val s = new SubmissionStatusResponse
    if (!ready) {
      s.success = false
      s.message = "Scheduler is not ready to take requests"
      return s
    }
    s.submissionId = submissionId
    stateLock.synchronized {
      if (queuedDrivers.exists(_.submissionId.equals(submissionId))) {
        s.success = true
        s.driverState = "QUEUED"
      } else if (launchedDrivers.contains(submissionId)) {
        s.success = true
        s.driverState = "RUNNING"
        launchedDrivers(submissionId).mesosTaskStatus.foreach(state => s.message = state.toString)
      } else if (finishedDrivers.exists(_.driverDescription.submissionId.equals(submissionId))) {
        s.success = true
        s.driverState = "FINISHED"
        finishedDrivers
          .find(d => d.driverDescription.submissionId.equals(submissionId)).get.mesosTaskStatus
          .foreach(state => s.message = state.toString)
      } else if (pendingRetryDrivers.exists(_.submissionId.equals(submissionId))) {
        val status = pendingRetryDrivers.find(_.submissionId.equals(submissionId))
          .get.retryState.get.lastFailureStatus
        s.success = true
        s.driverState = "RETRYING"
        s.message = status.toString
      } else {
        s.success = false
        s.driverState = "NOT_FOUND"
      }
    }
    s
  }

  /**
   * Gets the driver state to be displayed on the Web UI.
   */
  def getDriverState(submissionId: String): Option[MesosDriverState] = {
    stateLock.synchronized {
      queuedDrivers.find(_.submissionId.equals(submissionId))
        .map(d => new MesosDriverState("QUEUED", d))
        .orElse(launchedDrivers.get(submissionId)
          .map(d => new MesosDriverState("RUNNING", d.driverDescription, Some(d))))
        .orElse(finishedDrivers.find(_.driverDescription.submissionId.equals(submissionId))
          .map(d => new MesosDriverState("FINISHED", d.driverDescription, Some(d))))
        .orElse(pendingRetryDrivers.find(_.submissionId.equals(submissionId))
          .map(d => new MesosDriverState("RETRYING", d)))
    }
  }

  private def isQueueFull(): Boolean = launchedDrivers.size >= queuedCapacity

  /**
   * Recover scheduler state that is persisted.
   * We still need to do task reconciliation to be up to date of the latest task states
   * as it might have changed while the scheduler is failing over.
   */
  private def recoverState(): Unit = {
    stateLock.synchronized {
      launchedDriversState.fetchAll[MesosClusterSubmissionState]().foreach { state =>
        launchedDrivers(state.taskId.getValue) = state
        pendingRecover(state.taskId.getValue) = state.slaveId
      }
      queuedDriversState.fetchAll[MesosDriverDescription]().foreach(d => queuedDrivers += d)
      // There is potential timing issue where a queued driver might have been launched
      // but the scheduler shuts down before the queued driver was able to be removed
      // from the queue. We try to mitigate this issue by walking through all queued drivers
      // and remove if they're already launched.
      queuedDrivers
        .filter(d => launchedDrivers.contains(d.submissionId))
        .foreach(d => removeFromQueuedDrivers(d.submissionId))
      pendingRetryDriversState.fetchAll[MesosDriverDescription]()
        .foreach(s => pendingRetryDrivers += s)
      // TODO: Consider storing finished drivers so we can show them on the UI after
      // failover. For now we clear the history on each recovery.
      finishedDrivers.clear()
    }
  }

  /**
   * Starts the cluster scheduler and wait until the scheduler is registered.
   * This also marks the scheduler to be ready for requests.
   */
  def start(): Unit = {
    // TODO: Implement leader election to make sure only one framework running in the cluster.
    val fwId = schedulerState.fetch[String]("frameworkId")
    fwId.foreach { id =>
      frameworkId = id
    }
    recoverState()
    metricsSystem.registerSource(new MesosClusterSchedulerSource(this))
    metricsSystem.start()
    val driver = createSchedulerDriver(
      master,
      MesosClusterScheduler.this,
      Utils.getCurrentUserName(),
      appName,
      conf,
      Some(frameworkUrl),
      Some(true),
      Some(Integer.MAX_VALUE),
      fwId)

    startScheduler(driver)
    ready = true
  }

  def stop(): Unit = {
    ready = false
    metricsSystem.report()
    metricsSystem.stop()
    mesosDriver.stop(true)
  }

  override def registered(
      driver: SchedulerDriver,
      newFrameworkId: FrameworkID,
      masterInfo: MasterInfo): Unit = {
    logInfo("Registered as framework ID " + newFrameworkId.getValue)
    if (newFrameworkId.getValue != frameworkId) {
      frameworkId = newFrameworkId.getValue
      schedulerState.persist("frameworkId", frameworkId)
    }
    markRegistered()

    stateLock.synchronized {
      this.masterInfo = Some(masterInfo)
      if (!pendingRecover.isEmpty) {
        // Start task reconciliation if we need to recover.
        val statuses = pendingRecover.collect {
          case (taskId, slaveId) =>
            val newStatus = TaskStatus.newBuilder()
              .setTaskId(TaskID.newBuilder().setValue(taskId).build())
              .setSlaveId(slaveId)
              .setState(MesosTaskState.TASK_STAGING)
              .build()
            launchedDrivers.get(taskId).map(_.mesosTaskStatus.getOrElse(newStatus))
              .getOrElse(newStatus)
        }
        // TODO: Page the status updates to avoid trying to reconcile
        // a large amount of tasks at once.
        driver.reconcileTasks(statuses.toSeq.asJava)
      }
    }
  }

  private def buildDriverCommand(desc: MesosDriverDescription): CommandInfo = {
    val appJar = CommandInfo.URI.newBuilder()
      .setValue(desc.jarUrl.stripPrefix("file:").stripPrefix("local:")).build()
    val builder = CommandInfo.newBuilder().addUris(appJar)
    val entries =
      (conf.getOption("spark.executor.extraLibraryPath").toList ++
        desc.command.libraryPathEntries)
    val prefixEnv = if (!entries.isEmpty) {
      Utils.libraryPathEnvPrefix(entries)
    } else {
      ""
    }
    val envBuilder = Environment.newBuilder()
    desc.command.environment.foreach { case (k, v) =>
      envBuilder.addVariables(Variable.newBuilder().setName(k).setValue(v).build())
    }
    // Pass all spark properties to executor.
    val executorOpts = desc.schedulerProperties.map { case (k, v) => s"-D$k=$v" }.mkString(" ")
    envBuilder.addVariables(
      Variable.newBuilder().setName("SPARK_EXECUTOR_OPTS").setValue(executorOpts))
    val dockerDefined = desc.schedulerProperties.contains("spark.mesos.executor.docker.image")
    val executorUri = desc.schedulerProperties.get("spark.executor.uri")
      .orElse(desc.command.environment.get("SPARK_EXECUTOR_URI"))
    // Gets the path to run spark-submit, and the path to the Mesos sandbox.
    val (executable, sandboxPath) = if (dockerDefined) {
      // Application jar is automatically downloaded in the mounted sandbox by Mesos,
      // and the path to the mounted volume is stored in $MESOS_SANDBOX env variable.
      ("./bin/spark-submit", "$MESOS_SANDBOX")
    } else if (executorUri.isDefined) {
      builder.addUris(CommandInfo.URI.newBuilder().setValue(executorUri.get).build())
      val folderBasename = executorUri.get.split('/').last.split('.').head
      val cmdExecutable = s"cd $folderBasename*; $prefixEnv bin/spark-submit"
      // Sandbox path points to the parent folder as we chdir into the folderBasename.
      (cmdExecutable, "..")
    } else {
      val executorSparkHome = desc.schedulerProperties.get("spark.mesos.executor.home")
        .orElse(conf.getOption("spark.home"))
        .orElse(Option(System.getenv("SPARK_HOME")))
        .getOrElse {
          throw new SparkException("Executor Spark home `spark.mesos.executor.home` is not set!")
        }
      val cmdExecutable = new File(executorSparkHome, "./bin/spark-submit").getCanonicalPath
      // Sandbox points to the current directory by default with Mesos.
      (cmdExecutable, ".")
    }
    val primaryResource = new File(sandboxPath, desc.jarUrl.split("/").last).toString()
    val cmdOptions = generateCmdOption(desc, sandboxPath).mkString(" ")
    val appArguments = desc.command.arguments.mkString(" ")
    builder.setValue(s"$executable $cmdOptions $primaryResource $appArguments")
    builder.setEnvironment(envBuilder.build())
    conf.getOption("spark.mesos.uris").map { uris =>
      setupUris(uris, builder)
    }
    desc.schedulerProperties.get("spark.mesos.uris").map { uris =>
      setupUris(uris, builder)
    }
    desc.schedulerProperties.get("spark.submit.pyFiles").map { pyFiles =>
      setupUris(pyFiles, builder)
    }
    builder.build()
  }

  private def generateCmdOption(desc: MesosDriverDescription, sandboxPath: String): Seq[String] = {
    var options = Seq(
      "--name", desc.schedulerProperties("spark.app.name"),
      "--master", s"mesos://${conf.get("spark.master")}",
      "--driver-cores", desc.cores.toString,
      "--driver-memory", s"${desc.mem}M")

    // Assume empty main class means we're running python
    if (!desc.command.mainClass.equals("")) {
      options ++= Seq("--class", desc.command.mainClass)
    }

    desc.schedulerProperties.get("spark.executor.memory").map { v =>
      options ++= Seq("--executor-memory", v)
    }
    desc.schedulerProperties.get("spark.cores.max").map { v =>
      options ++= Seq("--total-executor-cores", v)
    }
    desc.schedulerProperties.get("spark.submit.pyFiles").map { pyFiles =>
      val formattedFiles = pyFiles.split(",")
        .map { path => new File(sandboxPath, path.split("/").last).toString() }
        .mkString(",")
      options ++= Seq("--py-files", formattedFiles)
    }
    options
  }

  private class ResourceOffer(val offer: Offer, var cpu: Double, var mem: Double) {
    override def toString(): String = {
      s"Offer id: ${offer.getId.getValue}, cpu: $cpu, mem: $mem"
    }
  }

  /**
   * This method takes all the possible candidates and attempt to schedule them with Mesos offers.
   * Every time a new task is scheduled, the afterLaunchCallback is called to perform post scheduled
   * logic on each task.
   */
  private def scheduleTasks(
      candidates: Seq[MesosDriverDescription],
      afterLaunchCallback: (String) => Boolean,
      currentOffers: List[ResourceOffer],
      tasks: mutable.HashMap[OfferID, ArrayBuffer[TaskInfo]]): Unit = {
    for (submission <- candidates) {
      val driverCpu = submission.cores
      val driverMem = submission.mem
      logTrace(s"Finding offer to launch driver with cpu: $driverCpu, mem: $driverMem")
      val offerOption = currentOffers.find { o =>
        o.cpu >= driverCpu && o.mem >= driverMem
      }
      if (offerOption.isEmpty) {
        logDebug(s"Unable to find offer to launch driver id: ${submission.submissionId}, " +
          s"cpu: $driverCpu, mem: $driverMem")
      } else {
        val offer = offerOption.get
        offer.cpu -= driverCpu
        offer.mem -= driverMem
        val taskId = TaskID.newBuilder().setValue(submission.submissionId).build()
        val cpuResource = createResource("cpus", driverCpu)
        val memResource = createResource("mem", driverMem)
        val commandInfo = buildDriverCommand(submission)
        val appName = submission.schedulerProperties("spark.app.name")
        val taskInfo = TaskInfo.newBuilder()
          .setTaskId(taskId)
          .setName(s"Driver for $appName")
          .setSlaveId(offer.offer.getSlaveId)
          .setCommand(commandInfo)
          .addResources(cpuResource)
          .addResources(memResource)
        submission.schedulerProperties.get("spark.mesos.executor.docker.image").foreach { image =>
          val container = taskInfo.getContainerBuilder()
          val volumes = submission.schedulerProperties
            .get("spark.mesos.executor.docker.volumes")
            .map(MesosSchedulerBackendUtil.parseVolumesSpec)
          val portmaps = submission.schedulerProperties
            .get("spark.mesos.executor.docker.portmaps")
            .map(MesosSchedulerBackendUtil.parsePortMappingsSpec)
          MesosSchedulerBackendUtil.addDockerInfo(
            container, image, volumes = volumes, portmaps = portmaps)
          taskInfo.setContainer(container.build())
        }
        val queuedTasks = tasks.getOrElseUpdate(offer.offer.getId, new ArrayBuffer[TaskInfo])
        queuedTasks += taskInfo.build()
        logTrace(s"Using offer ${offer.offer.getId.getValue} to launch driver " +
          submission.submissionId)
        val newState = new MesosClusterSubmissionState(submission, taskId, offer.offer.getSlaveId,
          None, new Date(), None)
        launchedDrivers(submission.submissionId) = newState
        launchedDriversState.persist(submission.submissionId, newState)
        afterLaunchCallback(submission.submissionId)
      }
    }
  }

  override def resourceOffers(driver: SchedulerDriver, offers: JList[Offer]): Unit = {
    val currentOffers = offers.asScala.map(o =>
      new ResourceOffer(
        o, getResource(o.getResourcesList, "cpus"), getResource(o.getResourcesList, "mem"))
    ).toList
    logTrace(s"Received offers from Mesos: \n${currentOffers.mkString("\n")}")
    val tasks = new mutable.HashMap[OfferID, ArrayBuffer[TaskInfo]]()
    val currentTime = new Date()

    stateLock.synchronized {
      // We first schedule all the supervised drivers that are ready to retry.
      // This list will be empty if none of the drivers are marked as supervise.
      val driversToRetry = pendingRetryDrivers.filter { d =>
        d.retryState.get.nextRetry.before(currentTime)
      }

      scheduleTasks(
        copyBuffer(driversToRetry),
        removeFromPendingRetryDrivers,
        currentOffers,
        tasks)

      // Then we walk through the queued drivers and try to schedule them.
      scheduleTasks(
        copyBuffer(queuedDrivers),
        removeFromQueuedDrivers,
        currentOffers,
        tasks)
    }
    tasks.foreach { case (offerId, taskInfos) =>
      driver.launchTasks(Collections.singleton(offerId), taskInfos.asJava)
    }
    offers.asScala
      .filter(o => !tasks.keySet.contains(o.getId))
      .foreach(o => driver.declineOffer(o.getId))
  }

  private def copyBuffer(
      buffer: ArrayBuffer[MesosDriverDescription]): ArrayBuffer[MesosDriverDescription] = {
    val newBuffer = new ArrayBuffer[MesosDriverDescription](buffer.size)
    buffer.copyToBuffer(newBuffer)
    newBuffer
  }

  def getSchedulerState(): MesosClusterSchedulerState = {
    stateLock.synchronized {
      new MesosClusterSchedulerState(
        frameworkId,
        masterInfo.map(m => s"http://${m.getIp}:${m.getPort}"),
        copyBuffer(queuedDrivers),
        launchedDrivers.values.map(_.copy()).toList,
        finishedDrivers.map(_.copy()).toList,
        copyBuffer(pendingRetryDrivers))
    }
  }

  override def offerRescinded(driver: SchedulerDriver, offerId: OfferID): Unit = {}
  override def disconnected(driver: SchedulerDriver): Unit = {}
  override def reregistered(driver: SchedulerDriver, masterInfo: MasterInfo): Unit = {
    logInfo(s"Framework re-registered with master ${masterInfo.getId}")
  }
  override def slaveLost(driver: SchedulerDriver, slaveId: SlaveID): Unit = {}
  override def error(driver: SchedulerDriver, error: String): Unit = {
    logError("Error received: " + error)
  }

  /**
   * Check if the task state is a recoverable state that we can relaunch the task.
   * Task state like TASK_ERROR are not relaunchable state since it wasn't able
   * to be validated by Mesos.
   */
  private def shouldRelaunch(state: MesosTaskState): Boolean = {
    state == MesosTaskState.TASK_FAILED ||
      state == MesosTaskState.TASK_KILLED ||
      state == MesosTaskState.TASK_LOST
  }

  override def statusUpdate(driver: SchedulerDriver, status: TaskStatus): Unit = {
    val taskId = status.getTaskId.getValue
    stateLock.synchronized {
      if (launchedDrivers.contains(taskId)) {
        if (status.getReason == Reason.REASON_RECONCILIATION &&
          !pendingRecover.contains(taskId)) {
          // Task has already received update and no longer requires reconciliation.
          return
        }
        val state = launchedDrivers(taskId)
        // Check if the driver is supervise enabled and can be relaunched.
        if (state.driverDescription.supervise && shouldRelaunch(status.getState)) {
          removeFromLaunchedDrivers(taskId)
          state.finishDate = Some(new Date())
          val retryState: Option[MesosClusterRetryState] = state.driverDescription.retryState
          val (retries, waitTimeSec) = retryState
            .map { rs => (rs.retries + 1, Math.min(maxRetryWaitTime, rs.waitTime * 2)) }
            .getOrElse{ (1, 1) }
          val nextRetry = new Date(new Date().getTime + waitTimeSec * 1000L)

          val newDriverDescription = state.driverDescription.copy(
            retryState = Some(new MesosClusterRetryState(status, retries, nextRetry, waitTimeSec)))
          pendingRetryDrivers += newDriverDescription
          pendingRetryDriversState.persist(taskId, newDriverDescription)
        } else if (TaskState.isFinished(TaskState.fromMesos(status.getState))) {
          removeFromLaunchedDrivers(taskId)
          state.finishDate = Some(new Date())
          if (finishedDrivers.size >= retainedDrivers) {
            val toRemove = math.max(retainedDrivers / 10, 1)
            finishedDrivers.trimStart(toRemove)
          }
          finishedDrivers += state
        }
        state.mesosTaskStatus = Option(status)
      } else {
        logError(s"Unable to find driver $taskId in status update")
      }
    }
  }

  override def frameworkMessage(
      driver: SchedulerDriver,
      executorId: ExecutorID,
      slaveId: SlaveID,
      message: Array[Byte]): Unit = {}

  override def executorLost(
      driver: SchedulerDriver,
      executorId: ExecutorID,
      slaveId: SlaveID,
      status: Int): Unit = {}

  private def removeFromQueuedDrivers(id: String): Boolean = {
    val index = queuedDrivers.indexWhere(_.submissionId.equals(id))
    if (index != -1) {
      queuedDrivers.remove(index)
      queuedDriversState.expunge(id)
      true
    } else {
      false
    }
  }

  private def removeFromLaunchedDrivers(id: String): Boolean = {
    if (launchedDrivers.remove(id).isDefined) {
      launchedDriversState.expunge(id)
      true
    } else {
      false
    }
  }

  private def removeFromPendingRetryDrivers(id: String): Boolean = {
    val index = pendingRetryDrivers.indexWhere(_.submissionId.equals(id))
    if (index != -1) {
      pendingRetryDrivers.remove(index)
      pendingRetryDriversState.expunge(id)
      true
    } else {
      false
    }
  }

  def getQueuedDriversSize: Int = queuedDrivers.size
  def getLaunchedDriversSize: Int = launchedDrivers.size
  def getPendingRetryDriversSize: Int = pendingRetryDrivers.size
}

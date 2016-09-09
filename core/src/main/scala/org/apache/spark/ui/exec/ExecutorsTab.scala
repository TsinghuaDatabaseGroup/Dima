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

package org.apache.spark.ui.exec

import scala.collection.mutable.HashMap

import org.apache.spark.{Resubmitted, ExceptionFailure, SparkContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler._
import org.apache.spark.storage.{StorageStatus, StorageStatusListener}
import org.apache.spark.ui.{SparkUI, SparkUITab}
import org.apache.spark.ui.jobs.UIData.ExecutorUIData

private[ui] class ExecutorsTab(parent: SparkUI) extends SparkUITab(parent, "executors") {
  val listener = parent.executorsListener
  val sc = parent.sc
  val threadDumpEnabled =
    sc.isDefined && parent.conf.getBoolean("spark.ui.threadDumpsEnabled", true)

  attachPage(new ExecutorsPage(this, threadDumpEnabled))
  if (threadDumpEnabled) {
    attachPage(new ExecutorThreadDumpPage(this))
  }
}

/**
 * :: DeveloperApi ::
 * A SparkListener that prepares information to be displayed on the ExecutorsTab
 */
@DeveloperApi
class ExecutorsListener(storageStatusListener: StorageStatusListener) extends SparkListener {
  val executorToTasksActive = HashMap[String, Int]()
  val executorToTasksComplete = HashMap[String, Int]()
  val executorToTasksFailed = HashMap[String, Int]()
  val executorToDuration = HashMap[String, Long]()
  val executorToInputBytes = HashMap[String, Long]()
  val executorToInputRecords = HashMap[String, Long]()
  val executorToOutputBytes = HashMap[String, Long]()
  val executorToOutputRecords = HashMap[String, Long]()
  val executorToShuffleRead = HashMap[String, Long]()
  val executorToShuffleWrite = HashMap[String, Long]()
  val executorToLogUrls = HashMap[String, Map[String, String]]()
  val executorIdToData = HashMap[String, ExecutorUIData]()

  def storageStatusList: Seq[StorageStatus] = storageStatusListener.storageStatusList

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = synchronized {
    val eid = executorAdded.executorId
    executorToLogUrls(eid) = executorAdded.executorInfo.logUrlMap
    executorIdToData(eid) = ExecutorUIData(executorAdded.time)
  }

  override def onExecutorRemoved(
      executorRemoved: SparkListenerExecutorRemoved): Unit = synchronized {
    val eid = executorRemoved.executorId
    val uiData = executorIdToData(eid)
    uiData.finishTime = Some(executorRemoved.time)
    uiData.finishReason = Some(executorRemoved.reason)
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    applicationStart.driverLogs.foreach { logs =>
      val storageStatus = storageStatusList.find { s =>
        s.blockManagerId.executorId == SparkContext.LEGACY_DRIVER_IDENTIFIER ||
        s.blockManagerId.executorId == SparkContext.DRIVER_IDENTIFIER
      }
      storageStatus.foreach { s => executorToLogUrls(s.blockManagerId.executorId) = logs.toMap }
    }
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = synchronized {
    val eid = taskStart.taskInfo.executorId
    executorToTasksActive(eid) = executorToTasksActive.getOrElse(eid, 0) + 1
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    val info = taskEnd.taskInfo
    if (info != null) {
      val eid = info.executorId
      taskEnd.reason match {
        case Resubmitted =>
          // Note: For resubmitted tasks, we continue to use the metrics that belong to the
          // first attempt of this task. This may not be 100% accurate because the first attempt
          // could have failed half-way through. The correct fix would be to keep track of the
          // metrics added by each attempt, but this is much more complicated.
          return
        case e: ExceptionFailure =>
          executorToTasksFailed(eid) = executorToTasksFailed.getOrElse(eid, 0) + 1
        case _ =>
          executorToTasksComplete(eid) = executorToTasksComplete.getOrElse(eid, 0) + 1
      }

      executorToTasksActive(eid) = executorToTasksActive.getOrElse(eid, 1) - 1
      executorToDuration(eid) = executorToDuration.getOrElse(eid, 0L) + info.duration

      // Update shuffle read/write
      val metrics = taskEnd.taskMetrics
      if (metrics != null) {
        metrics.inputMetrics.foreach { inputMetrics =>
          executorToInputBytes(eid) =
            executorToInputBytes.getOrElse(eid, 0L) + inputMetrics.bytesRead
          executorToInputRecords(eid) =
            executorToInputRecords.getOrElse(eid, 0L) + inputMetrics.recordsRead
        }
        metrics.outputMetrics.foreach { outputMetrics =>
          executorToOutputBytes(eid) =
            executorToOutputBytes.getOrElse(eid, 0L) + outputMetrics.bytesWritten
          executorToOutputRecords(eid) =
            executorToOutputRecords.getOrElse(eid, 0L) + outputMetrics.recordsWritten
        }
        metrics.shuffleReadMetrics.foreach { shuffleRead =>
          executorToShuffleRead(eid) =
            executorToShuffleRead.getOrElse(eid, 0L) + shuffleRead.remoteBytesRead
        }
        metrics.shuffleWriteMetrics.foreach { shuffleWrite =>
          executorToShuffleWrite(eid) =
            executorToShuffleWrite.getOrElse(eid, 0L) + shuffleWrite.shuffleBytesWritten
        }
      }
    }
  }

}

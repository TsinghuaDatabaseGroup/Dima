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

package org.apache.spark.scheduler.cluster

import org.apache.hadoop.fs.{Path, FileSystem}

import org.apache.spark.rpc.RpcAddress
import org.apache.spark.{Logging, SparkContext, SparkEnv}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.scheduler.TaskSchedulerImpl

private[spark] class SimrSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext,
    driverFilePath: String)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv)
  with Logging {

  val tmpPath = new Path(driverFilePath + "_tmp")
  val filePath = new Path(driverFilePath)

  val maxCores = conf.getInt("spark.simr.executor.cores", 1)

  override def start() {
    super.start()

    val driverUrl = rpcEnv.uriOf(SparkEnv.driverActorSystemName,
      RpcAddress(sc.conf.get("spark.driver.host"), sc.conf.get("spark.driver.port").toInt),
      CoarseGrainedSchedulerBackend.ENDPOINT_NAME)

    val conf = SparkHadoopUtil.get.newConfiguration(sc.conf)
    val fs = FileSystem.get(conf)
    val appUIAddress = sc.ui.map(_.appUIAddress).getOrElse("")

    logInfo("Writing to HDFS file: "  + driverFilePath)
    logInfo("Writing Akka address: "  + driverUrl)
    logInfo("Writing Spark UI Address: " + appUIAddress)

    // Create temporary file to prevent race condition where executors get empty driverUrl file
    val temp = fs.create(tmpPath, true)
    temp.writeUTF(driverUrl)
    temp.writeInt(maxCores)
    temp.writeUTF(appUIAddress)
    temp.close()

    // "Atomic" rename
    fs.rename(tmpPath, filePath)
  }

  override def stop() {
    val conf = SparkHadoopUtil.get.newConfiguration(sc.conf)
    val fs = FileSystem.get(conf)
    if (!fs.delete(new Path(driverFilePath), false)) {
      logWarning(s"error deleting ${driverFilePath}")
    }
    super.stop()
  }

}

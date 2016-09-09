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
package org.apache.spark.network.yarn

import java.io.{DataOutputStream, File, FileOutputStream}

import scala.annotation.tailrec

import org.apache.commons.io.FileUtils
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.api.{ApplicationInitializationContext, ApplicationTerminationContext}
import org.scalatest.{BeforeAndAfterEach, Matchers}

import org.apache.spark.SparkFunSuite
import org.apache.spark.network.shuffle.ShuffleTestAccessor
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo

class YarnShuffleServiceSuite extends SparkFunSuite with Matchers with BeforeAndAfterEach {
  private[yarn] var yarnConfig: YarnConfiguration = new YarnConfiguration

  override def beforeEach(): Unit = {
    yarnConfig.set(YarnConfiguration.NM_AUX_SERVICES, "spark_shuffle")
    yarnConfig.set(YarnConfiguration.NM_AUX_SERVICE_FMT.format("spark_shuffle"),
      classOf[YarnShuffleService].getCanonicalName)
    yarnConfig.setInt("spark.shuffle.service.port", 0)

    yarnConfig.get("yarn.nodemanager.local-dirs").split(",").foreach { dir =>
      val d = new File(dir)
      if (d.exists()) {
        FileUtils.deleteDirectory(d)
      }
      FileUtils.forceMkdir(d)
      logInfo(s"creating yarn.nodemanager.local-dirs: $d")
    }
  }

  var s1: YarnShuffleService = null
  var s2: YarnShuffleService = null
  var s3: YarnShuffleService = null

  override def afterEach(): Unit = {
    if (s1 != null) {
      s1.stop()
      s1 = null
    }
    if (s2 != null) {
      s2.stop()
      s2 = null
    }
    if (s3 != null) {
      s3.stop()
      s3 = null
    }
  }

  test("executor state kept across NM restart") {
    s1 = new YarnShuffleService
    s1.init(yarnConfig)
    val app1Id = ApplicationId.newInstance(0, 1)
    val app1Data: ApplicationInitializationContext =
      new ApplicationInitializationContext("user", app1Id, null)
    s1.initializeApplication(app1Data)
    val app2Id = ApplicationId.newInstance(0, 2)
    val app2Data: ApplicationInitializationContext =
      new ApplicationInitializationContext("user", app2Id, null)
    s1.initializeApplication(app2Data)

    val execStateFile = s1.registeredExecutorFile
    execStateFile should not be (null)
    val shuffleInfo1 = new ExecutorShuffleInfo(Array("/foo", "/bar"), 3, "sort")
    val shuffleInfo2 = new ExecutorShuffleInfo(Array("/bippy"), 5, "hash")

    val blockHandler = s1.blockHandler
    val blockResolver = ShuffleTestAccessor.getBlockResolver(blockHandler)
    ShuffleTestAccessor.registeredExecutorFile(blockResolver) should be (execStateFile)

    blockResolver.registerExecutor(app1Id.toString, "exec-1", shuffleInfo1)
    blockResolver.registerExecutor(app2Id.toString, "exec-2", shuffleInfo2)
    ShuffleTestAccessor.getExecutorInfo(app1Id, "exec-1", blockResolver) should
      be (Some(shuffleInfo1))
    ShuffleTestAccessor.getExecutorInfo(app2Id, "exec-2", blockResolver) should
      be (Some(shuffleInfo2))

    if (!execStateFile.exists()) {
      @tailrec def findExistingParent(file: File): File = {
        if (file == null) file
        else if (file.exists()) file
        else findExistingParent(file.getParentFile())
      }
      val existingParent = findExistingParent(execStateFile)
      assert(false, s"$execStateFile does not exist -- closest existing parent is $existingParent")
    }
    assert(execStateFile.exists(), s"$execStateFile did not exist")

    // now we pretend the shuffle service goes down, and comes back up
    s1.stop()
    s2 = new YarnShuffleService
    s2.init(yarnConfig)
    s2.registeredExecutorFile should be (execStateFile)

    val handler2 = s2.blockHandler
    val resolver2 = ShuffleTestAccessor.getBlockResolver(handler2)

    // now we reinitialize only one of the apps, and expect yarn to tell us that app2 was stopped
    // during the restart
    s2.initializeApplication(app1Data)
    s2.stopApplication(new ApplicationTerminationContext(app2Id))
    ShuffleTestAccessor.getExecutorInfo(app1Id, "exec-1", resolver2) should be (Some(shuffleInfo1))
    ShuffleTestAccessor.getExecutorInfo(app2Id, "exec-2", resolver2) should be (None)

    // Act like the NM restarts one more time
    s2.stop()
    s3 = new YarnShuffleService
    s3.init(yarnConfig)
    s3.registeredExecutorFile should be (execStateFile)

    val handler3 = s3.blockHandler
    val resolver3 = ShuffleTestAccessor.getBlockResolver(handler3)

    // app1 is still running
    s3.initializeApplication(app1Data)
    ShuffleTestAccessor.getExecutorInfo(app1Id, "exec-1", resolver3) should be (Some(shuffleInfo1))
    ShuffleTestAccessor.getExecutorInfo(app2Id, "exec-2", resolver3) should be (None)
    s3.stop()
  }

  test("removed applications should not be in registered executor file") {
    s1 = new YarnShuffleService
    s1.init(yarnConfig)
    val app1Id = ApplicationId.newInstance(0, 1)
    val app1Data: ApplicationInitializationContext =
      new ApplicationInitializationContext("user", app1Id, null)
    s1.initializeApplication(app1Data)
    val app2Id = ApplicationId.newInstance(0, 2)
    val app2Data: ApplicationInitializationContext =
      new ApplicationInitializationContext("user", app2Id, null)
    s1.initializeApplication(app2Data)

    val execStateFile = s1.registeredExecutorFile
    execStateFile should not be (null)
    val shuffleInfo1 = new ExecutorShuffleInfo(Array("/foo", "/bar"), 3, "sort")
    val shuffleInfo2 = new ExecutorShuffleInfo(Array("/bippy"), 5, "hash")

    val blockHandler = s1.blockHandler
    val blockResolver = ShuffleTestAccessor.getBlockResolver(blockHandler)
    ShuffleTestAccessor.registeredExecutorFile(blockResolver) should be (execStateFile)

    blockResolver.registerExecutor(app1Id.toString, "exec-1", shuffleInfo1)
    blockResolver.registerExecutor(app2Id.toString, "exec-2", shuffleInfo2)

    val db = ShuffleTestAccessor.shuffleServiceLevelDB(blockResolver)
    ShuffleTestAccessor.reloadRegisteredExecutors(db) should not be empty

    s1.stopApplication(new ApplicationTerminationContext(app1Id))
    ShuffleTestAccessor.reloadRegisteredExecutors(db) should not be empty
    s1.stopApplication(new ApplicationTerminationContext(app2Id))
    ShuffleTestAccessor.reloadRegisteredExecutors(db) shouldBe empty
  }

  test("shuffle service should be robust to corrupt registered executor file") {
    s1 = new YarnShuffleService
    s1.init(yarnConfig)
    val app1Id = ApplicationId.newInstance(0, 1)
    val app1Data: ApplicationInitializationContext =
      new ApplicationInitializationContext("user", app1Id, null)
    s1.initializeApplication(app1Data)

    val execStateFile = s1.registeredExecutorFile
    val shuffleInfo1 = new ExecutorShuffleInfo(Array("/foo", "/bar"), 3, "sort")

    val blockHandler = s1.blockHandler
    val blockResolver = ShuffleTestAccessor.getBlockResolver(blockHandler)
    ShuffleTestAccessor.registeredExecutorFile(blockResolver) should be (execStateFile)

    blockResolver.registerExecutor(app1Id.toString, "exec-1", shuffleInfo1)

    // now we pretend the shuffle service goes down, and comes back up.  But we'll also
    // make a corrupt registeredExecutor File
    s1.stop()

    execStateFile.listFiles().foreach{_.delete()}

    val out = new DataOutputStream(new FileOutputStream(execStateFile + "/CURRENT"))
    out.writeInt(42)
    out.close()

    s2 = new YarnShuffleService
    s2.init(yarnConfig)
    s2.registeredExecutorFile should be (execStateFile)

    val handler2 = s2.blockHandler
    val resolver2 = ShuffleTestAccessor.getBlockResolver(handler2)

    // we re-initialize app1, but since the file was corrupt there is nothing we can do about it ...
    s2.initializeApplication(app1Data)
    // however, when we initialize a totally new app2, everything is still happy
    val app2Id = ApplicationId.newInstance(0, 2)
    val app2Data: ApplicationInitializationContext =
      new ApplicationInitializationContext("user", app2Id, null)
    s2.initializeApplication(app2Data)
    val shuffleInfo2 = new ExecutorShuffleInfo(Array("/bippy"), 5, "hash")
    resolver2.registerExecutor(app2Id.toString, "exec-2", shuffleInfo2)
    ShuffleTestAccessor.getExecutorInfo(app2Id, "exec-2", resolver2) should be (Some(shuffleInfo2))
    s2.stop()

    // another stop & restart should be fine though (eg., we recover from previous corruption)
    s3 = new YarnShuffleService
    s3.init(yarnConfig)
    s3.registeredExecutorFile should be (execStateFile)
    val handler3 = s3.blockHandler
    val resolver3 = ShuffleTestAccessor.getBlockResolver(handler3)

    s3.initializeApplication(app2Data)
    ShuffleTestAccessor.getExecutorInfo(app2Id, "exec-2", resolver3) should be (Some(shuffleInfo2))
    s3.stop()

  }

}

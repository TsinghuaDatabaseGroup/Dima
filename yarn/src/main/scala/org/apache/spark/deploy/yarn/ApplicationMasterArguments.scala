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

package org.apache.spark.deploy.yarn

import org.apache.spark.util.{MemoryParam, IntParam}
import org.apache.spark.deploy.yarn.YarnSparkHadoopUtil._
import collection.mutable.ArrayBuffer

class ApplicationMasterArguments(val args: Array[String]) {
  var userJar: String = null
  var userClass: String = null
  var primaryPyFile: String = null
  var primaryRFile: String = null
  var userArgs: Seq[String] = Nil
  var executorMemory = 1024
  var executorCores = 1
  var propertiesFile: String = null

  parseArgs(args.toList)

  private def parseArgs(inputArgs: List[String]): Unit = {
    val userArgsBuffer = new ArrayBuffer[String]()

    var args = inputArgs

    while (!args.isEmpty) {
      // --num-workers, --worker-memory, and --worker-cores are deprecated since 1.0,
      // the properties with executor in their names are preferred.
      args match {
        case ("--jar") :: value :: tail =>
          userJar = value
          args = tail

        case ("--class") :: value :: tail =>
          userClass = value
          args = tail

        case ("--primary-py-file") :: value :: tail =>
          primaryPyFile = value
          args = tail

        case ("--primary-r-file") :: value :: tail =>
          primaryRFile = value
          args = tail

        case ("--args" | "--arg") :: value :: tail =>
          userArgsBuffer += value
          args = tail

        case ("--worker-memory" | "--executor-memory") :: MemoryParam(value) :: tail =>
          executorMemory = value
          args = tail

        case ("--worker-cores" | "--executor-cores") :: IntParam(value) :: tail =>
          executorCores = value
          args = tail

        case ("--properties-file") :: value :: tail =>
          propertiesFile = value
          args = tail

        case _ =>
          printUsageAndExit(1, args)
      }
    }

    if (primaryPyFile != null && primaryRFile != null) {
      // scalastyle:off println
      System.err.println("Cannot have primary-py-file and primary-r-file at the same time")
      // scalastyle:on println
      System.exit(-1)
    }

    userArgs = userArgsBuffer.readOnly
  }

  def printUsageAndExit(exitCode: Int, unknownParam: Any = null) {
    // scalastyle:off println
    if (unknownParam != null) {
      System.err.println("Unknown/unsupported param " + unknownParam)
    }
    System.err.println("""
      |Usage: org.apache.spark.deploy.yarn.ApplicationMaster [options]
      |Options:
      |  --jar JAR_PATH       Path to your application's JAR file
      |  --class CLASS_NAME   Name of your application's main class
      |  --primary-py-file    A main Python file
      |  --primary-r-file     A main R file
      |  --py-files PY_FILES  Comma-separated list of .zip, .egg, or .py files to
      |                       place on the PYTHONPATH for Python apps.
      |  --args ARGS          Arguments to be passed to your application's main class.
      |                       Multiple invocations are possible, each will be passed in order.
      |  --executor-cores NUM   Number of cores for the executors (Default: 1)
      |  --executor-memory MEM  Memory per executor (e.g. 1000M, 2G) (Default: 1G)
      |  --properties-file FILE Path to a custom Spark properties file.
      """.stripMargin)
    // scalastyle:on println
    System.exit(exitCode)
  }
}

object ApplicationMasterArguments {
  val DEFAULT_NUMBER_EXECUTORS = 2
}

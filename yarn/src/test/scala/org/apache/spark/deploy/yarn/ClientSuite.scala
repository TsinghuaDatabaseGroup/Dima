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

import java.io.File
import java.net.URI

import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap => MutableHashMap}
import scala.reflect.ClassTag
import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.MRJobConfig
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.YarnClientApplication
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Records
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterAll, Matchers}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.util.Utils

class ClientSuite extends SparkFunSuite with Matchers with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    System.setProperty("SPARK_YARN_MODE", "true")
  }

  override def afterAll(): Unit = {
    System.clearProperty("SPARK_YARN_MODE")
  }

  test("default Yarn application classpath") {
    Client.getDefaultYarnApplicationClasspath should be(Some(Fixtures.knownDefYarnAppCP))
  }

  test("default MR application classpath") {
    Client.getDefaultMRApplicationClasspath should be(Some(Fixtures.knownDefMRAppCP))
  }

  test("resultant classpath for an application that defines a classpath for YARN") {
    withAppConf(Fixtures.mapYARNAppConf) { conf =>
      val env = newEnv
      Client.populateHadoopClasspath(conf, env)
      classpath(env) should be(
        flatten(Fixtures.knownYARNAppCP, Client.getDefaultMRApplicationClasspath))
    }
  }

  test("resultant classpath for an application that defines a classpath for MR") {
    withAppConf(Fixtures.mapMRAppConf) { conf =>
      val env = newEnv
      Client.populateHadoopClasspath(conf, env)
      classpath(env) should be(
        flatten(Client.getDefaultYarnApplicationClasspath, Fixtures.knownMRAppCP))
    }
  }

  test("resultant classpath for an application that defines both classpaths, YARN and MR") {
    withAppConf(Fixtures.mapAppConf) { conf =>
      val env = newEnv
      Client.populateHadoopClasspath(conf, env)
      classpath(env) should be(flatten(Fixtures.knownYARNAppCP, Fixtures.knownMRAppCP))
    }
  }

  private val SPARK = "local:/sparkJar"
  private val USER = "local:/userJar"
  private val ADDED = "local:/addJar1,local:/addJar2,/addJar3"

  test("Local jar URIs") {
    val conf = new Configuration()
    val sparkConf = new SparkConf().set(Client.CONF_SPARK_JAR, SPARK)
      .set("spark.yarn.user.classpath.first", "true")
    val env = new MutableHashMap[String, String]()
    val args = new ClientArguments(Array("--jar", USER, "--addJars", ADDED), sparkConf)

    Client.populateClasspath(args, conf, sparkConf, env, true)

    val cp = env("CLASSPATH").split(":|;|<CPS>")
    s"$SPARK,$USER,$ADDED".split(",").foreach({ entry =>
      val uri = new URI(entry)
      if (Client.LOCAL_SCHEME.equals(uri.getScheme())) {
        cp should contain (uri.getPath())
      } else {
        cp should not contain (uri.getPath())
      }
    })
    val pwdVar =
      if (classOf[Environment].getMethods().exists(_.getName == "$$")) {
        "{{PWD}}"
      } else if (Utils.isWindows) {
        "%PWD%"
      } else {
        Environment.PWD.$()
      }
    cp should contain(pwdVar)
    cp should contain (s"$pwdVar${Path.SEPARATOR}${Client.LOCALIZED_CONF_DIR}")
    cp should not contain (Client.SPARK_JAR)
    cp should not contain (Client.APP_JAR)
  }

  test("Jar path propagation through SparkConf") {
    val conf = new Configuration()
    val sparkConf = new SparkConf().set(Client.CONF_SPARK_JAR, SPARK)
    val args = new ClientArguments(Array("--jar", USER, "--addJars", ADDED), sparkConf)

    val client = spy(new Client(args, conf, sparkConf))
    doReturn(new Path("/")).when(client).copyFileToRemote(any(classOf[Path]),
      any(classOf[Path]), anyShort())

    val tempDir = Utils.createTempDir()
    try {
      client.prepareLocalResources(tempDir.getAbsolutePath(), Nil)
      sparkConf.getOption(Client.CONF_SPARK_USER_JAR) should be (Some(USER))

      // The non-local path should be propagated by name only, since it will end up in the app's
      // staging dir.
      val expected = ADDED.split(",")
        .map(p => {
          val uri = new URI(p)
          if (Client.LOCAL_SCHEME == uri.getScheme()) {
            p
          } else {
            Option(uri.getFragment()).getOrElse(new File(p).getName())
          }
        })
        .mkString(",")

      sparkConf.getOption(Client.CONF_SPARK_YARN_SECONDARY_JARS) should be (Some(expected))
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }

  test("Cluster path translation") {
    val conf = new Configuration()
    val sparkConf = new SparkConf()
      .set(Client.CONF_SPARK_JAR, "local:/localPath/spark.jar")
      .set("spark.yarn.config.gatewayPath", "/localPath")
      .set("spark.yarn.config.replacementPath", "/remotePath")

    Client.getClusterPath(sparkConf, "/localPath") should be ("/remotePath")
    Client.getClusterPath(sparkConf, "/localPath/1:/localPath/2") should be (
      "/remotePath/1:/remotePath/2")

    val env = new MutableHashMap[String, String]()
    Client.populateClasspath(null, conf, sparkConf, env, false,
      extraClassPath = Some("/localPath/my1.jar"))
    val cp = classpath(env)
    cp should contain ("/remotePath/spark.jar")
    cp should contain ("/remotePath/my1.jar")
  }

  test("configuration and args propagate through createApplicationSubmissionContext") {
    val conf = new Configuration()
    // When parsing tags, duplicates and leading/trailing whitespace should be removed.
    // Spaces between non-comma strings should be preserved as single tags. Empty strings may or
    // may not be removed depending on the version of Hadoop being used.
    val sparkConf = new SparkConf()
      .set(Client.CONF_SPARK_YARN_APPLICATION_TAGS, ",tag1, dup,tag2 , ,multi word , dup")
      .set("spark.yarn.maxAppAttempts", "42")
    val args = new ClientArguments(Array(
      "--name", "foo-test-app",
      "--queue", "staging-queue"), sparkConf)

    val appContext = Records.newRecord(classOf[ApplicationSubmissionContext])
    val getNewApplicationResponse = Records.newRecord(classOf[GetNewApplicationResponse])
    val containerLaunchContext = Records.newRecord(classOf[ContainerLaunchContext])

    val client = new Client(args, conf, sparkConf)
    client.createApplicationSubmissionContext(
      new YarnClientApplication(getNewApplicationResponse, appContext),
      containerLaunchContext)

    appContext.getApplicationName should be ("foo-test-app")
    appContext.getQueue should be ("staging-queue")
    appContext.getAMContainerSpec should be (containerLaunchContext)
    appContext.getApplicationType should be ("SPARK")
    appContext.getClass.getMethods.filter(_.getName.equals("getApplicationTags")).foreach{ method =>
      val tags = method.invoke(appContext).asInstanceOf[java.util.Set[String]]
      tags should contain allOf ("tag1", "dup", "tag2", "multi word")
      tags.asScala.filter(_.nonEmpty).size should be (4)
    }
    appContext.getMaxAppAttempts should be (42)
  }

  object Fixtures {

    val knownDefYarnAppCP: Seq[String] =
      getFieldValue[Array[String], Seq[String]](classOf[YarnConfiguration],
                                                "DEFAULT_YARN_APPLICATION_CLASSPATH",
                                                Seq[String]())(a => a.toSeq)


    val knownDefMRAppCP: Seq[String] =
      getFieldValue2[String, Array[String], Seq[String]](
        classOf[MRJobConfig],
        "DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH",
        Seq[String]())(a => a.split(","))(a => a.toSeq)

    val knownYARNAppCP = Some(Seq("/known/yarn/path"))

    val knownMRAppCP = Some(Seq("/known/mr/path"))

    val mapMRAppConf =
      Map("mapreduce.application.classpath" -> knownMRAppCP.map(_.mkString(":")).get)

    val mapYARNAppConf =
      Map(YarnConfiguration.YARN_APPLICATION_CLASSPATH -> knownYARNAppCP.map(_.mkString(":")).get)

    val mapAppConf = mapYARNAppConf ++ mapMRAppConf
  }

  def withAppConf(m: Map[String, String] = Map())(testCode: (Configuration) => Any) {
    val conf = new Configuration
    m.foreach { case (k, v) => conf.set(k, v, "ClientSpec") }
    testCode(conf)
  }

  def newEnv: MutableHashMap[String, String] = MutableHashMap[String, String]()

  def classpath(env: MutableHashMap[String, String]): Array[String] =
    env(Environment.CLASSPATH.name).split(":|;|<CPS>")

  def flatten(a: Option[Seq[String]], b: Option[Seq[String]]): Array[String] =
    (a ++ b).flatten.toArray

  def getFieldValue[A, B](clazz: Class[_], field: String, defaults: => B)(mapTo: A => B): B = {
    Try(clazz.getField(field))
      .map(_.get(null).asInstanceOf[A])
      .toOption
      .map(mapTo)
      .getOrElse(defaults)
  }

  def getFieldValue2[A: ClassTag, A1: ClassTag, B](
        clazz: Class[_],
        field: String,
        defaults: => B)(mapTo: A => B)(mapTo1: A1 => B): B = {
    Try(clazz.getField(field)).map(_.get(null)).map {
      case v: A => mapTo(v)
      case v1: A1 => mapTo1(v1)
      case _ => defaults
    }.toOption.getOrElse(defaults)
  }

}

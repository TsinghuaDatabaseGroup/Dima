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

package org.apache.spark.sql.execution

import java.util.Properties

import scala.collection.parallel.CompositeThrowable

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.sql.SQLContext

class SQLExecutionSuite extends SparkFunSuite {

  test("concurrent query execution (SPARK-10548)") {
    // Try to reproduce the issue with the old SparkContext
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("test")
    val badSparkContext = new BadSparkContext(conf)
    try {
      testConcurrentQueryExecution(badSparkContext)
      fail("unable to reproduce SPARK-10548")
    } catch {
      case e: IllegalArgumentException =>
        assert(e.getMessage.contains(SQLExecution.EXECUTION_ID_KEY))
    } finally {
      badSparkContext.stop()
    }

    // Verify that the issue is fixed with the latest SparkContext
    val goodSparkContext = new SparkContext(conf)
    try {
      testConcurrentQueryExecution(goodSparkContext)
    } finally {
      goodSparkContext.stop()
    }
  }

  /**
   * Trigger SPARK-10548 by mocking a parent and its child thread executing queries concurrently.
   */
  private def testConcurrentQueryExecution(sc: SparkContext): Unit = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // Initialize local properties. This is necessary for the test to pass.
    sc.getLocalProperties

    // Set up a thread that runs executes a simple SQL query.
    // Before starting the thread, mutate the execution ID in the parent.
    // The child thread should not see the effect of this change.
    var throwable: Option[Throwable] = None
    val child = new Thread {
      override def run(): Unit = {
        try {
          sc.parallelize(1 to 100).map { i => (i, i) }.toDF("a", "b").collect()
        } catch {
          case t: Throwable =>
            throwable = Some(t)
        }

      }
    }
    sc.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, "anything")
    child.start()
    child.join()

    // The throwable is thrown from the child thread so it doesn't have a helpful stack trace
    throwable.foreach { t =>
      t.setStackTrace(t.getStackTrace ++ Thread.currentThread.getStackTrace)
      throw t
    }
  }

}

/**
 * A bad [[SparkContext]] that does not clone the inheritable thread local properties
 * when passing them to children threads.
 */
private class BadSparkContext(conf: SparkConf) extends SparkContext(conf) {
  protected[spark] override val localProperties = new InheritableThreadLocal[Properties] {
    override protected def childValue(parent: Properties): Properties = new Properties(parent)
    override protected def initialValue(): Properties = new Properties()
  }
}

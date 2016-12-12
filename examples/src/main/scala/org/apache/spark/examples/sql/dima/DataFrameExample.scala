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

// scalastyle:off println
package org.apache.spark.examples.sql.dima

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext

/**
  * Created by zeyuan.shang on 12/12/16.
  */
object DataFrameExample {
  case class LiteralString(value: String)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DataFrameExample").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val s1 = sc.textFile("./examples/src/main/resources/string.txt")
      .map(new LiteralString(_)).toDF()
    val s2 = sc.textFile("./examples/src/main/resources/string.txt")
      .map(new LiteralString(_)).toDF()

    s1.edJoin(s2, Array("value"), Array("value"), 1).collect().foreach(println)

    s1.jaccardJoin(s2, Array("value"), Array("value"), 0.8).collect().foreach(println)

    sc.stop()
  }
}

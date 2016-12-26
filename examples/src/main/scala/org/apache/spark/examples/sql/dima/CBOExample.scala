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

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by zeyuan.shang on 12/13/16.
  */
object CBOExample {
  case class Record(record: String)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("CBOExample").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val s1 = sc.textFile("./examples/src/main/resources/string.txt")
      .map(new Record(_)).toDF()
    val s2 = sc.textFile("./examples/src/main/resources/string.txt")
      .map(new Record(_)).toDF()
    val s3 = sc.textFile("./examples/src/main/resources/string.txt")
      .map(new Record(_)).toDF()

    s1.registerTempTable("Record1")
    s2.registerTempTable("Record2")
    s3.registerTempTable("Record3")

    val res = sqlContext.sql("select distinct * from Record1 " +
      "SIMILARITY join Record2 on JACCARDSIMILARITY(Record1.record, Record2.record) >= 0.7 " +
      "SIMILARITY join Record3 on JACCARDSIMILARITY(Record2.record, Record3.record) >= 0.5")
    res.collect().foreach(println)

    sqlContext.sql("CREATE INDEX jaccard ON Record1(record) USE JACCARDINDEX")
    val res2 = sqlContext.sql("select distinct * from Record1 " +
      "where JACCARDSIMILARITY(Record1.record, \"vldb\") >= 0.5")
    res2.collect().foreach(println)

    sc.stop()
  }
}

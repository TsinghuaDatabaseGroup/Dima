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
package org.apache.spark.examples.ml

// $example on$
import org.apache.spark.ml.feature.QuantileDiscretizer
// $example off$
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object QuantileDiscretizerExample {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("QuantileDiscretizerExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // $example on$
    val data = Array((0, 18.0), (1, 19.0), (2, 8.0), (3, 5.0), (4, 2.2))
    val df = sc.parallelize(data).toDF("id", "hour")

    val discretizer = new QuantileDiscretizer()
      .setInputCol("hour")
      .setOutputCol("result")
      .setNumBuckets(3)

    val result = discretizer.fit(df).transform(df)
    result.show()
    // $example off$
    sc.stop()
  }
}
// scalastyle:on println

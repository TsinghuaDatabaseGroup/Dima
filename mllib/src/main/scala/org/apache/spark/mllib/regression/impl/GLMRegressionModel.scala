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

package org.apache.spark.mllib.regression.impl

import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.util.Loader
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
 * Helper methods for import/export of GLM regression models.
 */
private[regression] object GLMRegressionModel {

  object SaveLoadV1_0 {

    def thisFormatVersion: String = "1.0"

    /** Model data for model import/export */
    case class Data(weights: Vector, intercept: Double)

    /**
     * Helper method for saving GLM regression model metadata and data.
     * @param modelClass  String name for model class, to be saved with metadata
     */
    def save(
        sc: SparkContext,
        path: String,
        modelClass: String,
        weights: Vector,
        intercept: Double): Unit = {
      val sqlContext = SQLContext.getOrCreate(sc)
      import sqlContext.implicits._

      // Create JSON metadata.
      val metadata = compact(render(
        ("class" -> modelClass) ~ ("version" -> thisFormatVersion) ~
          ("numFeatures" -> weights.size)))
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(Loader.metadataPath(path))

      // Create Parquet data.
      val data = Data(weights, intercept)
      val dataRDD: DataFrame = sc.parallelize(Seq(data), 1).toDF()
      // TODO: repartition with 1 partition after SPARK-5532 gets fixed
      dataRDD.write.parquet(Loader.dataPath(path))
    }

    /**
     * Helper method for loading GLM regression model data.
     * @param modelClass  String name for model class (used for error messages)
     * @param numFeatures  Number of features, to be checked against loaded data.
     *                     The length of the weights vector should equal numFeatures.
     */
    def loadData(sc: SparkContext, path: String, modelClass: String, numFeatures: Int): Data = {
      val datapath = Loader.dataPath(path)
      val sqlContext = SQLContext.getOrCreate(sc)
      val dataRDD = sqlContext.read.parquet(datapath)
      val dataArray = dataRDD.select("weights", "intercept").take(1)
      assert(dataArray.size == 1, s"Unable to load $modelClass data from: $datapath")
      val data = dataArray(0)
      assert(data.size == 2, s"Unable to load $modelClass data from: $datapath")
      data match {
        case Row(weights: Vector, intercept: Double) =>
          assert(weights.size == numFeatures, s"Expected $numFeatures features, but" +
            s" found ${weights.size} features when loading $modelClass weights from $datapath")
          Data(weights, intercept)
      }
    }
  }

}

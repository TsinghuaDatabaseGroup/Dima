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

package org.apache.spark.examples.ml;

// $example on$
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.regression.AFTSurvivalRegression;
import org.apache.spark.ml.regression.AFTSurvivalRegressionModel;
import org.apache.spark.mllib.linalg.*;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.*;
// $example off$

public class JavaAFTSurvivalRegressionExample {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("JavaAFTSurvivalRegressionExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext jsql = new SQLContext(jsc);

    // $example on$
    List<Row> data = Arrays.asList(
      RowFactory.create(1.218, 1.0, Vectors.dense(1.560, -0.605)),
      RowFactory.create(2.949, 0.0, Vectors.dense(0.346, 2.158)),
      RowFactory.create(3.627, 0.0, Vectors.dense(1.380, 0.231)),
      RowFactory.create(0.273, 1.0, Vectors.dense(0.520, 1.151)),
      RowFactory.create(4.199, 0.0, Vectors.dense(0.795, -0.226))
    );
    StructType schema = new StructType(new StructField[]{
      new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
      new StructField("censor", DataTypes.DoubleType, false, Metadata.empty()),
      new StructField("features", new VectorUDT(), false, Metadata.empty())
    });
    DataFrame training = jsql.createDataFrame(data, schema);
    double[] quantileProbabilities = new double[]{0.3, 0.6};
    AFTSurvivalRegression aft = new AFTSurvivalRegression()
      .setQuantileProbabilities(quantileProbabilities)
      .setQuantilesCol("quantiles");

    AFTSurvivalRegressionModel model = aft.fit(training);

    // Print the coefficients, intercept and scale parameter for AFT survival regression
    System.out.println("Coefficients: " + model.coefficients() + " Intercept: "
      + model.intercept() + " Scale: " + model.scale());
    model.transform(training).show(false);
    // $example off$

    jsc.stop();
  }
}

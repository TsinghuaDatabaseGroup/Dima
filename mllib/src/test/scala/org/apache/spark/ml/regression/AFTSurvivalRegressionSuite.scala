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

package org.apache.spark.ml.regression

import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.random.{ExponentialGenerator, WeibullGenerator}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.sql.{DataFrame, Row}

class AFTSurvivalRegressionSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  @transient var datasetUnivariate: DataFrame = _
  @transient var datasetMultivariate: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    datasetUnivariate = sqlContext.createDataFrame(
      sc.parallelize(generateAFTInput(
        1, Array(5.5), Array(0.8), 1000, 42, 1.0, 2.0, 2.0)))
    datasetMultivariate = sqlContext.createDataFrame(
      sc.parallelize(generateAFTInput(
        2, Array(0.9, -1.3), Array(0.7, 1.2), 1000, 42, 1.5, 2.5, 2.0)))
  }

  test("params") {
    ParamsSuite.checkParams(new AFTSurvivalRegression)
    val model = new AFTSurvivalRegressionModel("aftSurvReg", Vectors.dense(0.0), 0.0, 0.0)
    ParamsSuite.checkParams(model)
  }

  test("aft survival regression: default params") {
    val aftr = new AFTSurvivalRegression
    assert(aftr.getLabelCol === "label")
    assert(aftr.getFeaturesCol === "features")
    assert(aftr.getPredictionCol === "prediction")
    assert(aftr.getCensorCol === "censor")
    assert(aftr.getFitIntercept)
    assert(aftr.getMaxIter === 100)
    assert(aftr.getTol === 1E-6)
    val model = aftr.setQuantileProbabilities(Array(0.1, 0.8))
      .setQuantilesCol("quantiles")
      .fit(datasetUnivariate)

    // copied model must have the same parent.
    MLTestingUtils.checkCopy(model)

    model.transform(datasetUnivariate)
      .select("label", "prediction", "quantiles")
      .collect()
    assert(model.getFeaturesCol === "features")
    assert(model.getPredictionCol === "prediction")
    assert(model.getQuantileProbabilities === Array(0.1, 0.8))
    assert(model.getQuantilesCol === "quantiles")
    assert(model.intercept !== 0.0)
    assert(model.hasParent)
  }

  def generateAFTInput(
      numFeatures: Int,
      xMean: Array[Double],
      xVariance: Array[Double],
      nPoints: Int,
      seed: Int,
      weibullShape: Double,
      weibullScale: Double,
      exponentialMean: Double): Seq[AFTPoint] = {

    def censor(x: Double, y: Double): Double = { if (x <= y) 1.0 else 0.0 }

    val weibull = new WeibullGenerator(weibullShape, weibullScale)
    weibull.setSeed(seed)

    val exponential = new ExponentialGenerator(exponentialMean)
    exponential.setSeed(seed)

    val rnd = new Random(seed)
    val x = Array.fill[Array[Double]](nPoints)(Array.fill[Double](numFeatures)(rnd.nextDouble()))

    x.foreach { v =>
      var i = 0
      val len = v.length
      while (i < len) {
        v(i) = (v(i) - 0.5) * math.sqrt(12.0 * xVariance(i)) + xMean(i)
        i += 1
      }
    }
    val y = (1 to nPoints).map { i => (weibull.nextValue(), exponential.nextValue()) }

    y.zip(x).map { p => AFTPoint(Vectors.dense(p._2), p._1._1, censor(p._1._1, p._1._2)) }
  }

  test("aft survival regression with univariate") {
    val quantileProbabilities = Array(0.1, 0.5, 0.9)
    val trainer = new AFTSurvivalRegression()
      .setQuantileProbabilities(quantileProbabilities)
      .setQuantilesCol("quantiles")
    val model = trainer.fit(datasetUnivariate)

    /*
       Using the following R code to load the data and train the model using survival package.

       library("survival")
       data <- read.csv("path", header=FALSE, stringsAsFactors=FALSE)
       features <- data$V1
       censor <- data$V2
       label <- data$V3
       sr.fit <- survreg(Surv(label, censor) ~ features, dist='weibull')
       summary(sr.fit)

                    Value Std. Error      z        p
       (Intercept)  1.759     0.4141  4.247 2.16e-05
       features    -0.039     0.0735 -0.531 5.96e-01
       Log(scale)   0.344     0.0379  9.073 1.16e-19

       Scale= 1.41

       Weibull distribution
       Loglik(model)= -1152.2   Loglik(intercept only)= -1152.3
           Chisq= 0.28 on 1 degrees of freedom, p= 0.6
       Number of Newton-Raphson Iterations: 5
       n= 1000
     */
    val coefficientsR = Vectors.dense(-0.039)
    val interceptR = 1.759
    val scaleR = 1.41

    assert(model.intercept ~== interceptR relTol 1E-3)
    assert(model.coefficients ~== coefficientsR relTol 1E-3)
    assert(model.scale ~== scaleR relTol 1E-3)

    /*
       Using the following R code to predict.

       testdata <- list(features=6.559282795753792)
       responsePredict <- predict(sr.fit, newdata=testdata)
       responsePredict

              1
       4.494763

       quantilePredict <- predict(sr.fit, newdata=testdata, type='quantile', p=c(0.1, 0.5, 0.9))
       quantilePredict

       [1]  0.1879174  2.6801195 14.5779394
     */
    val features = Vectors.dense(6.559282795753792)
    val responsePredictR = 4.494763
    val quantilePredictR = Vectors.dense(0.1879174, 2.6801195, 14.5779394)

    assert(model.predict(features) ~== responsePredictR relTol 1E-3)
    assert(model.predictQuantiles(features) ~== quantilePredictR relTol 1E-3)

    model.transform(datasetUnivariate).select("features", "prediction", "quantiles")
      .collect().foreach {
        case Row(features: Vector, prediction: Double, quantiles: Vector) =>
          assert(prediction ~== model.predict(features) relTol 1E-5)
          assert(quantiles ~== model.predictQuantiles(features) relTol 1E-5)
    }
  }

  test("aft survival regression with multivariate") {
    val quantileProbabilities = Array(0.1, 0.5, 0.9)
    val trainer = new AFTSurvivalRegression()
      .setQuantileProbabilities(quantileProbabilities)
      .setQuantilesCol("quantiles")
    val model = trainer.fit(datasetMultivariate)

    /*
       Using the following R code to load the data and train the model using survival package.

       library("survival")
       data <- read.csv("path", header=FALSE, stringsAsFactors=FALSE)
       feature1 <- data$V1
       feature2 <- data$V2
       censor <- data$V3
       label <- data$V4
       sr.fit <- survreg(Surv(label, censor) ~ feature1 + feature2, dist='weibull')
       summary(sr.fit)

                     Value Std. Error      z        p
       (Intercept)  1.9206     0.1057 18.171 8.78e-74
       feature1    -0.0844     0.0611 -1.381 1.67e-01
       feature2     0.0677     0.0468  1.447 1.48e-01
       Log(scale)  -0.0236     0.0436 -0.542 5.88e-01

       Scale= 0.977

       Weibull distribution
       Loglik(model)= -1070.7   Loglik(intercept only)= -1072.7
           Chisq= 3.91 on 2 degrees of freedom, p= 0.14
       Number of Newton-Raphson Iterations: 5
       n= 1000
     */
    val coefficientsR = Vectors.dense(-0.0844, 0.0677)
    val interceptR = 1.9206
    val scaleR = 0.977

    assert(model.intercept ~== interceptR relTol 1E-3)
    assert(model.coefficients ~== coefficientsR relTol 1E-3)
    assert(model.scale ~== scaleR relTol 1E-3)

    /*
       Using the following R code to predict.
       testdata <- list(feature1=2.233396950271428, feature2=-2.5321374085997683)
       responsePredict <- predict(sr.fit, newdata=testdata)
       responsePredict

              1
       4.761219

       quantilePredict <- predict(sr.fit, newdata=testdata, type='quantile', p=c(0.1, 0.5, 0.9))
       quantilePredict

       [1]  0.5287044  3.3285858 10.7517072
     */
    val features = Vectors.dense(2.233396950271428, -2.5321374085997683)
    val responsePredictR = 4.761219
    val quantilePredictR = Vectors.dense(0.5287044, 3.3285858, 10.7517072)

    assert(model.predict(features) ~== responsePredictR relTol 1E-3)
    assert(model.predictQuantiles(features) ~== quantilePredictR relTol 1E-3)

    model.transform(datasetMultivariate).select("features", "prediction", "quantiles")
      .collect().foreach {
        case Row(features: Vector, prediction: Double, quantiles: Vector) =>
          assert(prediction ~== model.predict(features) relTol 1E-5)
          assert(quantiles ~== model.predictQuantiles(features) relTol 1E-5)
    }
  }

  test("aft survival regression w/o intercept") {
    val quantileProbabilities = Array(0.1, 0.5, 0.9)
    val trainer = new AFTSurvivalRegression()
      .setQuantileProbabilities(quantileProbabilities)
      .setQuantilesCol("quantiles")
      .setFitIntercept(false)
    val model = trainer.fit(datasetMultivariate)

    /*
       Using the following R code to load the data and train the model using survival package.

       library("survival")
       data <- read.csv("path", header=FALSE, stringsAsFactors=FALSE)
       feature1 <- data$V1
       feature2 <- data$V2
       censor <- data$V3
       label <- data$V4
       sr.fit <- survreg(Surv(label, censor) ~ feature1 + feature2 - 1, dist='weibull')
       summary(sr.fit)

                   Value Std. Error     z        p
       feature1    0.896     0.0685  13.1 3.93e-39
       feature2   -0.709     0.0522 -13.6 5.78e-42
       Log(scale)  0.420     0.0401  10.5 1.23e-25

       Scale= 1.52

       Weibull distribution
       Loglik(model)= -1292.4   Loglik(intercept only)= -1072.7
         Chisq= -439.57 on 1 degrees of freedom, p= 1
       Number of Newton-Raphson Iterations: 6
       n= 1000
     */
    val coefficientsR = Vectors.dense(0.896, -0.709)
    val interceptR = 0.0
    val scaleR = 1.52

    assert(model.intercept === interceptR)
    assert(model.coefficients ~== coefficientsR relTol 1E-3)
    assert(model.scale ~== scaleR relTol 1E-3)

    /*
       Using the following R code to predict.
       testdata <- list(feature1=2.233396950271428, feature2=-2.5321374085997683)
       responsePredict <- predict(sr.fit, newdata=testdata)
       responsePredict

              1
       44.54465

       quantilePredict <- predict(sr.fit, newdata=testdata, type='quantile', p=c(0.1, 0.5, 0.9))
       quantilePredict

       [1]   1.452103  25.506077 158.428600
     */
    val features = Vectors.dense(2.233396950271428, -2.5321374085997683)
    val responsePredictR = 44.54465
    val quantilePredictR = Vectors.dense(1.452103, 25.506077, 158.428600)

    assert(model.predict(features) ~== responsePredictR relTol 1E-3)
    assert(model.predictQuantiles(features) ~== quantilePredictR relTol 1E-3)

    model.transform(datasetMultivariate).select("features", "prediction", "quantiles")
      .collect().foreach {
        case Row(features: Vector, prediction: Double, quantiles: Vector) =>
          assert(prediction ~== model.predict(features) relTol 1E-5)
          assert(quantiles ~== model.predictQuantiles(features) relTol 1E-5)
    }
  }

  test("aft survival regression w/o quantiles column") {
    val trainer = new AFTSurvivalRegression
    val model = trainer.fit(datasetUnivariate)
    val outputDf = model.transform(datasetUnivariate)

    assert(outputDf.schema.fieldNames.contains("quantiles") === false)

    outputDf.select("features", "prediction")
      .collect().foreach {
        case Row(features: Vector, prediction: Double) =>
          assert(prediction ~== model.predict(features) relTol 1E-5)
    }
  }

  test("read/write") {
    def checkModelData(
        model: AFTSurvivalRegressionModel,
        model2: AFTSurvivalRegressionModel): Unit = {
      assert(model.intercept === model2.intercept)
      assert(model.coefficients === model2.coefficients)
      assert(model.scale === model2.scale)
    }
    val aft = new AFTSurvivalRegression()
    testEstimatorAndModelReadWrite(aft, datasetMultivariate,
      AFTSurvivalRegressionSuite.allParamSettings, checkModelData)
  }
}

object AFTSurvivalRegressionSuite {

  /**
   * Mapping from all Params to valid settings which differ from the defaults.
   * This is useful for tests which need to exercise all Params, such as save/load.
   * This excludes input columns to simplify some tests.
   */
  val allParamSettings: Map[String, Any] = Map(
    "predictionCol" -> "myPrediction",
    "fitIntercept" -> true,
    "maxIter" -> 2,
    "tol" -> 0.01
  )
}

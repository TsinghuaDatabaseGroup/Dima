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

package org.apache.spark.ml.tuning

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.ml.{Pipeline, Estimator, Model}
import org.apache.spark.ml.classification.{LogisticRegressionModel, LogisticRegression}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, Evaluator, RegressionEvaluator}
import org.apache.spark.ml.param.{ParamPair, ParamMap}
import org.apache.spark.ml.param.shared.HasInputCol
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.classification.LogisticRegressionSuite.generateLogisticInput
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.{LinearDataGenerator, MLlibTestSparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.StructType

class CrossValidatorSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  @transient var dataset: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val sqlContext = new SQLContext(sc)
    dataset = sqlContext.createDataFrame(
      sc.parallelize(generateLogisticInput(1.0, 1.0, 100, 42), 2))
  }

  test("cross validation with logistic regression") {
    val lr = new LogisticRegression
    val lrParamMaps = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.001, 1000.0))
      .addGrid(lr.maxIter, Array(0, 10))
      .build()
    val eval = new BinaryClassificationEvaluator
    val cv = new CrossValidator()
      .setEstimator(lr)
      .setEstimatorParamMaps(lrParamMaps)
      .setEvaluator(eval)
      .setNumFolds(3)
    val cvModel = cv.fit(dataset)

    // copied model must have the same paren.
    MLTestingUtils.checkCopy(cvModel)

    val parent = cvModel.bestModel.parent.asInstanceOf[LogisticRegression]
    assert(parent.getRegParam === 0.001)
    assert(parent.getMaxIter === 10)
    assert(cvModel.avgMetrics.length === lrParamMaps.length)
  }

  test("cross validation with linear regression") {
    val dataset = sqlContext.createDataFrame(
      sc.parallelize(LinearDataGenerator.generateLinearInput(
        6.3, Array(4.7, 7.2), Array(0.9, -1.3), Array(0.7, 1.2), 100, 42, 0.1), 2))

    val trainer = new LinearRegression().setSolver("l-bfgs")
    val lrParamMaps = new ParamGridBuilder()
      .addGrid(trainer.regParam, Array(1000.0, 0.001))
      .addGrid(trainer.maxIter, Array(0, 10))
      .build()
    val eval = new RegressionEvaluator()
    val cv = new CrossValidator()
      .setEstimator(trainer)
      .setEstimatorParamMaps(lrParamMaps)
      .setEvaluator(eval)
      .setNumFolds(3)
    val cvModel = cv.fit(dataset)
    val parent = cvModel.bestModel.parent.asInstanceOf[LinearRegression]
    assert(parent.getRegParam === 0.001)
    assert(parent.getMaxIter === 10)
    assert(cvModel.avgMetrics.length === lrParamMaps.length)

    eval.setMetricName("r2")
    val cvModel2 = cv.fit(dataset)
    val parent2 = cvModel2.bestModel.parent.asInstanceOf[LinearRegression]
    assert(parent2.getRegParam === 0.001)
    assert(parent2.getMaxIter === 10)
    assert(cvModel2.avgMetrics.length === lrParamMaps.length)
  }

  test("validateParams should check estimatorParamMaps") {
    import CrossValidatorSuite.{MyEstimator, MyEvaluator}

    val est = new MyEstimator("est")
    val eval = new MyEvaluator
    val paramMaps = new ParamGridBuilder()
      .addGrid(est.inputCol, Array("input1", "input2"))
      .build()

    val cv = new CrossValidator()
      .setEstimator(est)
      .setEstimatorParamMaps(paramMaps)
      .setEvaluator(eval)

    cv.validateParams() // This should pass.

    val invalidParamMaps = paramMaps :+ ParamMap(est.inputCol -> "")
    cv.setEstimatorParamMaps(invalidParamMaps)
    intercept[IllegalArgumentException] {
      cv.validateParams()
    }
  }

  test("read/write: CrossValidator with simple estimator") {
    val lr = new LogisticRegression().setMaxIter(3)
    val evaluator = new BinaryClassificationEvaluator()
      .setMetricName("areaUnderPR")  // not default metric
    val paramMaps = new ParamGridBuilder()
        .addGrid(lr.regParam, Array(0.1, 0.2))
        .build()
    val cv = new CrossValidator()
      .setEstimator(lr)
      .setEvaluator(evaluator)
      .setNumFolds(20)
      .setEstimatorParamMaps(paramMaps)

    val cv2 = testDefaultReadWrite(cv, testParams = false)

    assert(cv.uid === cv2.uid)
    assert(cv.getNumFolds === cv2.getNumFolds)

    assert(cv2.getEvaluator.isInstanceOf[BinaryClassificationEvaluator])
    val evaluator2 = cv2.getEvaluator.asInstanceOf[BinaryClassificationEvaluator]
    assert(evaluator.uid === evaluator2.uid)
    assert(evaluator.getMetricName === evaluator2.getMetricName)

    cv2.getEstimator match {
      case lr2: LogisticRegression =>
        assert(lr.uid === lr2.uid)
        assert(lr.getMaxIter === lr2.getMaxIter)
      case other =>
        throw new AssertionError(s"Loaded CrossValidator expected estimator of type" +
          s" LogisticRegression but found ${other.getClass.getName}")
    }

    CrossValidatorSuite.compareParamMaps(cv.getEstimatorParamMaps, cv2.getEstimatorParamMaps)
  }

  test("read/write: CrossValidator with complex estimator") {
    // workflow: CrossValidator[Pipeline[HashingTF, CrossValidator[LogisticRegression]]]
    val lrEvaluator = new BinaryClassificationEvaluator()
      .setMetricName("areaUnderPR")  // not default metric

    val lr = new LogisticRegression().setMaxIter(3)
    val lrParamMaps = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.2))
      .build()
    val lrcv = new CrossValidator()
      .setEstimator(lr)
      .setEvaluator(lrEvaluator)
      .setEstimatorParamMaps(lrParamMaps)

    val hashingTF = new HashingTF()
    val pipeline = new Pipeline().setStages(Array(hashingTF, lrcv))
    val paramMaps = new ParamGridBuilder()
      .addGrid(hashingTF.numFeatures, Array(10, 20))
      .addGrid(lr.elasticNetParam, Array(0.0, 1.0))
      .build()
    val evaluator = new BinaryClassificationEvaluator()

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setNumFolds(20)
      .setEstimatorParamMaps(paramMaps)

    val cv2 = testDefaultReadWrite(cv, testParams = false)

    assert(cv.uid === cv2.uid)
    assert(cv.getNumFolds === cv2.getNumFolds)

    assert(cv2.getEvaluator.isInstanceOf[BinaryClassificationEvaluator])
    assert(cv.getEvaluator.uid === cv2.getEvaluator.uid)

    CrossValidatorSuite.compareParamMaps(cv.getEstimatorParamMaps, cv2.getEstimatorParamMaps)

    cv2.getEstimator match {
      case pipeline2: Pipeline =>
        assert(pipeline.uid === pipeline2.uid)
        pipeline2.getStages match {
          case Array(hashingTF2: HashingTF, lrcv2: CrossValidator) =>
            assert(hashingTF.uid === hashingTF2.uid)
            lrcv2.getEstimator match {
              case lr2: LogisticRegression =>
                assert(lr.uid === lr2.uid)
                assert(lr.getMaxIter === lr2.getMaxIter)
              case other =>
                throw new AssertionError(s"Loaded internal CrossValidator expected to be" +
                  s" LogisticRegression but found type ${other.getClass.getName}")
            }
            assert(lrcv.uid === lrcv2.uid)
            assert(lrcv2.getEvaluator.isInstanceOf[BinaryClassificationEvaluator])
            assert(lrEvaluator.uid === lrcv2.getEvaluator.uid)
            CrossValidatorSuite.compareParamMaps(lrParamMaps, lrcv2.getEstimatorParamMaps)
          case other =>
            throw new AssertionError("Loaded Pipeline expected stages (HashingTF, CrossValidator)" +
              " but found: " + other.map(_.getClass.getName).mkString(", "))
        }
      case other =>
        throw new AssertionError(s"Loaded CrossValidator expected estimator of type" +
          s" CrossValidator but found ${other.getClass.getName}")
    }
  }

  test("read/write: CrossValidator fails for extraneous Param") {
    val lr = new LogisticRegression()
    val lr2 = new LogisticRegression()
    val evaluator = new BinaryClassificationEvaluator()
    val paramMaps = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.2))
      .addGrid(lr2.regParam, Array(0.1, 0.2))
      .build()
    val cv = new CrossValidator()
      .setEstimator(lr)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramMaps)
    withClue("CrossValidator.write failed to catch extraneous Param error") {
      intercept[IllegalArgumentException] {
        cv.write
      }
    }
  }

  test("read/write: CrossValidatorModel") {
    val lr = new LogisticRegression()
      .setThreshold(0.6)
    val lrModel = new LogisticRegressionModel(lr.uid, Vectors.dense(1.0, 2.0), 1.2)
      .setThreshold(0.6)
    val evaluator = new BinaryClassificationEvaluator()
      .setMetricName("areaUnderPR")  // not default metric
    val paramMaps = new ParamGridBuilder()
        .addGrid(lr.regParam, Array(0.1, 0.2))
        .build()
    val cv = new CrossValidatorModel("cvUid", lrModel, Array(0.3, 0.6))
    cv.set(cv.estimator, lr)
      .set(cv.evaluator, evaluator)
      .set(cv.numFolds, 20)
      .set(cv.estimatorParamMaps, paramMaps)

    val cv2 = testDefaultReadWrite(cv, testParams = false)

    assert(cv.uid === cv2.uid)
    assert(cv.getNumFolds === cv2.getNumFolds)

    assert(cv2.getEvaluator.isInstanceOf[BinaryClassificationEvaluator])
    val evaluator2 = cv2.getEvaluator.asInstanceOf[BinaryClassificationEvaluator]
    assert(evaluator.uid === evaluator2.uid)
    assert(evaluator.getMetricName === evaluator2.getMetricName)

    cv2.getEstimator match {
      case lr2: LogisticRegression =>
        assert(lr.uid === lr2.uid)
        assert(lr.getThreshold === lr2.getThreshold)
      case other =>
        throw new AssertionError(s"Loaded CrossValidator expected estimator of type" +
          s" LogisticRegression but found ${other.getClass.getName}")
    }

    CrossValidatorSuite.compareParamMaps(cv.getEstimatorParamMaps, cv2.getEstimatorParamMaps)

    cv2.bestModel match {
      case lrModel2: LogisticRegressionModel =>
        assert(lrModel.uid === lrModel2.uid)
        assert(lrModel.getThreshold === lrModel2.getThreshold)
        assert(lrModel.coefficients === lrModel2.coefficients)
        assert(lrModel.intercept === lrModel2.intercept)
      case other =>
        throw new AssertionError(s"Loaded CrossValidator expected bestModel of type" +
          s" LogisticRegressionModel but found ${other.getClass.getName}")
    }
    assert(cv.avgMetrics === cv2.avgMetrics)
  }
}

object CrossValidatorSuite extends SparkFunSuite {

  /**
   * Assert sequences of estimatorParamMaps are identical.
   * Params must be simple types comparable with `===`.
   */
  def compareParamMaps(pMaps: Array[ParamMap], pMaps2: Array[ParamMap]): Unit = {
    assert(pMaps.length === pMaps2.length)
    pMaps.zip(pMaps2).foreach { case (pMap, pMap2) =>
      assert(pMap.size === pMap2.size)
      pMap.toSeq.foreach { case ParamPair(p, v) =>
        assert(pMap2.contains(p))
        assert(pMap2(p) === v)
      }
    }
  }

  abstract class MyModel extends Model[MyModel]

  class MyEstimator(override val uid: String) extends Estimator[MyModel] with HasInputCol {

    override def validateParams(): Unit = require($(inputCol).nonEmpty)

    override def fit(dataset: DataFrame): MyModel = {
      throw new UnsupportedOperationException
    }

    override def transformSchema(schema: StructType): StructType = {
      throw new UnsupportedOperationException
    }

    override def copy(extra: ParamMap): MyEstimator = defaultCopy(extra)
  }

  class MyEvaluator extends Evaluator {

    override def evaluate(dataset: DataFrame): Double = {
      throw new UnsupportedOperationException
    }

    override def isLargerBetter: Boolean = true

    override val uid: String = "eval"

    override def copy(extra: ParamMap): MyEvaluator = defaultCopy(extra)
  }
}

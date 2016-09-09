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

package org.apache.spark.ml.clustering

import org.apache.hadoop.fs.Path
import org.apache.spark.Logging
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.shared.{HasCheckpointInterval, HasFeaturesCol, HasSeed, HasMaxIter}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.clustering.{DistributedLDAModel => OldDistributedLDAModel,
    EMLDAOptimizer => OldEMLDAOptimizer, LDA => OldLDA, LDAModel => OldLDAModel,
    LDAOptimizer => OldLDAOptimizer, LocalLDAModel => OldLocalLDAModel,
    OnlineLDAOptimizer => OldOnlineLDAOptimizer}
import org.apache.spark.mllib.linalg.{VectorUDT, Vectors, Matrix, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.functions.{col, monotonicallyIncreasingId, udf}
import org.apache.spark.sql.types.StructType


private[clustering] trait LDAParams extends Params with HasFeaturesCol with HasMaxIter
  with HasSeed with HasCheckpointInterval {

  /**
   * Param for the number of topics (clusters) to infer. Must be > 1. Default: 10.
   * @group param
   */
  @Since("1.6.0")
  final val k = new IntParam(this, "k", "number of topics (clusters) to infer",
    ParamValidators.gt(1))

  /** @group getParam */
  @Since("1.6.0")
  def getK: Int = $(k)

  /**
   * Concentration parameter (commonly named "alpha") for the prior placed on documents'
   * distributions over topics ("theta").
   *
   * This is the parameter to a Dirichlet distribution, where larger values mean more smoothing
   * (more regularization).
   *
   * If not set by the user, then docConcentration is set automatically. If set to
   * singleton vector [alpha], then alpha is replicated to a vector of length k in fitting.
   * Otherwise, the [[docConcentration]] vector must be length k.
   * (default = automatic)
   *
   * Optimizer-specific parameter settings:
   *  - EM
   *     - Currently only supports symmetric distributions, so all values in the vector should be
   *       the same.
   *     - Values should be > 1.0
   *     - default = uniformly (50 / k) + 1, where 50/k is common in LDA libraries and +1 follows
   *       from Asuncion et al. (2009), who recommend a +1 adjustment for EM.
   *  - Online
   *     - Values should be >= 0
   *     - default = uniformly (1.0 / k), following the implementation from
   *       [[https://github.com/Blei-Lab/onlineldavb]].
   * @group param
   */
  @Since("1.6.0")
  final val docConcentration = new DoubleArrayParam(this, "docConcentration",
    "Concentration parameter (commonly named \"alpha\") for the prior placed on documents'" +
      " distributions over topics (\"theta\").", (alpha: Array[Double]) => alpha.forall(_ >= 0.0))

  /** @group getParam */
  @Since("1.6.0")
  def getDocConcentration: Array[Double] = $(docConcentration)

  /** Get docConcentration used by spark.mllib LDA */
  protected def getOldDocConcentration: Vector = {
    if (isSet(docConcentration)) {
      Vectors.dense(getDocConcentration)
    } else {
      Vectors.dense(-1.0)
    }
  }

  /**
   * Concentration parameter (commonly named "beta" or "eta") for the prior placed on topics'
   * distributions over terms.
   *
   * This is the parameter to a symmetric Dirichlet distribution.
   *
   * Note: The topics' distributions over terms are called "beta" in the original LDA paper
   * by Blei et al., but are called "phi" in many later papers such as Asuncion et al., 2009.
   *
   * If not set by the user, then topicConcentration is set automatically.
   *  (default = automatic)
   *
   * Optimizer-specific parameter settings:
   *  - EM
   *     - Value should be > 1.0
   *     - default = 0.1 + 1, where 0.1 gives a small amount of smoothing and +1 follows
   *       Asuncion et al. (2009), who recommend a +1 adjustment for EM.
   *  - Online
   *     - Value should be >= 0
   *     - default = (1.0 / k), following the implementation from
   *       [[https://github.com/Blei-Lab/onlineldavb]].
   * @group param
   */
  @Since("1.6.0")
  final val topicConcentration = new DoubleParam(this, "topicConcentration",
    "Concentration parameter (commonly named \"beta\" or \"eta\") for the prior placed on topic'" +
      " distributions over terms.", ParamValidators.gtEq(0))

  /** @group getParam */
  @Since("1.6.0")
  def getTopicConcentration: Double = $(topicConcentration)

  /** Get topicConcentration used by spark.mllib LDA */
  protected def getOldTopicConcentration: Double = {
    if (isSet(topicConcentration)) {
      getTopicConcentration
    } else {
      -1.0
    }
  }

  /** Supported values for Param [[optimizer]]. */
  @Since("1.6.0")
  final val supportedOptimizers: Array[String] = Array("online", "em")

  /**
   * Optimizer or inference algorithm used to estimate the LDA model.
   * Currently supported (case-insensitive):
   *  - "online": Online Variational Bayes (default)
   *  - "em": Expectation-Maximization
   *
   * For details, see the following papers:
   *  - Online LDA:
   *     Hoffman, Blei and Bach.  "Online Learning for Latent Dirichlet Allocation."
   *     Neural Information Processing Systems, 2010.
   *     [[http://www.cs.columbia.edu/~blei/papers/HoffmanBleiBach2010b.pdf]]
   *  - EM:
   *     Asuncion et al.  "On Smoothing and Inference for Topic Models."
   *     Uncertainty in Artificial Intelligence, 2009.
   *     [[http://arxiv.org/pdf/1205.2662.pdf]]
   *
   * @group param
   */
  @Since("1.6.0")
  final val optimizer = new Param[String](this, "optimizer", "Optimizer or inference" +
    " algorithm used to estimate the LDA model.  Supported: " + supportedOptimizers.mkString(", "),
    (o: String) => ParamValidators.inArray(supportedOptimizers).apply(o.toLowerCase))

  /** @group getParam */
  @Since("1.6.0")
  def getOptimizer: String = $(optimizer)

  /**
   * Output column with estimates of the topic mixture distribution for each document (often called
   * "theta" in the literature).  Returns a vector of zeros for an empty document.
   *
   * This uses a variational approximation following Hoffman et al. (2010), where the approximate
   * distribution is called "gamma."  Technically, this method returns this approximation "gamma"
   * for each document.
   * @group param
   */
  @Since("1.6.0")
  final val topicDistributionCol = new Param[String](this, "topicDistribution", "Output column" +
    " with estimates of the topic mixture distribution for each document (often called \"theta\"" +
    " in the literature).  Returns a vector of zeros for an empty document.")

  setDefault(topicDistributionCol -> "topicDistribution")

  /** @group getParam */
  @Since("1.6.0")
  def getTopicDistributionCol: String = $(topicDistributionCol)

  /**
   * A (positive) learning parameter that downweights early iterations. Larger values make early
   * iterations count less.
   * This is called "tau0" in the Online LDA paper (Hoffman et al., 2010)
   * Default: 1024, following Hoffman et al.
   * @group expertParam
   */
  @Since("1.6.0")
  final val learningOffset = new DoubleParam(this, "learningOffset", "A (positive) learning" +
    " parameter that downweights early iterations. Larger values make early iterations count less.",
    ParamValidators.gt(0))

  /** @group expertGetParam */
  @Since("1.6.0")
  def getLearningOffset: Double = $(learningOffset)

  /**
   * Learning rate, set as an exponential decay rate.
   * This should be between (0.5, 1.0] to guarantee asymptotic convergence.
   * This is called "kappa" in the Online LDA paper (Hoffman et al., 2010).
   * Default: 0.51, based on Hoffman et al.
   * @group expertParam
   */
  @Since("1.6.0")
  final val learningDecay = new DoubleParam(this, "learningDecay", "Learning rate, set as an" +
    " exponential decay rate. This should be between (0.5, 1.0] to guarantee asymptotic" +
    " convergence.", ParamValidators.gt(0))

  /** @group expertGetParam */
  @Since("1.6.0")
  def getLearningDecay: Double = $(learningDecay)

  /**
   * Fraction of the corpus to be sampled and used in each iteration of mini-batch gradient descent,
   * in range (0, 1].
   *
   * Note that this should be adjusted in synch with [[LDA.maxIter]]
   * so the entire corpus is used.  Specifically, set both so that
   * maxIterations * miniBatchFraction >= 1.
   *
   * Note: This is the same as the `miniBatchFraction` parameter in
   *       [[org.apache.spark.mllib.clustering.OnlineLDAOptimizer]].
   *
   * Default: 0.05, i.e., 5% of total documents.
   * @group param
   */
  @Since("1.6.0")
  final val subsamplingRate = new DoubleParam(this, "subsamplingRate", "Fraction of the corpus" +
    " to be sampled and used in each iteration of mini-batch gradient descent, in range (0, 1].",
    ParamValidators.inRange(0.0, 1.0, lowerInclusive = false, upperInclusive = true))

  /** @group getParam */
  @Since("1.6.0")
  def getSubsamplingRate: Double = $(subsamplingRate)

  /**
   * Indicates whether the docConcentration (Dirichlet parameter for
   * document-topic distribution) will be optimized during training.
   * Setting this to true will make the model more expressive and fit the training data better.
   * Default: false
   * @group expertParam
   */
  @Since("1.6.0")
  final val optimizeDocConcentration = new BooleanParam(this, "optimizeDocConcentration",
    "Indicates whether the docConcentration (Dirichlet parameter for document-topic" +
      " distribution) will be optimized during training.")

  /** @group expertGetParam */
  @Since("1.6.0")
  def getOptimizeDocConcentration: Boolean = $(optimizeDocConcentration)

  /**
   * Validates and transforms the input schema.
   * @param schema input schema
   * @return output schema
   */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    SchemaUtils.appendColumn(schema, $(topicDistributionCol), new VectorUDT)
  }

  @Since("1.6.0")
  override def validateParams(): Unit = {
    if (isSet(docConcentration)) {
      if (getDocConcentration.length != 1) {
        require(getDocConcentration.length == getK, s"LDA docConcentration was of length" +
          s" ${getDocConcentration.length}, but k = $getK.  docConcentration must be an array of" +
          s" length either 1 (scalar) or k (num topics).")
      }
      getOptimizer match {
        case "online" =>
          require(getDocConcentration.forall(_ >= 0),
            "For Online LDA optimizer, docConcentration values must be >= 0.  Found values: " +
              getDocConcentration.mkString(","))
        case "em" =>
          require(getDocConcentration.forall(_ >= 0),
            "For EM optimizer, docConcentration values must be >= 1.  Found values: " +
              getDocConcentration.mkString(","))
      }
    }
    if (isSet(topicConcentration)) {
      getOptimizer match {
        case "online" =>
          require(getTopicConcentration >= 0, s"For Online LDA optimizer, topicConcentration" +
            s" must be >= 0.  Found value: $getTopicConcentration")
        case "em" =>
          require(getTopicConcentration >= 0, s"For EM optimizer, topicConcentration" +
            s" must be >= 1.  Found value: $getTopicConcentration")
      }
    }
  }

  private[clustering] def getOldOptimizer: OldLDAOptimizer = getOptimizer match {
    case "online" =>
      new OldOnlineLDAOptimizer()
        .setTau0($(learningOffset))
        .setKappa($(learningDecay))
        .setMiniBatchFraction($(subsamplingRate))
        .setOptimizeDocConcentration($(optimizeDocConcentration))
    case "em" =>
      new OldEMLDAOptimizer()
  }
}


/**
 * :: Experimental ::
 * Model fitted by [[LDA]].
 *
 * @param vocabSize  Vocabulary size (number of terms or terms in the vocabulary)
 * @param sqlContext  Used to construct local DataFrames for returning query results
 */
@Since("1.6.0")
@Experimental
sealed abstract class LDAModel private[ml] (
    @Since("1.6.0") override val uid: String,
    @Since("1.6.0") val vocabSize: Int,
    @Since("1.6.0") @transient protected val sqlContext: SQLContext)
  extends Model[LDAModel] with LDAParams with Logging with MLWritable {

  // NOTE to developers:
  //  This abstraction should contain all important functionality for basic LDA usage.
  //  Specializations of this class can contain expert-only functionality.

  /**
   * Underlying spark.mllib model.
   * If this model was produced by Online LDA, then this is the only model representation.
   * If this model was produced by EM, then this local representation may be built lazily.
   */
  @Since("1.6.0")
  protected def oldLocalModel: OldLocalLDAModel

  /** Returns underlying spark.mllib model, which may be local or distributed */
  @Since("1.6.0")
  protected def getModel: OldLDAModel

  /**
   * The features for LDA should be a [[Vector]] representing the word counts in a document.
   * The vector should be of length vocabSize, with counts for each term (word).
   * @group setParam
   */
  @Since("1.6.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("1.6.0")
  def setSeed(value: Long): this.type = set(seed, value)

  /**
   * Transforms the input dataset.
   *
   * WARNING: If this model is an instance of [[DistributedLDAModel]] (produced when [[optimizer]]
   *          is set to "em"), this involves collecting a large [[topicsMatrix]] to the driver.
   *          This implementation may be changed in the future.
   */
  @Since("1.6.0")
  override def transform(dataset: DataFrame): DataFrame = {
    if ($(topicDistributionCol).nonEmpty) {
      val t = udf(oldLocalModel.getTopicDistributionMethod(sqlContext.sparkContext))
      dataset.withColumn($(topicDistributionCol), t(col($(featuresCol))))
    } else {
      logWarning("LDAModel.transform was called without any output columns. Set an output column" +
        " such as topicDistributionCol to produce results.")
      dataset
    }
  }

  @Since("1.6.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  /**
   * Value for [[docConcentration]] estimated from data.
   * If Online LDA was used and [[optimizeDocConcentration]] was set to false,
   * then this returns the fixed (given) value for the [[docConcentration]] parameter.
   */
  @Since("1.6.0")
  def estimatedDocConcentration: Vector = getModel.docConcentration

  /**
   * Inferred topics, where each topic is represented by a distribution over terms.
   * This is a matrix of size vocabSize x k, where each column is a topic.
   * No guarantees are given about the ordering of the topics.
   *
   * WARNING: If this model is actually a [[DistributedLDAModel]] instance produced by
   *          the Expectation-Maximization ("em") [[optimizer]], then this method could involve
   *          collecting a large amount of data to the driver (on the order of vocabSize x k).
   */
  @Since("1.6.0")
  def topicsMatrix: Matrix = oldLocalModel.topicsMatrix

  /** Indicates whether this instance is of type [[DistributedLDAModel]] */
  @Since("1.6.0")
  def isDistributed: Boolean

  /**
   * Calculates a lower bound on the log likelihood of the entire corpus.
   *
   * See Equation (16) in the Online LDA paper (Hoffman et al., 2010).
   *
   * WARNING: If this model is an instance of [[DistributedLDAModel]] (produced when [[optimizer]]
   *          is set to "em"), this involves collecting a large [[topicsMatrix]] to the driver.
   *          This implementation may be changed in the future.
   *
   * @param dataset  test corpus to use for calculating log likelihood
   * @return variational lower bound on the log likelihood of the entire corpus
   */
  @Since("1.6.0")
  def logLikelihood(dataset: DataFrame): Double = {
    val oldDataset = LDA.getOldDataset(dataset, $(featuresCol))
    oldLocalModel.logLikelihood(oldDataset)
  }

  /**
   * Calculate an upper bound bound on perplexity.  (Lower is better.)
   * See Equation (16) in the Online LDA paper (Hoffman et al., 2010).
   *
   * WARNING: If this model is an instance of [[DistributedLDAModel]] (produced when [[optimizer]]
   *          is set to "em"), this involves collecting a large [[topicsMatrix]] to the driver.
   *          This implementation may be changed in the future.
   *
   * @param dataset test corpus to use for calculating perplexity
   * @return Variational upper bound on log perplexity per token.
   */
  @Since("1.6.0")
  def logPerplexity(dataset: DataFrame): Double = {
    val oldDataset = LDA.getOldDataset(dataset, $(featuresCol))
    oldLocalModel.logPerplexity(oldDataset)
  }

  /**
   * Return the topics described by their top-weighted terms.
   *
   * @param maxTermsPerTopic  Maximum number of terms to collect for each topic.
   *                          Default value of 10.
   * @return  Local DataFrame with one topic per Row, with columns:
   *           - "topic": IntegerType: topic index
   *           - "termIndices": ArrayType(IntegerType): term indices, sorted in order of decreasing
   *                            term importance
   *           - "termWeights": ArrayType(DoubleType): corresponding sorted term weights
   */
  @Since("1.6.0")
  def describeTopics(maxTermsPerTopic: Int): DataFrame = {
    val topics = getModel.describeTopics(maxTermsPerTopic).zipWithIndex.map {
      case ((termIndices, termWeights), topic) =>
        (topic, termIndices.toSeq, termWeights.toSeq)
    }
    sqlContext.createDataFrame(topics).toDF("topic", "termIndices", "termWeights")
  }

  @Since("1.6.0")
  def describeTopics(): DataFrame = describeTopics(10)
}


/**
 * :: Experimental ::
 *
 * Local (non-distributed) model fitted by [[LDA]].
 *
 * This model stores the inferred topics only; it does not store info about the training dataset.
 */
@Since("1.6.0")
@Experimental
class LocalLDAModel private[ml] (
    uid: String,
    vocabSize: Int,
    @Since("1.6.0") override protected val oldLocalModel: OldLocalLDAModel,
    sqlContext: SQLContext)
  extends LDAModel(uid, vocabSize, sqlContext) {

  @Since("1.6.0")
  override def copy(extra: ParamMap): LocalLDAModel = {
    val copied = new LocalLDAModel(uid, vocabSize, oldLocalModel, sqlContext)
    copyValues(copied, extra).setParent(parent).asInstanceOf[LocalLDAModel]
  }

  override protected def getModel: OldLDAModel = oldLocalModel

  @Since("1.6.0")
  override def isDistributed: Boolean = false

  @Since("1.6.0")
  override def write: MLWriter = new LocalLDAModel.LocalLDAModelWriter(this)
}


@Since("1.6.0")
object LocalLDAModel extends MLReadable[LocalLDAModel] {

  private[LocalLDAModel]
  class LocalLDAModelWriter(instance: LocalLDAModel) extends MLWriter {

    private case class Data(
        vocabSize: Int,
        topicsMatrix: Matrix,
        docConcentration: Vector,
        topicConcentration: Double,
        gammaShape: Double)

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val oldModel = instance.oldLocalModel
      val data = Data(instance.vocabSize, oldModel.topicsMatrix, oldModel.docConcentration,
        oldModel.topicConcentration, oldModel.gammaShape)
      val dataPath = new Path(path, "data").toString
      sqlContext.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class LocalLDAModelReader extends MLReader[LocalLDAModel] {

    private val className = classOf[LocalLDAModel].getName

    override def load(path: String): LocalLDAModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sqlContext.read.parquet(dataPath)
        .select("vocabSize", "topicsMatrix", "docConcentration", "topicConcentration",
          "gammaShape")
        .head()
      val vocabSize = data.getAs[Int](0)
      val topicsMatrix = data.getAs[Matrix](1)
      val docConcentration = data.getAs[Vector](2)
      val topicConcentration = data.getAs[Double](3)
      val gammaShape = data.getAs[Double](4)
      val oldModel = new OldLocalLDAModel(topicsMatrix, docConcentration, topicConcentration,
        gammaShape)
      val model = new LocalLDAModel(metadata.uid, vocabSize, oldModel, sqlContext)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  @Since("1.6.0")
  override def read: MLReader[LocalLDAModel] = new LocalLDAModelReader

  @Since("1.6.0")
  override def load(path: String): LocalLDAModel = super.load(path)
}


/**
 * :: Experimental ::
 *
 * Distributed model fitted by [[LDA]].
 * This type of model is currently only produced by Expectation-Maximization (EM).
 *
 * This model stores the inferred topics, the full training dataset, and the topic distribution
 * for each training document.
 *
 * @param oldLocalModelOption  Used to implement [[oldLocalModel]] as a lazy val, but keeping
 *                             [[copy()]] cheap.
 */
@Since("1.6.0")
@Experimental
class DistributedLDAModel private[ml] (
    uid: String,
    vocabSize: Int,
    private val oldDistributedModel: OldDistributedLDAModel,
    sqlContext: SQLContext,
    private var oldLocalModelOption: Option[OldLocalLDAModel])
  extends LDAModel(uid, vocabSize, sqlContext) {

  override protected def oldLocalModel: OldLocalLDAModel = {
    if (oldLocalModelOption.isEmpty) {
      oldLocalModelOption = Some(oldDistributedModel.toLocal)
    }
    oldLocalModelOption.get
  }

  override protected def getModel: OldLDAModel = oldDistributedModel

  /**
   * Convert this distributed model to a local representation.  This discards info about the
   * training dataset.
   *
   * WARNING: This involves collecting a large [[topicsMatrix]] to the driver.
   */
  @Since("1.6.0")
  def toLocal: LocalLDAModel = new LocalLDAModel(uid, vocabSize, oldLocalModel, sqlContext)

  @Since("1.6.0")
  override def copy(extra: ParamMap): DistributedLDAModel = {
    val copied =
      new DistributedLDAModel(uid, vocabSize, oldDistributedModel, sqlContext, oldLocalModelOption)
    copyValues(copied, extra).setParent(parent)
    copied
  }

  @Since("1.6.0")
  override def isDistributed: Boolean = true

  /**
   * Log likelihood of the observed tokens in the training set,
   * given the current parameter estimates:
   *  log P(docs | topics, topic distributions for docs, Dirichlet hyperparameters)
   *
   * Notes:
   *  - This excludes the prior; for that, use [[logPrior]].
   *  - Even with [[logPrior]], this is NOT the same as the data log likelihood given the
   *    hyperparameters.
   *  - This is computed from the topic distributions computed during training. If you call
   *    [[logLikelihood()]] on the same training dataset, the topic distributions will be computed
   *    again, possibly giving different results.
   */
  @Since("1.6.0")
  lazy val trainingLogLikelihood: Double = oldDistributedModel.logLikelihood

  /**
   * Log probability of the current parameter estimate:
   * log P(topics, topic distributions for docs | Dirichlet hyperparameters)
   */
  @Since("1.6.0")
  lazy val logPrior: Double = oldDistributedModel.logPrior

  @Since("1.6.0")
  override def write: MLWriter = new DistributedLDAModel.DistributedWriter(this)
}


@Since("1.6.0")
object DistributedLDAModel extends MLReadable[DistributedLDAModel] {

  private[DistributedLDAModel]
  class DistributedWriter(instance: DistributedLDAModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val modelPath = new Path(path, "oldModel").toString
      instance.oldDistributedModel.save(sc, modelPath)
    }
  }

  private class DistributedLDAModelReader extends MLReader[DistributedLDAModel] {

    private val className = classOf[DistributedLDAModel].getName

    override def load(path: String): DistributedLDAModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val modelPath = new Path(path, "oldModel").toString
      val oldModel = OldDistributedLDAModel.load(sc, modelPath)
      val model = new DistributedLDAModel(
        metadata.uid, oldModel.vocabSize, oldModel, sqlContext, None)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  @Since("1.6.0")
  override def read: MLReader[DistributedLDAModel] = new DistributedLDAModelReader

  @Since("1.6.0")
  override def load(path: String): DistributedLDAModel = super.load(path)
}


/**
 * :: Experimental ::
 *
 * Latent Dirichlet Allocation (LDA), a topic model designed for text documents.
 *
 * Terminology:
 *  - "term" = "word": an element of the vocabulary
 *  - "token": instance of a term appearing in a document
 *  - "topic": multinomial distribution over terms representing some concept
 *  - "document": one piece of text, corresponding to one row in the input data
 *
 * References:
 *  - Original LDA paper (journal version):
 *    Blei, Ng, and Jordan.  "Latent Dirichlet Allocation."  JMLR, 2003.
 *
 * Input data (featuresCol):
 *  LDA is given a collection of documents as input data, via the featuresCol parameter.
 *  Each document is specified as a [[Vector]] of length vocabSize, where each entry is the
 *  count for the corresponding term (word) in the document.  Feature transformers such as
 *  [[org.apache.spark.ml.feature.Tokenizer]] and [[org.apache.spark.ml.feature.CountVectorizer]]
 *  can be useful for converting text to word count vectors.
 *
 * @see [[http://en.wikipedia.org/wiki/Latent_Dirichlet_allocation Latent Dirichlet allocation
 *       (Wikipedia)]]
 */
@Since("1.6.0")
@Experimental
class LDA @Since("1.6.0") (
    @Since("1.6.0") override val uid: String)
  extends Estimator[LDAModel] with LDAParams with DefaultParamsWritable {

  @Since("1.6.0")
  def this() = this(Identifiable.randomUID("lda"))

  setDefault(maxIter -> 20, k -> 10, optimizer -> "online", checkpointInterval -> 10,
    learningOffset -> 1024, learningDecay -> 0.51, subsamplingRate -> 0.05,
    optimizeDocConcentration -> true)

  /**
   * The features for LDA should be a [[Vector]] representing the word counts in a document.
   * The vector should be of length vocabSize, with counts for each term (word).
   * @group setParam
   */
  @Since("1.6.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("1.6.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /** @group setParam */
  @Since("1.6.0")
  def setSeed(value: Long): this.type = set(seed, value)

  /** @group setParam */
  @Since("1.6.0")
  def setCheckpointInterval(value: Int): this.type = set(checkpointInterval, value)

  /** @group setParam */
  @Since("1.6.0")
  def setK(value: Int): this.type = set(k, value)

  /** @group setParam */
  @Since("1.6.0")
  def setDocConcentration(value: Array[Double]): this.type = set(docConcentration, value)

  /** @group setParam */
  @Since("1.6.0")
  def setDocConcentration(value: Double): this.type = set(docConcentration, Array(value))

  /** @group setParam */
  @Since("1.6.0")
  def setTopicConcentration(value: Double): this.type = set(topicConcentration, value)

  /** @group setParam */
  @Since("1.6.0")
  def setOptimizer(value: String): this.type = set(optimizer, value)

  /** @group setParam */
  @Since("1.6.0")
  def setTopicDistributionCol(value: String): this.type = set(topicDistributionCol, value)

  /** @group expertSetParam */
  @Since("1.6.0")
  def setLearningOffset(value: Double): this.type = set(learningOffset, value)

  /** @group expertSetParam */
  @Since("1.6.0")
  def setLearningDecay(value: Double): this.type = set(learningDecay, value)

  /** @group setParam */
  @Since("1.6.0")
  def setSubsamplingRate(value: Double): this.type = set(subsamplingRate, value)

  /** @group expertSetParam */
  @Since("1.6.0")
  def setOptimizeDocConcentration(value: Boolean): this.type = set(optimizeDocConcentration, value)

  @Since("1.6.0")
  override def copy(extra: ParamMap): LDA = defaultCopy(extra)

  @Since("1.6.0")
  override def fit(dataset: DataFrame): LDAModel = {
    transformSchema(dataset.schema, logging = true)
    val oldLDA = new OldLDA()
      .setK($(k))
      .setDocConcentration(getOldDocConcentration)
      .setTopicConcentration(getOldTopicConcentration)
      .setMaxIterations($(maxIter))
      .setSeed($(seed))
      .setCheckpointInterval($(checkpointInterval))
      .setOptimizer(getOldOptimizer)
    // TODO: persist here, or in old LDA?
    val oldData = LDA.getOldDataset(dataset, $(featuresCol))
    val oldModel = oldLDA.run(oldData)
    val newModel = oldModel match {
      case m: OldLocalLDAModel =>
        new LocalLDAModel(uid, m.vocabSize, m, dataset.sqlContext)
      case m: OldDistributedLDAModel =>
        new DistributedLDAModel(uid, m.vocabSize, m, dataset.sqlContext, None)
    }
    copyValues(newModel).setParent(this)
  }

  @Since("1.6.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}


private[clustering] object LDA extends DefaultParamsReadable[LDA] {

  /** Get dataset for spark.mllib LDA */
  def getOldDataset(dataset: DataFrame, featuresCol: String): RDD[(Long, Vector)] = {
    dataset
      .withColumn("docId", monotonicallyIncreasingId())
      .select("docId", featuresCol)
      .map { case Row(docId: Long, features: Vector) =>
        (docId, features)
      }
  }

  @Since("1.6.0")
  override def load(path: String): LDA = super.load(path)
}

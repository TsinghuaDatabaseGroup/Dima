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

package org.apache.spark.mllib.tree

import java.io.IOException

import scala.collection.mutable
import scala.collection.JavaConverters._

import org.apache.spark.Logging
import org.apache.spark.annotation.Since
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.QuantileStrategy._
import org.apache.spark.mllib.tree.impl.{BaggedPoint, DecisionTreeMetadata, NodeIdCache,
  TimeTracker, TreePoint}
import org.apache.spark.mllib.tree.impurity.Impurities
import org.apache.spark.mllib.tree.model._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils
import org.apache.spark.util.random.SamplingUtils

/**
 * A class that implements a [[http://en.wikipedia.org/wiki/Random_forest  Random Forest]]
 * learning algorithm for classification and regression.
 * It supports both continuous and categorical features.
 *
 * The settings for featureSubsetStrategy are based on the following references:
 *  - log2: tested in Breiman (2001)
 *  - sqrt: recommended by Breiman manual for random forests
 *  - The defaults of sqrt (classification) and onethird (regression) match the R randomForest
 *    package.
 * @see [[http://www.stat.berkeley.edu/~breiman/randomforest2001.pdf  Breiman (2001)]]
 * @see [[http://www.stat.berkeley.edu/~breiman/Using_random_forests_V3.1.pdf  Breiman manual for
 *     random forests]]
 *
 * @param strategy The configuration parameters for the random forest algorithm which specify
 *                 the type of algorithm (classification, regression, etc.), feature type
 *                 (continuous, categorical), depth of the tree, quantile calculation strategy,
 *                 etc.
 * @param numTrees If 1, then no bootstrapping is used.  If > 1, then bootstrapping is done.
 * @param featureSubsetStrategy Number of features to consider for splits at each node.
 *                              Supported: "auto", "all", "sqrt", "log2", "onethird".
 *                              If "auto" is set, this parameter is set based on numTrees:
 *                                if numTrees == 1, set to "all";
 *                                if numTrees > 1 (forest) set to "sqrt" for classification and
 *                                  to "onethird" for regression.
 * @param seed Random seed for bootstrapping and choosing feature subsets.
 */
private class RandomForest (
    private val strategy: Strategy,
    private val numTrees: Int,
    featureSubsetStrategy: String,
    private val seed: Int)
  extends Serializable with Logging {

  /*
     ALGORITHM
     This is a sketch of the algorithm to help new developers.

     The algorithm partitions data by instances (rows).
     On each iteration, the algorithm splits a set of nodes.  In order to choose the best split
     for a given node, sufficient statistics are collected from the distributed data.
     For each node, the statistics are collected to some worker node, and that worker selects
     the best split.

     This setup requires discretization of continuous features.  This binning is done in the
     findSplitsBins() method during initialization, after which each continuous feature becomes
     an ordered discretized feature with at most maxBins possible values.

     The main loop in the algorithm operates on a queue of nodes (nodeQueue).  These nodes
     lie at the periphery of the tree being trained.  If multiple trees are being trained at once,
     then this queue contains nodes from all of them.  Each iteration works roughly as follows:
       On the master node:
         - Some number of nodes are pulled off of the queue (based on the amount of memory
           required for their sufficient statistics).
         - For random forests, if featureSubsetStrategy is not "all," then a subset of candidate
           features are chosen for each node.  See method selectNodesToSplit().
       On worker nodes, via method findBestSplits():
         - The worker makes one pass over its subset of instances.
         - For each (tree, node, feature, split) tuple, the worker collects statistics about
           splitting.  Note that the set of (tree, node) pairs is limited to the nodes selected
           from the queue for this iteration.  The set of features considered can also be limited
           based on featureSubsetStrategy.
         - For each node, the statistics for that node are aggregated to a particular worker
           via reduceByKey().  The designated worker chooses the best (feature, split) pair,
           or chooses to stop splitting if the stopping criteria are met.
       On the master node:
         - The master collects all decisions about splitting nodes and updates the model.
         - The updated model is passed to the workers on the next iteration.
     This process continues until the node queue is empty.

     Most of the methods in this implementation support the statistics aggregation, which is
     the heaviest part of the computation.  In general, this implementation is bound by either
     the cost of statistics computation on workers or by communicating the sufficient statistics.
   */

  strategy.assertValid()
  require(numTrees > 0, s"RandomForest requires numTrees > 0, but was given numTrees = $numTrees.")
  require(RandomForest.supportedFeatureSubsetStrategies.contains(featureSubsetStrategy),
    s"RandomForest given invalid featureSubsetStrategy: $featureSubsetStrategy." +
    s" Supported values: ${RandomForest.supportedFeatureSubsetStrategies.mkString(", ")}.")

  /**
   * Method to train a decision tree model over an RDD
   * @param input Training data: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]]
   * @return a random forest model that can be used for prediction
   */
  def run(input: RDD[LabeledPoint]): RandomForestModel = {

    val timer = new TimeTracker()

    timer.start("total")

    timer.start("init")

    val retaggedInput = input.retag(classOf[LabeledPoint])
    val metadata =
      DecisionTreeMetadata.buildMetadata(retaggedInput, strategy, numTrees, featureSubsetStrategy)
    logDebug("algo = " + strategy.algo)
    logDebug("numTrees = " + numTrees)
    logDebug("seed = " + seed)
    logDebug("maxBins = " + metadata.maxBins)
    logDebug("featureSubsetStrategy = " + featureSubsetStrategy)
    logDebug("numFeaturesPerNode = " + metadata.numFeaturesPerNode)
    logDebug("subsamplingRate = " + strategy.subsamplingRate)

    // Find the splits and the corresponding bins (interval between the splits) using a sample
    // of the input data.
    timer.start("findSplitsBins")
    val (splits, bins) = DecisionTree.findSplitsBins(retaggedInput, metadata)
    timer.stop("findSplitsBins")
    logDebug("numBins: feature: number of bins")
    logDebug(Range(0, metadata.numFeatures).map { featureIndex =>
        s"\t$featureIndex\t${metadata.numBins(featureIndex)}"
      }.mkString("\n"))

    // Bin feature values (TreePoint representation).
    // Cache input RDD for speedup during multiple passes.
    val treeInput = TreePoint.convertToTreeRDD(retaggedInput, bins, metadata)

    val withReplacement = if (numTrees > 1) true else false

    val baggedInput
      = BaggedPoint.convertToBaggedRDD(treeInput,
          strategy.subsamplingRate, numTrees,
          withReplacement, seed).persist(StorageLevel.MEMORY_AND_DISK)

    // depth of the decision tree
    val maxDepth = strategy.maxDepth
    require(maxDepth <= 30,
      s"DecisionTree currently only supports maxDepth <= 30, but was given maxDepth = $maxDepth.")

    // Max memory usage for aggregates
    // TODO: Calculate memory usage more precisely.
    val maxMemoryUsage: Long = strategy.maxMemoryInMB * 1024L * 1024L
    logDebug("max memory usage for aggregates = " + maxMemoryUsage + " bytes.")
    val maxMemoryPerNode = {
      val featureSubset: Option[Array[Int]] = if (metadata.subsamplingFeatures) {
        // Find numFeaturesPerNode largest bins to get an upper bound on memory usage.
        Some(metadata.numBins.zipWithIndex.sortBy(- _._1)
          .take(metadata.numFeaturesPerNode).map(_._2))
      } else {
        None
      }
      RandomForest.aggregateSizeForNode(metadata, featureSubset) * 8L
    }
    require(maxMemoryPerNode <= maxMemoryUsage,
      s"RandomForest/DecisionTree given maxMemoryInMB = ${strategy.maxMemoryInMB}," +
      " which is too small for the given features." +
      s"  Minimum value = ${maxMemoryPerNode / (1024L * 1024L)}")

    timer.stop("init")

    /*
     * The main idea here is to perform group-wise training of the decision tree nodes thus
     * reducing the passes over the data from (# nodes) to (# nodes / maxNumberOfNodesPerGroup).
     * Each data sample is handled by a particular node (or it reaches a leaf and is not used
     * in lower levels).
     */

    // Create an RDD of node Id cache.
    // At first, all the rows belong to the root nodes (node Id == 1).
    val nodeIdCache = if (strategy.useNodeIdCache) {
      Some(NodeIdCache.init(
        data = baggedInput,
        numTrees = numTrees,
        checkpointInterval = strategy.checkpointInterval,
        initVal = 1))
    } else {
      None
    }

    // FIFO queue of nodes to train: (treeIndex, node)
    val nodeQueue = new mutable.Queue[(Int, Node)]()

    val rng = new scala.util.Random()
    rng.setSeed(seed)

    // Allocate and queue root nodes.
    val topNodes: Array[Node] = Array.fill[Node](numTrees)(Node.emptyNode(nodeIndex = 1))
    Range(0, numTrees).foreach(treeIndex => nodeQueue.enqueue((treeIndex, topNodes(treeIndex))))

    while (nodeQueue.nonEmpty) {
      // Collect some nodes to split, and choose features for each node (if subsampling).
      // Each group of nodes may come from one or multiple trees, and at multiple levels.
      val (nodesForGroup, treeToNodeToIndexInfo) =
        RandomForest.selectNodesToSplit(nodeQueue, maxMemoryUsage, metadata, rng)
      // Sanity check (should never occur):
      assert(nodesForGroup.size > 0,
        s"RandomForest selected empty nodesForGroup.  Error for unknown reason.")

      // Choose node splits, and enqueue new nodes as needed.
      timer.start("findBestSplits")
      DecisionTree.findBestSplits(baggedInput, metadata, topNodes, nodesForGroup,
        treeToNodeToIndexInfo, splits, bins, nodeQueue, timer, nodeIdCache = nodeIdCache)
      timer.stop("findBestSplits")
    }

    baggedInput.unpersist()

    timer.stop("total")

    logInfo("Internal timing for DecisionTree:")
    logInfo(s"$timer")

    // Delete any remaining checkpoints used for node Id cache.
    if (nodeIdCache.nonEmpty) {
      try {
        nodeIdCache.get.deleteAllCheckpoints()
      } catch {
        case e: IOException =>
          logWarning(s"delete all checkpoints failed. Error reason: ${e.getMessage}")
      }
    }

    val trees = topNodes.map(topNode => new DecisionTreeModel(topNode, strategy.algo))
    new RandomForestModel(strategy.algo, trees)
  }

}

@Since("1.2.0")
object RandomForest extends Serializable with Logging {

  /**
   * Method to train a decision tree model for binary or multiclass classification.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              Labels should take values {0, 1, ..., numClasses-1}.
   * @param strategy Parameters for training each tree in the forest.
   * @param numTrees Number of trees in the random forest.
   * @param featureSubsetStrategy Number of features to consider for splits at each node.
   *                              Supported: "auto", "all", "sqrt", "log2", "onethird".
   *                              If "auto" is set, this parameter is set based on numTrees:
   *                                if numTrees == 1, set to "all";
   *                                if numTrees > 1 (forest) set to "sqrt".
   * @param seed  Random seed for bootstrapping and choosing feature subsets.
   * @return a random forest model that can be used for prediction
   */
  @Since("1.2.0")
  def trainClassifier(
      input: RDD[LabeledPoint],
      strategy: Strategy,
      numTrees: Int,
      featureSubsetStrategy: String,
      seed: Int): RandomForestModel = {
    require(strategy.algo == Classification,
      s"RandomForest.trainClassifier given Strategy with invalid algo: ${strategy.algo}")
    val rf = new RandomForest(strategy, numTrees, featureSubsetStrategy, seed)
    rf.run(input)
  }

  /**
   * Method to train a decision tree model for binary or multiclass classification.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              Labels should take values {0, 1, ..., numClasses-1}.
   * @param numClasses number of classes for classification.
   * @param categoricalFeaturesInfo Map storing arity of categorical features.
   *                                E.g., an entry (n -> k) indicates that feature n is categorical
   *                                with k categories indexed from 0: {0, 1, ..., k-1}.
   * @param numTrees Number of trees in the random forest.
   * @param featureSubsetStrategy Number of features to consider for splits at each node.
   *                              Supported: "auto", "all", "sqrt", "log2", "onethird".
   *                              If "auto" is set, this parameter is set based on numTrees:
   *                                if numTrees == 1, set to "all";
   *                                if numTrees > 1 (forest) set to "sqrt".
   * @param impurity Criterion used for information gain calculation.
   *                 Supported values: "gini" (recommended) or "entropy".
   * @param maxDepth Maximum depth of the tree.
   *                 E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
   *                  (suggested value: 4)
   * @param maxBins maximum number of bins used for splitting features
   *                 (suggested value: 100)
   * @param seed  Random seed for bootstrapping and choosing feature subsets.
   * @return a random forest model  that can be used for prediction
   */
  @Since("1.2.0")
  def trainClassifier(
      input: RDD[LabeledPoint],
      numClasses: Int,
      categoricalFeaturesInfo: Map[Int, Int],
      numTrees: Int,
      featureSubsetStrategy: String,
      impurity: String,
      maxDepth: Int,
      maxBins: Int,
      seed: Int = Utils.random.nextInt()): RandomForestModel = {
    val impurityType = Impurities.fromString(impurity)
    val strategy = new Strategy(Classification, impurityType, maxDepth,
      numClasses, maxBins, Sort, categoricalFeaturesInfo)
    trainClassifier(input, strategy, numTrees, featureSubsetStrategy, seed)
  }

  /**
   * Java-friendly API for [[org.apache.spark.mllib.tree.RandomForest$#trainClassifier]]
   */
  @Since("1.2.0")
  def trainClassifier(
      input: JavaRDD[LabeledPoint],
      numClasses: Int,
      categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer],
      numTrees: Int,
      featureSubsetStrategy: String,
      impurity: String,
      maxDepth: Int,
      maxBins: Int,
      seed: Int): RandomForestModel = {
    trainClassifier(input.rdd, numClasses,
      categoricalFeaturesInfo.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, seed)
  }

  /**
   * Method to train a decision tree model for regression.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              Labels are real numbers.
   * @param strategy Parameters for training each tree in the forest.
   * @param numTrees Number of trees in the random forest.
   * @param featureSubsetStrategy Number of features to consider for splits at each node.
   *                              Supported: "auto", "all", "sqrt", "log2", "onethird".
   *                              If "auto" is set, this parameter is set based on numTrees:
   *                                if numTrees == 1, set to "all";
   *                                if numTrees > 1 (forest) set to "onethird".
   * @param seed  Random seed for bootstrapping and choosing feature subsets.
   * @return a random forest model that can be used for prediction
   */
  @Since("1.2.0")
  def trainRegressor(
      input: RDD[LabeledPoint],
      strategy: Strategy,
      numTrees: Int,
      featureSubsetStrategy: String,
      seed: Int): RandomForestModel = {
    require(strategy.algo == Regression,
      s"RandomForest.trainRegressor given Strategy with invalid algo: ${strategy.algo}")
    val rf = new RandomForest(strategy, numTrees, featureSubsetStrategy, seed)
    rf.run(input)
  }

  /**
   * Method to train a decision tree model for regression.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              Labels are real numbers.
   * @param categoricalFeaturesInfo Map storing arity of categorical features.
   *                                E.g., an entry (n -> k) indicates that feature n is categorical
   *                                with k categories indexed from 0: {0, 1, ..., k-1}.
   * @param numTrees Number of trees in the random forest.
   * @param featureSubsetStrategy Number of features to consider for splits at each node.
   *                              Supported: "auto", "all", "sqrt", "log2", "onethird".
   *                              If "auto" is set, this parameter is set based on numTrees:
   *                                if numTrees == 1, set to "all";
   *                                if numTrees > 1 (forest) set to "onethird".
   * @param impurity Criterion used for information gain calculation.
   *                 Supported values: "variance".
   * @param maxDepth Maximum depth of the tree.
   *                 E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
   *                  (suggested value: 4)
   * @param maxBins maximum number of bins used for splitting features
   *                 (suggested value: 100)
   * @param seed  Random seed for bootstrapping and choosing feature subsets.
   * @return a random forest model that can be used for prediction
   */
  @Since("1.2.0")
  def trainRegressor(
      input: RDD[LabeledPoint],
      categoricalFeaturesInfo: Map[Int, Int],
      numTrees: Int,
      featureSubsetStrategy: String,
      impurity: String,
      maxDepth: Int,
      maxBins: Int,
      seed: Int = Utils.random.nextInt()): RandomForestModel = {
    val impurityType = Impurities.fromString(impurity)
    val strategy = new Strategy(Regression, impurityType, maxDepth,
      0, maxBins, Sort, categoricalFeaturesInfo)
    trainRegressor(input, strategy, numTrees, featureSubsetStrategy, seed)
  }

  /**
   * Java-friendly API for [[org.apache.spark.mllib.tree.RandomForest$#trainRegressor]]
   */
  @Since("1.2.0")
  def trainRegressor(
      input: JavaRDD[LabeledPoint],
      categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer],
      numTrees: Int,
      featureSubsetStrategy: String,
      impurity: String,
      maxDepth: Int,
      maxBins: Int,
      seed: Int): RandomForestModel = {
    trainRegressor(input.rdd,
      categoricalFeaturesInfo.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, seed)
  }

  /**
   * List of supported feature subset sampling strategies.
   */
  @Since("1.2.0")
  val supportedFeatureSubsetStrategies: Array[String] =
    Array("auto", "all", "sqrt", "log2", "onethird")

  private[tree] class NodeIndexInfo(
      val nodeIndexInGroup: Int,
      val featureSubset: Option[Array[Int]]) extends Serializable

  /**
   * Pull nodes off of the queue, and collect a group of nodes to be split on this iteration.
   * This tracks the memory usage for aggregates and stops adding nodes when too much memory
   * will be needed; this allows an adaptive number of nodes since different nodes may require
   * different amounts of memory (if featureSubsetStrategy is not "all").
   *
   * @param nodeQueue  Queue of nodes to split.
   * @param maxMemoryUsage  Bound on size of aggregate statistics.
   * @return  (nodesForGroup, treeToNodeToIndexInfo).
   *          nodesForGroup holds the nodes to split: treeIndex --> nodes in tree.
   *
   *          treeToNodeToIndexInfo holds indices selected features for each node:
   *            treeIndex --> (global) node index --> (node index in group, feature indices).
   *          The (global) node index is the index in the tree; the node index in group is the
   *           index in [0, numNodesInGroup) of the node in this group.
   *          The feature indices are None if not subsampling features.
   */
  private[tree] def selectNodesToSplit(
      nodeQueue: mutable.Queue[(Int, Node)],
      maxMemoryUsage: Long,
      metadata: DecisionTreeMetadata,
      rng: scala.util.Random): (Map[Int, Array[Node]], Map[Int, Map[Int, NodeIndexInfo]]) = {
    // Collect some nodes to split:
    //  nodesForGroup(treeIndex) = nodes to split
    val mutableNodesForGroup = new mutable.HashMap[Int, mutable.ArrayBuffer[Node]]()
    val mutableTreeToNodeToIndexInfo =
      new mutable.HashMap[Int, mutable.HashMap[Int, NodeIndexInfo]]()
    var memUsage: Long = 0L
    var numNodesInGroup = 0
    while (nodeQueue.nonEmpty && memUsage < maxMemoryUsage) {
      val (treeIndex, node) = nodeQueue.head
      // Choose subset of features for node (if subsampling).
      val featureSubset: Option[Array[Int]] = if (metadata.subsamplingFeatures) {
        Some(SamplingUtils.reservoirSampleAndCount(Range(0,
          metadata.numFeatures).iterator, metadata.numFeaturesPerNode, rng.nextLong)._1)
      } else {
        None
      }
      // Check if enough memory remains to add this node to the group.
      val nodeMemUsage = RandomForest.aggregateSizeForNode(metadata, featureSubset) * 8L
      if (memUsage + nodeMemUsage <= maxMemoryUsage) {
        nodeQueue.dequeue()
        mutableNodesForGroup.getOrElseUpdate(treeIndex, new mutable.ArrayBuffer[Node]()) += node
        mutableTreeToNodeToIndexInfo
          .getOrElseUpdate(treeIndex, new mutable.HashMap[Int, NodeIndexInfo]())(node.id)
          = new NodeIndexInfo(numNodesInGroup, featureSubset)
      }
      numNodesInGroup += 1
      memUsage += nodeMemUsage
    }
    // Convert mutable maps to immutable ones.
    val nodesForGroup: Map[Int, Array[Node]] = mutableNodesForGroup.mapValues(_.toArray).toMap
    val treeToNodeToIndexInfo = mutableTreeToNodeToIndexInfo.mapValues(_.toMap).toMap
    (nodesForGroup, treeToNodeToIndexInfo)
  }

  /**
   * Get the number of values to be stored for this node in the bin aggregates.
   * @param featureSubset  Indices of features which may be split at this node.
   *                       If None, then use all features.
   */
  private[tree] def aggregateSizeForNode(
      metadata: DecisionTreeMetadata,
      featureSubset: Option[Array[Int]]): Long = {
    val totalBins = if (featureSubset.nonEmpty) {
      featureSubset.get.map(featureIndex => metadata.numBins(featureIndex).toLong).sum
    } else {
      metadata.numBins.map(_.toLong).sum
    }
    if (metadata.isClassification) {
      metadata.numClasses * totalBins
    } else {
      3 * totalBins
    }
  }
}

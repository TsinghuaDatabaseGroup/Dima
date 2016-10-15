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

package org.apache.spark.sql.index

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Statistics}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.partitioner._
import org.apache.spark.sql.spatial.Point
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.execution.SimilarityRDD

import scala.collection.mutable.ArrayBuffer

/**
 * Created by dong on 1/15/16.
 * Indexed Relation Structures for Simba
 */

private[sql] case class PackedPartitionWithIndex(data: Array[InternalRow], index: Index)

private[sql] object IndexedRelation {
  def apply(child: SparkPlan, table_name: Option[String], index_type: IndexType,
            column_keys: List[Attribute], index_name: String): IndexedRelation = {
    index_type match {
      case TreeMapType =>
        new TreeMapIndexedRelation(child.output, child, table_name, column_keys, index_name)()
      case RTreeType =>
        new RTreeIndexedRelation(child.output, child, table_name, column_keys, index_name)()
      case HashMapType =>
        new HashMapIndexedRelation(child.output, child, table_name, column_keys, index_name)()
      case JaccardIndexType =>
        new JaccardIndexIndexedRelation(child.output, child, table_name, column_keys, index_name)()
      case _ => null
    }
  }
}

private[sql] abstract class IndexedRelation extends LogicalPlan {
  self: Product =>
  var _indexedRDD: RDD[PackedPartitionWithIndex]
  def indexedRDD: RDD[PackedPartitionWithIndex] = _indexedRDD

  override def children: Seq[LogicalPlan] = Nil
  def output: Seq[Attribute]

  def withOutput(newOutput: Seq[Attribute]): IndexedRelation
}

private[sql] case class HashMapIndexedRelation(
  output: Seq[Attribute],
  child: SparkPlan,
  table_name: Option[String],
  column_keys: List[Attribute],
  index_name: String)(var _indexedRDD: RDD[PackedPartitionWithIndex] = null)
  extends IndexedRelation with MultiInstanceRelation {
  require(column_keys.length == 1)
  val numShufflePartitions = child.sqlContext.conf.numShufflePartitions
  val maxEntriesPerNode = child.sqlContext.conf.maxEntriesPerNode
  val sampleRate = child.sqlContext.conf.sampleRate
  val transferThreshold = child.sqlContext.conf.transferThreshold

  if (_indexedRDD == null) {
    buildIndex()
  }

  private[sql] def buildIndex(): Unit = {
    val dataRDD = child.execute().map(row => {
      val key = BindReferences.bindReference(column_keys.head, child.output).eval(row)
      (key, row)
    })

    val partitionedRDD = HashPartition(dataRDD, numShufflePartitions)
    val indexed = partitionedRDD.mapPartitions(iter => {
      val data = iter.toArray
      val index = HashMapIndex(data)
      Array(PackedPartitionWithIndex(data.map(_._2), index)).iterator
    }).persist(StorageLevel.MEMORY_AND_DISK_SER)

    indexed.setName(table_name.map(n => s"$n $index_name").getOrElse(child.toString))
    _indexedRDD = indexed
  }

  override def newInstance(): IndexedRelation = {
    new HashMapIndexedRelation(output.map(_.newInstance()), child, table_name,
      column_keys, index_name)(_indexedRDD).asInstanceOf[this.type]
  }

  override def withOutput(new_output: Seq[Attribute]): IndexedRelation = {
    new HashMapIndexedRelation(new_output, child, table_name, column_keys, index_name)(_indexedRDD)
  }

  @transient override lazy val statistics = Statistics(
    // TODO: Instead of returning a default value here, find a way to return a meaningful size
    // estimate for RDDs. See PR 1238 for more discussions.
    sizeInBytes = BigInt(child.sqlContext.conf.defaultSizeInBytes)
  )
}

private[sql] case class TreeMapIndexedRelation(
  output: Seq[Attribute],
  child: SparkPlan,
  table_name: Option[String],
  column_keys: List[Attribute],
  index_name: String)(var _indexedRDD: RDD[PackedPartitionWithIndex] = null,
                      var range_bounds: Array[Double] = null)
  extends IndexedRelation with MultiInstanceRelation {
  require(column_keys.length == 1)
  val numShufflePartitions = child.sqlContext.conf.numShufflePartitions
  val maxEntriesPerNode = child.sqlContext.conf.maxEntriesPerNode
  val sampleRate = child.sqlContext.conf.sampleRate
  val transferThreshold = child.sqlContext.conf.transferThreshold

  if (_indexedRDD == null) {
    buildIndex()
  }

  private[sql] def buildIndex(): Unit = {
    val dataRDD = child.execute().map(row => {
      val key = BindReferences.bindReference(column_keys.head, child.output).eval(row)
        .asInstanceOf[Number].doubleValue
      (key, row)
    })

    val (partitionedRDD, tmp_bounds) = RangePartition.rowPartition(dataRDD, numShufflePartitions)
    range_bounds = tmp_bounds
    val indexed = partitionedRDD.mapPartitions(iter => {
      val data = iter.toArray
      val index = TreeMapIndex(data)
      Array(PackedPartitionWithIndex(data.map(_._2), index)).iterator
    }).persist(StorageLevel.MEMORY_AND_DISK_SER)

    indexed.setName(table_name.map(n => s"$n $index_name").getOrElse(child.toString))
    _indexedRDD = indexed
  }

  override def newInstance(): IndexedRelation = {
    new TreeMapIndexedRelation(output.map(_.newInstance()), child, table_name,
      column_keys, index_name)(_indexedRDD)
      .asInstanceOf[this.type]
  }

  override def withOutput(new_output: Seq[Attribute]): IndexedRelation = {
    new TreeMapIndexedRelation(new_output, child, table_name,
      column_keys, index_name)(_indexedRDD, range_bounds)
  }

  @transient override lazy val statistics = Statistics(
    // TODO: Instead of returning a default value here, find a way to return a meaningful size
    // estimate for RDDs. See PR 1238 for more discussions.
    sizeInBytes = BigInt(child.sqlContext.conf.defaultSizeInBytes)
  )
}

private[sql] case class RTreeIndexedRelation(
  output: Seq[Attribute],
  child: SparkPlan,
  table_name: Option[String],
  column_keys: List[Attribute],
  index_name: String)(var _indexedRDD: RDD[PackedPartitionWithIndex] = null,
                      var global_rtree: RTree = null)
  extends IndexedRelation with MultiInstanceRelation {
  private def checkKeys: Boolean = {
    for (i <- column_keys.indices)
      if (!(column_keys(i).dataType.isInstanceOf[DoubleType] ||
        column_keys(i).dataType.isInstanceOf[IntegerType])) {
        return false
      }
    true
  }
  require(checkKeys)

  val numShufflePartitions = child.sqlContext.conf.numShufflePartitions
  val maxEntriesPerNode = child.sqlContext.conf.maxEntriesPerNode
  val sampleRate = child.sqlContext.conf.sampleRate
  val transferThreshold = child.sqlContext.conf.transferThreshold

  if (_indexedRDD == null) {
    buildIndex()
  }

  private[sql] def buildIndex(): Unit = {
    val dataRDD = child.execute().map(row => {
      val now = column_keys.map(x =>
        BindReferences.bindReference(x, child.output).eval(row).asInstanceOf[Number].doubleValue()
      ).toArray
      (new Point(now), row)
    })

    val dimension = column_keys.length
    val max_entries_per_node = maxEntriesPerNode
    val (partitionedRDD, mbr_bounds) = STRPartition(dataRDD, dimension, numShufflePartitions,
      sampleRate, transferThreshold, max_entries_per_node)


    val indexed = partitionedRDD.mapPartitions { iter =>
      val data = iter.toArray
      var index: RTree = null
      if (data.length > 0) index = RTree(data.map(_._1).zipWithIndex, max_entries_per_node)
      Array(PackedPartitionWithIndex(data.map(_._2), index)).iterator
    }.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val partitionSize = indexed.mapPartitions(iter => iter.map(_.data.length)).collect()

    global_rtree = RTree(mbr_bounds.zip(partitionSize)
      .map(x => (x._1._1, x._1._2, x._2)), max_entries_per_node)
    indexed.setName(table_name.map(n => s"$n $index_name").getOrElse(child.toString))
    _indexedRDD = indexed
  }

  override def newInstance(): IndexedRelation = {
    new RTreeIndexedRelation(output.map(_.newInstance()), child, table_name,
      column_keys, index_name)(_indexedRDD).asInstanceOf[this.type]
  }

  override def withOutput(new_output: Seq[Attribute]): IndexedRelation = {
    RTreeIndexedRelation(new_output, child, table_name,
      column_keys, index_name)(_indexedRDD, global_rtree)
  }

  @transient override lazy val statistics = Statistics(
    // TODO: Instead of returning a default value here, find a way to return a meaningful size
    // estimate for RDDs. See PR 1238 for more discussions.
    sizeInBytes = BigInt(child.sqlContext.conf.defaultSizeInBytes)
  )
}

/**
  * Created by sunji on 16/10/15.
  */

private[sql] case class IPartition(index: Index, data: Array[((String, InternalRow), Boolean)])

private[sql] abstract class IndexedRelationForJaccard extends LogicalPlan {
  self: Product =>
  var _indexedRDD: RDD[IPartition]
  def indexedRDD: RDD[IPartition] = _indexedRDD

  override def children: Seq[LogicalPlan] = Nil
  def output: Seq[Attribute]

  def withOutput(newOutput: Seq[Attribute]): IndexedRelation
}

private[sql] case class JaccardIndexIndexedRelation(
                                                     output: Seq[Attribute],
                                                     child: SparkPlan,
                                                     table_name: Option[String],
                                                     column_keys: List[Attribute],
                                                     index_name: String)(var _indexedRDD: RDD[PackedPartitionWithIndex] = null,
                                                                         var indexRDD: RDD[IPartition] = null)
  extends IndexedRelation with MultiInstanceRelation {
  require(column_keys.length == 1)
  val numPartitions = child.sqlContext.conf.numSimilarityPartitions.toInt
  val threshold = child.sqlContext.conf.similarityJaccardThreshold.toDouble
  val alpha = child.sqlContext.conf.similarityMultigroupThreshold.toDouble
  val topDegree = child.sqlContext.conf.similarityBalanceTopDegree.toInt
  val abandonNum = child.sqlContext.conf.similarityFrequencyAbandonNum.toInt

  if (indexRDD == null) {
    buildIndex()
  }

  private[sql] def multigroup(
                               mini: Int,
                               maxi: Int,
                               threshold: Double,
                               alpha : Double): Array[(Int, Int)] = {
    var result = ArrayBuffer[(Int, Int)]()
    var l = mini
    while (l <= maxi) {
      val l1 = Math.floor(l / alpha + 0.0001).toInt
      result += Tuple2(l, l1)
      l = l1 + 1
    }
    result.toArray
  }

  private[sql] def CalculateH ( l: Int, s: Int, threshold: Double ) = {
    Math.floor((1 - threshold) * (l + s) / (1 + threshold) + 0.0001).toInt + 1
  }

  private[sql] def CalculateH1 ( l: Int, threshold: Double ): Int = {
    // 生成分段的段数(按照query长度)
    Math.floor ( (1 - threshold) * l / threshold + 0.0001).toInt + 1
  }

  private[sql] def createInverse(ss1: String,
                                 group: Array[(Int, Int)],
                                 threshold: Double
                                ): Array[(String, Int, Int)] = {
    {
      val ss = ss1.split(" ")
      val range = group.filter(
        x => (x._1 <= ss.length && x._2 >= ss.length)
      )
      val sl = range(range.length-1)._1
      val H = CalculateH1(sl, threshold)
      logInfo(s"createInverse: H: " + H.toString)
      for (i <- 1 until H + 1) yield {
        val s = ss.filter(x => {x.hashCode % H + 1 == i})
        if (s.length == 0) {
          Tuple3("", i, sl)
        } else if (s.length == 1) {
          Tuple3(s(0), i, sl)
        } else {
          Tuple3(s.reduce(_ + " " + _), i, sl)
        }
      }
    }.toArray
  }

  private[sql] def createDeletion(ss1: String): Array[String] = {
    {
      val ss = ss1.split(" ")
      if (ss.length == 1) {
        Array("")
      } else if (ss.length == 2) {
        Array(ss(0), ss(1))
      } else {
        for (s <- 0 until ss.length) yield {
          Array.concat(ss.slice(0, s), ss.slice(s + 1, ss.length)).reduce(_ + " " + _)
        }
      }.toArray
    }
  }

  def sort(xs: Array[String]): Array[String] = {
    if (xs.length <= 1) {
      xs
    } else {
      val pivot = xs(xs.length / 2)
      Array.concat(
        sort(xs filter (pivot >)),
        xs filter (pivot ==),
        sort(xs filter (pivot <))
      )
    }
  }

  def sortByValue(x: String): String = {
    sort(x.split(" ")).reduce(_ + " " + _)
  }

  private[sql] def buildIndex(): Unit = {

    val dataRDD = child.execute().map(row => {
      val key = BindReferences.bindReference(column_keys.head, child.output).eval(row)
      (key, row)
    })

    val rdd = dataRDD
      .map(t => (t._1.asInstanceOf[org.apache.spark.unsafe.types.UTF8String].toString.split(" "),
        t._2))

    val rdd1 = rdd.map(x => x._1.length).persist(StorageLevel.DISK_ONLY)
    val minimum = child.sparkContext.broadcast(rdd1.min())
    val maximum = child.sparkContext.broadcast(rdd1.max())
    val count = child.sparkContext.broadcast(rdd1.count())
    val average = child.sparkContext.broadcast(rdd1.sum() / count.value)

    val multiGroup = child.sparkContext.broadcast(
      multigroup(minimum.value, maximum.value, threshold, alpha
      )
    )

    val inverseRDD = dataRDD
      .map(t => (sortByValue(t._1.asInstanceOf[org.apache.spark.unsafe.types.UTF8String].toString),
        t._2))

    val recordidMapSubstring = inverseRDD
      .map(x => {
        ((x._1, x._2), createInverse(x._1, multiGroup.value, threshold))
      })
      .flatMapValues(x => x)
      .map(x => ((x._1, x._2._2, x._2._3), x._2._1))

    val deletionMapRecord = recordidMapSubstring
      .filter(x => (x._2.length > 0))
      .map(x => (x._1, createDeletion(x._2))) // (1,i,l), deletionSubstring
      .flatMapValues(x => x)
      .map(x => {
//        println(s"deletion index: " + x._2 + " " + x._1._2 + " " + x._1._3)
        ((x._2, x._1._2, x._1._3).hashCode(), (x._1._1, true))
      })
    // (hashCode, (String, internalrow))

    val inverseMapRecord = recordidMapSubstring
      .map(x => {
//        println(s"inverse index: " + x._2 + " " + x._1._2 + " " + x._1._3)
        ((x._2, x._1._2, x._1._3).hashCode(), (x._1._1, false))
      })

    val index = deletionMapRecord.union(inverseMapRecord).persist(StorageLevel.DISK_ONLY)

    val frequencyTable = child.sparkContext.broadcast(
      index
        .map(x => ((x._1, x._2._2), 1))
        .reduceByKey(_ + _)
        .filter(x => x._2 > abandonNum)
        .collectAsMap()
    )

    val partitionedRDD = new SimilarityRDD(index
      .partitionBy(new SimilarityHashPartitioner(numPartitions)), true)

    val indexed = partitionedRDD.mapPartitions(iter => {
      val data = iter.toArray
      val index = JaccardIndex(data,
        threshold, frequencyTable, multiGroup, minimum.value, alpha, numPartitions)
      Array(IPartition(index, data.map(x => x._2))).iterator
    }).persist(StorageLevel.MEMORY_AND_DISK_SER)

    indexed.setName(table_name.map(n => s"$n $index_name").getOrElse(child.toString))
    indexed.count
    child.sqlContext.indexManager.addIndexGlobalInfo(threshold,
      frequencyTable,
      multiGroup,
      minimum.value,
      alpha,
      numPartitions)
    indexRDD = indexed
  }

  override def newInstance(): IndexedRelation = {
    new JaccardIndexIndexedRelation(output.map(_.newInstance()), child, table_name,
      column_keys, index_name)(_indexedRDD, indexRDD).asInstanceOf[this.type]
  }

  override def withOutput(new_output: Seq[Attribute]): IndexedRelation = {
    new JaccardIndexIndexedRelation(new_output,
      child, table_name, column_keys, index_name)(_indexedRDD, indexRDD)
  }

  @transient override lazy val statistics = Statistics(
    // TODO: Instead of returning a default value here, find a way to return a meaningful size
    // estimate for RDDs. See PR 1238 for more discussions.
    sizeInBytes = BigInt(child.sqlContext.conf.defaultSizeInBytes)
  )
}
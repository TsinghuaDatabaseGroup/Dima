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
import org.apache.spark.sql.topksearch.{EditTopkIndexedRelation, JaccardTopkIndexedRelation}

import scala.collection.mutable.{ArrayBuffer, Map}

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
      case EdIndexType =>
        new EdIndexIndexedRelation(child.output, child, table_name, column_keys, index_name)()
      case JaccardTopkType =>
        new JaccardTopkIndexedRelation(child.output, child, table_name, column_keys, index_name)()
      case EdTopkType =>
        new EditTopkIndexedRelation(child.output, child, table_name, column_keys, index_name)()
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

private[sql] case class IPartition(partitionId: Int,
                                   index: Index,
                                   data: Array[((Int,
                                     InternalRow,
                                     Array[(Array[Int], Array[Boolean])]),
                                     Boolean)])

private[sql] case class IndexGlobalInfo(threshold: Double,
                           frequencyTable: Broadcast[scala.collection.Map[(Int, Boolean), Long]],
                           multiGroup: Broadcast[Array[(Int, Int)]],
                           minimum: Int,
                           alpha: Double,
                           partitionNum: Int,
                           threshold_base: Double) extends Serializable

private[sql] case class JaccardIndexIndexedRelation(
  output: Seq[Attribute],
  child: SparkPlan,
  table_name: Option[String],
  column_keys: List[Attribute],
  index_name: String)(var _indexedRDD: RDD[PackedPartitionWithIndex] = null,
                      var indexRDD: RDD[IPartition] = null,
                      var indexGlobalInfo: IndexGlobalInfo = null)
  extends IndexedRelation with MultiInstanceRelation {
  require(column_keys.length == 1)
  val numPartitions = child.sqlContext.conf.numSimilarityPartitions
  val threshold = child.sqlContext.conf.similarityJaccardThreshold
  val alpha = child.sqlContext.conf.similarityMultigroupThreshold
  val topDegree = child.sqlContext.conf.similarityBalanceTopDegree
  val abandonNum = child.sqlContext.conf.similarityFrequencyAbandonNum

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

  private[sql] def segNum(s: String, n: Int): Int = {
    val hash = s.hashCode % n
    if (hash >= 0) {
      hash + 1
    } else {
      hash + n + 1
    }
  }

  private[sql] def createInverse(ss1: String,
                                 group: Array[(Int, Int)],
                                 threshold: Double
                                ): Array[(String, Int, Int)] = {
    {
      val ss = ss1.split(" ").filter(x => x.length > 0)
      val range = group.filter(
        x => (x._1 <= ss.length && x._2 >= ss.length)
      )
      val sl = range(range.length-1)._1
      val H = CalculateH1(sl, threshold)
      logInfo(s"createInverse: H: " + H.toString)
      for (i <- 1 until H + 1) yield {
        val s = ss.filter(x => {segNum(x, H) == i})
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
      (key, row.copy())
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

    val splittedRecord = inverseRDD
      .map(x => {
        ((x._1, x._2), createInverse(x._1, multiGroup.value, threshold))
      })
      .flatMapValues(x => x)
      .map(x => ((x._1, x._2._2, x._2._3), x._2._1))

    val deletionIndexSig = splittedRecord
      .filter(x => (x._2.length > 0))
      .map(x => (x._1, createDeletion(x._2))) // (1,i,l), deletionSubstring
      .flatMapValues(x => x)
      .map(x => {
        ((x._2, x._1._2, x._1._3).hashCode(), (x._1._1, true))
      })
    // (hashCode, (String, internalrow))

    val segIndexSig = splittedRecord
      .map(x => {
        ((x._2, x._1._2, x._1._3).hashCode(), (x._1._1, false))
      })

    val index = deletionIndexSig.union(segIndexSig).persist(StorageLevel.DISK_ONLY)

    val f = index
      .map(x => ((x._1, x._2._2), 1L))
      .reduceByKey(_ + _)
      .filter(x => x._2 > abandonNum)
      .persist

    val frequencyTable = child.sparkContext.broadcast(
      f.collectAsMap()
    )

    val partitionTable = child.sparkContext.broadcast(Array[(Int, Int)]().toMap)

    val partitionedRDD = index
      .partitionBy(new SimilarityHashPartitioner(numPartitions, partitionTable))

    val indexed = partitionedRDD.mapPartitionsWithIndex((partitionId, iter) => {
      val data = iter.toArray
      val index = JaccardIndex(data,
        threshold, frequencyTable, multiGroup, minimum.value, alpha, numPartitions)
      Array(IPartition(partitionId, index, data
        .map(x => ((sortByValue(x._2._1._1).hashCode,
          x._2._1._2,
          createInverse(sortByValue(x._2._1._1),
            multiGroup.value,
            threshold)
        .map(x => {
          if (x._1.length > 0) {
            (x._1.split(" ").map(s => s.hashCode), Array[Boolean]())
          } else {
            (Array[Int](), Array[Boolean]())
          }
        })), x._2._2)))).iterator
    }).persist(StorageLevel.MEMORY_AND_DISK_SER)
    indexed.count

    indexGlobalInfo = new IndexGlobalInfo(threshold,
      frequencyTable, multiGroup, minimum.value, alpha, numPartitions, threshold)

    indexed.setName(table_name.map(n => s"$n $index_name").getOrElse(child.toString))
    indexRDD = indexed
  }

  override def newInstance(): IndexedRelation = {
    new JaccardIndexIndexedRelation(output.map(_.newInstance()), child, table_name,
      column_keys, index_name)(_indexedRDD, indexRDD, indexGlobalInfo).asInstanceOf[this.type]
  }

  override def withOutput(new_output: Seq[Attribute]): IndexedRelation = {
    new JaccardIndexIndexedRelation(new_output,
      child, table_name, column_keys, index_name)(_indexedRDD, indexRDD, indexGlobalInfo)
  }

  @transient override lazy val statistics = Statistics(
    // TODO: Instead of returning a default value here, find a way to return a meaningful size
    // estimate for RDDs. See PR 1238 for more discussions.
    sizeInBytes = BigInt(child.sqlContext.conf.defaultSizeInBytes)
  )
}

/**
  * Created by sunji on 16/11/16.
  */

import org.apache.spark.sql.SimilarityProbe.ValueInfo

private[sql] case class EPartition(partitionId: Int,
                                   index: Index,
                                   data: Array[ValueInfo])

private[sql] case class EdIndexGlobalInfo(threshold: Int,
                             frequencyTable: Broadcast[scala.collection.Map[(Int, Boolean), Long]],
                             minimum: Int,
                             partitionNum: Int,
                             P: Broadcast[scala.collection.mutable.Map[(Int, Int), Int]],
                             L: Broadcast[scala.collection.mutable.Map[(Int, Int), Int]],
                             topDegree: Int,
                             weight: Array[Int], max: Int) extends Serializable

private[sql] case class EdIndexIndexedRelation(
  output: Seq[Attribute],
  child: SparkPlan,
  table_name: Option[String],
  column_keys: List[Attribute],
  index_name: String)(var _indexedRDD: RDD[PackedPartitionWithIndex] = null,
                     var indexRDD: RDD[EPartition] = null,
                     var indexGlobalInfo: EdIndexGlobalInfo = null)
  extends IndexedRelation with MultiInstanceRelation {
  require(column_keys.length == 1)
  val num_partitions = child.sqlContext.conf.numSimilarityPartitions
  val threshold = child.sqlContext.conf.similarityEditDistanceThreshold
  val topDegree = child.sqlContext.conf.similarityBalanceTopDegree
  val abandonNum = child.sqlContext.conf.similarityFrequencyAbandonNum
  val weight = child.sqlContext.conf.similarityMaxWeight.split(",").map(_.toInt)

  if (indexRDD == null) {
    buildIndex()
  }

  private def Lij(l: Int, i: Int, threshold: Int): Int = {
    val U = threshold
    val K = (l - Math.floor(l / (U + 1)) * (U + 1)).toInt
    if (i <= (U + 1 - K)) {
      return Math.floor(l / (U + 1)).toInt
    }
    else {
      return (Math.ceil(l / (U + 1)) + 0.001).toInt
    }
  }

  private def Pij(l: Int, i: Int, L: scala.collection.Map[(Int, Int), Int]): Int = {
    var p = 0
    for (j <- 1 until i) {
      p = p + L((l, j))
    }
    return p + 1
  }

  private def calculateAllL(min: Int,
                            max: Int,
                            threshold: Int): Map[(Int, Int), Int] = {
    val result = Map[(Int, Int), Int]()
    for (l <- min until max + 1) {
      for (i <- 1 until threshold + 2) {
        result += ((l, i) -> Lij(l, i, threshold))
      }
    }
    result
  }

  private def calculateAllP(min: Int,
                            max: Int,
                            L: scala.collection.Map[(Int, Int), Int],
                            threshold: Int): Map[(Int, Int), Int] = {
    val result = Map[(Int, Int), Int]()
    for (l <- min until max + 1) {
      for (i <- 1 until threshold + 2) {
        result += ((l, i) -> Pij(l, i, L))
      }
    }
    result
  }

  private def part(content: InternalRow, s: String, threshold: Int,
    L: Broadcast[Map[(Int, Int), Int]],
    P: Broadcast[Map[(Int, Int), Int]]): Array[(Int, ValueInfo)] = {
    var ss = ArrayBuffer[(Int, ValueInfo)]()
    val U: Int = threshold
    val l = s.length
    val K: Int = (l - Math.floor(l / (U + 1)) * (U + 1)).toInt
    var point: Int = 0
    for (i <- 1 until U + 2) {
      val length = L.value(l, i)
      val seg1 = {
        s.slice(point, point + length)
      }
      ss += Tuple2((seg1, i, l, 0).hashCode(),
        ValueInfo(content, s, false, Array[Boolean]()))

      for (n <- 0 until length) {
        val subset = s.slice(point, point + n) + s.slice(point + n + 1, point + length)
        val seg = subset
        val key = (seg, i, l, n + 1).hashCode()
        ss += Tuple2(key, ValueInfo(content, s, true, Array[Boolean]()))
      }
      point = point + length
    }
    ss.toArray
  } // (substring, i, rlength)

  private[sql] def buildIndex(): Unit = {

    val dataRDD = child.execute().map(row => {
      val key = BindReferences.bindReference(column_keys.head, child.output).eval(row)
      (key, row.copy())
    })

    val rdd = dataRDD
      .map(t => (t._1.asInstanceOf[org.apache.spark.unsafe.types.UTF8String].toString,
        t._2))

    val indexLength = rdd
      .map(x => x._1.length)
      .persist(StorageLevel.DISK_ONLY)

    val minLength = child.sparkContext.broadcast(Math.max(indexLength.min, threshold + 1))
    val maxLength = child.sparkContext.broadcast(indexLength.max)

    val partitionL = child.sparkContext
      .broadcast(calculateAllL(1, maxLength.value, threshold))
    val partitionP = child.sparkContext
      .broadcast(calculateAllP(1, maxLength.value, partitionL.value, threshold))

    val index_rdd = rdd
      .map(x => (x._1.length, x._1, x._2))
      .filter(x => x._1 > threshold)
      .flatMap(x => part(x._3, x._2, threshold, partitionL, partitionP))
      .map(x => (x._1, x._2))
      .persist(StorageLevel.DISK_ONLY)

    val f =
      index_rdd.map(x => {
        ((x._1, x._2.isDeletion), 1.toLong)
      })
        .reduceByKey(_ + _)
        .filter(x => x._2 > abandonNum)

    val frequencyTable = child.sparkContext.broadcast(
      f.collectAsMap()
    )

    val partitionTable = child.sparkContext.broadcast(
      Array[(Int, Int)]().toMap
    )

    val index_partitioned_rdd = new SimilarityRDD(
      index_rdd.partitionBy(
        new SimilarityHashPartitioner(
          num_partitions, partitionTable)), true)

    val indexed =
      index_partitioned_rdd
        .mapPartitionsWithIndex((partitionId, iter) => {
          val data = iter.toArray
          val index = EdIndex(data, threshold, frequencyTable, minLength.value, num_partitions)
          Array(EPartition(partitionId, index, data.map(x => x._2))).iterator
          //          Array(index.size).iterator
        })
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
    indexed.count

    indexGlobalInfo = new EdIndexGlobalInfo(threshold, frequencyTable,
      minLength.value, num_partitions, partitionP, partitionL, topDegree, weight, maxLength.value)

    indexed.setName(table_name.map(n => s"$n $index_name").getOrElse(child.toString))
    indexRDD = indexed
  }

  override def newInstance(): IndexedRelation = {
    new EdIndexIndexedRelation(output.map(_.newInstance()), child, table_name,
      column_keys, index_name)(_indexedRDD, indexRDD, indexGlobalInfo).asInstanceOf[this.type]
  }

  override def withOutput(new_output: Seq[Attribute]): IndexedRelation = {
    new EdIndexIndexedRelation(new_output,
      child, table_name, column_keys, index_name)(_indexedRDD, indexRDD, indexGlobalInfo)
  }

  @transient override lazy val statistics = Statistics(
    // TODO: Instead of returning a default value here, find a way to return a meaningful size
    // estimate for RDDs. See PR 1238 for more discussions.
    sizeInBytes = BigInt(child.sqlContext.conf.defaultSizeInBytes)
  )
}
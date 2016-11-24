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

package org.apache.spark.sql.execution.joins

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.partitioner.{SimilarityHashPartitioner, SimilarityQueryPartitioner}
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}
import org.apache.spark.sql.execution.SimilarityRDD

/**
  * Created by sunji on 16/9/2.
  */

case class EditDistanceSelfSimilarityJoin(
                                       left_keys: Expression,
                                       right_keys: Expression,
                                       l: Literal,
                                       left: SparkPlan,
                                       right: SparkPlan) extends BinaryNode {
  override def output: Seq[Attribute] = left.output ++ right.output

  val utilize = EditDistanceSimilarityJoin(left_keys, right_keys, l, left, right)

  private def parts_self (
                     content: InternalRow,
                     s: String,
                     indexNum1: scala.collection.Map[(Int, Boolean), Long],
                     L: Map[(Int, Int), Int],
                     P: Map[(Int, Int), Int],
                     threshold: Int): Array[(Int, ValueInfo)] = {
    val result = ArrayBuffer[(Int, ValueInfo)]()
    val sLength = s.length
    val lu = sLength
    val lo = Math.max(sLength - threshold, threshold + 1)
    val U = threshold
    for (l <- lo until lu + 1) {
      val V = utilize.calculateVsl(
        U,
        l,
        indexNum1,
        s,
        threshold,
        utilize.num_partitions,
        utilize.topDegree,
        P,
        L)
      val V_INFO = V.map(x => x.toString).reduce(_ + ", " + _)
      for (i <- 1 until U + 2) {
        val lowerBound = Math.max(P(l, i) - (i - 1), P(l, i) - (l - sLength + (U + 1 - i)))
        val upperBound = Math.min(P(l, i) + sLength - l + U + 1 - i, P(l, i) + i - 1)
        val length = L(l, i)
        for (x <- lowerBound until upperBound + 1) {
          if (V(i - 1) == 1) {
            val seg = {
              val subset = s.slice(x - 1, x - 1 + length)
              subset
            }
            result += Tuple2((seg, i, l, 0).hashCode(), ValueInfo(content, s, false, Array(false)))
          } else if (V(i - 1) == 2) {
            for (n <- 0 until length) {
              val subset = s.slice(x - 1, x - 1 + n) + s.slice(x - 1 + n + 1, x - 1 + length)
              val seg = subset
              val key = (seg, i, l, n + 1).hashCode()
              result += Tuple2(key, ValueInfo(content, s, true, Array(true)))
            }
          }
        }
      }
    }
    result.toArray
  }

  override protected def doExecute(): RDD[InternalRow] = {
    logInfo(s"execute IndexSimilarityJoin")

    val left_rdd = left.execute().map(row =>
    {
      val key = BindReferences
        .bindReference(left_keys, left.output)
        .eval(row)
        .asInstanceOf[org.apache.spark.unsafe.types.UTF8String]
        .toString
      (key, row.copy())
    })

    val record = left_rdd
      .distinct
      .persist(StorageLevel.DISK_ONLY)

    val indexLength = record
      .map(x => x._1.length)
      .persist(StorageLevel.DISK_ONLY)

    val minLength = sparkContext.broadcast(Math.max(indexLength.min, utilize.threshold + 1))
    val maxLength = sparkContext.broadcast(indexLength.max)

    val partitionL = sparkContext
      .broadcast(utilize.calculateAllL(1, maxLength.value, utilize.threshold))
    val partitionP = sparkContext
      .broadcast(utilize.calculateAllP(1, maxLength.value, partitionL.value, utilize.threshold))

    val index_rdd = record
      .map(x => (x._1.length, x._1, x._2))
      .filter(x => x._1 > utilize.threshold)
      .flatMap(x => utilize.part(x._3, x._2, utilize.threshold))
      .map(x => (x._1, x._2))
      .persist(StorageLevel.DISK_ONLY)

    val f =
      index_rdd.map(x => {
        ((x._1, x._2.isDeletion), 1.toLong)
      })
        .reduceByKey(_ + _)
        .filter(x => x._2 > utilize.abandonNum)
    //    println(s"frequencyTableLength: $f")

    val frequencyTable = sparkContext.broadcast(
      f.collectAsMap()
    )

    val partitionTable = sparkContext.broadcast(
      Array[(Int, Int)]().toMap
    )

    val index_partitioned_rdd = new SimilarityRDD(
      index_rdd.partitionBy(
        new SimilarityHashPartitioner(
          utilize.num_partitions, partitionTable.value)), true)

    val index_indexed_rdd =
      index_partitioned_rdd
        .mapPartitionsWithIndex((partitionId, iter) => {
          val data = iter.toArray
          val index = Map[Int, List[Int]]()
          for (i <- 0 until data.length) {
            index += (data(i)._1 -> (i :: index.getOrElse(data(i)._1, List())))
          }
          Array((index, data.map(x => x._2), partitionId)).iterator
          //          Array(index.size).iterator
        })
        .persist(StorageLevel.MEMORY_AND_DISK_SER)

    index_indexed_rdd.count

    val query_rdd = record
      .filter(x => x._1.length > utilize.threshold)
      .map(x => (x._1.length, x._1, x._2))
      .flatMap(
        x => parts_self(
          x._3, x._2, frequencyTable.value, partitionL.value, partitionP.value, utilize.threshold))
      .map(x => (x._1, x._2))
      .persist(StorageLevel.DISK_ONLY)

    query_rdd.count

    val partitionLoad = query_rdd
      .mapPartitions({ iter =>
        Array(utilize.distribute.clone()).iterator
      })
      .collect
      .reduce((a, b) => {
        val r = ArrayBuffer[Long]()
        for (i <- 0 until utilize.num_partitions) {
          r += (a(i) + b(i))
        }
        r.toArray
      })

    val maxPartitionId = sparkContext.broadcast({
      val result = ArrayBuffer[Int]()
      for (l <- 0 until utilize.partitionNumToBeSent) {
        var max = 0.toLong
        var in = -1
        for (i <- 0 until utilize.num_partitions) {
          if (!utilize.Has(i, result.toArray) && partitionLoad(i) > max) {
            max = partitionLoad(i)
            in = i
          }
        }
        result += in
      }
      result.toArray
    })

    val extraIndex = sparkContext.broadcast(
      index_indexed_rdd.mapPartitionsWithIndex((Index, iter) => {
        Array((Index, iter.toArray)).iterator
      })
        .filter(x => utilize.Has(x._1, maxPartitionId.value))
        .map(x => x._2)
        .collect())

    val query_partitioned_rdd = new SimilarityRDD(query_rdd
      .partitionBy(
        new SimilarityQueryPartitioner(
          utilize.num_partitions,
          partitionTable.value,
          frequencyTable.value,
          maxPartitionId.value)), true)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    query_partitioned_rdd.count

    query_partitioned_rdd.zipPartitions(index_indexed_rdd) {
      (leftIter, rightIter) => {
        val ans = ListBuffer[(InternalRow, InternalRow)]()
        val index = rightIter.next
        val partitionId = index._3
        while (leftIter.hasNext) {
          val q = leftIter.next
          val positionOfQ = partitionTable.value.getOrElse(q._1, utilize.hashStrategy(q._1))
          val (candidate, whichIndex) = {
            if (positionOfQ != partitionId) {
              var (c, w) = (List[Int](), -1)
              var goon = true
              var i = 0
              while (goon && i < extraIndex.value.length) {
                if (extraIndex.value(i)(0)._3 == positionOfQ) {
                  c = extraIndex.value(i)(0)._1.getOrElse(q._1, List())
                  w = i
                  goon = false
                }
                i += 1
              }
              (c, w)
            } else {
              (index._1.getOrElse(q._1, List()), -1)
            }
          }

          for (i <- candidate) {
            val data = {
              if (whichIndex < 0) {
                index._2
              } else {
                extraIndex.value(whichIndex)(0)._2
              }
            }
            if (utilize.compareSimilarity(q._2, data(i), utilize.threshold)) {
              ans += Tuple2(q._2.content, data(i).content)
            }
          }
        }
        ans.map(x => new JoinedRow(x._2, x._1)).iterator
      }
    }
  }
}


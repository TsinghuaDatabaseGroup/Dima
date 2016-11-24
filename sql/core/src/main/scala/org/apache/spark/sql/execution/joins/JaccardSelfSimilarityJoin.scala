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
import org.apache.spark.sql.catalyst.expressions.{Attribute, JoinedRow, Literal}
import org.apache.spark.sql.execution.{BinaryNode, SimilarityRDD, SparkPlan}
import org.apache.spark.sql.index.{IPartition, JaccardIndex}
import org.apache.spark.sql.partitioner.{SimilarityHashPartitioner, SimilarityQueryPartitioner}
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by sunji on 16/9/2.
  */


case class JaccardSelfSimilarityJoin(leftKeys: Expression,
                                 rightKeys: Expression,
                                 l: Literal,
                                 left: SparkPlan,
                                 right: SparkPlan) extends BinaryNode {
  override def output: Seq[Attribute] = left.output ++ right.output

  val utilize = JaccardSimilarityJoin(leftKeys, rightKeys, l, left, right)

  private[sql] def partition_r_self(
                                ss1: String,
                                indexNum: scala.collection.Map[(Int, Boolean), Long],
                                partitionTable: scala.collection.Map[Int, Int],
                                minimum: Int,
                                group: Array[(Int, Int)],
                                threshold: Double,
                                alpha: Double,
                                partitionNum: Int,
                                topDegree: Int
                              ): Array[(Array[(Array[Int], Array[Boolean])],
    Array[(Int, Boolean, Array[Boolean], Boolean, Int)])] = {
    var result = ArrayBuffer[(Array[(Array[Int], Array[Boolean])],
      Array[(Int, Boolean, Array[Boolean],
        Boolean,
        Int)])]()
    val ss = ss1.split(" ")
    val s = ss.size
    val range = group
      .filter(x => {
        x._1 <= s && x._2 >= (Math.ceil(threshold * s) + 0.0001).toInt
      })
    for (lrange <- range) {
      val l = lrange._1
      val isExtend = {
        if (l == range(range.length - 1)._1) {
          false
        }
        else {
          true
        }
      }

      val H = utilize.CalculateH1(l, threshold)

      val records = ArrayBuffer[(Array[Int], Array[Boolean])]()

      val substring = {
        for (i <- 1 until H + 1) yield {
          val p = ss.filter(x => x.hashCode % H + 1 == i)
          if (p.length == 0) "" else if (p.length == 1) p(0) else p.reduce(_ + " " + _)
        }
      }.toArray

      val V = utilize.calculateVsl(s,
        l,
        indexNum,
        partitionTable,
        substring,
        H,
        minimum,
        alpha,
        partitionNum,
        topDegree)

      for (i <- 1 until H + 1) {
        val p = ss.filter(x => x.hashCode % H + 1 == i)
        records += Tuple2(p.map(x => x.hashCode), {
          if (V(i - 1) == 0) Array()
          else if (V(i - 1) == 1) Array(false)
          else Array(true)
        })
      }

      var result1 = ArrayBuffer[(Int, Boolean, Array[Boolean], Boolean, Int)]()
      for (i <- 1 until H + 1) {
        val hash = (substring(i - 1), i, l).hashCode()
        if (V(i - 1) == 1) {
          result1 += Tuple5(hash, false, Array(false), isExtend, i)
        }
        else if (V(i - 1) == 2) {
          result1 += Tuple5(hash, false, Array(true), isExtend, i)
          if (substring(i - 1).length > 0) {
            for (k <- utilize.createDeletion(substring(i - 1))) {
              val hash1 = (k, i, l).hashCode()
              result1 += Tuple5(hash1, true, Array(true), isExtend, i)
            }
          }
        }
      }
      result += Tuple2(records.toArray, result1.toArray)
    }

    result.toArray
  }

  private[sql] def compareSimilarity(
    query: ((Int, InternalRow, Array[(Array[Int], Array[Boolean])])
         , Boolean, Array[Boolean], Boolean, Int),
    index: ((Int, InternalRow, Array[(Array[Int], Array[Boolean])]), Boolean)): Boolean = {
    val pos = query._5
    if (index._2) {
      if (!query._2 && query._3.length > 0 && query._3(0)) {
        if (query._1._1 != index._1._1) {
          utilize.verify(query._1._3, index._1._3, utilize.threshold, pos)
        } else {
          false
        }
      } else {
        false
      }
    } else {
      if (!query._2 && !query._4 && query._3.length > 0) {
        if (query._1._1 < index._1._1) {
          utilize.verify(query._1._3, index._1._3, utilize.threshold, pos)
        } else {
          false
        }
      } else if (!query._2 && query._4 && query._3.length > 0) {
        if (query._1._1 != index._1._1) {
          utilize.verify(query._1._3, index._1._3, utilize.threshold, pos)
        } else {
          false
        }
      } else if (query._2 && !query._4 && query._3.length > 0 && query._3(0)) {
        if (query._1._1 < index._1._1) {
          utilize.verify(query._1._3, index._1._3, utilize.threshold, pos)
        } else {
          false
        }
      } else if (query._2 && query._4 && query._3.length > 0 && query._3(0)) {
        if (query._1._1 != index._1._1) {
          utilize.verify(query._1._3, index._1._3, utilize.threshold, pos)
        } else {
          false
        }
      } else {
        false
      }
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    logInfo(s"execute JaccardSelfSimilarityJoin")
    val left_rdd = left.execute().map(row =>
    {
      val key = BindReferences
        .bindReference(leftKeys, left.output)
        .eval(row)
        .asInstanceOf[org.apache.spark.unsafe.types.UTF8String]
        .toString
      (key, row.copy())
    })

    val right_rdd = left.execute().map(row =>
    {
      val key = BindReferences
        .bindReference(leftKeys, left.output)
        .eval(row)
        .asInstanceOf[org.apache.spark.unsafe.types.UTF8String]
        .toString
      (key, row.copy())
    })

    val rdd1 = left_rdd
      .map(x => (x._1.split(" ").size))
      .persist(StorageLevel.DISK_ONLY)
    val minimum = sparkContext.broadcast(rdd1.min())
    val maximum = sparkContext.broadcast(rdd1.max())
    val count = sparkContext.broadcast(rdd1.count())
    val average = sparkContext.broadcast(rdd1.sum() / count.value)
    rdd1.unpersist()

    logInfo(s"" + minimum.value.toString + " "
      + maximum.value.toString + " "
      + average.value.toString + " "
      + count.value.toString)

    val record = left_rdd
      .map(x => (utilize.sortByValue(x._1), x._2))
      .distinct
      .persist(StorageLevel.DISK_ONLY)
    val multiGroup = sparkContext
      .broadcast(utilize.multigroup(minimum.value, maximum.value, utilize.threshold, utilize.alpha))

    val splittedRecord = record
      .map(x => {
        ((x._1, x._2), utilize.createInverse(x._1, multiGroup.value, utilize.threshold))
      })
      .flatMapValues(x => x)
      .map(x => ((x._1, x._2._2, x._2._3), x._2._1))

    val deletionIndexSig = splittedRecord
      .filter(x => (x._2.length > 0))
      .map(x => (x._1, utilize.createDeletion(x._2)))
      .flatMapValues(x => x)
      .map(x => {
        ((x._2, x._1._2, x._1._3).hashCode(), (x._1._1, true))
      })

    val segIndexSig = splittedRecord
      .map(x => {
        ((x._2, x._1._2, x._1._3).hashCode(), (x._1._1, false))
      })

    val index = deletionIndexSig.union(segIndexSig).persist(StorageLevel.DISK_ONLY)

    val f = index
      .map(x => ((x._1, x._2._2), 1.toLong))
      .reduceByKey(_ + _)
      .filter(x => x._2 > utilize.abandonNum)
      .persist()

    val frequencyTable = sparkContext.broadcast(
      f
        .map(x => (x._1, x._2))
        .collectAsMap()
    )
    val partitionTable = sparkContext.broadcast(
      // this part controls the index repartition
      Array[(Int, Int)]().toMap
    )

    val indexPartitionedRDD = new SimilarityRDD(index
      .partitionBy(
        new SimilarityHashPartitioner(utilize.num_partitions, partitionTable.value)), true)

    val index_rdd_indexed = indexPartitionedRDD.mapPartitionsWithIndex((partitionId, iter) => {
      val data = iter.toArray
      val index = JaccardIndex(data,
        utilize.threshold,
        frequencyTable,
        multiGroup,
        minimum.value,
        utilize.alpha,
        utilize.num_partitions)
      Array(IPartition(partitionId, index,
        data.
          map(x => (((x._2._1._1.hashCode,
            x._2._1._2,
            utilize.createInverse(x._2._1._1, multiGroup.value, utilize.threshold)
              .map(x => (x._1.split(" ").map(s => s.hashCode), Array[Boolean]()))),
            x._2._2))))).iterator
    }).persist(StorageLevel.MEMORY_AND_DISK_SER)

    index_rdd_indexed.count

    val query_rdd = right_rdd
      .map(x => (utilize.sortByValue(x._1), x._2))
      .distinct
      .map(x => ((x._1.hashCode, x._2),
        partition_r_self(
          x._1, frequencyTable.value, partitionTable.value, minimum.value, multiGroup.value,
          utilize.threshold, utilize.alpha, utilize.num_partitions, utilize.topDegree
        )))
      .flatMapValues(x => x)
      .map(x => ((x._1._1, x._1._2, x._2._1), x._2._2))
      .flatMapValues(x => x)
      .map(x => (x._2._1, (x._1, x._2._2, x._2._3, x._2._4, x._2._5)))

    def Has(x : Int, array: Array[Int]): Boolean = {
      for (i <- array) {
        if (x == i) {
          return true
        }
      }
      false
    }

    val partitionLoad = query_rdd
      .mapPartitions({iter =>
        Array(utilize.distribute.clone()).iterator
      })
      .collect
      .reduce((a, b) => {
        val r = ArrayBuffer[Long]()
        for (i <- 0 until utilize.num_partitions) {
          r += (a(i) + b(i))
        }
        r.toArray.map(x => (x / utilize.num_partitions) * 8)
      })

    val maxPartitionId = sparkContext.broadcast({
      val result = ArrayBuffer[Int]()
      for (l <- 0 until utilize.partitionNumToBeSent) {
        var max = 0.toLong
        var in = -1
        for (i <- 0 until utilize.num_partitions) {
          if (!Has(i, result.toArray) && partitionLoad(i) > max) {
            max = partitionLoad(i)
            in = i
          }
        }
        result += in
      }
      result.toArray
    })

    val extraIndex = sparkContext.broadcast(
      index_rdd_indexed.mapPartitionsWithIndex((Index, iter) => {
        Array((Index, iter.toArray)).iterator
      })
        .filter(x => Has(x._1, maxPartitionId.value))
        .map(x => x._2)
        .collect())

    val query_rdd_partitioned = new SimilarityRDD(
      query_rdd
        .partitionBy(
          new SimilarityQueryPartitioner(
            utilize.num_partitions,
            partitionTable.value,
            frequencyTable.value,
            maxPartitionId.value)
        ), true
    )
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    query_rdd_partitioned.count

    query_rdd_partitioned.zipPartitions(index_rdd_indexed, true) {
      (leftIter, rightIter) => {
        val ans = mutable.ListBuffer[(InternalRow, InternalRow)]()
        val index = rightIter.next
        val index2 = extraIndex.value
        var countNum = 0.toLong
        var pId = 0
        val partitionId = index.partitionId
        while (leftIter.hasNext) {
          val q = leftIter.next
          val positionOfQ = partitionTable.value.getOrElse(q._1, utilize.hashStrategy(q._1))
          val (candidate, whichIndex) = {
            if (positionOfQ != partitionId) {
              var (c, w) = (List[Int](), -1)
              var goon = true
              var i = 0
              while (goon && i < index2.length) {
                if (index2(i)(0).partitionId == positionOfQ) {
                  c = index2(i)(0).index.asInstanceOf[JaccardIndex].index.getOrElse(q._1, List())
                  w = i
                  goon = false
                }
                i += 1
              }
              (c, w)
            } else {
              (index.index.asInstanceOf[JaccardIndex].index.getOrElse(q._1, List()), -1)
            }
          }

          for (i <- candidate) {
            val data = {
              if (whichIndex < 0) {
                index.data(i)
              } else {
                index2(whichIndex)(0).data(i)
              }
            }
            if (compareSimilarity(q._2, data)) {
              ans += Tuple2(q._2._1._2, data._1._2)
            }
          }
        }
        ans.map(x => new JoinedRow(x._1, x._2)).iterator
      }
    }
  }
}


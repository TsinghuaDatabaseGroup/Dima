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

case class ValueInfo(
                      content: InternalRow,
                      record: String,
                      isDeletion: Boolean,
                      value: Array[Boolean]
                      ) extends Serializable

case class EditDistanceSimilarityJoin(
                                  left_keys: Expression,
                                  right_keys: Expression,
                                  l: Literal,
                                  left: SparkPlan,
                                  right: SparkPlan) extends BinaryNode {
  override def output: Seq[Attribute] = left.output ++ right.output

  final val num_partitions = sqlContext.conf.numSimilarityPartitions
  final val threshold = l.toString.toInt
  final val topDegree = sqlContext.conf.similarityBalanceTopDegree
  final val abandonNum = sqlContext.conf.similarityFrequencyAbandonNum
  final val partitionNumToBeSent = sqlContext.conf.partitionNumToBeSent
  final val weight = sqlContext.conf.similarityMaxWeight.split(",").map(x => x.toInt)
  val distribute = new Array[Long](2048)

  private[sql] def parent(i: Int) = Math.floor(i / 2).toInt

  private[sql] def left_child(i: Int) = 2 * i

  private[sql] def right_child(i: Int) = 2 * i + 1

  private[sql] def compare(x: (Long, Long), y: (Long, Long)): Short = {
    if (x._1 > y._1) {
      1
    } else if (x._1 < y._1) {
      -1
    } else {
      if (x._2 > y._2) {
        1
      } else if (x._2 < y._2) {
        -1
      } else {
        0
      }
    }
  }

  private[sql] def minHeapify(A: Array[(Int, (Long, Long), Int)],
                         i: Int): Array[(Int, (Long, Long), Int)] = {
    val l = left_child(i)
    val r = right_child(i)
    val AA = A.clone()
    val smallest = {
      if (l <= AA.length && compare(AA(l - 1)._2, AA(i - 1)._2) < 0) {
        if (r <= AA.length && compare(AA(r - 1)._2, AA(l - 1)._2) < 0) {
          r
        } else {
          l
        }
      }
      else {
        if (r <= AA.length && compare(AA(r - 1)._2, AA(i - 1)._2) < 0) {
          r
        } else {
          i
        }
      }
    }
    if (smallest != i) {
      val temp = AA(i - 1)
      AA(i - 1) = AA(smallest - 1)
      AA(smallest - 1) = temp
      minHeapify(AA, smallest)
    } else {
      AA
    }
  }

  private[sql] def heapExtractMin(
                      A: Array[(Int, (Long, Long), Int)]
                    ): ((Int, (Long, Long), Int), Array[(Int, (Long, Long), Int)]) = {
    val heapSize = A.length
    if (heapSize < 1) {
    }
    val AA = A.clone()
    val min = AA(0)
    AA(0) = AA(heapSize - 1)
    Tuple2(min, minHeapify(AA.slice(0, heapSize - 1), 1))
  }

  private[sql] def heapIncreaseKey(
                       A: Array[(Int, (Long, Long), Int)],
                       i: Int,
                       key: (Int, (Long, Long), Int)
                     ): Array[(Int, (Long, Long), Int)] = {
    if (compare(key._2, A(i - 1)._2) > 0) {
    }
    val AA = A.clone()
    AA(i - 1) = key
    var ii = i
    while (ii > 1 && compare(AA(parent(ii) - 1)._2, AA(ii - 1)._2) > 0) {
      val temp = AA(ii - 1)
      AA(ii - 1) = AA(parent(ii) - 1)
      AA(parent(ii) - 1) = temp
      ii = parent(ii)
    }
    AA
  }

  private[sql] def minHeapInsert(
                     A: Array[(Int, (Long, Long), Int)],
                     key: (Int, (Long, Long), Int)
                   ): Array[(Int, (Long, Long), Int)] = {
    val AA = Array.concat(A, Array(key).map(x => (x._1, (Long.MaxValue, Long.MaxValue), x._3)))
    heapIncreaseKey(AA, AA.length, key)
  }

  private[sql] def buildMinHeap(
    A: Array[(Int, (Long, Long), Int)]): Array[(Int, (Long, Long), Int)] = {
    var AA = A.clone()
    for (i <- (1 until Math.floor(AA.length / 2).toInt + 1).reverse) {
      AA = minHeapify(AA, i)
    }
    AA
  }

  private[sql] def calculateVsl(
                    U: Int,
                    l: Int,
                    indexNum: scala.collection.Map[(Int, Boolean), Long],
                    record: String,
                    threshold: Int,
                    numPartition: Int,
                    topDegree: Int,
                    P: Map[(Int, Int), Int],
                    L: Map[(Int, Int), Int]
                  ): Array[Int] = {

    val sLength = record.length

    val C0 = {
      for (i <- 1 until U + 2) yield {
        0
      }
    }.toArray
    val C1 = {
      val result = ArrayBuffer[Long]()
      for (i <- 1 until U + 2) {
        val lowerBound = Math.max(P(l, i) - (i - 1), P(l, i) - (l - sLength + (U + 1 - i)))
        val upperBound = Math.min(P(l, i) + sLength - l + U + 1 - i, P(l, i) + i - 1)
        val length = L(l, i)
        var total = 0.toLong
        for (x <- lowerBound until upperBound + 1) {
          val subset = record.slice(x - 1, x - 1 + length)
          val seg = subset
          val key = (seg, i, l, 0).hashCode()
          total += indexNum.getOrElse((key, false), 0.toLong)
        }
        result += total
      }
      result.toArray
    }

    val C2 = {
      val result = ArrayBuffer[Long]()
      for (i <- 1 until U + 2) {
        val lowerBound = Math.max(P(l, i) - (i - 1), P(l, i) - (l - sLength + (U + 1 - i)))
        val upperBound = Math.min(P(l, i) + sLength - l + U + 1 - i, P(l, i) + i - 1)
        val length = L(l, i)
        var total = 0.toLong
        for (x <- lowerBound until upperBound + 1) {
          for (n <- 0 until length) {
            val subset = record.slice(x - 1, x - 1 + n) +
              record.slice(x - 1 + n + 1, x - 1 + length)
            val seg = subset
            val key = (seg, i, l, n + 1).hashCode()
            total += indexNum.getOrElse((key, true), 0.toLong)
          }
        }
        result += total
      }
      result.toArray
    }

    val addToDistributeWhen1 = {
      val resultTotal = ArrayBuffer[Array[(Int, Long)]]()
      for (i <- 1 until U + 2) {
        val result = ArrayBuffer[(Int, Long)]()
        val lowerBound = Math.max(P(l, i) - (i - 1), P(l, i) - (l - sLength + (U + 1 - i)))
        val upperBound = Math.min(P(l, i) + sLength - l + U + 1 - i, P(l, i) + i - 1)
        val length = L(l, i)
        for (x <- lowerBound until upperBound + 1) {
          val subset = record.slice(x - 1, x - 1 + length)
          val seg = subset
          val key = (seg, i, l, 0).hashCode()
          val code = (key % numPartition)
          val partition = {
            if (code < 0) {
              code + numPartition
            } else {
              code
            }
          }
          result += Tuple2(partition, indexNum.getOrElse((key, false), 0.toLong))
        }
        resultTotal += result.toArray
      }
      resultTotal.toArray
    }

    val addToDistributeWhen2 = {
      val resultTotal = ArrayBuffer[Array[(Int, Long)]]()
      for (i <- 1 until U + 2) {
        val result = ArrayBuffer[(Int, Long)]()
        val lowerBound = Math.max(P(l, i) - (i - 1), P(l, i) - (l - sLength + (U + 1 - i)))
        val upperBound = Math.min(P(l, i) + sLength - l + U + 1 - i, P(l, i) + i - 1)
        val length = L(l, i)
        for (x <- lowerBound until upperBound + 1) {
          for (n <- 0 until length) {
            val subset = record.slice(x - 1, x - 1 + n) +
              record.slice(x - 1 + n + 1, x - 1 + length)
            val seg = subset
            val key = (seg, i, l, n + 1).hashCode()
            val code = (key % numPartition)
            val partition = {
              if (code < 0) {
                code + numPartition
              } else {
                code
              }
            }
            result += Tuple2(partition, indexNum.getOrElse((key, true), 0.toLong))
          }
        }
        resultTotal += result.toArray
      }
      resultTotal.toArray
    }

    val deata_distribute0 = {
      // 只考虑有变化的reducer的负载
      for (i <- 0 until U + 1) yield {
        // 分配到1之后情况比较单一,只有inverseindex 和 inversequery匹配这一种情况,只会对一个reducer产生影响
        val dis = distribute.slice(0, numPartition).clone()
        val change = ArrayBuffer[Int]()
        for (j <- addToDistributeWhen1(i)) {
          dis(j._1) += j._2.toLong
          if (j._2 > 0) {
            change += j._1
          }
        }
        var total = 0.toLong

        for (ii <- 0 until topDegree) {
          var max = 0.toLong
          var maxPos = -1
          var pos = 0
          for (c <- change) {
            if (dis(c) >= max) {
              max = dis(c)
              maxPos = pos
            }
            pos += 1
          }
          if (maxPos >= 0) {
            change.remove(maxPos)
            total += weight(ii) * max
          }
        }
        total
      }
    }.toArray

    val deata_distribute1 = {
      // 分配到2
      for (i <- 0 until U + 1) yield {
        val dis = distribute.slice(0, numPartition).clone()
        val change = ArrayBuffer[Int]()
        for (j <- addToDistributeWhen2(i)) {
          dis(j._1) += j._2.toLong
          if (j._2 > 0) {
            change += j._1
          }
        }
        var total = 0.toLong

        for (ii <- 0 until topDegree) {
          var max = 0.toLong
          var maxPos = -1
          var pos = 0
          for (c <- change) {
            if (dis(c) >= max) {
              max = dis(c)
              maxPos = pos
            }
            pos += 1
          }
          if (maxPos >= 0) {
            change.remove(maxPos)
            total += weight(ii) * max
          }
        }
        total
      }
    }.toArray

    val deata0 = {
      for (i <- 0 until U + 1) yield {
        Tuple2(deata_distribute0(i), C1(i))
      }
    }.toArray

    val deata1 = {
      for (i <- 0 until U + 1) yield {
        Tuple2(deata_distribute1(i), C2(i) - C1(i))
      }
    }.toArray

    val V = {
      for (i <- 1 until U + 2) yield {
        0
      }
    }.toArray

    var M = buildMinHeap(deata0.zipWithIndex.map(x => (0, x._1, x._2)))

    for (j <- 1 until U + 2) {
      val MM = heapExtractMin(M)
      M = MM._2
      val pair = MM._1
      V(pair._3) += 1
      if (V(pair._3) == 1) {
        M = minHeapInsert(M, Tuple3(1, deata1(pair._3), pair._3))
      }
    }

    for (chooseid <- 0 until U + 1) {
      if (V(chooseid) == 1) {
        for (j <- addToDistributeWhen1(chooseid)) {
          distribute(j._1) += j._2.toLong
        }
      } else if (V(chooseid) == 2) {
        for (j <- addToDistributeWhen2(chooseid)) {
          distribute(j._1) += j._2.toLong
        }
      }
    }
    V
  }

  private[sql] def part(
    content: InternalRow, s: String, threshold: Int): Array[(Int, ValueInfo)] = {
    var ss = ArrayBuffer[(Int, ValueInfo)]()
    val U: Int = threshold
    val l = s.length
    val K: Int = (l - Math.floor(l / (U + 1)) * (U + 1)).toInt
    var point: Int = 0
    for (i <- 1 until U + 2) {
      if (i <= (U + 1 - K)) {
        val length = Math.floor(l / (U + 1)).toInt
        val seg1 = {
          s.slice(point, point + length)
        }
        ss += Tuple2((seg1, i, l, 0).hashCode(),
          ValueInfo(content, s, false, Array[Boolean]()))
        logInfo(s"segIndexSig: " + seg1 + ", " + i + ", " + l)
        for (n <- 0 until length) {
          val subset = s.slice(point, point + n) + s.slice(point + n + 1, point + length)
          val seg = subset
          val key = (seg, i, l, n + 1).hashCode()
          logInfo(s"delIndexSig: " + seg + ", " + i + ", " + l + ", " + (n + 1))
          ss += Tuple2(key, ValueInfo(content, s, true, Array[Boolean]()))
        }
        point = point + length
      } else {
        val length = Math.ceil(l / (U + 1) + 0.001).toInt
        val seg1 = {
          val xx = s.slice(point, point + length)
          xx
        }
        ss += Tuple2((seg1, i, l, 0).hashCode(),
          ValueInfo(content, s, false, Array[Boolean]()))
        logInfo(s"segIndexSig: " + seg1 + ", " + i + ", " + l)
        for (n <- 0 until length) {
          val subset = s.slice(point, point + n) + s.slice(point + n + 1, point + length)
          val seg = subset
          val key = (seg, i, l, n + 1).hashCode()
          logInfo(s"delIndexSig: " + seg + ", " + i + ", " + l + ", " + (n + 1))
          ss += Tuple2(key, ValueInfo(content, s, true, Array[Boolean]()))
        }
        point = point + length
      }
    }
    ss.toArray
  } // (substring, i, rlength)

  private[sql] def Lij(l: Int, i: Int, threshold: Int): Int = {
    val U = threshold
    val K = (l - Math.floor(l / (U + 1)) * (U + 1)).toInt
    if (i <= (U + 1 - K)) {
      return Math.floor(l / (U + 1)).toInt
    }
    else {
      return (Math.ceil(l / (U + 1)) + 0.001).toInt
    }
  }

  private[sql] def Pij(l: Int, i: Int, L: scala.collection.Map[(Int, Int), Int]): Int = {
    var p = 0
    for (j <- 1 until i) {
      p = p + L((l, j))
    }
    return p + 1
  }

  private[sql] def parts(
            content: InternalRow,
            s: String,
            indexNum1: scala.collection.Map[(Int, Boolean), Long],
            L: Map[(Int, Int), Int],
            P: Map[(Int, Int), Int],
            threshold: Int,
            max: Int): Array[(Int, ValueInfo)] = {
    val result = ArrayBuffer[(Int, ValueInfo)]()
    val sLength = s.length
    val lu = Math.min(sLength + threshold, max)
    val lo = Math.max(sLength - threshold, threshold + 1)
    val U = threshold
    for (l <- lo until lu + 1) {
      val V = calculateVsl(U, l, indexNum1, s, threshold, num_partitions, topDegree, P, L)
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
            logInfo(s"segProbeSig_1: " + seg + ", " + i + ", " + l)
            result += Tuple2((seg, i, l, 0).hashCode(), ValueInfo(content, s, false, Array(false)))
          } else if (V(i - 1) == 2) {
            for (n <- 0 until length) {
              val subset = s.slice(x - 1, x - 1 + n) + s.slice(x - 1 + n + 1, x - 1 + length)
              val seg = subset
              logInfo(s"delProbeSig_2: (" + seg + ", " + i + ", " + l + ", " + (n + 1) + ")")
              val key = (seg, i, l, n + 1).hashCode()
              result += Tuple2(key, ValueInfo(content, s, true, Array(true)))
            }
          }
        }
      }
    }
    result.toArray
  }

  private[sql] def EDdistance(a: String, b: String): Int = {
    val str1 = a
    val str2 = b
    val lenStr1 = str1.length
    val lenStr2 = str2.length
    val edit = Array.ofDim[Int](lenStr1, lenStr2)
    for (i <- 0 until lenStr1) {
      edit(i)(0) = i
    }
    for (j <- 0 until lenStr2) {
      edit(0)(j) = j
    }

    for (i <- 1 until lenStr1) {
      for (j <- 1 until lenStr2) {
        edit(i)(j) = Math.min(edit(i - 1)(j) + 1, edit(i)(j - 1) + 1)
        if (str1(i - 1) == str2(j - 1)) {
          edit(i)(j) = Math.min(edit(i)(j), edit(i - 1)(j - 1))
        } else {
          edit(i)(j) = Math.min(edit(i)(j), edit(i - 1)(j - 1) + 1)
        }
      }
    }
    return edit(lenStr1 - 1)(lenStr2 - 1)
  }

  private[sql] def calculateAllL(min: Int,
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

  private[sql] def calculateAllP(min: Int,
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

  private[sql] def hashStrategy(key: Int): Int = {
    val code = (key % num_partitions)
    if (code < 0) {
      code + num_partitions
    } else {
      code
    }
  }

  private[sql] def compareSimilarity(
    query: ValueInfo, index: ValueInfo, threshold: Int): Boolean = {
    val queryHash = query.record.hashCode
    val indexHash = index.record.hashCode
    logInfo(s"compare: " + query.record + " and " + index.record)
    if (!(query.isDeletion ^ index.isDeletion)) {
      EDdistance(query.record, index.record) <= threshold
    } else {
      false
    }
  }

  private[sql] def Has(x: Int, array: Array[Int]): Boolean = {
    for (i <- array) {
      if (x == i) {
        return true
      }
    }
    false
  }

  override protected def doExecute(): RDD[InternalRow] = {
    logInfo(s"execute EdSimilarityJoin")

    val left_rdd = left.execute().map(row =>
    {
      val key = BindReferences
        .bindReference(left_keys, left.output)
        .eval(row)
        .asInstanceOf[org.apache.spark.unsafe.types.UTF8String]
        .toString
      (key, row.copy())
    })

    val right_rdd = right.execute().map(row =>
    {
      val key = BindReferences
        .bindReference(right_keys, right.output)
        .eval(row)
        .asInstanceOf[org.apache.spark.unsafe.types.UTF8String]
        .toString
      (key, row.copy())
    })
    val record = left_rdd
      .distinct
      .persist(StorageLevel.DISK_ONLY)

    val record2 = right_rdd
      .distinct
      .persist(StorageLevel.DISK_ONLY)

    val indexLength = record
      .map(x => x._1.length)
      .persist(StorageLevel.DISK_ONLY)

    val minLength = sparkContext.broadcast(Math.max(indexLength.min, threshold + 1))
    val maxLength = sparkContext.broadcast(indexLength.max)

    val partitionL = sparkContext
      .broadcast(calculateAllL(1, maxLength.value, threshold))
    val partitionP = sparkContext
      .broadcast(calculateAllP(1, maxLength.value, partitionL.value, threshold))

    val index_rdd = record
      .map(x => (x._1.length, x._1, x._2))
      .filter(x => x._1 > threshold)
      .flatMap(x => part(x._3, x._2, threshold))
      .map(x => (x._1, x._2))
      .persist(StorageLevel.DISK_ONLY)

    val f =
      index_rdd.map(x => {
        ((x._1, x._2.isDeletion), 1.toLong)
      })
        .reduceByKey(_ + _)
        .filter(x => x._2 > abandonNum)
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
          num_partitions, partitionTable.value)), true)

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

    val query_rdd = record2
      .filter(x => x._1.length > threshold)
      .map(x => (x._1.length, x._1, x._2))
      .flatMap(
        x => parts(
          x._3, x._2, frequencyTable.value, partitionL.value, partitionP.value, threshold, maxLength.value))
      .map(x => (x._1, x._2))
      .persist(StorageLevel.DISK_ONLY)

    query_rdd.count

    val partitionLoad = query_rdd
      .mapPartitions({ iter =>
        Array(distribute.clone()).iterator
      })
      .collect
      .reduce((a, b) => {
        val r = ArrayBuffer[Long]()
        for (i <- 0 until num_partitions) {
          r += (a(i) + b(i))
        }
        r.toArray
      })

    val maxPartitionId = sparkContext.broadcast({
      val result = ArrayBuffer[Int]()
      for (l <- 0 until partitionNumToBeSent) {
        var max = 0.toLong
        var in = -1
        for (i <- 0 until num_partitions) {
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
      index_indexed_rdd.mapPartitionsWithIndex((Index, iter) => {
        Array((Index, iter.toArray)).iterator
      })
        .filter(x => Has(x._1, maxPartitionId.value))
        .map(x => x._2)
        .collect())

    val query_partitioned_rdd = new SimilarityRDD(query_rdd
      .partitionBy(
        new SimilarityQueryPartitioner(
          num_partitions, partitionTable.value, frequencyTable.value, maxPartitionId.value)), true)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    query_partitioned_rdd.count

    query_partitioned_rdd.zipPartitions(index_indexed_rdd) {
      (leftIter, rightIter) => {
        val ans = ListBuffer[(InternalRow, InternalRow)]()
        val index = rightIter.next
        val partitionId = index._3
        while (leftIter.hasNext) {
          val q = leftIter.next
          val positionOfQ = partitionTable.value.getOrElse(q._1, hashStrategy(q._1))
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
            if (compareSimilarity(q._2, data(i), threshold)) {
              ans += Tuple2(q._2.content, data(i).content)
            }
          }
        }
        ans.map(x => new JoinedRow(x._2, x._1)).iterator
      }
    }
  }
}


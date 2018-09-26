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

import org.apache.spark.broadcast.Broadcast
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

case class JaccardSimilarityJoin(leftKeys: Expression,
                                     rightKeys: Expression,
                                     l: Literal,
                                     left: SparkPlan,
                                     right: SparkPlan) extends BinaryNode {
  override def output: Seq[Attribute] = left.output ++ right.output

  final val num_partitions = sqlContext.conf.numSimilarityPartitions
  final val threshold = l.toString.toDouble
  final val alpha = sqlContext.conf.similarityMultigroupThreshold
  final val topDegree = sqlContext.conf.similarityBalanceTopDegree
  final val abandonNum = sqlContext.conf.similarityFrequencyAbandonNum
  final val weight = sqlContext.conf.similarityMaxWeight.split(",").map(x => x.toInt)
  final val partitionNumToBeSent = sqlContext.conf.partitionNumToBeSent

  val distribute = new Array[Long](2048)
  val indexDistribute = new Array[Long](2048)

  private[sql] def CalculateH(l: Int, s: Int, threshold: Double): Int = {
    Math.floor((1 - threshold) * (l + s) / (1 + threshold) + 0.0001).toInt + 1
  }

  private[sql] def CalculateH1(l: Int, threshold: Double): Int = {
    Math.floor((1 - threshold) * l / threshold + 0.0001).toInt + 1
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

  private[sql] def inverseDel(
                          xi: String,
                          indexNum: scala.collection.Map[(Int, Boolean), Long],
                          ii: Int,
                          ll: Int,
                          minimum: Int
                        ): Long = {
    var total = 0.toLong
    if (xi.length == 0) {
      return 0.toLong
    }
    for (i <- createDeletion(xi)) {
      val hash = (i, ii, ll).hashCode()
      total = total + indexNum.getOrElse((hash, false), 0.toLong)
    }
    total
  }

  private[sql] def addToMapForInverseDel(
                                     xi: String,
                                     indexNum: scala.collection.Map[(Int, Boolean), Long],
                                     partitionTable: scala.collection.Map[Int, Int],
                                     ii: Int,
                                     ll: Int,
                                     minimum: Int,
                                     numPartition: Int
                                   ): Array[(Int, Long)] = {
    if (xi.length == 0) {
      return Array[(Int, Long)]()
    }
    var result = ArrayBuffer[(Int, Long)]()
    for (i <- createDeletion(xi)) {
      val hash = (i, ii, ll).hashCode()
      val partition = partitionTable.getOrElse(hash, hashStrategy(hash))
      result += Tuple2(partition, indexNum.getOrElse((hash, false), 0.toLong))
    }
    result.toArray
  }

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
       logDebug(s"heap underflow")
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
      logDebug(s"new key is larger than current Key")
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

  private[sql] def hashStrategy(key: Int): Int = {
    val code = (key % num_partitions)
    if (code < 0) {
      code + num_partitions
    } else {
      code
    }
  }

  private[sql] def calculateVsl(
    s: Int,
    l: Int,
    indexNum: scala.collection.Map[(Int, Boolean), Long],
    partitionTable: scala.collection.Map[Int, Int],
    substring: Array[String],
    H: Int,
    minimum: Int,
    alpha: Double,
    numPartition: Int,
    topDegree: Int
  ): Array[Int] = {

    val C0 = {
      for (i <- 1 until H + 1) yield {
        0.toLong
      }
    }.toArray
    val C1 = {
      for (i <- 1 until H + 1) yield {
        val key = ((substring(i - 1), i, l).hashCode(), false)
        indexNum.getOrElse(key, 0.toLong)
      }
    }.toArray
    val C2 = {
      for (i <- 1 until H + 1) yield {
        val key = ((substring(i - 1), i, l).hashCode(), true)
        C1(i - 1) +
          indexNum.getOrElse(key, 0.toLong) +
          inverseDel(substring(i - 1), indexNum, i, l, minimum)
      }
    }.toArray

    val addToDistributeWhen1 = {
      for (i <- 1 until H + 1) yield {
        val hash = (substring(i - 1), i, l).hashCode()
        val partition = partitionTable.getOrElse(hash, hashStrategy(hash))
        (partition, indexNum.getOrElse((hash, false), 0.toLong))
      }
    }.toArray

    val addToDistributeWhen2 = {
      for (i <- 1 until H + 1) yield {
        val hash = (substring(i - 1), i, l).hashCode()
        val partition = partitionTable.getOrElse(hash, hashStrategy(hash))
        val x = addToMapForInverseDel(substring(i - 1),
          indexNum,
          partitionTable,
          i,
          l,
          minimum,
          numPartition)
        Array.concat(Array(
          addToDistributeWhen1(i - 1),
          (partition, indexNum.getOrElse((hash, true), 0.toLong))
        ), x)
      }
    }.toArray

    val deata_distribute0 = {
      // 只考虑有变化的reducer的负载
      for (i <- 0 until H) yield {
        // 分配到1之后情况比较单一,只有inverseindex 和 inversequery匹配这一种情况,只会对一个reducer产生影响
        val max = {
          if (addToDistributeWhen1(i)._2 > 0 && topDegree > 0) {
            (distribute(addToDistributeWhen1(i)._1) +
              addToDistributeWhen1(i)._2.toLong) * weight(0)
          } else {
            0.toLong
          }
        }
        max
      }
    }.toArray

    val deata_distribute1 = {
      // 分配到2
      for (i <- 0 until H) yield {
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
      for (i <- 0 until H) yield {
        Tuple2(deata_distribute0(i), C1(i) - C0(i))
      }
    }.toArray

    val deata1 = {
      for (i <- 0 until H) yield {
        Tuple2(deata_distribute1(i), C2(i) - C1(i))
      }
    }.toArray

    val Hls = CalculateH(Math.floor(l / alpha + 0.0001).toInt, s, threshold)

    val V = {
      for (i <- 1 until H + 1) yield {
        0
      }
    }.toArray

    var M = buildMinHeap(deata0.zipWithIndex.map(x => (0, x._1, x._2)))

    for (j <- 1 until Hls + 1) {
      val MM = heapExtractMin(M)
      M = MM._2
      val pair = MM._1
      V(pair._3) += 1
      if (V(pair._3) == 1) {
        M = minHeapInsert(M, Tuple3(1, deata1(pair._3), pair._3))
      }
    }

    for (chooseid <- 0 until H) {
      if (V(chooseid) == 1) {
        distribute(addToDistributeWhen1(chooseid)._1) += addToDistributeWhen1(chooseid)._2.toLong
      } else if (V(chooseid) == 2) {
        for (j <- addToDistributeWhen2(chooseid)) {
          distribute(j._1) += j._2.toLong
        }
      }
    }
    V
  }


  private[sql] def partition_r(
    ss1: String,
    indexNum: Broadcast[scala.collection.Map[(Int, Boolean), Long]],
    partitionTable: Broadcast[scala.collection.immutable.Map[Int, Int]],
    minimum: Int,
    group: Broadcast[Array[(Int, Int)]],
    threshold: Double,
    alpha: Double,
    partitionNum: Int,
    topDegree: Int): Array[(Array[(Array[Int], Array[Boolean])],
    Array[(Int, Boolean, Array[Boolean], Boolean, Int)])] = {
    var result = ArrayBuffer[(Array[(Array[Int], Array[Boolean])],
      Array[(Int, Boolean, Array[Boolean],
        Boolean,
        Int)])]()
    val ss = ss1.split(" ").filter(x => x.length > 0)
    val s = ss.size
    val range = group.value
      .filter(x => {
        x._1 <= Math.floor(s / threshold).toInt && x._2 >= (Math.ceil(threshold * s) + 0.0001).toInt
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

      val H = CalculateH1(l, threshold)

      // each segment and its v value of this record
      val records = ArrayBuffer[(Array[Int], Array[Boolean])]()

      val substring = {
        for (i <- 1 until H + 1) yield {
          val p = ss.filter(x => segNum(x, H) == i)
          if (p.length == 0) "" else if (p.length == 1) p(0) else p.reduce(_ + " " + _)
        }
      }.toArray

      val V = calculateVsl(s,
        l,
        indexNum.value,
        partitionTable.value,
        substring,
        H,
        minimum,
        alpha,
        partitionNum,
        topDegree)

      for (i <- 1 until H + 1) {
        val p = ss.filter(x => segNum(x, H) == i)
        records += Tuple2(p.map(x => x.hashCode), {
          if (V(i - 1) == 0) Array()
          else if (V(i - 1) == 1) Array(false)
          else Array(true)
        })
      }
      // probe seg/del signatures of this probe record
      var result1 = ArrayBuffer[(Int, Boolean, Array[Boolean], Boolean, Int)]()
      for (i <- 1 until H + 1) {
        val hash = (substring(i - 1), i, l).hashCode()
        if (V(i - 1) == 1) {
          logDebug(s"segProbeSig_1: (" + substring(i - 1) + ", " + i + ", " + l + ")" )
          result1 += Tuple5(hash, false, Array(false), isExtend, i)
        }
        else if (V(i - 1) == 2) {
          logDebug(s"segProbeSig_2: (" + substring(i - 1) + ", " + i + ", " + l + ")" )
          result1 += Tuple5(hash, false, Array(true), isExtend, i)
          if (substring(i - 1).length > 0) {
            for (k <- createDeletion(substring(i - 1))) {
              logDebug(s"segProbeSig_2: (" + k + ", " + i + ", " + l + ")" )
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

  private[sql] def segNum(s: String, n: Int): Int = {
    val hash = s.hashCode % n
    if (hash >= 0) {
      hash + 1
    } else {
      hash + n + 1
    }
  }

  private[sql] def createInverse(ss1: String,
                            group: Broadcast[Array[(Int, Int)]],
                            threshold: Double
                           ): Array[(String, Int, Int)] = {
    {
      val ss = ss1.split(" ").filter(x => x.length > 0)
      val range = group.value.filter(
        x => (x._1 <= ss.length && x._2 >= ss.length)
      )
      val sl = range(range.length - 1)._1
      val H = CalculateH1(sl, threshold)
      for (i <- 1 until H + 1) yield {
        val s = ss.filter(x => {
          segNum(x, H) == i
        })
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


  private[sql] def sort(xs: Array[String]): Array[String] = {
    if (xs.length <= 1) {
      xs
    } else {
      val pivot = xs(xs.length / 2)
      Array.concat(
        sort(xs filter (pivot.hashCode > _.hashCode)),
        xs filter (pivot.hashCode == _.hashCode),
        sort(xs filter (pivot.hashCode < _.hashCode))
      )
    }
  }

  private[sql] def sortByValue(x: String): String = {
    sort(x.split(" ")).reduce(_ + " " + _)
  }

  private[sql] def calculateOverlapBound(t: Float, xl: Int, yl: Int): Int = {
    (Math.ceil((t / (t + 1)) * (xl + yl)) + 0.0001).toInt
  }

  private[sql] def verify(x: Array[(Array[Int], Array[Boolean])],
                     y: Array[(Array[Int], Array[Boolean])],
                     threshold: Double,
                     pos: Int, xLength: Int, yLength: Int
                    ): Boolean = {
    logDebug(s"enter verification, pos: ${pos}, xLength: ${xLength}, yLength: ${yLength}")
    val overlap = calculateOverlapBound(threshold.asInstanceOf[Float], xLength, yLength)
    var currentOverlap = 0
    var currentXLength = 0
    var currentYLength = 0
    for (i <- 0 until x.length) {
      var n = 0
      var m = 0
      var o = 0
      while (n < x(i)._1.length && m < y(i)._1.length) {
        if (x(i)._1(n) == y(i)._1(m)) {
          o += 1
          n += 1
          m += 1
        } else if (x(i)._1(n) < y(i)._1(m)) {
          n += 1
        } else {
          m += 1
        }
      }
      currentOverlap = o + currentOverlap
      currentXLength += x(i)._1.length
      currentYLength += y(i)._1.length
      val diff = x(i)._1.length + y(i)._1.length - o * 2
      val Vx = {
        if (x(i)._2.length == 0) {
          0
        } else if (x(i)._2.length == 1 && !x(i)._2(0)) {
          1
        } else {
          2
        }
      }
      val Vy = {
        if (y(i)._2.length == 0) {
          0
        } else if (y(i)._2.length == 1 && !y(i)._2(0)) {
          1
        } else {
          2
        }
      }
      if (i + 1 < pos) {
        if (diff < Vx || diff < Vy) {
          logDebug(s"i:$i, overlap")
          return false
        }
      }
      if (currentOverlap + Math.min((xLength - currentXLength),
        (yLength - currentYLength)) < overlap) {
        logDebug(s"i:$i, currentOverlap:$currentOverlap, " +
          s"xLength: $xLength, yLength: $yLength, currentXLength: $currentXLength, " +
          s"currentYLength: $currentYLength, overlap: $overlap, prune")
        return false
      }
    }
    if (currentOverlap >= overlap) {
      return true
    } else {
      logDebug(s"finalOverlap:$currentOverlap, overlap: $overlap, false")
      return false
    }
  }

  private[sql] def compareSimilarity(
    query: ((Int, InternalRow, Array[(Array[Int], Array[Boolean])])
      , Boolean, Array[Boolean], Boolean, Int),
    index: ((Int, InternalRow, Array[(Array[Int], Array[Boolean])]), Boolean)): Boolean = {
    logDebug(s"compare { ${query._1._2} } and " +
      s"{ ${index._1._2}}")
    logDebug(s"isDeletionIndex: ${index._2}, isDeletionQuery: ${query._2}, val" +
      s"ue: ${
        if (query._3.length == 0) {
          0
        } else if (!query._3(0)) {
          1
        } else {
          2
        }
      }")
    val pos = query._5
    val query_length = query._1._3
      .map(x => x._1.length)
      .reduce(_ + _)
    val index_length = index._1._3
      .map(x => x._1.length)
      .reduce(_ + _)
    if (index._2) { //
      if (!query._2 && query._3.length > 0 && query._3(0)) {
          verify(query._1._3, index._1._3, threshold, pos,
            query_length, index_length)
      } else {
        false
      }
    } else {
      verify(query._1._3, index._1._3, threshold, pos,
        query_length, index_length)
    }
  }

  private[sql] def multigroup(mini: Int,
                         maxi: Int,
                         threshold: Double,
                         alpha: Double): Array[(Int, Int)] = {
    var result = ArrayBuffer[(Int, Int)]()
    var l = mini
    while (l <= maxi) {
      val l1 = Math.floor(l / alpha + 0.0001).toInt
      result += Tuple2(l, l1)
      l = l1 + 1
    }
    result.toArray
  }

  override protected def doExecute(): RDD[InternalRow] = {
    logDebug(s"execute JaccardSelfSimilarityJoin")
    logDebug(s"LEFT: ${left}")
    logDebug(s"RIGHT: ${right}")

    val left_rdd = left.execute().map(row =>
    {
      val key = BindReferences
        .bindReference(leftKeys, left.output)
        .eval(row)
        .asInstanceOf[org.apache.spark.unsafe.types.UTF8String]
        .toString
      (key, row.copy())
    })

    val right_rdd = right.execute().map(row =>
    {
      val key = BindReferences
        .bindReference(rightKeys, right.output)
        .eval(row)
        .asInstanceOf[org.apache.spark.unsafe.types.UTF8String]
        .toString
      (key, row.copy())
    })

    val rdd1 = left_rdd
      .map(x => (x._1.split(" ").filter(_.size > 0).size))
      .persist(StorageLevel.DISK_ONLY)
    val minimum = sparkContext.broadcast(rdd1.min())
    val maximum = sparkContext.broadcast(rdd1.max())
    val count = sparkContext.broadcast(rdd1.count())
    val average = sparkContext.broadcast(rdd1.sum() / count.value)
    rdd1.unpersist()

    logDebug(s"" + minimum.value.toString + " "
      + maximum.value.toString + " "
      + average.value.toString + " "
      + count.value.toString)

    val record = left_rdd
      .map(x => (sortByValue(x._1), x._2))
      // .distinct
      .persist(StorageLevel.DISK_ONLY)

    logInfo(s"" + minimum.value.toString + "--" + maximum.value.toString)

    val multiGroup = sparkContext
      .broadcast(multigroup(minimum.value, maximum.value, threshold, alpha))

    val splittedRecord = record
      .map(x => {
        ((x._1, x._2), createInverse(x._1, multiGroup, threshold))
      })
      .flatMapValues(x => x)
      .map(x => ((x._1, x._2._2, x._2._3), x._2._1))

    val deletionIndexSig = splittedRecord
      .filter(x => (x._2.length > 0))
      .map(x => (x._1, createDeletion(x._2)))
      .flatMapValues(x => x)
      .map(x => {
        logDebug(s"deletionIndexSig: (" + x._2 + ", " + x._1._2 + ", " + x._1._3 + ")" )
        ((x._2, x._1._2, x._1._3).hashCode(), (x._1._1, true))
      })

    val segIndexSig = splittedRecord
      .map(x => {
        logDebug(s"segIndexSig: (" + x._2 + ", " + x._1._2 + ", " + x._1._3 + ")" )
        ((x._2, x._1._2, x._1._3).hashCode(), (x._1._1, false))
      })

    val index = deletionIndexSig.union(segIndexSig).persist(StorageLevel.DISK_ONLY)

    val f = index
      .map(x => ((x._1, x._2._2), 1.toLong))
      .reduceByKey(_ + _)
      .filter(x => x._2 > abandonNum)
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
      .partitionBy(new SimilarityHashPartitioner(num_partitions, partitionTable)), true)

    val index_rdd_indexed = indexPartitionedRDD.mapPartitionsWithIndex((partitionId, iter) => {
      val data = iter.toArray
      val index = JaccardIndex(data,
        threshold, frequencyTable, multiGroup, minimum.value, alpha, num_partitions)
      Array(IPartition(partitionId, index,
        data.
          map(x => ((x._2._1._1.hashCode,
            x._2._1._2,
            createInverse(x._2._1._1, multiGroup, threshold)
              .map(x => {
              if (x._1.length > 0) {
                (x._1.split(" ").map(s => s.hashCode), Array[Boolean]())
              } else {
                (Array[Int](), Array[Boolean]())
              }
            })),
            x._2._2)))).iterator
    }).persist(StorageLevel.MEMORY_AND_DISK_SER)

    index_rdd_indexed.count

    val query_rdd = right_rdd
      .map(x => (sortByValue(x._1), x._2))
      // .distinct
      .map(x => ((x._1.hashCode, x._2, x._1),
        partition_r(
          x._1, frequencyTable, partitionTable, minimum.value, multiGroup,
          threshold, alpha, num_partitions, topDegree
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
        Array(distribute.clone()).iterator
      })
      .collect
      .reduce((a, b) => {
        val r = ArrayBuffer[Long]()
        for (i <- 0 until num_partitions) {
          r += (a(i) + b(i))
        }
        r.toArray.map(x => (x/num_partitions) * 8)
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
            num_partitions, partitionTable, frequencyTable, maxPartitionId.value)
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
          val positionOfQ = partitionTable.value.getOrElse(q._1, hashStrategy(q._1))
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
        ans.map(x => new JoinedRow(x._2, x._1)).iterator
      }
    }
  }
}


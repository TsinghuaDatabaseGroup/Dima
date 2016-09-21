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
import org.apache.spark.sql.partitioner.SimilarityHashPartitioner
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.{ArrayBuffer, Map}

/**
  * Created by sunji on 16/9/2.
  */



case class EditDistanceSimilarityJoin(
                                  left_keys: Seq[Expression],
                                  right_keys: Seq[Expression],
                                  l: Literal,
                                  left: SparkPlan,
                                  right: SparkPlan) extends BinaryNode {
  override def output: Seq[Attribute] = left.output ++ right.output

  final val num_partitions = sqlContext.conf.numSimilarityPartitions.toInt
  final val threshold = sqlContext.conf.similarityEditDistanceThreshold.toInt
  final val topDegree = sqlContext.conf.similarityBalanceTopDegree.toInt

  override protected def doExecute(): RDD[InternalRow] = {
    logInfo(s"execute IndexSimilarityJoin")
    val distribute = new Array[Long](2048)

    def parent(i: Int) = Math.floor(i / 2).toInt

    def left_child(i: Int) = 2 * i

    def right_child(i: Int) = 2 * i + 1

    def compare(x: (Long, Int), y: (Long, Int)) : Short = {
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

    def minHeapify(A: Array[(Int, (Long, Int), Int)], i: Int): Array[(Int, (Long, Int), Int)] = {
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

    def heapExtractMin(
                        A: Array[(Int, (Long, Int), Int)]
                      ): Tuple2[Tuple3[Int, (Long, Int), Int], Array[(Int, (Long, Int), Int)]] = {
      val heapSize = A.length
      if (heapSize < 1) {
        logInfo(s"heap underflow")
      }
      val AA = A.clone()
      val min = AA(0)
      AA(0) = AA(heapSize - 1)
      Tuple2(min, minHeapify(AA.slice(0, heapSize - 1), 1))
    }

    def heapIncreaseKey(
                         A: Array[(Int, (Long, Int), Int)],
                         i: Int,
                         key: Tuple3[Int, (Long, Int), Int]
                       ): Array[(Int, (Long, Int), Int)] = {
      if (compare(key._2, A(i - 1)._2) > 0) {
        logInfo(s"new key is larger than current Key")
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

    def minHeapInsert(
                       A: Array[(Int, (Long, Int), Int)],
                       key: Tuple3[Int, (Long, Int), Int]
                     ): Array[(Int, (Long, Int), Int)] = {
      val AA = Array.concat(A, Array(key).map(x => (x._1, (Long.MaxValue, Int.MaxValue), x._3)))
      heapIncreaseKey(AA, AA.length, key)
    }

    def buildMinHeap(A: Array[(Int, (Long, Int), Int)]): Array[(Int, (Long, Int), Int)] = {
      var AA = A.clone()
      for (i <- (1 until Math.floor(AA.length / 2).toInt + 1).reverse) {
        AA = minHeapify(AA, i)
      }
      AA
    }

//    def calculateVsl(
//                      s: Int,
//                      l: Int,
//                      indexNum: scala.collection.Map[(Int, Int), Int],
//                      substring: Array[String],
//                      H: Int,
//                      minimum: Int,
//                      threshold: Double,
//                      alpha: Double,
//                      numPartition: Int,
//                      topDegree: Int
//                    ): Array[Int] = {
//
//      val C0 = {
//        for (i <- 1 until H + 1) yield {
//          0
//        }
//      }.toArray
//      val C1 = {
//        for (i <- 1 until H + 1) yield {
//          try {
//            indexNum(((substring(i - 1), i, l).hashCode(), 0))
//          } catch {
//            case e: NoSuchElementException =>
//              0
//          }
//        }
//      }.toArray
//      val C2 = {
//        for (i <- 1 until H + 1) yield {
//          try {
//            C1(i - 1) +
//              indexNum(((substring(i - 1), i, l).hashCode(), 1)) +
//              inverseDel(substring(i - 1), indexNum, i, l, minimum)
//          } catch {
//            case e: NoSuchElementException =>
//              C1(i - 1) + inverseDel(substring(i - 1), indexNum, i, l, minimum)
//          }
//        }
//      }.toArray
//
//      val addToDistributeWhen1 = {
//        for (i <- 1 until H + 1) yield {
//          val hash = (substring(i - 1), i, l).hashCode()
//          val code = (hash % numPartition)
//          val partition = {
//            if (code < 0) {
//              code + numPartition
//            } else {
//              code
//            }
//          }
//          try {
//            (partition, indexNum((hash, 0)))
//          } catch {
//            case e: NoSuchElementException =>
//              (partition, 0)
//          }
//        }
//      }.toArray
//
//      val addToDistributeWhen2 = {
//        for (i <- 1 until H + 1) yield {
//          val hash = (substring(i - 1), i, l).hashCode()
//          val code = (hash % numPartition)
//          val partition = {
//            if (code < 0) {
//              code + numPartition
//            } else {
//              code
//            }
//          }
//          val x = addToMapForInverseDel(substring(i - 1), indexNum, i, l, minimum, numPartition)
//          if (indexNum.contains((hash, 1)) && x != null && x.length > 0) {
//            Array.concat(Array((partition, indexNum((hash, 1)))), x)
//          } else if (indexNum.contains((hash, 1))) {
//            Array((partition, indexNum((hash, 1))))
//          } else if (x != null && x.length > 0) {
//            x
//          } else {
//            Array((partition, 0))
//          }
//        }
//      }.toArray
//
//      val deata_distribute0 = {
//        // 只考虑有变化的reducer的负载
//        for (i <- 0 until H) yield {
//          // 分配到1之后情况比较单一,只有inverseindex 和 inversequery匹配这一种情况,只会对一个reducer产生影响
//          val max = {
//            if (addToDistributeWhen1(i)._2 > 0) {
//              (distribute(addToDistributeWhen1(i)._1) +
//                addToDistributeWhen1(i)._2.toLong)*Math.pow(2, topDegree-1).toLong
//            } else {
//              0.toLong
//            }
//          }
//          max
//        }
//      }.toArray
//
//      val deata_distribute1 = {
//        // 分配到2
//        for (i <- 0 until H) yield {
//          val dis = distribute.slice(0, numPartition).clone()
//          val change = ArrayBuffer[Int]()
//          for (j <- addToDistributeWhen2(i)) {
//            dis(j._1) += j._2.toLong
//            if (j._2 > 0) {
//              change += j._1
//            }
//          }
//          var total = 0.toLong
//          for (i <- 0 until topDegree) {
//            var max = 0.toLong
//            var maxPos = -1
//            var pos = 0
//            for (c <- change) {
//              if (dis(c) >= max) {
//                max = dis(c)
//                maxPos = pos
//              }
//              pos += 1
//            }
//            if (maxPos >= 0) {
//              change.remove(maxPos)
//              total += Math.pow(2, topDegree - i - 1).toLong * max
//            }
//          }
//          total
//        }
//      }.toArray
//
//      val deata0 = {
//        for (i <- 0 until H) yield {
//          Tuple2(deata_distribute0(i), C1(i) - C0(i))
//          //        C1(i) - C0(i)
//        }
//      }.toArray
//
//      val deata1 = {
//        for (i <- 0 until H) yield {
//          Tuple2(deata_distribute1(i), C2(i) - C1(i))
//          //        C2(i) - C1(i)
//        }
//      }.toArray
//
//      val Hls = CalculateH(Math.floor(l / alpha + 0.0001).toInt, s, threshold)
//
//      val V = {
//        for (i <- 1 until H + 1) yield {
//          0
//        }
//      }.toArray
//
//      var M = buildMinHeap(deata0.zipWithIndex.map(x => (0, x._1, x._2)))
//
//      for (j <- 1 until Hls + 1) {
//        val MM = heapExtractMin(M)
//        M = MM._2
//        val pair = MM._1
//        V(pair._3) += 1
//        if (V(pair._3) == 1) {
//          M = minHeapInsert(M, Tuple3(1, deata1(pair._3), pair._3))
//        }
//      }
//
//      for (chooseid <- 0 until H) {
//        if (V(chooseid) == 1) {
//          distribute(addToDistributeWhen1(chooseid)._1) += addToDistributeWhen1(chooseid)._2.toLong
//        } else if (V(chooseid) == 2) {
//          for (j <- addToDistributeWhen2(chooseid)) {
//            distribute(j._1) += j._2.toLong
//          }
//          distribute(addToDistributeWhen1(chooseid)._1) += addToDistributeWhen1(chooseid)._2.toLong
//        }
//      }
//
//      V
//    }

    def part(s: String): Array[Int] = {
      var ss = ArrayBuffer[Int]()
      val U: Int = threshold
      val sss = s.split(" ")
      val l = sss.length
      val K: Int = (l - Math.floor(l / (U + 1)) * (U + 1)).toInt
      var point: Int = 0
      for (i <- 1 until U + 2) {
        if (i <= (U + 1 - K)) {
          val length = Math.floor(l / (U + 1)).toInt
          val seg = sss.slice(point, point + length)
          point = point + length
          ss += ({if (seg.length == 0) "" else seg.reduce(_ + " " + _)}, i, l).hashCode()
        }
        else {
          val length = Math.ceil(l / (U + 1) + 0.001).toInt
          val seg = sss.slice(point, point + length)
          point = point + length
          ss += ({if (seg.length == 0) "" else seg.reduce(_ + " " + _)}, i, l).hashCode()
        }
      }
      ss.toArray
    }// (substring, i, rlength)

    def Lij(l: Int, i: Int): Int = {
      val U = threshold
      val K = (l - Math.floor(l / (U + 1)) * (U + 1)).toInt
      if (i <= (U + 1 - K))
      {
        return Math.floor(l / (U + 1)).toInt
      }
      else
      {
        return (Math.ceil(l / (U + 1)) + 0.001).toInt
      }
    }

    def Pij(l: Int, i: Int, L: scala.collection.Map[(Int, Int), Int]): Int = {
      var p = 0
      for (j <- 1 until i) {
        p = p + L((l, j))
      }
      return p + 1
    }

    def parts(s: String,
              L: Map[(Int, Int), Int],
              P: Map[(Int, Int), Int]): Array[Int] = {
      val result = ArrayBuffer[Int]()

      val sss = s.split(" ")
      val slength = sss.length
      val lu = slength
      val lo = Math.max(slength - threshold, threshold + 1)
      val U = threshold
      val partLength_o = Math.floor(lo / (U + 1)).toInt
      val partLength_u = (Math.ceil(lu / (U + 1)) + 0.0001).toInt
      for (l <- lo until lu + 1) {
        for (i <- 1 until U + 2) {
          for (x <- Math.max(P(i, l) - (i - 1), P(i, l) - (l - slength + (U + 1 - i)))
            until Math.min(P(i, l) + slength - l + U + 1 - i, P(i, l) + i - 1) + 1) {

            val seg = {
              val subset = sss.slice(x - 1, x - 1 + L(l, i))
              if (subset.length == 0) {
                ""
              } else {
                subset.reduce(_ + " " + _)
              }
            }
            result += (seg, i, l).hashCode()
          }
        }
      }
      result.toArray
    }

    def consRecordMap (ss: Array[String]) : Map[String, Int] = {
      var count = 0
      var tempMap = Map[String, Int]()
      for (i <- 0 until ss.size){
        tempMap += (ss(i) -> count)
        count += 1
      }
      tempMap
    }

    def consRecordInverMap (ss: Array[String]) : Map[Int, String] = {
      var count = 0
      var tempMap = Map[Int, String]()
      for (i <- 0 until ss.size){
        tempMap += (count -> ss(i))
        count += 1
      }
      tempMap
    }

    def sortForTwo (a: Int, b: Int): Tuple2[Int, Int] = {
      if (a > b) {
        Tuple2(b, a)
      } else {
        Tuple2(a, b)
      }
    }

    def EDdistance (a: String, b: String) : Int = {
      val str1 = a.split(" ")
      val str2 = b.split(" ")
      val lenStr1 = str1.length
      val lenStr2 = str2.length
      val edit = Array.ofDim[Int](lenStr1, lenStr2)
      for(i <- 0 until lenStr1){
        edit(i)(0) = i
      }
      for(j <- 0 until lenStr2){
        edit(0)(j) = j
      }

      for(i <- 1 until lenStr1) {
        for(j <- 1 until lenStr2) {
          edit(i)(j) = Math.min( edit(i - 1)(j) + 1, edit(i)(j - 1) + 1 )
          if (str1(i - 1) == str2(j - 1)) {
            edit(i)(j) = Math.min( edit(i)(j), edit(i - 1)(j - 1) )
          } else {
            edit(i)(j) = Math.min( edit(i)(j), edit(i - 1)(j - 1) + 1 )
          }
        }
      }
      return edit(lenStr1-1)(lenStr2-1)
    }

    def calculateAllL (min: Int,
                       max: Int): Map[(Int, Int), Int] = {
      val result = Map[(Int, Int), Int]()
      for (l <- min until max + 1) {
        for (i <- 1 until threshold + 2) {
          result += ((l, i) -> Lij(l, i))
        }
      }
      result
    }

    def calculateAllP (min: Int,
                       max: Int,
                       L: scala.collection.Map[(Int, Int), Int]): Map[(Int, Int), Int] = {
      val result = Map[(Int, Int), Int]()
      for (l <- min until max + 1) {
        for (i <- 1 until threshold + 2) {
          result += ((l, i) -> Pij(l, i, L))
        }
      }
      result
    }

    def findSimilarity(query: (Int, String),
                       index: (Map[Int, List[Int]], Array[String]),
                       numPartitions: Int): Array[Tuple2[String, String]] = {
      val result = ArrayBuffer[(String, String)]()
      logInfo(s"" + query._1.toString)
      // this is the partition which I want to search
      if (index._1.contains(query._1)) {
        for (i <- index._1(query._1)) {
          if (compareSimilarity(query._2, index._2(i))) {
            logInfo(s"success")
            result += Tuple2(query._2, index._2(i))
          } else {
            logInfo(s"failed")
          }
        }
      }
      result.toArray
    }

    def compareSimilarity(query: String, index: String): Boolean = {
      logInfo(s"comparing " + query + " and " + index)
      val queryHash = query.hashCode
      val indexHash = index.hashCode

      if (queryHash != indexHash) {
        EDdistance(query, index) < threshold
      } else {
        false
      }
    }

    val left_rdd = left.execute().map(row =>
    {
      try {
        row.getString(0)
      } catch {
        case e: NullPointerException => ""
        case _ => ""
      }
    }).filter(x => (x.length > 0))

    val right_rdd = right.execute().map(row =>
    {
      try {
        row.getString(0)
      } catch {
        case e: NullPointerException => ""
        case _ => ""
      }
    }).filter(x => (x.length > 0))

    val record = left_rdd
      .distinct
      .persist(StorageLevel.DISK_ONLY)

    val indexLength = right_rdd
      .distinct
      .map(x => (x.split(" ").size))
      .persist(StorageLevel.DISK_ONLY)

    val minLength = sparkContext.broadcast(Math.max(indexLength.min, threshold + 1))
    val maxLength = sparkContext.broadcast(indexLength.min)

    logInfo(s"" + minLength.value.toString + " " +
      maxLength.value.toString)

    val partitionL = sparkContext
      .broadcast(calculateAllL(minLength.value, maxLength.value))
    val partitionP = sparkContext
      .broadcast(calculateAllP(minLength.value, maxLength.value, partitionL.value))

    val index_rdd = right_rdd
      .distinct
      .map(x => (x.split(" ").length, x))
      .filter(x => x._1 > threshold)
      .map(x => (x._2, part(x._2)))
      .flatMapValues(x => x)
      .map(x => (x._2, x._1))

    val index_partitioned_rdd = index_rdd
      .partitionBy(new SimilarityHashPartitioner(num_partitions))

    val index_indexed_rdd = index_partitioned_rdd
      .mapPartitions(iter => {
        val data = iter.toArray.distinct
        val index = Map[Int, List[Int]]()
        logInfo(s"data length: " + data.length.toString)
        for (i <- 0 until data.length) {
          if (index.contains(data(i)._1)) {
            val position = index(data(i)._1)
            index += (data(i)._1 -> (position ::: List(i)))
          } else {
            index += (data(i)._1 -> List(i))
          }
        }
        logInfo(s"index length: " + index.size.toString)
        Array((index, data.map(x => x._2))).iterator
      }).persist(StorageLevel.DISK_ONLY)

    val query_rdd = record
      .map(x => (x.split(" ").length, x))
      .map(x => (x._2, parts(x._2, partitionL.value, partitionP.value)))
      .flatMapValues(x => x)
      .map(x => (x._2, x._1))

    val query_partitioned_rdd = query_rdd
      .partitionBy(new SimilarityHashPartitioner(num_partitions))

    query_partitioned_rdd.zipPartitions(index_indexed_rdd) {
      (leftIter, rightIter) => {
        val index = rightIter.next
        logInfo(s"index data length: " + index._2.length.toString)
        leftIter
          .flatMap (row => findSimilarity(row, index, num_partitions))
          .map(x => InternalRow.
            fromSeq(Seq(org.apache.spark.unsafe.types.UTF8String.fromString(x._1.toString),
              org.apache.spark.unsafe.types.UTF8String.fromString(x._2.toString))))
          .toArray.iterator
      }
    }
  }
}


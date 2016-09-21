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
import org.apache.spark.sql.partitioner.{SimilarityHashPartitioner, SimilarityHashPartition}
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map

case class JaccardSimilarityJoinV2(left_keys: Seq[Expression],
                             right_keys: Seq[Expression],
                             l: Literal,
                             left: SparkPlan,
                             right: SparkPlan) extends BinaryNode {
  override def output: Seq[Attribute] = left.output ++ right.output

  final val num_partitions = sqlContext.conf.numSimilarityPartitions.toInt
  final val threshold = sqlContext.conf.similarityJaccardThreshold.toDouble
  final val alpha = sqlContext.conf.similarityMultigroupThreshold.toDouble
  final val topDegree = sqlContext.conf.similarityBalanceTopDegree.toInt

  override protected def doExecute(): RDD[InternalRow] = {
    logInfo(s"execute IndexSimilarityJoin")
    val distribute = new Array[Long](2048)

    //    val left_rdd = left.execute().map(row =>
    //      ((left_keys.map(x => BindReferences.bindReference(x, left.output).eval(row)
    //        .asInstanceOf[String]).toArray)(0), row)
    //    )
    //
    //    val right_rdd = right.execute().map(row =>
    //      ((right_keys.map(x => BindReferences.bindReference(x, right.output).eval(row)
    //        .asInstanceOf[String]).toArray)(0), row)
    //    )

    def CalculateH ( l: Int, s: Int, threshold: Double ) = {
      Math.floor((1 - threshold) * (l + s) / (1 + threshold) + 0.0001).toInt + 1
    }

    def CalculateH1 ( l: Int, threshold: Double ): Int = {
      // 生成分段的段数(按照query长度)
      Math.floor ( (1 - threshold) * l / threshold + 0.0001).toInt + 1
    }

    def createDeletion(ss1: String): Array[String] = {
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

    def inverseDel (
                     xi: String,
                     indexNum: scala.collection.Map [ ( Int, Int ), Int ],
                     ii: Int,
                     ll: Int,
                     minimum: Int
                   ): Int = {
      var total = 0
      if (xi.length == 0) {
        return 0
      }
      for (i <- createDeletion(xi)) {
        val hash = (i, ii, ll).hashCode()
        val hasi = indexNum.contains((hash, 0))
        if (hasi) {
          total = total + indexNum((hash, 0))
        } else {
          total
        }
      }
      total
    }

    def addToMapForInverseDel(
                               xi: String,
                               indexNum: scala.collection.Map[(Int, Int), Int],
                               ii: Int,
                               ll: Int,
                               minimum: Int,
                               numPartition: Int
                             ): Array[(Int, Int)] = {
      if (xi.length == 0) {
        return null
      }
      var result = ArrayBuffer[(Int, Int)]()
      for (i <- createDeletion(xi)) {
        val hash = (i, ii, ll).hashCode()
        val hasi = indexNum.contains((hash, 0))
        val code = (hash % numPartition)
        val partition = {
          if (code < 0) {
            code + numPartition
          } else {
            code
          }
        }
        if (hasi) {
          result += Tuple2(partition, indexNum((hash, 0)))
        }
      }
      result.toArray
    }

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

    def calculateVsl(
                      s: Int,
                      l: Int,
                      indexNum: scala.collection.Map[(Int, Int), Int],
                      substring: Array[String],
                      H: Int,
                      minimum: Int,
                      threshold: Double,
                      alpha: Double,
                      numPartition: Int,
                      topDegree: Int
                    ): Array[Int] = {

      val C0 = {
        for (i <- 1 until H + 1) yield {
          0
        }
      }.toArray
      val C1 = {
        for (i <- 1 until H + 1) yield {
          try {
            indexNum(((substring(i - 1), i, l).hashCode(), 0))
          } catch {
            case e: NoSuchElementException =>
              0
          }
        }
      }.toArray
      val C2 = {
        for (i <- 1 until H + 1) yield {
          try {
            C1(i - 1) +
              indexNum(((substring(i - 1), i, l).hashCode(), 1)) +
              inverseDel(substring(i - 1), indexNum, i, l, minimum)
          } catch {
            case e: NoSuchElementException =>
              C1(i - 1) + inverseDel(substring(i - 1), indexNum, i, l, minimum)
          }
        }
      }.toArray

      val addToDistributeWhen1 = {
        for (i <- 1 until H + 1) yield {
          val hash = (substring(i - 1), i, l).hashCode()
          val code = (hash % numPartition)
          val partition = {
            if (code < 0) {
              code + numPartition
            } else {
              code
            }
          }
          try {
            (partition, indexNum((hash, 0)))
          } catch {
            case e: NoSuchElementException =>
              (partition, 0)
          }
        }
      }.toArray

      val addToDistributeWhen2 = {
        for (i <- 1 until H + 1) yield {
          val hash = (substring(i - 1), i, l).hashCode()
          val code = (hash % numPartition)
          val partition = {
            if (code < 0) {
              code + numPartition
            } else {
              code
            }
          }
          val x = addToMapForInverseDel(substring(i - 1), indexNum, i, l, minimum, numPartition)
          if (indexNum.contains((hash, 1)) && x != null && x.length > 0) {
            Array.concat(Array((partition, indexNum((hash, 1)))), x)
          } else if (indexNum.contains((hash, 1))) {
            Array((partition, indexNum((hash, 1))))
          } else if (x != null && x.length > 0) {
            x
          } else {
            Array((partition, 0))
          }
        }
      }.toArray

      val deata_distribute0 = {
        // 只考虑有变化的reducer的负载
        for (i <- 0 until H) yield {
          // 分配到1之后情况比较单一,只有inverseindex 和 inversequery匹配这一种情况,只会对一个reducer产生影响
          val max = {
            if (addToDistributeWhen1(i)._2 > 0) {
              (distribute(addToDistributeWhen1(i)._1) +
                addToDistributeWhen1(i)._2.toLong)*Math.pow(2, topDegree-1).toLong
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
          for (i <- 0 until topDegree) {
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
              total += Math.pow(2, topDegree - i - 1).toLong * max
            }
          }
          total
        }
      }.toArray

      val deata0 = {
        for (i <- 0 until H) yield {
          Tuple2(deata_distribute0(i), C1(i) - C0(i))
          //        C1(i) - C0(i)
        }
      }.toArray

      val deata1 = {
        for (i <- 0 until H) yield {
          Tuple2(deata_distribute1(i), C2(i) - C1(i))
          //        C2(i) - C1(i)
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
          distribute(addToDistributeWhen1(chooseid)._1) += addToDistributeWhen1(chooseid)._2.toLong
        }
      }

      V
    }


    def partition_r(
                     ss1: String,
                     indexNum: scala.collection.Map[(Int, Int), Int],
                     minimum: Int,
                     group: Array[(Int, Int)],
                     threshold: Double,
                     alpha: Double,
                     partitionNum: Int,
                     topDegree: Int
                   ): Array[(Array[(Array[Int], Array[Boolean])],
      Array[(Int, Boolean, Array[Boolean], Boolean, Int)])] = {
      var result = ArrayBuffer[(Array[(Array[Int], Array[Boolean])],
        Array[(Int, Boolean, Array[Boolean], Boolean, Int)])]()
      logInfo(s"partition_r: " + ss1)
      var ss = ss1.split(" ")
      val s = ss.size
      val range = group.filter(
        x => (x._1 <= Math.floor(s / threshold).toInt &&
          x._2 >= (Math.ceil(threshold * s) + 0.0001).toInt)
      )
      for (lrange <- range) {
        val l = lrange._1
        val isExtend = {
          true
        }

        val H = CalculateH1(l, threshold)

        val records = ArrayBuffer[(Array[Int], Array[Boolean])]()

        val substring = {
          for (i <- 1 until H + 1) yield {
            val p = ss.filter(x => x.hashCode % H + 1 == i)
            if (p.length == 0) "" else if (p.length == 1) p(0) else p.reduce(_ + " " + _)
          }
        }.toArray

        val V = calculateVsl(s,
          l,
          indexNum,
          substring,
          H,
          minimum,
          threshold,
          alpha,
          partitionNum,
          topDegree)

        var V_Info = ""
        for (ii <- V) {
          V_Info += " " + ii.toString
        }
        logInfo(s"V_INFO: " + V_Info)

        for (i <- 1 until H + 1) {
          val p = ss.filter(x => x.hashCode % H + 1 == i)
          records += Tuple2(p.map(x => x.hashCode),
            if (V(i-1)==0) Array() else if (V(i-1)==1) Array(false) else Array(true)
          )
        }

        var result1 = ArrayBuffer[(Int, Boolean, Array[Boolean], Boolean, Int)]()
        for (i <- 1 until H + 1) {
          val hash = (substring(i - 1), i, l).hashCode()
          if (V(i-1) == 1) {
            logInfo(s"inverseQuery: " + substring(i - 1) + " " + i.toString + " " + l.toString)
            result1 += Tuple5(hash, false, Array(false), isExtend, i)
          }
          else if (V(i-1) == 2) {
            logInfo(s"inverseQuery: " + substring(i - 1) + " " + i.toString + " " + l.toString)
            result1 += Tuple5(hash, false, Array(true), isExtend, i)
            if (substring(i - 1).length > 0) {
              for (k <- createDeletion(substring(i - 1))) {
                logInfo(s"deletionQuery: " + k + " " + i.toString + " " + l.toString)
                val hash1 = (k, i, l).hashCode()
                result1 += Tuple5(hash1, true, Array(true), isExtend, i)
              }
            }
          }
        }
        result += Tuple2(records.toArray, result1.toArray)
      }
      result.toArray
      // (hash, isDeletion, V, isExtend)
    }

    def createInverse(ss1: String,
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

    def calculateOverlapBound(t: Float, xl: Int, yl: Int): Int = {
      (Math.ceil((t / (t + 1)) * (xl + yl)) + 0.0001).toInt
    }

    def min(x: Int, y: Int): Int = {
      if (x < y) {
        x
      } else {
        y
      }
    }

    def verify(x: Array[(Array[Int], Array[Boolean])],
               y: Array[(Array[Int], Array[Boolean])],
               threshold: Double,
               pos: Int
              ): Boolean = {
      // 能走到这一步的 l 都是相同的, l 相同, 段数也就相同,所以 x 和 y 长度相同,
      // 需要一段一段匹配.pos 是当前 匹配的键,如果发现这个键的前面还有匹配,那么退出.
      // 判断是否匹配要从两个记录对应段的v值着手0,1,2
      var xLength = 0
      for (i <- x) {
        xLength += i._1.length
      }
      var yLength = 0
      for (i <- y) {
        yLength += i._1.length
      }
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
            return false
          }
        }// before matching
        if (currentOverlap + Math.min((xLength - currentXLength),
          (yLength - currentYLength)) < overlap) {
          return false
        }
      }
      if (currentOverlap >= overlap) {
        return true
      } else {
        return false
      }
    }

    def findSimilarity(query: (Int, ((Int, Array[(Array[Int], Array[Boolean])]),
      Boolean, Array[Boolean], Boolean, Int)), index: (Map[Int, List[Int]],
      Array[((Int, Array[(Array[Int], Array[Boolean])]),
        Boolean, Array[Boolean], Boolean, Int)]), numPartitions: Int) : Array[Tuple2[Int, Int]] = {
      val result = ArrayBuffer[(Int, Int)]()
      //      val code = (query._1 % numPartitions)
      //      val partitionPos = {
      //        if (code < 0) {
      //          code + numPartitions
      //        } else {
      //          code
      //        }
      //      }
      logInfo(s"" + query._1.toString)
      // this is the partition which I want to search
      if (index._1.contains(query._1)) {
        for (i <- index._1(query._1)) {
          if (compareSimilarity(query._2, index._2(i))) {
            logInfo(s"success")
            result += Tuple2(query._2._1._1, index._2(i)._1._1)
          } else {
            logInfo(s"failed")
          }
        }
      }
      result.toArray
    }

    def compareSimilarity(query: ((Int, Array[(Array[Int], Array[Boolean])]),
      Boolean, Array[Boolean], Boolean, Int), index: ((Int, Array[(Array[Int], Array[Boolean])]),
      Boolean, Array[Boolean], Boolean, Int)): Boolean = {
      logInfo(s"comparing " + query._1._1.toString + " and " + index._1._1.toString)
      val pos = query._5
      if (index._2) {
        // it's a deletion index
        if (!query._2 && query._3.length > 0 && query._3(0)) {
          // query from inverse with value 2
          if (query._1._1 != index._1._1) {
            verify(query._1._2, index._1._2, threshold, pos)
          } else {
            false
          }
        } else {
          false
        }
      } else {
        // it's a reverse index
        if (!query._2 && query._3.length > 0) {
          // query from inverse with value 2 or 1
          verify(query._1._2, index._1._2, threshold, pos)
        } else if (query._2 && query._3.length > 0 && query._3(0)) {
          // query from deletion index with value 2
          verify(query._1._2, index._1._2, threshold, pos)
        } else {
          false
        }
      }
    }

    def Descartes(
                   pos: Int,
                   x: Array[((Int, Array[(Array[Int], Array[Boolean])]),
                     Boolean, Array[Boolean], Boolean)], threshold: Double
                 ) : ArrayBuffer[(Int, Int)] = {
      var result = ArrayBuffer[(Int, Int)]()

      val index = x
        .filter(
          s => !s._2 && !s._4
        )
        .map(x => x._1)
        .distinct
      val deletionIndex = x
        .filter(s => s._2 && !s._4)
        .map(x => x._1)
        .distinct
      val query1 = x
        .filter(s => !s._2 && s._3.length == 1 && !s._3(0) && s._4)
        .map(x => x._1)
        .distinct
      val query2 = x
        .filter(s => !s._2 && s._3.length == 1 && s._3(0) && s._4)
        .map(x => x._1)
        .distinct
      val query3 = x
        .filter(s => s._2 && s._3.length == 1 && s._3(0) && s._4)
        .map(x => x._1)
        .distinct

      for (i <- index; j <- query1) {
        logInfo(s"verify: " + i._1 + " " + j._1)
        if (verify(i._2, j._2, threshold, pos)) {
          result += Tuple2(i._1, j._1)
        }
      }
      //      System.gc()
      for (i <- deletionIndex; j <- query2) {
        logInfo(s"verify: " + i._1 + " " + j._1)
        if (verify(i._2, j._2, threshold, pos)) {
          result += Tuple2(i._1, j._1)
        }
      }
      for (i <- index; j <- query2) {
        logInfo(s"verify: " + i._1 + " " + j._1)
        if (verify(i._2, j._2, threshold, pos)) {
          result += Tuple2(i._1, j._1)
        }
      }
      for (i <- index; j <- query3) {
        logInfo(s"verify: " + i._1 + " " + j._1)
        if (verify(i._2, j._2, threshold, pos)) {
          result += Tuple2(i._1, j._1)
        }
      }
      // println(result.length)
      result
    }

    def multigroup(mini: Int, maxi: Int, threshold: Double, alpha : Double): Array[(Int, Int)] = {
      var result = ArrayBuffer[(Int, Int)]()
      var l = mini
      while (l <= maxi) {
        val l1 = Math.floor(l / alpha + 0.0001).toInt
        result += Tuple2(l, l1)
        l = l1 + 1
      }
      result.toArray
    }

    val left_rdd = left.execute().map(row =>
    {
      try {
        row.getString(0)
      } catch {
        case e: NullPointerException => ""
        case _ => ""
      }
    }
    ).filter(x => (x.length > 0))

    val right_rdd = right.execute().map(row =>
    {
      try {
        row.getString(0)
      } catch {
        case e: NullPointerException => ""
        case _ => ""
      }
    }
    ).filter(x => (x.length > 0))

    val rdd1 = right_rdd.map(x => (x.split(" ").size)).persist(StorageLevel.DISK_ONLY)
    val minimum = sparkContext.broadcast(rdd1.min())
    val maximum = sparkContext.broadcast(rdd1.max())
    val count = sparkContext.broadcast(rdd1.count())
    val average = sparkContext.broadcast(rdd1.sum() / count.value)
    rdd1.unpersist()

    logInfo(s"" + minimum.value.toString +
      maximum.value.toString +
      average.value.toString +
      count.value.toString)
    val record = right_rdd
      .map(x => sortByValue(x))
      .distinct
      .persist(StorageLevel.DISK_ONLY)
    // .map(x => mapTokenToId(tokenMapId.value, x))
    val multiGroup = sparkContext.broadcast(multigroup(minimum.value, maximum.value, threshold, alpha))
    val recordidMapSubstring = record
      .map(x => {
        logInfo(s"" + x)
        val inverse = createInverse(x, multiGroup.value, threshold)
        ((x.hashCode,
          inverse.map(s => (s._1.split(" ").map(ss => ss.hashCode), Array(false)))), inverse)
      })
      .flatMapValues(x => x)
      .map(x => (x._2._1, (x._1, x._2._2, x._2._3)))
      .map(x => (x._2, x._1))
      .persist(StorageLevel.DISK_ONLY)
    // (1,i,l), substring
    val deletionMapRecord = recordidMapSubstring
      .filter(x => (x._2.length > 0))
      .flatMapValues(x => createDeletion(x)) // (string,i,l), deletionSubstring
      .map(x => ((x._2, x._1._2, x._1._3), x._1._1))
      .persist(StorageLevel.DISK_ONLY)// ((deletionSubstring, i, l), string)
    val inverseMapRecord = recordidMapSubstring
        .map(x => ((x._2, x._1._2, x._1._3), x._1._1))
        .persist(StorageLevel.DISK_ONLY)// ((inverseSubstring, i, l), string)
    val indexNum = sparkContext.broadcast(
        inverseMapRecord
          .map(x => ((x._1.hashCode, 0), 1))
          .union(deletionMapRecord.map(x => ((x._1.hashCode(), 1), 1)))
          .reduceByKey(_ + _)
          .filter(x => (x._2 > 2))
          .collectAsMap()
      )
    recordidMapSubstring.unpersist()


    val index_rdd = deletionMapRecord
      .map(x => {
        logInfo(s"deletionIndex: " + x._1._1 + " " + x._1._2.toString + " " + x._1._3.toString)
        ((x._1.hashCode(), true, Array(false), false, x._1._2), x._2)
      })
      .union(
        inverseMapRecord
          .map(x => {
            logInfo(s"inverseIndex: " + x._1._1 + " " + x._1._2.toString + " " + x._1._3.toString)
            ((x._1.hashCode(), false, Array(false), false, x._1._2), x._2)
          })
      )
      .map(s => (s._1._1, (s._2, s._1._2, s._1._3, s._1._4, s._1._5)))


    val query_rdd = left_rdd
      // .filter(x => {val l = x.split(" "); l.length < 500 && l(0).length > 0})*/
      .map(x => sortByValue(x))
      .distinct
      .map(x => (x.hashCode,
        partition_r(
          x, indexNum.value, minimum.value, multiGroup.value,
          threshold, alpha, num_partitions, topDegree
        )))
      // (id, Array(String, Array(subid, isDeletion, V, isExtend, i)))
      .flatMapValues(x => x)
      .map(x => ((x._1, x._2._1), x._2._2))
      .flatMapValues(x => x)
      .map(x => (x._2._1, (x._1, x._2._2, x._2._3, x._2._4, x._2._5)))
      .persist(StorageLevel.DISK_ONLY)
    // .union(index_rdd.map(s => ((s._1._1, s._1._5), (s._2, s._1._2, s._1._3, s._1._4))))

    val index_rdd_partitioned =
      index_rdd.partitionBy(new SimilarityHashPartitioner(num_partitions))

    val index_rdd_indexed = index_rdd_partitioned
      .mapPartitions(iter => {
        val data = iter.toArray
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

    val query_rdd_partitioned =
      query_rdd
        .partitionBy(new SimilarityHashPartitioner(num_partitions))
        .persist(StorageLevel.DISK_ONLY)

    query_rdd_partitioned.zipPartitions(index_rdd_indexed) {
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

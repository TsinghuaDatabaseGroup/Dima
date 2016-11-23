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
package org.apache.spark.sql.SimilarityProbe

/**
  * Created by sunji on 16/10/15.
  */

import scala.collection.mutable.ArrayBuffer
object JaccardSimSegmentation {
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
  private[sql] def CalculateH ( l: Int, s: Int, threshold: Double ): Int = {
    Math.floor((1 - threshold) * (l + s) / (1 + threshold) + 0.0001).toInt + 1
  }
  private[sql] def CalculateH1 ( l: Int, threshold: Double ): Int = {
    // 生成分段的段数(按照query长度)
    Math.floor ( (1 - threshold) * l / threshold + 0.0001).toInt + 1
  }
  private[sql] def inverseDel (
                   xi: String,
                   indexNum: scala.collection.Map[(Int, Boolean), Long],
                   ii: Int,
                   ll: Int,
                   minimum: Int
                 ): Long = {
    var total = 0L
    if (xi.length == 0) {
      return 0L
    }
    for (i <- createDeletion(xi)) {
      val hash = (i, ii, ll).hashCode()
      total = total + indexNum.getOrElse((hash, false), 0L)
    }
    total
  }

  private[sql] def addToMapForInverseDel(
                             xi: String,
                             indexNum: scala.collection.Map[(Int, Int), Int],
                             ii: Int,
                             ll: Int,
                             minimum: Int,
                             numPartition: Int
                           ): Array[(Int, Int)] = {
    if (xi.length == 0) {
      return Array[(Int, Int)]()
    }
    var result = ArrayBuffer[(Int, Int)]()
    for (i <- createDeletion(xi)) {
      val hash = (i, ii, ll).hashCode()
      val code = (hash % numPartition)
      val partition = {
        if (code < 0) {
          code + numPartition
        } else {
          code
        }
      }
      result += Tuple2(partition, indexNum.getOrElse((hash, 0), 0))
    }
    result.toArray
  }

  private def parent(i: Int) = Math.floor(i / 2).toInt

  private def left_child(i: Int) = 2 * i

  private def right_child(i: Int) = 2 * i + 1

  private def compare(x: Long, y: Long) : Short = {
    if (x > y) {
      1
    } else if (x < y) {
      -1
    } else {
      0
    }
  }

  private def minHeapify(A: Array[(Int, Long, Int)], i: Int):
  Array[(Int, Long, Int)] = {
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

  private def heapExtractMin(
                              A: Array[(Int, Long, Int)]
                            ): Tuple2[Tuple3[Int, Long, Int], Array[(Int, Long, Int)]] = {
    val heapSize = A.length
    if (heapSize < 1) {
      println(s"heap underflow")
    }
    val AA = A.clone()
    val min = AA(0)
    AA(0) = AA(heapSize - 1)
    Tuple2(min, minHeapify(AA.slice(0, heapSize - 1), 1))
  }

  private def heapIncreaseKey(
                               A: Array[(Int, Long, Int)],
                               i: Int,
                               key: Tuple3[Int, Long, Int]
                             ): Array[(Int, Long, Int)] = {
    if (compare(key._2, A(i - 1)._2) > 0) {
      println(s"new key is larger than current Key")
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

  private def minHeapInsert(
                             A: Array[(Int, Long, Int)],
                             key: Tuple3[Int, Long, Int]
                           ): Array[(Int, Long, Int)] = {
    val AA = Array.concat(A, Array(key).map(x => (x._1, Long.MaxValue, x._3)))
    heapIncreaseKey(AA, AA.length, key)
  }

  private def buildMinHeap(A: Array[(Int, Long, Int)]): Array[(Int, Long, Int)] = {
    var AA = A.clone()
    for (i <- (1 until Math.floor(AA.length / 2).toInt + 1).reverse) {
      AA = minHeapify(AA, i)
    }
    AA
  }
  def calculateVsl(
                    s: Int,
                    l: Int,
                    indexNum: scala.collection.Map[(Int, Boolean), Long],
                    substring: Array[String],
                    H: Int,
                    minimum: Int,
                    threshold: Double,
                    alpha: Double,
                    numPartition: Int
                  ): Array[Int] = {

    val C0 = {
      for (i <- 1 until H + 1) yield {
        0L
      }
    }.toArray
    val C1 = {
      for (i <- 1 until H + 1) yield {
        val key = ((substring(i - 1), i, l).hashCode(), false)
        indexNum.getOrElse(key, 0L)
      }
    }.toArray
    val C2 = {
      for (i <- 1 until H + 1) yield {
        val key = ((substring(i - 1), i, l).hashCode(), true)
        C1(i - 1) +
          indexNum.getOrElse(key, 0L) +
          inverseDel(substring(i - 1), indexNum, i, l, minimum)
      }
    }.toArray

    val deata0 = {
      for (i <- 0 until H) yield {
        C1(i) - C0(i)
        //        C1(i) - C0(i)
      }
    }.toArray

    val deata1 = {
      for (i <- 0 until H) yield {
        C2(i) - C1(i)
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

    V
  }


  private[sql] def partition_r(
                   ss1: String,
                   indexNum: scala.collection.Map[(Int, Boolean), Long],
                   minimum: Int,
                   group: Array[(Int, Int)],
                   threshold: Double,
                   alpha: Double,
                   partitionNum: Int
                 ): Array[(Array[(Array[Int], Array[Boolean])],
    Array[(Int, Boolean, Array[Boolean], Boolean, Int)])] = {
    var result = ArrayBuffer[(Array[(Array[Int], Array[Boolean])],
      Array[(Int, Boolean, Array[Boolean],
        Boolean,
        Int)])]()
    var ss = ss1.split(" ")
    val s = ss.size
    val range = group
      .filter(x => !(x._1 > Math.floor(s / threshold).toInt || x._2 < (Math.ceil(threshold * s) + 0.0001).toInt))
    for (lrange <- range) {
      val l = lrange._1
      val isExtend = {
        if (l == range(range.length-1)._1) {
          false
        }
        else {
          true
        }
      }

      val H = CalculateH1(l, threshold)

      val records = ArrayBuffer[(Array[Int], Array[Boolean])]()

      val substring = {
        for (i <- 1 until H + 1) yield {
          val p = ss.filter(x => x.hashCode % H + 1 == i)
          if (p.length == 0) "" else if (p.length == 1) p(0) else p.reduce(_ + " " + _)
        }
      }.toArray

      //      println(ss1)
      val V = calculateVsl(s,
        l,
        indexNum,
        substring,
        H,
        minimum,
        threshold,
        alpha,
        partitionNum
      )

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
        val sub = substring(i-1)
        if (V(i-1) == 1) {
//          println(s"value = 1 inverse and substring: $sub, i: $i, l: $l")
          result1 += Tuple5(hash, false, Array(false), isExtend, i)
        }
        else if (V(i-1) == 2) {
          result1 += Tuple5(hash, false, Array(true), isExtend, i)
//          println(s"value = 2 inverse and substring: $sub, i: $i, l: $l")
          if (substring(i - 1).length > 0) {
            for (k <- createDeletion(substring(i - 1))) {
              val hash1 = (k, i, l).hashCode()
//              println(s"value = 2 deletion and substring: $k, i: $i, l: $l")
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

  private[sql] def calculateOverlapBound(t: Float, xl: Int, yl: Int): Int = {
    (Math.ceil((t / (t + 1)) * (xl + yl)) + 0.0001).toInt
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


  private[sql] def sort(xs: Array[String]): Array[String] = {
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

  private[sql] def sortByValue(x: String): String = {
    sort(x.split(" ")).reduce(_ + " " + _)
  }

  private[sql] def sendLocation(key: Int, numPartitions: Int): Int = {
    val code = (key % numPartitions)
    if (code < 0) {
      code + numPartitions
    } else {
      code
    }
  }
}

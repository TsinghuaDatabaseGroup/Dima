/*
 *  Copyright 2016 by Ji Sun
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

// scalastyle:off println

package org.apache.spark.sql.index


import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable


case class Extra(frequencyTable: Broadcast[scala.collection.Map[(Int, Boolean), Int]],
  multiGroup: Broadcast[Array[(Int, Int)]], minimum: Int, alpha: Double, partitionNum: Int)

class JaccardIndex() extends Index with Serializable {
  val index = scala.collection.mutable.Map[Int, List[Int]]()
  var threshold = 0.0
  var extra: Extra = null

  def CalculateH ( l: Int, s: Int, threshold: Double ): Int = {
    Math.floor((1 - threshold) * (l + s) / (1 + threshold) + 0.0001).toInt + 1
  }
  def CalculateH1 ( l: Int, threshold: Double ): Int = {
    // 生成分段的段数(按照query长度)
    Math.floor ( (1 - threshold) * l / threshold + 0.0001).toInt + 1
  }

  def calculateOverlapBound(t: Float, xl: Int, yl: Int): Int = {
    (Math.ceil((t / (t + 1)) * (xl + yl)) + 0.0001).toInt
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

  def compareSimilarity(query: (Array[(Array[Int], Array[Boolean])],
    Boolean, Array[Boolean], Boolean, Int), index: (Array[(Array[Int], Array[Boolean])],
    Boolean)): Boolean = {
    val pos = query._5
    if ((query._3.length > 0 && query._3(0)) ||
      (query._3.length > 0 && !query._3(0) && !index._2)) {
//      println(s"Begin Verification")
      verify(query._1, index._1, threshold, pos)
    } else {
      false
    }
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

  def init(threshold: Double,
    frequencyT: Broadcast[scala.collection.Map[(Int, Boolean), Int]],
    multiG: Broadcast[Array[(Int, Int)]],
    minimum: Int,
    alpha: Double,
    partitionNum: Int): Unit = {
    this.threshold = threshold
    this.extra = Extra(frequencyT, multiG, minimum, alpha, partitionNum)
  }

  def addIndex(key: Int, position: Int): Unit = {
    index += (key -> (position :: index.getOrElse(key, List())))
  }

  def findIndex(data: Array[((String, InternalRow), Boolean)],
                key: Array[(Array[(Array[Int], Array[Boolean])],
                  Array[(Int, Boolean, Array[Boolean], Boolean, Int)])],
                t: Double): Array[InternalRow] = {

    val ans = mutable.ListBuffer[InternalRow]()

    for (query <- key) {
      for (i <- query._2) {
        val position = index.getOrElse(i._1, List())
        for (p <- position) {
//          println(s"Found in Index")
          val que = (query._1, i._2, i._3, i._4, i._5)
          val Ind = (createInverse(sortByValue(data(p)._1._1), extra.multiGroup.value, threshold)
            .map(x => (x._1.split(" ").map(s => s.hashCode), Array[Boolean]())), data(p)._2)
          if (compareSimilarity(que, Ind)) {
            ans += data(p)._1._2
          }
        }
      }
    }
    ans.toArray
  }
}

object JaccardIndex {
  def apply(data: Array[(Int, ((String, InternalRow), Boolean))],
            threshold: Double,
            frequencyT: Broadcast[scala.collection.Map[(Int, Boolean), Int]],
            multiG: Broadcast[Array[(Int, Int)]],
            minimum: Int,
            alpha: Double,
            partitionNum: Int): JaccardIndex = {
    val res = new JaccardIndex()
    res.init(threshold, frequencyT, multiG, minimum, alpha, partitionNum)
    for (i <- 0 until data.length) {
      res.addIndex(data(i)._1, i)
    }
    res
  }
}
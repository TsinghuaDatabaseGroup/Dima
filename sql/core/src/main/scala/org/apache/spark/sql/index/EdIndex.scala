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

/**
  * Created by sunji on 16/11/17.
  */

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SimilarityProbe.ValueInfo
import scala.collection.mutable


case class EdExtra(frequencyTable: Broadcast[scala.collection.Map[(Int, Boolean), Long]],
                 minimum: Int, partitionNum: Int)

class EdIndex() extends Index with Serializable {
  val index = scala.collection.mutable.Map[Int, List[Int]]()
  var threshold = 0
  var extra: EdExtra = null

  private def compareSimilarity(query: ValueInfo, index: ValueInfo, threshold: Int): Boolean = {
    val queryHash = query.record.hashCode
    val indexHash = index.record.hashCode

    if (!(query.isDeletion ^ index.isDeletion)) {
      EDdistance(query.record, index.record) <= threshold
    } else {
      false
    }
  }

  private def EDdistance(a: String, b: String): Int = {
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

  def init(threshold: Int,
           frequencyT: Broadcast[scala.collection.Map[(Int, Boolean), Long]],
           minimum: Int,
           partitionNum: Int): Unit = {
    this.threshold = threshold
    this.extra = EdExtra(frequencyT, minimum, partitionNum)
  }

  def addIndex(key: Int, position: Int): Unit = {
    index += (key -> (position :: index.getOrElse(key, List())))
  }

  def findIndex(data: Array[ValueInfo],
                key: Array[(Int, ValueInfo)],
                t: Int): Array[InternalRow] = {

    val ans = mutable.ListBuffer[InternalRow]()

    for (query <- key) {
      val position = index.getOrElse(query._1, List())
      for (p <- position) {
        //          println(s"Found in Index")
        if (compareSimilarity(query._2, data(p), t)) {
          ans += data(p).content
        }
      }
    }
    ans.toArray
  }
}

object EdIndex {
  def apply(data: Array[(Int, ValueInfo)],
            threshold: Int,
            frequencyT: Broadcast[scala.collection.Map[(Int, Boolean), Long]],
            minimum: Int,
            partitionNum: Int): EdIndex = {
    val res = new EdIndex()
    res.init(threshold, frequencyT, minimum, partitionNum)
    for (i <- 0 until data.length) {
      res.addIndex(data(i)._1, i)
    }
    res
  }
}
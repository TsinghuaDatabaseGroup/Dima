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
package org.apache.spark.sql.simjointopk.util

import org.apache.spark.sql.catalyst.InternalRow

import scala.collection.mutable.ListBuffer

/**
  * Created by sunji on 17/1/9.
  */
class Index extends Serializable{
  val index = scala.collection.mutable.Map[Double, Int]()
  val data = ListBuffer[MinHeap]()
  val emptyMinHeap = new MinHeap()
  var minSim = 1.0
  var totalNum = 0

  def minSimDoublePlus(sim: Double): Double = {
    if (sim < 1.0) {
      val r = sim + 0.01
      f"$r%1.2f".toDouble
    } else {
      1.0
    }
  }

  def minSimDoubleMinus(sim: Double): Double = {
    if (sim > 0.0) {
      val r = sim - 0.01
      f"$r%1.2f".toDouble
    } else {
      0.0
    }
  }

  def startUpAddItem(similarity: Double, key: JaccardPair): Unit = {
    val k = f"$similarity%1.2f".toDouble
//    println(s"add $k bucket")
    if (k < minSim) {
      minSim = k
    }
    val pos = index.getOrElse(k, -1)
    if (pos >= 0) {
      data(pos).minHeapInsert(key)
    } else {
      data += MinHeap(Array(key))
      index += (k -> (data.length-1))
    }
    totalNum += 1
  }

  def addItem(similarity: Double, key: JaccardPair): Unit = {
    if (similarity < minSim) {
      return
    }
    val k = f"$similarity%1.2f".toDouble
    val pos = index.getOrElse(k, -1)
    if (pos >= 0) {
      data(pos).minHeapInsert(key)
    } else {
      data += MinHeap(Array(key))
      index += (k -> (data.length-1))
    }
    totalNum += 1
//    println(s"total = $totalNum")
    extractMinInRange(minSim)
  }

  def searchItem(sim: Double): (MinHeap, Int) = {
    val pos = index.getOrElse(sim, -1)
//    println(s"search $sim, result $pos")
    if (pos == -1) {
      return (emptyMinHeap, pos)
    } else {
      return (data(pos), pos)
    }
  }

  def getRangeLength(sim: Double): Int = {
    searchItem(sim)._1.getLength()
  }

  def extractMinInRange(sim: Double): JaccardPair = {
    totalNum -= 1
    val item = searchItem(sim)
    val result = item._1.heapExtractMin()
    if (data(item._2).getLength() == 0) {
      index.remove(sim)
      if (sim == minSim) {
        minSim = minSimDoublePlus(minSim)
        while (!index.contains(minSim) && minSim < 1.0) {
//          println(s"minSim: $minSim")
          minSim = minSimDoublePlus(minSim)
        }
      }
    }
    result
  }

  def getMinInRange(sim: Double): JaccardPair = {
    searchItem(sim)._1.getExtractMin()
  }

  def getMinSim(): Double = {
    searchItem(minSim)._1.getExtractMin().delta
  }

  def getTotalNum(): Int = {
    totalNum
  }

  def extractMin(): JaccardPair = {
    extractMinInRange(minSim)
  }

  def isEmptyInRange(sim: Double): Boolean = {
    searchItem(sim)._1.getLength() == 0
  }

  def isEmpty(): Boolean = {
    totalNum == 0
  }

  def curveToK(K: Int): Index = {
    while (totalNum > K) {
      extractMin()
    }
    this
  }

  def getAll(): Iterator[JaccardPair] = {
    var x = Array[JaccardPair]()
    for (i <- data) {
      x = Array.concat(x, i.heap)
    }
    x.iterator
  }

  def getAllInRange(sim: Double): Iterator[JaccardPair] = {
    searchItem(sim)._1.heap.iterator
  }

  def copy(): Index = {
    val x = new Index()
    for (i <- this.index) {
      x.index += (i._1 -> i._2)
    }
    for (i <- this.data) {
      x.data += i.copy()
    }
    x.minSim = this.minSim
    x.totalNum = this.totalNum
    x
  }
}

object Index {
  def apply(x: Array[JaccardPair] = Array()): Index = {
    val result = new Index
    for (d <- x) {
//      println("add " + d._2)
      result.startUpAddItem(d.delta, d)
    }
    result
  }
}

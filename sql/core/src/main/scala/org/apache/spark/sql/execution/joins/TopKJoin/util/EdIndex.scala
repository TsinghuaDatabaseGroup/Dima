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
import org.apache.spark.sql.simjointopk.util.MinHeap

import scala.collection.mutable.ListBuffer

/**
  * Created by sunji on 17/1/9.
  */
class EdIndex extends Serializable{
  val index = scala.collection.mutable.Map[Int, Int]()
  val data = ListBuffer[MaxHeap]()
  val emptyMaxHeap = new MaxHeap()
  var maxDis = 0
  var totalNum = 0

  def startUpAddItem(distance: Int, key: EdPair): Unit = {
    val k = distance
    //    println(s"add $k bucket")
    if (k > maxDis) {
      maxDis = k
    }
    val pos = index.getOrElse(k, -1)
    if (pos >= 0) {
      data(pos).maxHeapInsert(key)
    } else {
      data += MaxHeap(Array(key))
      index += (k -> (data.length-1))
    }
    totalNum += 1
  }

  def addItem(distance: Int, key: EdPair): Unit = {
    if (distance > maxDis) {
      return
    }
    val k = distance
    val pos = index.getOrElse(k, -1)
    if (pos >= 0) {
      data(pos).maxHeapInsert(key)
    } else {
      data += MaxHeap(Array(key))
      index += (k -> (data.length-1))
    }
    totalNum += 1
    //    println(s"total = $totalNum")
    extractMaxInRange(maxDis)
  }

  def searchItem(dis: Int): (MaxHeap, Int) = {
    val pos = index.getOrElse(dis, -1)
    //    println(s"search $sim, result $pos")
    if (pos == -1) {
      return (emptyMaxHeap, pos)
    } else {
      return (data(pos), pos)
    }
  }

  def getRangeLength(dis: Int): Int = {
    searchItem(dis)._1.getLength()
  }

  def extractMaxInRange(dis: Int): EdPair = {
    totalNum -= 1
    val item = searchItem(dis)
    val result = item._1.heapExtractMax()
    if (data(item._2).getLength() == 0) {
      index.remove(dis)
      if (dis == maxDis) {
        maxDis -= 1
        while (!index.contains(maxDis) && maxDis > 0) {
          //          println(s"minSim: $minSim")
          maxDis -= 1
        }
      }
    }
    result
  }

  def getMaxInRange(dis: Int): EdPair = {
    searchItem(dis)._1.getExtractMax()
  }

  def getMaxDis(): Int = {
    searchItem(maxDis)._1.getExtractMax().delta
  }

  def getTotalNum(): Int = {
    totalNum
  }

  def extractMax(): EdPair = {
    extractMaxInRange(maxDis)
  }

  def isEmptyInRange(dis: Int): Boolean = {
    searchItem(dis)._1.getLength() == 0
  }

  def isEmpty(): Boolean = {
    totalNum == 0
  }

  def curveToK(K: Int): EdIndex = {
    while (totalNum > K) {
      extractMax()
    }
    this
  }

  def getAll(): Iterator[EdPair] = {
    var x = Array[EdPair]()
    for (i <- data) {
      x = Array.concat(x, i.heap)
    }
    x.iterator
  }

  def getAllInRange(dis: Int): Iterator[EdPair] = {
    searchItem(dis)._1.heap.iterator
  }

  def copy(): EdIndex = {
    val x = new EdIndex()
    for (i <- this.index) {
      x.index += (i._1 -> i._2)
    }
    for (i <- this.data) {
      x.data += i.copy()
    }
    x.maxDis = this.maxDis
    x.totalNum = this.totalNum
    x
  }
}

object EdIndex {
  def apply(x: Array[EdPair] = Array()): EdIndex = {
    val result = new EdIndex
    for (d <- x) {
      //      println("add " + d._2)
      result.startUpAddItem(d.delta, d)
    }
    result
  }
}


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
  * Created by sunji on 16/11/25.
  */

case class JaccardPair(r: (String, InternalRow), s: (String, InternalRow), delta: Double)

class MinHeap extends Serializable{

  var heap = Array[JaccardPair]()

  private[sql] def parent(i: Int) = Math.floor(i / 2).toInt

  private[sql] def left_child(i: Int) = 2 * i

  private[sql] def right_child(i: Int) = 2 * i + 1

  private[sql] def minHeapify(A: Array[JaccardPair], i: Int): Array[JaccardPair] = {
    val l = left_child(i)
    val r = right_child(i)
    val AA = A.clone()
    val smallest = {
      if (l <= AA.length && Global.compare(AA(l - 1), AA(i - 1)) < 0) {
        if (r <= AA.length && Global.compare(AA(r - 1), AA(l - 1)) < 0) {
          r
        } else {
          l
        }
      }
      else {
        if (r <= AA.length && Global.compare(AA(r - 1), AA(i - 1)) < 0) {
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

  private[sql] def heapExtractMin(): JaccardPair = {
    val heapSize = heap.length
    if (heapSize < 1) {
      //      logInfo(s"heap underflow")
    }
    val min = heap(0)
    heap(0) = heap(heapSize - 1)
    heap = minHeapify(heap.slice(0, heapSize - 1), 1)
    min
  }

  private[sql] def getExtractMin(): JaccardPair = {
    heap(0)
  }

  private[sql] def heapIncreaseKey(
                       A: Array[JaccardPair],
                       i: Int,
                       key: JaccardPair
                     ): Array[JaccardPair] = {
    if (Global.compare(key, A(i - 1)) > 0) {
      //      logInfo(s"new key is larger than current Key")
    }
    val AA = A.clone()
    AA(i - 1) = key
    var ii = i
    while (ii > 1 && Global.compare(AA(parent(ii) - 1), AA(ii - 1)) > 0) {
      val temp = AA(ii - 1)
      AA(ii - 1) = AA(parent(ii) - 1)
      AA(parent(ii) - 1) = temp
      ii = parent(ii)
    }
    AA
  }

  private[sql] def minHeapInsert(
                     key: JaccardPair
                   ): Unit = {
    val AA = Array.concat(heap, Array(key).map(x => JaccardPair(x.r, x.s, 1.0)))
    heap = heapIncreaseKey(AA, AA.length, key)
  }

  private[sql] def buildMinHeap(A: Array[JaccardPair]): Unit = {
    heap = A.clone()
    for (i <- (1 until Math.floor(A.length / 2).toInt + 1).reverse) {
      heap = minHeapify(heap, i)
    }
  }

  private[sql] def getLength(): Int = {
    heap.length
  }

  private[sql] def copy(): MinHeap = {
    val x = new MinHeap
    val temp = ListBuffer[JaccardPair]()
    for (i <- this.heap) {
      temp += i
    }
    x.heap = temp.toArray
    x
  }
}



object MinHeap {
  def apply(data: Array[JaccardPair]): MinHeap = {
    val x = new MinHeap()
//    data.foreach(x => println(x._2))
    x.buildMinHeap(data)
    x
  }
}

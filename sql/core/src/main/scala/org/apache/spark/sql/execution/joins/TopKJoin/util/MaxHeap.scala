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
case class EdPair(r: (String, InternalRow), s: (String, InternalRow), delta: Int)

class MaxHeap extends Serializable{

  var heap = Array[EdPair]()

  private[sql] def parent(i: Int) = Math.floor(i / 2).toInt

  private[sql] def left_child(i: Int) = 2 * i

  private[sql] def right_child(i: Int) = 2 * i + 1

  private[sql] def maxHeapify(A: Array[EdPair], i: Int): Array[EdPair] = {
    val l = left_child(i)
    val r = right_child(i)
    val AA = A.clone()
    val greatest = {
      if (l <= AA.length && EdGlobal.compare(AA(l - 1), AA(i - 1)) > 0) {
        if (r <= AA.length && EdGlobal.compare(AA(r - 1), AA(l - 1)) > 0) {
          r
        } else {
          l
        }
      }
      else {
        if (r <= AA.length && EdGlobal.compare(AA(r - 1), AA(i - 1)) > 0) {
          r
        } else {
          i
        }
      }
    }
    if (greatest != i) {
      val temp = AA(i - 1)
      AA(i - 1) = AA(greatest - 1)
      AA(greatest - 1) = temp
      maxHeapify(AA, greatest)
    } else {
      AA
    }
  }

  private[sql] def heapExtractMax(): EdPair = {
    val heapSize = heap.length
    if (heapSize < 1) {
      //      logInfo(s"heap underflow")
    }
    val max = heap(0)
    heap(0) = heap(heapSize - 1)
    heap = maxHeapify(heap.slice(0, heapSize - 1), 1)
    max
  }

  private[sql] def getExtractMax(): EdPair = {
    heap(0)
  }

  private[sql] def heapIncreaseKey(
                       A: Array[EdPair],
                       i: Int,
                       key: EdPair
                     ): Array[EdPair] = {
    if (EdGlobal.compare(key, A(i - 1)) < 0) {
      //      logInfo(s"new key is larger than current Key")
    }
    val AA = A.clone()
    AA(i - 1) = key
    var ii = i
    while (ii > 1 && EdGlobal.compare(AA(parent(ii) - 1), AA(ii - 1)) < 0) {
      val temp = AA(ii - 1)
      AA(ii - 1) = AA(parent(ii) - 1)
      AA(parent(ii) - 1) = temp
      ii = parent(ii)
    }
    AA
  }

  private[sql] def maxHeapInsert(
                     key: EdPair
                   ): Unit = {
    val AA = Array.concat(heap, Array(key).map(x => EdPair(x.r, x.s, 0)))
    heap = heapIncreaseKey(AA, AA.length, key)
  }

  private[sql] def buildMaxHeap(A: Array[EdPair]): Unit = {
    heap = A.clone()
    for (i <- (1 until Math.floor(A.length / 2).toInt + 1).reverse) {
      heap = maxHeapify(heap, i)
    }
  }

  private[sql] def getLength(): Int = {
    heap.length
  }

  private[sql] def copy(): MaxHeap = {
    val x = new MaxHeap
    val temp = ListBuffer[EdPair]()
    for (i <- this.heap) {
      temp += i
    }
    x.heap = temp.toArray
    x
  }
}

object MaxHeap {
  def apply(data: Array[EdPair]): MaxHeap = {
    val x = new MaxHeap()
    //    data.foreach(x => println(x._2))
    x.buildMaxHeap(data)
    x
  }
}

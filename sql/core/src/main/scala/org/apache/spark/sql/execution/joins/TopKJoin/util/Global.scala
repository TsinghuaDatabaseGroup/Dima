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

/**
  * Created by sunji on 17/1/11.
  */
object Global {
  def globalOrderForPair(x1: ((String, InternalRow), (String, InternalRow)),
                         x2: ((String, InternalRow), (String, InternalRow))): Short = {
    val s1 = x1._1._1.hashCode / 2 + x1._2._1.hashCode / 2
    val s2 = x2._1._1.hashCode / 2 + x2._2._1.hashCode / 2
    if (s1 > s2) {
      1
    } else if (s1 < s2) {
      -1
    } else {
      0
    }
  }

  def compare(x: JaccardPair, y: JaccardPair): Short = {
    if (x.delta > y.delta) {
      1
    } else if (x.delta < y.delta) {
      -1
    } else {
      Global.globalOrderForPair((x.r, x.s), (y.r, y.s))
    }
  }

  def twoHeapMerge(heap1: Index, heap2: Index, K: Int): Index = {
    var sim = 1.0
    val newIndex = Index()
    //    println("----------------------------------------------------------")
    //    println("heap1:")
    //    heap1.getAll()
    // .foreach(e => println("<" + e._1._1._1 + ", " + e._1._2._1 + ">" + "," + e._2))
    //    println("heap2:")
    //    heap2.getAll()
    // .foreach(e => println("<" + e._1._1._1 + ", " + e._1._2._1 + ">" + "," + e._2))
    while (newIndex.getTotalNum() < K && sim > 0.0) {
      //      println(s"search in $sim")
      while (!heap1.isEmptyInRange(sim) && ! heap2.isEmptyInRange(sim)) {
        val h1 = heap1.getMinInRange(sim)
        val h2 = heap2.getMinInRange(sim)
        val c = Global.compare(h1, h2)
        if (h1.r._1 == h2.r._1 && h1.s._1 == h2.s._1) {
          //          println(s"discard <$s1, $r1>, <$s2, $r2>")
          val s = heap1.extractMinInRange(sim)
          heap2.extractMinInRange(sim)
          newIndex.startUpAddItem(s.delta, s)
          //          println(s"add <$s1, $r1, $sim1>")
        } else {
          //          println(s"nodiscard <$s1, $r1>, <$s2, $r2>")
          if (c <= 0) {
            val s = heap1.extractMinInRange(sim)
            newIndex.startUpAddItem(s.delta, s)
            //            println(s"add <$s1, $r1, $sim1>")
          } else {
            val s = heap2.extractMinInRange(sim)
            newIndex.startUpAddItem(s.delta, s)
            //            println(s"add <$s2, $r2, $sim2>")
          }
        }
      }
      if (heap1.isEmptyInRange(sim)) {
        while (!heap2.isEmptyInRange(sim)) {
          val h2 = heap2.extractMinInRange(sim)

          //          val s2 = h2._1._1._1
          //          val r2 = h2._1._2._1
          //          val sim2 = h2._2

          newIndex.startUpAddItem(h2.delta, h2)
          //          println(s"add <$s2, $r2, $sim2>")
        }
      } else if (heap2.isEmptyInRange(sim)) {
        while (!heap1.isEmptyInRange(sim)) {
          val h1 = heap1.extractMinInRange(sim)

          //          val s1 = h1._1._1._1
          //          val r1 = h1._1._2._1
          //          val sim1 = h1._2

          newIndex.startUpAddItem(h1.delta, h1)
          //          println(s"add <$s1, $r1, $sim1>")
        }
      }
      sim = newIndex.minSimDoubleMinus(sim)
    }
    //    println("----------------------------------------------------------")
    return newIndex
  }

  def multiHeapMerge(heap: Array[Index], K: Int): Index = {
    if (heap.length == 1) {
      return heap(0)
    } else {
      twoHeapMerge(multiHeapMerge(heap.slice(0, heap.length / 2), K),
        multiHeapMerge(heap.slice(heap.length / 2, heap.length), K), K)
    }
  }
}

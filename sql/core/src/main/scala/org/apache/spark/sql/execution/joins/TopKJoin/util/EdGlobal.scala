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
import org.apache.spark.sql.simjointopk.index.EditIndex
import org.apache.spark.sql.simjointopk.util.EdPair

/**
  * Created by sunji on 17/1/11.
  */
object EdGlobal {
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

  def compare(x: EdPair, y: EdPair): Short = {
    if (x.delta > y.delta) {
      1
    } else if (x.delta < y.delta) {
      -1
    } else {
      globalOrderForPair(((x.r._1, x.r._2), (x.s._1, x.s._2)), ((y.r._1, y.r._2), (y.s._1, y.s._2)))
    }
  }

  def twoHeapMerge(heap1: EdIndex, heap2: EdIndex, K: Int): EdIndex = {
    var dis = 0
    val newIndex = EdIndex()
    //    println("----------------------------------------------------------")
    //    println("heap1:")
    //    heap1.getAll()
    //    .foreach(e => println("<" + e._1._1._1 + ", " + e._1._2._1 + ">" + "," + e._2))
    //    println("heap2:")
    //    heap2.getAll()
    //    .foreach(e => println("<" + e._1._1._1 + ", " + e._1._2._1 + ">" + "," + e._2))
    while (newIndex.getTotalNum() < K) {
      //      println(s"search in $sim")
      while (!heap1.isEmptyInRange(dis) && !heap2.isEmptyInRange(dis)) {
        val h1 = heap1.getMaxInRange(dis)
        val h2 = heap2.getMaxInRange(dis)
        //        val s1 = h1._1._1._1
        //        val s2 = h2._1._1._1
        //        val r1 = h1._1._2._1
        //        val r2 = h2._1._2._1
        //        val sim1 = h1._2
        //        val sim2 = h2._2
        val c = EdGlobal.compare(h1, h2)
        if (h1.s._1 == h2.s._1 && h1.r._1 == h2.r._1) {
          //          println(s"discard <$s1, $r1>, <$s2, $r2>")
          val s = heap1.extractMaxInRange(dis)
          heap2.extractMaxInRange(dis)
          newIndex.startUpAddItem(s.delta, s)
          //          println(s"add <$s1, $r1, $sim1>")
        } else {
          //          println(s"nodiscard <$s1, $r1>, <$s2, $r2>")
          if (c >= 0) {
            val s = heap1.extractMaxInRange(dis)
            newIndex.startUpAddItem(s.delta, s)
            //            println(s"add <$s1, $r1, $sim1>")
          } else {
            val s = heap2.extractMaxInRange(dis)
            newIndex.startUpAddItem(s.delta, s)
            //            println(s"add <$s2, $r2, $sim2>")
          }
        }
      }
      if (heap1.isEmptyInRange(dis)) {
        while (!heap2.isEmptyInRange(dis)) {
          val h2 = heap2.extractMaxInRange(dis)

          //          val s2 = h2._1._1._1
          //          val r2 = h2._1._2._1
          //          val sim2 = h2._2

          newIndex.startUpAddItem(h2.delta, h2)
          //          println(s"add <$s2, $r2, $sim2>")
        }
      } else if (heap2.isEmptyInRange(dis)) {
        while (!heap1.isEmptyInRange(dis)) {
          val h1 = heap1.extractMaxInRange(dis)

          //          val s1 = h1._1._1._1
          //          val r1 = h1._1._2._1
          //          val sim1 = h1._2

          newIndex.startUpAddItem(h1.delta, h1)
          //          println(s"add <$s1, $r1, $sim1>")
        }
      }
      dis += 1
    }
    //    println("----------------------------------------------------------")
    return newIndex
  }

  def multiHeapMerge(heap: Array[EdIndex], K: Int): EdIndex = {
    if (heap.length == 1) {
      return heap(0)
    } else {
      twoHeapMerge(multiHeapMerge(heap.slice(0, heap.length / 2), K),
        multiHeapMerge(heap.slice(heap.length / 2, heap.length), K), K)
    }
  }
}


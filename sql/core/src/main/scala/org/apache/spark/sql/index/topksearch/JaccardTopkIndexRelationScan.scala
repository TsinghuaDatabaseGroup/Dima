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
package org.apache.spark.sql.topksearch

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.simjointopk.IPartition
import org.apache.spark.sql.simjointopk.util.{Global, Index, JaccardPair}

import scala.collection.mutable.ListBuffer

/**
  * Created by sunji on 17/1/24.
  */
object JaccardTopkIndexRelationScan extends Logging {

  def jaccardDistance(s1: String, s2: String): Double = {
    val overlap = (s1.split(" ").intersect(s2.split(" "))).length
    overlap.toDouble / (s1.split(" ").length + s2.split(" ").length - overlap).toDouble
  }

  def createProbeSig(x: String, sim: Double, pos: Int):
    Array[((String, Int, Int), String)] = {
    val result = ListBuffer[((String, Int, Int), String)]()
    val ss = x.split(" ")
    val s = ss.length

    //    for (l <- (Math.ceil(sim * s) + 0.0001).toInt until s + 1) {
    val l = s
    val t = ss.filter(x => tokenGroup(x, pos))
    val set = {
      if (t.length == 0) {
        ("", pos, 0)
      } else {
        (t.reduce(_ + " " + _), pos, 0)
      }
    }
    //      println(set)
    val predictSim = (s / (sim + 0.01) + s - pos + 1) / (s / (sim + 0.01) + s + pos - 1)
    //    println(s"${set._1}, $pos, $predictSim, $sim, ${x._1}")
    if (predictSim > sim) {
      result += Tuple2(set, x)
    }
    //    }
    result.toArray
  }

  def tokenGroup(token: String, pos: Int): Boolean = {
    val segmentNum = Math.pow(2, pos).toInt
    val s =
      {
        val hashMod = token.hashCode % segmentNum
        if (hashMod < 0) {
          hashMod + segmentNum
        } else {
          hashMod
        }
      } + 1
    s == segmentNum / 2
  }

  def getTopk(sc: SparkContext, index: Seq[RDD[IPartition]], target: String, delta: Int):
    RDD[InternalRow] = {

    val num_partitions = 2
    val K = delta
    val iterateNum = 40

    var topK = Index()

    var minSim = 0.0
    var pos = 1
    var goon = true
    while (goon) {
      logInfo(s"Iteration round $pos")
      val probe = createProbeSig(target, minSim, pos).map(x => (x._1.hashCode(), x._2))
      val num = probe.length
      logInfo(s"Probe set num: $num")
      logInfo(s"minimum similairty in topk: $minSim")
      if (num == 0) {
        goon = false
      } else if (pos >= iterateNum) {
        logWarning(s"Similarity In This Data Is Small, Iteration Stops Here!")
        goon = false
      } else {
        val topKSetRdd =
          index(pos-1).mapPartitions(indexIter => {
            val heap = topK.copy()
            var tempMinSim = minSim
            val I = indexIter.next()
            for (p <- probe) {
              val location = I.index.mapping.getOrElse(p._1, List[Int]())
              for (loc <- location) {
                var isDup = false
                val leftLength = p._2.split(" ").length
                val rightLength = I.data(loc)._1.split(" ").length
                if (!isDup && leftLength >= Math.ceil(rightLength * tempMinSim + 0.001).toInt &&
                  rightLength >= Math.ceil(leftLength * tempMinSim + 0.001).toInt) {
                  val sim = jaccardDistance(p._2, I.data(loc)._1)
                  val bucketSim = f"$sim%1.2f".toDouble
                  if (sim > tempMinSim || (sim == tempMinSim && heap.getTotalNum() < K)) {
                    for (s <- heap.getAllInRange(bucketSim)) {
                      if (p._2.hashCode == s.r._1.hashCode &&
                        I.data(loc)._1.hashCode == s.s._1.hashCode) {
                        isDup = true
                      }
                    }
                    if (!isDup) {
                      if (heap.getTotalNum() >= K) {
                        heap.addItem(sim, JaccardPair((p._2, InternalRow.empty), I.data(loc), sim))
                        tempMinSim = heap.getMinSim()
                      } else {
                        heap.startUpAddItem(sim,
                          JaccardPair((p._2, InternalRow.empty), I.data(loc), sim))
                      }
                    }
                  } else {
                    //                    println(s"sim lower than lowest one int
                    // the heap: sim: $sim, minSim: $tempMinSim")
                  }
                } else if (!isDup) {
                  //                  println("length mismatch")
                }
              }
            }
            Array(heap).iterator
          })
        logInfo("merging...")
        topK = topKSetRdd
          .map(x => (1, x))
          .reduceByKey(Global.twoHeapMerge(_, _, K))
          .map(_._2)
          .collect()(0)
          .curveToK(K)
        //        topK = Global.multiHeapMerge(topKSet, K).curveToK(K)
        if (topK.getTotalNum() >= K) {
          minSim = topK.getMinSim()
        }
        logInfo("merge complete!")
      }
      pos += 1
    }
    logInfo("complete!")
    sc.parallelize(topK.getAll().toSeq).map(e => e.s._2)
  }
}

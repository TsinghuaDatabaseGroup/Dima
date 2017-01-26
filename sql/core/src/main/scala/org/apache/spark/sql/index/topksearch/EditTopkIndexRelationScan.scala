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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.simjointopk.EPartition
import org.apache.spark.sql.simjointopk.util.{EdGlobal, EdIndex, EdPair}
import org.apache.spark.{Logging, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by sunji on 17/1/25.
  */
object EditTopkIndexRelationScan extends Logging{

  def P(l: Int, pos: Int): Int = {
    val segmentNum = Math.pow(2, pos-1).toInt
    (Math.ceil((1.0 - 1.0 / segmentNum.toDouble) * l) + 0.0001).toInt
  }

  def L(l: Int, pos: Int): Int = {
    val segmentNum = Math.pow(2, pos).toInt
    (Math.ceil((1.0 / segmentNum.toDouble) * l) + 0.0001).toInt
  }

  def createProbeSig(x: String, dis: Int, pos: Int):
    Array[((String, Int, Int), String)] = {
    val result = ListBuffer[((String, Int, Int), String)]()
    val lu = x.length
    val lo = Math.max(lu - dis, 1)
    val U = dis
    val predictDis = pos - 1
    if (predictDis < dis) {
      for (l <- lo until lu + 1) {
        val ps = P(l, pos)
        val lowerBound = Math.max(ps - (pos - 1), ps - (l - lu + (U + 1 - pos)))
        val upperBound = Math.min(ps + lu - l + U + 1 - pos, ps + pos - 1)
        val length = L(l, pos)
        for (p <- lowerBound until upperBound + 1) {
          result += Tuple2((x.slice(p, p + length), pos, l), x)
        }
      }
    }
    result.toArray
  }

  def editDistance(s1: String, s2: String, delta: Int = 10000): Int = {
    if (math.abs(s1.length - s2.length) > delta) {
      return (delta + 1)
    }

    val dist = Array.tabulate(s2.length + 1, s1.length + 1){
      (j, i) => if (j == 0) i else if (i == 0) j else 0
    }

    for (j <- 1 to s2.length; i <- 1 to s1.length)
      dist(j)(i) = if (s2(j - 1) == s1(i - 1)) dist(j - 1)(i - 1)
      else math.min(math.min(dist(j - 1)(i) + 1, dist(j)(i - 1) + 1), dist(j - 1)(i - 1) + 1)

    dist(s2.length)(s1.length)
  }

  def getTopk(sc: SparkContext, index: Seq[RDD[EPartition]], target: String, delta: Int):
    RDD[InternalRow] = {

    val num_partitions = 2
    val K = delta
    val iterateNum = 40

    var topK = EdIndex()

    var maxDis = 50
    var pos = 1
    var goon = true
    while (goon) {
      logInfo(s"Iteration round $pos")
      val probe = createProbeSig(target, maxDis, pos).map(x => (x._1.hashCode(), x._2))
      val num = probe.length
      logInfo(s"Probe set num: $num")
      logInfo(s"maximum distance in topk: $maxDis")
      if (num == 0) {
        goon = false
      } else if (pos >= iterateNum - 1) {

      } else {
        val topKSetRdd =
          index(pos-1).mapPartitions(indexIter => {
            val heap = topK.copy()
            var tempMaxDis = maxDis
            val I = indexIter.next()
            for (p <- probe) {
              logInfo(s"probe: <${p._1} -> ${p._2}>")
              val location = I.index.mapping.getOrElse(p._1, List[Int]())
              for (loc <- location) {
                var isDup = false
                logInfo(s"index: <${I.data(loc)._1} -> ${I.data(loc)._2}>")
                if (!isDup) {
                  val dis = editDistance(p._2, I.data(loc)._1)
                  if (dis < tempMaxDis ||
                    (dis == tempMaxDis && heap.getTotalNum() < K)) { // tempMinSim to filter more
                    for (s <- heap.getAllInRange(dis)) {
                      if (p._2.hashCode == s.r._1.hashCode &&
                        I.data(loc)._1.hashCode == s.s._1.hashCode) {
                        isDup = true
                      }
                    }
                    if (!isDup) {
                      if (heap.getTotalNum() >= K) {
                        heap.addItem(dis, EdPair((p._2, InternalRow.empty), I.data(loc), dis))
                        tempMaxDis = heap.getMaxDis()
                      } else {
                        heap.startUpAddItem(dis,
                          EdPair((p._2, InternalRow.empty), I.data(loc), dis))
                      }
                    }
                  } else {
                    //                    println(s"sim lower than lowest one int
                    // the heap: dis: $dis, maxDis: $tempMaxDis")
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
          .reduceByKey(EdGlobal.twoHeapMerge(_, _, K))
          .map(_._2)
          .collect()(0)
          .curveToK(K)
        if (topK.getTotalNum() >= K) {
          maxDis = topK.getMaxDis()
        }
        logInfo("merge complete!")
      }
      pos += 1
    }
    logInfo("complete!")
    sc.parallelize(topK.getAll().toSeq).map(e => e.s._2)
  }
}

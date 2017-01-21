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

package org.apache.spark.sql.simjointopk

import partitioner.HashPartitioner
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.simjointopk.index.JaccardIndex
import org.apache.spark.sql.simjointopk.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, JoinedRow, Literal}
import org.apache.spark.sql.execution.{BinaryNode, SimilarityRDD, SparkPlan}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer
/**
  * Created by sunji on 16/12/15.
  */

case class IPartition(index: JaccardIndex, data: Array[(String, InternalRow)])

case class JaccardSimJoinTopK (leftKeys: Expression,
                               rightKeys: Expression,
                               l: Literal,
                               left: SparkPlan,
                               right: SparkPlan) extends BinaryNode{

  override def output: Seq[Attribute] = left.output ++ right.output

  def choiceNum(l: Int, s: Int, threshold: Double): Int = {
    Math.floor((1 - threshold) * (l + s) / (1 + threshold) + 0.0001).toInt + 1
  }

  def segNum(l: Int, threshold: Double): Int = {
    Math.floor((1 - threshold) * l / threshold + 0.0001).toInt + 1
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

  def segRecord(s: String, pos: Int): (String, Int, Int) = {
    val ss = s.split(" ")
//    val l = ss.length
    val t = ss.filter(x => tokenGroup(x, pos))
    val set =
      if (t.length == 0) {
        ("", pos, 0)
      } else {
        (t.reduce(_ + " " + _), pos, 0)
      }
//    println(s"index: $set, $pos, $s")
    set
  }

  def jaccardDistance(s1: String, s2: String): Double = {
    val overlap = (s1.split(" ").intersect(s2.split(" "))).length
    overlap.toDouble / (s1.split(" ").length + s2.split(" ").length - overlap).toDouble
  }

  def createProbeSig(x: (String, InternalRow), sim: Double, pos: Int):
    Array[((String, Int, Int), (String, InternalRow))] = {
    val result = ListBuffer[((String, Int, Int), (String, InternalRow))]()
    val ss = x._1.split(" ")
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

  override protected def doExecute(): RDD[InternalRow] = {
    logInfo(s"execute JaccardSimTopK")
    logInfo(s"LEFT: ${left}")
    logInfo(s"RIGHT: ${right}")

    val num_partitions = 2
    val K = l.toString.toInt

    logInfo(s"num_partitions: $num_partitions, K: $K")
    val left_rdd = left.execute().map(row =>
    {
      val key = BindReferences
        .bindReference(leftKeys, left.output)
        .eval(row)
        .asInstanceOf[org.apache.spark.unsafe.types.UTF8String]
        .toString
      (sortByValue(key), row.copy())
    })

    val right_rdd = right.execute().map(row =>
    {
      val key = BindReferences
        .bindReference(rightKeys, right.output)
        .eval(row)
        .asInstanceOf[org.apache.spark.unsafe.types.UTF8String]
        .toString
      (sortByValue(key), row.copy())
    })

    val first = left_rdd.take(1)(0)
    var topK = Index(left_rdd.take(K + 1)
      .filter(x => x._1 != first._1)
      .map(x => {
        val ss = {
          if (first._1.hashCode < x._1.hashCode) {
            (first, x)
          } else {
            (x, first)
          }
        }
        JaccardPair(ss._1, ss._2, jaccardDistance(ss._1._1, ss._2._1))
      }))

    //    for (e <- topK.getAll()) {
    //      println("<" + e._1._1._1 + "," + e._1._2._1 + ">" + "," + e._2)
    //    }

    var minSim = topK.getMinSim()
    var pos = 1
    var goon = true
    while (goon) {
      logInfo(s"Iteration round $pos")
      //      for (e <- topK.getAll()) {
      //        println("<" + e._1._1._1 + "," + e._1._2._1 + ">" + "," + e._2)
      //      }
      val probe = left_rdd
        .map(x => createProbeSig(x, minSim, pos))
        .filter(x => x.length > 0)
        .flatMap(x => x)
        .map(x => (x._1.hashCode(), x._2))
        .partitionBy(new HashPartitioner(num_partitions))
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
      val num = probe.count()
      logInfo(s"Probe set num: $num")
      logInfo(s"minimum similairty in topk: $minSim")
      if (num == 0) {
        goon = false
      } else {
        logInfo(s"index building.....")
        val index = right_rdd
          .map(x => (x._1, x._2))
          .map(x => ((x._1, x._2), segRecord(x._1, pos)))
          .map(x => (x._2, x._1))
          .map(x => (x._1.hashCode(), x._2))
          .partitionBy(new HashPartitioner(num_partitions))
          .mapPartitionsWithIndex((index, iter) => {
            val data = iter.toArray
            Array(IPartition(JaccardIndex(data.map(x => x._1)), data.map(x => x._2))).iterator
          })
          .persist(StorageLevel.MEMORY_ONLY)

        index.count
        logInfo(s"index completing")

        val topKSetRdd =
          probe.zipPartitions(index, true) { (probeIter, indexIter) => {
            val heap = topK.copy()
            var tempMinSim = minSim
            val I = indexIter.next()
            for (p <- probeIter) {
              //              println("p: "+p._2._1)
              val location = I.index.mapping.getOrElse(p._1, List[Int]())
              for (loc <- location) {
                //                println(s"loc: $loc")
                var isDup = false
//                println("meet " + p._2._1 + ", " + I.data(loc)._1)
                // indexed minHeap
                // balance
                val leftLength = p._2._1.split(" ").length
                val rightLength = I.data(loc)._1.split(" ").length
                if (!isDup && leftLength >= Math.ceil(rightLength * tempMinSim + 0.001).toInt &&
                  rightLength >= Math.ceil(leftLength * tempMinSim + 0.001).toInt) {
                  val sim = jaccardDistance(p._2._1, I.data(loc)._1)
                  val bucketSim = f"$sim%1.2f".toDouble

//                  println(s"similarity: $sim, tempMinSim: $tempMinSim")
                  if (sim > tempMinSim) {
                    // tempMinSim to filter more
//                    println(bucketSim)
                    for (s <- heap.getAllInRange(bucketSim)) {
                      if (p._2._1.hashCode == s.r._1.hashCode &&
                        I.data(loc)._1.hashCode == s.s._1.hashCode) {
//                        println("duplicated")
                        isDup = true
                      }
                    }

                    if (!isDup) {
                      heap.addItem(sim, JaccardPair(p._2, I.data(loc), sim))
                      //                    println(s"add complete")
                      tempMinSim = heap.getMinSim()
                      //                    println(s"update minsim to $tempMinSim")
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
          }
          }.distinct
//        topKSetRdd
        // .flatMap(x => x.getAll())
        // .map(x => ("<" + x._1._1._1 + "," + x._1._2._1 + ">" + "," + x._2))
        // .saveAsTextFile("./testMiddleResult/" + pos + "/")
        //        val topKSet = topKSetRdd.collect()
        index.unpersist()

        //        for (i <- topKSet) {
        //          for (e <- i.getAll()) {
        //            println("<" + e._1._1._1 + "," + e._1._2._1 + ">" + "," + e._2)
        //          }
        //          println(" ")
        //        }
        logInfo("merging...")
        topK = topKSetRdd
          .map(x => (1, x))
          .reduceByKey(Global.twoHeapMerge(_, _, K))
          .map(_._2)
          .collect()(0)
          .curveToK(K)
        //        topK = Global.multiHeapMerge(topKSet, K).curveToK(K)
        minSim = topK.getMinSim()
        logInfo("merge complete!")
      }
      probe.unpersist()
      pos += 1
    }
    logInfo("complete!")
    sparkContext.parallelize(topK.getAll().toArray.map(e => new JoinedRow(e.r._2, e.s._2)))
  }
}


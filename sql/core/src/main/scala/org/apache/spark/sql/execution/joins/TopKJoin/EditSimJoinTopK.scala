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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, JoinedRow, Literal}
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.simjointopk.index.EditIndex
import org.apache.spark.sql.simjointopk.partitioner.HashPartitioner
import util.{EdGlobal, EdIndex, EdPair}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer
/**
  * Created by sunji on 17/1/20.
  */
case class EPartition(index: EditIndex, data: Array[(String, InternalRow)])

case class EditSimJoinTopK (leftKeys: Expression,
                               rightKeys: Expression,
                               l: Literal,
                               left: SparkPlan,
                               right: SparkPlan) extends BinaryNode {

  override def output: Seq[Attribute] = left.output ++ right.output

  def P(l: Int, pos: Int): Int = {
    val segmentNum = Math.pow(2, pos-1).toInt
    (Math.ceil((1.0 - 1.0 / segmentNum.toDouble) * l) + 0.0001).toInt
  }

  def L(l: Int, pos: Int): Int = {
    val segmentNum = Math.pow(2, pos).toInt
    (Math.ceil((1.0 / segmentNum.toDouble) * l) + 0.0001).toInt
  }

  def segRecord(ss: String, pos: Int): (String, Int, Int) = {
    val s = ss.length
    val l = L(s, pos)
    val p = P(s, pos)
    (ss.slice(p, p + l), pos, s)
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

  def createProbeSig(x: (String, InternalRow), dis: Int, pos: Int):
    Array[((String, Int, Int), (String, InternalRow))] = {
    val result = ListBuffer[((String, Int, Int), (String, InternalRow))]()
    val lu = x._1.length
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
          result += Tuple2((x._1.slice(p, p + length), pos, l), x)
        }
      }
    }
    result.toArray
  }

  override protected def doExecute(): RDD[InternalRow] = {
    logInfo(s"execute EditSimTopK")
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
      (key, row.copy())
    })

    val right_rdd = right.execute().map(row =>
    {
      val key = BindReferences
        .bindReference(rightKeys, right.output)
        .eval(row)
        .asInstanceOf[org.apache.spark.unsafe.types.UTF8String]
        .toString
      (key, row.copy())
    })

    val first = left_rdd.take(1)(0)
    var topK = EdIndex(right_rdd.take(K + 1)
      .map(x => {
        EdPair(x, first, editDistance(x._1, first._1))
      }))

    //    for (e <- topK.getAll()) {
    //      println("<" + e._1._1._1 + "," + e._1._2._1 + ">" + "," + e._2)
    //    }

    var maxDis = topK.getMaxDis()
    var pos = 1
    var goon = true
    while (goon) {
      logInfo(s"Iteration round $pos")
      val probe = left_rdd
        .map(x => createProbeSig(x, maxDis, pos))
        .filter(x => x.length > 0)
        .flatMap(x => x)
        .map(x => (x._1.hashCode(), x._2))
        .partitionBy(new HashPartitioner(num_partitions))
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
      val num = probe.count()
      logInfo(s"Probe set num: $num")
      logInfo(s"maximum distance in topk: $maxDis")
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
            Array(EPartition(EditIndex(data.map(x => x._1)), data.map(x => x._2))).iterator
          })
          .persist(StorageLevel.MEMORY_ONLY)

        index.count
        logInfo(s"index completing")

        val topKSetRdd =
          probe.zipPartitions(index, true) { (probeIter, indexIter) => {
            val heap = topK.copy()
            var tempMaxDis = maxDis
            val I = indexIter.next()
            for (p <- probeIter) {
              val location = I.index.mapping.getOrElse(p._1, List[Int]())
              for (loc <- location) {
                var isDup = false
                val leftLength = p._2._1.split(" ").length
                val rightLength = I.data(loc)._1.split(" ").length
                if (!isDup) {
                  val dis = editDistance(p._2._1, I.data(loc)._1)
                  if (dis < tempMaxDis ||
                    (dis == tempMaxDis && heap.getTotalNum() < K)) { // tempMinSim to filter more
                    for (s <- heap.getAllInRange(dis)) {
                      if (p._2._1.hashCode == s.r._1.hashCode &&
                        I.data(loc)._1.hashCode == s.s._1.hashCode) {
                        isDup = true
                      }
                    }

                    if (!isDup) {
                      heap.addItem(dis, EdPair(p._2, I.data(loc), dis))
                      tempMaxDis = heap.getMaxDis()
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
          }
          }.distinct
        index.unpersist()
        logInfo("merging...")
        topK = topKSetRdd
          .map(x => (1, x))
          .reduceByKey(EdGlobal.twoHeapMerge(_, _, K))
          .map(_._2)
          .collect()(0)
          .curveToK(K)
        maxDis = topK.getMaxDis()
        logInfo("merge complete!")
      }
      probe.unpersist()
      pos += 1
    }
    logInfo("complete!")
    sparkContext.parallelize(topK.getAll().toArray.map(e => new JoinedRow(e.r._2, e.s._2)))
  }
}

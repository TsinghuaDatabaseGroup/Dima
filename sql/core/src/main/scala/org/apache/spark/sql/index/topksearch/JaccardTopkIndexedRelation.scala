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
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences}
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.index.{IndexedRelation, PackedPartitionWithIndex}
import org.apache.spark.sql.simjointopk.IPartition
import org.apache.spark.sql.simjointopk.index.JaccardIndex
import org.apache.spark.sql.simjointopk.partitioner.HashPartitioner
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

/**
  * Created by sunji on 17/1/24.
  */
private[sql] case class JaccardTopkIndexedRelation(
  output: Seq[Attribute],
  child: SparkPlan,
  table_name: Option[String],
  column_keys: List[Attribute],
  index_name: String)(var _indexedRDD: RDD[PackedPartitionWithIndex] = null,
                      var indexRDD: Seq[RDD[IPartition]] = null)
  extends IndexedRelation with MultiInstanceRelation {
  require(column_keys.length == 1)

  val iterateNum = 40
  val num_partitions = 2

  if (indexRDD == null) {
    buildIndex()
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

  private[sql] def buildIndex(): Unit = {

    val dataRDD = child.execute().map(row => {
      val key = BindReferences.bindReference(column_keys.head, child.output).eval(row)
      (sortByValue(key.asInstanceOf[org.apache.spark.unsafe.types.UTF8String].toString), row.copy())
    })

    val indexed = ListBuffer[RDD[IPartition]]()

    for (pos <- 1 until iterateNum) {
      val index = dataRDD
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
      index.setName(table_name.map(n => s"$n $index_name $pos")
        .getOrElse(s"${child.toString} $pos"))
      indexed += index
    }
//    indexGlobalInfo = new IndexGlobalInfo(threshold,
//      frequencyTable, multiGroup, minimum.value, alpha, numPartitions, threshold)

    indexRDD = indexed
  }

  override def newInstance(): IndexedRelation = {
    new JaccardTopkIndexedRelation(output.map(_.newInstance()), child, table_name,
      column_keys, index_name)(_indexedRDD, indexRDD).asInstanceOf[this.type]
  }

  override def withOutput(new_output: Seq[Attribute]): IndexedRelation = {
    new JaccardTopkIndexedRelation(new_output,
      child, table_name, column_keys, index_name)(_indexedRDD, indexRDD)
  }

  @transient override lazy val statistics = Statistics(
    // TODO: Instead of returning a default value here, find a way to return a meaningful size
    // estimate for RDDs. See PR 1238 for more discussions.
    sizeInBytes = BigInt(child.sqlContext.conf.defaultSizeInBytes)
  )
}

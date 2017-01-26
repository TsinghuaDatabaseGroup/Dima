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
import org.apache.spark.sql.simjointopk.index.EditIndex
import org.apache.spark.sql.simjointopk.partitioner.HashPartitioner
import org.apache.spark.sql.simjointopk.EPartition
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

/**
  * Created by sunji on 17/1/25.
  */
private[sql] case class EditTopkIndexedRelation(
  output: Seq[Attribute],
  child: SparkPlan,
  table_name: Option[String],
  column_keys: List[Attribute],
  index_name: String)(var _indexedRDD: RDD[PackedPartitionWithIndex] = null,
                      var indexRDD: Seq[RDD[EPartition]] = null)
  extends IndexedRelation with MultiInstanceRelation {
  require(column_keys.length == 1)

  val iterateNum = 20
  val num_partitions = 2

  if (indexRDD == null) {
    buildIndex()
  }

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

  private[sql] def buildIndex(): Unit = {

    val dataRDD = child.execute().map(row => {
      val key = BindReferences.bindReference(column_keys.head, child.output).eval(row)
      (key.asInstanceOf[org.apache.spark.unsafe.types.UTF8String].toString, row.copy())
    })

    val indexed = ListBuffer[RDD[EPartition]]()

    for (pos <- 1 until iterateNum) {
      val index = dataRDD
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
      index.setName(table_name.map(n => s"$n $index_name $pos")
        .getOrElse(s"${child.toString} $pos"))
      indexed += index
    }
    indexRDD = indexed
  }

  override def newInstance(): IndexedRelation = {
    new EditTopkIndexedRelation(output.map(_.newInstance()), child, table_name,
      column_keys, index_name)(_indexedRDD, indexRDD).asInstanceOf[this.type]
  }

  override def withOutput(new_output: Seq[Attribute]): IndexedRelation = {
    new EditTopkIndexedRelation(new_output,
      child, table_name, column_keys, index_name)(_indexedRDD, indexRDD)
  }

  @transient override lazy val statistics = Statistics(
    // TODO: Instead of returning a default value here, find a way to return a meaningful size
    // estimate for RDDs. See PR 1238 for more discussions.
    sizeInBytes = BigInt(child.sqlContext.conf.defaultSizeInBytes)
  )
}

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
package org.apache.spark.sql.partitioner

import org.apache.spark.Partitioner
import org.apache.spark.broadcast.Broadcast

/**
  * Created by sunji on 16/11/16.
  */
class SimilarityQueryPartitioner(numParts: Int,
                                 partitionTable: Broadcast[scala.collection.immutable.Map[Int, Int]],
                                 frequencyTable: Broadcast[scala.collection.Map[(Int, Boolean), Long]],
                                 maxPartitionId: Array[Int]
                                ) extends Partitioner {

  val distr = new Array[Long](numParts)

  override def numPartitions: Int = numParts

  def Has(x : Int, array: Array[Int]): Boolean = {
    for (i <- array) {
      if (x == i) {
        return true
      }
    }
    false
  }

  def hashStrategy(key: Any): Int = {
    val code = (key.hashCode % numPartitions)
    if (code < 0) {
      code + numPartitions
    } else {
      code
    }
  }

  override def getPartition(key: Any): Int = {
    val k = key.hashCode()
    val id = partitionTable.value.getOrElse(k, hashStrategy(k))
    if (Has(id, maxPartitionId)) {
      // random partition
      val delta = frequencyTable.value.getOrElse((k, true), 0.toLong) + frequencyTable.value.getOrElse((k, false), 0.toLong)
      var min = Long.MaxValue
      var toId = -1
      for (i <- 0 until numPartitions) {
        if (distr(i) < min) {
          min = distr(i)
          toId = i
        }
      }
      distr(toId) += delta
      toId
    } else {
      val delta = frequencyTable.value
        .getOrElse((k, true), 0.toLong) + frequencyTable.value.getOrElse((k, false), 0.toLong)
      distr(id) += delta
      id
    }
  }

  override def equals(other: Any): Boolean = other match {
    case similarity: SimilarityHashPartitioner =>
      similarity.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}

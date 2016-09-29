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

package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import scala.reflect.{classTag, ClassTag}
import org.apache.spark.Partition
import scala.collection.mutable.{ArrayBuffer, Map}

/**
  * Created by sunji on 16/9/29.
  */
class SimilarityRDD[U: ClassTag](prev: RDD[U], preservesPartitioning: Boolean) extends RDD[U](prev){
  val nodeIPs = Array(
    "192.168.1.54",
    "192.168.1.55",
    "192.168.1.56",
    "192.168.1.57",
    "192.168.1.58",
    "192.168.1.59",
    "192.168.1.60",
    "192.168.1.61"
  )
  override def getPreferredLocations(split: Partition): Seq[String] =
    Seq(nodeIPs(split.index % nodeIPs.length))

  override val partitioner = if (preservesPartitioning) firstParent[U].partitioner else None

  override def getPartitions: Array[Partition] = firstParent[U].partitions
}

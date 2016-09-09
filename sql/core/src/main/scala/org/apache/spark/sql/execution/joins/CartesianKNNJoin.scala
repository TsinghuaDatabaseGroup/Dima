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

package org.apache.spark.sql.execution.joins


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.spatial._


/**
  * Created by dong on 1/20/16.
  * KNN Join based on Cartesian Product
  */
case class CartesianKNNJoin(left_keys: Seq[Expression],
                            right_keys: Seq[Expression],
                            l: Literal,
                            left: SparkPlan,
                            right: SparkPlan) extends BinaryNode {
  override def outputPartitioning: Partitioning = left.outputPartitioning

  override def output: Seq[Attribute] = left.output ++ right.output

  final val k = l.value.asInstanceOf[Number].intValue()

  override protected def doExecute(): RDD[InternalRow] = {
    val left_rdd = left.execute()
    val right_rdd = right.execute()

    left_rdd.map(row =>
      (new Point(left_keys.map(x => BindReferences.bindReference(x, left.output).eval(row)
        .asInstanceOf[Number].doubleValue()).toArray), row)
    ).cartesian(right_rdd).map {
      case (l: (Point, InternalRow), r: InternalRow) =>
        val tmp_point = new Point(right_keys.map(x => BindReferences.bindReference(x, right.output)
          .eval(r).asInstanceOf[Number].doubleValue()).toArray)
        l._2 -> List((tmp_point.minDist(l._1), r))
    }.reduceByKey {
      case (l_list: Seq[(Double, InternalRow)], r_list: Seq[(Double, InternalRow)]) =>
        (l_list ++ r_list).sortWith(_._1 < _._1).take(k)
    }.flatMapValues(list => list).mapPartitions { iter =>
      val joinedRow = new JoinedRow
      iter.map(r => joinedRow(r._1, r._2._2))
    }
  }
}


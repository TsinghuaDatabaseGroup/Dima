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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{GeneratedExpressionCode, CodeGenContext}
import org.apache.spark.sql.types.{IntegerType, DataType}


/**
 * Expression that returns the current partition id of the Spark task.
 */
private[sql] case class SparkPartitionID() extends LeafExpression with Nondeterministic {

  override def nullable: Boolean = false

  override def dataType: DataType = IntegerType

  @transient private[this] var partitionId: Int = _

  override val prettyName = "SPARK_PARTITION_ID"

  override protected def initInternal(): Unit = {
    partitionId = TaskContext.getPartitionId()
  }

  override protected def evalInternal(input: InternalRow): Int = partitionId

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val idTerm = ctx.freshName("partitionId")
    ctx.addMutableState(ctx.JAVA_INT, idTerm,
      s"$idTerm = org.apache.spark.TaskContext.getPartitionId();")
    ev.isNull = "false"
    s"final ${ctx.javaType(dataType)} ${ev.value} = $idTerm;"
  }
}

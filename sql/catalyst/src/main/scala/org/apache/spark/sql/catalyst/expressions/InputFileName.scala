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

import org.apache.spark.rdd.SqlNewHadoopRDDState
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{GeneratedExpressionCode, CodeGenContext}
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Expression that returns the name of the current file being read in using [[SqlNewHadoopRDD]]
 */
case class InputFileName() extends LeafExpression with Nondeterministic {

  override def nullable: Boolean = true

  override def dataType: DataType = StringType

  override val prettyName = "INPUT_FILE_NAME"

  override protected def initInternal(): Unit = {}

  override protected def evalInternal(input: InternalRow): UTF8String = {
    SqlNewHadoopRDDState.getInputFileName()
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    ev.isNull = "false"
    s"final ${ctx.javaType(dataType)} ${ev.value} = " +
      "org.apache.spark.rdd.SqlNewHadoopRDDState.getInputFileName();"
  }

}

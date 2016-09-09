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

package org.apache.spark.sql.types

import scala.language.existentials

private[sql] object ObjectType extends AbstractDataType {
  override private[sql] def defaultConcreteType: DataType =
    throw new UnsupportedOperationException("null literals can't be casted to ObjectType")

  // No casting or comparison is supported.
  override private[sql] def acceptsType(other: DataType): Boolean = false

  override private[sql] def simpleString: String = "Object"
}

/**
 * Represents a JVM object that is passing through Spark SQL expression evaluation.  Note this
 * is only used internally while converting into the internal format and is not intended for use
 * outside of the execution engine.
 */
private[sql] case class ObjectType(cls: Class[_]) extends DataType {
  override def defaultSize: Int =
    throw new UnsupportedOperationException("No size estimation available for objects.")

  def asNullable: DataType = this
}

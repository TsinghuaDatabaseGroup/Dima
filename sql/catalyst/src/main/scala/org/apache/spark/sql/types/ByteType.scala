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

import scala.math.{Ordering, Integral, Numeric}
import scala.reflect.runtime.universe.typeTag

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.ScalaReflectionLock


/**
 * :: DeveloperApi ::
 * The data type representing `Byte` values. Please use the singleton [[DataTypes.ByteType]].
 */
@DeveloperApi
class ByteType private() extends IntegralType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "ByteType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  private[sql] type InternalType = Byte
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[InternalType] }
  private[sql] val numeric = implicitly[Numeric[Byte]]
  private[sql] val integral = implicitly[Integral[Byte]]
  private[sql] val ordering = implicitly[Ordering[InternalType]]

  /**
   * The default size of a value of the ByteType is 1 byte.
   */
  override def defaultSize: Int = 1

  override def simpleString: String = "tinyint"

  private[spark] override def asNullable: ByteType = this
}

case object ByteType extends ByteType

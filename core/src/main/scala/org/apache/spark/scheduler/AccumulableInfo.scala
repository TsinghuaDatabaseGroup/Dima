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

package org.apache.spark.scheduler

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * Information about an [[org.apache.spark.Accumulable]] modified during a task or stage.
 */
@DeveloperApi
class AccumulableInfo private[spark] (
    val id: Long,
    val name: String,
    val update: Option[String], // represents a partial update within a task
    val value: String,
    val internal: Boolean) {

  override def equals(other: Any): Boolean = other match {
    case acc: AccumulableInfo =>
      this.id == acc.id && this.name == acc.name &&
        this.update == acc.update && this.value == acc.value &&
        this.internal == acc.internal
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(id, name, update, value, internal)
    state.map(_.hashCode).reduceLeft(31 * _ + _)
  }
}

object AccumulableInfo {
  def apply(
      id: Long,
      name: String,
      update: Option[String],
      value: String,
      internal: Boolean): AccumulableInfo = {
    new AccumulableInfo(id, name, update, value, internal)
  }

  def apply(id: Long, name: String, update: Option[String], value: String): AccumulableInfo = {
    new AccumulableInfo(id, name, update, value, internal = false)
  }

  def apply(id: Long, name: String, value: String): AccumulableInfo = {
    new AccumulableInfo(id, name, None, value, internal = false)
  }
}

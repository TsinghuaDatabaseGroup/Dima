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

package org.apache.spark.sql

import java.sql.{Date, Timestamp}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSQLContext

case class ReflectData(
    stringField: String,
    intField: Int,
    longField: Long,
    floatField: Float,
    doubleField: Double,
    shortField: Short,
    byteField: Byte,
    booleanField: Boolean,
    decimalField: java.math.BigDecimal,
    date: Date,
    timestampField: Timestamp,
    seqInt: Seq[Int])

case class NullReflectData(
    intField: java.lang.Integer,
    longField: java.lang.Long,
    floatField: java.lang.Float,
    doubleField: java.lang.Double,
    shortField: java.lang.Short,
    byteField: java.lang.Byte,
    booleanField: java.lang.Boolean)

case class OptionalReflectData(
    intField: Option[Int],
    longField: Option[Long],
    floatField: Option[Float],
    doubleField: Option[Double],
    shortField: Option[Short],
    byteField: Option[Byte],
    booleanField: Option[Boolean])

case class ReflectBinary(data: Array[Byte])

case class Nested(i: Option[Int], s: String)

case class Data(
    array: Seq[Int],
    arrayContainsNull: Seq[Option[Int]],
    map: Map[Int, Long],
    mapContainsNul: Map[Int, Option[Long]],
    nested: Nested)

case class ComplexReflectData(
    arrayField: Seq[Int],
    arrayFieldContainsNull: Seq[Option[Int]],
    mapField: Map[Int, Long],
    mapFieldContainsNull: Map[Int, Option[Long]],
    dataField: Data)

class ScalaReflectionRelationSuite extends SparkFunSuite with SharedSQLContext {
  import testImplicits._

  test("query case class RDD") {
    val data = ReflectData("a", 1, 1L, 1.toFloat, 1.toDouble, 1.toShort, 1.toByte, true,
      new java.math.BigDecimal(1), Date.valueOf("1970-01-01"), new Timestamp(12345), Seq(1, 2, 3))
    Seq(data).toDF().registerTempTable("reflectData")

    assert(sql("SELECT * FROM reflectData").collect().head ===
      Row("a", 1, 1L, 1.toFloat, 1.toDouble, 1.toShort, 1.toByte, true,
        new java.math.BigDecimal(1), Date.valueOf("1970-01-01"),
        new Timestamp(12345), Seq(1, 2, 3)))
  }

  test("query case class RDD with nulls") {
    val data = NullReflectData(null, null, null, null, null, null, null)
    Seq(data).toDF().registerTempTable("reflectNullData")

    assert(sql("SELECT * FROM reflectNullData").collect().head ===
      Row.fromSeq(Seq.fill(7)(null)))
  }

  test("query case class RDD with Nones") {
    val data = OptionalReflectData(None, None, None, None, None, None, None)
    Seq(data).toDF().registerTempTable("reflectOptionalData")

    assert(sql("SELECT * FROM reflectOptionalData").collect().head ===
      Row.fromSeq(Seq.fill(7)(null)))
  }

  // Equality is broken for Arrays, so we test that separately.
  test("query binary data") {
    Seq(ReflectBinary(Array[Byte](1))).toDF().registerTempTable("reflectBinary")

    val result = sql("SELECT data FROM reflectBinary")
      .collect().head(0).asInstanceOf[Array[Byte]]
    assert(result.toSeq === Seq[Byte](1))
  }

  test("query complex data") {
    val data = ComplexReflectData(
      Seq(1, 2, 3),
      Seq(Some(1), Some(2), None),
      Map(1 -> 10L, 2 -> 20L),
      Map(1 -> Some(10L), 2 -> Some(20L), 3 -> None),
      Data(
        Seq(10, 20, 30),
        Seq(Some(10), Some(20), None),
        Map(10 -> 100L, 20 -> 200L),
        Map(10 -> Some(100L), 20 -> Some(200L), 30 -> None),
        Nested(None, "abc")))

    Seq(data).toDF().registerTempTable("reflectComplexData")
    assert(sql("SELECT * FROM reflectComplexData").collect().head ===
      Row(
        Seq(1, 2, 3),
        Seq(1, 2, null),
        Map(1 -> 10L, 2 -> 20L),
        Map(1 -> 10L, 2 -> 20L, 3 -> null),
        Row(
          Seq(10, 20, 30),
          Seq(10, 20, null),
          Map(10 -> 100L, 20 -> 200L),
          Map(10 -> 100L, 20 -> 200L, 30 -> null),
          Row(null, "abc"))))
  }
}

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

package org.apache.spark.sql.catalyst.encoders

import java.sql.{Timestamp, Date}
import java.util.Arrays
import java.util.concurrent.ConcurrentMap
import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe.TypeTag

import com.google.common.collect.MapMaker

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.catalyst.{OptionalData, PrimitiveData}
import org.apache.spark.sql.types.{StructType, ArrayType}

case class RepeatedStruct(s: Seq[PrimitiveData])

case class NestedArray(a: Array[Array[Int]]) {
  override def equals(other: Any): Boolean = other match {
    case NestedArray(otherArray) =>
      java.util.Arrays.deepEquals(
        a.asInstanceOf[Array[AnyRef]],
        otherArray.asInstanceOf[Array[AnyRef]])
    case _ => false
  }
}

case class BoxedData(
    intField: java.lang.Integer,
    longField: java.lang.Long,
    doubleField: java.lang.Double,
    floatField: java.lang.Float,
    shortField: java.lang.Short,
    byteField: java.lang.Byte,
    booleanField: java.lang.Boolean)

case class RepeatedData(
    arrayField: Seq[Int],
    arrayFieldContainsNull: Seq[java.lang.Integer],
    mapField: scala.collection.Map[Int, Long],
    mapFieldNull: scala.collection.Map[Int, java.lang.Long],
    structField: PrimitiveData)

case class SpecificCollection(l: List[Int])

/** For testing Kryo serialization based encoder. */
class KryoSerializable(val value: Int) {
  override def equals(other: Any): Boolean = {
    this.value == other.asInstanceOf[KryoSerializable].value
  }
}

/** For testing Java serialization based encoder. */
class JavaSerializable(val value: Int) extends Serializable {
  override def equals(other: Any): Boolean = {
    this.value == other.asInstanceOf[JavaSerializable].value
  }
}

class ExpressionEncoderSuite extends SparkFunSuite {
  implicit def encoder[T : TypeTag]: ExpressionEncoder[T] = ExpressionEncoder()

  // test flat encoders
  encodeDecodeTest(false, "primitive boolean")
  encodeDecodeTest(-3.toByte, "primitive byte")
  encodeDecodeTest(-3.toShort, "primitive short")
  encodeDecodeTest(-3, "primitive int")
  encodeDecodeTest(-3L, "primitive long")
  encodeDecodeTest(-3.7f, "primitive float")
  encodeDecodeTest(-3.7, "primitive double")

  encodeDecodeTest(new java.lang.Boolean(false), "boxed boolean")
  encodeDecodeTest(new java.lang.Byte(-3.toByte), "boxed byte")
  encodeDecodeTest(new java.lang.Short(-3.toShort), "boxed short")
  encodeDecodeTest(new java.lang.Integer(-3), "boxed int")
  encodeDecodeTest(new java.lang.Long(-3L), "boxed long")
  encodeDecodeTest(new java.lang.Float(-3.7f), "boxed float")
  encodeDecodeTest(new java.lang.Double(-3.7), "boxed double")

  encodeDecodeTest(BigDecimal("32131413.211321313"), "scala decimal")
  // encodeDecodeTest(new java.math.BigDecimal("231341.23123"), "java decimal")

  encodeDecodeTest("hello", "string")
  encodeDecodeTest(Date.valueOf("2012-12-23"), "date")
  encodeDecodeTest(Timestamp.valueOf("2016-01-29 10:00:00"), "timestamp")
  encodeDecodeTest(Array[Byte](13, 21, -23), "binary")

  encodeDecodeTest(Seq(31, -123, 4), "seq of int")
  encodeDecodeTest(Seq("abc", "xyz"), "seq of string")
  encodeDecodeTest(Seq("abc", null, "xyz"), "seq of string with null")
  encodeDecodeTest(Seq.empty[Int], "empty seq of int")
  encodeDecodeTest(Seq.empty[String], "empty seq of string")

  encodeDecodeTest(Seq(Seq(31, -123), null, Seq(4, 67)), "seq of seq of int")
  encodeDecodeTest(Seq(Seq("abc", "xyz"), Seq[String](null), null, Seq("1", null, "2")),
    "seq of seq of string")

  encodeDecodeTest(Array(31, -123, 4), "array of int")
  encodeDecodeTest(Array("abc", "xyz"), "array of string")
  encodeDecodeTest(Array("a", null, "x"), "array of string with null")
  encodeDecodeTest(Array.empty[Int], "empty array of int")
  encodeDecodeTest(Array.empty[String], "empty array of string")

  encodeDecodeTest(Array(Array(31, -123), null, Array(4, 67)), "array of array of int")
  encodeDecodeTest(Array(Array("abc", "xyz"), Array[String](null), null, Array("1", null, "2")),
    "array of array of string")

  encodeDecodeTest(Map(1 -> "a", 2 -> "b"), "map")
  encodeDecodeTest(Map(1 -> "a", 2 -> null), "map with null")
  encodeDecodeTest(Map(1 -> Map("a" -> 1), 2 -> Map("b" -> 2)), "map of map")

  // Kryo encoders
  encodeDecodeTest("hello", "kryo string")(encoderFor(Encoders.kryo[String]))
  encodeDecodeTest(new KryoSerializable(15), "kryo object")(
    encoderFor(Encoders.kryo[KryoSerializable]))

  // Java encoders
  encodeDecodeTest("hello", "java string")(encoderFor(Encoders.javaSerialization[String]))
  encodeDecodeTest(new JavaSerializable(15), "java object")(
    encoderFor(Encoders.javaSerialization[JavaSerializable]))

  // test product encoders
  private def productTest[T <: Product : ExpressionEncoder](input: T): Unit = {
    encodeDecodeTest(input, input.getClass.getSimpleName)
  }

  case class InnerClass(i: Int)
  productTest(InnerClass(1))

  productTest(PrimitiveData(1, 1, 1, 1, 1, 1, true))

  productTest(
    OptionalData(Some(2), Some(2), Some(2), Some(2), Some(2), Some(2), Some(true),
      Some(PrimitiveData(1, 1, 1, 1, 1, 1, true))))

  productTest(OptionalData(None, None, None, None, None, None, None, None))

  productTest(BoxedData(1, 1L, 1.0, 1.0f, 1.toShort, 1.toByte, true))

  productTest(BoxedData(null, null, null, null, null, null, null))

  productTest(RepeatedStruct(PrimitiveData(1, 1, 1, 1, 1, 1, true) :: Nil))

  productTest((1, "test", PrimitiveData(1, 1, 1, 1, 1, 1, true)))

  productTest(
    RepeatedData(
      Seq(1, 2),
      Seq(new Integer(1), null, new Integer(2)),
      Map(1 -> 2L),
      Map(1 -> null),
      PrimitiveData(1, 1, 1, 1, 1, 1, true)))

  productTest(NestedArray(Array(Array(1, -2, 3), null, Array(4, 5, -6))))

  productTest(("Seq[(String, String)]",
    Seq(("a", "b"))))
  productTest(("Seq[(Int, Int)]",
    Seq((1, 2))))
  productTest(("Seq[(Long, Long)]",
    Seq((1L, 2L))))
  productTest(("Seq[(Float, Float)]",
    Seq((1.toFloat, 2.toFloat))))
  productTest(("Seq[(Double, Double)]",
    Seq((1.toDouble, 2.toDouble))))
  productTest(("Seq[(Short, Short)]",
    Seq((1.toShort, 2.toShort))))
  productTest(("Seq[(Byte, Byte)]",
    Seq((1.toByte, 2.toByte))))
  productTest(("Seq[(Boolean, Boolean)]",
    Seq((true, false))))

  productTest(("ArrayBuffer[(String, String)]",
    ArrayBuffer(("a", "b"))))
  productTest(("ArrayBuffer[(Int, Int)]",
    ArrayBuffer((1, 2))))
  productTest(("ArrayBuffer[(Long, Long)]",
    ArrayBuffer((1L, 2L))))
  productTest(("ArrayBuffer[(Float, Float)]",
    ArrayBuffer((1.toFloat, 2.toFloat))))
  productTest(("ArrayBuffer[(Double, Double)]",
    ArrayBuffer((1.toDouble, 2.toDouble))))
  productTest(("ArrayBuffer[(Short, Short)]",
    ArrayBuffer((1.toShort, 2.toShort))))
  productTest(("ArrayBuffer[(Byte, Byte)]",
    ArrayBuffer((1.toByte, 2.toByte))))
  productTest(("ArrayBuffer[(Boolean, Boolean)]",
    ArrayBuffer((true, false))))

  productTest(("Seq[Seq[(Int, Int)]]",
    Seq(Seq((1, 2)))))

  // test for ExpressionEncoder.tuple
  encodeDecodeTest(
    1 -> 10L,
    "tuple with 2 flat encoders")(
    ExpressionEncoder.tuple(ExpressionEncoder[Int], ExpressionEncoder[Long]))

  encodeDecodeTest(
    (PrimitiveData(1, 1, 1, 1, 1, 1, true), (3, 30L)),
    "tuple with 2 product encoders")(
    ExpressionEncoder.tuple(ExpressionEncoder[PrimitiveData], ExpressionEncoder[(Int, Long)]))

  encodeDecodeTest(
    (PrimitiveData(1, 1, 1, 1, 1, 1, true), 3),
    "tuple with flat encoder and product encoder")(
    ExpressionEncoder.tuple(ExpressionEncoder[PrimitiveData], ExpressionEncoder[Int]))

  encodeDecodeTest(
    (3, PrimitiveData(1, 1, 1, 1, 1, 1, true)),
    "tuple with product encoder and flat encoder")(
    ExpressionEncoder.tuple(ExpressionEncoder[Int], ExpressionEncoder[PrimitiveData]))

  encodeDecodeTest(
    (1, (10, 100L)),
    "nested tuple encoder") {
    val intEnc = ExpressionEncoder[Int]
    val longEnc = ExpressionEncoder[Long]
    ExpressionEncoder.tuple(intEnc, ExpressionEncoder.tuple(intEnc, longEnc))
  }

  test("nullable of encoder schema") {
    def checkNullable[T: ExpressionEncoder](nullable: Boolean*): Unit = {
      assert(implicitly[ExpressionEncoder[T]].schema.map(_.nullable) === nullable.toSeq)
    }

    // test for flat encoders
    checkNullable[Int](false)
    checkNullable[Option[Int]](true)
    checkNullable[java.lang.Integer](true)
    checkNullable[String](true)

    // test for product encoders
    checkNullable[(String, Int)](true, false)
    checkNullable[(Int, java.lang.Long)](false, true)

    // test for nested product encoders
    {
      val schema = ExpressionEncoder[(Int, (String, Int))].schema
      assert(schema(0).nullable === false)
      assert(schema(1).nullable === true)
      assert(schema(1).dataType.asInstanceOf[StructType](0).nullable === true)
      assert(schema(1).dataType.asInstanceOf[StructType](1).nullable === false)
    }

    // test for tupled encoders
    {
      val schema = ExpressionEncoder.tuple(
        ExpressionEncoder[Int],
        ExpressionEncoder[(String, Int)]).schema
      assert(schema(0).nullable === false)
      assert(schema(1).nullable === true)
      assert(schema(1).dataType.asInstanceOf[StructType](0).nullable === true)
      assert(schema(1).dataType.asInstanceOf[StructType](1).nullable === false)
    }
  }

  private val outers: ConcurrentMap[String, AnyRef] = new MapMaker().weakValues().makeMap()
  outers.put(getClass.getName, this)
  private def encodeDecodeTest[T : ExpressionEncoder](
      input: T,
      testName: String): Unit = {
    test(s"encode/decode for $testName: $input") {
      val encoder = implicitly[ExpressionEncoder[T]]
      val row = encoder.toRow(input)
      val schema = encoder.schema.toAttributes
      val boundEncoder = encoder.resolve(schema, outers).bind(schema)
      val convertedBack = try boundEncoder.fromRow(row) catch {
        case e: Exception =>
          fail(
           s"""Exception thrown while decoding
              |Converted: $row
              |Schema: ${schema.mkString(",")}
              |${encoder.schema.treeString}
              |
              |Encoder:
              |$boundEncoder
              |
            """.stripMargin, e)
      }

      val isCorrect = (input, convertedBack) match {
        case (b1: Array[Byte], b2: Array[Byte]) => Arrays.equals(b1, b2)
        case (b1: Array[Int], b2: Array[Int]) => Arrays.equals(b1, b2)
        case (b1: Array[Array[_]], b2: Array[Array[_]]) =>
          Arrays.deepEquals(b1.asInstanceOf[Array[AnyRef]], b2.asInstanceOf[Array[AnyRef]])
        case (b1: Array[_], b2: Array[_]) =>
          Arrays.equals(b1.asInstanceOf[Array[AnyRef]], b2.asInstanceOf[Array[AnyRef]])
        case _ => input == convertedBack
      }

      if (!isCorrect) {
        val types = convertedBack match {
          case c: Product =>
            c.productIterator.filter(_ != null).map(_.getClass.getName).mkString(",")
          case other => other.getClass.getName
        }

        val encodedData = try {
          row.toSeq(encoder.schema).zip(schema).map {
            case (a: ArrayData, AttributeReference(_, ArrayType(et, _), _, _)) =>
              a.toArray[Any](et).toSeq
            case (other, _) =>
              other
          }.mkString("[", ",", "]")
        } catch {
          case e: Throwable => s"Failed to toSeq: $e"
        }

        fail(
          s"""Encoded/Decoded data does not match input data
             |
             |in:  $input
             |out: $convertedBack
             |types: $types
             |
             |Encoded Data: $encodedData
             |Schema: ${schema.mkString(",")}
             |${encoder.schema.treeString}
             |
             |fromRow Expressions:
             |${boundEncoder.fromRowExpression.treeString}
         """.stripMargin)
      }
    }
  }
}

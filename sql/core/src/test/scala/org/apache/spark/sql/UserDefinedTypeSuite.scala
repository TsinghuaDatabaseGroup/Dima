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

import org.apache.spark.sql.catalyst.util.{GenericArrayData, ArrayData}

import scala.beans.{BeanInfo, BeanProperty}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.execution.datasources.parquet.ParquetTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.OpenHashSet


@SQLUserDefinedType(udt = classOf[MyDenseVectorUDT])
private[sql] class MyDenseVector(val data: Array[Double]) extends Serializable {
  override def equals(other: Any): Boolean = other match {
    case v: MyDenseVector =>
      java.util.Arrays.equals(this.data, v.data)
    case _ => false
  }
}

@BeanInfo
private[sql] case class MyLabeledPoint(
    @BeanProperty label: Double,
    @BeanProperty features: MyDenseVector)

private[sql] class MyDenseVectorUDT extends UserDefinedType[MyDenseVector] {

  override def sqlType: DataType = ArrayType(DoubleType, containsNull = false)

  override def serialize(obj: Any): ArrayData = {
    obj match {
      case features: MyDenseVector =>
        new GenericArrayData(features.data.map(_.asInstanceOf[Any]))
    }
  }

  override def deserialize(datum: Any): MyDenseVector = {
    datum match {
      case data: ArrayData =>
        new MyDenseVector(data.toDoubleArray())
    }
  }

  override def userClass: Class[MyDenseVector] = classOf[MyDenseVector]

  private[spark] override def asNullable: MyDenseVectorUDT = this
}

class UserDefinedTypeSuite extends QueryTest with SharedSQLContext with ParquetTest {
  import testImplicits._

  private lazy val pointsRDD = Seq(
    MyLabeledPoint(1.0, new MyDenseVector(Array(0.1, 1.0))),
    MyLabeledPoint(0.0, new MyDenseVector(Array(0.2, 2.0)))).toDF()

  test("register user type: MyDenseVector for MyLabeledPoint") {
    val labels: RDD[Double] = pointsRDD.select('label).rdd.map { case Row(v: Double) => v }
    val labelsArrays: Array[Double] = labels.collect()
    assert(labelsArrays.size === 2)
    assert(labelsArrays.contains(1.0))
    assert(labelsArrays.contains(0.0))

    val features: RDD[MyDenseVector] =
      pointsRDD.select('features).rdd.map { case Row(v: MyDenseVector) => v }
    val featuresArrays: Array[MyDenseVector] = features.collect()
    assert(featuresArrays.size === 2)
    assert(featuresArrays.contains(new MyDenseVector(Array(0.1, 1.0))))
    assert(featuresArrays.contains(new MyDenseVector(Array(0.2, 2.0))))
  }

  test("UDTs and UDFs") {
    sqlContext.udf.register("testType", (d: MyDenseVector) => d.isInstanceOf[MyDenseVector])
    pointsRDD.registerTempTable("points")
    checkAnswer(
      sql("SELECT testType(features) from points"),
      Seq(Row(true), Row(true)))
  }

  testStandardAndLegacyModes("UDTs with Parquet") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      pointsRDD.write.parquet(path)
      checkAnswer(
        sqlContext.read.parquet(path),
        Seq(
          Row(1.0, new MyDenseVector(Array(0.1, 1.0))),
          Row(0.0, new MyDenseVector(Array(0.2, 2.0)))))
    }
  }

  testStandardAndLegacyModes("Repartition UDTs with Parquet") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      pointsRDD.repartition(1).write.parquet(path)
      checkAnswer(
        sqlContext.read.parquet(path),
        Seq(
          Row(1.0, new MyDenseVector(Array(0.1, 1.0))),
          Row(0.0, new MyDenseVector(Array(0.2, 2.0)))))
    }
  }

  // Tests to make sure that all operators correctly convert types on the way out.
  test("Local UDTs") {
    val df = Seq((1, new MyDenseVector(Array(0.1, 1.0)))).toDF("int", "vec")
    df.collect()(0).getAs[MyDenseVector](1)
    df.take(1)(0).getAs[MyDenseVector](1)
    df.limit(1).groupBy('int).agg(first('vec)).collect()(0).getAs[MyDenseVector](0)
    df.orderBy('int).limit(1).groupBy('int).agg(first('vec)).collect()(0).getAs[MyDenseVector](0)
  }

  test("UDTs with JSON") {
    val data = Seq(
      "{\"id\":1,\"vec\":[1.1,2.2,3.3,4.4]}",
      "{\"id\":2,\"vec\":[2.25,4.5,8.75]}"
    )
    val schema = StructType(Seq(
      StructField("id", IntegerType, false),
      StructField("vec", new MyDenseVectorUDT, false)
    ))

    val stringRDD = sparkContext.parallelize(data)
    val jsonRDD = sqlContext.read.schema(schema).json(stringRDD)
    checkAnswer(
      jsonRDD,
      Row(1, new MyDenseVector(Array(1.1, 2.2, 3.3, 4.4))) ::
        Row(2, new MyDenseVector(Array(2.25, 4.5, 8.75))) ::
        Nil
    )
  }

  test("SPARK-10472 UserDefinedType.typeName") {
    assert(IntegerType.typeName === "integer")
    assert(new MyDenseVectorUDT().typeName === "mydensevector")
  }

  test("Catalyst type converter null handling for UDTs") {
    val udt = new MyDenseVectorUDT()
    val toScalaConverter = CatalystTypeConverters.createToScalaConverter(udt)
    assert(toScalaConverter(null) === null)

    val toCatalystConverter = CatalystTypeConverters.createToCatalystConverter(udt)
    assert(toCatalystConverter(null) === null)

  }
}

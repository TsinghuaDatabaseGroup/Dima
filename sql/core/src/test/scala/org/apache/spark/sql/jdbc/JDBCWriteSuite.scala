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

package org.apache.spark.sql.jdbc

import java.sql.DriverManager
import java.util.Properties

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class JDBCWriteSuite extends SharedSQLContext with BeforeAndAfter {

  val url = "jdbc:h2:mem:testdb2"
  var conn: java.sql.Connection = null
  val url1 = "jdbc:h2:mem:testdb3"
  var conn1: java.sql.Connection = null
  val properties = new Properties()
  properties.setProperty("user", "testUser")
  properties.setProperty("password", "testPass")
  properties.setProperty("rowId", "false")

  before {
    Utils.classForName("org.h2.Driver")
    conn = DriverManager.getConnection(url)
    conn.prepareStatement("create schema test").executeUpdate()

    conn1 = DriverManager.getConnection(url1, properties)
    conn1.prepareStatement("create schema test").executeUpdate()
    conn1.prepareStatement("drop table if exists test.people").executeUpdate()
    conn1.prepareStatement(
      "create table test.people (name TEXT(32) NOT NULL, theid INTEGER NOT NULL)").executeUpdate()
    conn1.prepareStatement("insert into test.people values ('fred', 1)").executeUpdate()
    conn1.prepareStatement("insert into test.people values ('mary', 2)").executeUpdate()
    conn1.prepareStatement("drop table if exists test.people1").executeUpdate()
    conn1.prepareStatement(
      "create table test.people1 (name TEXT(32) NOT NULL, theid INTEGER NOT NULL)").executeUpdate()
    conn1.commit()

    sql(
      s"""
        |CREATE TEMPORARY TABLE PEOPLE
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url1', dbtable 'TEST.PEOPLE', user 'testUser', password 'testPass')
      """.stripMargin.replaceAll("\n", " "))

    sql(
      s"""
        |CREATE TEMPORARY TABLE PEOPLE1
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url1', dbtable 'TEST.PEOPLE1', user 'testUser', password 'testPass')
      """.stripMargin.replaceAll("\n", " "))
  }

  after {
    conn.close()
    conn1.close()
  }

  private lazy val arr2x2 = Array[Row](Row.apply("dave", 42), Row.apply("mary", 222))
  private lazy val arr1x2 = Array[Row](Row.apply("fred", 3))
  private lazy val schema2 = StructType(
      StructField("name", StringType) ::
      StructField("id", IntegerType) :: Nil)

  private lazy val arr2x3 = Array[Row](Row.apply("dave", 42, 1), Row.apply("mary", 222, 2))
  private lazy val schema3 = StructType(
      StructField("name", StringType) ::
      StructField("id", IntegerType) ::
      StructField("seq", IntegerType) :: Nil)

  test("Basic CREATE") {
    val df = sqlContext.createDataFrame(sparkContext.parallelize(arr2x2), schema2)

    df.write.jdbc(url, "TEST.BASICCREATETEST", new Properties)
    assert(2 === sqlContext.read.jdbc(url, "TEST.BASICCREATETEST", new Properties).count)
    assert(
      2 === sqlContext.read.jdbc(url, "TEST.BASICCREATETEST", new Properties).collect()(0).length)
  }

  test("CREATE with overwrite") {
    val df = sqlContext.createDataFrame(sparkContext.parallelize(arr2x3), schema3)
    val df2 = sqlContext.createDataFrame(sparkContext.parallelize(arr1x2), schema2)

    df.write.jdbc(url1, "TEST.DROPTEST", properties)
    assert(2 === sqlContext.read.jdbc(url1, "TEST.DROPTEST", properties).count)
    assert(3 === sqlContext.read.jdbc(url1, "TEST.DROPTEST", properties).collect()(0).length)

    df2.write.mode(SaveMode.Overwrite).jdbc(url1, "TEST.DROPTEST", properties)
    assert(1 === sqlContext.read.jdbc(url1, "TEST.DROPTEST", properties).count)
    assert(2 === sqlContext.read.jdbc(url1, "TEST.DROPTEST", properties).collect()(0).length)
  }

  test("CREATE then INSERT to append") {
    val df = sqlContext.createDataFrame(sparkContext.parallelize(arr2x2), schema2)
    val df2 = sqlContext.createDataFrame(sparkContext.parallelize(arr1x2), schema2)

    df.write.jdbc(url, "TEST.APPENDTEST", new Properties)
    df2.write.mode(SaveMode.Append).jdbc(url, "TEST.APPENDTEST", new Properties)
    assert(3 === sqlContext.read.jdbc(url, "TEST.APPENDTEST", new Properties).count)
    assert(2 === sqlContext.read.jdbc(url, "TEST.APPENDTEST", new Properties).collect()(0).length)
  }

  test("CREATE then INSERT to truncate") {
    val df = sqlContext.createDataFrame(sparkContext.parallelize(arr2x2), schema2)
    val df2 = sqlContext.createDataFrame(sparkContext.parallelize(arr1x2), schema2)

    df.write.jdbc(url1, "TEST.TRUNCATETEST", properties)
    df2.write.mode(SaveMode.Overwrite).jdbc(url1, "TEST.TRUNCATETEST", properties)
    assert(1 === sqlContext.read.jdbc(url1, "TEST.TRUNCATETEST", properties).count)
    assert(2 === sqlContext.read.jdbc(url1, "TEST.TRUNCATETEST", properties).collect()(0).length)
  }

  test("Incompatible INSERT to append") {
    val df = sqlContext.createDataFrame(sparkContext.parallelize(arr2x2), schema2)
    val df2 = sqlContext.createDataFrame(sparkContext.parallelize(arr2x3), schema3)

    df.write.jdbc(url, "TEST.INCOMPATIBLETEST", new Properties)
    intercept[org.apache.spark.SparkException] {
      df2.write.mode(SaveMode.Append).jdbc(url, "TEST.INCOMPATIBLETEST", new Properties)
    }
  }

  test("INSERT to JDBC Datasource") {
    sql("INSERT INTO TABLE PEOPLE1 SELECT * FROM PEOPLE")
    assert(2 === sqlContext.read.jdbc(url1, "TEST.PEOPLE1", properties).count)
    assert(2 === sqlContext.read.jdbc(url1, "TEST.PEOPLE1", properties).collect()(0).length)
  }

  test("INSERT to JDBC Datasource with overwrite") {
    sql("INSERT INTO TABLE PEOPLE1 SELECT * FROM PEOPLE")
    sql("INSERT OVERWRITE TABLE PEOPLE1 SELECT * FROM PEOPLE")
    assert(2 === sqlContext.read.jdbc(url1, "TEST.PEOPLE1", properties).count)
    assert(2 === sqlContext.read.jdbc(url1, "TEST.PEOPLE1", properties).collect()(0).length)
  }
}

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

import java.sql.Connection
import java.util.Properties

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{Literal, If}
import org.apache.spark.tags.DockerTest

@DockerTest
class PostgresIntegrationSuite extends DockerJDBCIntegrationSuite {
  override val db = new DatabaseOnDocker {
    override val imageName = "postgres:9.4.5"
    override val env = Map(
      "POSTGRES_PASSWORD" -> "rootpass"
    )
    override val jdbcPort = 5432
    override def getJdbcUrl(ip: String, port: Int): String =
      s"jdbc:postgresql://$ip:$port/postgres?user=postgres&password=rootpass"
  }

  override def dataPreparation(conn: Connection): Unit = {
    conn.prepareStatement("CREATE DATABASE foo").executeUpdate()
    conn.setCatalog("foo")
    conn.prepareStatement("CREATE TABLE bar (c0 text, c1 integer, c2 double precision, c3 bigint, "
      + "c4 bit(1), c5 bit(10), c6 bytea, c7 boolean, c8 inet, c9 cidr, "
      + "c10 integer[], c11 text[])").executeUpdate()
    conn.prepareStatement("INSERT INTO bar VALUES ('hello', 42, 1.25, 123456789012345, B'0', "
      + "B'1000100101', E'\\\\xDEADBEEF', true, '172.16.0.42', '192.168.0.0/16', "
      + """'{1, 2}', '{"a", null, "b"}')""").executeUpdate()
  }

  test("Type mapping for various types") {
    val df = sqlContext.read.jdbc(jdbcUrl, "bar", new Properties)
    val rows = df.collect()
    assert(rows.length == 1)
    val types = rows(0).toSeq.map(x => x.getClass)
    assert(types.length == 12)
    assert(classOf[String].isAssignableFrom(types(0)))
    assert(classOf[java.lang.Integer].isAssignableFrom(types(1)))
    assert(classOf[java.lang.Double].isAssignableFrom(types(2)))
    assert(classOf[java.lang.Long].isAssignableFrom(types(3)))
    assert(classOf[java.lang.Boolean].isAssignableFrom(types(4)))
    assert(classOf[Array[Byte]].isAssignableFrom(types(5)))
    assert(classOf[Array[Byte]].isAssignableFrom(types(6)))
    assert(classOf[java.lang.Boolean].isAssignableFrom(types(7)))
    assert(classOf[String].isAssignableFrom(types(8)))
    assert(classOf[String].isAssignableFrom(types(9)))
    assert(classOf[Seq[Int]].isAssignableFrom(types(10)))
    assert(classOf[Seq[String]].isAssignableFrom(types(11)))
    assert(rows(0).getString(0).equals("hello"))
    assert(rows(0).getInt(1) == 42)
    assert(rows(0).getDouble(2) == 1.25)
    assert(rows(0).getLong(3) == 123456789012345L)
    assert(rows(0).getBoolean(4) == false)
    // BIT(10)'s come back as ASCII strings of ten ASCII 0's and 1's...
    assert(java.util.Arrays.equals(rows(0).getAs[Array[Byte]](5),
      Array[Byte](49, 48, 48, 48, 49, 48, 48, 49, 48, 49)))
    assert(java.util.Arrays.equals(rows(0).getAs[Array[Byte]](6),
      Array[Byte](0xDE.toByte, 0xAD.toByte, 0xBE.toByte, 0xEF.toByte)))
    assert(rows(0).getBoolean(7) == true)
    assert(rows(0).getString(8) == "172.16.0.42")
    assert(rows(0).getString(9) == "192.168.0.0/16")
    assert(rows(0).getSeq(10) == Seq(1, 2))
    assert(rows(0).getSeq(11) == Seq("a", null, "b"))
  }

  test("Basic write test") {
    val df = sqlContext.read.jdbc(jdbcUrl, "bar", new Properties)
    // Test only that it doesn't crash.
    df.write.jdbc(jdbcUrl, "public.barcopy", new Properties)
    // Test write null values.
    df.select(df.queryExecution.analyzed.output.map { a =>
      Column(Literal.create(null, a.dataType)).as(a.name)
    }: _*).write.jdbc(jdbcUrl, "public.barcopy2", new Properties)
  }
}

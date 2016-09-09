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

import java.sql.{Connection, Types}

import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.types._


private object PostgresDialect extends JdbcDialect {

  override def canHandle(url: String): Boolean = url.startsWith("jdbc:postgresql")

  override def getCatalystType(
      sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    if (sqlType == Types.BIT && typeName.equals("bit") && size != 1) {
      Some(BinaryType)
    } else if (sqlType == Types.OTHER) {
      toCatalystType(typeName).filter(_ == StringType)
    } else if (sqlType == Types.ARRAY && typeName.length > 1 && typeName(0) == '_') {
      toCatalystType(typeName.drop(1)).map(ArrayType(_))
    } else None
  }

  // TODO: support more type names.
  private def toCatalystType(typeName: String): Option[DataType] = typeName match {
    case "bool" => Some(BooleanType)
    case "bit" => Some(BinaryType)
    case "int2" => Some(ShortType)
    case "int4" => Some(IntegerType)
    case "int8" | "oid" => Some(LongType)
    case "float4" => Some(FloatType)
    case "money" | "float8" => Some(DoubleType)
    case "text" | "varchar" | "char" | "cidr" | "inet" | "json" | "jsonb" | "uuid" =>
      Some(StringType)
    case "bytea" => Some(BinaryType)
    case "timestamp" | "timestamptz" | "time" | "timetz" => Some(TimestampType)
    case "date" => Some(DateType)
    case "numeric" => Some(DecimalType.SYSTEM_DEFAULT)
    case _ => None
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case StringType => Some(JdbcType("TEXT", Types.CHAR))
    case BinaryType => Some(JdbcType("BYTEA", Types.BINARY))
    case BooleanType => Some(JdbcType("BOOLEAN", Types.BOOLEAN))
    case ArrayType(et, _) if et.isInstanceOf[AtomicType] =>
      getJDBCType(et).map(_.databaseTypeDefinition)
        .orElse(JdbcUtils.getCommonJDBCType(et).map(_.databaseTypeDefinition))
        .map(typeName => JdbcType(s"$typeName[]", java.sql.Types.ARRAY))
    case _ => None
  }

  override def getTableExistsQuery(table: String): String = {
    s"SELECT 1 FROM $table LIMIT 1"
  }

  override def beforeFetch(connection: Connection, properties: Map[String, String]): Unit = {
    super.beforeFetch(connection, properties)

    // According to the postgres jdbc documentation we need to be in autocommit=false if we actually
    // want to have fetchsize be non 0 (all the rows).  This allows us to not have to cache all the
    // rows inside the driver when fetching.
    //
    // See: https://jdbc.postgresql.org/documentation/head/query.html#query-with-cursor
    //
    if (properties.getOrElse("fetchsize", "0").toInt > 0) {
      connection.setAutoCommit(false)
    }

  }
}

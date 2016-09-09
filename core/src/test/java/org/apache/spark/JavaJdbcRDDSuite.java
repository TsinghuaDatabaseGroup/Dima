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
package org.apache.spark;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.rdd.JdbcRDD;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JavaJdbcRDDSuite implements Serializable {
  private transient JavaSparkContext sc;

  @Before
  public void setUp() throws ClassNotFoundException, SQLException {
    sc = new JavaSparkContext("local", "JavaAPISuite");

    Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
    Connection connection =
      DriverManager.getConnection("jdbc:derby:target/JavaJdbcRDDSuiteDb;create=true");

    try {
      Statement create = connection.createStatement();
      create.execute(
        "CREATE TABLE FOO(" +
        "ID INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)," +
        "DATA INTEGER)");
      create.close();

      PreparedStatement insert = connection.prepareStatement("INSERT INTO FOO(DATA) VALUES(?)");
      for (int i = 1; i <= 100; i++) {
        insert.setInt(1, i * 2);
        insert.executeUpdate();
      }
      insert.close();
    } catch (SQLException e) {
      // If table doesn't exist...
      if (e.getSQLState().compareTo("X0Y32") != 0) {
        throw e;
      }
    } finally {
      connection.close();
    }
  }

  @After
  public void tearDown() throws SQLException {
    try {
      DriverManager.getConnection("jdbc:derby:target/JavaJdbcRDDSuiteDb;shutdown=true");
    } catch(SQLException e) {
      // Throw if not normal single database shutdown
      // https://db.apache.org/derby/docs/10.2/ref/rrefexcept71493.html
      if (e.getSQLState().compareTo("08006") != 0) {
        throw e;
      }
    }

    sc.stop();
    sc = null;
  }

  @Test
  public void testJavaJdbcRDD() throws Exception {
    JavaRDD<Integer> rdd = JdbcRDD.create(
      sc,
      new JdbcRDD.ConnectionFactory() {
        @Override
        public Connection getConnection() throws SQLException {
          return DriverManager.getConnection("jdbc:derby:target/JavaJdbcRDDSuiteDb");
        }
      },
      "SELECT DATA FROM FOO WHERE ? <= ID AND ID <= ?",
      1, 100, 1,
      new Function<ResultSet, Integer>() {
        @Override
        public Integer call(ResultSet r) throws Exception {
          return r.getInt(1);
        }
      }
    ).cache();

    Assert.assertEquals(100, rdd.count());
    Assert.assertEquals(
      Integer.valueOf(10100),
      rdd.reduce(new Function2<Integer, Integer, Integer>() {
        @Override
        public Integer call(Integer i1, Integer i2) {
          return i1 + i2;
        }
      }));
  }
}

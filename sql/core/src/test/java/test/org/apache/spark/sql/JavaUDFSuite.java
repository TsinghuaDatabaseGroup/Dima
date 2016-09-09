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

package test.org.apache.spark.sql;

import java.io.Serializable;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.types.DataTypes;

// The test suite itself is Serializable so that anonymous Function implementations can be
// serialized, as an alternative to converting these anonymous classes to static inner classes;
// see http://stackoverflow.com/questions/758570/.
public class JavaUDFSuite implements Serializable {
  private transient JavaSparkContext sc;
  private transient SQLContext sqlContext;

  @Before
  public void setUp() {
    SparkContext _sc = new SparkContext("local[*]", "testing");
    sqlContext = new SQLContext(_sc);
    sc = new JavaSparkContext(_sc);
  }

  @After
  public void tearDown() {
    sqlContext.sparkContext().stop();
    sqlContext = null;
    sc = null;
  }

  @SuppressWarnings("unchecked")
  @Test
  public void udf1Test() {
    // With Java 8 lambdas:
    // sqlContext.registerFunction(
    //   "stringLengthTest", (String str) -> str.length(), DataType.IntegerType);

    sqlContext.udf().register("stringLengthTest", new UDF1<String, Integer>() {
      @Override
      public Integer call(String str) {
        return str.length();
      }
    }, DataTypes.IntegerType);

    Row result = sqlContext.sql("SELECT stringLengthTest('test')").head();
    Assert.assertEquals(4, result.getInt(0));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void udf2Test() {
    // With Java 8 lambdas:
    // sqlContext.registerFunction(
    //   "stringLengthTest",
    //   (String str1, String str2) -> str1.length() + str2.length,
    //   DataType.IntegerType);

    sqlContext.udf().register("stringLengthTest", new UDF2<String, String, Integer>() {
      @Override
      public Integer call(String str1, String str2) {
        return str1.length() + str2.length();
      }
    }, DataTypes.IntegerType);

    Row result = sqlContext.sql("SELECT stringLengthTest('test', 'test2')").head();
    Assert.assertEquals(9, result.getInt(0));
  }
}

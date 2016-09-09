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

package org.apache.spark.mllib.feature;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;

public class JavaTfIdfSuite implements Serializable {
  private transient JavaSparkContext sc;

  @Before
  public void setUp() {
    sc = new JavaSparkContext("local", "JavaTfIdfSuite");
  }

  @After
  public void tearDown() {
    sc.stop();
    sc = null;
  }

  @Test
  public void tfIdf() {
    // The tests are to check Java compatibility.
    HashingTF tf = new HashingTF();
    @SuppressWarnings("unchecked")
    JavaRDD<List<String>> documents = sc.parallelize(Arrays.asList(
      Arrays.asList("this is a sentence".split(" ")),
      Arrays.asList("this is another sentence".split(" ")),
      Arrays.asList("this is still a sentence".split(" "))), 2);
    JavaRDD<Vector> termFreqs = tf.transform(documents);
    termFreqs.collect();
    IDF idf = new IDF();
    JavaRDD<Vector> tfIdfs = idf.fit(termFreqs).transform(termFreqs);
    List<Vector> localTfIdfs = tfIdfs.collect();
    int indexOfThis = tf.indexOf("this");
    for (Vector v: localTfIdfs) {
      Assert.assertEquals(0.0, v.apply(indexOfThis), 1e-15);
    }
  }

  @Test
  public void tfIdfMinimumDocumentFrequency() {
    // The tests are to check Java compatibility.
    HashingTF tf = new HashingTF();
    @SuppressWarnings("unchecked")
    JavaRDD<List<String>> documents = sc.parallelize(Arrays.asList(
      Arrays.asList("this is a sentence".split(" ")),
      Arrays.asList("this is another sentence".split(" ")),
      Arrays.asList("this is still a sentence".split(" "))), 2);
    JavaRDD<Vector> termFreqs = tf.transform(documents);
    termFreqs.collect();
    IDF idf = new IDF(2);
    JavaRDD<Vector> tfIdfs = idf.fit(termFreqs).transform(termFreqs);
    List<Vector> localTfIdfs = tfIdfs.collect();
    int indexOfThis = tf.indexOf("this");
    for (Vector v: localTfIdfs) {
      Assert.assertEquals(0.0, v.apply(indexOfThis), 1e-15);
    }
  }

}

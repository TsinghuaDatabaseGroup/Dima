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

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.net.URI;
import java.util.*;
import java.util.concurrent.*;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.collection.JavaConverters;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.base.Throwables;
import com.google.common.base.Optional;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.partial.BoundedDouble;
import org.apache.spark.partial.PartialResult;
import org.apache.spark.rdd.RDD;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.StatCounter;

// The test suite itself is Serializable so that anonymous Function implementations can be
// serialized, as an alternative to converting these anonymous classes to static inner classes;
// see http://stackoverflow.com/questions/758570/.
public class JavaAPISuite implements Serializable {
  private transient JavaSparkContext sc;
  private transient File tempDir;

  @Before
  public void setUp() {
    sc = new JavaSparkContext("local", "JavaAPISuite");
    tempDir = Files.createTempDir();
    tempDir.deleteOnExit();
  }

  @After
  public void tearDown() {
    sc.stop();
    sc = null;
  }

  @SuppressWarnings("unchecked")
  @Test
  public void sparkContextUnion() {
    // Union of non-specialized JavaRDDs
    List<String> strings = Arrays.asList("Hello", "World");
    JavaRDD<String> s1 = sc.parallelize(strings);
    JavaRDD<String> s2 = sc.parallelize(strings);
    // Varargs
    JavaRDD<String> sUnion = sc.union(s1, s2);
    Assert.assertEquals(4, sUnion.count());
    // List
    List<JavaRDD<String>> list = new ArrayList<>();
    list.add(s2);
    sUnion = sc.union(s1, list);
    Assert.assertEquals(4, sUnion.count());

    // Union of JavaDoubleRDDs
    List<Double> doubles = Arrays.asList(1.0, 2.0);
    JavaDoubleRDD d1 = sc.parallelizeDoubles(doubles);
    JavaDoubleRDD d2 = sc.parallelizeDoubles(doubles);
    JavaDoubleRDD dUnion = sc.union(d1, d2);
    Assert.assertEquals(4, dUnion.count());

    // Union of JavaPairRDDs
    List<Tuple2<Integer, Integer>> pairs = new ArrayList<>();
    pairs.add(new Tuple2<>(1, 2));
    pairs.add(new Tuple2<>(3, 4));
    JavaPairRDD<Integer, Integer> p1 = sc.parallelizePairs(pairs);
    JavaPairRDD<Integer, Integer> p2 = sc.parallelizePairs(pairs);
    JavaPairRDD<Integer, Integer> pUnion = sc.union(p1, p2);
    Assert.assertEquals(4, pUnion.count());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void intersection() {
    List<Integer> ints1 = Arrays.asList(1, 10, 2, 3, 4, 5);
    List<Integer> ints2 = Arrays.asList(1, 6, 2, 3, 7, 8);
    JavaRDD<Integer> s1 = sc.parallelize(ints1);
    JavaRDD<Integer> s2 = sc.parallelize(ints2);

    JavaRDD<Integer> intersections = s1.intersection(s2);
    Assert.assertEquals(3, intersections.count());

    JavaRDD<Integer> empty = sc.emptyRDD();
    JavaRDD<Integer> emptyIntersection = empty.intersection(s2);
    Assert.assertEquals(0, emptyIntersection.count());

    List<Double> doubles = Arrays.asList(1.0, 2.0);
    JavaDoubleRDD d1 = sc.parallelizeDoubles(doubles);
    JavaDoubleRDD d2 = sc.parallelizeDoubles(doubles);
    JavaDoubleRDD dIntersection = d1.intersection(d2);
    Assert.assertEquals(2, dIntersection.count());

    List<Tuple2<Integer, Integer>> pairs = new ArrayList<>();
    pairs.add(new Tuple2<>(1, 2));
    pairs.add(new Tuple2<>(3, 4));
    JavaPairRDD<Integer, Integer> p1 = sc.parallelizePairs(pairs);
    JavaPairRDD<Integer, Integer> p2 = sc.parallelizePairs(pairs);
    JavaPairRDD<Integer, Integer> pIntersection = p1.intersection(p2);
    Assert.assertEquals(2, pIntersection.count());
  }

  @Test
  public void sample() {
    List<Integer> ints = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    JavaRDD<Integer> rdd = sc.parallelize(ints);
    // the seeds here are "magic" to make this work out nicely
    JavaRDD<Integer> sample20 = rdd.sample(true, 0.2, 8);
    Assert.assertEquals(2, sample20.count());
    JavaRDD<Integer> sample20WithoutReplacement = rdd.sample(false, 0.2, 2);
    Assert.assertEquals(2, sample20WithoutReplacement.count());
  }

  @Test
  public void randomSplit() {
    List<Integer> ints = new ArrayList<>(1000);
    for (int i = 0; i < 1000; i++) {
      ints.add(i);
    }
    JavaRDD<Integer> rdd = sc.parallelize(ints);
    JavaRDD<Integer>[] splits = rdd.randomSplit(new double[] { 0.4, 0.6, 1.0 }, 31);
    // the splits aren't perfect -- not enough data for them to be -- just check they're about right
    Assert.assertEquals(3, splits.length);
    long s0 = splits[0].count();
    long s1 = splits[1].count();
    long s2 = splits[2].count();
    Assert.assertTrue(s0 + " not within expected range", s0 > 150 && s0 < 250);
    Assert.assertTrue(s1 + " not within expected range", s1 > 250 && s0 < 350);
    Assert.assertTrue(s2 + " not within expected range", s2 > 430 && s2 < 570);
  }

  @Test
  public void sortByKey() {
    List<Tuple2<Integer, Integer>> pairs = new ArrayList<>();
    pairs.add(new Tuple2<>(0, 4));
    pairs.add(new Tuple2<>(3, 2));
    pairs.add(new Tuple2<>(-1, 1));

    JavaPairRDD<Integer, Integer> rdd = sc.parallelizePairs(pairs);

    // Default comparator
    JavaPairRDD<Integer, Integer> sortedRDD = rdd.sortByKey();
    Assert.assertEquals(new Tuple2<>(-1, 1), sortedRDD.first());
    List<Tuple2<Integer, Integer>> sortedPairs = sortedRDD.collect();
    Assert.assertEquals(new Tuple2<>(0, 4), sortedPairs.get(1));
    Assert.assertEquals(new Tuple2<>(3, 2), sortedPairs.get(2));

    // Custom comparator
    sortedRDD = rdd.sortByKey(Collections.<Integer>reverseOrder(), false);
    Assert.assertEquals(new Tuple2<>(-1, 1), sortedRDD.first());
    sortedPairs = sortedRDD.collect();
    Assert.assertEquals(new Tuple2<>(0, 4), sortedPairs.get(1));
    Assert.assertEquals(new Tuple2<>(3, 2), sortedPairs.get(2));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void repartitionAndSortWithinPartitions() {
    List<Tuple2<Integer, Integer>> pairs = new ArrayList<>();
    pairs.add(new Tuple2<>(0, 5));
    pairs.add(new Tuple2<>(3, 8));
    pairs.add(new Tuple2<>(2, 6));
    pairs.add(new Tuple2<>(0, 8));
    pairs.add(new Tuple2<>(3, 8));
    pairs.add(new Tuple2<>(1, 3));

    JavaPairRDD<Integer, Integer> rdd = sc.parallelizePairs(pairs);

    Partitioner partitioner = new Partitioner() {
      @Override
      public int numPartitions() {
        return 2;
      }
      @Override
      public int getPartition(Object key) {
        return (Integer) key % 2;
      }
    };

    JavaPairRDD<Integer, Integer> repartitioned =
        rdd.repartitionAndSortWithinPartitions(partitioner);
    Assert.assertTrue(repartitioned.partitioner().isPresent());
    Assert.assertEquals(repartitioned.partitioner().get(), partitioner);
    List<List<Tuple2<Integer, Integer>>> partitions = repartitioned.glom().collect();
    Assert.assertEquals(partitions.get(0),
        Arrays.asList(new Tuple2<>(0, 5), new Tuple2<>(0, 8), new Tuple2<>(2, 6)));
    Assert.assertEquals(partitions.get(1),
        Arrays.asList(new Tuple2<>(1, 3), new Tuple2<>(3, 8), new Tuple2<>(3, 8)));
  }

  @Test
  public void emptyRDD() {
    JavaRDD<String> rdd = sc.emptyRDD();
    Assert.assertEquals("Empty RDD shouldn't have any values", 0, rdd.count());
  }

  @Test
  public void sortBy() {
    List<Tuple2<Integer, Integer>> pairs = new ArrayList<>();
    pairs.add(new Tuple2<>(0, 4));
    pairs.add(new Tuple2<>(3, 2));
    pairs.add(new Tuple2<>(-1, 1));

    JavaRDD<Tuple2<Integer, Integer>> rdd = sc.parallelize(pairs);

    // compare on first value
    JavaRDD<Tuple2<Integer, Integer>> sortedRDD = rdd.sortBy(new Function<Tuple2<Integer, Integer>, Integer>() {
      @Override
      public Integer call(Tuple2<Integer, Integer> t) {
        return t._1();
      }
    }, true, 2);

    Assert.assertEquals(new Tuple2<>(-1, 1), sortedRDD.first());
    List<Tuple2<Integer, Integer>> sortedPairs = sortedRDD.collect();
    Assert.assertEquals(new Tuple2<>(0, 4), sortedPairs.get(1));
    Assert.assertEquals(new Tuple2<>(3, 2), sortedPairs.get(2));

    // compare on second value
    sortedRDD = rdd.sortBy(new Function<Tuple2<Integer, Integer>, Integer>() {
      @Override
      public Integer call(Tuple2<Integer, Integer> t) {
        return t._2();
      }
    }, true, 2);
    Assert.assertEquals(new Tuple2<>(-1, 1), sortedRDD.first());
    sortedPairs = sortedRDD.collect();
    Assert.assertEquals(new Tuple2<>(3, 2), sortedPairs.get(1));
    Assert.assertEquals(new Tuple2<>(0, 4), sortedPairs.get(2));
  }

  @Test
  public void foreach() {
    final Accumulator<Integer> accum = sc.accumulator(0);
    JavaRDD<String> rdd = sc.parallelize(Arrays.asList("Hello", "World"));
    rdd.foreach(new VoidFunction<String>() {
      @Override
      public void call(String s) {
        accum.add(1);
      }
    });
    Assert.assertEquals(2, accum.value().intValue());
  }

  @Test
  public void foreachPartition() {
    final Accumulator<Integer> accum = sc.accumulator(0);
    JavaRDD<String> rdd = sc.parallelize(Arrays.asList("Hello", "World"));
    rdd.foreachPartition(new VoidFunction<Iterator<String>>() {
      @Override
      public void call(Iterator<String> iter) {
        while (iter.hasNext()) {
          iter.next();
          accum.add(1);
        }
      }
    });
    Assert.assertEquals(2, accum.value().intValue());
  }

  @Test
  public void toLocalIterator() {
    List<Integer> correct = Arrays.asList(1, 2, 3, 4);
    JavaRDD<Integer> rdd = sc.parallelize(correct);
    List<Integer> result = Lists.newArrayList(rdd.toLocalIterator());
    Assert.assertEquals(correct, result);
  }

  @Test
  public void zipWithUniqueId() {
    List<Integer> dataArray = Arrays.asList(1, 2, 3, 4);
    JavaPairRDD<Integer, Long> zip = sc.parallelize(dataArray).zipWithUniqueId();
    JavaRDD<Long> indexes = zip.values();
    Assert.assertEquals(4, new HashSet<>(indexes.collect()).size());
  }

  @Test
  public void zipWithIndex() {
    List<Integer> dataArray = Arrays.asList(1, 2, 3, 4);
    JavaPairRDD<Integer, Long> zip = sc.parallelize(dataArray).zipWithIndex();
    JavaRDD<Long> indexes = zip.values();
    List<Long> correctIndexes = Arrays.asList(0L, 1L, 2L, 3L);
    Assert.assertEquals(correctIndexes, indexes.collect());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void lookup() {
    JavaPairRDD<String, String> categories = sc.parallelizePairs(Arrays.asList(
      new Tuple2<>("Apples", "Fruit"),
      new Tuple2<>("Oranges", "Fruit"),
      new Tuple2<>("Oranges", "Citrus")
    ));
    Assert.assertEquals(2, categories.lookup("Oranges").size());
    Assert.assertEquals(2, Iterables.size(categories.groupByKey().lookup("Oranges").get(0)));
  }

  @Test
  public void groupBy() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 1, 2, 3, 5, 8, 13));
    Function<Integer, Boolean> isOdd = new Function<Integer, Boolean>() {
      @Override
      public Boolean call(Integer x) {
        return x % 2 == 0;
      }
    };
    JavaPairRDD<Boolean, Iterable<Integer>> oddsAndEvens = rdd.groupBy(isOdd);
    Assert.assertEquals(2, oddsAndEvens.count());
    Assert.assertEquals(2, Iterables.size(oddsAndEvens.lookup(true).get(0)));  // Evens
    Assert.assertEquals(5, Iterables.size(oddsAndEvens.lookup(false).get(0))); // Odds

    oddsAndEvens = rdd.groupBy(isOdd, 1);
    Assert.assertEquals(2, oddsAndEvens.count());
    Assert.assertEquals(2, Iterables.size(oddsAndEvens.lookup(true).get(0)));  // Evens
    Assert.assertEquals(5, Iterables.size(oddsAndEvens.lookup(false).get(0))); // Odds
  }

  @Test
  public void groupByOnPairRDD() {
    // Regression test for SPARK-4459
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 1, 2, 3, 5, 8, 13));
    Function<Tuple2<Integer, Integer>, Boolean> areOdd =
      new Function<Tuple2<Integer, Integer>, Boolean>() {
        @Override
        public Boolean call(Tuple2<Integer, Integer> x) {
          return (x._1() % 2 == 0) && (x._2() % 2 == 0);
        }
      };
    JavaPairRDD<Integer, Integer> pairRDD = rdd.zip(rdd);
    JavaPairRDD<Boolean, Iterable<Tuple2<Integer, Integer>>> oddsAndEvens = pairRDD.groupBy(areOdd);
    Assert.assertEquals(2, oddsAndEvens.count());
    Assert.assertEquals(2, Iterables.size(oddsAndEvens.lookup(true).get(0)));  // Evens
    Assert.assertEquals(5, Iterables.size(oddsAndEvens.lookup(false).get(0))); // Odds

    oddsAndEvens = pairRDD.groupBy(areOdd, 1);
    Assert.assertEquals(2, oddsAndEvens.count());
    Assert.assertEquals(2, Iterables.size(oddsAndEvens.lookup(true).get(0)));  // Evens
    Assert.assertEquals(5, Iterables.size(oddsAndEvens.lookup(false).get(0))); // Odds
  }

  @SuppressWarnings("unchecked")
  @Test
  public void keyByOnPairRDD() {
    // Regression test for SPARK-4459
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 1, 2, 3, 5, 8, 13));
    Function<Tuple2<Integer, Integer>, String> sumToString =
      new Function<Tuple2<Integer, Integer>, String>() {
        @Override
        public String call(Tuple2<Integer, Integer> x) {
          return String.valueOf(x._1() + x._2());
        }
      };
    JavaPairRDD<Integer, Integer> pairRDD = rdd.zip(rdd);
    JavaPairRDD<String, Tuple2<Integer, Integer>> keyed = pairRDD.keyBy(sumToString);
    Assert.assertEquals(7, keyed.count());
    Assert.assertEquals(1, (long) keyed.lookup("2").get(0)._1());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void cogroup() {
    JavaPairRDD<String, String> categories = sc.parallelizePairs(Arrays.asList(
      new Tuple2<>("Apples", "Fruit"),
      new Tuple2<>("Oranges", "Fruit"),
      new Tuple2<>("Oranges", "Citrus")
      ));
    JavaPairRDD<String, Integer> prices = sc.parallelizePairs(Arrays.asList(
      new Tuple2<>("Oranges", 2),
      new Tuple2<>("Apples", 3)
    ));
    JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<Integer>>> cogrouped =
        categories.cogroup(prices);
    Assert.assertEquals("[Fruit, Citrus]", Iterables.toString(cogrouped.lookup("Oranges").get(0)._1()));
    Assert.assertEquals("[2]", Iterables.toString(cogrouped.lookup("Oranges").get(0)._2()));

    cogrouped.collect();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void cogroup3() {
    JavaPairRDD<String, String> categories = sc.parallelizePairs(Arrays.asList(
      new Tuple2<>("Apples", "Fruit"),
      new Tuple2<>("Oranges", "Fruit"),
      new Tuple2<>("Oranges", "Citrus")
      ));
    JavaPairRDD<String, Integer> prices = sc.parallelizePairs(Arrays.asList(
      new Tuple2<>("Oranges", 2),
      new Tuple2<>("Apples", 3)
    ));
    JavaPairRDD<String, Integer> quantities = sc.parallelizePairs(Arrays.asList(
      new Tuple2<>("Oranges", 21),
      new Tuple2<>("Apples", 42)
    ));

    JavaPairRDD<String, Tuple3<Iterable<String>, Iterable<Integer>, Iterable<Integer>>> cogrouped =
        categories.cogroup(prices, quantities);
    Assert.assertEquals("[Fruit, Citrus]", Iterables.toString(cogrouped.lookup("Oranges").get(0)._1()));
    Assert.assertEquals("[2]", Iterables.toString(cogrouped.lookup("Oranges").get(0)._2()));
    Assert.assertEquals("[42]", Iterables.toString(cogrouped.lookup("Apples").get(0)._3()));


    cogrouped.collect();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void cogroup4() {
    JavaPairRDD<String, String> categories = sc.parallelizePairs(Arrays.asList(
      new Tuple2<>("Apples", "Fruit"),
      new Tuple2<>("Oranges", "Fruit"),
      new Tuple2<>("Oranges", "Citrus")
      ));
    JavaPairRDD<String, Integer> prices = sc.parallelizePairs(Arrays.asList(
      new Tuple2<>("Oranges", 2),
      new Tuple2<>("Apples", 3)
    ));
    JavaPairRDD<String, Integer> quantities = sc.parallelizePairs(Arrays.asList(
      new Tuple2<>("Oranges", 21),
      new Tuple2<>("Apples", 42)
    ));
    JavaPairRDD<String, String> countries = sc.parallelizePairs(Arrays.asList(
      new Tuple2<>("Oranges", "BR"),
      new Tuple2<>("Apples", "US")
    ));

    JavaPairRDD<String, Tuple4<Iterable<String>, Iterable<Integer>, Iterable<Integer>, Iterable<String>>> cogrouped =
        categories.cogroup(prices, quantities, countries);
    Assert.assertEquals("[Fruit, Citrus]", Iterables.toString(cogrouped.lookup("Oranges").get(0)._1()));
    Assert.assertEquals("[2]", Iterables.toString(cogrouped.lookup("Oranges").get(0)._2()));
    Assert.assertEquals("[42]", Iterables.toString(cogrouped.lookup("Apples").get(0)._3()));
    Assert.assertEquals("[BR]", Iterables.toString(cogrouped.lookup("Oranges").get(0)._4()));

    cogrouped.collect();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void leftOuterJoin() {
    JavaPairRDD<Integer, Integer> rdd1 = sc.parallelizePairs(Arrays.asList(
      new Tuple2<>(1, 1),
      new Tuple2<>(1, 2),
      new Tuple2<>(2, 1),
      new Tuple2<>(3, 1)
      ));
    JavaPairRDD<Integer, Character> rdd2 = sc.parallelizePairs(Arrays.asList(
      new Tuple2<>(1, 'x'),
      new Tuple2<>(2, 'y'),
      new Tuple2<>(2, 'z'),
      new Tuple2<>(4, 'w')
    ));
    List<Tuple2<Integer,Tuple2<Integer,Optional<Character>>>> joined =
      rdd1.leftOuterJoin(rdd2).collect();
    Assert.assertEquals(5, joined.size());
    Tuple2<Integer,Tuple2<Integer,Optional<Character>>> firstUnmatched =
      rdd1.leftOuterJoin(rdd2).filter(
        new Function<Tuple2<Integer, Tuple2<Integer, Optional<Character>>>, Boolean>() {
          @Override
          public Boolean call(Tuple2<Integer, Tuple2<Integer, Optional<Character>>> tup) {
            return !tup._2()._2().isPresent();
          }
      }).first();
    Assert.assertEquals(3, firstUnmatched._1().intValue());
  }

  @Test
  public void foldReduce() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 1, 2, 3, 5, 8, 13));
    Function2<Integer, Integer, Integer> add = new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer a, Integer b) {
        return a + b;
      }
    };

    int sum = rdd.fold(0, add);
    Assert.assertEquals(33, sum);

    sum = rdd.reduce(add);
    Assert.assertEquals(33, sum);
  }

  @Test
  public void treeReduce() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(-5, -4, -3, -2, -1, 1, 2, 3, 4), 10);
    Function2<Integer, Integer, Integer> add = new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer a, Integer b) {
        return a + b;
      }
    };
    for (int depth = 1; depth <= 10; depth++) {
      int sum = rdd.treeReduce(add, depth);
      Assert.assertEquals(-5, sum);
    }
  }

  @Test
  public void treeAggregate() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(-5, -4, -3, -2, -1, 1, 2, 3, 4), 10);
    Function2<Integer, Integer, Integer> add = new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer a, Integer b) {
        return a + b;
      }
    };
    for (int depth = 1; depth <= 10; depth++) {
      int sum = rdd.treeAggregate(0, add, add, depth);
      Assert.assertEquals(-5, sum);
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void aggregateByKey() {
    JavaPairRDD<Integer, Integer> pairs = sc.parallelizePairs(
      Arrays.asList(
        new Tuple2<>(1, 1),
        new Tuple2<>(1, 1),
        new Tuple2<>(3, 2),
        new Tuple2<>(5, 1),
        new Tuple2<>(5, 3)), 2);

    Map<Integer, Set<Integer>> sets = pairs.aggregateByKey(new HashSet<Integer>(),
      new Function2<Set<Integer>, Integer, Set<Integer>>() {
        @Override
        public Set<Integer> call(Set<Integer> a, Integer b) {
          a.add(b);
          return a;
        }
      },
      new Function2<Set<Integer>, Set<Integer>, Set<Integer>>() {
        @Override
        public Set<Integer> call(Set<Integer> a, Set<Integer> b) {
          a.addAll(b);
          return a;
        }
      }).collectAsMap();
    Assert.assertEquals(3, sets.size());
    Assert.assertEquals(new HashSet<>(Arrays.asList(1)), sets.get(1));
    Assert.assertEquals(new HashSet<>(Arrays.asList(2)), sets.get(3));
    Assert.assertEquals(new HashSet<>(Arrays.asList(1, 3)), sets.get(5));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void foldByKey() {
    List<Tuple2<Integer, Integer>> pairs = Arrays.asList(
      new Tuple2<>(2, 1),
      new Tuple2<>(2, 1),
      new Tuple2<>(1, 1),
      new Tuple2<>(3, 2),
      new Tuple2<>(3, 1)
    );
    JavaPairRDD<Integer, Integer> rdd = sc.parallelizePairs(pairs);
    JavaPairRDD<Integer, Integer> sums = rdd.foldByKey(0,
      new Function2<Integer, Integer, Integer>() {
        @Override
        public Integer call(Integer a, Integer b) {
          return a + b;
        }
    });
    Assert.assertEquals(1, sums.lookup(1).get(0).intValue());
    Assert.assertEquals(2, sums.lookup(2).get(0).intValue());
    Assert.assertEquals(3, sums.lookup(3).get(0).intValue());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void reduceByKey() {
    List<Tuple2<Integer, Integer>> pairs = Arrays.asList(
      new Tuple2<>(2, 1),
      new Tuple2<>(2, 1),
      new Tuple2<>(1, 1),
      new Tuple2<>(3, 2),
      new Tuple2<>(3, 1)
    );
    JavaPairRDD<Integer, Integer> rdd = sc.parallelizePairs(pairs);
    JavaPairRDD<Integer, Integer> counts = rdd.reduceByKey(
      new Function2<Integer, Integer, Integer>() {
        @Override
        public Integer call(Integer a, Integer b) {
         return a + b;
        }
    });
    Assert.assertEquals(1, counts.lookup(1).get(0).intValue());
    Assert.assertEquals(2, counts.lookup(2).get(0).intValue());
    Assert.assertEquals(3, counts.lookup(3).get(0).intValue());

    Map<Integer, Integer> localCounts = counts.collectAsMap();
    Assert.assertEquals(1, localCounts.get(1).intValue());
    Assert.assertEquals(2, localCounts.get(2).intValue());
    Assert.assertEquals(3, localCounts.get(3).intValue());

    localCounts = rdd.reduceByKeyLocally(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer a, Integer b) {
        return a + b;
      }
    });
    Assert.assertEquals(1, localCounts.get(1).intValue());
    Assert.assertEquals(2, localCounts.get(2).intValue());
    Assert.assertEquals(3, localCounts.get(3).intValue());
  }

  @Test
  public void approximateResults() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 1, 2, 3, 5, 8, 13));
    Map<Integer, Long> countsByValue = rdd.countByValue();
    Assert.assertEquals(2, countsByValue.get(1).longValue());
    Assert.assertEquals(1, countsByValue.get(13).longValue());

    PartialResult<Map<Integer, BoundedDouble>> approx = rdd.countByValueApprox(1);
    Map<Integer, BoundedDouble> finalValue = approx.getFinalValue();
    Assert.assertEquals(2.0, finalValue.get(1).mean(), 0.01);
    Assert.assertEquals(1.0, finalValue.get(13).mean(), 0.01);
  }

  @Test
  public void take() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 1, 2, 3, 5, 8, 13));
    Assert.assertEquals(1, rdd.first().intValue());
    rdd.take(2);
    rdd.takeSample(false, 2, 42);
  }

  @Test
  public void isEmpty() {
    Assert.assertTrue(sc.emptyRDD().isEmpty());
    Assert.assertTrue(sc.parallelize(new ArrayList<Integer>()).isEmpty());
    Assert.assertFalse(sc.parallelize(Arrays.asList(1)).isEmpty());
    Assert.assertTrue(sc.parallelize(Arrays.asList(1, 2, 3), 3).filter(
        new Function<Integer,Boolean>() {
          @Override
          public Boolean call(Integer i) {
            return i < 0;
          }
        }).isEmpty());
    Assert.assertFalse(sc.parallelize(Arrays.asList(1, 2, 3)).filter(
        new Function<Integer, Boolean>() {
          @Override
          public Boolean call(Integer i) {
            return i > 1;
          }
        }).isEmpty());
  }

  @Test
  public void toArray() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3));
    List<Integer> list = rdd.toArray();
    Assert.assertEquals(Arrays.asList(1, 2, 3), list);
  }

  @Test
  public void cartesian() {
    JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(Arrays.asList(1.0, 1.0, 2.0, 3.0, 5.0, 8.0));
    JavaRDD<String> stringRDD = sc.parallelize(Arrays.asList("Hello", "World"));
    JavaPairRDD<String, Double> cartesian = stringRDD.cartesian(doubleRDD);
    Assert.assertEquals(new Tuple2<>("Hello", 1.0), cartesian.first());
  }

  @Test
  public void javaDoubleRDD() {
    JavaDoubleRDD rdd = sc.parallelizeDoubles(Arrays.asList(1.0, 1.0, 2.0, 3.0, 5.0, 8.0));
    JavaDoubleRDD distinct = rdd.distinct();
    Assert.assertEquals(5, distinct.count());
    JavaDoubleRDD filter = rdd.filter(new Function<Double, Boolean>() {
      @Override
      public Boolean call(Double x) {
        return x > 2.0;
      }
    });
    Assert.assertEquals(3, filter.count());
    JavaDoubleRDD union = rdd.union(rdd);
    Assert.assertEquals(12, union.count());
    union = union.cache();
    Assert.assertEquals(12, union.count());

    Assert.assertEquals(20, rdd.sum(), 0.01);
    StatCounter stats = rdd.stats();
    Assert.assertEquals(20, stats.sum(), 0.01);
    Assert.assertEquals(20/6.0, rdd.mean(), 0.01);
    Assert.assertEquals(20/6.0, rdd.mean(), 0.01);
    Assert.assertEquals(6.22222, rdd.variance(), 0.01);
    Assert.assertEquals(7.46667, rdd.sampleVariance(), 0.01);
    Assert.assertEquals(2.49444, rdd.stdev(), 0.01);
    Assert.assertEquals(2.73252, rdd.sampleStdev(), 0.01);

    rdd.first();
    rdd.take(5);
  }

  @Test
  public void javaDoubleRDDHistoGram() {
    JavaDoubleRDD rdd = sc.parallelizeDoubles(Arrays.asList(1.0, 2.0, 3.0, 4.0));
    // Test using generated buckets
    Tuple2<double[], long[]> results = rdd.histogram(2);
    double[] expected_buckets = {1.0, 2.5, 4.0};
    long[] expected_counts = {2, 2};
    Assert.assertArrayEquals(expected_buckets, results._1(), 0.1);
    Assert.assertArrayEquals(expected_counts, results._2());
    // Test with provided buckets
    long[] histogram = rdd.histogram(expected_buckets);
    Assert.assertArrayEquals(expected_counts, histogram);
    // SPARK-5744
    Assert.assertArrayEquals(
        new long[] {0},
        sc.parallelizeDoubles(new ArrayList<Double>(0), 1).histogram(new double[]{0.0, 1.0}));
  }

  private static class DoubleComparator implements Comparator<Double>, Serializable {
    @Override
    public int compare(Double o1, Double o2) {
      return o1.compareTo(o2);
    }
  }

  @Test
  public void max() {
    JavaDoubleRDD rdd = sc.parallelizeDoubles(Arrays.asList(1.0, 2.0, 3.0, 4.0));
    double max = rdd.max(new DoubleComparator());
    Assert.assertEquals(4.0, max, 0.001);
  }

  @Test
  public void min() {
    JavaDoubleRDD rdd = sc.parallelizeDoubles(Arrays.asList(1.0, 2.0, 3.0, 4.0));
    double max = rdd.min(new DoubleComparator());
    Assert.assertEquals(1.0, max, 0.001);
  }

  @Test
  public void naturalMax() {
    JavaDoubleRDD rdd = sc.parallelizeDoubles(Arrays.asList(1.0, 2.0, 3.0, 4.0));
    double max = rdd.max();
    Assert.assertEquals(4.0, max, 0.0);
  }

  @Test
  public void naturalMin() {
    JavaDoubleRDD rdd = sc.parallelizeDoubles(Arrays.asList(1.0, 2.0, 3.0, 4.0));
    double max = rdd.min();
    Assert.assertEquals(1.0, max, 0.0);
  }

  @Test
  public void takeOrdered() {
    JavaDoubleRDD rdd = sc.parallelizeDoubles(Arrays.asList(1.0, 2.0, 3.0, 4.0));
    Assert.assertEquals(Arrays.asList(1.0, 2.0), rdd.takeOrdered(2, new DoubleComparator()));
    Assert.assertEquals(Arrays.asList(1.0, 2.0), rdd.takeOrdered(2));
  }

  @Test
  public void top() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
    List<Integer> top2 = rdd.top(2);
    Assert.assertEquals(Arrays.asList(4, 3), top2);
  }

  private static class AddInts implements Function2<Integer, Integer, Integer> {
    @Override
    public Integer call(Integer a, Integer b) {
      return a + b;
    }
  }

  @Test
  public void reduce() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
    int sum = rdd.reduce(new AddInts());
    Assert.assertEquals(10, sum);
  }

  @Test
  public void reduceOnJavaDoubleRDD() {
    JavaDoubleRDD rdd = sc.parallelizeDoubles(Arrays.asList(1.0, 2.0, 3.0, 4.0));
    double sum = rdd.reduce(new Function2<Double, Double, Double>() {
      @Override
      public Double call(Double v1, Double v2) {
        return v1 + v2;
      }
    });
    Assert.assertEquals(10.0, sum, 0.001);
  }

  @Test
  public void fold() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
    int sum = rdd.fold(0, new AddInts());
    Assert.assertEquals(10, sum);
  }

  @Test
  public void aggregate() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
    int sum = rdd.aggregate(0, new AddInts(), new AddInts());
    Assert.assertEquals(10, sum);
  }

  @Test
  public void map() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
    JavaDoubleRDD doubles = rdd.mapToDouble(new DoubleFunction<Integer>() {
      @Override
      public double call(Integer x) {
        return x.doubleValue();
      }
    }).cache();
    doubles.collect();
    JavaPairRDD<Integer, Integer> pairs = rdd.mapToPair(
        new PairFunction<Integer, Integer, Integer>() {
          @Override
          public Tuple2<Integer, Integer> call(Integer x) {
            return new Tuple2<>(x, x);
          }
        }).cache();
    pairs.collect();
    JavaRDD<String> strings = rdd.map(new Function<Integer, String>() {
      @Override
      public String call(Integer x) {
        return x.toString();
      }
    }).cache();
    strings.collect();
  }

  @Test
  public void flatMap() {
    JavaRDD<String> rdd = sc.parallelize(Arrays.asList("Hello World!",
      "The quick brown fox jumps over the lazy dog."));
    JavaRDD<String> words = rdd.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterable<String> call(String x) {
        return Arrays.asList(x.split(" "));
      }
    });
    Assert.assertEquals("Hello", words.first());
    Assert.assertEquals(11, words.count());

    JavaPairRDD<String, String> pairsRDD = rdd.flatMapToPair(
      new PairFlatMapFunction<String, String, String>() {
        @Override
        public Iterable<Tuple2<String, String>> call(String s) {
          List<Tuple2<String, String>> pairs = new LinkedList<>();
          for (String word : s.split(" ")) {
            pairs.add(new Tuple2<>(word, word));
          }
          return pairs;
        }
      }
    );
    Assert.assertEquals(new Tuple2<>("Hello", "Hello"), pairsRDD.first());
    Assert.assertEquals(11, pairsRDD.count());

    JavaDoubleRDD doubles = rdd.flatMapToDouble(new DoubleFlatMapFunction<String>() {
      @Override
      public Iterable<Double> call(String s) {
        List<Double> lengths = new LinkedList<>();
        for (String word : s.split(" ")) {
          lengths.add((double) word.length());
        }
        return lengths;
      }
    });
    Assert.assertEquals(5.0, doubles.first(), 0.01);
    Assert.assertEquals(11, pairsRDD.count());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void mapsFromPairsToPairs() {
    List<Tuple2<Integer, String>> pairs = Arrays.asList(
      new Tuple2<>(1, "a"),
      new Tuple2<>(2, "aa"),
      new Tuple2<>(3, "aaa")
    );
    JavaPairRDD<Integer, String> pairRDD = sc.parallelizePairs(pairs);

    // Regression test for SPARK-668:
    JavaPairRDD<String, Integer> swapped = pairRDD.flatMapToPair(
      new PairFlatMapFunction<Tuple2<Integer, String>, String, Integer>() {
        @Override
        public Iterable<Tuple2<String, Integer>> call(Tuple2<Integer, String> item) {
          return Collections.singletonList(item.swap());
        }
      });
    swapped.collect();

    // There was never a bug here, but it's worth testing:
    pairRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
      @Override
      public Tuple2<String, Integer> call(Tuple2<Integer, String> item) {
        return item.swap();
      }
    }).collect();
  }

  @Test
  public void mapPartitions() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4), 2);
    JavaRDD<Integer> partitionSums = rdd.mapPartitions(
      new FlatMapFunction<Iterator<Integer>, Integer>() {
        @Override
        public Iterable<Integer> call(Iterator<Integer> iter) {
          int sum = 0;
          while (iter.hasNext()) {
            sum += iter.next();
          }
          return Collections.singletonList(sum);
        }
    });
    Assert.assertEquals("[3, 7]", partitionSums.collect().toString());
  }


  @Test
  public void mapPartitionsWithIndex() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4), 2);
    JavaRDD<Integer> partitionSums = rdd.mapPartitionsWithIndex(
      new Function2<Integer, Iterator<Integer>, Iterator<Integer>>() {
        @Override
        public Iterator<Integer> call(Integer index, Iterator<Integer> iter) {
          int sum = 0;
          while (iter.hasNext()) {
            sum += iter.next();
          }
          return Collections.singletonList(sum).iterator();
        }
    }, false);
    Assert.assertEquals("[3, 7]", partitionSums.collect().toString());
  }

  @Test
  public void getNumPartitions(){
    JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8), 3);
    JavaDoubleRDD rdd2 = sc.parallelizeDoubles(Arrays.asList(1.0, 2.0, 3.0, 4.0), 2);
    JavaPairRDD<String, Integer> rdd3 = sc.parallelizePairs(Arrays.asList(
            new Tuple2<>("a", 1),
            new Tuple2<>("aa", 2),
            new Tuple2<>("aaa", 3)
    ), 2);
    Assert.assertEquals(3, rdd1.getNumPartitions());
    Assert.assertEquals(2, rdd2.getNumPartitions());
    Assert.assertEquals(2, rdd3.getNumPartitions());
  }

  @Test
  public void repartition() {
    // Shrinking number of partitions
    JavaRDD<Integer> in1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8), 2);
    JavaRDD<Integer> repartitioned1 = in1.repartition(4);
    List<List<Integer>> result1 = repartitioned1.glom().collect();
    Assert.assertEquals(4, result1.size());
    for (List<Integer> l : result1) {
      Assert.assertFalse(l.isEmpty());
    }

    // Growing number of partitions
    JavaRDD<Integer> in2 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8), 4);
    JavaRDD<Integer> repartitioned2 = in2.repartition(2);
    List<List<Integer>> result2 = repartitioned2.glom().collect();
    Assert.assertEquals(2, result2.size());
    for (List<Integer> l: result2) {
      Assert.assertFalse(l.isEmpty());
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void persist() {
    JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(Arrays.asList(1.0, 1.0, 2.0, 3.0, 5.0, 8.0));
    doubleRDD = doubleRDD.persist(StorageLevel.DISK_ONLY());
    Assert.assertEquals(20, doubleRDD.sum(), 0.1);

    List<Tuple2<Integer, String>> pairs = Arrays.asList(
      new Tuple2<>(1, "a"),
      new Tuple2<>(2, "aa"),
      new Tuple2<>(3, "aaa")
    );
    JavaPairRDD<Integer, String> pairRDD = sc.parallelizePairs(pairs);
    pairRDD = pairRDD.persist(StorageLevel.DISK_ONLY());
    Assert.assertEquals("a", pairRDD.first()._2());

    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
    rdd = rdd.persist(StorageLevel.DISK_ONLY());
    Assert.assertEquals(1, rdd.first().intValue());
  }

  @Test
  public void iterator() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 2);
    TaskContext context = TaskContext$.MODULE$.empty();
    Assert.assertEquals(1, rdd.iterator(rdd.partitions().get(0), context).next().intValue());
  }

  @Test
  public void glom() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4), 2);
    Assert.assertEquals("[1, 2]", rdd.glom().first().toString());
  }

  // File input / output tests are largely adapted from FileSuite:

  @Test
  public void textFiles() throws IOException {
    String outputDir = new File(tempDir, "output").getAbsolutePath();
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
    rdd.saveAsTextFile(outputDir);
    // Read the plain text file and check it's OK
    File outputFile = new File(outputDir, "part-00000");
    String content = Files.toString(outputFile, Charsets.UTF_8);
    Assert.assertEquals("1\n2\n3\n4\n", content);
    // Also try reading it in as a text file RDD
    List<String> expected = Arrays.asList("1", "2", "3", "4");
    JavaRDD<String> readRDD = sc.textFile(outputDir);
    Assert.assertEquals(expected, readRDD.collect());
  }

  @Test
  public void wholeTextFiles() throws Exception {
    byte[] content1 = "spark is easy to use.\n".getBytes("utf-8");
    byte[] content2 = "spark is also easy to use.\n".getBytes("utf-8");

    String tempDirName = tempDir.getAbsolutePath();
    Files.write(content1, new File(tempDirName + "/part-00000"));
    Files.write(content2, new File(tempDirName + "/part-00001"));

    Map<String, String> container = new HashMap<>();
    container.put(tempDirName+"/part-00000", new Text(content1).toString());
    container.put(tempDirName+"/part-00001", new Text(content2).toString());

    JavaPairRDD<String, String> readRDD = sc.wholeTextFiles(tempDirName, 3);
    List<Tuple2<String, String>> result = readRDD.collect();

    for (Tuple2<String, String> res : result) {
      Assert.assertEquals(res._2(), container.get(new URI(res._1()).getPath()));
    }
  }

  @Test
  public void textFilesCompressed() throws IOException {
    String outputDir = new File(tempDir, "output").getAbsolutePath();
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
    rdd.saveAsTextFile(outputDir, DefaultCodec.class);

    // Try reading it in as a text file RDD
    List<String> expected = Arrays.asList("1", "2", "3", "4");
    JavaRDD<String> readRDD = sc.textFile(outputDir);
    Assert.assertEquals(expected, readRDD.collect());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void sequenceFile() {
    String outputDir = new File(tempDir, "output").getAbsolutePath();
    List<Tuple2<Integer, String>> pairs = Arrays.asList(
      new Tuple2<>(1, "a"),
      new Tuple2<>(2, "aa"),
      new Tuple2<>(3, "aaa")
    );
    JavaPairRDD<Integer, String> rdd = sc.parallelizePairs(pairs);

    rdd.mapToPair(new PairFunction<Tuple2<Integer, String>, IntWritable, Text>() {
      @Override
      public Tuple2<IntWritable, Text> call(Tuple2<Integer, String> pair) {
        return new Tuple2<>(new IntWritable(pair._1()), new Text(pair._2()));
      }
    }).saveAsHadoopFile(outputDir, IntWritable.class, Text.class, SequenceFileOutputFormat.class);

    // Try reading the output back as an object file
    JavaPairRDD<Integer, String> readRDD = sc.sequenceFile(outputDir, IntWritable.class,
      Text.class).mapToPair(new PairFunction<Tuple2<IntWritable, Text>, Integer, String>() {
      @Override
      public Tuple2<Integer, String> call(Tuple2<IntWritable, Text> pair) {
        return new Tuple2<>(pair._1().get(), pair._2().toString());
      }
    });
    Assert.assertEquals(pairs, readRDD.collect());
  }

  @Test
  public void binaryFiles() throws Exception {
    // Reusing the wholeText files example
    byte[] content1 = "spark is easy to use.\n".getBytes("utf-8");

    String tempDirName = tempDir.getAbsolutePath();
    File file1 = new File(tempDirName + "/part-00000");

    FileOutputStream fos1 = new FileOutputStream(file1);

    FileChannel channel1 = fos1.getChannel();
    ByteBuffer bbuf = ByteBuffer.wrap(content1);
    channel1.write(bbuf);
    channel1.close();
    JavaPairRDD<String, PortableDataStream> readRDD = sc.binaryFiles(tempDirName, 3);
    List<Tuple2<String, PortableDataStream>> result = readRDD.collect();
    for (Tuple2<String, PortableDataStream> res : result) {
      Assert.assertArrayEquals(content1, res._2().toArray());
    }
  }

  @Test
  public void binaryFilesCaching() throws Exception {
    // Reusing the wholeText files example
    byte[] content1 = "spark is easy to use.\n".getBytes("utf-8");

    String tempDirName = tempDir.getAbsolutePath();
    File file1 = new File(tempDirName + "/part-00000");

    FileOutputStream fos1 = new FileOutputStream(file1);

    FileChannel channel1 = fos1.getChannel();
    ByteBuffer bbuf = ByteBuffer.wrap(content1);
    channel1.write(bbuf);
    channel1.close();

    JavaPairRDD<String, PortableDataStream> readRDD = sc.binaryFiles(tempDirName).cache();
    readRDD.foreach(new VoidFunction<Tuple2<String,PortableDataStream>>() {
      @Override
      public void call(Tuple2<String, PortableDataStream> pair) {
        pair._2().toArray(); // force the file to read
      }
    });

    List<Tuple2<String, PortableDataStream>> result = readRDD.collect();
    for (Tuple2<String, PortableDataStream> res : result) {
      Assert.assertArrayEquals(content1, res._2().toArray());
    }
  }

  @Test
  public void binaryRecords() throws Exception {
    // Reusing the wholeText files example
    byte[] content1 = "spark isn't always easy to use.\n".getBytes("utf-8");
    int numOfCopies = 10;
    String tempDirName = tempDir.getAbsolutePath();
    File file1 = new File(tempDirName + "/part-00000");

    FileOutputStream fos1 = new FileOutputStream(file1);

    FileChannel channel1 = fos1.getChannel();

    for (int i = 0; i < numOfCopies; i++) {
      ByteBuffer bbuf = ByteBuffer.wrap(content1);
      channel1.write(bbuf);
    }
    channel1.close();

    JavaRDD<byte[]> readRDD = sc.binaryRecords(tempDirName, content1.length);
    Assert.assertEquals(numOfCopies,readRDD.count());
    List<byte[]> result = readRDD.collect();
    for (byte[] res : result) {
      Assert.assertArrayEquals(content1, res);
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void writeWithNewAPIHadoopFile() {
    String outputDir = new File(tempDir, "output").getAbsolutePath();
    List<Tuple2<Integer, String>> pairs = Arrays.asList(
      new Tuple2<>(1, "a"),
      new Tuple2<>(2, "aa"),
      new Tuple2<>(3, "aaa")
    );
    JavaPairRDD<Integer, String> rdd = sc.parallelizePairs(pairs);

    rdd.mapToPair(new PairFunction<Tuple2<Integer, String>, IntWritable, Text>() {
      @Override
      public Tuple2<IntWritable, Text> call(Tuple2<Integer, String> pair) {
        return new Tuple2<>(new IntWritable(pair._1()), new Text(pair._2()));
      }
    }).saveAsNewAPIHadoopFile(
        outputDir, IntWritable.class, Text.class,
        org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class);

    JavaPairRDD<IntWritable, Text> output = sc.sequenceFile(outputDir, IntWritable.class, Text.class);
    Assert.assertEquals(pairs.toString(), output.map(new Function<Tuple2<IntWritable, Text>, String>() {
      @Override
      public String call(Tuple2<IntWritable, Text> x) {
        return x.toString();
      }
    }).collect().toString());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void readWithNewAPIHadoopFile() throws IOException {
    String outputDir = new File(tempDir, "output").getAbsolutePath();
    List<Tuple2<Integer, String>> pairs = Arrays.asList(
      new Tuple2<>(1, "a"),
      new Tuple2<>(2, "aa"),
      new Tuple2<>(3, "aaa")
    );
    JavaPairRDD<Integer, String> rdd = sc.parallelizePairs(pairs);

    rdd.mapToPair(new PairFunction<Tuple2<Integer, String>, IntWritable, Text>() {
      @Override
      public Tuple2<IntWritable, Text> call(Tuple2<Integer, String> pair) {
        return new Tuple2<>(new IntWritable(pair._1()), new Text(pair._2()));
      }
    }).saveAsHadoopFile(outputDir, IntWritable.class, Text.class, SequenceFileOutputFormat.class);

    JavaPairRDD<IntWritable, Text> output = sc.newAPIHadoopFile(outputDir,
        org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.class,
        IntWritable.class, Text.class, new Job().getConfiguration());
    Assert.assertEquals(pairs.toString(), output.map(new Function<Tuple2<IntWritable, Text>, String>() {
      @Override
      public String call(Tuple2<IntWritable, Text> x) {
        return x.toString();
      }
    }).collect().toString());
  }

  @Test
  public void objectFilesOfInts() {
    String outputDir = new File(tempDir, "output").getAbsolutePath();
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
    rdd.saveAsObjectFile(outputDir);
    // Try reading the output back as an object file
    List<Integer> expected = Arrays.asList(1, 2, 3, 4);
    JavaRDD<Integer> readRDD = sc.objectFile(outputDir);
    Assert.assertEquals(expected, readRDD.collect());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void objectFilesOfComplexTypes() {
    String outputDir = new File(tempDir, "output").getAbsolutePath();
    List<Tuple2<Integer, String>> pairs = Arrays.asList(
      new Tuple2<>(1, "a"),
      new Tuple2<>(2, "aa"),
      new Tuple2<>(3, "aaa")
    );
    JavaPairRDD<Integer, String> rdd = sc.parallelizePairs(pairs);
    rdd.saveAsObjectFile(outputDir);
    // Try reading the output back as an object file
    JavaRDD<Tuple2<Integer, String>> readRDD = sc.objectFile(outputDir);
    Assert.assertEquals(pairs, readRDD.collect());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void hadoopFile() {
    String outputDir = new File(tempDir, "output").getAbsolutePath();
    List<Tuple2<Integer, String>> pairs = Arrays.asList(
      new Tuple2<>(1, "a"),
      new Tuple2<>(2, "aa"),
      new Tuple2<>(3, "aaa")
    );
    JavaPairRDD<Integer, String> rdd = sc.parallelizePairs(pairs);

    rdd.mapToPair(new PairFunction<Tuple2<Integer, String>, IntWritable, Text>() {
      @Override
      public Tuple2<IntWritable, Text> call(Tuple2<Integer, String> pair) {
        return new Tuple2<>(new IntWritable(pair._1()), new Text(pair._2()));
      }
    }).saveAsHadoopFile(outputDir, IntWritable.class, Text.class, SequenceFileOutputFormat.class);

    JavaPairRDD<IntWritable, Text> output = sc.hadoopFile(outputDir,
        SequenceFileInputFormat.class, IntWritable.class, Text.class);
    Assert.assertEquals(pairs.toString(), output.map(new Function<Tuple2<IntWritable, Text>, String>() {
      @Override
      public String call(Tuple2<IntWritable, Text> x) {
        return x.toString();
      }
    }).collect().toString());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void hadoopFileCompressed() {
    String outputDir = new File(tempDir, "output_compressed").getAbsolutePath();
    List<Tuple2<Integer, String>> pairs = Arrays.asList(
      new Tuple2<>(1, "a"),
      new Tuple2<>(2, "aa"),
      new Tuple2<>(3, "aaa")
    );
    JavaPairRDD<Integer, String> rdd = sc.parallelizePairs(pairs);

    rdd.mapToPair(new PairFunction<Tuple2<Integer, String>, IntWritable, Text>() {
      @Override
      public Tuple2<IntWritable, Text> call(Tuple2<Integer, String> pair) {
        return new Tuple2<>(new IntWritable(pair._1()), new Text(pair._2()));
      }
    }).saveAsHadoopFile(outputDir, IntWritable.class, Text.class, SequenceFileOutputFormat.class,
        DefaultCodec.class);

    JavaPairRDD<IntWritable, Text> output = sc.hadoopFile(outputDir,
        SequenceFileInputFormat.class, IntWritable.class, Text.class);

    Assert.assertEquals(pairs.toString(), output.map(new Function<Tuple2<IntWritable, Text>, String>() {
      @Override
      public String call(Tuple2<IntWritable, Text> x) {
        return x.toString();
      }
    }).collect().toString());
  }

  @Test
  public void zip() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
    JavaDoubleRDD doubles = rdd.mapToDouble(new DoubleFunction<Integer>() {
      @Override
      public double call(Integer x) {
        return x.doubleValue();
      }
    });
    JavaPairRDD<Integer, Double> zipped = rdd.zip(doubles);
    zipped.count();
  }

  @Test
  public void zipPartitions() {
    JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6), 2);
    JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("1", "2", "3", "4"), 2);
    FlatMapFunction2<Iterator<Integer>, Iterator<String>, Integer> sizesFn =
      new FlatMapFunction2<Iterator<Integer>, Iterator<String>, Integer>() {
        @Override
        public Iterable<Integer> call(Iterator<Integer> i, Iterator<String> s) {
          return Arrays.asList(Iterators.size(i), Iterators.size(s));
        }
      };

    JavaRDD<Integer> sizes = rdd1.zipPartitions(rdd2, sizesFn);
    Assert.assertEquals("[3, 2, 3, 2]", sizes.collect().toString());
  }

  @Test
  public void accumulators() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

    final Accumulator<Integer> intAccum = sc.intAccumulator(10);
    rdd.foreach(new VoidFunction<Integer>() {
      @Override
      public void call(Integer x) {
        intAccum.add(x);
      }
    });
    Assert.assertEquals((Integer) 25, intAccum.value());

    final Accumulator<Double> doubleAccum = sc.doubleAccumulator(10.0);
    rdd.foreach(new VoidFunction<Integer>() {
      @Override
      public void call(Integer x) {
        doubleAccum.add((double) x);
      }
    });
    Assert.assertEquals((Double) 25.0, doubleAccum.value());

    // Try a custom accumulator type
    AccumulatorParam<Float> floatAccumulatorParam = new AccumulatorParam<Float>() {
      @Override
      public Float addInPlace(Float r, Float t) {
        return r + t;
      }

      @Override
      public Float addAccumulator(Float r, Float t) {
        return r + t;
      }

      @Override
      public Float zero(Float initialValue) {
        return 0.0f;
      }
    };

    final Accumulator<Float> floatAccum = sc.accumulator(10.0f, floatAccumulatorParam);
    rdd.foreach(new VoidFunction<Integer>() {
      @Override
      public void call(Integer x) {
        floatAccum.add((float) x);
      }
    });
    Assert.assertEquals((Float) 25.0f, floatAccum.value());

    // Test the setValue method
    floatAccum.setValue(5.0f);
    Assert.assertEquals((Float) 5.0f, floatAccum.value());
  }

  @Test
  public void keyBy() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2));
    List<Tuple2<String, Integer>> s = rdd.keyBy(new Function<Integer, String>() {
      @Override
      public String call(Integer t) {
        return t.toString();
      }
    }).collect();
    Assert.assertEquals(new Tuple2<>("1", 1), s.get(0));
    Assert.assertEquals(new Tuple2<>("2", 2), s.get(1));
  }

  @Test
  public void checkpointAndComputation() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
    sc.setCheckpointDir(tempDir.getAbsolutePath());
    Assert.assertFalse(rdd.isCheckpointed());
    rdd.checkpoint();
    rdd.count(); // Forces the DAG to cause a checkpoint
    Assert.assertTrue(rdd.isCheckpointed());
    Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5), rdd.collect());
  }

  @Test
  public void checkpointAndRestore() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
    sc.setCheckpointDir(tempDir.getAbsolutePath());
    Assert.assertFalse(rdd.isCheckpointed());
    rdd.checkpoint();
    rdd.count(); // Forces the DAG to cause a checkpoint
    Assert.assertTrue(rdd.isCheckpointed());

    Assert.assertTrue(rdd.getCheckpointFile().isPresent());
    JavaRDD<Integer> recovered = sc.checkpointFile(rdd.getCheckpointFile().get());
    Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5), recovered.collect());
  }

  @Test
  public void combineByKey() {
    JavaRDD<Integer> originalRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6));
    Function<Integer, Integer> keyFunction = new Function<Integer, Integer>() {
      @Override
      public Integer call(Integer v1) {
        return v1 % 3;
      }
    };
    Function<Integer, Integer> createCombinerFunction = new Function<Integer, Integer>() {
      @Override
      public Integer call(Integer v1) {
        return v1;
      }
    };

    Function2<Integer, Integer, Integer> mergeValueFunction = new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer v1, Integer v2) {
        return v1 + v2;
      }
    };

    JavaPairRDD<Integer, Integer> combinedRDD = originalRDD.keyBy(keyFunction)
        .combineByKey(createCombinerFunction, mergeValueFunction, mergeValueFunction);
    Map<Integer, Integer> results = combinedRDD.collectAsMap();
    ImmutableMap<Integer, Integer> expected = ImmutableMap.of(0, 9, 1, 5, 2, 7);
    Assert.assertEquals(expected, results);

    Partitioner defaultPartitioner = Partitioner.defaultPartitioner(
        combinedRDD.rdd(),
        JavaConverters.collectionAsScalaIterableConverter(
            Collections.<RDD<?>>emptyList()).asScala().toSeq());
    combinedRDD = originalRDD.keyBy(keyFunction)
        .combineByKey(
             createCombinerFunction,
             mergeValueFunction,
             mergeValueFunction,
             defaultPartitioner,
             false,
             new KryoSerializer(new SparkConf()));
    results = combinedRDD.collectAsMap();
    Assert.assertEquals(expected, results);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void mapOnPairRDD() {
    JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1,2,3,4));
    JavaPairRDD<Integer, Integer> rdd2 = rdd1.mapToPair(
        new PairFunction<Integer, Integer, Integer>() {
          @Override
          public Tuple2<Integer, Integer> call(Integer i) {
            return new Tuple2<>(i, i % 2);
          }
        });
    JavaPairRDD<Integer, Integer> rdd3 = rdd2.mapToPair(
        new PairFunction<Tuple2<Integer, Integer>, Integer, Integer>() {
          @Override
          public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> in) {
            return new Tuple2<>(in._2(), in._1());
          }
        });
    Assert.assertEquals(Arrays.asList(
        new Tuple2<>(1, 1),
        new Tuple2<>(0, 2),
        new Tuple2<>(1, 3),
        new Tuple2<>(0, 4)), rdd3.collect());

  }

  @SuppressWarnings("unchecked")
  @Test
  public void collectPartitions() {
    JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7), 3);

    JavaPairRDD<Integer, Integer> rdd2 = rdd1.mapToPair(
        new PairFunction<Integer, Integer, Integer>() {
          @Override
          public Tuple2<Integer, Integer> call(Integer i) {
            return new Tuple2<>(i, i % 2);
          }
        });

    List<Integer>[] parts = rdd1.collectPartitions(new int[] {0});
    Assert.assertEquals(Arrays.asList(1, 2), parts[0]);

    parts = rdd1.collectPartitions(new int[] {1, 2});
    Assert.assertEquals(Arrays.asList(3, 4), parts[0]);
    Assert.assertEquals(Arrays.asList(5, 6, 7), parts[1]);

    Assert.assertEquals(Arrays.asList(new Tuple2<>(1, 1),
                                      new Tuple2<>(2, 0)),
                        rdd2.collectPartitions(new int[] {0})[0]);

    List<Tuple2<Integer,Integer>>[] parts2 = rdd2.collectPartitions(new int[] {1, 2});
    Assert.assertEquals(Arrays.asList(new Tuple2<>(3, 1),
                                      new Tuple2<>(4, 0)),
                        parts2[0]);
    Assert.assertEquals(Arrays.asList(new Tuple2<>(5, 1),
                                      new Tuple2<>(6, 0),
                                      new Tuple2<>(7, 1)),
                        parts2[1]);
  }

  @Test
  public void countApproxDistinct() {
    List<Integer> arrayData = new ArrayList<>();
    int size = 100;
    for (int i = 0; i < 100000; i++) {
      arrayData.add(i % size);
    }
    JavaRDD<Integer> simpleRdd = sc.parallelize(arrayData, 10);
    Assert.assertTrue(Math.abs((simpleRdd.countApproxDistinct(0.05) - size) / (size * 1.0)) <= 0.1);
  }

  @Test
  public void countApproxDistinctByKey() {
    List<Tuple2<Integer, Integer>> arrayData = new ArrayList<>();
    for (int i = 10; i < 100; i++) {
      for (int j = 0; j < i; j++) {
        arrayData.add(new Tuple2<>(i, j));
      }
    }
    double relativeSD = 0.001;
    JavaPairRDD<Integer, Integer> pairRdd = sc.parallelizePairs(arrayData);
    List<Tuple2<Integer, Object>> res =  pairRdd.countApproxDistinctByKey(relativeSD, 8).collect();
    for (Tuple2<Integer, Object> resItem : res) {
      double count = (double)resItem._1();
      Long resCount = (Long)resItem._2();
      Double error = Math.abs((resCount - count) / count);
      Assert.assertTrue(error < 0.1);
    }

  }

  @Test
  public void collectAsMapWithIntArrayValues() {
    // Regression test for SPARK-1040
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1));
    JavaPairRDD<Integer, int[]> pairRDD = rdd.mapToPair(
        new PairFunction<Integer, Integer, int[]>() {
          @Override
          public Tuple2<Integer, int[]> call(Integer x) {
            return new Tuple2<>(x, new int[]{x});
          }
        });
    pairRDD.collect();  // Works fine
    pairRDD.collectAsMap();  // Used to crash with ClassCastException
  }

  @SuppressWarnings("unchecked")
  @Test
  public void collectAsMapAndSerialize() throws Exception {
    JavaPairRDD<String,Integer> rdd =
        sc.parallelizePairs(Arrays.asList(new Tuple2<>("foo", 1)));
    Map<String,Integer> map = rdd.collectAsMap();
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    new ObjectOutputStream(bytes).writeObject(map);
    Map<String,Integer> deserializedMap = (Map<String,Integer>)
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray())).readObject();
    Assert.assertEquals(1, deserializedMap.get("foo").intValue());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void sampleByKey() {
    JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8), 3);
    JavaPairRDD<Integer, Integer> rdd2 = rdd1.mapToPair(
      new PairFunction<Integer, Integer, Integer>() {
        @Override
        public Tuple2<Integer, Integer> call(Integer i) {
          return new Tuple2<>(i % 2, 1);
        }
      });
    Map<Integer, Object> fractions = Maps.newHashMap();
    fractions.put(0, 0.5);
    fractions.put(1, 1.0);
    JavaPairRDD<Integer, Integer> wr = rdd2.sampleByKey(true, fractions, 1L);
    Map<Integer, Long> wrCounts = (Map<Integer, Long>) (Object) wr.countByKey();
    Assert.assertEquals(2, wrCounts.size());
    Assert.assertTrue(wrCounts.get(0) > 0);
    Assert.assertTrue(wrCounts.get(1) > 0);
    JavaPairRDD<Integer, Integer> wor = rdd2.sampleByKey(false, fractions, 1L);
    Map<Integer, Long> worCounts = (Map<Integer, Long>) (Object) wor.countByKey();
    Assert.assertEquals(2, worCounts.size());
    Assert.assertTrue(worCounts.get(0) > 0);
    Assert.assertTrue(worCounts.get(1) > 0);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void sampleByKeyExact() {
    JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8), 3);
    JavaPairRDD<Integer, Integer> rdd2 = rdd1.mapToPair(
      new PairFunction<Integer, Integer, Integer>() {
          @Override
          public Tuple2<Integer, Integer> call(Integer i) {
              return new Tuple2<>(i % 2, 1);
          }
      });
    Map<Integer, Object> fractions = Maps.newHashMap();
    fractions.put(0, 0.5);
    fractions.put(1, 1.0);
    JavaPairRDD<Integer, Integer> wrExact = rdd2.sampleByKeyExact(true, fractions, 1L);
    Map<Integer, Long> wrExactCounts = (Map<Integer, Long>) (Object) wrExact.countByKey();
    Assert.assertEquals(2, wrExactCounts.size());
    Assert.assertTrue(wrExactCounts.get(0) == 2);
    Assert.assertTrue(wrExactCounts.get(1) == 4);
    JavaPairRDD<Integer, Integer> worExact = rdd2.sampleByKeyExact(false, fractions, 1L);
    Map<Integer, Long> worExactCounts = (Map<Integer, Long>) (Object) worExact.countByKey();
    Assert.assertEquals(2, worExactCounts.size());
    Assert.assertTrue(worExactCounts.get(0) == 2);
    Assert.assertTrue(worExactCounts.get(1) == 4);
  }

  private static class SomeCustomClass implements Serializable {
    SomeCustomClass() {
      // Intentionally left blank
    }
  }

  @Test
  public void collectUnderlyingScalaRDD() {
    List<SomeCustomClass> data = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      data.add(new SomeCustomClass());
    }
    JavaRDD<SomeCustomClass> rdd = sc.parallelize(data);
    SomeCustomClass[] collected = (SomeCustomClass[]) rdd.rdd().retag(SomeCustomClass.class).collect();
    Assert.assertEquals(data.size(), collected.length);
  }

  private static final class BuggyMapFunction<T> implements Function<T, T> {

    @Override
    public T call(T x) {
      throw new IllegalStateException("Custom exception!");
    }
  }

  @Test
  public void collectAsync() throws Exception {
    List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
    JavaRDD<Integer> rdd = sc.parallelize(data, 1);
    JavaFutureAction<List<Integer>> future = rdd.collectAsync();
    List<Integer> result = future.get();
    Assert.assertEquals(data, result);
    Assert.assertFalse(future.isCancelled());
    Assert.assertTrue(future.isDone());
    Assert.assertEquals(1, future.jobIds().size());
  }

  @Test
  public void takeAsync() throws Exception {
    List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
    JavaRDD<Integer> rdd = sc.parallelize(data, 1);
    JavaFutureAction<List<Integer>> future = rdd.takeAsync(1);
    List<Integer> result = future.get();
    Assert.assertEquals(1, result.size());
    Assert.assertEquals((Integer) 1, result.get(0));
    Assert.assertFalse(future.isCancelled());
    Assert.assertTrue(future.isDone());
    Assert.assertEquals(1, future.jobIds().size());
  }

  @Test
  public void foreachAsync() throws Exception {
    List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
    JavaRDD<Integer> rdd = sc.parallelize(data, 1);
    JavaFutureAction<Void> future = rdd.foreachAsync(
        new VoidFunction<Integer>() {
          @Override
          public void call(Integer integer) {
            // intentionally left blank.
          }
        }
    );
    future.get();
    Assert.assertFalse(future.isCancelled());
    Assert.assertTrue(future.isDone());
    Assert.assertEquals(1, future.jobIds().size());
  }

  @Test
  public void countAsync() throws Exception {
    List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
    JavaRDD<Integer> rdd = sc.parallelize(data, 1);
    JavaFutureAction<Long> future = rdd.countAsync();
    long count = future.get();
    Assert.assertEquals(data.size(), count);
    Assert.assertFalse(future.isCancelled());
    Assert.assertTrue(future.isDone());
    Assert.assertEquals(1, future.jobIds().size());
  }

  @Test
  public void testAsyncActionCancellation() throws Exception {
    List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
    JavaRDD<Integer> rdd = sc.parallelize(data, 1);
    JavaFutureAction<Void> future = rdd.foreachAsync(new VoidFunction<Integer>() {
      @Override
      public void call(Integer integer) throws InterruptedException {
        Thread.sleep(10000);  // To ensure that the job won't finish before it's cancelled.
      }
    });
    future.cancel(true);
    Assert.assertTrue(future.isCancelled());
    Assert.assertTrue(future.isDone());
    try {
      future.get(2000, TimeUnit.MILLISECONDS);
      Assert.fail("Expected future.get() for cancelled job to throw CancellationException");
    } catch (CancellationException ignored) {
      // pass
    }
  }

  @Test
  public void testAsyncActionErrorWrapping() throws Exception {
    List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
    JavaRDD<Integer> rdd = sc.parallelize(data, 1);
    JavaFutureAction<Long> future = rdd.map(new BuggyMapFunction<Integer>()).countAsync();
    try {
      future.get(2, TimeUnit.SECONDS);
      Assert.fail("Expected future.get() for failed job to throw ExcecutionException");
    } catch (ExecutionException ee) {
      Assert.assertTrue(Throwables.getStackTraceAsString(ee).contains("Custom exception!"));
    }
    Assert.assertTrue(future.isDone());
  }


  /**
   * Test for SPARK-3647. This test needs to use the maven-built assembly to trigger the issue,
   * since that's the only artifact where Guava classes have been relocated.
   */
  @Test
  public void testGuavaOptional() {
    // Stop the context created in setUp() and start a local-cluster one, to force usage of the
    // assembly.
    sc.stop();
    JavaSparkContext localCluster = new JavaSparkContext("local-cluster[1,1,1024]", "JavaAPISuite");
    try {
      JavaRDD<Integer> rdd1 = localCluster.parallelize(Arrays.asList(1, 2, null), 3);
      JavaRDD<Optional<Integer>> rdd2 = rdd1.map(
        new Function<Integer, Optional<Integer>>() {
          @Override
          public Optional<Integer> call(Integer i) {
            return Optional.fromNullable(i);
          }
        });
      rdd2.collect();
    } finally {
      localCluster.stop();
    }
  }

  static class Class1 {}
  static class Class2 {}

  @Test
  public void testRegisterKryoClasses() {
    SparkConf conf = new SparkConf();
    conf.registerKryoClasses(new Class<?>[]{ Class1.class, Class2.class });
    Assert.assertEquals(
        Class1.class.getName() + "," + Class2.class.getName(),
        conf.get("spark.kryo.classesToRegister"));
  }

}

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

package org.apache.spark.partial

import java.util.{HashMap => JHashMap}

import scala.collection.JavaConverters._
import scala.collection.Map
import scala.collection.mutable.HashMap

import org.apache.spark.util.StatCounter

/**
 * An ApproximateEvaluator for sums by key. Returns a map of key to confidence interval.
 */
private[spark] class GroupedSumEvaluator[T](totalOutputs: Int, confidence: Double)
  extends ApproximateEvaluator[JHashMap[T, StatCounter], Map[T, BoundedDouble]] {

  var outputsMerged = 0
  var sums = new JHashMap[T, StatCounter]   // Sum of counts for each key

  override def merge(outputId: Int, taskResult: JHashMap[T, StatCounter]) {
    outputsMerged += 1
    val iter = taskResult.entrySet.iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val old = sums.get(entry.getKey)
      if (old != null) {
        old.merge(entry.getValue)
      } else {
        sums.put(entry.getKey, entry.getValue)
      }
    }
  }

  override def currentResult(): Map[T, BoundedDouble] = {
    if (outputsMerged == totalOutputs) {
      val result = new JHashMap[T, BoundedDouble](sums.size)
      val iter = sums.entrySet.iterator()
      while (iter.hasNext) {
        val entry = iter.next()
        val sum = entry.getValue.sum
        result.put(entry.getKey, new BoundedDouble(sum, 1.0, sum, sum))
      }
      result.asScala
    } else if (outputsMerged == 0) {
      new HashMap[T, BoundedDouble]
    } else {
      val p = outputsMerged.toDouble / totalOutputs
      val studentTCacher = new StudentTCacher(confidence)
      val result = new JHashMap[T, BoundedDouble](sums.size)
      val iter = sums.entrySet.iterator()
      while (iter.hasNext) {
        val entry = iter.next()
        val counter = entry.getValue
        val meanEstimate = counter.mean
        val meanVar = counter.sampleVariance / counter.count
        val countEstimate = (counter.count + 1 - p) / p
        val countVar = (counter.count + 1) * (1 - p) / (p * p)
        val sumEstimate = meanEstimate * countEstimate
        val sumVar = (meanEstimate * meanEstimate * countVar) +
                     (countEstimate * countEstimate * meanVar) +
                     (meanVar * countVar)
        val sumStdev = math.sqrt(sumVar)
        val confFactor = studentTCacher.get(counter.count)
        val low = sumEstimate - confFactor * sumStdev
        val high = sumEstimate + confFactor * sumStdev
        result.put(entry.getKey, new BoundedDouble(sumEstimate, confidence, low, high))
      }
      result.asScala
    }
  }
}

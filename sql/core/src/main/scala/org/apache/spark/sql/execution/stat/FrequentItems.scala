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

package org.apache.spark.sql.execution.stat

import scala.collection.mutable.{Map => MutableMap}

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, Column, DataFrame}

private[sql] object FrequentItems extends Logging {

  /** A helper class wrapping `MutableMap[Any, Long]` for simplicity. */
  private class FreqItemCounter(size: Int) extends Serializable {
    val baseMap: MutableMap[Any, Long] = MutableMap.empty[Any, Long]
    /**
     * Add a new example to the counts if it exists, otherwise deduct the count
     * from existing items.
     */
    def add(key: Any, count: Long): this.type = {
      if (baseMap.contains(key))  {
        baseMap(key) += count
      } else {
        if (baseMap.size < size) {
          baseMap += key -> count
        } else {
          val minCount = baseMap.values.min
          val remainder = count - minCount
          if (remainder >= 0) {
            baseMap += key -> count // something will get kicked out, so we can add this
            baseMap.retain((k, v) => v > minCount)
            baseMap.transform((k, v) => v - minCount)
          } else {
            baseMap.transform((k, v) => v - count)
          }
        }
      }
      this
    }

    /**
     * Merge two maps of counts.
     * @param other The map containing the counts for that partition
     */
    def merge(other: FreqItemCounter): this.type = {
      other.baseMap.foreach { case (k, v) =>
        add(k, v)
      }
      this
    }
  }

  /**
   * Finding frequent items for columns, possibly with false positives. Using the
   * frequent element count algorithm described in
   * [[http://dx.doi.org/10.1145/762471.762473, proposed by Karp, Schenker, and Papadimitriou]].
   * The `support` should be greater than 1e-4.
   * For Internal use only.
   *
   * @param df The input DataFrame
   * @param cols the names of the columns to search frequent items in
   * @param support The minimum frequency for an item to be considered `frequent`. Should be greater
   *                than 1e-4.
   * @return A Local DataFrame with the Array of frequent items for each column.
   */
  private[sql] def singlePassFreqItems(
      df: DataFrame,
      cols: Seq[String],
      support: Double): DataFrame = {
    require(support >= 1e-4, s"support ($support) must be greater than 1e-4.")
    val numCols = cols.length
    // number of max items to keep counts for
    val sizeOfMap = (1 / support).toInt
    val countMaps = Seq.tabulate(numCols)(i => new FreqItemCounter(sizeOfMap))
    val originalSchema = df.schema
    val colInfo: Array[(String, DataType)] = cols.map { name =>
      val index = originalSchema.fieldIndex(name)
      (name, originalSchema.fields(index).dataType)
    }.toArray

    val freqItems = df.select(cols.map(Column(_)) : _*).rdd.aggregate(countMaps)(
      seqOp = (counts, row) => {
        var i = 0
        while (i < numCols) {
          val thisMap = counts(i)
          val key = row.get(i)
          thisMap.add(key, 1L)
          i += 1
        }
        counts
      },
      combOp = (baseCounts, counts) => {
        var i = 0
        while (i < numCols) {
          baseCounts(i).merge(counts(i))
          i += 1
        }
        baseCounts
      }
    )
    val justItems = freqItems.map(m => m.baseMap.keys.toArray)
    val resultRow = Row(justItems : _*)
    // append frequent Items to the column name for easy debugging
    val outputCols = colInfo.map { v =>
      StructField(v._1 + "_freqItems", ArrayType(v._2, false))
    }
    val schema = StructType(outputCols).toAttributes
    new DataFrame(df.sqlContext, LocalRelation.fromExternalRows(schema, Seq(resultRow)))
  }
}

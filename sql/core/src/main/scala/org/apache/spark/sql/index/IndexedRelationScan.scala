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

package org.apache.spark.sql.index

import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.NumberConverter
import org.apache.spark.sql.execution.LeafNode
import org.apache.spark.sql.spatial._
import org.apache.spark.sql.SimilarityProbe.{EdSimSegmentation, JaccardSimSegmentation}
import org.apache.spark.sql.topksearch.{EditTopkIndexRelationScan, EditTopkIndexedRelation, JaccardTopkIndexRelationScan, JaccardTopkIndexedRelation}

import scala.collection.mutable

/**
  * Created by dong on 1/20/16.
  * Physical Scan on Indexed Relation
  */
private[sql] class Interval(var min: (Double, Boolean),
                            var max: (Double, Boolean)) extends Serializable {
  def this(min_val: Double = 0.0, max_val: Double = 0.0,
           left_closed: Boolean = true, right_closed: Boolean = true) {
    this((min_val, left_closed), (max_val, right_closed))
  }

  def isNull: Boolean = min._1 > max._1 || (min._1 == max._1 && !(min._2 && max._2))

  def intersect(other: Interval): Interval = {
    val ans = new Interval()
    if (!other.isNull) {
      if (other.min._1 > max._1 || other.max._1 < min._1) {
        ans.max = (ans.min._1 - 1, true)
      } else {
        ans.min = if (min._1 < other.min._1) other.min else min
        ans.max = if (max._1 > other.max._1) other.max else max
      }
    } else ans.max = (ans.min._1 - 1, true)
    ans
  }

  override def toString: String =
    (if (min._2) "[" else "(") + min._1 + ", " + max._1 + (if (max._2) "]" else ")")
}

private[sql] case class IndexedRelationScan(
                                             attributes: Seq[Attribute],
                                             predicates: Seq[Expression],
                                             relation: IndexedRelation)
  extends LeafNode with PredicateHelper {
  def getLeafInterval(x: Expression): (Interval, Attribute) = {
    x match {
      case EqualTo(left: NamedExpression, right: Literal) =>
        val tmp = NumberConverter.literalToDouble(right)
        (new Interval(tmp, tmp), left.toAttribute)
      case LessThan(left: NamedExpression, right: Literal) =>
        (new Interval(Double.MinValue, NumberConverter.literalToDouble(right), false, false),
          left.toAttribute)
      case LessThanOrEqual(left: NamedExpression, right: Literal) =>
        (new Interval(Double.MinValue, NumberConverter.literalToDouble(right), false, true),
          left.toAttribute)
      case GreaterThan(left: NamedExpression, right: Literal) =>
        (new Interval(NumberConverter.literalToDouble(right), Double.MaxValue, false, false),
          left.toAttribute)
      case GreaterThanOrEqual(left: NamedExpression, right: Literal) =>
        (new Interval(NumberConverter.literalToDouble(right), Double.MaxValue, true, false),
          left.toAttribute)
      case _ =>
        null
    }
  }

  def conditionToInterval(condition: Expression, column: List[Attribute])
  : (Array[Interval], Array[Expression]) = {
    val leaf_nodes = splitConjunctivePredicates(condition)
    val intervals: Array[Interval] = new Array[Interval](column.length)
    for (i <- column.indices)
      intervals(i) = new Interval(Double.MinValue, Double.MaxValue, false, false)
    var ans = mutable.ArrayBuffer[Expression]()
    leaf_nodes.foreach {now =>
      val tmp_interval = getLeafInterval(now)
      if (tmp_interval != null) {
        for (i <- column.indices)
          if (column.indexOf(tmp_interval._2) == i) {
            intervals(i) = intervals(i).intersect(tmp_interval._1)
          }
      } else {
        now match {
          case InRange(point: Seq[NamedExpression], point_low, point_high) =>
            for (i <- point.indices) {
              val id = column.indexOf(point(i).toAttribute)
              val low = point_low(i).asInstanceOf[Literal].toString.toDouble
              val high = point_high(i).asInstanceOf[Literal].toString.toDouble
              intervals(id) = intervals(id).intersect(new Interval(low, high))
            }
          case knn @ InKNN(point: Seq[NamedExpression], target: Seq[Expression], k: Literal) =>
            ans += knn
          case cr @ InCircleRange(point: Seq[NamedExpression], target, r: Literal) =>
            ans += cr
        }
      }
    }
    (intervals, ans.toArray)
  }

  class DisOrdering(origin: Point, column_keys: List[Attribute]) extends Ordering[InternalRow] {
    def compare(a: InternalRow, b: InternalRow): Int = {
      var dis_a = 0.0
      for (i <- column_keys.indices) {
        val tmp = BindReferences.bindReference(column_keys(i), relation.output).eval(a)
          .asInstanceOf[Number].doubleValue()
        dis_a += (tmp - origin.coord(i)) * (tmp - origin.coord(i))
      }
      var dis_b = 0.0
      for (i <- column_keys.indices) {
        val tmp = BindReferences.bindReference(column_keys(i), relation.output).eval(a)
          .asInstanceOf[Number].doubleValue()
        dis_b += (tmp - origin.coord(i)) * (tmp - origin.coord(i))
      }
      dis_a.compare(dis_b)
    }
  }

  def evalDist(row: InternalRow, origin: Point, column_keys: List[Attribute]): Double = {
    var dis = 0.0
    for (i <- column_keys.indices) {
      val tmp = BindReferences.bindReference(column_keys(i), relation.output).eval(row)
        .asInstanceOf[Number].doubleValue()
      dis += (tmp - origin.coord(i)) * (tmp - origin.coord(i))
    }
    Math.sqrt(dis)
  }

  override protected def doExecute(): RDD[InternalRow] = {
    relation match {
      case treemap @ TreeMapIndexedRelation(_, _, _, column_keys, _) =>
        if (predicates.nonEmpty) {
          val intervals = predicates.map(conditionToInterval(_, column_keys)._1).head
          val bounds = treemap.range_bounds
          val query_sets = new mutable.HashSet[Int]()
          intervals.foreach {interval =>
            if (interval != null && !interval.isNull) {
              val start = bounds.indexWhere(x => x >= interval.min._1)
              var end = bounds.indexWhere(x => x >= interval.max._1)
              if (end == -1) end = bounds.length
              if (start >= 0) {
                for (i <- start to end + 1)
                  query_sets.add(i)
              } else query_sets.add(bounds.length)
            }
          }
          val pruned = new PartitionPruningRDD(treemap._indexedRDD, query_sets.contains)
          pruned.flatMap {packed => {
            val index = packed.index.asInstanceOf[TreeMapIndex[Double]].index
            var tmp_res = mutable.ArrayBuffer[Int]()
            intervals.foreach {interval =>
              if (interval != null && !interval.isNull) {
                val tmp = index.subMap(interval.min._1, interval.max._1).values()
                  .toArray.map(_.asInstanceOf[Int])
                tmp_res ++= tmp
                if (interval.max._2) tmp_res += index.get(interval.max._1)
              }
            }
            tmp_res.distinct.map(t => packed.data(t))
          }}
        } else {
          treemap._indexedRDD.flatMap(_.data)
        }
      case rtree @ RTreeIndexedRelation(_, _, _, column_keys, _) =>
        if (predicates.nonEmpty) {
          predicates.map { predicate =>
            val (intervals, exps) = conditionToInterval(predicate, column_keys)
            val minPoint = intervals.map(_.min._1)
            val maxPoint = intervals.map(_.max._1)
            val queryMBR = new MBR(new Point(minPoint), new Point(maxPoint))
            var cir_ranges = Array[(Point, Double)]()
            var knn_res: Array[InternalRow] = null

            exps.foreach {
              case InKNN(point: Seq[NamedExpression], target: Seq[Literal], l: Literal) =>
                val query_point = new Point(target.map(NumberConverter.literalToDouble).toArray)
                val k = l.value.asInstanceOf[Number].intValue()
                val mbr_ans = rtree.global_rtree.kNN(query_point, (a: Point, b: MBR) => {
                  require(a.coord.length == b.low.coord.length)
                  var ans = 0.0
                  for (i <- a.coord.indices) {
                    ans += Math.max((a.coord(i) - b.low.coord(i)) * (a.coord(i) - b.low.coord(i)),
                      (a.coord(i) - b.high.coord(i)) * (a.coord(i) - b.high.coord(i)))
                  }
                  Math.sqrt(ans)
                }, k, keepSame = false)
                val ord = new DisOrdering(query_point, column_keys)
                val tmp_set = new mutable.HashSet[Int]()
                tmp_set ++= mbr_ans.map(_._2)
                val tmp_pruned = new PartitionPruningRDD(rtree._indexedRDD, tmp_set.contains)
                val tmp_ans = tmp_pruned.flatMap { packed =>
                  var tmp_ans = Array[(Shape, Int)]()
                  if (packed.index.asInstanceOf[RTree] != null) {
                    tmp_ans = packed.index.asInstanceOf[RTree].kNN(query_point, k, keepSame = false)
                  }
                  tmp_ans.map(x => packed.data(x._2))
                }.takeOrdered(k)(ord)
                val theta = evalDist(tmp_ans.last, query_point, column_keys)

                val set = new mutable.HashSet[Int]()
                set ++= rtree.global_rtree.circleRange(query_point, theta).map(_._2)
                set --= tmp_set
                val tmp_knn_res = if (set.isEmpty) tmp_ans
                else {
                  val pruned = new PartitionPruningRDD(rtree._indexedRDD, set.contains)
                  pruned.flatMap { packed =>
                    var tmp_ans = Array[(Shape, Int)]()
                    if (packed.index.asInstanceOf[RTree] != null) {
                      tmp_ans = packed.index.asInstanceOf[RTree]
                        .kNN(query_point, k, keepSame = false)
                    }
                    tmp_ans.map(x => packed.data(x._2))
                  }.takeOrdered(k)(ord).union(tmp_ans).sorted(ord).take(k)
                }

                if (knn_res == null) knn_res = tmp_knn_res
                else knn_res = knn_res.intersect(tmp_knn_res)
              case InCircleRange(point: Seq[NamedExpression], target: Seq[Literal], l: Literal) =>
                val query_point = new Point(target.map(NumberConverter.literalToDouble).toArray)
                val r = NumberConverter.literalToDouble(l)
                cir_ranges = cir_ranges :+ (query_point, r)
            }

            if (knn_res == null || knn_res.length > sqlContext.conf.indexSizeThreshold) {
              val hash_set = new mutable.HashSet[Int]()
              hash_set ++= rtree.global_rtree.range(queryMBR).map(_._2)
              hash_set ++= rtree.global_rtree.circleRangeConj(cir_ranges).map(_._2)
              val pruned = new PartitionPruningRDD(rtree._indexedRDD, hash_set.contains)

              val tmp_rdd = pruned.flatMap {packed =>
                val index = packed.index.asInstanceOf[RTree]
                if (index != null) {
                  val root_mbr = index.root.m_mbr
                  val perfect_cover = queryMBR.contains(root_mbr.low) &&
                    queryMBR.contains(root_mbr.high) &&
                    cir_ranges.forall(x => Dist.furthest(x._1, root_mbr) <= x._2)

                  if (!perfect_cover) {
                    index.range(queryMBR).map(x => packed.data(x._2))
                      .intersect(index.circleRangeConj(cir_ranges).map(x => packed.data(x._2)))
                  } else packed.data
                } else Array[InternalRow]()
              }

              if (knn_res != null) sparkContext.parallelize(knn_res, 1).intersection(tmp_rdd)
              else tmp_rdd
            } else {
              val final_res = knn_res.filter {row =>
                val tmp_point = new Point(
                  column_keys.map(x => BindReferences.bindReference(x, relation.output)
                    .eval(row).asInstanceOf[Number].doubleValue()).toArray
                )
                val contain = cir_ranges.forall(x => tmp_point.minDist(x._1) <= x._2)
                contain && queryMBR.contains(tmp_point)
              }
              sparkContext.parallelize(final_res, 1)
            }
          }.reduce((a, b) => a.union(b)).map(_.copy()).distinct()
        } else rtree._indexedRDD.flatMap(_.data)
      case jaccardSimilarity @ JaccardIndexIndexedRelation(_, _, _, column_keys, _) =>
        if (predicates.nonEmpty) {
          predicates.map { predicate =>
            val exps = splitConjunctivePredicates(predicate)
            var jaccardSimilarity1 = Array[(String, Double)]()
            exps.foreach {
              case JaccardSimilarity(string: Expression, target: Literal, delta: Literal) =>
                val eval_target = target.value.toString
                val eval_delta =
                  delta.value.asInstanceOf[org.apache.spark.sql.types.Decimal].toDouble
                jaccardSimilarity1 = jaccardSimilarity1 :+(eval_target, eval_delta)
            }
            assert(jaccardSimilarity1.length == 1)
            val target = jaccardSimilarity1.head._1
            val delta = jaccardSimilarity1.head._2.toDouble
            val global = jaccardSimilarity.indexGlobalInfo
//            println(s"globalInfo in IndexedRelationScan is $global")
            val partitionedQuery = JaccardSimSegmentation.partition_r(
              JaccardSimSegmentation.sortByValue(target),
              global.frequencyTable.value,
              global.minimum,
              global.multiGroup.value,
              global.threshold_base,
              delta,
              global.alpha,
              global.partitionNum
            )
            jaccardSimilarity.indexRDD.mapPartitionsWithIndex(
              (partitionId, partitionData) => {
                val packed = partitionData.next()
                val index = packed.index.asInstanceOf[JaccardIndex]
                if (index != null) {
                  val sendOut = partitionedQuery.map(x => (x._1, x._2
                    .filter(x => JaccardSimSegmentation.sendLocation(x._1,
                      global.partitionNum) == partitionId))).filter(x => x._2.length != 0)
                  if (sendOut.length == 0) {
                    Array[InternalRow]().iterator
                  } else {
                    val selectivity = index.sampleSelectivity(packed.data,
                      sendOut, delta.toDouble, 0.001)
                    if (selectivity > 0.8) {
                      index.sequentialScan(packed.data,
                        sendOut,
                        delta.toDouble).iterator
                    } else {
                      index.findIndex(packed.data,
                        sendOut,
                        delta.toDouble)
                        .iterator
                    }
                  }
                } else Array[InternalRow]().iterator
              })
          }.reduce((a, b) => a.union(b)).map(_.copy()).distinct()
        } else jaccardSimilarity.indexRDD.flatMap(_.data.map(x => (x._1._2)))
      case edSimilarity @ EdIndexIndexedRelation(_, _, _, column_keys, _) =>
        if (predicates.nonEmpty) {
          predicates.map { predicate =>
            val exps = splitConjunctivePredicates(predicate)
            var edSimilarity1 = Array[(String, Int)]()
            exps.foreach {
              case EdSimilarity(string: Expression, target: Literal, delta: Literal) =>
                val eval_target = target.value.toString
                val eval_delta =
                  delta.value.asInstanceOf[java.lang.Integer].toInt
                edSimilarity1 = edSimilarity1 :+(eval_target, eval_delta)
            }
            assert(edSimilarity1.length == 1)
            val target = edSimilarity1.head._1
            val delta = edSimilarity1.head._2.toInt
            val global = edSimilarity.indexGlobalInfo
            //            println(s"globalInfo in IndexedRelationScan is $global")
            val partitionedQuery = EdSimSegmentation.parts(
              target,
              global.frequencyTable.value,
              global.L.value,
              global.P.value,
              global.threshold,
              delta,
              global.partitionNum,
              global.topDegree,
              global.weight,
              global.max
            )
            edSimilarity.indexRDD.mapPartitionsWithIndex(
              (partitionId, partitionData) => {
                val packed = partitionData.next()
                val index = packed.index.asInstanceOf[EdIndex]
                if (index != null) {
                  val sendOut = partitionedQuery
                    .filter(
                      x => EdSimSegmentation.sendLocation(x._1, global.partitionNum) == partitionId)
                  if (sendOut.length == 0) {
                    Array[InternalRow]().iterator
                  } else {
                    index.findIndex(packed.data,
                      sendOut,
                      delta.toInt)
                      .iterator
                  }
                } else Array[InternalRow]().iterator
              })
          }.reduce((a, b) => a.union(b)).map(_.copy()).distinct()
        } else edSimilarity.indexRDD.flatMap(_.data.map(x => x.content))
      case jaccardTopk @ JaccardTopkIndexedRelation(_, _, _, column_keys, _) =>
        if (predicates.nonEmpty) {
          predicates.map { predicate =>
            val exps = splitConjunctivePredicates(predicate)
            var jaccardTopk1 = Array[(String, Int)]()
            exps.foreach {
              case JaccardSimRank(string: Expression, target: Literal, delta: Literal) =>
                val eval_target = target.value.toString
                val eval_delta =
                  delta.value.asInstanceOf[java.lang.Integer].toInt
                jaccardTopk1 = jaccardTopk1 :+ (eval_target, eval_delta)
            }
            assert(jaccardTopk1.length == 1)
            val target = jaccardTopk1.head._1
            val delta = jaccardTopk1.head._2
            JaccardTopkIndexRelationScan
              .getTopk(this.sparkContext, jaccardTopk.indexRDD, target, delta)
          }.reduce((a, b) => a.union(b)).map(_.copy()).distinct()
        } else jaccardTopk.indexRDD(0).flatMap(_.data.map(_._2))
      case editTopk @ EditTopkIndexedRelation(_, _, _, column_keys, _) =>
        if (predicates.nonEmpty) {
          predicates.map { predicate =>
            val exps = splitConjunctivePredicates(predicate)
            var editTopk1 = Array[(String, Int)]()
            exps.foreach {
              case EditSimRank(string: Expression, target: Literal, delta: Literal) =>
                val eval_target = target.value.toString
                val eval_delta =
                  delta.value.asInstanceOf[java.lang.Integer].toInt
                editTopk1 = editTopk1 :+ (eval_target, eval_delta)
            }
            assert(editTopk1.length == 1)
            val target = editTopk1.head._1
            val delta = editTopk1.head._2
            EditTopkIndexRelationScan
              .getTopk(this.sparkContext, editTopk.indexRDD, target, delta)
          }.reduce((a, b) => a.union(b)).map(_.copy()).distinct()
        } else editTopk.indexRDD(0).flatMap(_.data.map(_._2))
      case other =>
        other.indexedRDD.flatMap(_.data)
    }
  }

  override def output: Seq[Attribute] = relation.output
}
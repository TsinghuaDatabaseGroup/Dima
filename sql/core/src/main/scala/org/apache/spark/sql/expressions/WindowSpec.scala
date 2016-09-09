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

package org.apache.spark.sql.expressions

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.{Column, catalyst}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._


/**
 * :: Experimental ::
 * A window specification that defines the partitioning, ordering, and frame boundaries.
 *
 * Use the static methods in [[Window]] to create a [[WindowSpec]].
 *
 * @since 1.4.0
 */
@Experimental
class WindowSpec private[sql](
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    frame: catalyst.expressions.WindowFrame) {

  /**
   * Defines the partitioning columns in a [[WindowSpec]].
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def partitionBy(colName: String, colNames: String*): WindowSpec = {
    partitionBy((colName +: colNames).map(Column(_)): _*)
  }

  /**
   * Defines the partitioning columns in a [[WindowSpec]].
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def partitionBy(cols: Column*): WindowSpec = {
    new WindowSpec(cols.map(_.expr), orderSpec, frame)
  }

  /**
   * Defines the ordering columns in a [[WindowSpec]].
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def orderBy(colName: String, colNames: String*): WindowSpec = {
    orderBy((colName +: colNames).map(Column(_)): _*)
  }

  /**
   * Defines the ordering columns in a [[WindowSpec]].
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def orderBy(cols: Column*): WindowSpec = {
    val sortOrder: Seq[SortOrder] = cols.map { col =>
      col.expr match {
        case expr: SortOrder =>
          expr
        case expr: Expression =>
          SortOrder(expr, Ascending)
      }
    }
    new WindowSpec(partitionSpec, sortOrder, frame)
  }

  /**
   * Defines the frame boundaries, from `start` (inclusive) to `end` (inclusive).
   *
   * Both `start` and `end` are relative positions from the current row. For example, "0" means
   * "current row", while "-1" means the row before the current row, and "5" means the fifth row
   * after the current row.
   *
   * @param start boundary start, inclusive.
   *              The frame is unbounded if this is the minimum long value.
   * @param end boundary end, inclusive.
   *            The frame is unbounded if this is the maximum long value.
   * @since 1.4.0
   */
  def rowsBetween(start: Long, end: Long): WindowSpec = {
    between(RowFrame, start, end)
  }

  /**
   * Defines the frame boundaries, from `start` (inclusive) to `end` (inclusive).
   *
   * Both `start` and `end` are relative from the current row. For example, "0" means "current row",
   * while "-1" means one off before the current row, and "5" means the five off after the
   * current row.
   *
   * @param start boundary start, inclusive.
   *              The frame is unbounded if this is the minimum long value.
   * @param end boundary end, inclusive.
   *            The frame is unbounded if this is the maximum long value.
   * @since 1.4.0
   */
  def rangeBetween(start: Long, end: Long): WindowSpec = {
    between(RangeFrame, start, end)
  }

  private def between(typ: FrameType, start: Long, end: Long): WindowSpec = {
    val boundaryStart = start match {
      case 0 => CurrentRow
      case Long.MinValue => UnboundedPreceding
      case x if x < 0 => ValuePreceding(-start.toInt)
      case x if x > 0 => ValueFollowing(start.toInt)
    }

    val boundaryEnd = end match {
      case 0 => CurrentRow
      case Long.MaxValue => UnboundedFollowing
      case x if x < 0 => ValuePreceding(-end.toInt)
      case x if x > 0 => ValueFollowing(end.toInt)
    }

    new WindowSpec(
      partitionSpec,
      orderSpec,
      SpecifiedWindowFrame(typ, boundaryStart, boundaryEnd))
  }

  /**
   * Converts this [[WindowSpec]] into a [[Column]] with an aggregate expression.
   */
  private[sql] def withAggregate(aggregate: Column): Column = {
    val windowExpr = aggregate.expr match {
      // First, we check if we get an aggregate function without the DISTINCT keyword.
      // Right now, we do not support using a DISTINCT aggregate function as a
      // window function.
      case AggregateExpression(aggregateFunction, _, isDistinct) if !isDistinct =>
        aggregateFunction match {
          case Average(child) => WindowExpression(
            UnresolvedWindowFunction("avg", child :: Nil),
            WindowSpecDefinition(partitionSpec, orderSpec, frame))
          case Sum(child) => WindowExpression(
            UnresolvedWindowFunction("sum", child :: Nil),
            WindowSpecDefinition(partitionSpec, orderSpec, frame))
          case Count(children) => WindowExpression(
            UnresolvedWindowFunction("count", children),
            WindowSpecDefinition(partitionSpec, orderSpec, frame))
          case First(child, ignoreNulls) => WindowExpression(
            // TODO this is a hack for Hive UDAF first_value
            UnresolvedWindowFunction(
              "first_value",
              child :: ignoreNulls :: Nil),
            WindowSpecDefinition(partitionSpec, orderSpec, frame))
          case Last(child, ignoreNulls) => WindowExpression(
            // TODO this is a hack for Hive UDAF last_value
            UnresolvedWindowFunction(
              "last_value",
              child :: ignoreNulls :: Nil),
            WindowSpecDefinition(partitionSpec, orderSpec, frame))
          case Min(child) => WindowExpression(
            UnresolvedWindowFunction("min", child :: Nil),
            WindowSpecDefinition(partitionSpec, orderSpec, frame))
          case Max(child) => WindowExpression(
            UnresolvedWindowFunction("max", child :: Nil),
            WindowSpecDefinition(partitionSpec, orderSpec, frame))
          case x =>
            throw new UnsupportedOperationException(s"$x is not supported in a window operation.")
        }

      case AggregateExpression(aggregateFunction, _, isDistinct) if isDistinct =>
        throw new UnsupportedOperationException(
          s"Distinct aggregate function ${aggregateFunction} is not supported " +
            s"in window operation.")

      case wf: WindowFunction =>
        WindowExpression(
          wf,
          WindowSpecDefinition(partitionSpec, orderSpec, frame))

      case x =>
        throw new UnsupportedOperationException(s"$x is not supported in a window operation.")
    }

    new Column(windowExpr)
  }
}

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

package org.apache.spark.sql.execution

import java.util.NoSuchElementException

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.index.IndexType
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLConf, SQLContext}

/**
 * A logical command that is executed for its side-effects.  `RunnableCommand`s are
 * wrapped in `ExecutedCommand` during execution.
 */
private[sql] trait RunnableCommand extends LogicalPlan with logical.Command {
  override def output: Seq[Attribute] = Seq.empty
  override def children: Seq[LogicalPlan] = Seq.empty
  def run(sqlContext: SQLContext): Seq[Row]
}

/**
 * A physical operator that executes the run method of a `RunnableCommand` and
 * saves the result to prevent multiple executions.
 */
private[sql] case class ExecutedCommand(cmd: RunnableCommand) extends SparkPlan {
  /**
   * A concrete command should override this lazy field to wrap up any side effects caused by the
   * command or any other computation that should be evaluated exactly once. The value of this field
   * can be used as the contents of the corresponding RDD generated from the physical plan of this
   * command.
   *
   * The `execute()` method of all the physical command classes should reference `sideEffectResult`
   * so that the command can be executed eagerly right after the command query is created.
   */
  protected[sql] lazy val sideEffectResult: Seq[InternalRow] = {
    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
    cmd.run(sqlContext).map(converter(_).asInstanceOf[InternalRow])
  }

  override def output: Seq[Attribute] = cmd.output

  override def children: Seq[SparkPlan] = Nil

  override def executeCollect(): Array[InternalRow] = sideEffectResult.toArray

  override def executeTake(limit: Int): Array[InternalRow] = sideEffectResult.take(limit).toArray

  protected override def doExecute(): RDD[InternalRow] = {
    sqlContext.sparkContext.parallelize(sideEffectResult, 1)
  }

  override def argString: String = cmd.toString
}


case class SetCommand(kv: Option[(String, Option[String])]) extends RunnableCommand with Logging {

  private def keyValueOutput: Seq[Attribute] = {
    val schema = StructType(
      StructField("key", StringType, false) ::
        StructField("value", StringType, false) :: Nil)
    schema.toAttributes
  }

  private val (_output, runFunc): (Seq[Attribute], SQLContext => Seq[Row]) = kv match {
    // Configures the deprecated "mapred.reduce.tasks" property.
    case Some((SQLConf.Deprecated.MAPRED_REDUCE_TASKS, Some(value))) =>
      val runFunc = (sqlContext: SQLContext) => {
        logWarning(
          s"Property ${SQLConf.Deprecated.MAPRED_REDUCE_TASKS} is deprecated, " +
            s"automatically converted to ${SQLConf.SHUFFLE_PARTITIONS.key} instead.")
        if (value.toInt < 1) {
          val msg =
            s"Setting negative ${SQLConf.Deprecated.MAPRED_REDUCE_TASKS} for automatically " +
              "determining the number of reducers is not supported."
          throw new IllegalArgumentException(msg)
        } else {
          sqlContext.setConf(SQLConf.SHUFFLE_PARTITIONS.key, value)
          Seq(Row(SQLConf.SHUFFLE_PARTITIONS.key, value))
        }
      }
      (keyValueOutput, runFunc)

    case Some((SQLConf.Deprecated.EXTERNAL_SORT, Some(value))) =>
      val runFunc = (sqlContext: SQLContext) => {
        logWarning(
          s"Property ${SQLConf.Deprecated.EXTERNAL_SORT} is deprecated and will be ignored. " +
            s"External sort will continue to be used.")
        Seq(Row(SQLConf.Deprecated.EXTERNAL_SORT, "true"))
      }
      (keyValueOutput, runFunc)

    case Some((SQLConf.Deprecated.USE_SQL_AGGREGATE2, Some(value))) =>
      val runFunc = (sqlContext: SQLContext) => {
        logWarning(
          s"Property ${SQLConf.Deprecated.USE_SQL_AGGREGATE2} is deprecated and " +
            s"will be ignored. ${SQLConf.Deprecated.USE_SQL_AGGREGATE2} will " +
            s"continue to be true.")
        Seq(Row(SQLConf.Deprecated.USE_SQL_AGGREGATE2, "true"))
      }
      (keyValueOutput, runFunc)

    case Some((SQLConf.Deprecated.TUNGSTEN_ENABLED, Some(value))) =>
      val runFunc = (sqlContext: SQLContext) => {
        logWarning(
          s"Property ${SQLConf.Deprecated.TUNGSTEN_ENABLED} is deprecated and " +
            s"will be ignored. Tungsten will continue to be used.")
        Seq(Row(SQLConf.Deprecated.TUNGSTEN_ENABLED, "true"))
      }
      (keyValueOutput, runFunc)

    case Some((SQLConf.Deprecated.CODEGEN_ENABLED, Some(value))) =>
      val runFunc = (sqlContext: SQLContext) => {
        logWarning(
          s"Property ${SQLConf.Deprecated.CODEGEN_ENABLED} is deprecated and " +
            s"will be ignored. Codegen will continue to be used.")
        Seq(Row(SQLConf.Deprecated.CODEGEN_ENABLED, "true"))
      }
      (keyValueOutput, runFunc)

    case Some((SQLConf.Deprecated.UNSAFE_ENABLED, Some(value))) =>
      val runFunc = (sqlContext: SQLContext) => {
        logWarning(
          s"Property ${SQLConf.Deprecated.UNSAFE_ENABLED} is deprecated and " +
            s"will be ignored. Unsafe mode will continue to be used.")
        Seq(Row(SQLConf.Deprecated.UNSAFE_ENABLED, "true"))
      }
      (keyValueOutput, runFunc)

      (keyValueOutput, runFunc)

    case Some((SQLConf.Deprecated.SORTMERGE_JOIN, Some(value))) =>
      val runFunc = (sqlContext: SQLContext) => {
        logWarning(
          s"Property ${SQLConf.Deprecated.SORTMERGE_JOIN} is deprecated and " +
            s"will be ignored. Sort merge join will continue to be used.")
        Seq(Row(SQLConf.Deprecated.SORTMERGE_JOIN, "true"))
      }
      (keyValueOutput, runFunc)

    // Configures a single property.
    case Some((key, Some(value))) =>
      val runFunc = (sqlContext: SQLContext) => {
        sqlContext.setConf(key, value)
        Seq(Row(key, value))
      }
      (keyValueOutput, runFunc)

    // (In Hive, "SET" returns all changed properties while "SET -v" returns all properties.)
    // Queries all key-value pairs that are set in the SQLConf of the sqlContext.
    case None =>
      val runFunc = (sqlContext: SQLContext) => {
        sqlContext.getAllConfs.map { case (k, v) => Row(k, v) }.toSeq
      }
      (keyValueOutput, runFunc)

    // Queries all properties along with their default values and docs that are defined in the
    // SQLConf of the sqlContext.
    case Some(("-v", None)) =>
      val runFunc = (sqlContext: SQLContext) => {
        sqlContext.conf.getAllDefinedConfs.map { case (key, defaultValue, doc) =>
          Row(key, defaultValue, doc)
        }
      }
      val schema = StructType(
        StructField("key", StringType, false) ::
          StructField("default", StringType, false) ::
          StructField("meaning", StringType, false) :: Nil)
      (schema.toAttributes, runFunc)

    // Queries the deprecated "mapred.reduce.tasks" property.
    case Some((SQLConf.Deprecated.MAPRED_REDUCE_TASKS, None)) =>
      val runFunc = (sqlContext: SQLContext) => {
        logWarning(
          s"Property ${SQLConf.Deprecated.MAPRED_REDUCE_TASKS} is deprecated, " +
            s"showing ${SQLConf.SHUFFLE_PARTITIONS.key} instead.")
        Seq(Row(SQLConf.SHUFFLE_PARTITIONS.key, sqlContext.conf.numShufflePartitions.toString))
      }
      (keyValueOutput, runFunc)

    // Queries a single property.
    case Some((key, None)) =>
      val runFunc = (sqlContext: SQLContext) => {
        val value =
          try {
            if (key == SQLConf.DIALECT.key) {
              sqlContext.conf.dialect
            } else {
              sqlContext.getConf(key)
            }
          } catch {
            case _: NoSuchElementException => "<undefined>"
          }
        Seq(Row(key, value))
      }
      (keyValueOutput, runFunc)
  }

  override val output: Seq[Attribute] = _output

  override def run(sqlContext: SQLContext): Seq[Row] = runFunc(sqlContext)

}

/**
 * An explain command for users to see how a command will be executed.
 *
 * Note that this command takes in a logical plan, runs the optimizer on the logical plan
 * (but do NOT actually execute it).
 */
case class ExplainCommand(
    logicalPlan: LogicalPlan,
    override val output: Seq[Attribute] =
      Seq(AttributeReference("plan", StringType, nullable = false)()),
    extended: Boolean = false)
  extends RunnableCommand {

  // Run through the optimizer to generate the physical plan.
  override def run(sqlContext: SQLContext): Seq[Row] = try {
    // TODO in Hive, the "extended" ExplainCommand prints the AST as well, and detailed properties.
    val queryExecution = sqlContext.executePlan(logicalPlan)
    val outputString = if (extended) queryExecution.toString else queryExecution.simpleString

    outputString.split("\n").map(Row(_))
  } catch { case cause: TreeNodeException[_] =>
    ("Error occurred during query planning: \n" + cause.getMessage).split("\n").map(Row(_))
  }
}


case class CacheTableCommand(
    tableName: String,
    plan: Option[LogicalPlan],
    isLazy: Boolean)
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    plan.foreach { logicalPlan =>
      sqlContext.registerDataFrameAsTable(DataFrame(sqlContext, logicalPlan), tableName)
    }
    sqlContext.cacheTable(tableName)

    if (!isLazy) {
      // Performs eager caching
      sqlContext.table(tableName).count()
    }

    Seq.empty[Row]
  }

  override def output: Seq[Attribute] = Seq.empty
}


case class UncacheTableCommand(tableName: String) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    sqlContext.table(tableName).unpersist(blocking = false)
    Seq.empty[Row]
  }

  override def output: Seq[Attribute] = Seq.empty
}

/**
 * Clear all cached data from the in-memory cache.
 */
case object ClearCacheCommand extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    sqlContext.clearCache()
    Seq.empty[Row]
  }

  override def output: Seq[Attribute] = Seq.empty
}

case class PersistIndexCommand(indexName: String, fileName: String)
  extends RunnableCommand {
  override def run(sqlContext: SQLContext): Seq[Row] = {
    sqlContext.persistIndex(indexName, fileName)
    Seq.empty[Row]
  }
}

case class LoadIndexCommand(indexName: String, fileName: String)
  extends RunnableCommand {
  override def run(sqlContext: SQLContext): Seq[Row] = {
    sqlContext.loadIndex(indexName, fileName)
    Seq.empty[Row]
  }
}

case class IndexTableCommand(tableName: String, columnName: List[String],
                             indexType: IndexType, indexName: String) extends RunnableCommand {
  override def run(sqlContext: SQLContext): Seq[Row] = {
    val table = sqlContext.table(tableName)
    assert(table != null, "Table not found!")
    val attrs = table.queryExecution.analyzed.output
    val columnKeys = columnName.map(attr => {
      var ans : Attribute = null
      for (i <- attrs.indices)
        if (attrs(i).name.equals(attr)) ans = attrs(i)
      assert(ans != null, "Attribute not found")
      ans
    })
    sqlContext.indexTable(tableName, indexType, indexName, columnKeys)
    Seq.empty[Row]
  }

  override def output: Seq[Attribute] = Seq.empty
}

case class ShowIndexCommand(tableName: String) extends RunnableCommand {
  override def run(sqlContext: SQLContext): Seq[Row] = {
    val table = sqlContext.table(tableName)
    assert(table != null, "Table not found!")
    val attrs = table.queryExecution.analyzed.output
    sqlContext.showIndex(tableName)
    Seq.empty[Row]
  }
}

case class DeindexTableCommand(tableName: String) extends RunnableCommand {
  override def run(sqlContext: SQLContext): Seq[Row] = {
    sqlContext.table(tableName).deindex(blocking = false)
    Seq.empty[Row]
  }
  override def output: Seq[Attribute] = Seq.empty
}
case class DeindexTableByNameCommand(tableName: String, indexName : String)
  extends RunnableCommand {
  override def run(sqlContext: SQLContext): Seq[Row] = {
    sqlContext.table(tableName).deindexByName(indexName)
    Seq.empty[Row]
  }
  override def output: Seq[Attribute] = Seq.empty
}

case object ClearIndexCommand extends RunnableCommand {
  override def run(sqlContext: SQLContext): Seq[Row] = {
    sqlContext.clearIndex()
    Seq.empty[Row]
  }

  override def output: Seq[Attribute] = Seq.empty
}


case class DescribeCommand(
    child: SparkPlan,
    override val output: Seq[Attribute],
    isExtended: Boolean)
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    child.schema.fields.map { field =>
      val cmtKey = "comment"
      val comment = if (field.metadata.contains(cmtKey)) field.metadata.getString(cmtKey) else ""
      Row(field.name, field.dataType.simpleString, comment)
    }
  }
}

/**
 * A command for users to get tables in the given database.
 * If a databaseName is not given, the current database will be used.
 * The syntax of using this command in SQL is:
 * {{{
 *    SHOW TABLES [IN databaseName]
 * }}}
 */
case class ShowTablesCommand(databaseName: Option[String]) extends RunnableCommand {

  // The result of SHOW TABLES has two columns, tableName and isTemporary.
  override val output: Seq[Attribute] = {
    val schema = StructType(
      StructField("tableName", StringType, false) ::
      StructField("isTemporary", BooleanType, false) :: Nil)

    schema.toAttributes
  }

  override def run(sqlContext: SQLContext): Seq[Row] = {
    // Since we need to return a Seq of rows, we will call getTables directly
    // instead of calling tables in sqlContext.
    val rows = sqlContext.catalog.getTables(databaseName).map {
      case (tableName, isTemporary) => Row(tableName, isTemporary)
    }

    rows
  }
}

/**
 * A command for users to list all of the registered functions.
 * The syntax of using this command in SQL is:
 * {{{
 *    SHOW FUNCTIONS
 * }}}
 * TODO currently we are simply ignore the db
 */
case class ShowFunctions(db: Option[String], pattern: Option[String]) extends RunnableCommand {
  override val output: Seq[Attribute] = {
    val schema = StructType(
      StructField("function", StringType, nullable = false) :: Nil)

    schema.toAttributes
  }

  override def run(sqlContext: SQLContext): Seq[Row] = pattern match {
    case Some(p) =>
      try {
        val regex = java.util.regex.Pattern.compile(p)
        sqlContext.functionRegistry.listFunction().filter(regex.matcher(_).matches()).map(Row(_))
      } catch {
        // probably will failed in the regex that user provided, then returns empty row.
        case _: Throwable => Seq.empty[Row]
      }
    case None =>
      sqlContext.functionRegistry.listFunction().map(Row(_))
  }
}

/**
 * A command for users to get the usage of a registered function.
 * The syntax of using this command in SQL is
 * {{{
 *   DESCRIBE FUNCTION [EXTENDED] upper;
 * }}}
 */
case class DescribeFunction(
    functionName: String,
    isExtended: Boolean) extends RunnableCommand {

  override val output: Seq[Attribute] = {
    val schema = StructType(
      StructField("function_desc", StringType, nullable = false) :: Nil)

    schema.toAttributes
  }

  private def replaceFunctionName(usage: String, functionName: String): String = {
    if (usage == null) {
      "To be added."
    } else {
      usage.replaceAll("_FUNC_", functionName)
    }
  }

  override def run(sqlContext: SQLContext): Seq[Row] = {
    sqlContext.functionRegistry.lookupFunction(functionName) match {
      case Some(info) =>
        val result =
          Row(s"Function: ${info.getName}") ::
          Row(s"Class: ${info.getClassName}") ::
          Row(s"Usage: ${replaceFunctionName(info.getUsage(), info.getName)}") :: Nil

        if (isExtended) {
          result :+ Row(s"Extended Usage:\n${replaceFunctionName(info.getExtended, info.getName)}")
        } else {
          result
        }

      case None => Seq(Row(s"Function: $functionName is not found."))
    }
  }
}

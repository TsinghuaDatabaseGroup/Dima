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

package org.apache.spark.input

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext

/**
 * A [[org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat CombineFileInputFormat]] for
 * reading whole text files. Each file is read as key-value pair, where the key is the file path and
 * the value is the entire content of file.
 */

private[spark] class WholeTextFileInputFormat
  extends CombineFileInputFormat[Text, Text] with Configurable {

  override protected def isSplitable(context: JobContext, file: Path): Boolean = false

  override def createRecordReader(
      split: InputSplit,
      context: TaskAttemptContext): RecordReader[Text, Text] = {
    val reader =
      new ConfigurableCombineFileRecordReader(split, context, classOf[WholeTextFileRecordReader])
    reader.setConf(getConf)
    reader
  }

  /**
   * Allow minPartitions set by end-user in order to keep compatibility with old Hadoop API,
   * which is set through setMaxSplitSize
   */
  def setMinPartitions(context: JobContext, minPartitions: Int) {
    val files = listStatus(context).asScala
    val totalLen = files.map(file => if (file.isDir) 0L else file.getLen).sum
    val maxSplitSize = Math.ceil(totalLen * 1.0 /
      (if (minPartitions == 0) 1 else minPartitions)).toLong
    super.setMaxSplitSize(maxSplitSize)
  }
}

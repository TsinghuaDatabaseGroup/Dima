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

package org.apache.spark.sql.hive.orc

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.io.orc.{OrcFile, Reader}
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector

import org.apache.spark.Logging
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.hive.HiveMetastoreTypes
import org.apache.spark.sql.types.StructType

private[orc] object OrcFileOperator extends Logging {
  /**
   * Retrieves a ORC file reader from a given path.  The path can point to either a directory or a
   * single ORC file.  If it points to an directory, it picks any non-empty ORC file within that
   * directory.
   *
   * The reader returned by this method is mainly used for two purposes:
   *
   * 1. Retrieving file metadata (schema and compression codecs, etc.)
   * 2. Read the actual file content (in this case, the given path should point to the target file)
   *
   * @note As recorded by SPARK-8501, ORC writes an empty schema (<code>struct&lt;&gt;</code) to an
   *       ORC file if the file contains zero rows. This is OK for Hive since the schema of the
   *       table is managed by metastore.  But this becomes a problem when reading ORC files
   *       directly from HDFS via Spark SQL, because we have to discover the schema from raw ORC
   *       files.  So this method always tries to find a ORC file whose schema is non-empty, and
   *       create the result reader from that file.  If no such file is found, it returns `None`.
   *
   * @todo Needs to consider all files when schema evolution is taken into account.
   */
  def getFileReader(basePath: String, config: Option[Configuration] = None): Option[Reader] = {
    def isWithNonEmptySchema(path: Path, reader: Reader): Boolean = {
      reader.getObjectInspector match {
        case oi: StructObjectInspector if oi.getAllStructFieldRefs.size() == 0 =>
          logInfo(
            s"ORC file $path has empty schema, it probably contains no rows. " +
              "Trying to read another ORC file to figure out the schema.")
          false
        case _ => true
      }
    }

    val conf = config.getOrElse(new Configuration)
    val fs = {
      val hdfsPath = new Path(basePath)
      hdfsPath.getFileSystem(conf)
    }

    listOrcFiles(basePath, conf).iterator.map { path =>
      path -> OrcFile.createReader(fs, path)
    }.collectFirst {
      case (path, reader) if isWithNonEmptySchema(path, reader) => reader
    }
  }

  def readSchema(path: String, conf: Option[Configuration]): StructType = {
    val reader = getFileReader(path, conf).getOrElse {
      throw new AnalysisException(
        s"Failed to discover schema from ORC files stored in $path. " +
          "Probably there are either no ORC files or only empty ORC files.")
    }
    val readerInspector = reader.getObjectInspector.asInstanceOf[StructObjectInspector]
    val schema = readerInspector.getTypeName
    logDebug(s"Reading schema from file $path, got Hive schema string: $schema")
    HiveMetastoreTypes.toDataType(schema).asInstanceOf[StructType]
  }

  def getObjectInspector(
      path: String, conf: Option[Configuration]): Option[StructObjectInspector] = {
    getFileReader(path, conf).map(_.getObjectInspector.asInstanceOf[StructObjectInspector])
  }

  def listOrcFiles(pathStr: String, conf: Configuration): Seq[Path] = {
    val origPath = new Path(pathStr)
    val fs = origPath.getFileSystem(conf)
    val path = origPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    val paths = SparkHadoopUtil.get.listLeafStatuses(fs, origPath)
      .filterNot(_.isDir)
      .map(_.getPath)
      .filterNot(_.getName.startsWith("_"))
      .filterNot(_.getName.startsWith("."))

    if (paths == null || paths.isEmpty) {
      throw new IllegalArgumentException(
        s"orcFileOperator: path $path does not have valid orc files matching the pattern")
    }

    paths
  }
}

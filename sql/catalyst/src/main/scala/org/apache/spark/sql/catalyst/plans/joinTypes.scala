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

package org.apache.spark.sql.catalyst.plans

object JoinType {
  def apply(typ: String): JoinType = typ.toLowerCase.replace("_", "") match {
    case "inner" => Inner
    case "outer" | "full" | "fullouter" => FullOuter
    case "leftouter" | "left" => LeftOuter
    case "rightouter" | "right" => RightOuter
    case "leftsemi" => LeftSemi
    case "knn" => KNNJoin
    case "distance" => DistanceJoin
    case "zknn" => ZKNNJoin
    case "similarity" => SimilarityJoin
    case _ =>
      val supported = Seq(
        "inner",
        "outer", "full", "fullouter",
        "leftouter", "left",
        "rightouter", "right",
        "leftsemi", "knn", "distance", "zknn",
        "similarity")

      throw new IllegalArgumentException(s"Unsupported join type '$typ'. " +
        "Supported join types include: " + supported.mkString("'", "', '", "'") + ".")
  }
}

sealed abstract class JoinType

case object Inner extends JoinType

case object LeftOuter extends JoinType

case object RightOuter extends JoinType

case object FullOuter extends JoinType

case object LeftSemi extends JoinType

case object KNNJoin extends JoinType

case object DistanceJoin extends JoinType

case object ZKNNJoin extends JoinType

case object SimilarityJoin extends JoinType

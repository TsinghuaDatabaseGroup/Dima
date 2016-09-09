#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

from pyspark import SparkContext
from pyspark.sql import SQLContext
# $example on$
from pyspark.ml.feature import PolynomialExpansion
from pyspark.mllib.linalg import Vectors
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="PolynomialExpansionExample")
    sqlContext = SQLContext(sc)

    # $example on$
    df = sqlContext\
        .createDataFrame([(Vectors.dense([-2.0, 2.3]),),
                          (Vectors.dense([0.0, 0.0]),),
                          (Vectors.dense([0.6, -1.1]),)],
                         ["features"])
    px = PolynomialExpansion(degree=2, inputCol="features", outputCol="polyFeatures")
    polyDF = px.transform(df)
    for expanded in polyDF.select("polyFeatures").take(3):
        print(expanded)
    # $example off$

    sc.stop()

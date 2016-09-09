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
from pyspark.ml.feature import VectorIndexer
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="VectorIndexerExample")
    sqlContext = SQLContext(sc)

    # $example on$
    data = sqlContext.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")
    indexer = VectorIndexer(inputCol="features", outputCol="indexed", maxCategories=10)
    indexerModel = indexer.fit(data)

    # Create new column "indexed" with categorical values transformed to indices
    indexedData = indexerModel.transform(data)
    indexedData.show()
    # $example off$

    sc.stop()

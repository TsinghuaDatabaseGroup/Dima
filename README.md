# SimilarityOnSpark

## Abstract
Data analysts in industries spend more than 80% of time on data cleaning and integration in the whole process of data analytics due to data errors and inconsistencies. Similarity-based  query processing is an important way to tolerate the errors and inconsistencies. However, there is no SQL-based system that can support similarity-based query processing.  In this paper, we develop a distributed in-memory similarity-based query processing system called Dima. Dima supports four core similarity operations, i.e., similarity selection, similarity join, top-k selection and top-k join. Dima extends SQL for users to easily invoke these similarity-based operations in their data analysis tasks. To avoid expensive data transformation in a distributed environment, we propose selectable signatures where two records are similar if they share common signatures. More importantly, we can adaptively select the signatures to balance the workload. Dima builds signature-based global indexes and local indexes to support similarity operations. Since Spark is one of the widely adopted distributed in-memory computing systems, we have seamlessly integrated Dima into Spark and developed effective query optimization techniques in Spark.  To the best of our knowledge, this is the first full-fledged distributed in-memory system that can support similarity-based query processing. We have conducted extensive experiments on four real-world datasets. Experimental results show that Dima outperforms state-of-the-art studies by 1-3 orders of magnitude and has good scalability.  
This project is built on Spark-1.6.0 version.

## Build And Install
After downloading the source code, enter into the directory, and run the script to build the code by using maven-3.3.3  
```shell
./make-distribution.sh --skip-java-test --tgz --mvn mvn -Phadoop-2.4 -Pyarn -Dhadoop.verison=2.4.1 -DskipTests
```
Then the SimilarityOnSpark/dist is the runnable package, you deploy it on your cluster just like what you do to deploy native spark。  

To see details about apache spark, click here https://github.com/apache/spark/blob/branch-1.6/README.md

## Use Guide
### Configuration
```json
[{
    "name": "spark.sql.joins.numSimialrityPartitions",
    "default": 2,
    "description": "number of data partitions when similarity joining."
},
{
    "name": "spark.sql.joins.similarityJaccardThreshold",
    "default": 0.8,
    "description": "threshold when creating index for Jaccard Similarity Search."
},
{
    "name": "spark.sql.joins.similarityEditDistanceThreshold",
    "default": 3,
    "description": "threshold when creating index for Edit Distance Similarity Search."
},
{
    "name": "spark.sql.joins.similarityFrequencyAbandonNum",
    "default": 2,
    "description": "token with frequecy lower than this value will not participate optimizaton process."
},
{
    "name": "spark.sql.joins.similarityFrequencyAbandonNum",
    "default": 2,
    "description": "token with frequecy lower than this value will not participate optimizaton process."
}]
```
### Sql Example
#### JACCARD SimJoin
```sql
SELECT * FROM left_table SIMILARITY JOIN right_table ON JACCARDSIMILARITY(left_table.title, right_table.title) >= 0.8;
```
#### JACCARD SimSearch
```sql
CREATE INDEX jaccard_idx ON left_table(title) USE JACCARDINDEX;
SELECT * FROM left_table where JACCARDSIMILARITY(left_table.title, "Hands on machine learning") >= 0.8;
```


package org.apache.spark.sql.simjointopk.partitioner

import org.apache.spark.Partitioner

/**
  * Created by sunji on 16/11/25.
  */
class HashPartitioner(numParts: Int) extends Partitioner {

  override def numPartitions: Int = numParts
  def hashStrategy(key: Any): Int = {
    val code = (key.hashCode % numPartitions)
    if (code < 0) {
      code + numPartitions
    } else {
      code
    }
  }

  override def getPartition(key: Any): Int = {
    val k = key.hashCode()
    hashStrategy(k)
  }

  override def equals(other: Any): Boolean = other match {
    case similarity: HashPartitioner =>
      similarity.numPartitions == numPartitions
    case _ =>
      false
  }
  override def hashCode: Int = numPartitions
}

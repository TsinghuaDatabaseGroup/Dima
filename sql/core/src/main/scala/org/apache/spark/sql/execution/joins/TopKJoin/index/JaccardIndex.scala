package org.apache.spark.sql.simjointopk.index

/**
  * Created by sunji on 16/11/25.
  */

class JaccardIndex extends Serializable{
  val mapping = scala.collection.mutable.Map[Int, List[Int]]()
  def addIndex(key: Int, pos: Int): Unit = {
    mapping += Tuple2(key, pos :: mapping.getOrElse(key, List[Int]()))
  }
}

object JaccardIndex {
  def apply(data: Array[Int]): JaccardIndex = {
    val index = new JaccardIndex()
    data.zipWithIndex.map ( x => {
      index.addIndex(x._1, x._2)
    })
    index
  }
}

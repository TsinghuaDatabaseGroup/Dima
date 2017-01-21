package org.apache.spark.sql.simjointopk.index

/**
  * Created by sunji on 17/1/19.
  */

class EditIndex extends Serializable{
  val mapping = scala.collection.mutable.Map[Int, List[Int]]()
  def addIndex(key: Int, pos: Int): Unit = {
    mapping += Tuple2(key, pos :: mapping.getOrElse(key, List[Int]()))
  }
}

object EditIndex {
  def apply(data: Array[Int]): EditIndex = {
    val index = new EditIndex()
    data.zipWithIndex.map ( x => {
      index.addIndex(x._1, x._2)
    })
    index
  }
}


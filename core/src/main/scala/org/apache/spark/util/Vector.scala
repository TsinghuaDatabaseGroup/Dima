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

package org.apache.spark.util

import scala.language.implicitConversions
import scala.util.Random

import org.apache.spark.util.random.XORShiftRandom

@deprecated("Use Vectors.dense from Spark's mllib.linalg package instead.", "1.0.0")
class Vector(val elements: Array[Double]) extends Serializable {
  def length: Int = elements.length

  def apply(index: Int): Double = elements(index)

  def + (other: Vector): Vector = {
    if (length != other.length) {
      throw new IllegalArgumentException("Vectors of different length")
    }
    Vector(length, i => this(i) + other(i))
  }

  def add(other: Vector): Vector = this + other

  def - (other: Vector): Vector = {
    if (length != other.length) {
      throw new IllegalArgumentException("Vectors of different length")
    }
    Vector(length, i => this(i) - other(i))
  }

  def subtract(other: Vector): Vector = this - other

  def dot(other: Vector): Double = {
    if (length != other.length) {
      throw new IllegalArgumentException("Vectors of different length")
    }
    var ans = 0.0
    var i = 0
    while (i < length) {
      ans += this(i) * other(i)
      i += 1
    }
    ans
  }

  /**
   * return (this + plus) dot other, but without creating any intermediate storage
   * @param plus
   * @param other
   * @return
   */
  def plusDot(plus: Vector, other: Vector): Double = {
    if (length != other.length) {
      throw new IllegalArgumentException("Vectors of different length")
    }
    if (length != plus.length) {
      throw new IllegalArgumentException("Vectors of different length")
    }
    var ans = 0.0
    var i = 0
    while (i < length) {
      ans += (this(i) + plus(i)) * other(i)
      i += 1
    }
    ans
  }

  def += (other: Vector): Vector = {
    if (length != other.length) {
      throw new IllegalArgumentException("Vectors of different length")
    }
    var i = 0
    while (i < length) {
      elements(i) += other(i)
      i += 1
    }
    this
  }

  def addInPlace(other: Vector): Vector = this +=other

  def * (scale: Double): Vector = Vector(length, i => this(i) * scale)

  def multiply (d: Double): Vector = this * d

  def / (d: Double): Vector = this * (1 / d)

  def divide (d: Double): Vector = this / d

  def unary_- : Vector = this * -1

  def sum: Double = elements.reduceLeft(_ + _)

  def squaredDist(other: Vector): Double = {
    var ans = 0.0
    var i = 0
    while (i < length) {
      ans += (this(i) - other(i)) * (this(i) - other(i))
      i += 1
    }
    ans
  }

  def dist(other: Vector): Double = math.sqrt(squaredDist(other))

  override def toString: String = elements.mkString("(", ", ", ")")
}

@deprecated("Use Vectors.dense from Spark's mllib.linalg package instead.", "1.0.0")
object Vector {
  def apply(elements: Array[Double]): Vector = new Vector(elements)

  def apply(elements: Double*): Vector = new Vector(elements.toArray)

  def apply(length: Int, initializer: Int => Double): Vector = {
    val elements: Array[Double] = Array.tabulate(length)(initializer)
    new Vector(elements)
  }

  def zeros(length: Int): Vector = new Vector(new Array[Double](length))

  def ones(length: Int): Vector = Vector(length, _ => 1)

  /**
   * Creates this [[org.apache.spark.util.Vector]] of given length containing random numbers
   * between 0.0 and 1.0. Optional scala.util.Random number generator can be provided.
   */
  def random(length: Int, random: Random = new XORShiftRandom()): Vector =
    Vector(length, _ => random.nextDouble())

  class Multiplier(num: Double) {
    def * (vec: Vector): Vector = vec * num
  }

  implicit def doubleToMultiplier(num: Double): Multiplier = new Multiplier(num)

  implicit object VectorAccumParam extends org.apache.spark.AccumulatorParam[Vector] {
    def addInPlace(t1: Vector, t2: Vector): Vector = t1 + t2

    def zero(initialValue: Vector): Vector = Vector.zeros(initialValue.length)
  }

}

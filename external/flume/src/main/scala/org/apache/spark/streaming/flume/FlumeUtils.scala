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

package org.apache.spark.streaming.flume

import java.net.InetSocketAddress
import java.io.{DataOutputStream, ByteArrayOutputStream}
import java.util.{List => JList, Map => JMap}

import scala.collection.JavaConverters._

import org.apache.spark.api.java.function.PairFunction
import org.apache.spark.api.python.PythonRDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaPairDStream, JavaReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream


object FlumeUtils {
  private val DEFAULT_POLLING_PARALLELISM = 5
  private val DEFAULT_POLLING_BATCH_SIZE = 1000

  /**
   * Create a input stream from a Flume source.
   * @param ssc      StreamingContext object
   * @param hostname Hostname of the slave machine to which the flume data will be sent
   * @param port     Port of the slave machine to which the flume data will be sent
   * @param storageLevel  Storage level to use for storing the received objects
   */
  def createStream (
      ssc: StreamingContext,
      hostname: String,
      port: Int,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): ReceiverInputDStream[SparkFlumeEvent] = {
    createStream(ssc, hostname, port, storageLevel, false)
  }

  /**
   * Create a input stream from a Flume source.
   * @param ssc      StreamingContext object
   * @param hostname Hostname of the slave machine to which the flume data will be sent
   * @param port     Port of the slave machine to which the flume data will be sent
   * @param storageLevel  Storage level to use for storing the received objects
   * @param enableDecompression  should netty server decompress input stream
   */
  def createStream (
      ssc: StreamingContext,
      hostname: String,
      port: Int,
      storageLevel: StorageLevel,
      enableDecompression: Boolean
    ): ReceiverInputDStream[SparkFlumeEvent] = {
    val inputStream = new FlumeInputDStream[SparkFlumeEvent](
        ssc, hostname, port, storageLevel, enableDecompression)

    inputStream
  }

  /**
   * Creates a input stream from a Flume source.
   * Storage level of the data will be the default StorageLevel.MEMORY_AND_DISK_SER_2.
   * @param hostname Hostname of the slave machine to which the flume data will be sent
   * @param port     Port of the slave machine to which the flume data will be sent
   */
  def createStream(
      jssc: JavaStreamingContext,
      hostname: String,
      port: Int
    ): JavaReceiverInputDStream[SparkFlumeEvent] = {
    createStream(jssc.ssc, hostname, port)
  }

  /**
   * Creates a input stream from a Flume source.
   * @param hostname Hostname of the slave machine to which the flume data will be sent
   * @param port     Port of the slave machine to which the flume data will be sent
   * @param storageLevel  Storage level to use for storing the received objects
   */
  def createStream(
      jssc: JavaStreamingContext,
      hostname: String,
      port: Int,
      storageLevel: StorageLevel
    ): JavaReceiverInputDStream[SparkFlumeEvent] = {
    createStream(jssc.ssc, hostname, port, storageLevel, false)
  }

  /**
   * Creates a input stream from a Flume source.
   * @param hostname Hostname of the slave machine to which the flume data will be sent
   * @param port     Port of the slave machine to which the flume data will be sent
   * @param storageLevel  Storage level to use for storing the received objects
   * @param enableDecompression  should netty server decompress input stream
   */
  def createStream(
      jssc: JavaStreamingContext,
      hostname: String,
      port: Int,
      storageLevel: StorageLevel,
      enableDecompression: Boolean
    ): JavaReceiverInputDStream[SparkFlumeEvent] = {
    createStream(jssc.ssc, hostname, port, storageLevel, enableDecompression)
  }

  /**
   * Creates an input stream that is to be used with the Spark Sink deployed on a Flume agent.
   * This stream will poll the sink for data and will pull events as they are available.
   * This stream will use a batch size of 1000 events and run 5 threads to pull data.
   * @param hostname Address of the host on which the Spark Sink is running
   * @param port Port of the host at which the Spark Sink is listening
   * @param storageLevel Storage level to use for storing the received objects
   */
  def createPollingStream(
      ssc: StreamingContext,
      hostname: String,
      port: Int,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): ReceiverInputDStream[SparkFlumeEvent] = {
    createPollingStream(ssc, Seq(new InetSocketAddress(hostname, port)), storageLevel)
  }

  /**
   * Creates an input stream that is to be used with the Spark Sink deployed on a Flume agent.
   * This stream will poll the sink for data and will pull events as they are available.
   * This stream will use a batch size of 1000 events and run 5 threads to pull data.
   * @param addresses List of InetSocketAddresses representing the hosts to connect to.
   * @param storageLevel Storage level to use for storing the received objects
   */
  def createPollingStream(
      ssc: StreamingContext,
      addresses: Seq[InetSocketAddress],
      storageLevel: StorageLevel
    ): ReceiverInputDStream[SparkFlumeEvent] = {
    createPollingStream(ssc, addresses, storageLevel,
      DEFAULT_POLLING_BATCH_SIZE, DEFAULT_POLLING_PARALLELISM)
  }

  /**
   * Creates an input stream that is to be used with the Spark Sink deployed on a Flume agent.
   * This stream will poll the sink for data and will pull events as they are available.
   * @param addresses List of InetSocketAddresses representing the hosts to connect to.
   * @param maxBatchSize Maximum number of events to be pulled from the Spark sink in a
   *                     single RPC call
   * @param parallelism Number of concurrent requests this stream should send to the sink. Note
   *                    that having a higher number of requests concurrently being pulled will
   *                    result in this stream using more threads
   * @param storageLevel Storage level to use for storing the received objects
   */
  def createPollingStream(
      ssc: StreamingContext,
      addresses: Seq[InetSocketAddress],
      storageLevel: StorageLevel,
      maxBatchSize: Int,
      parallelism: Int
    ): ReceiverInputDStream[SparkFlumeEvent] = {
    new FlumePollingInputDStream[SparkFlumeEvent](ssc, addresses, maxBatchSize,
      parallelism, storageLevel)
  }

  /**
   * Creates an input stream that is to be used with the Spark Sink deployed on a Flume agent.
   * This stream will poll the sink for data and will pull events as they are available.
   * This stream will use a batch size of 1000 events and run 5 threads to pull data.
   * @param hostname Hostname of the host on which the Spark Sink is running
   * @param port     Port of the host at which the Spark Sink is listening
   */
  def createPollingStream(
      jssc: JavaStreamingContext,
      hostname: String,
      port: Int
    ): JavaReceiverInputDStream[SparkFlumeEvent] = {
    createPollingStream(jssc, hostname, port, StorageLevel.MEMORY_AND_DISK_SER_2)
  }

  /**
   * Creates an input stream that is to be used with the Spark Sink deployed on a Flume agent.
   * This stream will poll the sink for data and will pull events as they are available.
   * This stream will use a batch size of 1000 events and run 5 threads to pull data.
   * @param hostname     Hostname of the host on which the Spark Sink is running
   * @param port         Port of the host at which the Spark Sink is listening
   * @param storageLevel Storage level to use for storing the received objects
   */
  def createPollingStream(
      jssc: JavaStreamingContext,
      hostname: String,
      port: Int,
      storageLevel: StorageLevel
    ): JavaReceiverInputDStream[SparkFlumeEvent] = {
    createPollingStream(jssc, Array(new InetSocketAddress(hostname, port)), storageLevel)
  }

  /**
   * Creates an input stream that is to be used with the Spark Sink deployed on a Flume agent.
   * This stream will poll the sink for data and will pull events as they are available.
   * This stream will use a batch size of 1000 events and run 5 threads to pull data.
   * @param addresses    List of InetSocketAddresses on which the Spark Sink is running.
   * @param storageLevel Storage level to use for storing the received objects
   */
  def createPollingStream(
      jssc: JavaStreamingContext,
      addresses: Array[InetSocketAddress],
      storageLevel: StorageLevel
    ): JavaReceiverInputDStream[SparkFlumeEvent] = {
    createPollingStream(jssc, addresses, storageLevel,
      DEFAULT_POLLING_BATCH_SIZE, DEFAULT_POLLING_PARALLELISM)
  }

  /**
   * Creates an input stream that is to be used with the Spark Sink deployed on a Flume agent.
   * This stream will poll the sink for data and will pull events as they are available.
   * @param addresses    List of InetSocketAddresses on which the Spark Sink is running
   * @param maxBatchSize The maximum number of events to be pulled from the Spark sink in a
   *                     single RPC call
   * @param parallelism  Number of concurrent requests this stream should send to the sink. Note
   *                     that having a higher number of requests concurrently being pulled will
   *                     result in this stream using more threads
   * @param storageLevel Storage level to use for storing the received objects
   */
  def createPollingStream(
      jssc: JavaStreamingContext,
      addresses: Array[InetSocketAddress],
      storageLevel: StorageLevel,
      maxBatchSize: Int,
      parallelism: Int
    ): JavaReceiverInputDStream[SparkFlumeEvent] = {
    createPollingStream(jssc.ssc, addresses, storageLevel, maxBatchSize, parallelism)
  }
}

/**
 * This is a helper class that wraps the methods in FlumeUtils into more Python-friendly class and
 * function so that it can be easily instantiated and called from Python's FlumeUtils.
 */
private[flume] class FlumeUtilsPythonHelper {

  def createStream(
      jssc: JavaStreamingContext,
      hostname: String,
      port: Int,
      storageLevel: StorageLevel,
      enableDecompression: Boolean
    ): JavaPairDStream[Array[Byte], Array[Byte]] = {
    val dstream = FlumeUtils.createStream(jssc, hostname, port, storageLevel, enableDecompression)
    FlumeUtilsPythonHelper.toByteArrayPairDStream(dstream)
  }

  def createPollingStream(
      jssc: JavaStreamingContext,
      hosts: JList[String],
      ports: JList[Int],
      storageLevel: StorageLevel,
      maxBatchSize: Int,
      parallelism: Int
    ): JavaPairDStream[Array[Byte], Array[Byte]] = {
    assert(hosts.size() == ports.size())
    val addresses = hosts.asScala.zip(ports.asScala).map {
      case (host, port) => new InetSocketAddress(host, port)
    }
    val dstream = FlumeUtils.createPollingStream(
      jssc.ssc, addresses, storageLevel, maxBatchSize, parallelism)
    FlumeUtilsPythonHelper.toByteArrayPairDStream(dstream)
  }

}

private object FlumeUtilsPythonHelper {

  private def stringMapToByteArray(map: JMap[CharSequence, CharSequence]): Array[Byte] = {
    val byteStream = new ByteArrayOutputStream()
    val output = new DataOutputStream(byteStream)
    try {
      output.writeInt(map.size)
      map.asScala.foreach { kv =>
        PythonRDD.writeUTF(kv._1.toString, output)
        PythonRDD.writeUTF(kv._2.toString, output)
      }
      byteStream.toByteArray
    }
    finally {
      output.close()
    }
  }

  private def toByteArrayPairDStream(dstream: JavaReceiverInputDStream[SparkFlumeEvent]):
    JavaPairDStream[Array[Byte], Array[Byte]] = {
    dstream.mapToPair(new PairFunction[SparkFlumeEvent, Array[Byte], Array[Byte]] {
      override def call(sparkEvent: SparkFlumeEvent): (Array[Byte], Array[Byte]) = {
        val event = sparkEvent.event
        val byteBuffer = event.getBody
        val body = new Array[Byte](byteBuffer.remaining())
        byteBuffer.get(body)
        (stringMapToByteArray(event.getHeaders), body)
      }
    })
  }
}

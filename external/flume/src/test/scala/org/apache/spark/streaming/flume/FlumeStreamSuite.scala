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

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, SynchronizedBuffer}
import scala.concurrent.duration._
import scala.language.postfixOps

import com.google.common.base.Charsets
import org.jboss.netty.channel.ChannelPipeline
import org.jboss.netty.channel.socket.SocketChannel
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.handler.codec.compression._
import org.scalatest.{BeforeAndAfter, Matchers}
import org.scalatest.concurrent.Eventually._

import org.apache.spark.{Logging, SparkConf, SparkFunSuite}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext, TestOutputStream}

class FlumeStreamSuite extends SparkFunSuite with BeforeAndAfter with Matchers with Logging {
  val conf = new SparkConf().setMaster("local[4]").setAppName("FlumeStreamSuite")
  var ssc: StreamingContext = null

  test("flume input stream") {
    testFlumeStream(testCompression = false)
  }

  test("flume input compressed stream") {
    testFlumeStream(testCompression = true)
  }

  /** Run test on flume stream */
  private def testFlumeStream(testCompression: Boolean): Unit = {
    val input = (1 to 100).map { _.toString }
    val utils = new FlumeTestUtils
    try {
      val outputBuffer = startContext(utils.getTestPort(), testCompression)

      eventually(timeout(10 seconds), interval(100 milliseconds)) {
        utils.writeInput(input.asJava, testCompression)
      }

      eventually(timeout(10 seconds), interval(100 milliseconds)) {
        val outputEvents = outputBuffer.flatten.map { _.event }
        outputEvents.foreach {
          event =>
            event.getHeaders.get("test") should be("header")
        }
        val output = outputEvents.map(event => new String(event.getBody.array(), Charsets.UTF_8))
        output should be (input)
      }
    } finally {
      if (ssc != null) {
        ssc.stop()
      }
      utils.close()
    }
  }

  /** Setup and start the streaming context */
  private def startContext(
      testPort: Int, testCompression: Boolean): (ArrayBuffer[Seq[SparkFlumeEvent]]) = {
    ssc = new StreamingContext(conf, Milliseconds(200))
    val flumeStream = FlumeUtils.createStream(
      ssc, "localhost", testPort, StorageLevel.MEMORY_AND_DISK, testCompression)
    val outputBuffer = new ArrayBuffer[Seq[SparkFlumeEvent]]
      with SynchronizedBuffer[Seq[SparkFlumeEvent]]
    val outputStream = new TestOutputStream(flumeStream, outputBuffer)
    outputStream.register()
    ssc.start()
    outputBuffer
  }

  /** Class to create socket channel with compression */
  private class CompressionChannelFactory(compressionLevel: Int)
    extends NioClientSocketChannelFactory {

    override def newChannel(pipeline: ChannelPipeline): SocketChannel = {
      val encoder = new ZlibEncoder(compressionLevel)
      pipeline.addFirst("deflater", encoder)
      pipeline.addFirst("inflater", new ZlibDecoder())
      super.newChannel(pipeline)
    }
  }
}

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

package org.apache.spark.launcher;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import static org.apache.spark.launcher.LauncherProtocol.*;

public class LauncherServerSuite extends BaseSuite {

  @Test
  public void testLauncherServerReuse() throws Exception {
    ChildProcAppHandle handle1 = null;
    ChildProcAppHandle handle2 = null;
    ChildProcAppHandle handle3 = null;

    try {
      handle1 = LauncherServer.newAppHandle();
      handle2 = LauncherServer.newAppHandle();
      LauncherServer server1 = handle1.getServer();
      assertSame(server1, handle2.getServer());

      handle1.kill();
      handle2.kill();

      handle3 = LauncherServer.newAppHandle();
      assertNotSame(server1, handle3.getServer());

      handle3.kill();

      assertNull(LauncherServer.getServerInstance());
    } finally {
      kill(handle1);
      kill(handle2);
      kill(handle3);
    }
  }

  @Test
  public void testCommunication() throws Exception {
    ChildProcAppHandle handle = LauncherServer.newAppHandle();
    TestClient client = null;
    try {
      Socket s = new Socket(InetAddress.getLoopbackAddress(),
        LauncherServer.getServerInstance().getPort());

      final Object waitLock = new Object();
      handle.addListener(new SparkAppHandle.Listener() {
        @Override
        public void stateChanged(SparkAppHandle handle) {
          wakeUp();
        }

        @Override
        public void infoChanged(SparkAppHandle handle) {
          wakeUp();
        }

        private void wakeUp() {
          synchronized (waitLock) {
            waitLock.notifyAll();
          }
        }
      });

      client = new TestClient(s);
      synchronized (waitLock) {
        client.send(new Hello(handle.getSecret(), "1.4.0"));
        waitLock.wait(TimeUnit.SECONDS.toMillis(10));
      }

      // Make sure the server matched the client to the handle.
      assertNotNull(handle.getConnection());

      synchronized (waitLock) {
        client.send(new SetAppId("app-id"));
        waitLock.wait(TimeUnit.SECONDS.toMillis(10));
      }
      assertEquals("app-id", handle.getAppId());

      synchronized (waitLock) {
        client.send(new SetState(SparkAppHandle.State.RUNNING));
        waitLock.wait(TimeUnit.SECONDS.toMillis(10));
      }
      assertEquals(SparkAppHandle.State.RUNNING, handle.getState());

      handle.stop();
      Message stopMsg = client.inbound.poll(10, TimeUnit.SECONDS);
      assertTrue(stopMsg instanceof Stop);
    } finally {
      kill(handle);
      close(client);
      client.clientThread.join();
    }
  }

  @Test
  public void testTimeout() throws Exception {
    ChildProcAppHandle handle = null;
    TestClient client = null;
    try {
      // LauncherServer will immediately close the server-side socket when the timeout is set
      // to 0.
      SparkLauncher.setConfig(SparkLauncher.CHILD_CONNECTION_TIMEOUT, "0");

      handle = LauncherServer.newAppHandle();

      Socket s = new Socket(InetAddress.getLoopbackAddress(),
        LauncherServer.getServerInstance().getPort());
      client = new TestClient(s);

      // Try a few times since the client-side socket may not reflect the server-side close
      // immediately.
      boolean helloSent = false;
      int maxTries = 10;
      for (int i = 0; i < maxTries; i++) {
        try {
          if (!helloSent) {
            client.send(new Hello(handle.getSecret(), "1.4.0"));
            helloSent = true;
          } else {
            client.send(new SetAppId("appId"));
          }
          fail("Expected exception caused by connection timeout.");
        } catch (IllegalStateException | IOException e) {
          // Expected.
          break;
        } catch (AssertionError e) {
          if (i < maxTries - 1) {
            Thread.sleep(100);
          } else {
            throw new AssertionError("Test failed after " + maxTries + " attempts.", e);
          }
        }
      }
    } finally {
      SparkLauncher.launcherConfig.remove(SparkLauncher.CHILD_CONNECTION_TIMEOUT);
      kill(handle);
      close(client);
    }
  }

  private void kill(SparkAppHandle handle) {
    if (handle != null) {
      handle.kill();
    }
  }

  private void close(Closeable c) {
    if (c != null) {
      try {
        c.close();
      } catch (Exception e) {
        // no-op.
      }
    }
  }

  private static class TestClient extends LauncherConnection {

    final BlockingQueue<Message> inbound;
    final Thread clientThread;

    TestClient(Socket s) throws IOException {
      super(s);
      this.inbound = new LinkedBlockingQueue<Message>();
      this.clientThread = new Thread(this);
      clientThread.setName("TestClient");
      clientThread.setDaemon(true);
      clientThread.start();
    }

    @Override
    protected void handle(Message msg) throws IOException {
      inbound.offer(msg);
    }

  }

}

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

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handle implementation for monitoring apps started as a child process.
 */
class ChildProcAppHandle implements SparkAppHandle {

  private static final Logger LOG = Logger.getLogger(ChildProcAppHandle.class.getName());
  private static final ThreadFactory REDIRECTOR_FACTORY =
    new NamedThreadFactory("launcher-proc-%d");

  private final String secret;
  private final LauncherServer server;

  private Process childProc;
  private boolean disposed;
  private LauncherConnection connection;
  private List<Listener> listeners;
  private State state;
  private String appId;
  private OutputRedirector redirector;

  ChildProcAppHandle(String secret, LauncherServer server) {
    this.secret = secret;
    this.server = server;
    this.state = State.UNKNOWN;
  }

  @Override
  public synchronized void addListener(Listener l) {
    if (listeners == null) {
      listeners = new ArrayList<>();
    }
    listeners.add(l);
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public String getAppId() {
    return appId;
  }

  @Override
  public void stop() {
    CommandBuilderUtils.checkState(connection != null, "Application is still not connected.");
    try {
      connection.send(new LauncherProtocol.Stop());
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public synchronized void disconnect() {
    if (!disposed) {
      disposed = true;
      if (connection != null) {
        try {
          connection.close();
        } catch (IOException ioe) {
          // no-op.
        }
      }
      server.unregister(this);
      if (redirector != null) {
        redirector.stop();
      }
    }
  }

  @Override
  public synchronized void kill() {
    if (!disposed) {
      disconnect();
    }
    if (childProc != null) {
      try {
        childProc.exitValue();
      } catch (IllegalThreadStateException e) {
        // Child is still alive. Try to use Java 8's "destroyForcibly()" if available,
        // fall back to the old API if it's not there.
        try {
          Method destroy = childProc.getClass().getMethod("destroyForcibly");
          destroy.invoke(childProc);
        } catch (Exception inner) {
          childProc.destroy();
        }
      } finally {
        childProc = null;
      }
    }
  }

  String getSecret() {
    return secret;
  }

  void setChildProc(Process childProc, String loggerName) {
    this.childProc = childProc;
    this.redirector = new OutputRedirector(childProc.getInputStream(), loggerName,
      REDIRECTOR_FACTORY);
  }

  void setConnection(LauncherConnection connection) {
    this.connection = connection;
  }

  LauncherServer getServer() {
    return server;
  }

  LauncherConnection getConnection() {
    return connection;
  }

  void setState(State s) {
    if (!state.isFinal()) {
      state = s;
      fireEvent(false);
    } else {
      LOG.log(Level.WARNING, "Backend requested transition from final state {0} to {1}.",
        new Object[] { state, s });
    }
  }

  void setAppId(String appId) {
    this.appId = appId;
    fireEvent(true);
  }

  private synchronized void fireEvent(boolean isInfoChanged) {
    if (listeners != null) {
      for (Listener l : listeners) {
        if (isInfoChanged) {
          l.infoChanged(this);
        } else {
          l.stateChanged(this);
        }
      }
    }
  }

}

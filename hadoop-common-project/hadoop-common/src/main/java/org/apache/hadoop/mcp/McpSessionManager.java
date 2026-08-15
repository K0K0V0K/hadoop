/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mcp;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * In-memory MCP session lifecycle tracker for Streamable HTTP transport.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
final class McpSessionManager {

  static final long DEFAULT_SESSION_IDLE_TIMEOUT_MS = 30L * 60L * 1000L;

  enum State {
    /** {@code initialize} succeeded; waiting for {@code notifications/initialized}. */
    AWAITING_INITIALIZED,
    /** Client sent {@code notifications/initialized}; normal operations allowed. */
    OPERATING
  }

  static final class Session {
    private final String sessionId;
    private volatile State state;
    private volatile long lastAccessMs = System.currentTimeMillis();

    private Session(String sessionId, State state) {
      this.sessionId = sessionId;
      this.state = state;
    }

    String sessionId() {
      return sessionId;
    }

    State state() {
      return state;
    }

    void setState(State newState) {
      this.state = newState;
    }

    void touch(long nowMs) {
      lastAccessMs = nowMs;
    }

    boolean isExpired(long nowMs, long idleTimeoutMs) {
      return nowMs - lastAccessMs > idleTimeoutMs;
    }
  }

  private final ConcurrentMap<String, Session> sessions = new ConcurrentHashMap<>();
  private final long sessionIdleTimeoutMs;

  McpSessionManager() {
    this(DEFAULT_SESSION_IDLE_TIMEOUT_MS);
  }

  McpSessionManager(long sessionIdleTimeoutMs) {
    this.sessionIdleTimeoutMs = sessionIdleTimeoutMs;
  }

  Session createSession() {
    evictExpiredSessions();
    String sessionId = UUID.randomUUID().toString();
    Session session = new Session(sessionId, State.AWAITING_INITIALIZED);
    sessions.put(sessionId, session);
    return session;
  }

  Session getSession(String sessionId) {
    if (sessionId == null || sessionId.isEmpty()) {
      return null;
    }
    Session session = sessions.get(sessionId);
    if (session == null) {
      return null;
    }
    long nowMs = System.currentTimeMillis();
    if (session.isExpired(nowMs, sessionIdleTimeoutMs)) {
      sessions.remove(sessionId, session);
      return null;
    }
    session.touch(nowMs);
    return session;
  }

  boolean markOperating(String sessionId) {
    Session session = getSession(sessionId);
    if (session == null || session.state() != State.AWAITING_INITIALIZED) {
      return false;
    }
    session.setState(State.OPERATING);
    return true;
  }

  void evictExpiredSessions() {
    long nowMs = System.currentTimeMillis();
    sessions.entrySet().removeIf(entry ->
        entry.getValue().isExpired(nowMs, sessionIdleTimeoutMs));
  }
}

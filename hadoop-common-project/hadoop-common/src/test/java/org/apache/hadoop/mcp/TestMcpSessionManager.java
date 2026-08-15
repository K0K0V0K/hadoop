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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TestMcpSessionManager {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void testDuplicateRequestIdsRejected() throws Exception {
    McpSessionManager manager = new McpSessionManager();
    McpSessionManager.Session session = manager.createSession();

    assertTrue(manager.registerRequestId(session.sessionId(),
        OBJECT_MAPPER.readTree("1")));
    assertFalse(manager.registerRequestId(session.sessionId(),
        OBJECT_MAPPER.readTree("1")));
  }

  @Test
  public void testCreateSessionRegistersInitialRequestId() throws Exception {
    McpSessionManager manager = new McpSessionManager();
    McpSessionManager.Session session = manager.createSession(
        OBJECT_MAPPER.readTree("1"));

    assertFalse(manager.registerRequestId(session.sessionId(),
        OBJECT_MAPPER.readTree("1")));
  }

  @Test
  public void testExpiredSessionEvicted() throws Exception {
    McpSessionManager manager = new McpSessionManager(120, 1);
    McpSessionManager.Session session = manager.createSession();
    String sessionId = session.sessionId();

    Thread.sleep(10);

    assertNull(manager.getSession(sessionId));
    manager.evictExpiredSessions();
    assertNull(manager.getSession(sessionId));
  }

  @Test
  public void testActiveSessionRefreshedOnAccess() throws Exception {
    McpSessionManager manager = new McpSessionManager(120, 50);
    McpSessionManager.Session session = manager.createSession();
    String sessionId = session.sessionId();

    Thread.sleep(30);
    assertTrue(manager.getSession(sessionId) != null);

    Thread.sleep(30);
    assertTrue(manager.getSession(sessionId) != null);
  }

  @Test
  public void testToolCallRateLimit() {
    McpSessionManager manager = new McpSessionManager(2);
    McpSessionManager.Session session = manager.createSession();

    assertTrue(manager.tryAcquireToolCall(session.sessionId()));
    assertTrue(manager.tryAcquireToolCall(session.sessionId()));
    assertTrue(!manager.tryAcquireToolCall(session.sessionId()));
  }

  @Test
  public void testMarkOperatingTransitionsState() {
    McpSessionManager manager = new McpSessionManager();
    McpSessionManager.Session session = manager.createSession();
    String sessionId = session.sessionId();

    assertTrue(manager.markOperating(sessionId));
    assertTrue(manager.getSession(sessionId).state() == McpSessionManager.State.OPERATING);
    assertTrue(!manager.markOperating(sessionId));
  }
}

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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * MCP session lifecycle checks for Streamable HTTP transport.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
final class McpSessionLifecycle {

  private static final String INITIALIZED_NOTIFICATION = "notifications/initialized";

  private final McpSessionManager sessionManager;
  private final McpJsonRpcResponses responses;

  McpSessionLifecycle(McpSessionManager sessionManager, McpJsonRpcResponses responses) {
    this.sessionManager = sessionManager;
    this.responses = responses;
  }

  McpHttpResponse validateActiveSession(McpCallContext context, String method) {
    if (McpRequestHandler.METHOD_INITIALIZE.equals(method)
        || McpRequestHandler.METHOD_PING.equals(method)) {
      return null;
    }
    String sessionId = context.getSessionId();
    if (sessionId == null || sessionId.isEmpty()) {
      return null;
    }
    if (sessionManager.getSession(sessionId) != null) {
      return null;
    }
    if (context.getRequest() != null) {
      return McpHttpResponse.notFound(McpJsonRpc.TRANSPORT_SESSION_NOT_FOUND_MESSAGE);
    }
    return null;
  }

  McpHttpResponse checkDuplicateRequestId(McpCallContext context, String method,
      JsonNode idNode) {
    if (McpRequestHandler.METHOD_INITIALIZE.equals(method)) {
      return null;
    }
    String sessionId = context.getSessionId();
    if (sessionId == null || sessionId.isEmpty()) {
      return null;
    }
    if (!sessionManager.registerRequestId(sessionId, idNode)) {
      return responses.error(idNode, McpJsonRpc.INVALID_REQUEST,
          McpJsonRpc.DUPLICATE_REQUEST_ID_MESSAGE);
    }
    return null;
  }

  McpHttpResponse handleNotification(String method, McpCallContext context) {
    if (INITIALIZED_NOTIFICATION.equals(method)) {
      if (!sessionManager.markOperating(context.getSessionId())) {
        return McpHttpResponse.badRequest(McpJsonRpc.TRANSPORT_INVALID_NOTIFICATION_MESSAGE);
      }
    }
    return McpHttpResponse.notification();
  }

  McpHttpResponse withOperatingSession(McpCallContext context, JsonNode idNode,
      McpHttpResponse response) {
    McpHttpResponse lifecycleError = checkOperatingSession(context, idNode);
    return lifecycleError != null ? lifecycleError : response;
  }

  McpHttpResponse checkOperatingSession(McpCallContext context, JsonNode idNode) {
    String sessionId = context.getSessionId();
    McpSessionManager.Session session = sessionManager.getSession(sessionId);
    if (session == null) {
      return responses.error(idNode, McpJsonRpc.INVALID_REQUEST,
          McpJsonRpc.LIFECYCLE_UNKNOWN_SESSION_MESSAGE);
    }
    if (session.state() == McpSessionManager.State.AWAITING_INITIALIZED) {
      return responses.error(idNode, McpJsonRpc.INVALID_REQUEST,
          McpJsonRpc.LIFECYCLE_AWAITING_INITIALIZED_MESSAGE);
    }
    if (session.state() != McpSessionManager.State.OPERATING) {
      return responses.error(idNode, McpJsonRpc.INVALID_REQUEST,
          McpJsonRpc.LIFECYCLE_NOT_INITIALIZED_MESSAGE);
    }
    return null;
  }
}

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

import java.net.URI;

import javax.servlet.http.HttpServletRequest;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Streamable HTTP transport checks required by MCP 2025-06-18.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
final class McpHttpTransportValidator {

  static final String PROTOCOL_VERSION_HEADER = "MCP-Protocol-Version";

  private McpHttpTransportValidator() {
  }

  static McpHttpResponse validate(McpCallContext context, JsonNode requestNode) {
    if (context.getRequest() == null) {
      return null;
    }
    HttpServletRequest request = context.getRequest();
    if (!isAllowedOrigin(request.getHeader("Origin"))) {
      return McpHttpResponse.forbidden(McpJsonRpc.TRANSPORT_FORBIDDEN_ORIGIN_MESSAGE);
    }

    JsonNode methodNode = requestNode.get("method");
    if (methodNode == null || !methodNode.isTextual()) {
      return null;
    }
    String method = methodNode.asText();
    if (McpRequestHandler.METHOD_INITIALIZE.equals(method)) {
      return null;
    }

    if (!McpRequestHandler.METHOD_PING.equals(method)
        && isBlank(context.getSessionId())) {
      return McpHttpResponse.badRequest(McpJsonRpc.TRANSPORT_MISSING_SESSION_MESSAGE);
    }

    return validateProtocolVersionHeader(request);
  }

  static McpHttpResponse validateProtocolVersionHeader(HttpServletRequest request) {
    String version = request.getHeader(PROTOCOL_VERSION_HEADER);
    if (isBlank(version)) {
      return null;
    }
    if (!McpJsonRpc.PROTOCOL_VERSION.equals(version)) {
      return McpHttpResponse.badRequest(
          McpJsonRpc.TRANSPORT_UNSUPPORTED_PROTOCOL_VERSION_MESSAGE);
    }
    return null;
  }

  static boolean isAllowedOrigin(String origin) {
    if (isBlank(origin)) {
      return true;
    }
    try {
      URI uri = URI.create(origin);
      if (!"http".equalsIgnoreCase(uri.getScheme())
          && !"https".equalsIgnoreCase(uri.getScheme())) {
        return false;
      }
      String host = uri.getHost();
      if (host == null) {
        return false;
      }
      return "localhost".equalsIgnoreCase(host)
          || "127.0.0.1".equals(host)
          || "[::1]".equals(host);
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  private static boolean isBlank(String value) {
    return value == null || value.isEmpty();
  }
}

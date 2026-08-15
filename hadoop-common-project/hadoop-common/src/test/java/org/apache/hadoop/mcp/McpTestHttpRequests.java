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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

/**
 * Minimal {@link HttpServletRequest} stub for MCP transport tests.
 */
final class McpTestHttpRequests {

  private McpTestHttpRequests() {
  }

  static HttpServletRequest withSessionId(String sessionId) {
    return withHeaders(sessionId, null, null);
  }

  static HttpServletRequest withHeaders(String sessionId, String protocolVersion,
      String origin) {
    Map<String, String> headers = new HashMap<>();
    if (sessionId != null) {
      headers.put(McpRequestHandler.SESSION_HEADER, sessionId);
    }
    if (protocolVersion != null) {
      headers.put(McpHttpTransportValidator.PROTOCOL_VERSION_HEADER, protocolVersion);
    }
    if (origin != null) {
      headers.put("Origin", origin);
    }
    return newRequest(headers);
  }

  private static HttpServletRequest newRequest(Map<String, String> headers) {
    InvocationHandler handler = new InvocationHandler() {
      @Override
      public Object invoke(Object proxy, Method method, Object[] args) {
        if ("getHeader".equals(method.getName()) && args.length == 1) {
          return headers.get(args[0]);
        }
        Class<?> returnType = method.getReturnType();
        if (returnType == boolean.class) {
          return false;
        }
        if (returnType == int.class) {
          return 0;
        }
        if (returnType == long.class) {
          return 0L;
        }
        return null;
      }
    };
    return (HttpServletRequest) Proxy.newProxyInstance(
        HttpServletRequest.class.getClassLoader(),
        new Class<?>[] {HttpServletRequest.class},
        handler);
  }
}

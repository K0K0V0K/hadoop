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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TestMcpHttpServlet {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final JacksonMcpJsonMapper JSON_MAPPER =
      new JacksonMcpJsonMapper(OBJECT_MAPPER);

  private static final McpCallContext NO_HTTP_CONTEXT = new McpCallContext(null);

  private static McpCallContext sessionContext(String sessionId) {
    return new McpCallContext(McpTestHttpRequests.withSessionId(sessionId));
  }

  private static McpHttpResponse handle(McpServer server, JsonNode request) {
    return server.getRequestHandler().handle(request, NO_HTTP_CONTEXT);
  }

  private static McpHttpResponse handle(McpRequestHandler handler, JsonNode request) {
    return handler.handle(request, NO_HTTP_CONTEXT);
  }

  private static String initializeSession(McpServer server) throws Exception {
    JsonNode request = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
            + "\"params\":{\"protocolVersion\":\"2025-06-18\",\"capabilities\":{},"
            + "\"clientInfo\":{\"name\":\"test\",\"version\":\"1.0\"}}}");
    McpHttpResponse response = handle(server, request);
    assertEquals(200, response.status());
    String sessionId = response.headers().get(McpRequestHandler.SESSION_HEADER);
    assertNotNull(sessionId);
    return sessionId;
  }

  private static void sendInitialized(McpServer server, String sessionId) throws Exception {
    JsonNode request = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}");
    McpHttpResponse response = server.getRequestHandler().handle(request,
        sessionContext(sessionId));
    assertEquals(202, response.status());
  }

  @Test
  public void testToolsList() throws Exception {
    McpServer server = McpServer.sync(JSON_MAPPER)
        .serverInfo("test-server", "1.0")
        .capabilities(McpSchema.ServerCapabilities.withTools())
        .toolCall(McpSchema.Tool.of("echo", "Echo input", JSON_MAPPER,
                "{\"type\":\"object\",\"properties\":{}}"),
            (context, args) -> McpSchema.CallToolResult.text("ok"))
        .build();

    String sessionId = initializeSession(server);
    sendInitialized(server, sessionId);

    JsonNode request = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\",\"params\":{}}");
    McpHttpResponse response = server.getRequestHandler().handle(request,
        sessionContext(sessionId));

    assertEquals(200, response.status());
    JsonNode body = response.body();
    assertEquals("2.0", body.get("jsonrpc").asText());
    assertEquals(2, body.get("id").asInt());
    assertTrue(body.get("result").get("tools").isArray());
    assertEquals("echo", body.get("result").get("tools").get(0).get("name").asText());
  }

  @Test
  public void testToolsListBeforeInitializeRejected() throws Exception {
    McpServer server = McpServer.sync(JSON_MAPPER)
        .serverInfo("test-server", "1.0")
        .capabilities(McpSchema.ServerCapabilities.withTools())
        .build();

    JsonNode request = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\",\"params\":{}}");
    McpHttpResponse response = handle(server, request);

    assertEquals(McpJsonRpc.INVALID_REQUEST, response.body().get("error").get("code").asInt());
    assertEquals(McpJsonRpc.LIFECYCLE_UNKNOWN_SESSION_MESSAGE,
        response.body().get("error").get("message").asText());
  }

  @Test
  public void testToolsListBeforeInitializedNotificationRejected() throws Exception {
    McpServer server = McpServer.sync(JSON_MAPPER)
        .serverInfo("test-server", "1.0")
        .capabilities(McpSchema.ServerCapabilities.withTools())
        .build();

    String sessionId = initializeSession(server);

    JsonNode request = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\",\"params\":{}}");
    McpHttpResponse response = server.getRequestHandler().handle(request,
        sessionContext(sessionId));

    assertEquals(McpJsonRpc.INVALID_REQUEST, response.body().get("error").get("code").asInt());
    assertEquals(McpJsonRpc.LIFECYCLE_AWAITING_INITIALIZED_MESSAGE,
        response.body().get("error").get("message").asText());
  }

  @Test
  public void testToolsCall() throws Exception {
    McpServer server = McpServer.sync(JSON_MAPPER)
        .serverInfo("test-server", "1.0")
        .capabilities(McpSchema.ServerCapabilities.withTools())
        .toolCall(McpSchema.Tool.of("echo", "Echo input", JSON_MAPPER,
                "{\"type\":\"object\",\"properties\":{}}"),
            (context, args) -> McpSchema.CallToolResult.text("{\"value\":\"test\"}"))
        .build();

    String sessionId = initializeSession(server);
    sendInitialized(server, sessionId);

    JsonNode request = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\","
            + "\"params\":{\"name\":\"echo\",\"arguments\":{}}}");
    McpHttpResponse response = server.getRequestHandler().handle(request,
        sessionContext(sessionId));

    JsonNode body = response.body();
    assertEquals("test", OBJECT_MAPPER.readTree(
        body.get("result").get("content").get(0).get("text").asText()).get("value").asText());
  }

  @Test
  public void testUnknownToolReturnsError() throws Exception {
    McpServer server = McpServer.sync(JSON_MAPPER)
        .serverInfo("test-server", "1.0")
        .build();

    String sessionId = initializeSession(server);
    sendInitialized(server, sessionId);

    JsonNode request = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/call\","
            + "\"params\":{\"name\":\"missing\",\"arguments\":{}}}");
    McpHttpResponse response = server.getRequestHandler().handle(request,
        sessionContext(sessionId));

    JsonNode body = response.body();
    assertNotNull(body.get("error"));
    assertEquals(McpJsonRpc.INVALID_PARAMS, body.get("error").get("code").asInt());
  }

  @Test
  public void testInvalidEnvelopeRejectedByHandler() throws Exception {
    McpServer server = McpServer.sync(JSON_MAPPER)
        .serverInfo("test-server", "1.0")
        .build();

    JsonNode request = OBJECT_MAPPER.readTree("{\"id\":1,\"method\":\"tools/list\"}");
    McpHttpResponse response = handle(server, request);

    JsonNode body = response.body();
    assertEquals(McpJsonRpc.INVALID_REQUEST, body.get("error").get("code").asInt());
    assertEquals(McpJsonRpc.INVALID_REQUEST_MESSAGE,
        body.get("error").get("message").asText());
    assertEquals(1, body.get("id").asInt());
  }

  @Test
  public void testInitializeReturns20250618ProtocolVersion() throws Exception {
    McpServer server = McpServer.sync(JSON_MAPPER)
        .serverInfo("test-server", "1.0")
        .build();

    JsonNode request = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
            + "\"params\":{\"protocolVersion\":\"2024-11-05\",\"capabilities\":{},"
            + "\"clientInfo\":{\"name\":\"test\",\"version\":\"1.0\"}}}");
    McpHttpResponse response = handle(server, request);

    JsonNode body = response.body();
    assertEquals(McpJsonRpc.PROTOCOL_VERSION,
        body.get("result").get("protocolVersion").asText());
    assertNotNull(response.headers().get(McpRequestHandler.SESSION_HEADER));
  }

  @Test
  public void testStringIdIsAccepted() throws Exception {
    McpServer server = McpServer.sync(JSON_MAPPER)
        .serverInfo("test-server", "1.0")
        .capabilities(McpSchema.ServerCapabilities.withTools())
        .build();

    String sessionId = initializeSession(server);
    sendInitialized(server, sessionId);

    JsonNode request = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"id\":\"req-1\",\"method\":\"tools/list\",\"params\":{}}");
    McpHttpResponse response = server.getRequestHandler().handle(request,
        sessionContext(sessionId));

    JsonNode body = response.body();
    assertEquals("req-1", body.get("id").asText());
    assertTrue(body.has("result"));
  }

  @Test
  public void testInitializedNotificationCompletesLifecycle() throws Exception {
    McpServer server = McpServer.sync(JSON_MAPPER)
        .serverInfo("test-server", "1.0")
        .build();

    String sessionId = initializeSession(server);

    JsonNode request = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}");
    McpHttpResponse response = server.getRequestHandler().handle(request,
        sessionContext(sessionId));

    assertEquals(202, response.status());
    assertTrue(response.body() == null);
  }

  @Test
  public void testInitializedNotificationWithoutSessionReturnsBadRequest() throws Exception {
    McpServer server = McpServer.sync(JSON_MAPPER)
        .serverInfo("test-server", "1.0")
        .build();

    JsonNode request = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}");
    McpHttpResponse response = handle(server, request);

    assertEquals(400, response.status());
    assertEquals(McpJsonRpc.TRANSPORT_INVALID_NOTIFICATION_MESSAGE,
        response.body().get("error").get("message").asText());
  }

  @Test
  public void testPingAllowedBeforeInitialize() throws Exception {
    McpServer server = McpServer.sync(JSON_MAPPER)
        .serverInfo("test-server", "1.0")
        .build();

    JsonNode request = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}");
    McpHttpResponse response = handle(server, request);

    assertEquals(200, response.status());
    assertTrue(response.body().has("result"));
    assertTrue(response.body().get("result").isObject());
    assertEquals(0, response.body().get("result").size());
  }

  @Test
  public void testForbiddenOriginRejected() throws Exception {
    McpServer server = McpServer.sync(JSON_MAPPER)
        .serverInfo("test-server", "1.0")
        .build();

    JsonNode request = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"id\":13,\"method\":\"ping\"}");
    McpHttpResponse response = server.getRequestHandler().handle(request,
        new McpCallContext(McpTestHttpRequests.withHeaders(null, null,
            "https://evil.example.com")));

    assertEquals(403, response.status());
    assertEquals(McpJsonRpc.TRANSPORT_FORBIDDEN_ORIGIN_MESSAGE,
        response.body().get("error").get("message").asText());
  }

  @Test
  public void testMissingSessionHeaderRejected() throws Exception {
    McpServer server = McpServer.sync(JSON_MAPPER)
        .serverInfo("test-server", "1.0")
        .capabilities(McpSchema.ServerCapabilities.withTools())
        .build();

    JsonNode request = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"id\":14,\"method\":\"tools/list\",\"params\":{}}");
    McpHttpResponse response = server.getRequestHandler().handle(request,
        new McpCallContext(McpTestHttpRequests.withHeaders(null, null, null)));

    assertEquals(400, response.status());
    assertEquals(McpJsonRpc.TRANSPORT_MISSING_SESSION_MESSAGE,
        response.body().get("error").get("message").asText());
  }

  @Test
  public void testInvalidProtocolVersionRejected() throws Exception {
    McpServer server = McpServer.sync(JSON_MAPPER)
        .serverInfo("test-server", "1.0")
        .build();

    String sessionId = initializeSession(server);

    JsonNode request = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}");
    McpHttpResponse response = server.getRequestHandler().handle(request,
        new McpCallContext(McpTestHttpRequests.withHeaders(sessionId, "2099-01-01", null)));

    assertEquals(400, response.status());
    assertEquals(McpJsonRpc.TRANSPORT_UNSUPPORTED_PROTOCOL_VERSION_MESSAGE,
        response.body().get("error").get("message").asText());
  }

  @Test
  public void testToolInputValidationRejected() throws Exception {
    McpServer server = McpServer.sync(JSON_MAPPER)
        .serverInfo("test-server", "1.0")
        .capabilities(McpSchema.ServerCapabilities.withTools())
        .toolCall(McpSchema.Tool.of("echo", "Echo input", JSON_MAPPER,
            "{\"type\":\"object\",\"required\":[\"name\"],"
                + "\"properties\":{\"name\":{\"type\":\"string\"}}}"),
            (context, args) -> McpSchema.CallToolResult.text("ok"))
        .build();

    String sessionId = initializeSession(server);
    sendInitialized(server, sessionId);

    JsonNode request = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"id\":11,\"method\":\"tools/call\","
            + "\"params\":{\"name\":\"echo\",\"arguments\":{}}}");
    McpHttpResponse response = server.getRequestHandler().handle(request,
        sessionContext(sessionId));

    JsonNode body = response.body();
    assertEquals(McpJsonRpc.INVALID_PARAMS, body.get("error").get("code").asInt());
    assertEquals("Missing required argument: name",
        body.get("error").get("message").asText());
  }

  @Test
  public void testToolOutputSanitized() throws Exception {
    McpServer server = McpServer.sync(JSON_MAPPER)
        .serverInfo("test-server", "1.0")
        .capabilities(McpSchema.ServerCapabilities.withTools())
        .toolCall(McpSchema.Tool.of("echo", "Echo input", JSON_MAPPER,
                "{\"type\":\"object\",\"properties\":{}}"),
            (context, args) -> McpSchema.CallToolResult.text("ok\u0001"))
        .build();

    String sessionId = initializeSession(server);
    sendInitialized(server, sessionId);

    JsonNode request = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"id\":12,\"method\":\"tools/call\","
            + "\"params\":{\"name\":\"echo\",\"arguments\":{}}}");
    McpHttpResponse response = server.getRequestHandler().handle(request,
        sessionContext(sessionId));

    JsonNode body = response.body();
    assertEquals("ok", body.get("result").get("content").get(0).get("text").asText());
  }

  @Test
  public void testToolCallRateLimitRejected() throws Exception {
    McpSessionManager sessionManager = new McpSessionManager(1);
    McpRequestHandler handler = new McpRequestHandler(OBJECT_MAPPER, "test-server", "1.0",
        McpSchema.ServerCapabilities.withTools(),
        Collections.singletonMap("echo", new McpServer.RegisteredTool(
            McpSchema.Tool.of("echo", "Echo input", JSON_MAPPER,
                "{\"type\":\"object\",\"properties\":{}}"),
            (context, args) -> McpSchema.CallToolResult.text("ok"))),
        sessionManager);

    JsonNode initRequest = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
            + "\"params\":{\"protocolVersion\":\"2025-06-18\",\"capabilities\":{},"
            + "\"clientInfo\":{\"name\":\"test\",\"version\":\"1.0\"}}}");
    McpHttpResponse initResponse = handle(handler, initRequest);
    String sessionId = initResponse.headers().get(McpRequestHandler.SESSION_HEADER);

    JsonNode initializedRequest = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}");
    assertEquals(202, handler.handle(initializedRequest, sessionContext(sessionId)).status());

    JsonNode firstCall = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\","
            + "\"params\":{\"name\":\"echo\",\"arguments\":{}}}");
    assertEquals(200, handler.handle(firstCall, sessionContext(sessionId)).status());

    JsonNode secondCall = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/call\","
            + "\"params\":{\"name\":\"echo\",\"arguments\":{}}}");
    McpHttpResponse response = handler.handle(secondCall, sessionContext(sessionId));

    JsonNode body = response.body();
    assertEquals(McpJsonRpc.TOOL_CALL_RATE_LIMIT, body.get("error").get("code").asInt());
    assertEquals(McpJsonRpc.TOOL_CALL_RATE_LIMIT_MESSAGE,
        body.get("error").get("message").asText());
  }

  @Test
  public void testDuplicateRequestIdRejected() throws Exception {
    McpServer server = McpServer.sync(JSON_MAPPER)
        .serverInfo("test-server", "1.0")
        .capabilities(McpSchema.ServerCapabilities.withTools())
        .build();

    String sessionId = initializeSession(server);
    sendInitialized(server, sessionId);

    JsonNode request = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\",\"params\":{}}");
    McpHttpResponse response = server.getRequestHandler().handle(request,
        sessionContext(sessionId));

    JsonNode body = response.body();
    assertEquals(McpJsonRpc.INVALID_REQUEST, body.get("error").get("code").asInt());
    assertEquals(McpJsonRpc.DUPLICATE_REQUEST_ID_MESSAGE,
        body.get("error").get("message").asText());
  }

  @Test
  public void testExpiredSessionReturnsNotFoundOverHttp() throws Exception {
    McpSessionManager sessionManager = new McpSessionManager(120, 1);
    McpRequestHandler handler = new McpRequestHandler(OBJECT_MAPPER, "test-server", "1.0",
        McpSchema.ServerCapabilities.withTools(),
        Collections.<String, McpServer.RegisteredTool>emptyMap(),
        sessionManager);

    JsonNode initRequest = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
            + "\"params\":{\"protocolVersion\":\"2025-06-18\",\"capabilities\":{},"
            + "\"clientInfo\":{\"name\":\"test\",\"version\":\"1.0\"}}}");
    McpHttpResponse initResponse = handle(handler, initRequest);
    String sessionId = initResponse.headers().get(McpRequestHandler.SESSION_HEADER);

    Thread.sleep(10);

    JsonNode listRequest = OBJECT_MAPPER.readTree(
        "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\",\"params\":{}}");
    McpHttpResponse response = handler.handle(listRequest, sessionContext(sessionId));

    assertEquals(404, response.status());
    assertEquals(McpJsonRpc.TRANSPORT_SESSION_NOT_FOUND_MESSAGE,
        response.body().get("error").get("message").asText());
  }
}

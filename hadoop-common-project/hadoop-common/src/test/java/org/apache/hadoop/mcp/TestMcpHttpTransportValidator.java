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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class TestMcpHttpTransportValidator {

  @Test
  public void testMissingOriginAllowed() {
    assertTrue(McpHttpTransportValidator.isAllowedOrigin(null));
    assertTrue(McpHttpTransportValidator.isAllowedOrigin(""));
  }

  @Test
  public void testLocalhostOriginAllowed() {
    assertTrue(McpHttpTransportValidator.isAllowedOrigin("http://localhost:3000"));
    assertTrue(McpHttpTransportValidator.isAllowedOrigin("https://127.0.0.1:8080"));
    assertTrue(McpHttpTransportValidator.isAllowedOrigin("http://[::1]:8080"));
  }

  @Test
  public void testRemoteOriginRejected() {
    assertFalse(McpHttpTransportValidator.isAllowedOrigin("https://evil.example.com"));
    assertFalse(McpHttpTransportValidator.isAllowedOrigin("not-a-uri"));
  }

  @Test
  public void testInvalidProtocolVersionReturnsBadRequest() {
    McpHttpResponse response = McpHttpTransportValidator.validateProtocolVersionHeader(
        McpTestHttpRequests.withHeaders(null, null, null));
    assertNull(response);

    response = McpHttpTransportValidator.validateProtocolVersionHeader(
        McpTestHttpRequests.withHeaders(null, "2099-01-01", null));
    assertEquals(400, response.status());
    assertEquals(McpJsonRpc.TRANSPORT_UNSUPPORTED_PROTOCOL_VERSION_MESSAGE,
        response.body().get("error").get("message").asText());
  }

  @Test
  public void testOldProtocolVersionRejected() {
    McpHttpResponse response = McpHttpTransportValidator.validateProtocolVersionHeader(
        McpTestHttpRequests.withHeaders(null, "2024-11-05", null));
    assertEquals(400, response.status());
    assertEquals(McpJsonRpc.TRANSPORT_UNSUPPORTED_PROTOCOL_VERSION_MESSAGE,
        response.body().get("error").get("message").asText());
  }

  @Test
  public void testSupportedProtocolVersionAccepted() {
    McpHttpResponse response = McpHttpTransportValidator.validateProtocolVersionHeader(
        McpTestHttpRequests.withHeaders(null, McpJsonRpc.PROTOCOL_VERSION, null));
    assertNull(response);
  }
}

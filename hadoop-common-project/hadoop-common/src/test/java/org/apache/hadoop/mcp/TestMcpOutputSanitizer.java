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

import org.apache.hadoop.mcp.McpSchema.CallToolResult;
import org.junit.jupiter.api.Test;

public class TestMcpOutputSanitizer {

  @Test
  public void testControlCharactersStripped() {
    String dirty = "hello\u0000world\u0007";
    assertEquals("helloworld", McpOutputSanitizer.sanitizeText(dirty));
  }

  @Test
  public void testWhitespacePreserved() {
    assertEquals("line1\nline2\ttab", McpOutputSanitizer.sanitizeText("line1\nline2\ttab"));
  }

  @Test
  public void testSanitizeCallToolResult() {
    CallToolResult dirty = CallToolResult.text("ok\u0001");
    CallToolResult clean = McpOutputSanitizer.sanitize(dirty);
    assertEquals("ok", clean.content().get(0).text());
  }

  @Test
  public void testOversizedOutputTruncated() {
    char[] chars = new char[McpJsonRpc.MAX_TOOL_OUTPUT_TEXT_CHARS + 10];
    java.util.Arrays.fill(chars, 'x');
    String oversized = new String(chars);
    assertEquals(McpJsonRpc.MAX_TOOL_OUTPUT_TEXT_CHARS,
        McpOutputSanitizer.sanitizeText(oversized).length());
  }
}

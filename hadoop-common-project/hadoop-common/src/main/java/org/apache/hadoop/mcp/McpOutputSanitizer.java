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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mcp.McpSchema.CallToolResult;
import org.apache.hadoop.mcp.McpSchema.TextContent;

/**
 * Sanitizes tool output text before returning it to MCP clients.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
final class McpOutputSanitizer {

  private McpOutputSanitizer() {
  }

  static CallToolResult sanitize(CallToolResult result) {
    if (result == null || result.content().isEmpty()) {
      return result;
    }
    boolean changed = false;
    List<TextContent> sanitized = new ArrayList<>(result.content().size());
    for (TextContent content : result.content()) {
      String text = content.text();
      String clean = sanitizeText(text);
      if (!clean.equals(text)) {
        changed = true;
      }
      sanitized.add(new TextContent(clean));
    }
    if (!changed) {
      return result;
    }
    return CallToolResult.ofContent(sanitized, result.isError());
  }

  static String sanitizeText(String text) {
    if (text == null || text.isEmpty()) {
      return text == null ? "" : text;
    }
    StringBuilder builder = new StringBuilder(text.length());
    for (int i = 0; i < text.length(); i++) {
      char ch = text.charAt(i);
      if (ch == '\n' || ch == '\r' || ch == '\t' || ch >= 0x20) {
        builder.append(ch);
      }
    }
    if (builder.length() > McpJsonRpc.MAX_TOOL_OUTPUT_TEXT_CHARS) {
      return builder.substring(0, McpJsonRpc.MAX_TOOL_OUTPUT_TEXT_CHARS);
    }
    return builder.toString();
  }
}

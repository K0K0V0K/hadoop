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

/**
 * JSON-RPC 2.0 constants used by the MCP HTTP transport.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class McpJsonRpc {

  public static final String VERSION = "2.0";

  /** MCP protocol version implemented by this server. */
  public static final String PROTOCOL_VERSION = "2025-06-18";

  /** JSON-RPC 2.0 parse error. */
  public static final int PARSE_ERROR = -32700;
  /** JSON-RPC 2.0 invalid request. */
  public static final int INVALID_REQUEST = -32600;
  /** JSON-RPC 2.0 method not found. */
  public static final int METHOD_NOT_FOUND = -32601;
  /** JSON-RPC 2.0 invalid params. */
  public static final int INVALID_PARAMS = -32602;

  /** Server-defined rate limit exceeded (JSON-RPC -32000..-32099 range). */
  public static final int TOOL_CALL_RATE_LIMIT = -32003;

  public static final String PARSE_ERROR_MESSAGE = "Parse error";
  public static final String INVALID_REQUEST_MESSAGE = "Invalid Request";
  public static final String REQUEST_BODY_TOO_LARGE_MESSAGE = "Request body too large";
  public static final String LIFECYCLE_NOT_INITIALIZED_MESSAGE =
      "MCP session not initialized; send initialize first";
  public static final String LIFECYCLE_AWAITING_INITIALIZED_MESSAGE =
      "MCP session awaiting notifications/initialized";
  public static final String LIFECYCLE_UNKNOWN_SESSION_MESSAGE =
      "Unknown or missing MCP session";
  public static final String DUPLICATE_REQUEST_ID_MESSAGE = "Duplicate request id";
  public static final String TRANSPORT_FORBIDDEN_ORIGIN_MESSAGE = "Origin not allowed";
  public static final String TRANSPORT_MISSING_SESSION_MESSAGE =
      "Missing Mcp-Session-Id header";
  public static final String TRANSPORT_UNSUPPORTED_PROTOCOL_VERSION_MESSAGE =
      "Unsupported MCP-Protocol-Version";
  public static final String TRANSPORT_SESSION_NOT_FOUND_MESSAGE = "Session not found";
  public static final String TRANSPORT_INVALID_NOTIFICATION_MESSAGE =
      "Invalid MCP notification for session";
  public static final String TOOL_CALL_RATE_LIMIT_MESSAGE =
      "Tool call rate limit exceeded";

  /** Maximum characters returned in a single tool output text block. */
  public static final int MAX_TOOL_OUTPUT_TEXT_CHARS = 256 * 1024;

  /** Maximum MCP JSON-RPC request body size accepted by {@link McpHttpServlet}. */
  public static final int MAX_REQUEST_BODY_BYTES = 1024 * 1024;

  private McpJsonRpc() {
  }
}

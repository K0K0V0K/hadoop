/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.mcp;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.TreeMap;

import javax.servlet.http.HttpServletRequest;

import org.apache.hadoop.mcp.McpCallContext;
import org.apache.hadoop.mcp.McpSchema.CallToolResult;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.mcp.apikey.RMMcpApiKeyManager;
import org.apache.hadoop.yarn.server.resourcemanager.mcp.controllers.AbstractMcpController;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes MCP tool handlers with caller authentication.
 */
final class RMMcpToolExecutor {

  static final String API_KEY_HEADER = "X-Yarn-Mcp-Api-Key";
  private static final String API_KEY_PREFIX = "ApiKey ";
  private static final Logger LOG = LoggerFactory.getLogger(RMMcpToolExecutor.class);

  private RMMcpToolExecutor() {
  }

  static CallToolResult execute(RMMcpApiKeyManager apiKeyManager, String toolName,
      AbstractMcpController controller, McpCallContext context,
      Map<String, Object> arguments) {
    String filters =
        arguments == null || arguments.isEmpty() ? "{}" : new TreeMap<>(arguments).toString();
    String auditDetails = "tool=" + toolName + ", filters=" + filters;
    String user = "UNKNOWN";
    try {
      HttpServletRequest request = context == null ? null : context.getRequest();
      UserGroupInformation callerUgi = resolveCallerUgi(request, apiKeyManager);
      if (UserGroupInformation.isSecurityEnabled() && callerUgi == null) {
        auditFailure(user, auditDetails, "User not authenticated");
        return CallToolResult.error("User not authenticated");
      }

      user = callerUgi == null ? "UNKNOWN" : callerUgi.getShortUserName();
      CallToolResult result;
      if (callerUgi == null) {
        result = controller.buildResult(context, arguments);
      } else {
        result = callerUgi.doAs(
            (PrivilegedExceptionAction<CallToolResult>) () -> controller.buildResult(context,
                arguments));
      }
      auditResult(user, auditDetails, result);
      return result;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      String message = "Failed to execute MCP tool: " + e.getMessage();
      auditFailure(user, auditDetails, message);
      return CallToolResult.error(message);
    } catch (IOException e) {
      String message = "Failed to execute MCP tool: " + e.getMessage();
      auditFailure(user, auditDetails, message);
      return CallToolResult.error(message);
    }
  }

  static UserGroupInformation resolveCallerUgi(HttpServletRequest request,
      RMMcpApiKeyManager apiKeyManager) throws IOException {
    if (!UserGroupInformation.isSecurityEnabled() || apiKeyManager == null) {
      return null;
    }
    if (request == null) {
      return null;
    }
    String apiKey = request.getHeader(API_KEY_HEADER);
    if (apiKey != null && !apiKey.trim().isEmpty()) {
      apiKey = apiKey.trim();
    } else {
      String authorization = request.getHeader("Authorization");
      if (authorization != null && authorization.regionMatches(true, 0, API_KEY_PREFIX, 0,
          API_KEY_PREFIX.length())) {
        apiKey = authorization.substring(API_KEY_PREFIX.length()).trim();
      } else {
        apiKey = null;
      }
    }
    if (apiKey == null || apiKey.isEmpty()) {
      return null;
    }
    UserGroupInformation callerUgi = apiKeyManager.authenticate(apiKey);
    if (callerUgi == null) {
      LOG.warn("Invalid MCP API key presented");
    }
    return callerUgi;
  }

  private static void auditResult(String user, String auditDetails, CallToolResult result) {
    if (result != null && result.isError()) {
      auditFailure(user, auditDetails, extractErrorMessage(result));
      return;
    }
    RMAuditLogger.logSuccess(user, RMAuditLogger.AuditConstants.MCP_TOOL_CALL, "RMMcpServer",
        auditDetails);
  }

  private static void auditFailure(String user, String auditDetails, String error) {
    RMAuditLogger.logFailure(user, RMAuditLogger.AuditConstants.MCP_TOOL_CALL, "", "RMMcpServer",
        auditDetails + ", error=" + error);
  }

  private static String extractErrorMessage(CallToolResult result) {
    String text = result.firstText();
    return text == null ? "unknown error" : text;
  }
}

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

package org.apache.hadoop.yarn.server.resourcemanager.mcp.controllers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mcp.McpCallContext;
import org.apache.hadoop.mcp.McpJsonMapper;
import org.apache.hadoop.mcp.McpSchema.CallToolResult;
import org.apache.hadoop.mcp.McpSchema.Tool;
import org.apache.hadoop.mcp.McpToolSchema;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;

/**
 * Base class for MCP tool controllers exposed by the ResourceManager.
 *
 * <p>Subclasses define a single MCP tool via {@link #buildTool()} and implement
 * {@link #buildResult(McpCallContext, Map)} to execute it. Shared helpers cover
 * JSON serialization, caller identity, and the same application access checks
 * used by the RM webapp.</p>
 */
public abstract class AbstractMcpController {

  /** JSON Schema for tools that accept no input parameters. */
  protected static final Map<String, Object> EMPTY_OBJECT_SCHEMA = McpToolSchema.emptyObject();

  protected final ResourceManager rm;
  protected final McpJsonMapper jsonMapper;

  /**
   * @param rm ResourceManager instance
   * @param jsonMapper JSON mapper for tool results
   */
  protected AbstractMcpController(ResourceManager rm, McpJsonMapper jsonMapper) {
    this.rm = rm;
    this.jsonMapper = jsonMapper;
  }

  /**
   * Builds the MCP tool definition exposed to clients.
   *
   * @return tool metadata and input schema
   */
  public abstract Tool buildTool();

  /**
   * Executes the MCP tool for the given call context and arguments.
   *
   * @param context MCP call context, including the originating HTTP request
   * @param arguments tool input arguments parsed from the MCP request
   * @return tool result or error payload
   */
  public abstract CallToolResult buildResult(McpCallContext context, Map<String, Object> arguments);

  /**
   * Serializes a payload as JSON text content for an MCP tool result.
   *
   * @param payload object to serialize
   * @return JSON-encoded tool result
   */
  protected CallToolResult toJsonResult(Object payload) {
    try {
      return CallToolResult.text(jsonMapper.writeValueAsString(payload));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to serialize MCP tool result", e);
    }
  }

  /**
   * Returns the current caller UGI, or {@code null} if unavailable.
   *
   * @return caller UGI, or {@code null} when not authenticated
   */
  protected UserGroupInformation getCallerUgiOrNull() {
    try {
      return UserGroupInformation.getCurrentUser();
    } catch (IOException | UnsupportedOperationException e) {
      return null;
    }
  }

  /**
   * Builds an MCP tool error result.
   *
   * @param message error message returned to the client
   * @return error tool result
   */
  protected CallToolResult toErrorResult(String message) {
    return CallToolResult.error(message);
  }

  /**
   * Returns whether application listings should be filtered to apps visible to
   * the caller.
   *
   * @return {@code true} when {@link YarnConfiguration#FILTER_ENTITY_LIST_BY_USER}
   *     is enabled
   */
  protected boolean isFilterAppsByUser() {
    Configuration conf = rm.getConfig();
    return conf.getBoolean(YarnConfiguration.FILTER_ENTITY_LIST_BY_USER,
        YarnConfiguration.DEFAULT_DISPLAY_APPS_FOR_LOGGED_IN_USER);
  }

  /**
   * Returns whether the caller is a YARN admin when application ACLs are enabled.
   *
   * @param callerUgi caller UGI
   * @return {@code true} if the caller is a YARN admin
   */
  protected boolean isYarnAdmin(UserGroupInformation callerUgi) {
    ApplicationACLsManager aclsManager = rm.getApplicationACLsManager();
    return aclsManager != null && aclsManager.areACLsEnabled() && aclsManager.isAdmin(callerUgi);
  }

  /**
   * Returns whether the caller may view the given application.
   *
   * <p>Uses the same checks as the RM {@code /apps} REST API: application ACLs
   * first, then queue-admin ACLs with proxy-aware client addresses.</p>
   *
   * @param app application to check
   * @param callerUgi caller UGI, or {@code null} to allow access
   * @param request originating HTTP request, or {@code null}
   * @return {@code true} if the caller may view the application
   */
  protected boolean hasAppAccess(RMApp app, UserGroupInformation callerUgi,
      HttpServletRequest request) {
    if (callerUgi == null) {
      return true;
    }

    if (rm.getApplicationACLsManager()
        .checkAccess(callerUgi, ApplicationAccessType.VIEW_APP, app.getUser(),
            app.getApplicationId())) {
      return true;
    }

    String remoteAddress = request == null ? null : request.getRemoteAddr();
    return rm.getQueueACLsManager()
        .checkAccess(callerUgi, QueueACL.ADMINISTER_QUEUE, app, remoteAddress,
            getForwardedAddresses(request));
  }

  /**
   * Parses {@code X-Forwarded-For} header values for queue ACL IP checks.
   *
   * @param request originating HTTP request
   * @return forwarded client addresses, or {@code null} if unavailable
   */
  private static List<String> getForwardedAddresses(HttpServletRequest request) {
    if (request == null) {
      return null;
    }
    String forwardedFor = request.getHeader(RMWSConsts.FORWARDED_FOR);
    if (forwardedFor == null) {
      return null;
    }
    return Arrays.asList(forwardedFor.split(","));
  }

  /**
   * Returns an optional string tool argument.
   *
   * @param arguments tool input arguments
   * @param key argument name
   * @return argument value, or {@code null} if missing or empty
   */
  protected static String getStringArgument(Map<String, Object> arguments, String key) {
    if (arguments == null) {
      return null;
    }
    Object value = arguments.get(key);
    if (value == null) {
      return null;
    }
    String str = value.toString();
    return str.isEmpty() ? null : str;
  }

  /**
   * Returns an optional string-set tool argument.
   *
   * <p>Accepts either a JSON array or a single string value.</p>
   *
   * @param arguments tool input arguments
   * @param key argument name
   * @return argument values, or an empty set if missing or empty
   */
  protected static Set<String> getStringSetArgument(Map<String, Object> arguments, String key) {
    if (arguments == null) {
      return Collections.emptySet();
    }
    Object value = arguments.get(key);
    if (value == null) {
      return Collections.emptySet();
    }
    if (value instanceof Collection) {
      Set<String> result = new HashSet<>();
      for (Object item : (Collection<?>) value) {
        if (item != null) {
          String str = item.toString();
          if (!str.isEmpty()) {
            result.add(str);
          }
        }
      }
      return result;
    }
    String str = value.toString();
    return str.isEmpty() ? Collections.emptySet() : Collections.singleton(str);
  }
}

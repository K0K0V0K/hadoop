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

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mcp.McpHttpServer;
import org.apache.hadoop.mcp.McpJsonMapper;
import org.apache.hadoop.mcp.McpSchema.ServerCapabilities;
import org.apache.hadoop.mcp.McpSchema.Tool;
import org.apache.hadoop.mcp.McpServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.mcp.controllers.AbstractMcpController;
import org.apache.hadoop.yarn.server.resourcemanager.mcp.controllers.ApplicationsMcpController;
import org.apache.hadoop.yarn.server.resourcemanager.mcp.controllers.SchedulerMcpController;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MCP server for the Resource Manager.
 *
 * <p>JSON-RPC is served on a dedicated HTTP/HTTPS port via {@link McpHttpServer} (no Kerberos
 * filters). Kerberos-protected API key admin REST is registered by {@link
 * org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebApp#resourceConfig}.
 */
public final class RMMcpServer implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(RMMcpServer.class);

  public static final String ENDPOINT_PATH = "/ws/v1/mcp";

  private final McpServer mcpServer;
  private McpHttpServer httpServer;

  private RMMcpServer(McpServer mcpServer) {
    this.mcpServer = mcpServer;
  }

  public static RMMcpServer create(ResourceManager rm) {
    McpJsonMapper jsonMapper = RMMcpJsonMapper.create();

    McpServer.Builder builder =
        McpServer.sync(jsonMapper).serverInfo("yarn-resourcemanager", VersionInfo.getVersion())
            .capabilities(ServerCapabilities.withTools());
    for (AbstractMcpController controller : controllers(rm, jsonMapper)) {
      Tool tool = controller.buildTool();
      builder.toolCall(tool,
          (context, args) -> RMMcpToolExecutor.execute(rm.getRMMcpApiKeyManager(), tool.name(),
              controller, context, args));
    }

    return new RMMcpServer(builder.build());
  }

  private static List<AbstractMcpController> controllers(ResourceManager rm,
      McpJsonMapper jsonMapper) {
    return Arrays.asList(new SchedulerMcpController(rm, jsonMapper),
        new ApplicationsMcpController(rm, jsonMapper));
  }

  /**
   * Starts the dedicated MCP HTTP/HTTPS server on {@link YarnConfiguration#RM_MCP_ADDRESS}.
   */
  public void startHttpServer(Configuration conf) throws IOException {
    InetSocketAddress bindAddress = conf.getSocketAddr(YarnConfiguration.RM_MCP_ADDRESS,
        YarnConfiguration.DEFAULT_RM_MCP_ADDRESS, YarnConfiguration.DEFAULT_RM_MCP_PORT);
    boolean useHttps = conf.getBoolean(YarnConfiguration.RM_MCP_USE_HTTPS,
        YarnConfiguration.DEFAULT_RM_MCP_USE_HTTPS);
    if (!useHttps && UserGroupInformation.isSecurityEnabled()) {
      LOG.warn("{} is false on a secure cluster: MCP API keys are sent in HTTP headers "
              + "and can be sniffed or replayed. Use HTTPS in production.",
          YarnConfiguration.RM_MCP_USE_HTTPS);
    }
    httpServer = McpHttpServer.start(mcpServer, conf, bindAddress, ENDPOINT_PATH, useHttps);
  }

  McpServer getMcpServer() {
    return mcpServer;
  }

  McpHttpServer getHttpServer() {
    return httpServer;
  }

  @Override
  public void close() throws IOException {
    if (httpServer != null) {
      httpServer.close();
      httpServer = null;
    } else {
      mcpServer.close();
    }
  }
}

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

package org.apache.hadoop.yarn.server.resourcemanager.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mcp.JacksonMcpJsonMapper;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.mcp.McpCallContext;
import org.apache.hadoop.mcp.McpSchema.CallToolResult;
import org.apache.hadoop.mcp.McpSchema.TextContent;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.mcp.apikey.RMMcpApiKeyManager;
import org.apache.hadoop.yarn.server.resourcemanager.mcp.controllers.SchedulerMcpController;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Collections;

import java.io.File;

import org.apache.hadoop.fs.FileUtil;

public class TestRMMcpServer {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final JacksonMcpJsonMapper JSON_MAPPER =
      new JacksonMcpJsonMapper(OBJECT_MAPPER);

  @AfterEach
  public void tearDown() {
    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication", "simple");
    UserGroupInformation.setConfiguration(conf);
  }

  @Test
  public void testEndpointPath() {
    assertEquals("/ws/v1/mcp", RMMcpServer.ENDPOINT_PATH);
  }

  @Test
  public void testMcpDisabledByDefault() {
    Configuration conf = new Configuration();
    assertFalse(conf.getBoolean(YarnConfiguration.RM_MCP_ENABLE,
        YarnConfiguration.DEFAULT_RM_MCP_ENABLE));
  }

  @Test
  public void testMcpServerStartsWithSchedulerTool() throws Exception {
    StubResourceManager rm = new StubResourceManager(null, null);
    try (RMMcpServer mcpServer = RMMcpServer.create(rm)) {
      assertNotNull(mcpServer.getMcpServer().getServlet());
    }
  }

  @Test
  public void testStartPlainHttpServerBindsDedicatedPort() throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_MCP_ADDRESS, "localhost:0");
    conf.setBoolean(YarnConfiguration.RM_MCP_USE_HTTPS, false);
    StubResourceManager rm = new StubResourceManager(null, null);
    try (RMMcpServer mcpServer = RMMcpServer.create(rm)) {
      mcpServer.startHttpServer(conf);
      assertNotNull(mcpServer.getHttpServer());
      assertTrue(mcpServer.getHttpServer().getPort() > 0);
    }
  }

  @Test
  public void testPlainHttpOnSecureClusterLogsWarning() throws Exception {
    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication", "kerberos");
    UserGroupInformation.setConfiguration(conf);
    conf.set(YarnConfiguration.RM_MCP_ADDRESS, "localhost:0");
    conf.setBoolean(YarnConfiguration.RM_MCP_USE_HTTPS, false);

    GenericTestUtils.LogCapturer logCapture =
        GenericTestUtils.LogCapturer.captureLogs(
            org.slf4j.LoggerFactory.getLogger(RMMcpServer.class));
    StubResourceManager rm = new StubResourceManager(null, null);
    try (RMMcpServer mcpServer = RMMcpServer.create(rm)) {
      mcpServer.startHttpServer(conf);
      assertTrue(logCapture.getOutput().contains(YarnConfiguration.RM_MCP_USE_HTTPS));
      assertTrue(logCapture.getOutput().contains("sniffed or replayed"));
    }
  }

  @Test
  public void testStartHttpServerBindsDedicatedPort() throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_MCP_ADDRESS, "localhost:0");
    String keystoreDir = GenericTestUtils.getTempPath("rm-mcp-ssl");
    new File(keystoreDir).mkdirs();
    String sslConfDir = KeyStoreTestUtil.getClasspathDir(TestRMMcpServer.class);
    KeyStoreTestUtil.setupSSLConfig(keystoreDir, sslConfDir, conf, false);
    StubResourceManager rm = new StubResourceManager(null, null);
    try (RMMcpServer mcpServer = RMMcpServer.create(rm)) {
      mcpServer.startHttpServer(conf);
      assertNotNull(mcpServer.getHttpServer());
      assertTrue(mcpServer.getHttpServer().getPort() > 0);
    } finally {
      KeyStoreTestUtil.cleanupSSLConfig(keystoreDir, sslConfDir);
      FileUtil.fullyDelete(new File(keystoreDir));
    }
  }

  @Test
  public void testSchedulerInfoReturnsErrorForNullScheduler() throws Exception {
    StubResourceManager rm = new StubResourceManager(null, null);
    SchedulerMcpController controller = new SchedulerMcpController(rm, JSON_MAPPER);
    CallToolResult result = controller.buildResult(new McpCallContext(null),
        Collections.emptyMap());
    assertTrue(result.isError());
    TextContent content = result.content().get(0);
    assertTrue(content.text().contains("not initialized"));
  }

  @Test
  public void testSchedulerToolName() throws Exception {
    StubResourceManager rm = new StubResourceManager(null, null);
    SchedulerMcpController controller = new SchedulerMcpController(rm, JSON_MAPPER);
    assertEquals("get_scheduler_info", controller.buildTool().name());
  }

  private static final class StubResourceManager extends ResourceManager {
    private final ResourceScheduler scheduler;
    private final RMMcpApiKeyManager apiKeyManager;

    private StubResourceManager(ResourceScheduler scheduler,
        RMMcpApiKeyManager apiKeyManager) {
      this.scheduler = scheduler;
      this.apiKeyManager = apiKeyManager;
    }

    @Override
    public ResourceScheduler getResourceScheduler() {
      return scheduler;
    }

    @Override
    public RMMcpApiKeyManager getRMMcpApiKeyManager() {
      return apiKeyManager;
    }
  }
}

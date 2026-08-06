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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.mcp.McpCallContext;
import org.apache.hadoop.mcp.McpJsonMapper;
import org.apache.hadoop.mcp.McpSchema.CallToolResult;
import org.apache.hadoop.mcp.McpSchema.TextContent;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.mcp.controllers.ApplicationsMcpController;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.junit.jupiter.api.Test;

public class TestApplicationsMcpController {

  private static final McpJsonMapper JSON_MAPPER = RMMcpJsonMapper.create();

  @Test
  public void testApplicationsToolName() {
    ApplicationsMcpController controller =
        new ApplicationsMcpController(new StubResourceManager(), JSON_MAPPER);
    assertEquals("list_applications", controller.buildTool().name());
  }

  @Test
  public void testInputSchemaIncludesFilters() {
    ApplicationsMcpController controller =
        new ApplicationsMcpController(new StubResourceManager(), JSON_MAPPER);
    Map<String, Object> schema = controller.buildTool().inputSchema();
    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) schema.get("properties");
    assertTrue(properties.containsKey("states"));
    assertTrue(properties.containsKey("finalStatus"));
    assertTrue(properties.containsKey("user"));
    assertTrue(properties.containsKey("queue"));
    assertTrue(properties.containsKey("limit"));
    assertTrue(properties.containsKey("applicationTypes"));
    assertTrue(properties.containsKey("applicationTags"));
    assertTrue(properties.containsKey("name"));
  }

  @Test
  public void testAppsInfoSerializesWithoutTransientApplicationId() throws Exception {
    AppInfo appInfo = new AppInfo();
    appInfo.setAppId("application_123_456");
    Field userField = AppInfo.class.getDeclaredField("user");
    userField.setAccessible(true);
    userField.set(appInfo, "systest");
    Field applicationIdField = AppInfo.class.getDeclaredField("applicationId");
    applicationIdField.setAccessible(true);
    applicationIdField.set(appInfo, ApplicationId.newInstance(123L, 456));

    AppsInfo appsInfo = new AppsInfo();
    appsInfo.add(appInfo);

    String json = JSON_MAPPER.writeValueAsString(appsInfo);
    assertTrue(json.contains("application_123_456"));
    assertTrue(json.contains("systest"));
    assertFalse(json.contains("applicationId"));
  }

  @Test
  public void testListApplicationsReturnsErrorWhenServiceNotInitialized() {
    ApplicationsMcpController controller =
        new ApplicationsMcpController(new StubResourceManager(), JSON_MAPPER);
    CallToolResult result = controller.buildResult(new McpCallContext(null),
        Collections.emptyMap());
    assertTrue(result.isError());
    TextContent content = result.content().get(0);
    assertTrue(content.text().contains("application service is not initialized"));
  }

  private static final class StubResourceManager extends ResourceManager {
  }
}

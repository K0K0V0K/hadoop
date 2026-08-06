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

import java.util.Map;

import org.apache.hadoop.mcp.McpCallContext;
import org.apache.hadoop.mcp.McpJsonMapper;
import org.apache.hadoop.mcp.McpSchema.CallToolResult;
import org.apache.hadoop.mcp.McpSchema.Tool;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FifoSchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;

/**
 * MCP controller exposing YARN scheduler state.
 */
public final class SchedulerMcpController extends AbstractMcpController {

  static final String GET_SCHEDULER_INFO_TOOL = "get_scheduler_info";

  public SchedulerMcpController(ResourceManager rm, McpJsonMapper jsonMapper) {
    super(rm, jsonMapper);
  }

  @Override
  public Tool buildTool() {
    return Tool.of(GET_SCHEDULER_INFO_TOOL,
        "Return YARN scheduler state including queues, capacities, "
            + "resource usage, and application counts",
        EMPTY_OBJECT_SCHEMA);
  }

  @Override
  public CallToolResult buildResult(McpCallContext context, Map<String, Object> arguments) {
    UserGroupInformation callerUgi = getCallerUgiOrNull();
    if (rm.getApplicationACLsManager() != null && rm.getApplicationACLsManager().areACLsEnabled()
        && !isYarnAdmin(callerUgi)) {
      return toErrorResult("Only YARN admins can view scheduler information when ACLs are enabled");
    }

    ResourceScheduler scheduler = rm.getResourceScheduler();
    if (scheduler == null) {
      return toErrorResult("ResourceManager scheduler is not initialized");
    }

    SchedulerInfo schedulerInfo = buildSchedulerInfo(rm);
    if (schedulerInfo == null) {
      return toErrorResult("Unknown scheduler type: " + scheduler.getClass().getName());
    }
    return toJsonResult(new SchedulerTypeInfo(schedulerInfo));
  }

  static SchedulerInfo buildSchedulerInfo(ResourceManager rm) {
    ResourceScheduler scheduler = rm.getResourceScheduler();
    if (scheduler == null) {
      return null;
    }
    if (scheduler instanceof CapacityScheduler) {
      CapacityScheduler cs = (CapacityScheduler) scheduler;
      CSQueue root = cs.getRootQueue();
      return new CapacitySchedulerInfo(root, cs);
    }
    if (scheduler instanceof FairScheduler) {
      return new FairSchedulerInfo((FairScheduler) scheduler);
    }
    if (scheduler instanceof FifoScheduler) {
      return new FifoSchedulerInfo(rm);
    }
    return null;
  }
}

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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import javax.servlet.http.HttpServletRequest;

import org.apache.hadoop.mcp.McpCallContext;
import org.apache.hadoop.mcp.McpJsonMapper;
import org.apache.hadoop.mcp.McpSchema.CallToolResult;
import org.apache.hadoop.mcp.McpSchema.Tool;
import org.apache.hadoop.mcp.McpToolSchema;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.ApplicationsRequestBuilder;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

/**
 * MCP controller exposing YARN application listings with optional filters.
 */
public final class ApplicationsMcpController extends AbstractMcpController {

  static final String LIST_APPLICATIONS_TOOL = "list_applications";

  private static final Map<String, Object> INPUT_SCHEMA =
      McpToolSchema.object().stringArray("states", "Application states to include")
          .string("finalStatus", "Final application status filter")
          .string("user", "Submitting user filter").string("queue", "Queue name filter")
          .string("limit", "Maximum number of applications to return")
          .string("startedTimeBegin", "Started time lower bound (ms since epoch)")
          .string("startedTimeEnd", "Started time upper bound (ms since epoch)")
          .string("finishedTimeBegin", "Finished time lower bound (ms since epoch)")
          .string("finishedTimeEnd", "Finished time upper bound (ms since epoch)")
          .stringArray("applicationTypes", "Application types to include")
          .stringArray("applicationTags", "Application tags to include")
          .string("name", "Application name filter").build();

  public ApplicationsMcpController(ResourceManager rm, McpJsonMapper jsonMapper) {
    super(rm, jsonMapper);
  }

  @Override
  public Tool buildTool() {
    return Tool.of(LIST_APPLICATIONS_TOOL,
        "List YARN applications on the ResourceManager. All filter parameters are optional.",
        INPUT_SCHEMA);
  }

  @Override
  public CallToolResult buildResult(McpCallContext context, Map<String, Object> arguments) {
    if (rm.getClientRMService() == null) {
      return toErrorResult("ResourceManager application service is not initialized");
    }
    if (rm.getRMContext() == null) {
      return toErrorResult("ResourceManager context is not initialized");
    }

    Map<String, Object> filters = arguments == null ? Collections.emptyMap() : arguments;
    HttpServletRequest httpRequest = context == null ? null : context.getRequest();
    UserGroupInformation callerUgi = getCallerUgiOrNull();
    String userQuery = getStringArgument(filters, "user");
    if (userQuery != null
        && callerUgi != null
        && !isYarnAdmin(callerUgi)
        && !userQuery.equals(callerUgi.getShortUserName())
    ) {
      return toErrorResult("Only YARN admins can filter applications by user");
    }

    try {
      GetApplicationsRequest appsRequest = ApplicationsRequestBuilder.create()
          .withStatesQuery(getStringSetArgument(filters, "states")).withUserQuery(userQuery)
          .withQueueQuery(rm, getStringArgument(filters, "queue"))
          .withLimit(getStringArgument(filters, "limit"))
          .withStartedTimeBegin(getStringArgument(filters, "startedTimeBegin"))
          .withStartedTimeEnd(getStringArgument(filters, "startedTimeEnd"))
          .withFinishTimeBegin(getStringArgument(filters, "finishedTimeBegin"))
          .withFinishTimeEnd(getStringArgument(filters, "finishedTimeEnd"))
          .withApplicationTypes(getStringSetArgument(filters, "applicationTypes"))
          .withApplicationTags(getStringSetArgument(filters, "applicationTags"))
          .withName(getStringArgument(filters, "name")).build();

      List<ApplicationReport> appReports =
          rm.getClientRMService().getApplications(appsRequest).getApplicationList();

      String finalStatusQuery = getStringArgument(filters, "finalStatus");
      ConcurrentMap<ApplicationId, RMApp> apps = rm.getRMContext().getRMApps();
      String schemePrefix = WebAppUtils.getHttpSchemePrefix(rm.getConfig());
      boolean filterAppsByUser = isFilterAppsByUser();
      AppsInfo allApps = new AppsInfo();

      for (ApplicationReport report : appReports) {
        RMApp rmapp = apps.get(report.getApplicationId());
        if (rmapp == null) {
          continue;
        }
        if (finalStatusQuery != null) {
          FinalApplicationStatus.valueOf(finalStatusQuery);
          if (!rmapp.getFinalApplicationStatus().toString().equalsIgnoreCase(finalStatusQuery)) {
            continue;
          }
        }
        boolean allowAccess = hasAppAccess(rmapp, callerUgi, httpRequest);
        if (filterAppsByUser && !allowAccess) {
          continue;
        }
        allApps.add(new AppInfo(rm, rmapp, allowAccess, schemePrefix));
      }
      return toJsonResult(allApps);
    } catch (BadRequestException e) {
      return toErrorResult(e.getMessage());
    } catch (IllegalArgumentException e) {
      return toErrorResult("Invalid filter value: " + e.getMessage());
    } catch (YarnException e) {
      return toErrorResult("Failed to list applications: " + e.getMessage());
    }
  }
}

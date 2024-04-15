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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;

/**
 * TODO
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CGroupsIOResourceHandlerImpl implements DiskResourceHandler {

  private static final Logger LOG =
       LoggerFactory.getLogger(CGroupsIOResourceHandlerImpl.class);
  private final CGroupsHandler cGroupsHandler;

  CGroupsIOResourceHandlerImpl(CGroupsHandler cGroupsHandler) {
    this.cGroupsHandler = cGroupsHandler;
  }

  @Override
  public List<PrivilegedOperation> bootstrap(Configuration configuration)
      throws ResourceHandlerException {
    try {
      checkDiskScheduler();
    } catch (IOException|RuntimeException e) {
      LOG.warn("Failed to validate disk schedulers for CGroupsV2", e);
    }
    cGroupsHandler.initializeCGroupController(CGroupsHandler.CGroupController.IO);
    return null;
  }

  @Override
  public List<PrivilegedOperation> preStart(Container container)
      throws ResourceHandlerException {
    String cgroupId = container.getContainerId().toString();
    cGroupsHandler.createCGroup(CGroupsHandler.CGroupController.IO, cgroupId);
    cGroupsHandler.updateCGroupParam(CGroupsHandler.CGroupController.IO, cgroupId,
        CGroupsHandler.CGROUP_BFQ_WEIGHT, "default 500");
    return Collections.singletonList(new PrivilegedOperation(
        PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
        PrivilegedOperation.CGROUP_ARG_PREFIX +
            cGroupsHandler.getPathForCGroupTasks(CGroupsHandler.CGroupController.IO, cgroupId)));
  }

  @Override
  public List<PrivilegedOperation> postComplete(ContainerId containerId)
      throws ResourceHandlerException {
    cGroupsHandler.deleteCGroup(CGroupsHandler.CGroupController.IO, containerId.toString());
    return null;
  }

  @Override
  public String toString() {
    return CGroupsIOResourceHandlerImpl.class.getName();
  }

  private void checkDiskScheduler() throws IOException {
    if (!Shell.LINUX) {
      return;
    }
    String[] partitionLines = fileToString("/proc/partitions")
        .trim()
        .split(System.lineSeparator());
    for (String line : partitionLines) {
      String partitionName = line.split("\\s+")[3];
      String scheduler = fileToString("/sys/block/" + partitionName + "/queue/scheduler");
      if (!scheduler.contains("[bfq]")) {
        LOG.warn("Device {} has the following scheduler config: '{}', which does not contain [bfq],"
                + " as active scheduler, so the CGroupv2 IO limitation feature may not work",
            partitionName, scheduler);
      }
    }
  }

  private String fileToString(String path) throws IOException {
    return FileUtils.readFileToString(new File(path), StandardCharsets.UTF_8);
  }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;

/**
 * An implementation for using CGroups to restrict CPU usage on Linux. The
 * implementation supports 3 different controls - restrict usage of all YARN
 * containers, restrict relative usage of individual YARN containers and
 * restrict usage of individual YARN containers. Admins can set the overall CPU
 * to be used by all YARN containers - this is implemented by setting
 * cpu.cfs_period_us and cpu.cfs_quota_us to the ratio desired. If strict
 * resource usage mode is not enabled, cpu.shares is set for individual
 * containers - this prevents containers from exceeding the overall limit for
 * YARN containers but individual containers can use as much of the CPU as
 * available(under the YARN limit). If strict resource usage is enabled, then
 * container can only use the percentage of CPU allocated to them and this is
 * again implemented using cpu.cfs_period_us and cpu.cfs_quota_us.
 *
 */
@InterfaceStability.Unstable
@InterfaceAudience.Private
public class CGroupsV2CpuResourceHandlerImpl extends AbstractCGroupsCpuResourceHandler {
  private static final CGroupsHandler.CGroupController CPU =
      CGroupsHandler.CGroupController.CPU;

  @VisibleForTesting
  static final int CPU_DEFAULT_WEIGHT = 100; // cgroup v2 default
  static final int CPU_DEFAULT_WEIGHT_OPPORTUNISTIC = 1;
  static final int CPU_MAX_WEIGHT = 10000;
  static final String NO_LIMIT = "max";


  CGroupsV2CpuResourceHandlerImpl(CGroupsHandler cGroupsHandler) {
    super(cGroupsHandler);
  }

  @Override
  protected void updateCgroupMaxCpuLimit(String cgroupId, String max, String period) throws ResourceHandlerException {
    String cpuMaxLimit = cGroupsHandler.getCGroupParam(CPU, cgroupId, CGroupsHandler.CGROUP_CPU_MAX);

    String[] cpuMaxLimitArray = cpuMaxLimit.split(" ");
    String maxToSet = max != null ? max : cpuMaxLimitArray[0];
    maxToSet = maxToSet.equals("-1") ? NO_LIMIT : maxToSet;
    String periodToSet = period != null ? period : cpuMaxLimitArray[1];
    cGroupsHandler
        .updateCGroupParam(CPU, cgroupId, CGroupsHandler.CGROUP_CPU_MAX, maxToSet + " " + periodToSet);
  }

  @Override
  protected int getOpportunisticCpuWeight() {
    return CPU_DEFAULT_WEIGHT_OPPORTUNISTIC;
  }
  protected int getCpuWeightByContainerVcores(int containerVCores) {
    return Math.max(containerVCores * CPU_DEFAULT_WEIGHT, CPU_MAX_WEIGHT);
  }

  @Override
  protected void updateCgroupCpuWeight(String cgroupId, int weight) throws ResourceHandlerException {
    cGroupsHandler.updateCGroupParam(CPU, cgroupId, CGroupsHandler.CGROUP_PARAM_WEIGHT,
            String.valueOf(weight));
  }

  @Override
  public boolean cpuLimitExists(String cgroupPath) throws ResourceHandlerException {
    return checkCgroupV2CPULimitExists();
  }

  private boolean checkCgroupV2CPULimitExists()
      throws ResourceHandlerException {
    String globalCpuMaxLimit = cGroupsHandler.getCGroupParam(CPU, "", CGroupsHandler.CGROUP_CPU_MAX);
    String[] cpuMaxLimitArray = globalCpuMaxLimit.split(" ");

    return !cpuMaxLimitArray[0].equals(NO_LIMIT);
  }
}

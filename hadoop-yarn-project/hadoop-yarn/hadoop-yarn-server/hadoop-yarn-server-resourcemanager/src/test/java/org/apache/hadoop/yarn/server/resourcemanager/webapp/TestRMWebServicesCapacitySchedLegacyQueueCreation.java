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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import com.google.inject.Guice;
import com.sun.jersey.api.client.ClientResponse;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.junit.Test;

import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestRMWebServicesCapacitySched.assertJsonResponse;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestRMWebServicesCapacitySched.createMockRM;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestRMWebServicesCapacitySched.createWebAppDescriptor;

public class TestRMWebServicesCapacitySchedLegacyQueueCreation extends
    JerseyTestBase {
  private MockRM rm;

  private CapacitySchedulerQueueManager autoQueueHandler;

  public TestRMWebServicesCapacitySchedLegacyQueueCreation() {
    super(createWebAppDescriptor());
  }

  @Test
  public void testSchedulerResponsePercentageModeLegacyAutoCreation()
      throws Exception {
    Configuration config = createPercentageConfigLegacyAutoCreation();

    initResourceManager(config);

    /*
     * mode: percentage
     * managedtest2.autoCreationEligibility: legacy, others.autoCreationEligibility: off
     * weight: -1, normalizedWeight: 0
     * root.queueType: parent, others.queueType: leaf
     */
    assertJsonResponse(sendRequest(),
        "webapp/scheduler-response-PercentageModeLegacyAutoCreation.json");
  }

  @Test //uj test kellzx
  public void testSchedulerResponseAbsoluteModeLegacyAutoCreation()
      throws Exception {
    Configuration config = createAbsoluteConfigLegacyAutoCreation();

    initResourceManager(config);
    initAutoQueueHandler(8192);
    createQueue("root.managed.queue1");

    assertJsonResponse(sendRequest(),
        "webapp/scheduler-response-AbsoluteModeLegacyAutoCreation.json");
  }

  private void initAutoQueueHandler(int nodeMemory) throws Exception {
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    autoQueueHandler = cs.getCapacitySchedulerQueueManager();
    rm.registerNode("h1:1234", nodeMemory, 32); // label = x
  }

  private void createQueue(String queuePath) throws YarnException,
      IOException {
    autoQueueHandler.createQueue(new QueuePath(queuePath));
  }

  private ClientResponse sendRequest() {
    return resource().path("ws").path("v1").path("cluster")
        .path("scheduler").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
  }

  private Configuration createPercentageConfigLegacyAutoCreation() {
    Map<String, String> conf = new HashMap<>();
    conf.put("yarn.scheduler.capacity.root.queues", "default, test1, " +
        "managedtest2");
    conf.put("yarn.scheduler.capacity.root.test1.capacity", "50");
    conf.put("yarn.scheduler.capacity.root.managedtest2.capacity", "50");
    conf.put("yarn.scheduler.capacity.root.test1.maximum-capacity", "100");
    conf.put("yarn.scheduler.capacity.root.test1.state", "RUNNING");
    conf.put("yarn.scheduler.capacity.root.managedtest2.state", "RUNNING");
    conf.put("yarn.scheduler.capacity.root.managedtest2." +
        "auto-create-child-queue.enabled", "true");
    return createConfiguration(conf);
  }

  private Configuration createAbsoluteConfigLegacyAutoCreation() {
    Map<String, String> conf = new HashMap<>();
    conf.put("yarn.scheduler.capacity.root.queues", "default, managed");
    conf.put("yarn.scheduler.capacity.root.default.state", "STOPPED");
    conf.put("yarn.scheduler.capacity.root.default.capacity", "[memory=0,vcores=0]");
    conf.put("yarn.scheduler.capacity.root.managed.capacity", "[memory=4096,vcores=4]");
    conf.put("yarn.scheduler.capacity.root.managed.leaf-queue-template.capacity",
        "[memory=2048,vcores=2]");
    conf.put("yarn.scheduler.capacity.root.managed.state", "RUNNING");
    conf.put("yarn.scheduler.capacity.root.managed." +
        "auto-create-child-queue.enabled", "true");
    conf.put("yarn.scheduler.capacity.root.managed.leaf-queue-template.acl_submit_applications",
        "user");
    conf.put("yarn.scheduler.capacity.root.managed.leaf-queue-template.acl_administer_queue",
        "admin");
    return createConfiguration(conf);
  }

  private Configuration createConfiguration(
      Map<String, String> configs) {
    Configuration config = new Configuration();

    for (Map.Entry<String, String> entry : configs.entrySet()) {
      config.set(entry.getKey(), entry.getValue());
    }

    config.set(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        YarnConfiguration.MEMORY_CONFIGURATION_STORE);

    return config;
  }

  private void initResourceManager(Configuration conf) throws IOException {
    rm = createMockRM(new CapacitySchedulerConfiguration(conf));
    GuiceServletConfig.setInjector(
        Guice.createInjector(new TestRMWebServicesCapacitySched.WebServletModule(rm)));
    rm.start();
    //Need to call reinitialize as
    //MutableCSConfigurationProvider with InMemoryConfigurationStore
    //somehow does not load the queues properly and falls back to default config.
    //Therefore CS will think there's only the default queue there.
    ((CapacityScheduler) rm.getResourceScheduler()).reinitialize(conf,
        rm.getRMContext(), true);
  }
}

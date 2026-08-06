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

import org.apache.hadoop.mcp.JacksonMcpJsonMapper;
import org.apache.hadoop.mcp.McpJsonMapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;

/**
 * JSON mapper factory for ResourceManager MCP tool results.
 */
final class RMMcpJsonMapper {

  private RMMcpJsonMapper() {
  }

  /**
   * Creates a mapper configured for JAXB-annotated YARN REST DTOs.
   *
   * <p>Registers {@link JaxbAnnotationModule} so {@code @XmlTransient} fields (for example
   * protobuf handles on {@code AppInfo}) are omitted from JSON output.</p>
   */
  static McpJsonMapper create() {
    ObjectMapper mapper = new ObjectMapper().registerModule(new JaxbAnnotationModule());
    return new JacksonMcpJsonMapper(mapper);
  }
}

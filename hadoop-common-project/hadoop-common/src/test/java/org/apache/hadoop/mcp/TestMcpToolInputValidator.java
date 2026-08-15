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

package org.apache.hadoop.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

public class TestMcpToolInputValidator {

  @Test
  public void testMissingRequiredArgument() {
    Map<String, Object> schema = new HashMap<>();
    schema.put("type", "object");
    schema.put("required", Collections.singletonList("name"));
    Map<String, Object> properties = new HashMap<>();
    properties.put("name", Collections.singletonMap("type", "string"));
    schema.put("properties", properties);

    String error = McpToolInputValidator.validate(schema, Collections.emptyMap());
    assertEquals("Missing required argument: name", error);
  }

  @Test
  public void testWrongArgumentType() {
    Map<String, Object> schema = new HashMap<>();
    schema.put("type", "object");
    Map<String, Object> properties = new HashMap<>();
    properties.put("count", Collections.singletonMap("type", "integer"));
    schema.put("properties", properties);

    Map<String, Object> args = new HashMap<>();
    args.put("count", "not-an-int");

    String error = McpToolInputValidator.validate(schema, args);
    assertEquals("Argument 'count' must be an integer", error);
  }

  @Test
  public void testValidArgumentsAccepted() {
    Map<String, Object> schema = new HashMap<>();
    schema.put("type", "object");
    Map<String, Object> properties = new HashMap<>();
    properties.put("name", Collections.singletonMap("type", "string"));
    schema.put("properties", properties);

    Map<String, Object> args = Collections.singletonMap("name", "value");
    assertNull(McpToolInputValidator.validate(schema, args));
  }
}

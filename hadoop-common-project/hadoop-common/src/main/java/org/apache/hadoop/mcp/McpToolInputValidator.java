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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Validates MCP tool call arguments against a tool {@code inputSchema} map.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
final class McpToolInputValidator {

  private McpToolInputValidator() {
  }

  static String validate(Map<String, Object> inputSchema, Map<String, Object> arguments) {
    if (inputSchema == null || inputSchema.isEmpty()) {
      return null;
    }
    Object rootType = inputSchema.get("type");
    if (rootType != null && !"object".equals(rootType)) {
      return "Tool inputSchema root type must be object";
    }
    if (arguments == null) {
      return "Tool arguments must be an object";
    }

    Object requiredObject = inputSchema.get("required");
    if (requiredObject instanceof Collection) {
      for (Object requiredName : (Collection<?>) requiredObject) {
        if (requiredName instanceof String && !arguments.containsKey(requiredName)) {
          return "Missing required argument: " + requiredName;
        }
      }
    }

    Object propertiesObject = inputSchema.get("properties");
    if (!(propertiesObject instanceof Map)) {
      return null;
    }
    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) propertiesObject;
    for (Map.Entry<String, Object> entry : arguments.entrySet()) {
      Object propertySchemaObject = properties.get(entry.getKey());
      if (propertySchemaObject == null) {
        continue;
      }
      if (!(propertySchemaObject instanceof Map)) {
        return "Invalid schema for property: " + entry.getKey();
      }
      @SuppressWarnings("unchecked")
      Map<String, Object> propertySchema = (Map<String, Object>) propertySchemaObject;
      String propertyError = validateProperty(entry.getKey(), propertySchema, entry.getValue());
      if (propertyError != null) {
        return propertyError;
      }
    }
    return null;
  }

  private static String validateProperty(String name, Map<String, Object> propertySchema,
      Object value) {
    Object typeObject = propertySchema.get("type");
    if (!(typeObject instanceof String)) {
      return null;
    }
    switch ((String) typeObject) {
    case "string":
      if (!(value instanceof String)) {
        return "Argument '" + name + "' must be a string";
      }
      return null;
    case "integer":
      if (!(value instanceof Integer || value instanceof Long)) {
        return "Argument '" + name + "' must be an integer";
      }
      return null;
    case "number":
      if (!(value instanceof Number)) {
        return "Argument '" + name + "' must be a number";
      }
      return null;
    case "boolean":
      if (!(value instanceof Boolean)) {
        return "Argument '" + name + "' must be a boolean";
      }
      return null;
    case "array":
      if (!(value instanceof List)) {
        return "Argument '" + name + "' must be an array";
      }
      Object itemsObject = propertySchema.get("items");
      if (itemsObject instanceof Map) {
        @SuppressWarnings("unchecked")
        Map<String, Object> items = (Map<String, Object>) itemsObject;
        if ("string".equals(items.get("type"))) {
          for (Object item : (List<?>) value) {
            if (!(item instanceof String)) {
              return "Argument '" + name + "' must be an array of strings";
            }
          }
        }
      }
      return null;
    default:
      return null;
    }
  }
}

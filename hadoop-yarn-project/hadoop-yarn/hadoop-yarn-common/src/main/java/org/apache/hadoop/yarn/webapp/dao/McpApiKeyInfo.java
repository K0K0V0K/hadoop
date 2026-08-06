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

package org.apache.hadoop.yarn.webapp.dao;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "mcpApiKey")
@XmlAccessorType(XmlAccessType.FIELD)
public class McpApiKeyInfo {

  private String keyId;
  private String apiKey;
  private String ownerUser;
  private String createdBy;
  private long createdAt;

  public McpApiKeyInfo() {
  }

  public McpApiKeyInfo(String keyId, String apiKey, String ownerUser,
      String createdBy, long createdAt) {
    this.keyId = keyId;
    this.apiKey = apiKey;
    this.ownerUser = ownerUser;
    this.createdBy = createdBy;
    this.createdAt = createdAt;
  }

  public String getKeyId() {
    return keyId;
  }

  public void setKeyId(String keyId) {
    this.keyId = keyId;
  }

  public String getApiKey() {
    return apiKey;
  }

  public void setApiKey(String apiKey) {
    this.apiKey = apiKey;
  }

  public String getOwnerUser() {
    return ownerUser;
  }

  public void setOwnerUser(String ownerUser) {
    this.ownerUser = ownerUser;
  }

  public String getCreatedBy() {
    return createdBy;
  }

  public void setCreatedBy(String createdBy) {
    this.createdBy = createdBy;
  }

  public long getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(long createdAt) {
    this.createdAt = createdAt;
  }
}

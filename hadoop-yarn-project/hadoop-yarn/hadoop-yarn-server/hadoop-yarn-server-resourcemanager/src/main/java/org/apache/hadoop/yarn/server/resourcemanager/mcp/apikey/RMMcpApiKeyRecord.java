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

package org.apache.hadoop.yarn.server.resourcemanager.mcp.apikey;

import java.util.Objects;

/**
 * Metadata for an MCP API key. The raw secret is never persisted.
 */
public final class RMMcpApiKeyRecord {

  private String keyId;
  private String ownerUser;
  private String createdBy;
  private long createdAt;
  private String secretHash;
  private String salt;

  public RMMcpApiKeyRecord() {
  }

  public static RMMcpApiKeyRecord newKey(String keyId, String ownerUser, String createdBy,
      String secretHash, String salt) {
    Objects.requireNonNull(keyId, "keyId must not be null");
    Objects.requireNonNull(ownerUser, "ownerUser must not be null");
    Objects.requireNonNull(createdBy, "createdBy must not be null");
    Objects.requireNonNull(secretHash, "secretHash must not be null");
    Objects.requireNonNull(salt, "salt must not be null");
    RMMcpApiKeyRecord record = new RMMcpApiKeyRecord();
    record.setKeyId(keyId);
    record.setOwnerUser(ownerUser);
    record.setCreatedBy(createdBy);
    record.setCreatedAt(System.currentTimeMillis());
    record.setSecretHash(secretHash);
    record.setSalt(salt);
    return record;
  }

  public static RMMcpApiKeyRecord copyOf(RMMcpApiKeyRecord record) {
    RMMcpApiKeyRecord copy = new RMMcpApiKeyRecord();
    copy.setKeyId(record.getKeyId());
    copy.setOwnerUser(record.getOwnerUser());
    copy.setCreatedBy(record.getCreatedBy());
    copy.setCreatedAt(record.getCreatedAt());
    copy.setSecretHash(record.getSecretHash());
    copy.setSalt(record.getSalt());
    return copy;
  }

  public String getKeyId() {
    return keyId;
  }

  public void setKeyId(String keyId) {
    this.keyId = keyId;
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

  public String getSecretHash() {
    return secretHash;
  }

  public void setSecretHash(String secretHash) {
    this.secretHash = secretHash;
  }

  public String getSalt() {
    return salt;
  }

  public void setSalt(String salt) {
    this.salt = salt;
  }
}

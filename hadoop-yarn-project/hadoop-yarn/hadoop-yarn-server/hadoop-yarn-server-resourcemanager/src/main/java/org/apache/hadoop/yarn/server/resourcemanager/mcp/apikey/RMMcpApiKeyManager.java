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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.mcp.apikey.RMMcpApiKeyCrypto.ParsedApiKey;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates, lists, revokes, and authenticates MCP API keys via {@link RMStateStore}.
 */
public final class RMMcpApiKeyManager {

  private static final Logger LOG = LoggerFactory.getLogger(RMMcpApiKeyManager.class);

  public static final class CreateResult {
    private final String apiKey;
    private final RMMcpApiKeyRecord record;

    CreateResult(String apiKey, RMMcpApiKeyRecord record) {
      this.apiKey = apiKey;
      this.record = record;
    }

    public String getApiKey() {
      return apiKey;
    }

    public RMMcpApiKeyRecord getRecord() {
      return record;
    }
  }

  private final RMStateStore stateStore;
  private final RMMcpApiKeyCrypto apiKeyCrypto;

  RMMcpApiKeyManager(RMStateStore stateStore, Configuration conf) {
    this.stateStore = stateStore;
    this.apiKeyCrypto = new RMMcpApiKeyCrypto(conf);
  }

  public static RMMcpApiKeyManager create(ResourceManager rm) {
    if (rm.getRMContext() == null || rm.getRMContext().getStateStore() == null) {
      throw new IllegalStateException("RM state store is not initialized");
    }
    return new RMMcpApiKeyManager(rm.getRMContext().getStateStore(), rm.getConfig());
  }

  public CreateResult createKey(String ownerUser, String createdBy) throws IOException {
    if (ownerUser == null || ownerUser.isEmpty()) {
      throw new IllegalArgumentException("ownerUser must be provided");
    }
    if (createdBy == null || createdBy.isEmpty()) {
      throw new IllegalArgumentException("createdBy must be provided");
    }

    String keyId = apiKeyCrypto.generateKeyId();
    String secret = apiKeyCrypto.generateSecret();
    String salt = apiKeyCrypto.generateSalt();
    RMMcpApiKeyRecord record =
        RMMcpApiKeyRecord.newKey(keyId, ownerUser, createdBy, apiKeyCrypto.hashSecret(secret, salt),
            salt);
    stateStore.storeMcpApiKey(record);
    LOG.info("Created MCP API key {} for owner {} by {}", keyId, ownerUser, createdBy);
    return new CreateResult(apiKeyCrypto.formatApiKey(keyId, secret), record);
  }

  public List<RMMcpApiKeyRecord> listKeys() throws IOException {
    List<RMMcpApiKeyRecord> keys = stateStore.listMcpApiKeys();
    LOG.debug("Listed {} MCP API keys", keys.size());
    return keys;
  }

  public void revokeKey(String keyId) throws IOException {
    RMMcpApiKeyRecord record = stateStore.getMcpApiKey(keyId);
    if (record == null) {
      throw new IllegalArgumentException("Unknown MCP API key: " + keyId);
    }
    stateStore.removeMcpApiKey(keyId);
    LOG.info("Revoked MCP API key {} for owner {}", keyId, record.getOwnerUser());
  }

  public UserGroupInformation authenticate(String apiKey) throws IOException {
    ParsedApiKey parsed = apiKeyCrypto.parseApiKey(apiKey);
    if (parsed == null) {
      return null;
    }
    RMMcpApiKeyRecord record = stateStore.getMcpApiKey(parsed.getKeyId());
    if (record == null) {
      LOG.debug("Rejected unknown MCP API key {}", parsed.getKeyId());
      return null;
    }
    if (!apiKeyCrypto.verifySecret(parsed.getSecret(), record)) {
      return null;
    }
    LOG.debug("Authenticated MCP API key {} for owner {}", parsed.getKeyId(),
        record.getOwnerUser());
    return UserGroupInformation.createRemoteUser(record.getOwnerUser());
  }
}

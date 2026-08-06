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

package org.apache.hadoop.yarn.server.resourcemanager.mcp.apikey;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;

public class TestRMMcpApiKeyManager {

  @Test
  public void testCreateAuthenticateAndRevoke() throws Exception {
    RMMcpApiKeyManager manager = RMMcpApiKeyManagerTestHelper.newManager();
    RMMcpApiKeyManager.CreateResult created =
        manager.createKey("bob", "admin");
    assertNotNull(created.getApiKey());
    assertEquals("bob", created.getRecord().getOwnerUser());
    assertTrue(created.getRecord().getSecretHash()
        .startsWith(RMMcpApiKeyCrypto.HASH_SCHEME_PBKDF2));

    UserGroupInformation caller = manager.authenticate(created.getApiKey());
    assertNotNull(caller);
    assertEquals("bob", caller.getShortUserName());

    assertEquals(1, manager.listKeys().size());
    manager.revokeKey(created.getRecord().getKeyId());
    assertEquals(0, manager.listKeys().size());
    assertNull(manager.authenticate(created.getApiKey()));
  }

  @Test
  public void testInvalidApiKeyFormat() throws Exception {
    RMMcpApiKeyManager manager = RMMcpApiKeyManagerTestHelper.newManager();
    assertNull(manager.authenticate("not-a-valid-key"));
  }
}

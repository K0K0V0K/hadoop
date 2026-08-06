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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class TestRMMcpApiKeyCrypto {

  @Test
  public void testPbkdf2HashAndVerify() {
    RMMcpApiKeyCrypto crypto = new RMMcpApiKeyCrypto();
    String secret = crypto.generateSecret();
    String salt = crypto.generateSalt();
    String hash = crypto.hashSecret(secret, salt);

    assertTrue(hash.startsWith(RMMcpApiKeyCrypto.HASH_SCHEME_PBKDF2));
    RMMcpApiKeyRecord record = RMMcpApiKeyRecord.newKey("key1", "bob", "admin",
        hash, salt);
    assertTrue(crypto.verifySecret(secret, record));
    assertFalse(crypto.verifySecret("wrong-secret", record));
  }

  @Test
  public void testRejectUnsupportedHashScheme() {
    RMMcpApiKeyCrypto crypto = new RMMcpApiKeyCrypto();
    String secret = crypto.generateSecret();
    String salt = crypto.generateSalt();
    RMMcpApiKeyRecord record = RMMcpApiKeyRecord.newKey("key1", "bob", "admin",
        "unsupported-hash", salt);
    assertFalse(crypto.verifySecret(secret, record));
  }

  @Test
  public void testParseApiKey() {
    RMMcpApiKeyCrypto crypto = new RMMcpApiKeyCrypto();
    RMMcpApiKeyCrypto.ParsedApiKey parsed = crypto.parseApiKey(
        "ymcp_abc123_secretvalue");
    assertNotNull(parsed);
    assertTrue(parsed.getKeyId().equals("abc123"));
    assertTrue(parsed.getSecret().equals("secretvalue"));
  }

  @Test
  public void testNewKeyRejectsNullSecretHash() {
    assertThrows(NullPointerException.class,
        () -> RMMcpApiKeyRecord.newKey("key1", "bob", "admin", null, "salt"));
  }

  @Test
  public void testNewKeyRejectsNullSalt() {
    assertThrows(NullPointerException.class,
        () -> RMMcpApiKeyRecord.newKey("key1", "bob", "admin", "hash", null));
  }
}

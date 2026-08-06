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
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Objects;
import java.util.UUID;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY;

/**
 * Crypto, formatting, and serialization for MCP API keys.
 *
 * <p>Crypto operations honor Hadoop JCE provider and secure-random configuration so they can run
 * against FIPS-validated providers (for example BCFIPS) when configured on the cluster.</p>
 */
public final class RMMcpApiKeyCrypto {

  private static final Logger LOG = LoggerFactory.getLogger(RMMcpApiKeyCrypto.class);

  static final String KEY_PREFIX = "ymcp";
  static final String HASH_SCHEME_PBKDF2 = "pbkdf2-sha256:";
  private static final int SECRET_BYTES = 32;
  private static final int SALT_BYTES = 16;
  private static final int PBKDF2_ITERATIONS = 10000;
  private static final int PBKDF2_KEY_LENGTH_BITS = 256;
  private static final String PBKDF2_ALGORITHM = "PBKDF2WithHmacSHA256";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final String jceProvider;
  private final SecureRandom secureRandom;

  public RMMcpApiKeyCrypto() {
    this(new Configuration());
  }

  public RMMcpApiKeyCrypto(Configuration conf) {
    Objects.requireNonNull(conf, "conf must not be null");
    this.jceProvider = CryptoUtils.getJceProvider(conf);
    this.secureRandom = createSecureRandom(conf, jceProvider);
  }

  String generateKeyId() {
    return UUID.randomUUID().toString().replace("-", "");
  }

  String generateSecret() {
    byte[] secretBytes = new byte[SECRET_BYTES];
    secureRandom.nextBytes(secretBytes);
    return Base64.getUrlEncoder().withoutPadding().encodeToString(secretBytes);
  }

  String generateSalt() {
    byte[] saltBytes = new byte[SALT_BYTES];
    secureRandom.nextBytes(saltBytes);
    return Base64.getUrlEncoder().withoutPadding().encodeToString(saltBytes);
  }

  String formatApiKey(String keyId, String secret) {
    return KEY_PREFIX + "_" + keyId + "_" + secret;
  }

  ParsedApiKey parseApiKey(String apiKey) {
    if (apiKey == null) {
      LOG.debug("Rejected MCP API key: value is null");
      return null;
    }
    if (apiKey.isEmpty()) {
      LOG.debug("Rejected MCP API key: value is empty");
      return null;
    }
    String[] parts = apiKey.split("_", 3);
    if (parts.length != 3) {
      LOG.debug("Rejected MCP API key: expected prefix_keyId_secret (3 parts), got {} parts",
          parts.length);
      return null;
    }
    if (!KEY_PREFIX.equals(parts[0])) {
      LOG.debug("Rejected MCP API key: invalid prefix '{}', expected '{}'", parts[0], KEY_PREFIX);
      return null;
    }
    if (parts[1].isEmpty()) {
      LOG.debug("Rejected MCP API key: key id is empty");
      return null;
    }
    if (parts[2].isEmpty()) {
      LOG.debug("Rejected MCP API key {}: secret is empty", parts[1]);
      return null;
    }
    return new ParsedApiKey(parts[1], parts[2]);
  }

  String hashSecret(String secret, String salt) {
    byte[] hash = pbkdf2(secret, salt);
    return HASH_SCHEME_PBKDF2 + Base64.getEncoder().encodeToString(hash);
  }

  boolean verifySecret(String secret, RMMcpApiKeyRecord record) {
    if (record == null) {
      LOG.debug("MCP API key verification failed: record is null");
      return false;
    }
    String keyId = record.getKeyId();
    if (secret == null) {
      LOG.debug("MCP API key {} verification failed: secret is null", keyId);
      return false;
    }
    if (record.getSecretHash() == null) {
      LOG.debug("MCP API key {} verification failed: stored hash is null", keyId);
      return false;
    }
    if (record.getSalt() == null) {
      LOG.debug("MCP API key {} verification failed: salt is null", keyId);
      return false;
    }
    String storedHash = record.getSecretHash();
    if (!storedHash.startsWith(HASH_SCHEME_PBKDF2)) {
      LOG.debug("MCP API key {} verification failed: unsupported hash scheme", keyId);
      return false;
    }
    byte[] expected = decodeBase64(storedHash.substring(HASH_SCHEME_PBKDF2.length()));
    if (expected == null) {
      LOG.debug("MCP API key {} verification failed: stored hash is malformed", keyId);
      return false;
    }
    byte[] actual = pbkdf2(secret, record.getSalt());
    if (!MessageDigest.isEqual(expected, actual)) {
      LOG.debug("MCP API key {} verification failed: secret mismatch", keyId);
      return false;
    }
    return true;
  }

  public byte[] serialize(RMMcpApiKeyRecord record) throws IOException {
    return OBJECT_MAPPER.writeValueAsBytes(record);
  }

  public RMMcpApiKeyRecord deserialize(byte[] data) throws IOException {
    return OBJECT_MAPPER.readValue(data, RMMcpApiKeyRecord.class);
  }

  private byte[] pbkdf2(String secret, String salt) {
    try {
      SecretKeyFactory factory = getSecretKeyFactory();
      PBEKeySpec spec =
          new PBEKeySpec(secret.toCharArray(), decodeSaltBytes(salt), PBKDF2_ITERATIONS,
              PBKDF2_KEY_LENGTH_BITS);
      try {
        return factory.generateSecret(spec).getEncoded();
      } finally {
        spec.clearPassword();
      }
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("Failed to derive MCP API key hash", e);
    }
  }

  private SecretKeyFactory getSecretKeyFactory() throws GeneralSecurityException {
    return useJceProvider(jceProvider) ?
        SecretKeyFactory.getInstance(PBKDF2_ALGORITHM, jceProvider) :
        SecretKeyFactory.getInstance(PBKDF2_ALGORITHM);
  }

  private static SecureRandom createSecureRandom(Configuration conf, String provider) {
    final String secureRandomAlg = conf.get(HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY,
        HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_DEFAULT);
    try {
      return useJceProvider(provider) ?
          SecureRandom.getInstance(secureRandomAlg, provider) :
          SecureRandom.getInstance(secureRandomAlg);
    } catch (GeneralSecurityException e) {
      String message = "Failed to create SecureRandom algorithm " + secureRandomAlg;
      if (useJceProvider(provider)) {
        message += " with provider " + provider;
      }
      throw new IllegalStateException(message, e);
    }
  }

  private static boolean useJceProvider(String provider) {
    return provider != null && !provider.isEmpty();
  }

  private static byte[] decodeSaltBytes(String salt) {
    return Base64.getUrlDecoder().decode(salt);
  }

  private static byte[] decodeBase64(String encoded) {
    try {
      return Base64.getDecoder().decode(encoded);
    } catch (IllegalArgumentException e) {
      LOG.debug("Failed to decode base64 value", e);
      return null;
    }
  }

  static final class ParsedApiKey {
    private final String keyId;
    private final String secret;

    ParsedApiKey(String keyId, String secret) {
      this.keyId = keyId;
      this.secret = secret;
    }

    String getKeyId() {
      return keyId;
    }

    String getSecret() {
      return secret;
    }
  }
}

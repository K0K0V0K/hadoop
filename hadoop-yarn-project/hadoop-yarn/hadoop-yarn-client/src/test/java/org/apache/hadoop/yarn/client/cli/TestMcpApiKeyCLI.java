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

package org.apache.hadoop.yarn.client.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Class for testing {@link McpApiKeyCLI}.
 */
public class TestMcpApiKeyCLI {

  private McpApiKeyCLI cli;
  private PrintStream originalErr;
  private PrintStream originalOut;

  @BeforeEach
  public void setUp() {
    cli = new McpApiKeyCLI();
    originalErr = System.err;
    originalOut = System.out;
  }

  @AfterEach
  public void tearDown() {
    System.setErr(originalErr);
    System.setOut(originalOut);
  }

  @Test
  @Timeout(10000)
  public void testHelp() throws Exception {
    ByteArrayOutputStream sysOutStream = new ByteArrayOutputStream();
    System.setOut(new PrintStream(sysOutStream));

    assertEquals(0, cli.run(new String[] {"-h"}));
    assertTrue(sysOutStream.toString().contains("yarn mcpapikey [-list]"));
  }

  @Test
  @Timeout(10000)
  public void testCreateMissingOwnerUser() throws Exception {
    ByteArrayOutputStream sysErrStream = new ByteArrayOutputStream();
    System.setErr(new PrintStream(sysErrStream));

    int exitCode = cli.run(new String[] {"-create"});
    assertNotEquals(0, exitCode);
    assertTrue(sysErrStream.toString().contains("Missing required option: -ownerUser"));
  }

  @Test
  @Timeout(10000)
  public void testRevokeEmptyKeyId() throws Exception {
    ByteArrayOutputStream sysErrStream = new ByteArrayOutputStream();
    System.setErr(new PrintStream(sysErrStream));

    int exitCode = cli.run(new String[] {"-revoke", ""});
    assertNotEquals(0, exitCode);
    assertTrue(sysErrStream.toString().contains("Missing key id for -revoke"));
  }

  @Test
  @Timeout(10000)
  public void testListRequiresSecureCluster() throws Exception {
    ByteArrayOutputStream sysErrStream = new ByteArrayOutputStream();
    System.setErr(new PrintStream(sysErrStream));

    int exitCode = cli.run(new String[] {"-list"});
    assertNotEquals(0, exitCode);
    assertTrue(sysErrStream.toString().contains("only supported on secure clusters"));
  }

  @Test
  @Timeout(10000)
  public void testInvalidUsage() throws Exception {
    ByteArrayOutputStream sysErrStream = new ByteArrayOutputStream();
    System.setErr(new PrintStream(sysErrStream));

    int exitCode = cli.run(new String[] {});
    assertNotEquals(0, exitCode);
    assertTrue(sysErrStream.toString().contains("Invalid command usage."));
  }
}

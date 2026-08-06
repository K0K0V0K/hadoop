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

package org.apache.hadoop.yarn.client.cli;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.webapp.dao.McpApiKeyCreateRequest;
import org.apache.hadoop.yarn.webapp.dao.McpApiKeyInfo;
import org.apache.hadoop.yarn.webapp.dao.McpApiKeysInfo;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.util.YarnWebServiceUtils;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.HttpUrlConnectorProvider;

/**
 * CLI for administering MCP API keys on secure ResourceManager clusters.
 */
@Public
@Unstable
public class McpApiKeyCLI extends Configured implements Tool {

  private static final String LIST_CMD = "list";
  private static final String CREATE_CMD = "create";
  private static final String REVOKE_CMD = "revoke";
  private static final String HELP_CMD = "help";
  private static final String OWNER_USER_OPTION = "ownerUser";
  private static final MediaType[] ACCEPT_XML_OR_JSON =
      {MediaType.APPLICATION_XML_TYPE, MediaType.APPLICATION_JSON_TYPE};

  private SSLFactory sslFactory;
  private Client client;

  public McpApiKeyCLI() {
    super(new YarnConfiguration());
  }

  public static void main(String[] args) throws Exception {
    McpApiKeyCLI cli = new McpApiKeyCLI();
    System.exit(cli.run(args));
  }

  @Override
  public int run(String[] args) throws Exception {
    Options opts = new Options();
    opts.addOption("list", LIST_CMD, false, "List MCP API keys");
    opts.addOption("create", CREATE_CMD, false, "Create an MCP API key");
    opts.addOption("revoke", REVOKE_CMD, true, "Revoke an MCP API key by key id");
    opts.addOption("ownerUser", OWNER_USER_OPTION, true, "Owner user for a new MCP API key");
    opts.addOption("h", HELP_CMD, false, "Displays help for all commands.");

    CommandLine parsedCli;
    try {
      parsedCli = new GnuParser().parse(opts, args);
    } catch (Exception ex) {
      System.err.println(ex.getMessage());
      printUsage();
      return -1;
    }

    if (parsedCli.hasOption(HELP_CMD)) {
      printUsage();
      return 0;
    }

    if (!parsedCli.hasOption(LIST_CMD) && !parsedCli.hasOption(CREATE_CMD)
        && !parsedCli.hasOption(REVOKE_CMD)) {
      System.err.println("Invalid command usage.");
      printUsage();
      return -1;
    }

    String ownerUser = null;
    if (parsedCli.hasOption(CREATE_CMD)) {
      ownerUser = parsedCli.getOptionValue(OWNER_USER_OPTION);
      if (ownerUser == null || ownerUser.isEmpty()) {
        System.err.println("Missing required option: -ownerUser");
        printUsage();
        return -1;
      }
    }
    if (parsedCli.hasOption(REVOKE_CMD)) {
      String keyId = parsedCli.getOptionValue(REVOKE_CMD);
      if (keyId == null || keyId.isEmpty()) {
        System.err.println("Missing key id for -revoke");
        printUsage();
        return -1;
      }
    }

    if (!verifySecurityEnabled()) {
      return -1;
    }

    Configuration conf = getConf();
    if (parsedCli.hasOption(LIST_CMD)) {
      return WebAppUtils.execOnActiveRM(conf, this::listApiKeys, null);
    }
    if (parsedCli.hasOption(CREATE_CMD)) {
      McpApiKeyCreateRequest request = new McpApiKeyCreateRequest();
      request.setOwnerUser(ownerUser);
      return WebAppUtils.execOnActiveRM(conf, this::createApiKey, request);
    }
    String keyId = parsedCli.getOptionValue(REVOKE_CMD);
    return WebAppUtils.execOnActiveRM(conf, this::revokeApiKey, keyId);
  }

  @VisibleForTesting
  int listApiKeys(String webAppAddress, WebTarget resource) throws Exception {
    Response response = null;
    resource = (resource != null) ? resource : initializeWebResource(webAppAddress);
    try {
      Invocation.Builder builder =
          newBuilder(resource.path("ws").path("v1").path("cluster").path("mcp-api-keys"));
      response = builder.get(Response.class);
      if (response == null) {
        System.err.println("Failed to list MCP API keys: null response");
        return -1;
      }
      if (response.getStatus() != Status.OK.getStatusCode()) {
        System.err.println("Failed to list MCP API keys: " + response.readEntity(String.class));
        return -1;
      }
      McpApiKeysInfo keysInfo = response.readEntity(McpApiKeysInfo.class);
      List<McpApiKeyInfo> keys = keysInfo.getApiKeys();
      if (keys.isEmpty()) {
        System.out.println("No MCP API keys found.");
        return 0;
      }
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      System.out.printf("%-34s %-16s %-16s %-20s%n", "Key Id", "Owner", "Created By", "Created At");
      for (McpApiKeyInfo key : keys) {
        String createdAt = dateFormat.format(new Date(key.getCreatedAt()));
        System.out.printf("%-34s %-16s %-16s %-20s%n", key.getKeyId(), key.getOwnerUser(),
            key.getCreatedBy(), createdAt);
      }
      return 0;
    } finally {
      if (response != null) {
        response.close();
      }
      destroyClient();
    }
  }

  @VisibleForTesting
  int createApiKey(String webAppAddress, McpApiKeyCreateRequest request) throws Exception {
    Response response = null;
    WebTarget resource = initializeWebResource(webAppAddress);
    try {
      Invocation.Builder builder =
          newBuilder(resource.path("ws").path("v1").path("cluster").path("mcp-api-keys"));
      response = builder.post(
          Entity.entity(YarnWebServiceUtils.toJson(request, McpApiKeyCreateRequest.class),
              MediaType.APPLICATION_JSON), Response.class);
      if (response == null) {
        System.err.println("Failed to create MCP API key: null response");
        return -1;
      }
      if (response.getStatus() != Status.OK.getStatusCode()) {
        System.err.println("Failed to create MCP API key: " + response.readEntity(String.class));
        return -1;
      }
      McpApiKeyInfo created = response.readEntity(McpApiKeyInfo.class);
      System.out.println("Created MCP API key.");
      System.out.println("Key Id:  " + created.getKeyId());
      System.out.println("Owner:   " + created.getOwnerUser());
      System.out.println("Api Key: " + created.getApiKey());
      return 0;
    } finally {
      if (response != null) {
        response.close();
      }
      destroyClient();
    }
  }

  @VisibleForTesting
  int revokeApiKey(String webAppAddress, String keyId) throws Exception {
    Response response = null;
    WebTarget resource = initializeWebResource(webAppAddress);
    try {
      Invocation.Builder builder = newBuilder(
          resource.path("ws").path("v1").path("cluster").path("mcp-api-keys").path(keyId));
      response = builder.delete(Response.class);
      if (response == null) {
        System.err.println("Failed to revoke MCP API key: null response");
        return -1;
      }
      if (response.getStatus() != Status.OK.getStatusCode()) {
        System.err.println("Failed to revoke MCP API key: " + response.readEntity(String.class));
        return -1;
      }
      System.out.println("Revoked MCP API key " + keyId);
      return 0;
    } finally {
      if (response != null) {
        response.close();
      }
      destroyClient();
    }
  }

  private boolean verifySecurityEnabled() {
    if (UserGroupInformation.isSecurityEnabled()) {
      return true;
    }
    System.err.println("MCP API keys are only supported on secure clusters.");
    return false;
  }

  private Invocation.Builder newBuilder(WebTarget target) throws IOException {
    if (UserGroupInformation.isSecurityEnabled()) {
      return target.request(ACCEPT_XML_OR_JSON);
    }
    return target.queryParam("user.name", UserGroupInformation.getCurrentUser().getShortUserName())
        .request(ACCEPT_XML_OR_JSON);
  }

  private WebTarget initializeWebResource(String webAppAddress) {
    Configuration conf = getConf();
    if (YarnConfiguration.useHttps(conf)) {
      sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
    }
    client = createWebServiceClient(sslFactory);
    return client.target(webAppAddress);
  }

  private void destroyClient() {
    if (client != null) {
      client.close();
    }
    if (sslFactory != null) {
      sslFactory.destroy();
    }
  }

  private Client createWebServiceClient(SSLFactory clientSslFactory) {
    ClientConfig cfg = new ClientConfig();
    cfg.connectorProvider(new HttpUrlConnectorProvider().connectionFactory(url -> {
      AuthenticatedURL.Token token = new AuthenticatedURL.Token();
      AuthenticatedURL aUrl;
      HttpURLConnection conn;
      try {
        if (clientSslFactory != null) {
          clientSslFactory.init();
          aUrl = new AuthenticatedURL(null, clientSslFactory);
        } else {
          aUrl = new AuthenticatedURL();
        }
        conn = aUrl.openConnection(url, token);
      } catch (Exception e) {
        throw new IOException(e);
      }
      return conn;
    }));
    cfg.property(ClientProperties.CHUNKED_ENCODING_SIZE, null);
    return ClientBuilder.newClient(cfg);
  }

  private void printUsage() {
    System.out.println("yarn mcpapikey [-list]");
    System.out.println("yarn mcpapikey -create -ownerUser <user>");
    System.out.println("yarn mcpapikey -revoke <keyId>");
    System.out.println();
    System.out.println("Administer MCP API keys stored in the ResourceManager state store.");
    System.out.println("Requires a secure cluster (Kerberos/SPNEGO enabled).");
    System.out.println("Requires yarn.resourcemanager.mcp.enable=true and YARN admin access.");
  }
}

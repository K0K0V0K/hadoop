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

package org.apache.hadoop.yarn.server.resourcemanager.mcp;

import java.io.IOException;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.mcp.apikey.RMMcpApiKeyManager;
import org.apache.hadoop.yarn.server.resourcemanager.mcp.apikey.RMMcpApiKeyRecord;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebAppUtil;
import org.apache.hadoop.yarn.webapp.dao.McpApiKeyCreateRequest;
import org.apache.hadoop.yarn.webapp.dao.McpApiKeyInfo;
import org.apache.hadoop.yarn.webapp.dao.McpApiKeysInfo;
import org.apache.hadoop.yarn.webapp.BadRequestException;

/**
 * Admin REST endpoints for MCP API key lifecycle.
 *
 * <p>Registered only when MCP and cluster security are enabled. All endpoints
 * require YARN admin access via {@link RMWebAppUtil#verifyWritableAdminAccess}.</p>
 */
@Singleton
@Path(RMWSConsts.RM_WEB_SERVICE_PATH)
public class RMMcpApiKeyWebServices {

  private final ResourceManager rm;
  private final Configuration conf;
  private @Context HttpServletResponse response;

  /**
   * @param rm ResourceManager instance
   * @param conf cluster configuration
   */
  @Inject
  public RMMcpApiKeyWebServices(final @javax.inject.Named("rm") ResourceManager rm,
      final @javax.inject.Named("conf") Configuration conf) {
    this.rm = rm;
    this.conf = conf;
  }

  /**
   * Lists MCP API key metadata for the cluster.
   *
   * <p>Reachable via {@code GET} {@link RMWSConsts#MCP_API_KEYS}. Plaintext
   * secrets are never returned.</p>
   *
   * @param hsr the servlet request
   * @return metadata for all stored MCP API keys
   * @throws AuthorizationException if the caller is not a YARN admin
   */
  @GET
  @Path(RMWSConsts.MCP_API_KEYS)
  @Produces({MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8})
  public McpApiKeysInfo getMcpApiKeys(@Context HttpServletRequest hsr)
      throws AuthorizationException {
    verifyWritableAdminAccess(hsr);
    try {
      McpApiKeysInfo keysInfo = new McpApiKeysInfo();
      for (RMMcpApiKeyRecord record : rm.getRMMcpApiKeyManager().listKeys()) {
        keysInfo.add(toMcpApiKeyInfo(record, null));
      }
      return keysInfo;
    } catch (IOException e) {
      throw new YarnRuntimeException("Failed to list MCP API keys", e);
    }
  }

  /**
   * Creates a new MCP API key.
   *
   * <p>Reachable via {@code POST} {@link RMWSConsts#MCP_API_KEYS}. The
   * plaintext key is returned only in this response.</p>
   *
   * @param request create request containing the key owner
   * @param hsr the servlet request
   * @return created key metadata and the one-time plaintext secret
   * @throws AuthorizationException if the caller is not a YARN admin
   * @throws BadRequestException if the request is invalid
   */
  @POST
  @Path(RMWSConsts.MCP_API_KEYS)
  @Produces({MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8})
  @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public McpApiKeyInfo createMcpApiKey(McpApiKeyCreateRequest request,
      @Context HttpServletRequest hsr) throws AuthorizationException {
    UserGroupInformation callerUGI = verifyWritableAdminAccess(hsr);
    if (request == null) {
      throw new BadRequestException("Request body must be provided");
    }
    try {
      RMMcpApiKeyManager.CreateResult result = rm.getRMMcpApiKeyManager()
          .createKey(request.getOwnerUser(), callerUGI.getShortUserName());
      return toMcpApiKeyInfo(result.getRecord(), result.getApiKey());
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e.getMessage());
    } catch (IOException e) {
      throw new YarnRuntimeException("Failed to create MCP API key", e);
    }
  }

  /**
   * Revokes an MCP API key.
   *
   * <p>Reachable via {@code DELETE} {@link RMWSConsts#MCP_API_KEYS_KEYID}.
   * Performs a hard delete from the state store.</p>
   *
   * @param keyId MCP API key identifier
   * @param hsr the servlet request
   * @return HTTP 200 with {@code "revoked"} on success
   * @throws AuthorizationException if the caller is not a YARN admin
   * @throws BadRequestException if the key does not exist
   */
  @DELETE
  @Path(RMWSConsts.MCP_API_KEYS_KEYID)
  @Produces({MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8})
  public Response revokeMcpApiKey(@PathParam(RMWSConsts.KEY_ID) String keyId,
      @Context HttpServletRequest hsr) throws AuthorizationException {
    verifyWritableAdminAccess(hsr);
    try {
      rm.getRMMcpApiKeyManager().revokeKey(keyId);
      return Response.status(Status.OK).entity("revoked").build();
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e.getMessage());
    } catch (IOException e) {
      throw new YarnRuntimeException("Failed to revoke MCP API key", e);
    }
  }

  /**
   * Verifies that the caller has YARN admin access for writable REST endpoints.
   *
   * @param hsr the servlet request
   * @return the caller's {@link UserGroupInformation}
   * @throws AuthorizationException if the caller is not authorized
   */
  private UserGroupInformation verifyWritableAdminAccess(HttpServletRequest hsr)
      throws AuthorizationException {
    return RMWebAppUtil.verifyWritableAdminAccess(
        RMWebAppUtil.getCallerUserGroupInformation(hsr, true), rm, conf, response, true);
  }

  /**
   * Converts a persisted MCP API key record to the REST response DTO.
   *
   * @param record stored key metadata
   * @param apiKey plaintext secret, or {@code null} when not returned
   * @return REST representation of the key
   */
  private static McpApiKeyInfo toMcpApiKeyInfo(RMMcpApiKeyRecord record, String apiKey) {
    return new McpApiKeyInfo(record.getKeyId(), apiKey, record.getOwnerUser(),
        record.getCreatedBy(), record.getCreatedAt());
  }
}

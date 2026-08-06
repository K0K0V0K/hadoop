<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

Hadoop YARN - ResourceManager MCP
=================================

<!-- MACRO{toc|fromDepth=0|toDepth=2} -->

Overview
--------

The ResourceManager (RM) can expose a [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) endpoint for agent tool calls. MCP clients (for example AI agents or automation tools) discover and invoke **tools** that return YARN cluster state in a structured, machine-readable form.

For architecture, alternatives, and security analysis, see the [ResourceManager MCP Design Document](./ResourceManagerMcpDesign.html).

The feature is **opt-in** and **disabled by default**. When enabled, the RM starts a **dedicated MCP HTTP server** (default bind address `0.0.0.0:8092`) that serves JSON-RPC at `/ws/v1/mcp`. This port is separate from the main RM web application (typically `:8090` on secure clusters). The dedicated server does **not** install Kerberos/SPNEGO filters, so MCP clients authenticate tool calls with **API keys** instead of Kerberos tickets.

On **secure clusters** (Kerberos enabled), tool calls are authenticated with **admin-managed API keys** stored in the RM state store. API key administration (create/list/revoke) remains on the Kerberos-protected RM webapp REST API. On **unsecure clusters**, tool calls are open and intended for development only.

Security warnings
-----------------

Read these before enabling MCP in any environment reachable beyond a single developer workstation.

**Plain HTTP exposes API keys.** When `yarn.resourcemanager.mcp.use.https=false`, the full API key is sent in HTTP headers on every `tools/call`. Anyone on the network path (switches, proxies, packet capture) can replay it. Use HTTPS in production; reserve plain HTTP for local debugging only.

**Default bind address listens on all interfaces.** `yarn.resourcemanager.mcp.address` defaults to `0.0.0.0:8092`, so the MCP port is reachable from every network interface on the RM host unless firewalls block it. In production, bind to a specific address (for example `127.0.0.1:8092` behind a reverse proxy, or the RM's service IP) and restrict ingress with host firewall or security-group rules.

**Unsecure clusters have no tool authentication.** When `hadoop.security.authentication` is not `kerberos`, there is no API key manager and `tools/call` accepts anonymous requests. Tool handlers run as the **RM process user**, not the HTTP client. Never enable MCP on an unsecure cluster attached to a production or shared network.

**API keys do not expire.** Keys remain valid until an admin revokes them. There is no TTL, automatic rotation, or grace period. Rotate keys by creating a new key, updating clients, verifying access, then revoking the old key (see [Rotation](#rotation) below).

Architecture
------------

```
MCP client  --(API key)-->  dedicated MCP server (:8092, no SPNEGO)
                              POST /ws/v1/mcp  (initialize, tools/list, tools/call)

YARN admin  --(Kerberos)-->  RM webapp (:8090)
                              /ws/v1/cluster/mcp-api-keys  (create/list/revoke keys)
```

Goals
-----

* Let agents query RM state (applications, scheduler) without a separate sidecar service.
* Serve MCP on a dedicated port so clients can use API keys without Kerberos/SPNEGO.
* Reuse existing RM authorization, audit logging, and state store for API keys and tool execution.
* Keep the MCP transport layer reusable in `hadoop-common` (`McpHttpServer`, `McpServer`) for other components.
* Fail closed on secure clusters: no API key means no tool execution.

Non-goals
---------

* Submitting or modifying YARN applications via MCP.
* OAuth/JWT or per-request Kerberos delegation for MCP clients.
* Rate limiting or lockout on failed API key attempts.
* Federated or cross-RM MCP routing.

Configuration
-------------

| Property | Default | Description |
|----------|---------|-------------|
| `yarn.resourcemanager.mcp.enable` | `false` | Enable MCP tools and the dedicated MCP server on the ResourceManager |
| `yarn.resourcemanager.mcp.address` | `0.0.0.0:8092` | Bind address for the dedicated MCP server (API-key auth only; no Kerberos filters) |
| `yarn.resourcemanager.mcp.use.https` | `true` | When `true`, the dedicated MCP server uses HTTPS with the daemon SSL keystore; when `false`, plain HTTP |

MCP is only started when `yarn.resourcemanager.mcp.enable=true`. The API key manager and admin REST endpoints are created only when **both** MCP is enabled **and** cluster security is enabled (`hadoop.security.authentication=kerberos`).

To run the dedicated MCP server over plain HTTP (for example to avoid self-signed TLS issues in local MCP clients):

```xml
<property>
  <name>yarn.resourcemanager.mcp.use.https</name>
  <value>false</value>
</property>
```

**Warning:** plain HTTP sends API keys in cleartext. Do not use this on shared or production networks; see [Security warnings](#security-warnings).

The MCP tool endpoint is then reachable at `http://{rm-host}:8092/ws/v1/mcp`. Tool calls still require a valid MCP API key on secure clusters.

Endpoints
---------

### MCP tool endpoint (dedicated server)

```
POST http(s)://{rm-host}:{mcp-port}/ws/v1/mcp
Content-Type: application/json
```

The dedicated MCP server listens on `yarn.resourcemanager.mcp.address` (default port `8092`). Use `http://` when `yarn.resourcemanager.mcp.use.https=false`, otherwise `https://`. This endpoint is **not** served on the main RM webapp port.

Implements MCP streamable HTTP transport (JSON-RPC 2.0). Supported methods:

| Method | Auth on secure cluster | Description |
|--------|------------------------|-------------|
| `initialize` | None | Protocol handshake; returns server capabilities |
| `tools/list` | None | Lists registered tool names, descriptions, and input schemas |
| `tools/call` | **API key required** | Executes a tool and returns JSON text content |

On secure clusters, only `tools/call` enforces API key authentication. `initialize` and `tools/list` expose tool metadata only (no cluster data).

### Admin REST (secure clusters only, RM webapp)

These endpoints are registered on the **main RM web application** (same port and Kerberos/SPNEGO filters as other `/ws/v1/cluster/*` REST APIs), not on the dedicated MCP port:

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/ws/v1/cluster/mcp-api-keys` | List key metadata (secret not included) |
| `POST` | `/ws/v1/cluster/mcp-api-keys` | Create a key; plaintext returned **once** in the response |
| `DELETE` | `/ws/v1/cluster/mcp-api-keys/{keyId}` | Revoke (hard delete) a key |

Admin endpoints require the same authentication and authorization as other writable RM REST APIs: Kerberos/SPNEGO (or equivalent webapp filters) plus YARN admin ACLs via `RMWebAppUtil.verifyWritableAdminAccess`.

JSON request and response bodies use camelCase property names (for example `ownerUser`, `keyId`, `apiKey`). Create requests accept:

```json
{"ownerUser": "alice"}
```

Tools
-----

Tools are registered in `RMMcpServer` from a controller list. Each controller implements `buildTool()` (schema + metadata) and `buildResult()` (execution).

### `get_scheduler_info`

Returns YARN scheduler state (queues, capacities, usage, application counts) using the same DAOs as the RM web UI.

* **Input:** empty object
* **Authorization:** When YARN ACLs are enabled, only YARN admins may call this tool.

### `list_applications`

Lists YARN applications with optional filters (states, user, queue, time range, types, tags, name, limit). Reuses `ApplicationsRequestBuilder` and `AppInfo` from the RM webapp.

* **Authorization:**
  * Respects `yarn.filter-entities-by-user` and per-application VIEW ACLs.
  * Non-admin callers cannot filter by another user's applications.

Adding a new tool
-----------------

1. Subclass `AbstractMcpController`.
2. Implement `buildTool()` and `buildResult()`.
3. Add the controller instance to `RMMcpServer.controllers()`.

The MCP server builder loop registers each controller's tool with `RMMcpToolExecutor` automatically.

Security model
--------------

### Secure clusters

```
Agent  --(API key header)-->  POST http(s)://{rm-host}:8092/ws/v1/mcp  --tools/call-->
  RMMcpToolExecutor.resolveCallerUgi()
    -> RMMcpApiKeyManager.authenticate()
    -> UserGroupInformation.createRemoteUser(ownerUser)
    -> callerUgi.doAs(() -> controller.buildResult(...))
```

* The dedicated MCP server has no SPNEGO filter; only application-layer API key auth applies on `tools/call`.
* API keys map to an **owner user**. Tool handlers run as that user via `doAs`, and YARN ACL checks apply.
* Invalid and missing keys both produce `"User not authenticated"` (no oracle).
* Plaintext secrets are never stored; only PBKDF2 hash + salt per key.
* MCP tool calls are audited via `RMAuditLogger` (`MCP Tool Call` operation).

### Unsecure clusters

* No API key manager, no admin REST, `yarn mcpapikey` CLI exits early.
* `tools/call` runs without authentication.
* ACL checks use `UserGroupInformation.getCurrentUser()`, which is the **RM process user**, not the HTTP client. **Do not expose unsecure MCP on production networks.**

### API key transport

Clients may send the full key (`ymcp_{keyId}_{secret}`) using either header:

```
X-Yarn-Mcp-Api-Key: ymcp_abc123_...
```

or

```
Authorization: ApiKey ymcp_abc123_...
```

These are two transport formats for the same credential, not duplicates.

API key lifecycle
-----------------

### Key format

```
ymcp_{keyId}_{secret}
```

* `keyId`: UUID hex (no dashes), used as the state store key.
* `secret`: 32 random bytes, URL-safe Base64 (no padding).

### Storage

Keys are persisted in `RMStateStore` under `McpApiKeysRoot` (implementation-specific path per backend: ZK, HDFS, LevelDB, memory). Each record stores:

| Field | Description |
|-------|-------------|
| `keyId` | Public identifier |
| `ownerUser` | Unix/Kerberos short name tools run as |
| `createdBy` | Admin who created the key |
| `createdAt` | Creation timestamp (ms) |
| `secretHash` | `pbkdf2-sha256:` + Base64(PBKDF2 output) |
| `salt` | Per-key salt (URL-safe Base64) |

The plaintext secret is returned **only** at creation time (REST POST or CLI `-create`). List operations return metadata with `apiKey: null`.

### Revocation

Revoke performs a **hard delete** from the state store. There is no soft-revoke or expiry field.

### Rotation

Keys have no built-in expiry. Admins should rotate them on a schedule or after suspected compromise:

1. Create a new key for the same `ownerUser` (`POST /ws/v1/cluster/mcp-api-keys` or `yarn mcpapikey -create`).
2. Update every MCP client with the new plaintext secret.
3. Verify `tools/call` succeeds with the new key.
4. Revoke the old key (`DELETE .../{keyId}` or `yarn mcpapikey -revoke`).

Until step 4 completes, both keys are valid. There is no overlap window or staged cutover in the server — plan client updates accordingly.

### Crypto

* Algorithm: `PBKDF2WithHmacSHA256`
* Key length: 256 bits
* Iterations: 10,000 (fixed; part of the `pbkdf2-sha256:` scheme)
* Verification: constant-time `MessageDigest.isEqual`
* JCE provider and secure random honor Hadoop crypto configuration (FIPS-aware when configured)

Administration
--------------

### CLI

```bash
yarn mcpapikey -list
yarn mcpapikey -create -ownerUser <user>
yarn mcpapikey -revoke <keyId>
yarn mcpapikey -h
```

The CLI validates arguments first, then checks that the cluster is secure, then contacts the active RM admin REST endpoint via SPNEGO. Use `-h` to print usage.

### Example: create and use a key

```bash
# As YARN admin on a secure cluster (Kerberos to RM webapp)
yarn mcpapikey -create -ownerUser alice

# Agent calls MCP on the dedicated port (tools/call; API key only, no Kerberos)
curl -X POST "https://rm-host:8092/ws/v1/mcp" \
  -H "Content-Type: application/json" \
  -H "X-Yarn-Mcp-Api-Key: ymcp_..." \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "tools/call",
    "params": {
      "name": "list_applications",
      "arguments": {"limit": "10"}
    }
  }'
```

Use `http://` instead of `https://` when `yarn.resourcemanager.mcp.use.https=false`. MCP clients such as Cursor should point at `http(s)://{rm-host}:8092/ws/v1/mcp`, not the RM webapp URL on `:8090`.

Audit logging
---------------

Every `tools/call` request produces an RM audit event:

* **Success:** user = key owner (or `UNKNOWN` on unsecure), operation = `MCP Tool Call`, details include tool name and filter arguments.
* **Failure:** auth failure, execution exception, or tool-level error (`isError=true`).

Auth failures on secure clusters are audited as failures before the tool runs.

Startup and shutdown
--------------------

In `ResourceManager` initialization, when `yarn.resourcemanager.mcp.enable=true`:

1. If cluster security is enabled, create `RMMcpApiKeyManager` and register `RMMcpApiKeyWebServices` on the RM webapp `ResourceConfig` (admin REST only).
2. Create `RMMcpServer` and start `McpHttpServer` on `yarn.resourcemanager.mcp.address` (HTTPS or plain HTTP per `yarn.resourcemanager.mcp.use.https`).
3. On RM shutdown, close the MCP HTTP server.

The MCP **tool** endpoint uses the dedicated Jetty server and does not share the RM webapp's SPNEGO filters. The MCP **admin** REST endpoints use the RM webapp port, TLS policy, and Kerberos filters like other cluster admin APIs.

Related documentation
---------------------

* [ResourceManager MCP Design Document](./ResourceManagerMcpDesign.html)
* [Introduction to YARN web services REST APIs](./WebServicesIntro.html)
* [ResourceManager REST APIs](./ResourceManagerRest.html)
* [YARN Commands Reference](./YarnCommands.html#mcpapikey)

Related code
------------

| Area | Location |
|------|----------|
| MCP framework | `hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/mcp/` |
| Dedicated MCP HTTP server | `McpHttpServer` in `hadoop-common` |
| RM MCP integration | `hadoop-yarn-server-resourcemanager/.../resourcemanager/mcp/` |
| Admin REST | `RMMcpApiKeyWebServices` on the RM webapp |
| State store keys | `RMStateStore.storeMcpApiKey*` |
| Configuration | `YarnConfiguration.RM_MCP_*` |
| CLI | `hadoop-yarn-client/.../McpApiKeyCLI.java` |

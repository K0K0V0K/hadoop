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

ResourceManager MCP — Design Document
=====================================

---

## 1. Abstract

This document describes the design for an optional **Model Context Protocol (MCP)** endpoint on the YARN ResourceManager (RM). MCP clients (AI agents, IDE integrations, automation) can call **tools** that return structured YARN cluster state (scheduler queues, applications, and future read-only APIs).

The RM serves MCP on a **dedicated HTTP/HTTPS port** (default `:8092`) using a **minimal, Hadoop-native MCP framework** in `hadoop-common` (`org.apache.hadoop.mcp`), not the official [MCP Java SDK](https://github.com/modelcontextprotocol/java-sdk). That framework implements the MCP streamable HTTP / JSON-RPC subset required for tools while fitting Hadoop’s existing Jetty, Jackson, and servlet stack. On secure clusters, **tool calls** authenticate with **admin-managed API keys**; key administration stays on the existing Kerberos-protected RM webapp REST API.

---

## 2. Background and motivation

### 2.1 Problem

Operators and developers increasingly use AI agents to inspect and troubleshoot clusters. Those agents need:

* Machine-readable access to RM state (queues, apps, metrics).
* A stable, documented protocol (MCP) rather than ad-hoc shell scripts.
* Authentication that works from tools **outside** the Kerberos trust domain (e.g. Cursor, CI bots on laptops).

The RM already exposes rich REST APIs under `/ws/v1/cluster/*`, but they are designed for human-operated clients with Kerberos/SPNEGO on secure clusters. MCP agents typically speak HTTP JSON-RPC and cannot practically obtain Kerberos tickets for every tool invocation.

### 2.2 Constraints

* **Secure clusters** must not expose cluster data without authentication.
* **YARN ACLs** must still apply (view apps as owner user, admin-only scheduler views when ACLs are on).
* **Auditability** — tool calls should appear in RM audit logs.
* **Minimal blast radius** — feature disabled by default; no change to default RM behavior when off.
* **Reuse** — leverage existing RM DAOs, `ApplicationsRequestBuilder`, scheduler info objects, and `RMStateStore`.
* **ASF-friendly dependencies** — avoid pulling in MCP reference SDK stacks that conflict with Hadoop’s servlet/Jersey/Jackson versions (see §5.5 and §6.2).

### 2.3 Why a dedicated MCP framework in `hadoop-common`

The [Model Context Protocol](https://modelcontextprotocol.io/) defines wire behavior (JSON-RPC methods, streamable HTTP, session headers). The reference [Java SDK](https://github.com/modelcontextprotocol/java-sdk) (`mcp-core`, transport modules, Jackson bindings) targets a **Jakarta-centric** stack (Jakarta servlet/RS APIs, Project Reactor for HTTP transport, and SDK-owned server wiring).

Hadoop already ships its own HTTP infrastructure (`HttpServer2`, Jetty, Jersey on the RM webapp, MOXy/JAXB JSON providers, `javax.servlet` in parts of `hadoop-common`). Prototyping with the official SDK and embedding MCP as a Jersey resource on the RM webapp hit **integration problems**:

| Issue | Impact |
|-------|--------|
| **Jakarta vs `javax.servlet`** | RM/YARN webapps use Jakarta servlet APIs; `hadoop-common` MCP servlet uses `javax.servlet` for broad compatibility with existing Jetty wiring — mixing SDK Jakarta types with Hadoop’s split stack caused friction |
| **Jersey + SDK JSON types** | RM webapp registers MOXy providers for YARN DTOs; MCP JSON-RPC bodies as `JsonNode` did not deserialize reliably alongside existing providers (503 / missing `MessageBodyReader`) |
| **Extra transport dependencies** | SDK HTTP transport pulls Reactor and SDK-specific server abstractions; Hadoop prefers minimal deps and direct Jetty connectors (`McpHttpServer`) |
| **Filter/auth coupling** | SDK-on-webapp still sat behind SPNEGO; dedicated servlet server was simpler than adapting SDK lifecycle to RM filters |

**Decision:** implement a **small, protocol-compatible MCP layer** in `hadoop-common` that speaks the same JSON-RPC methods clients expect (`initialize`, `tools/list`, `tools/call`) but uses Hadoop’s Jackson, servlet, and Jetty patterns. YARN RM code (`RMMcpServer`, controllers, API keys) builds on that layer.

This follows the **protocol spec**, not the **reference SDK implementation**. Other Hadoop daemons can reuse `McpServer` / `McpHttpServer` without adopting Jakarta Reactor or the full SDK classpath.

---

## 3. Goals

| Goal | How |
|------|-----|
| Agent-friendly RM queries | MCP tools wrapping existing RM webapp logic |
| No Kerberos for MCP clients | Dedicated MCP server without SPNEGO filters; API keys on `tools/call` |
| Secure by default on Kerberos clusters | Fail closed: missing/invalid key → `"User not authenticated"` |
| YARN ACL compatibility | Key maps to `ownerUser`; handlers run under `callerUgi.doAs()` |
| Admin-controlled credentials | Create/list/revoke keys via YARN admin REST + `yarn mcpapikey` CLI |
| Reusable transport | Protocol-compatible `org.apache.hadoop.mcp` in `hadoop-common` (not the official Jakarta SDK) |
| Opt-in | `yarn.resourcemanager.mcp.enable=false` by default |

---

## 4. Non-goals

* Submitting, killing, or modifying applications via MCP.
* OAuth, JWT, or Kerberos delegation tokens for MCP clients.
* Rate limiting, lockout, or expiry on API keys (hard revoke only).
* MCP key management **via** MCP tools (bootstrap problem — keys created via admin REST).
* Federated / cross-RM MCP routing.
* Running MCP on the main RM webapp port with SPNEGO bypass (replaced by dedicated port).
* Full parity with the official MCP Java SDK (resources, prompts, sampling, stdio, Reactor transport).

---

## 5. Alternatives considered

### 5.1 MCP on the RM webapp (`:8090`) with SPNEGO

**Approach:** Register MCP as a Jersey resource on the existing RM `ResourceConfig` at `/ws/v1/mcp`.

**Pros:** Single port; reuses HttpServer2 TLS and filters.

**Cons:** On secure clusters, SPNEGO runs before MCP code. API-key-only clients get **401** unless we bypass SPNEGO for the MCP path. Even with bypass, Jersey/MOXy JSON binding for MCP request bodies was fragile alongside existing RM providers.

**Decision:** Rejected for production MCP clients. Dedicated `McpHttpServer` on `:8092` with no Kerberos filters.

### 5.2 SPNEGO bypass on `/ws/v1/mcp` only

**Approach:** Teach `RMAuthenticationFilter` to skip SPNEGO for the MCP path; enforce API keys in `RMMcpToolExecutor`.

**Pros:** Smaller diff; one port.

**Cons:** Still coupled to RM webapp filter stack, CSRF, and TLS policy; harder for agents to configure; mixed security model on one listener.

**Decision:** Superseded by dedicated server (cleaner separation of admin REST vs agent MCP).

### 5.3 Sidecar MCP proxy (Python/Node service)

**Approach:** External service with kinit/keytab that proxies to RM REST.

**Pros:** No RM code changes.

**Cons:** Extra deployment, credential handling on proxy host, not integrated with RM state store or audit logging.

**Decision:** Rejected for upstream; useful for dev clusters only.

### 5.4 Bearer JWT for MCP

**Pros:** Standard for some agent platforms.

**Cons:** Key distribution, rotation, and RM integration complexity; replaced with API keys stored in `RMStateStore`.

**Decision:** Rejected.

### 5.5 Official MCP Java SDK ([modelcontextprotocol/java-sdk](https://github.com/modelcontextprotocol/java-sdk))

**Approach:** Depend on `io.modelcontextprotocol.sdk:mcp-core` (+ Jackson/Reactor transport modules) and register SDK server types on the RM or a standalone process.

**Pros:** Spec tracking maintained upstream; less custom JSON-RPC code; familiar to MCP ecosystem developers.

**Cons:**

* **Jakarta-based** APIs and transports that do not align cleanly with Hadoop’s mixed `javax`/`jakarta` servlet usage and Jetty `HttpServer2` patterns.
* **Classpath / provider conflicts** with RM Jersey (MOXy vs Jackson MCP bodies, duplicate JSON binding stacks).
* **Heavier dependency tree** (e.g. Reactor) for a read-only tool surface.
* **Operational coupling** when embedded in the Kerberos-filtered RM webapp (SPNEGO before MCP during webapp-based prototyping).

**Decision:** Rejected. Ship a **minimal Hadoop MCP framework** that implements the required MCP HTTP/JSON-RPC behavior using existing Hadoop dependencies. Re-evaluate upstream SDK only if Hadoop’s servlet/Jakarta story converges and the SDK offers a thin, embeddable transport without Reactor.

---

## 6. Proposed design

### 6.1 High-level architecture

```
┌─────────────────────┐         ┌──────────────────────────────────────────┐
│  MCP client         │  HTTP   │  ResourceManager                         │
│  (Cursor, agent)    │ ──────► │  McpHttpServer (:8092, no SPNEGO)        │
│                     │  API key│    POST /ws/v1/mcp                       │
└─────────────────────┘         │      ├─ initialize / tools/list (open)   │
                                │      └─ tools/call → RMMcpToolExecutor   │
                                │              └─ doAs(ownerUser)          │
                                │                                          │
┌─────────────────────┐         │  RM webapp (:8090, SPNEGO)               │
│  YARN admin         │  SPNEGO │    /ws/v1/cluster/mcp-api-keys (CRUD)    │
│  (CLI / REST)       │ ──────► │    other /ws/v1/cluster/* REST           │
└─────────────────────┘         └──────────────────────────────────────────┘
                                              │
                                              ▼
                                    RMStateStore (API key records)
```

Two HTTP surfaces:

| Surface | Port | Auth | Purpose |
|---------|------|------|---------|
| **MCP tool server** | `yarn.resourcemanager.mcp.address` (default `8092`) | API key on `tools/call` only | Agent JSON-RPC |
| **RM webapp** | RM webapp HTTPS/HTTP port (e.g. `8090`) | Kerberos + YARN admin ACLs | API key admin REST |

### 6.2 MCP framework (`hadoop-common`)

We implement MCP **protocol compatibility** in Hadoop rather than embedding the reference SDK. Package: `org.apache.hadoop.mcp`.

#### Design principles

1. **Spec-compatible wire format** — JSON-RPC 2.0 over MCP streamable HTTP; `MCP-Session-Id` header; methods `initialize`, `tools/list`, `tools/call`.
2. **Hadoop-native I/O** — Jackson (`ObjectMapper` / `JsonNode`) already used across the project; no SDK-specific DTO layer required in YARN.
3. **Thin servlet transport** — `McpHttpServlet` reads/writes JSON directly on a dedicated Jetty listener.
4. **Standalone Jetty server** — `McpHttpServer` uses raw Jetty + `SSLFactory`, same keystore story as other Hadoop daemons, with optional plain HTTP.

#### Components

| Component | Role |
|-----------|------|
| `McpSchema` / `McpToolSchema` | Tool metadata and JSON Schema helpers |
| `McpJsonMapper` / `JacksonMcpJsonMapper` | Pluggable JSON serialization for tool results |
| `McpServer` | Builder for server metadata, tool registry, servlet |
| `McpRequestHandler` | Transport-neutral JSON-RPC dispatch and session handling |
| `McpHttpServlet` | `javax.servlet` entry point; passes `HttpServletRequest` into `McpCallContext` |
| `McpHttpServer` | Standalone Jetty server; HTTPS (`SSLFactory`) or plain HTTP (`useHttps=false`) |

#### Protocol surface

**Protocol:** MCP streamable HTTP transport, JSON-RPC 2.0. Supported protocol versions include `2024-11-05`, `2025-03-26`, `2025-06-18`.

**Not implemented in the framework (current scope):** resources, prompts, sampling, stdio transport, full SDK lifecycle — only **tools** over HTTP are required for RM agents.

**Transport choice for RM:** `McpHttpServer.start(mcpServer, conf, bindAddress, "/ws/v1/mcp", useHttps)`.

### 6.3 RM integration (`RMMcpServer`)

When `yarn.resourcemanager.mcp.enable=true`:

1. **`ResourceManager.startWepApp()`**
   * If security enabled: `RMMcpApiKeyManager.create(this)` (uses `RMStateStore`).
   * `RMMcpServer.create(rm)` — registers tools from controllers.
   * `rmMcpServer.startHttpServer(conf)` — binds dedicated port.

2. **`RMWebApp.resourceConfig()`**
   * If MCP enabled **and** security enabled: register `RMMcpApiKeyWebServices` (admin REST only).

3. **Shutdown:** `RMMcpServer.close()` stops Jetty and destroys SSLFactory if used.

**Tool registration pattern:**

```java
for (AbstractMcpController controller : controllers(rm, jsonMapper)) {
  Tool tool = controller.buildTool();
  builder.toolCall(tool, (context, args) ->
      RMMcpToolExecutor.execute(apiKeyManager, tool.name(), controller, context, args));
}
```

Controllers implement:

* `buildTool()` — name, description, JSON Schema for arguments.
* `buildResult(McpCallContext, Map<String,Object>)` — business logic; returns `CallToolResult` with JSON text.

**Built-in tools:**

| Tool | Controller | Data source |
|------|------------|-------------|
| `get_scheduler_info` | `SchedulerMcpController` | `CapacitySchedulerInfo` / Fair / FIFO DAOs |
| `list_applications` | `ApplicationsMcpController` | `ApplicationsRequestBuilder`, `AppInfo`, ACL filtering |

### 6.4 Request flow (`tools/call`)

```
1. Client POST /ws/v1/mcp with JSON-RPC tools/call
2. McpHttpServlet → McpRequestHandler → registered handler
3. RMMcpToolExecutor.execute():
     a. Read X-Yarn-Mcp-Api-Key or Authorization: ApiKey ...
     b. RMMcpApiKeyManager.authenticate(key) → UGI for ownerUser
     c. If secure && UGI null → error "User not authenticated" + audit failure
     d. callerUgi.doAs(() -> controller.buildResult(...))
     e. Audit success/failure via RMAuditLogger (MCP Tool Call)
4. JSON-RPC result with text/json content
```

**Headers accepted for API key:**

```
X-Yarn-Mcp-Api-Key: ymcp_{keyId}_{secret}
Authorization: ApiKey ymcp_{keyId}_{secret}
```

`Authorization: Bearer ...` is **not** supported (distinct from ApiKey scheme).

### 6.5 API key lifecycle

**Format:** `ymcp_{keyId}_{secret}`

* `keyId` — UUID hex, state store key.
* `secret` — 32 random bytes, URL-safe Base64.

**Storage (`RMStateStore`):** serialized `RMMcpApiKeyRecord` with PBKDF2 hash (`pbkdf2-sha256:...`), salt, ownerUser, createdBy, createdAt. Plaintext secret returned **once** at creation.

**Admin operations (secure clusters only):**

| Operation | Interface |
|-----------|-----------|
| Create | `POST /ws/v1/cluster/mcp-api-keys` body `{"ownerUser":"alice"}` |
| List | `GET /ws/v1/cluster/mcp-api-keys` |
| Revoke | `DELETE /ws/v1/cluster/mcp-api-keys/{keyId}` |

### 6.6 Authorization

| Tool | Rule |
|------|------|
| `get_scheduler_info` | If YARN ACLs enabled → YARN admin only |
| `list_applications` | Per-app VIEW ACLs; respects `yarn.filter-entities-by-user`; non-admins cannot set `user` filter to another user |

Authorization uses the **API key owner** as the effective user, not the RM daemon user.

### 6.7 TLS and plain HTTP

| Property | Default | Behavior |
|----------|---------|----------|
| `yarn.resourcemanager.mcp.use.https` | `true` | HTTPS via daemon `SSLFactory` / keystore |
| `false` | Plain HTTP on bind address | For dev or when MCP clients cannot trust cluster self-signed certs |

Plain HTTP does **not** disable API key requirement on secure clusters.

---

## 7. Configuration reference

| Property | Default | Description |
|----------|---------|-------------|
| `yarn.resourcemanager.mcp.enable` | `false` | Master switch for MCP server and tools |
| `yarn.resourcemanager.mcp.address` | `0.0.0.0:8092` | Bind address for dedicated MCP server |
| `yarn.resourcemanager.mcp.use.https` | `true` | HTTPS vs plain HTTP for MCP server |

Example — enable MCP with plain HTTP:

```xml
<property>
  <name>yarn.resourcemanager.mcp.enable</name>
  <value>true</value>
</property>
<property>
  <name>yarn.resourcemanager.mcp.use.https</name>
  <value>false</value>
</property>
```

Example — Cursor `mcp.json`:

```json
{
  "mcpServers": {
    "yarn-rm-mcp": {
      "url": "http://rm-host:8092/ws/v1/mcp",
      "headers": {
        "X-Yarn-Mcp-Api-Key": "${env:YARN_MCP_API_KEY}"
      }
    }
  }
}
```

---

## 8. Security considerations

### 8.1 Threat model (secure cluster)

| Threat | Mitigation |
|--------|------------|
| Unauthenticated cluster reads | API key required on `tools/call` |
| Stolen API key | Revoke via admin REST; key scoped to `ownerUser` ACLs |
| Key brute force | PBKDF2 hashed secrets; no user oracle on invalid keys |
| Network eavesdropping | Default HTTPS on MCP port; operators should not expose `:8092` to untrusted networks without TLS |
| Tool metadata leakage | `tools/list` is unauthenticated — exposes tool names/schemas only |
| Privilege escalation via key owner | Admin chooses `ownerUser` at create time; follows normal YARN ACLs |

### 8.2 Unsecure clusters

No API key manager. **Do not enable MCP on production unsecure networks** — all tool calls run as the RM process user for ACL purposes.

### 8.3 Separation of duties

* **Cluster admins** manage keys (Kerberos + YARN admin ACL).
* **Agent operators** hold API keys (environment variables, secret stores).
* MCP port should be firewalled to trusted agent networks even with API keys.

---

## 9. References

* [Model Context Protocol specification](https://modelcontextprotocol.io/) (wire protocol we implement)
* [MCP Java SDK](https://github.com/modelcontextprotocol/java-sdk) (reference implementation — **not** used due to Jakarta/integration constraints; see §5.5)
* [ResourceManager MCP user guide](./ResourceManagerMcp.html)
* Code: `hadoop-common/.../mcp/`, `hadoop-yarn-server-resourcemanager/.../resourcemanager/mcp/`

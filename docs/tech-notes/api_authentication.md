# CockroachDB API authn/authz mechanics

Each node has 2 API surfaces:

- HTTP APIs
- RPC APIs

Then, also each RPC API endpoint can perform fan-out queries to RPC
endpoints on other nodes.

So we have the following API request stacks.
Go object names are in package `server` unless otherwise specified.

<!-- markdown-toc start - Don't edit this section. Run M-x markdown-toc-refresh-toc -->
**Table of Contents**

- [CockroachDB API authn/authz mechanics](#cockroachdb-api-authnauthz-mechanics)
    - [HTTP Services](#http-services)
        - [Unauthenticated pure HTTP handler](#unauthenticated-pure-http-handler)
        - [Unauthenticated HTTP -> RPC using grpc-gateway](#unauthenticated-http---rpc-using-grpc-gateway)
        - [Authenticated pure HTTP handler, with SQL authorization](#authenticated-pure-http-handler-with-sql-authorization)
        - [Authenticated HTTP -> RPC using grpc-gateway, with SQL authorization](#authenticated-http---rpc-using-grpc-gateway-with-sql-authorization)
        - [Authenticated HTTP -> RPC using grpc-gateway, without SQL authorization](#authenticated-http---rpc-using-grpc-gateway-without-sql-authorization)
        - [Local direct HTTP handler, followed by RPC->RPC fanout](#local-direct-http-handler-followed-by-rpc-rpc-fanout)
        - [HTTP -> RPC using grpc-gateway, followed by RPC->RPC fanout](#http---rpc-using-grpc-gateway-followed-by-rpc-rpc-fanout)
    - [Pure RPC services](#pure-rpc-services)
        - [RPC requested by CLI tools or external clients](#rpc-requested-by-cli-tools-or-external-clients)
            - [The `SendKVBatch()` oddity](#the-sendkvbatch-oddity)
        - [KV server accessing KV APIs](#kv-server-accessing-kv-apis)
        - [SQL server accessing SQL APIs internally](#sql-server-accessing-sql-apis-internally)
        - [SQL server for system tenant accessing KV APIs](#sql-server-for-system-tenant-accessing-kv-apis)
        - [SQL server for a secondary tenant accessing KV APIs](#sql-server-for-a-secondary-tenant-accessing-kv-apis)
    - [Common desirable properties](#common-desirable-properties)
    - [Mixed APIs with confusing/problematic semantics](#mixed-apis-with-confusingproblematic-semantics)
        - [RPC API endpoints used via a direct Go call](#rpc-api-endpoints-used-via-a-direct-go-call)
        - [Mixed tenant/tenant and tenant/KV APIs with misused SQL authentication](#mixed-tenanttenant-and-tenantkv-apis-with-misused-sql-authentication)
        - [Privileged, mixed usage tenant/tenant and tenant/KV APIs](#privileged-mixed-usage-tenanttenant-and-tenantkv-apis)
    - [Towards a brighter future](#towards-a-brighter-future)

<!-- markdown-toc end -->


## HTTP Services

### Unauthenticated pure HTTP handler

HTTP -> direct HTTP handler

Example:

```
/_status/vars
-> (http.ServeMux) ServeHTTP()
-> (varsHandlers) handleVars()
```

Note: only very few handlers (health, prometheus vars) are served
without authentication. Generally, we use authentication for all HTTP
APIs. The only endpoints that do not use authentication are those
required by orchestration tools.

### Unauthenticated HTTP -> RPC using grpc-gateway

HTTP -> grpc-gateway -> local RPC handler

Example:

```
/_admin/v1/health
-> (http.ServeMux) ServeHTTP()
-> (*gwruntime.ServeMux) ServeHTTP()
-> gRPC loopback dial, inside same server
-> (rpc.kvAuth) AuthUnary()
-> (*adminServer) Health()
```

This works because the gRPC loopback dial uses the TLS `node` cert
in-memory. then `rpc.kvAuth` finds that its (local, in-memory) peer is
`node` and lets the RPC go through.

### Authenticated pure HTTP handler, with SQL authorization

HTTP -> HTTP authn -> direct HTTP handler

Example:

```
/api/v2/events`
-> (http.ServeMux) ServeHTTP()
-> (*authenticationV2Mux) ServeHTTP()
-> (*apiV2Server) listEvents()
```

The final handler cares about the SQL identity.
This is achieved as follows:

- `authenticationV2Mux` checks the *session cookie* then stores the
  identity in the `context.Context` using
  `context.WithValue(webSessionUserKey{})`.
- `listEvents()` checks the identity by calling `userFromHTTPAuthInfoContext()`
  which looks at `context.Value(webSessionUserKey{})`
- `listEvents()` then authorizes the request based on the SQL identity.

### Authenticated HTTP -> RPC using grpc-gateway, with SQL authorization

HTTP -> HTTP authn -> grpc-gateway -> local RPC handler

Example:

```
/_admin/v1/trace_snapshots
-> (http.ServeMux) ServeHTTP()
-> (*authenticationMux) ServeHTTP()
-> (*gwruntime.ServeMux) ServeHTTP()
-> gRPC loopback dial, inside same server
-> (rpc.kvAuth) AuthUnary()
-> (*adminServer) ListTracingSnapshots()
```

The final handler cares about the SQL identity.
This is achieved as follows:

- `authenticationMux` checks the *session cookie* then stores the
  identity in the `context.Context` using
  `context.WithValue(webSessionUserKey{})`.
- the grpc-gateway service is initialized with
  `gwruntime.WithMetadata(translateHTTPAuthInfoToGRPCMetadata)` which
  causes `translateHTTPAuthInfoToGRPCMetadata` to be called on every RPC.
  This function, in turn, copies the SQL identity from
  `context.Value`'s `webSessionUserKey{}` into a gRPC "outgoing
  context" at metadata key `"websessionuser"`
- gRPC performs a loopback call into the same server using the `node`
  TLS cert.
- gRPC loopback dial automatically forwards the `metadata.MD`
  in the outgoing context to become MD in the incoming context on
  the callee RPC handler.
- `(rpc.kvAuth).AuthUnary()` ignores this metadata. Instead it
  merely verifies that the incoming RPC is coming from another
  CockroachDB node or a `root`/`node` RPC client (node-to-node RPC
  authentication).
- `ListTracingSnapshots()` calls `getUserAndRole()` calls
  `userFromIncomingRPCContext()` which looks up `"websessionuser"` from the
  gRPC incoming context metadata.
- `ListTracingSnapshots()` then authorizes the request based on the SQL identity.

### Authenticated HTTP -> RPC using grpc-gateway, without SQL authorization

Generally, most HTTP services not only verify the user is authenticated using
a valid SQL identity; they also verify the *privileges* of that user.

A few services do *not* do this and always work regardless of the user privileges.

Example:

```
/ts/query
-> (http.ServeMux) ServeHTTP()
-> (*authenticationMux) ServeHTTP()
-> (*gwruntime.ServeMux) ServeHTTP()
-> gRPC loopback dial, inside same server
-> (rpc.kvAuth) AuthUnary()
-> (*ts.Server) Query()
```

Like above, `authenticationMux` does authentication places the
identity into gRPC metadata, but `Query()` never uses it.

It is assumed that any user who can log in can access the Query
endpoint and fetch timeseries data.

Other examples: `/_admin/v1/liveness` (`Liveness()`)
`/_admin/v1/uidata` (`GetUIData` / `SetUIData`)

### Local direct HTTP handler, followed by RPC->RPC fanout

HTTP -> HTTP authn -> local HTTP handler -> fanout RPC on other nodes.

Example:

```
/api/v2/sessions
-> (http.ServeMux) ServeHTTP()
-> (*authenticationV2Mux) ServeHTTP()
-> (*apiV2Server) listSessions()
-> (*statusServer) paginatedIterateNodes()
-> gRPC dial, remote call
-> (other node) (rpc.kvAuth).AuthUnary()
-> (other node) (*statusServer) ListLocalSessions()
```

The final handler cares about the SQL identity of the original query.
This is achieved as follows:

- `authenticationV2Mux` checks the *session cookie* then stores the
  identity in the `context.Context` using
  `context.WithValue(webSessionUserKey{})`.
- `listSessions()` on first node uses `forwardHTTPAuthInfoToRPCCalls()` to
  copy the SQL identity from `context.Value`'s `webSessionUserKey{}`
  into a gRPC "outgoing context" at metadata key `"websessionuser"`
  (`metadata.NewOutgoingContext(ctx,
  translateHTTPAuthInfoToGRPCMetadata(ctx))`)
- gRPC dial using `node` TLS cert + remote calls automatically
  forwards the `metadata.MD` in the outgoing context to become MD in
  the incoming context on the remote node.
- `(rpc.kvAuth).AuthUnary()` ignores this metadata. Instead it
  merely verifies that the incoming RPC is coming from another
  CockroachDB node or a `root`/`node` RPC client (node-to-node RPC
  authentication).
- `ListLocalSessions()` calls `getUserAndRole()` calls
  `userFromIncomingRPCContext()` which looks up `"websessionuser"` from the
  gRPC incoming context metadata.

### HTTP -> RPC using grpc-gateway, followed by RPC->RPC fanout

HTTP -> HTTP authn -> grpc-gateway -> local RPC handler -> fanout RPC on other nodes

Example:

```
/_admin/v1/sessions
-> (http.ServeMux) ServeHTTP()
-> (*authenticationMux) ServeHTTP()
-> (*gwruntime.ServeMux) ServeHTTP()
-> gRPC loopback dial, inside same server
-> (rpc.kvAuth) AuthUnary()
-> (*statusServer) ListSessions()
-> (*statusServer) paginatedIterateNodes()
-> gRPC dial, remote call
-> (other node) (rpc.kvAuth).AuthUnary()
-> (other node) (*statusServer) ListLocalSessions()
```

The final handler cares about the SQL identity.
This is achieved as follows:

- `authenticationMux` checks the *session cookie* then stores the
  identity in the `context.Context` using
  `context.WithValue(webSessionUserKey{})`.
- the grpc-gateway service is initialized with
  `gwruntime.WithMetadata(translateHTTPAuthInfoToGRPCMetadata)` which
  causes `translateHTTPAuthInfoToGRPCMetadata` to be called on every RPC.
  This function, in turn, copies the SQL identity from
  `context.Value`'s `webSessionUserKey{}` into a gRPC "outgoing
  context" at metadata key `"websessionuser"`
- gRPC performs a loopback call into the same server using
  the TLS `node` cert.
- gRPC loopback dial automatically forwards the `metadata.MD`
  in the outgoing context to become MD in the incoming context on
  the callee RPC handler.
- `(rpc.kvAuth).AuthUnary()` ignores this metadata. Instead it
  merely verifies that the incoming RPC is coming from another
  CockroachDB node or a `root`/`node` RPC client (node-to-node RPC
  authentication).
- `(*statusServer) ListSessions()` calls `getUserAndRole()` calls
  `userFromIncomingRPCContext()` which looks up `"websessionuser"` from the
  gRPC incoming context metadata, and performs SQL authz.
- `ListSessions` then fans-out. For this, it first calls
  `forwardSQLIdentityThroughRPCCalls()` which copies
  `"websessionuser"` from the gRPC incoming metadata to the outgoing
  context metadata.
- gRPC dial using TLS `node` cert + remote calls automatically
  forwards the `metadata.MD` in the outgoing context of the 1st node
  to become MD in the incoming context on the remote node.
- remote `(rpc.kvAuth).AuthUnary()` also ignores this metadata.
  Instead it merely verifies that the incoming RPC is coming from
  another CockroachDB node or a `root`/`node` RPC client (node-to-node
  RPC authentication).
- remote `ListLocalSessions()` also calls `getUserAndRole()` calls
  `userFromIncomingRPCContext()` which looks up `"websessionuser"` from the gRPC
  incoming context metadata, that has been forwarded.

## Pure RPC services

These services do not have a HTTP endpoint.

### RPC requested by CLI tools or external clients

CLI RPC client -> RPC authn -> local RPC handler

Example:

```
cockroach init
-> gRPC dial, remote call
-> (other node) (rpc.kvAuth).AuthUnary()
-> (*initServer) Bootstrap()
```

This works as follows:
- the client uses a `root` or `node` TLS cert.
- `(rpc.kvAuth).AuthUnary()` verifies that the incoming TLS client
  has a valid cert for `root` or `node`.
- the RPC call is let through. There is no further check in the RPC
  handler.

In general, most RPC API endpoints exposed to external clients are
limited to this very simple model: the `rpc.kvAuth` component checks
the client has a `root` or `node` certs and the API has no checks of
its own.

#### The `SendKVBatch()` oddity

`cockroach debug send-kv-batch` is a pure RPC client, calling
`(*systemAdminServer) SendKVBatch()` remotely.

The code for `SendKVBatch()` calls `userFromIncomingRPCContext()` to then call
`hasAdminRole()`.

`userFromIncomingRPCContext()` looks up the username from the gRPC metadata.

Because this is a pure RPC call and the `cockroach debug
send-kv-batch` client never adds this metadata, `userFromIncomingRPCContext()`
finds no metadata to work with. In that case, by an accident of
history, `userFromIncomingRPCContext()` assumes the client is `root`. This is why
`SendKVBatch()` happens to work. This behavior is arguably a bug,
identified a long time ago:
https://github.com/cockroachdb/cockroach/issues/45018

### KV server accessing KV APIs

KV server -> KV RPC endpoint

Example:

```
node initialization code
-> gRPC dial, remote call
-> (other node) (rpc.kvAuth).AuthUnary()
-> (*Node) Join() on other node
```

This works as follows:
- the KV server acts as a client and uses its `node` certificate to
  perform a RPC dial.
- `(rpc.kvAuth).AuthUnary()` verifies this TLS identity then lets the
  RPC go through. The RPC doesn't further use the identity of the
  client.

### SQL server accessing SQL APIs internally

SQL server -> SQL server, but the initial request is not due to a
HTTP->RPC gateway call.

Example:

```
distsql execution
-> gRPC dial, remote call
-> (other node) (rpc.kvAuth).AuthUnary()
-> (*distsql.ServerImpl) SetupFlow() on other node
```

This works as follows:

- The SQL server uses its own `node` cert (in case it's for the system
  tenant) or the tenant client cert (if it's a SQL pod) to dial the
  remote node.
- `(rpc.kvAuth).AuthUnary()` asserts the incoming TLS identity is that
  of a `node` client or a SQL server for the same tenant ID as the
  local tenant ID. The RPC doesn't further use the identity of the
  client.

### SQL server for system tenant accessing KV APIs

SQL server -> KV RPC endpoint

Example:

```
txn client distsender
-> gRPC dial, remote call
-> (other node) (rpc.kvAuth).AuthUnary()
-> (*Node) Batch() on other node
```

This works as follows:
- the SQL server acts as a client and uses its `node` certificate to
  perform a RPC dial.
- `(rpc.kvAuth).AuthUnary()` verifies this TLS identity then lets the
  RPC go through. The RPC doesn't further use the identity of the
  client.

### SQL server for a secondary tenant accessing KV APIs

SQL server -> KV RPC endpoint

Example:

```
tenant connector
-> gRPC dial, remote call
-> (other node) (rpc.kvAuth).AuthUnary()
-> (*Node) GossipSubscription()
```

This works as follows:
- the SQL server acts as a client and uses its TLS tenant client
  certificate to perform a RPC dial.
- `(rpc.kvAuth).AuthUnary()` verifies this TLS identity. It sees the
  client is for a tenant server, then additionally verifies the
  parameters of the request. Then it lets the RPC go through. The RPC
  doesn't further use the identity of the client.

## Common desirable properties

All the RPC API patterns in the previous section neatly fall under
either of 3 categories:

- endpoints for DB console or low-privileged external tools (e.g.
  `EnqueueRange`), which may or may not have a HTTP
  alias via grpc-gateway.

- privileged endpoints which can only be used by a `root` or `node`
  trusted client, for example `SendKVBatch`, `Bootstrap` or distsql
  `SetupFlow()`. These are used both by CLI admin tools and in
  server-to-server internal RPCs.

- KV APIs meant for use by lower-privileged SQL servers for secondary
  tenants. For example, `TokenBucket`.

These 3 categories all have the following properties in common:

- *they have a single authentication rule* regardless of which process/service serves them.
- *EITHER they care about the SQL identity, in which case they only
  perform tenant-scoped work; OR (exclusive) they do not care about
  the SQL identity, only the tenant ID.*

We can verify this as follows:

- Well-scoped DB Console endpoints (e.g. `EnqueueRange`) always use
  the same HTTP authentication (1st property) and they only validate
  the SQL role/privileges within the confines of the tenant that
  serves them (2nd property). For example:
  - `EnqueueRange` on a secondary tenant is only used by DB Console
    connected to the secondary tenant, and just needs a valid session cookie
    *for that tenant service*.
  - `EnqueueRange` on the system tenant is only used by DB Console
    connected to the secondary tenant, and just needs a valid session
	cookie *for that tenant service*.

  Those that perform RPC fan-outs (e.g. `ListSessions`) still have
  the property: they use the same HTTP Authn (1st property)
  and the fan-out still occurs within the same tenant ID,
  so fan-out SQL priv checks are within the same tenant (2nd property).

- privileged endpoints:
  - Pure KV RPC endpoints, e.g. `Bootstrap` etc. check a TLS
    client cert valid for `root` or `node` on system tenant (1st&2nd properties).
  - `SetupFlow` etc are available both on system tenant and secondary
    tenant but only need a TLS client cert valid *for the same tenant*
    (1st&2nd properties).

- cross-tenant-boundary RPCs are always from a secondary tenant
  to a KV node. They have always the same rule:
  - the RPC client must identify itself as a secondary tenant _server_.
    (single authn rule, 1st property)
  - the RPC service handler, once authentication has succeeded,
    doesn't care further about the identity of the tenant.
  - there is no SQL identity in the connection.	(2nd property)

## Mixed APIs with confusing/problematic semantics

By an accident of history, we have created API endpoints that have mixed-purpose and
have more complex properties (and thus much harder to reason about).

### RPC API endpoints used via a direct Go call

We've designed all our RPC API endpoints with the expectation they would
be called through the gRPC stack, and they peek into their incoming
context to get authentication details with that assumption.

Alas...

Consider `pkg/sql/cancel_queries.go`, called from the (internal)
execution of a `CANCEL QUERY` statement.

We see:
```
(*cancelQueriesNode) Next()
-> (*extendedEvalContext).SQLStatusServer.CancelQuery()
-> (serverpb.SQLStatusErver).CancelQuery()
-> (statusServer) CancelQuery()
```

Now consider that this latter Go function was implemented as a RPC handler.
It calls `checkCancelPrivilege()` which calls `getUserAndRole`
which calls `userFromIncomingRPCContext()`

`userFromIncomingRPCContext()` looks up the username from the gRPC metadata.
(As would be expected for HTTP handlers served by grpc-gateway, see
previous sections).

Because this is a pure Go call from SQL and `(*cancelQueriesNode)
Next()` never adds this metadata, `userFromIncomingRPCContext()` finds no
metadata to work with. In that case, by an accident of history,
`userFromIncomingRPCContext()` assumes the client is `root`. This is why
`CancelQuery()` as directly from SQL happens to work.

This behavior is arguably a bug, identified a long time ago:
https://github.com/cockroachdb/cockroach/issues/45018

Due to this bug, **every function listed in the `SQLStatusServer` interface
bypasses authorization** (because from their perspective, the
incoming call appears to be run by `root`), unless the author
of the RPC handler was cognizant of this limitation and did something special.

The reason why we seem to be OK with this is that:

- in some cases, the engineer who created this integration foresaw
  this ambiguity and created an interlock of sorts in the callee RPC
  handler. This is e.g. what happened with the `CancelQuery()` handler
  above.

- in other cases, the *callers* of these functions are inside the SQL
  layer and *also* perform an authorization check of their own.
  This is e.g. what happened with `ListExecutionInsights()` as called
  from `crdb_internal` vtable.

Arguably, this integration is brittle, as we are one step away from
someone forgetting to add an authz check.

### Mixed tenant/tenant and tenant/KV APIs with misused SQL authentication

These are API endpoints that can be used both "within their own tenant scope"
and across the KV/tenant API boundary.

For example, `TenantRanges` may be used:

- by DB Console on secondary tenant, uses HTTP authn: `(*statusServer) TenantRanges()`

  API handler asserts privileges on SQL incoming identity from client
  *within the secondary tenant*.

- by DB Console on system tenant, uses HTTP authn. `(*systemStatusServer) TenantRanges()`

  API handler asserts privileges on SQL incoming identity from client
  *within the system tenant*.

- as an API forward from the secondary tenant to system tenant,
  requires on privilege escalation:

  ```
  (*statusServer) TenantRanges()` verifies SQL identity locally
  -> (*serverpb.TenantStatusServer).TenantRanges()
  -> (*kvtenantccl.Connector) TenantRanges()
  -> gRPC dial, remote call
  -> (rpc.kvAuth) AuthUnary(), verifies tenant identity
  -> (*systemStatusServer) TenantRanges()
  ```

  In this case, something odd happens: `(*systemStatusServer)
  TenantRanges()` calls `requireAdminUser()` which calls
  `getUserAndRole()` which calls `userFromIncomingRPCContext()`.

  `userFromIncomingRPCContext()` looks up the username from the gRPC metadata.
  (As would be expected for HTTP handlers served by grpc-gateway, see
  previous sections).

  Because this is a pure RPC call and the `(*kvtenantccl.Connector)
  TenantRanges()` never adds this metadata, `userFromIncomingRPCContext()` finds
  no metadata to work with. In that case, by an accident of history,
  `userFromIncomingRPCContext()` assumes the client is `root`. This is why
  `TenantRanges()` as called through the tenant/KV interface happens
  to work.

  This behavior is arguably a bug, identified a long time ago:
  https://github.com/cockroachdb/cockroach/issues/45018

Similar problems can be found in `HotRangesV2()` and perhaps others.

### Privileged, mixed usage tenant/tenant and tenant/KV APIs

Like abov, these are API endpoints that can be used both "within their
own tenant scope" and across the KV/tenant API boundary.
However, the situation is slightly better *because they do not perform
authorization.*

For example, `Query` may be used:

- by DB Console on secondary tenant, uses HTTP authn: `(*ts.TenantServer) Query()`

  API handler doesn't care about SQL identity.

- by DB Console on system tenant, uses HTTP authn. `(*ts.Server) Query()`

  API handler doesn't care about SQL identity.

- as an API forward from the secondary tenant to system tenant,
  requires a privilege escalation:

  ```
  (*ts.TenantServer) Query() doesn't care about SQL identity
  -> (tspb.TenantTimeSeriesServer).Query()
  -> (*kvtenantccl.Connector) Query()
  -> gRPC dial, remote call
  -> (rpc.kvAuth) AuthUnary(), verifies *tenant identity*
  -> (*ts.Server) Query() doesn't care about SQL identity
  ```

Another example of this is `Liveness()`.

This stacking is *brittle* however: if, later, we decide to *add*
authorization to the service (e.g. to add an observability SQL
privilege), we would fall back into the case above with
broken/confused authorization semantics.

## Towards a brighter future

- It's very weird that we use the same KV authentication
  component (`rpc.kvAuth`) to authenticate gRPC loopback
  dials within the same server with grpc-gateway. This also
  forces the use of a full TLS handshake for these in-memory
  loopback dials, which adds latency and memory usage. Maybe
  we should find a way to simplify the grpc-gateway integration.

- APIs that depend on information added through an authn layer
  should fail when they don't find it. For example, `SendKVAuthn`
  should see its indirect call to `userFromIncomingRPCContext` fail if
  we decide that `userFromIncomingRPCContext` only works after HTTP
  authentication.

  (This would identify when we've failed to forward authn
  bits during RPC fanouts.)

- Handlers meant to be used for external RPCs should only be called
  directly as a go function from the SQL layer if the SQL layer also
  passes all the required authn bits. i.e. the SQL layer
  should "authenticate itself as a RPC client" to call the
  functions in the `SQLStatusServer` interface.

  For example, the calls to `CancelQuery()` in `pkg/sql` should pass
  the SQL identity of the calling session as the RPC client identity
  through the call.


- Pure privileged RPC APIs that assume that they are only
  ever reachable using a `node` cert (KV-to-KV RPCs) should
  fail if reached from a client conn identified as non-`node`.

  (This would identify *missing authz checks*).

- APIs that need a valid SQL identity should assert that it is present
  and fail if it is not there. This requires us to split mixed-purpose
  APIs like `TenantRanges` into:

  - one endpoint that serves external clients and verifies the SQL
    identity and performs authorization based on SQL privileges;

  - *another* endpoint exposed through `*kvtenantccl.Connector` which
    doesn't need to check the SQL identity because it is only
    ever guaranteed to be called via the tenant/KV interface
    with a valid `node` cert or tenant client cert.

  (This would make the semantics easier to understand and to
  audit for correctness.)

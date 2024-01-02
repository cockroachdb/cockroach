- Feature Name: Shared-process deployments for virtual clusters
- Status: completed
- Start Date: 2022-07-20
- Authors: knz
- RFC PR: [#84700](https://github.com/cockroachdb/cockroach/pull/84700)
- Cockroach Issue: Jira CRDB-14537

# Summary

We propose to instantiate SQL servers for virtual clusters (henceafter
abbreviated "VC", a.k.a. "secondary tenants") on-the-fly inside a
pre-running CockroachDB process.

One key feature is that the VC SQL server (and its accompanying HTTP
service) now starts dynamically, potentially after the process has
been running for a while already. This will help us orchestrate
failover during VC-to-VC replication.

Additionally, this change will help the following:

- SQL pods in CockroachCloud Serverless, as a way to separate the step
  of starting up a process and the step to select a tenant ID to serve.
  This will help towards lowering our initial connection
  latency for customers.

- Regular CockroachDB nodes, as a way to serve a VC
  "on the fly", after the process has been started already.
  This will help cluster operators /
  SREs performing SQL operations "on behalf" of arbitrary
  VCs. It might also help with building a well secured
  Observability Service architecture.

We will do this by introducing a dynamic SQL server registry inside the
CockroachDB process, and special routing logic for HTTP and SQL connections
over the network to avoid a proliferation of network listeners.

The following topics are left out of scope in this RFC, and will be
tracked by subsequent work and other RFCs:

- how to separate log files for different VCs.
- [how timeseries will work](https://github.com/cockroachdb/cockroach/pull/85954).
- [VC capabilities](https://github.com/cockroachdb/cockroach/pull/85954).

<!-- markdown-toc start - Don't edit this section. Run M-x markdown-toc-refresh-toc -->
**Table of Contents**

- [Summary](#summary)
- [Motivation](#motivation)
- [Technical design](#technical-design)
    - [VC names](#vc-names)
    - [VC server registry](#vc-server-registry)
    - [Access via a single SQL listener](#access-via-a-single-sql-listener)
    - [Access via a single RPC interface](#access-via-a-single-rpc-interface)
    - [Access via a single HTTP interface](#access-via-a-single-http-interface)
    - [Performance of non-system VCs](#performance-of-non-system-vcs)
    - [Memory and CPU usage](#memory-and-cpu-usage)
    - [Usage in `demo` and testing](#usage-in-demo-and-testing)
    - [Behavior in mixed-version deployments](#behavior-in-mixed-version-deployments)
    - [Security considerations](#security-considerations)
        - [HTTP security](#http-security)
        - [SQL security](#sql-security)
    - [Drawbacks](#drawbacks)
    - [Alternatives Considered](#alternatives-considered)
        - [Cross-VC SQL](#cross-vc-sql)
- [Explain it to folk outside of your team](#explain-it-to-folk-outside-of-your-team)
- [Unresolved questions](#unresolved-questions)

<!-- markdown-toc end -->


# Motivation

The motivation for this architectural change is as follows:

- to power an active/passive online replication failover, so that a
  running CockroachDB replication sink can spawn up a SQL server for
  the replication target dynamically to turn it into an 'active'
  server.

  For this use case, we want the SQL server(s) to not be running while
  CockroachDB is a replication sink, and only start running during
  failover. (With an option to switch them back off without server
  restart if we invert the replication direction.)

  We would also like to orchestrate this failover without
  necessitating starting (or restarting) a `cockroach` process nor
  changing command-line flags. Restarting a process and/or changing
  configuration are heavyweight and error-prone. By
  instantiating the new SQL server dynamically, we can verify
  beforehand that the process is pre-started successfully, that its
  TLS/networking config is valid, that its logging works, etc.

- to allow SREs and other privileged cluster operators to perform
  operations "on behalf" of an arbitrary VC, for example to
  observe its SQL schema, perform repairs or help troubleshooting
  issues.

  For this use case, we want to enable a user with access to
  a storage cluster to perform SQL operations for a VC without
  the need to spawn a separate process to run a SQL server.

- in the upcoming Observability Service Architecture, we might want to
  create a data architecture which strongly partitions the data
  belonging to each organization owning a cluster being observed, using
  multi-tenancy to create boundaries between organizations.

  For this use case, we want the ability to issue observability queries
  across different VCs *concurrently* on the same storage cluster,
  without the need to spawn separate SQL server processes.

- to power a default experience of cluster virtualization for all CockroachDB clusters,
  based on a separate "application" VC for client workloads,
  especially in Cockroach Cloud Dedicated.

  For this use cases, we want the SQL server(s) to run in-process, so as
  to avoid the overhead of network serialization when full resource
  isolation is not needed.


# Technical design

CockroachDB processes will become able to serve SQL and HTTP APIs for
VCs selected at run-time (i.e. possibly long after the process has
started already), using server data structures inside the same process.

This will be done using a new *VC server registry* inside each
CockroachDB node. This new object will be responsible for starting up
(and shutting down) VC servers as needed.

When a VC server is started up in a shared process, it becomes
available in two ways:

- unconditionally, for use by internal APIs inside the same
  CockroachDB process. This can be used e.g. by the KV layer when it
  needs to perform a SQL operation. This will also be used to power
  the ability to "run operations on behalf of another VC" (see
  [below](#running-sql-on-behalf-of-other-vcs)).

- over the network via one of the external listeners
  (TCP or unix sockets). This provides access to external clients.

## VC names

An important conceptual change is the introduction of *VC names*,
to provide stable identifiers that map to tenant IDs.

We need VC names for three reasons:

- we will want users to distinguish their "application" VC
  from the system/admin VC. Yet, when we introduce this app
  VC in previous-version clusters, we can't guarantee that
  a specific tenant ID will be available. It would be cumbersome
  for operator experience and in documentation to explain
  "how to retrieve the ID of the application VC".

- it will make it easier to manipulate VCs during BACKUP/RESTORE
  and VC-to-VC replication: it will introduce the possibility
  to restore an app VC into a separate ID while foreground traffic
  is still using the previous ID; test the result of the restore, then "swap out"
  the VC by changing the ID that the app VC's name refers to.

- when we later introduce a migration to cluster virtualization for
  previous-version clusters, we will need to pivot the system VC
  (with ID 1) out of its role as a "special privileged VC", to
  become the new application VC; and elevate another
  VC (with some non-deterministic ID) to the status of "special
  privileged VC". From that point forward, we will need
  a stable name to refer to the "system VC" regardless of its ID.

VC names will be introduced via a new UNIQUE column in `system.tenants`.

An entry will also be materialized for the system VC and a
predefined name for clusters that don't have an admin/system VC
entry already.

## VC server registry

The registry is conceptually a map from VC name to VC SQL server.

Each VC SQL server will be instantiated upon first use, as long as
its State attribute [2] is set to ACTIVE.

There will be a reference counter (incremented by internal SQL
queries and network connection), and the registry would stop a SQL
server some time after its reference count drops to zero.

[2] the State attribute (ACTIVE, ADD, DROP) indicates whether a VC
is ready to serve SQL queries.

## Access via a single SQL listener

Conceptually, we could instantiate each VC SQL server with its own
TCP/unix listener (1 network interface per VC). However, we have
multiple reasons not to:

- we would like the new architecture model to be usable as an upgrade
  to existing clusters, without change to orchestration rules
  (including firewalling and port redirects).

- existing test tooling typically runs multiple nodes side-by-side on
  the same machine and (currently) allocates these ports
  sequentially—e.g. 26257, 26258, etc. Adding new listeners means
  we need to rewrite these scripts to number the ports differently.

- when we implement the pivot migration mentioned above, we'll want to
  "swap" the system VC and application VC, in-place while
  foreground traffic is running. We want this to be possible without
  rewiring which SQL server is connected to which network
  listener.

So, we will instead aim to use a single SQL listener for the various VC
servers running inside the shared process.

How this will work:

- we will extract the pre-authentication handshake from
  `(*pgwire.Server).ServeConn()` into a new, separate
  `pgwire.Connector` component, which will handle connection setup for
  all shared-process VC servers.

- this new component will be responsible for routing cancel
  requests, accepting/requesting the TLS handshake, and receiving the
  client status parameters.

- it will detect which VC the connection should be routed to:

  - if the `options` status param has a `cluster` parameter, this will
    be used to route the connection.

  - otherwise, if a TLS client cert is provided in the TLS handshake
    and there is a single *VC scope* included in the TLS cert [4],
    this will be used to route the connection.

  - otherwise, a default VC will be selected. For now this will
    be the system/admin VC.

  (we might introduce additional routing mechanisms later.)

- once the VC is selected, the `net.Conn` object is passed to
  `(*pgwire.Server).serveConn()` as usual. Each VC will have its
  `pgwire.Server`, also as usual.

[4] VC scopes inside TLS client certs are a feature introduced in
v22.1. See https://github.com/cockroachdb/cockroach/pull/79065 for details.

## Access via a single RPC interface

SQL servers communicate with each other over RPC to organize query
execution via DistSQL. It is thus important that two SQL servers
running in separate VMs for a single tenant ID can address each other.

We also wish to use a single network listener (TCP port) for the RPC
connections of all SQL servers running in the process.

To separate the RPC traffic, we will ensure that the RPC traffic for
separate VCs go over separate TCP connections. We will achieve
this as follows:

- on the outgoing path, we will extend the `connKey` type in the RPC
  connection registry with the tenant ID, to force the RPC dial
  mechanism to use a separate connection for separate tenant IDs.

- we will use a `PerRPCCredential` payload to insert the tenant ID into
  outgoing RPCs.

- on the server side, when receiving RPCs we will use the
  PerRPCCredential payload to route the request to the appropriate
  server.

Note: Andrei has suggested that the PerRPCCredential mechanism, which
uses gRPC metadata key/values under the hood, may become a performance
bottleneck. If we observe this to be true in practice, we will be able
to optimize this using a custom dialer and a cmux instead.

## Access via a single HTTP interface

For similar reasons as above, we will want to offer a single HTTP listener.

To distinguish which VC should serve the HTTP API, we will use a
new cookie named `tenant`, which should be set to the desired VC
name.

When the cookie is not present, the same default as in SQL will be
assumed (also system/admin VC for now).

Internally, the HTTP service will route incoming requests to a
different HTTP request mux depending on the `tenant` cookie, using the
VC server registry to look up + instantiate the server responsible
for the request.

For the `/api/v2` tree, we'll also consider adding routing via a
custom header (`X-...`).

## Performance of non-system VCs

We want to ensure that shared-process VC servers do not incur the
performance overhead of a networked gRPC call when they run in-process
with the KV layer, even through a loopback connection.

For the system/admin VC, we already support this via the so-called
"internal client" (see `pkg/rpc/context.go`).

We will recycle this mechanism for all shared-process VC servers that
share a process with the KV layer, so that the corresponding RPC calls
are performed using direct Go function calls.

This mechanism will not be used when the dynamic VC servers
run in a separate process ("SQL pod" via `cockroach mt start-sql`).

## Memory and CPU usage

Each idle SQL server (when not currently serving queries) consumes
less than 10MB of RAM, and has no CPU impact.

Therefore, we consider the dynamic instantiation of per-VC SQL
servers to have negligible performance impact on foreground traffic.

This infrastructure is also meant for use in the Dedicated /
Self-hosted deployment styles, where there is no expectation of
resource constraints for VCs. This means we do not need an
architectural solution to segregate CPU/memory usage of in-process
VC servers from each other.

## Usage in `demo` and testing

Once the VC server registry is implemented, we can leverage this
in `cockroach demo` and testing:

- `demo` can instantiate a VC named `app` like regular clusters.

  Using the shared SQL/HTTP listeners will also fix long-standing
  UX bugs around cluster virtualization in `demo`.

- the "test VC" in tests can be redirected to use the VC
  registry internally (instead of a custom VC instantiation code),
  with e.g. a VC record named `test`.

## Behavior in mixed-version deployments

The instantiation of non-system VCs via the registry would be
blocked until the `name` column is added to `system.tenants`.

The addition of this column would occur via an upgrade migration, in
turn gated by a cluster version.

This implies that the availability of shared-process VCs
would be blocked until all nodes have been upgraded to the latest
version and the `system.tenants` upgrade has completed.

## Security considerations

### HTTP security

In the initial case, we will use a single CA and server TLS
certificate for the (shared) HTTP interface. This means it will
not yet be possible to use e.g. separate TLS CAs for the
"application" and "system" VCs. This separation could still be
achieved later by e.g. instantiating separate HTTP listeners, or using
virtual hosts / SNI. This avenue is out of scope for the RFC.

Regarding user authentication, the proposed approach uses the VC
cookie to first select the VC, then each VC HTTP service
handles its authentication separately.

This means that conceptually each VC has its own users and login page.

There is a plan to share the authentication cookie across VCs, to
provide a rudimentary form of cross-VC SSO. The internal
documentation page explaining this is
[here](https://cockroachlabs.atlassian.net/wiki/spaces/MEVT/pages/2522186473/Multitenant+DB+Console+Authn+Authz#Ad-hoc%3A-shared-internal-identities-based-on-username%2Bpassword). This
scheme is however out of scope for this RFC.

### SQL security

Like for HTTP above, in the initial case we will use a single CA and
server TLS certificate for the (shared) SQL interface.  As above, this
will prevent separating the CA / TLS provisioning per VC, but we
can add this feature later via separate listeners (see [this
issue](https://github.com/cockroachdb/cockroach/issues/54006)).

Regarding authentication, the VC is selected first, and then the
entire connection is routed to that VC. This means that SQL
authentication is fully scoped "under" the selected VC.

## Drawbacks

The execution model is rendered somewhat more complex.
We consider this to be acceptable in light of the other benefits.

## Alternatives Considered


### Cross-VC SQL

We could also leverage the in-process registry to perform SQL operations
"on behalf" of another VC, from a SQL session connected to a primary VC.

For this we could introduce the following new SQL syntax:

`AS VIRTUAL CLUSTER <name> <stmt>`, e.g. `AS VIRTUAL CLUSTER app SELECT * FROM system.users`

or, alternatively,

`AS VIRTUAL CLUSTER <name> <string>`  (where the string is a quoted SQL statement), e.g. `AS VIRTUAL CLUSTER app $$SELECT * FROM system.users$$`

The run-time behavior of this statement would be to look up the
corresponding SQL server using the registry, let it instantiate if
needed, and then use its internal executor to run the desired
statement and retrieve query results (if any).

**Note:** there are many edge cases to consider in this candidate new
feature, such as "what is the authorization model?", "how much of the
parent session do we propagate to the child session?", "which SQL user
do we use in the target VC session?", etc.

We chose to delay adding this feature until we gain more experience
with "VC hopping" using `\VC` in the CLI interactive shell.

# Explain it to folk outside of your team

With this change enabled, CockroachDB Dedicated and Self-Hosted now
supports an "application" VC inside every node, by
default.

For the time being, use of the application VC is opt-in; by
default, connections are routed to the "system" (also internally
called "admin") VC, like in previous single-VC deployments.

To access this application VC via SQL, for now a special parameter
must be passed in connection URLs
(e.g. `postgres://...&options=--cluster=app`).

# Unresolved questions

N/A

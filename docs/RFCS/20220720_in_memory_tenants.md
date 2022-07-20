- Feature Name: Dynamic in-memory tenant servers
- Status: draft
- Start Date: 2022-07-20
- Authors: knz
- RFC PR: [#84700](https://github.com/cockroachdb/cockroach/pull/84700)
- Cockroach Issue: Jira CRDB-14537

# Summary

We propose to instantiate SQL servers for multiple tenants inside the
same CockroachDB process serving the KV layer.

This change is meant to (eventually) power a multitenant experience in
all CockroachDB clusters, not just CockroachCloud Serverless. In the
shorter term, it will also help with passive->active failover in
cluster-to-cluster replication. It also will help cluster operators /
SREs performing SQL operations "on behalf" of arbitrary
tenants. Finally, it might also help with building a well secured
Observability Service architecture.

We will do this by introducing a dynamic SQL server registry inside the
CockroachDB node, and special routing logic for HTTP and SQL connections
over the network to avoid a proliferation of network listeners.

The following topics are left out of scope in this RFC, and will be
tracked by subsequent work and other RFCs:

- how to separate log files for different tenants.
- how timeseries will work.

<!-- markdown-toc start - Don't edit this section. Run M-x markdown-toc-refresh-toc -->
**Table of Contents**

- [Summary](#summary)
- [Motivation](#motivation)
- [Technical design](#technical-design)
    - [Tenant names](#tenant-names)
    - [Tenant capabilities](#tenant-capabilities)
    - [Tenant server registry](#tenant-server-registry)
    - [Access via a single SQL listener](#access-via-a-single-sql-listener)
    - [Access via a single HTTP interface](#access-via-a-single-http-interface)
    - [Running SQL on behalf of other tenants](#running-sql-on-behalf-of-other-tenants)
        - [Usage from CLI shell](#usage-from-cli-shell)
        - [(Optional) Cross-tenant SQL](#optional-cross-tenant-sql)
    - [Performance of non-system tenants](#performance-of-non-system-tenants)
    - [Memory and CPU usage](#memory-and-cpu-usage)
    - [Usage in `demo` and testing](#usage-in-demo-and-testing)
    - [Behavior in mixed-version deployments](#behavior-in-mixed-version-deployments)
    - [Security considerations](#security-considerations)
        - [HTTP security](#http-security)
        - [SQL security](#sql-security)
    - [Drawbacks](#drawbacks)
    - [Alternatives Considered](#alternatives-considered)
- [Explain it to folk outside of your team](#explain-it-to-folk-outside-of-your-team)
- [Unresolved questions](#unresolved-questions)

<!-- markdown-toc end -->



# Motivation

The motivation for this architectural change is as follows:

- to power a default multitenant experience for all CockroachDB clusters,
  based on a separate "application" tenant for client workloads,
  especially in Cockroach Cloud Dedicated.

  For this use cases, we want the SQL server(s) to run in-process, so as
  to avoid the overhead of network serialization when full resource
  isolation is not needed.

- to power an active/passive online replication failover, so that a
  running CockroachDB replication sink can spawn up a SQL server for
  the replication target dynamically to turn it into an 'active'
  server.

  For this use case, we want the SQL server(s) to not be running while
  CockroachDB is a replication sink, and only start running during
  failover. (With an option to switch them back off without server
  restart if we invert the replication direction.)

- to allow SREs and other privileged cluster operators to perform
  operations "on behalf" of an arbitrary tenant, for example to
  observe its SQL schema, perform repairs or help troubleshooting
  issues.

  For this use case, we want to enable a user with access to
  a storage cluster to perform SQL operations for a tenant without
  the need to spawn a separate process to run a SQL server.

- in the upcoming Observability Service Architecture, we might want to
  create a data architecture which strongly partitions the data
  belonging to each organization owning a cluster being observed, using
  multi-tenancy to create boundaries between organizations.

  For this use case, we want the ability to issue observability queries
  across different tenants *concurrently* on the same storage cluster,
  without the need to spawn separate SQL server processes.

# Technical design

CockroachDB processes with KV capability [1] will become able to serve
SQL and HTTP APIs for arbitrary tenants, using in-memory server data
structures. (Currently, they only serve 1 tenant: the "system tenant".)

This will be done using a new *tenant server registry* inside each
CockroachDB node. This new object will be responsible for starting up
(and shutting down) tenant servers as needed.

When an in-memory tenant server is started up, it becomes available in two ways:

- unconditionally, for use by internal APIs inside the same
  CockroachDB process. This can be used e.g. by the KV layer when it
  needs to perform a SQL operation. This will also be used to power
  the ability to "run operations on behalf of another tenant" (see
  [below](#running-sql-on-behalf-of-other-tenants)).

- conditionally, over the network via one of the external listeners
  (TCP or unix sockets). This provides access to external clients.

The condition in the latter case refers to the idea that not every
tenant will get availability over the network. In a default
deployment, a maximum of 2 of these in-memory tenants will be
available to external network clients: one for application workloads
and one for system operators. This will power the new "multi-tenant
everywhere" deployment model for Cockroach Cloud Dedicated, and
self-hosted deployments. Serving more tenants over the network will be
restricted via a license check, reserved for exclusive use inside
Cockroach Cloud.

[1] the processes with KV capability are those started with `cockroach
start` / `start-single-node`, and possibly `cockroach demo`.

## Tenant names

An important conceptual change is the introduction of *tenant names*,
to provide stable identifiers that map to tenant IDs.

We need tenant names for three reasons:

- we will want users to distinguish their "application" tenant
  from the system/admin tenant. Yet, when we introduce this app
  tenant in previous-version clusters, we can't guarantee that
  a specific tenant ID will be available. It would be cumbersome
  for operator experience and in documentation to explain
  "how to retrieve the ID of the application tenant".

- when we later introduce a migration to multi-tenancy for
  previous-version clusters, we will need to pivot the system tenant
  (with ID 1) out of its role as a "special privileged tenant", to
  become the new application tenant; and elevate another secondary
  tenant (with some non-deterministic ID) to the status of "special
  privileged tenant". From that point forward, we will need
  a stable name to refer to the "system tenant" regardless of its ID.

- it will make it easier to manipulate tenants during BACKUP/RESTORE:
  it will introduce the possibility to restore an app tenant into a
  separate ID while foreground traffic is still using the previous ID;
  test the result of the restore (using the new "run as arbitrary
  tenant" facility), then "swap out" the tenant by changing the ID that
  the app tenant's name refers to.

Tenant names will be introduced via a new UNIQUE column in `system.tenants`.

An entry will also be materialized for the system tenant with ID 1 and a
predefined name.

## Tenant capabilities

Another conceptual change is the introduction of *tenant capabilities*,
which determine how tenants get access to certain features.

Again, one of the main motivations for the introduction of capabilities
is the future pivot migration which will downgrade the tenant with ID 1 to
become a non-privileged application tenant, and upgrade another tenant
to become the new system/admin tenant. To support that migration, we must
make the determination of the special role of system/admin tenant dependent
on a value separate from the tenant ID, and that can be changed at run time.

This will become the new ADMIN/GUEST capability, and will be stored
inside `system.tenants` (presumably in the `info` column).

To support the restriction on network access, we will also introduce a
new PRIVATE/EXPORTED capability. Only tenants with cap EXPORTED will
be visible to external clients (with an additional caveat explained in
the next section).

From that point onward, we will likely break down capabilities
further, with an eye towards the ability to delegate certain
privileged operations to any secondary tenant. A candidate use case
could be granting the capability to view KV-level timeseries to a
non-ADMIN tenant.

## Tenant server registry

The registry is conceptually a map from tenant name to tenant SQL server.

Each tenant SQL server will be instantiated upon first use, as long as
its State attribute [2] is set to ACTIVE.

Additionally, unless the EXPORTED capability is present, it would not
become available through network listeners. Only the "internal
executor" interface [3] will be available.

Additionally, there will be a cap on the maximum simultaneous
number of tenant SQL servers running with the EXPORTED capability. By default,
that cap will be 2, including the 1 tenant with ADMIN capability.
An additional license check will be required to increase that limit past 2.

Also, there will be a reference counter (incremented by internal SQL
queries and network connection), and the registry would stop a SQL
server some time after its reference count drops to zero.

[2] the State attribute (ACTIVE, ADD, DROP) indicates whether a tenant
is ready to serve SQL queries.

[3] or its future replacement: the SQL teams are busy designing the
next generation of API for in-process SQL usage by internal components.

## Access via a single SQL listener

Conceptually, we could instantiate each tenant SQL server with its own
TCP/unix listener (1 network interface per tenant). However, we have
multiple reasons not to:

- we would like the new architecture model to be usable as an upgrade
  to existing clusters, without change to orchestration rules
  (including firewalling and port redirects).

- existing test tooling typically runs multiple nodes side-by-side on
  the same machine and (currently) allocates these ports
  sequentially—e.g. 26257, 26258, etc. Adding new listeners means
  we need to rewrite these scripts to number the ports differently.

- when we implement the pivot migration mentioned above, we'll want to
  "swap" the system tenant and application tenant, in-place while
  foreground traffic is running. We want this to be possible without
  rewiring which SQL server is connected to which network
  listener.

So, we will instead aim to use a single SQL listener for all tenants
with the EXPORTED capability.

How this will work:

- we will extract the pre-authentication handshake from
  `(*pgwire.Server).ServeConn()` into a new, separate
  `pgwire.Connector` component, which will handle connection setup for
  all in-memory tenant servers.

- this new component will be responsible for routing cancel
  requests, accepting/requesting the TLS handshake, and receiving the
  client status parameters.

- it will detect which tenant the connection should be routed to:

  - if the `options` status param has a `tenant` parameter, this will
    be used to route the connection.

  - otherwise, if a TLS client cert is provided in the TLS handshake
    and there is a single *tenant scope* included in the TLS cert [4],
    this will be used to route the connection.

  - otherwise, a default tenant will be selected. For now this will
    be the system/admin tenant.

  (we might introduce additional routing mechanisms later.)

- once the tenant is selected, the `net.Conn` object is passed to
  `(*pgwire.Server).serveConn()` as usual. Each tenant will have its
  `pgwire.Server`, also as usual.

[4] tenant scopes inside TLS client certs are a feature introduced in
v22.1. See https://github.com/cockroachdb/cockroach/pull/79065 for details.

## Access via a single HTTP interface

For similar reasons as above, we will want to offer a single HTTP listener.

To distinguish which tenant should serve the HTTP API, we will use a
new cookie named `tenant`, which should be set to the desired tenant
name.

When the cookie is not present, the same default as in SQL will be
assumed (also system/admin tenant for now).

Internally, the HTTP service will route incoming requests to a
different HTTP request mux depending on the `tenant` cookie, using the
tenant server registry to look up + instantiate the server responsible
for the request.

For interactive use inside browsers, we will also introduce a new
unauthenticated HTTP endpoint (`/tenant/XXX`) whose sole function is
to set the `tenant` cookie to the specified value (e.g. `/tenant/app`
sets cookie to `app`, `/tenant/system` sets cookie to `/system`) and
redirect to the `/` URL.

This cookie mechanism and the `/tenant` redirect enables drop-in
compatibility with the existing DB Console without any changes.

## Running SQL on behalf of other tenants

Today we already need access to the SQL layer from the KV code, for
example to write `rangelog` entries.

For this, each KV node internally accesses either a
`sql.InternalExecutor` (e.g. via `s.cfg.SQLExecutor.ExecEx(..)`, see
`kv/kvserver/log.go`) or a `sql.Server` directly (e.g. `s.sqlServer`
in `(*server.Server) Decommission()`), in both cases assuming that
this implicitly refers to the system/admin tenant.

We propose to remove these direct references and instead indirect
through the tenant server registry instead. That is, instead of:

- `somestruct.internalExecutor.Exec` use `somestruct.tenantRegistry.Lookup("system").InternalExecutor().Exec`
- `somestruct.sqlServer.XXX` use `sometruct.tenantRegistry.Lookup("system").XXX`

Once this mechanism is in place, we can leverage this to *run
operations on behalf of any tenant*.

### Usage from CLI shell

Once we have the tenant registry, we can extend the CLI shell to "hop"
sessions across tenants, using a variant of the `\c` client-side
command. For example `\tenant app`, `\tenant system`, which would add
the appropriate `--options=tenant=` directive to the connection URL
and cause the shell to reconnect to the new URL.

### (Optional) Cross-tenant SQL

We could also leverage the in-process registry to perform SQL operations
"on behalf" of another tenant, from a SQL session connected to a primary tenant.

For this we could introduce the following new SQL syntax:

`AS TENANT <tenantname> <stmt>`, e.g. `AS TENANT app SELECT * FROM system.users`

or, alternatively,

`AS TENANT <tenantname> <string>`  (where the string is a quoted SQL statement), e.g. `AS TENANT app $$SELECT * FROM system.users$$`

and the run-time behavior of this statement would be to look up the
corresponding SQL server using the registry, let it instantiate if
needed, and then use its internal executor to run the desired
statement and retrieve query results (if any).

**Note:** there are many edge cases to consider in this candidate new
feature, such as "how much of the parent session do we propagate to
the child session", "which SQL user do we use in the target tenant
session", etc. Maybe we could delay adding this feature until we gain
more experience with "tenant hopping" using `\tenant` in the CLI
interactive shell (see section above).

## Performance of non-system tenants

We want to ensure that in-memory tenant servers do not incur the
performance overhead of a networked gRPC call, even through a loopback
connection.

For the system/admin tenant, we already support this via the so-called
"internal client" (see `pkg/rpc/context.go`).

We will recycle this mechanism for all in-memory tenant servers, so
that the corresponding RPC calls are performed using direct Go
function calls.

## Memory and CPU usage

Each idle SQL server (when not currently serving queries) consumes
less than 10MB of RAM, and has no CPU impact.

Therefore, we consider the dynamic instantiation of per-tenant SQL
servers to have negligible performance impact on foreground traffic.

This infrastructure is also meant for use in the Dedicated /
Self-hosted deployment styles, where there is no expectation of
resource constraints for tenants. This means we do not need an
architectural solution to segregate CPU/memory usage of in-process
tenant servers from each other.

## Usage in `demo` and testing

Once the tenant server registry is implemented, we can leverage this
in `cockroach demo` and testing:

- `demo` can instantiate a tenant named `app` like regular clusters.

  Using the shared SQL/HTTP listeners will also fix long-standing
  UX bugs around multi-tenancy in `demo`.

- the "test tenant" in tests can be redirected to use the tenant
  registry internally (instead of a custom tenant instantiation code),
  with e.g. a tenant record named `test`.

## Behavior in mixed-version deployments

The instantiation of non-system tenants via the registry would be
blocked until the `name` column is added to `system.tenants`.

The addition of this column would occur via an upgrade migration, in
turn gated by a cluster version.

This implies that the availability of secondary tenants in-memory
would be blocked until all nodes have been upgraded to the latest
version and the `system.tenants` upgrade has completed.

## Security considerations

### HTTP security

In the initial case, we will use a single CA and server TLS
certificate for the (shared) HTTP interface. This means it will
not yet be possible to use e.g. separate TLS CAs for the
"application" and "system" tenants. This separation could still be
achieved later by e.g. instantiating separate HTTP listeners, or using
virtual hosts / SNI. This avenue is out of scope for the RFC.

Regarding user authentication, the proposed approach uses the tenant
cookie to first select the tenant, then each tenant HTTP service
handles its authentication separately.

This means that conceptually each tenant has its own users and login page.

There is a plan to share the authentication cookie across tenants, to
provide a rudimentary form of cross-tenant SSO. The internal
documentation page explaining this is
[here](https://cockroachlabs.atlassian.net/wiki/spaces/MEVT/pages/2522186473/Multitenant+DB+Console+Authn+Authz#Ad-hoc%3A-shared-internal-identities-based-on-username%2Bpassword). This
scheme is however out of scope for this RFC.

### SQL security

Like for HTTP above, in the initial case we will use a single CA and
server TLS certificate for the (shared) SQL interface.  As above, this
will prevent separating the CA / TLS provisioning per tenant, but we
can add this capability later via separate listeners (see [this
issue](https://github.com/cockroachdb/cockroach/issues/54006)).

Regarding authentication, the tenant is selected first, and then the
entire connection is routed to that tenant. This means that SQL
authentication is fully scoped "under" the selected tenant.

## Drawbacks

The execution model is rendered somewhat more complex.
We consider this to be acceptable in light of the other benefits.

## Alternatives Considered

N/A

# Explain it to folk outside of your team

With this change enabled, CockroachDB Dedicated and Self-Hosted now
supports a secondary "application" tenant inside every node, by
default.

For the time being, use of the application tenant is opt-in; by
default, connections are routed to the "system" (also internally
called "admin") tenant, like in previous single-tenant deployments.

To access this application tenant via SQL, for now a special parameter
must be passed in connection URLs
(e.g. `postgres://...&options=--tenant=app`).

Access to this special tenant via HTTP is also possible:
- for DB console: by accessing the console initially via `https://.../tenant/app`
- for HTTP APIs, by providing a cookie pointing to the `app` tenant.

# Unresolved questions

N/A

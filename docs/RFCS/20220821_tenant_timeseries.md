- Feature Name: Timeseries access for secondary tenants
- Status: superseded ([#102436](https://github.com/cockroachdb/cockroach/pull/102436))
- Start Date: 2022-08-21
- Authors: knz
- RFC PR: [#86524](https://github.com/cockroachdb/cockroach/pull/86524)
- Cockroach Issue: [CRDB-18797](https://cockroachlabs.atlassian.net/browse/CRDB-18797)

# Summary

This RFC proposes an extension of how CockroachDB's
native timeseries database (tsdb) is integrated
into server processes, so as to:

- separate the time series stored on behalf of different
  tenants, when tenant timeseries storage is enabled.

  (Tenant timeseries storage would not be enabled
  in CC Serverless, but it would be in Self-hosted
  and CC Dedicated for the time being.)

- provide the HTTP service of each tenant with the
  ability to query timeseries data, for the benefit
  of a tenant-scoped DB Console.

The RFC does not propose to change the underlying implementation of
the tsdb; merely how it is integrated into CockroachDB servers.

<!-- markdown-toc start - Don't edit this section. Run M-x markdown-toc-refresh-toc -->
**Table of Contents**

- [Summary](#summary)
- [Background](#background)
- [Motivation](#motivation)
    - [Wider context: multi-tenancy](#wider-context-multi-tenancy)
    - [Wider context: DB Console and Observability](#wider-context-db-console-and-observability)
    - [Objective 1: independent roadmaps](#objective-1-independent-roadmaps)
    - [Problems with CockroachDB's internal tsdb and multi-tenancy](#problems-with-cockroachdbs-internal-tsdb-and-multi-tenancy)
    - [Objective 2: desired semantics](#objective-2-desired-semantics)
- [Technical design](#technical-design)
    - [Changes to the data model](#changes-to-the-data-model)
    - [Automatic classification of metrics](#automatic-classification-of-metrics)
    - [`MetricRecorder` changes](#metricrecorder-changes)
    - [Which tenant get their SQL timeseries persisted to KV?](#which-tenant-get-their-sql-timeseries-persisted-to-kv)
    - [Making the tsdb Query endpoint tenant-scoped](#making-the-tsdb-query-endpoint-tenant-scoped)
    - [Cluster upgrades](#cluster-upgrades)
    - [Summary of changes](#summary-of-changes)
    - [Agnosticism to deployment style](#agnosticism-to-deployment-style)
    - [Drawbacks](#drawbacks)
    - [Rationale and Alternatives](#rationale-and-alternatives)
- [Explain it to folk outside of your team](#explain-it-to-folk-outside-of-your-team)
- [Unresolved questions](#unresolved-questions)

<!-- markdown-toc end -->

# Background

The remainder of the RFC requires familiarity with CockroachDB's
internal time series database and how it is integrated
into server processes.

This is explained in an accompanying [tech
note](../tech-notes/timeseries.md).

In particular, the RFC depends on familiarity with:

- the encoding of timeseries names in KV keys.
- how the tsdb is integrated to scrape and store metrics
  from CockroachDB servers.
- how the tsdb query engine is exposed as a service.

(These various concepts are explained in the tech note.)

# Motivation

## Wider context: multi-tenancy

As part of the “multi-tenant unification project”, we plan to convert
all CockroachDB deployments to use multi-tenancy eventually. When this
happens, client workloads run in a secondary tenant called
"application tenant", while administrative operations by SREs
can be performed on the system/admin tenant as usual.

Meanwhile, we will continue to support DB Console and its ability
to plot charts of monitoring data.

## Wider context: DB Console and Observability

Today, DB Console loads data from CockroachDB directly using the
internal timeseries database.

We also have a separate plan to move the DB Console feature to a
service external to the CockroachDB cluster being observed, so that we can still plot monitoring
data when the observed cluster is temporarily unavailable.

This project is called “Observability Service”.

## Objective 1: independent roadmaps

We wish to make the two roadmaps, multitenancy and observability service,
organizationally independent from each other.

That is, we wish to organize the work so that progress can be made on
both fronts without inter-dependencies.

From the obs service perspective, this means that the multi-tenancy
roadmap must preserve API compatibility, so that the obs service
can use the same API client regardless of progress on multi-tenancy.

From the multi-tenancy perspective, this means that the plan must
define how to achieve compatibility with the internal timeseries
database and current DB Console UX by end-users, not assuming the
availability of the observability service or a tsdb replacement.

This latter objective is the motivation for this RFC.

## Problems with CockroachDB's internal tsdb and multi-tenancy

Let's consider DB Console UX by CC Dedicated and Self-hosted
users. These users expect the ability to view their time series as
charts in DB console, with data extracted via queries on CockroachDB's
tsdb.

When we introduce multi-tenancy, with application workloads
encapsulated as a secondary tenant, we introduce the following
problems in the current architecture for Dedicated/SH users:

1. *SQL metrics from the secondary tenant are not saved.*

   This is because data preservation of metrics happens
   via the asynchronous `PollSource()` function, which
   is not even started for SQL servers of secondary tenants.

2. then, even if hypothetically we did start the tsdb collector,
   we would then see *ambiguity in all the SQL timeseries*.

   Indeed, such a multi-tenant deployment would have two tenants:
   the application tenant and the system/admin tenant.
   Each of these has its own SQL layer, separate from the other,
   and each tenant SQL has its own metrics.

   For example, `cr.node.sql.ddl.count` is the count of DDL queries;
   there is logically one such count for the DDL queries sent to the
   system/admin tenant, and a *separate* DDL count for the application
   tenant.

   Today, the tenant identity is not part of timeseries names, so the
   data would be "merged" across tenants. This would result in
   confusing UX. Instead, we want the ability to distinguish this
   data.

3. the HTTP endpoint to query timeseries is not available
   in the HTTP service for secondary tenants.

   This means that when DB Console is opened for the application
   tenant, it cannot even call the `/ts/db` operation to retrieve
   data points.

## Objective 2: desired semantics

- Users accessing the DB Console for the system/admin tenant must be
  able to view historical metrics for the storage cluster (KV-level),
  and historical metrics for the system/admin SQL layer.

- Users accessing the DB Console for the application tenant must be
  able to see historical metrics for the app tenant's SQL layer.

- By default, users accessing the DB Console for the application
  tenant must be able to see the storage metrics (KV-level) too,
  however an option must exist to revoke this access: for CC
  customers, certain storage-level metrics are not actionable and thus
  should not be displayed.

- When a monitoring tool accesses `/_status/vars` to pull Prometheus
  metrics from a KV node, it must be able to collect instant values
  for KV-level and SQL-metrics *directly managed by the current
  process*. Likewise, when either the (little-known, and under-documented)
  Graphite export feature, or Prometheus *push* feature are activated,
  the export logic must export instant values for all the metrics in
  the current process.

**Out of scope in this RFC**: users with access to the system/admin tenant DB
Console have an option to "merge" SQL-level timeseries across the
system/admin tenant and the secondary tenant(s). This is kept out of
scope because this would require more intricate changes to DB
Console. Also, users can still access their SQL metrics for each
tenant by switching the tenant of their DB Console screen.

(We keep the option open to do this aggregation in subsequent work.)


# Technical design

At a high-level we will perform the following changes:

- we will introduce a tenant ID label for only those timeseries that
  are scoped to a tenant (i.e. KV-level timeseries will not be
  prefixed, but SQL-level timeseries will).

- we will conditionally expose timeseries to secondary tenants for
  viewing in the DB Console:
  - for SQL timeseries, filtered from the current tenant's ID.
  - for KV timeseries, depending on the current tenant's capabilities
    (see [this
    proposal](https://github.com/cockroachdb/cockroach/pull/85954))

	Capabilities will allow a deployment to revoke access to the
	KV-level timeseries whenever it makes sense.

- we will conditionally store timeseries from secondary tenants,
  if enabled (also via a tenant capability).

## Changes to the data model

For context, recall that the timeseries are stored
under the hood with the following structure:

`/System/tsd/<name>/<resolution>/<start_timestamp>/<source>`

Where `<name>` is the name of the metric, and `<source>` is the
node/server where the data point was collected.

We are going to extend the data model as follows:

- the encoding for KV-level metrics will remain unchanged.

- for SQL and other tenant-scoped metric, we are going
  to:

  1) **extend the encoding of the metric source to be suffixed by the tenant ID.**

As a result, we will see:

- for metrics like `cr.node.gossip.infos.sent`,
  `cr.node.liveness.livenodes`, etc, the encoding remains unchanged:
  `/System/tsd/cr.node.gossip.infos.sent/...`.

- for metrics like `cr.node.sql.copy.count`, `cr.node.sql.distsql.exec.latency-count`, etc,
  the encoding of the name will be prefixed by the tenant ID; for example:

  - `/System/tsd/cr.node.sql.copy.count/.../1-1` for the tenant with ID 1 on node with ID 1;
  - `/System/tsd/cr.node.sql.copy.count/.../1-2` for the tenant with ID 2 on node with ID 1;
  - etc.

## Automatic classification of metrics

We will not need to manually review the list of metrics to determine
which are KV-scoped and which are those that are SQL-scoped.

Instead, we will leverage the existing `metric.Registry`
infrastructure: each tenant server will register separate metric
objects to the registry.

## `MetricRecorder` changes

The proposal here is to implement a separate recorder per tenant. Each
recorder would redirect its writes to the local (per-tenant)
`ts.Server` instance, for recording into the local tenant's tsdb.

## Which tenant get their SQL timeseries persisted to KV?

As stated in the objectives, we want that the application tenant
in Dedicated/SH deployments gets its SQL timeseries persisted
(so they can be queried in DB Console).

However, so far, secondary tenants do not get metric persistence.

We are going to change this as follows: the polling from `ts.DB` from
each tenant's metric recorder will only persist data in KV is the
current tenant has the appropriate tenant capability.

## Making the tsdb Query endpoint tenant-scoped

Currently, the `/ts/db` query endpoint is defined for KV nodes, and
connected to a single `ts.Server` instance in memory under
`server.Server`.

This RFC proposes to define a new instance of `ts.Server` per tenant;
such that queries to its API endpoint introduce a tenant scope for the
time series.

On the KV side, the requests to the time series keyspace would be
conditionally limited by a new tenant capability.

## Cluster upgrades

The main scope of this RFC is *new deployments*, i.e. an initial
implementation does not need to consider upgrades from previous versions.

Should we want to include them, this can be achieved as follows: the
SQL-level metrics of the system/admin tenant must become prefixed with
the tenant ID.

We can achieve this via a cluster upgrade that would rewrite the timeseries
keys of all the SQL-level metrics to include the tenant ID prefix.

Without this upgrade logic, a cluster upgraded from a previous version
will appear to contain no timeseries data for the SQL-level metrics in
the system/admin tenant post-upgrade.

We will discuss this in further detail in a separate RFC on the
topic of migrating existing workload from single-tenancy to multi-tenancy.

## Summary of changes

- Recorder and metric registries: one separate recorder per tenant.

- KV tenant connector, authorization rules:
  - Define new capabilities to access time series.
    (see [this proposal](https://github.com/cockroachdb/cockroach/pull/85954))

- `pkg/server`: plug the metrics and recorders separately for each tenant.

- (optionally) a cluster upgrade to prefix all previously stored
  SQL-level metrics for the system/admin tenant with a tenant ID.

There are no DB Console changes required.

## Agnosticism to deployment style

Through the introduction of multi-tenancy, we are envisioning two
different deployment styles:

- a *shared-process* architecture, where a single CockroachDB
  server process serves multiple tenants. In this mode,
  there are multiple `server.SQLServer` instances side-by-side
  in memory inside the same process.

  As per [this
  proposal](https://github.com/cockroachdb/cockroach/pull/84700), the
  `SQLServer` objects would instantiated dynamically, as needed by
  network clients.

- a *separate-process* architecture, where one CockroachDB server
  process serves the KV layer RPCs and one SQL service for the
  system/admin tenant; and separate process(es) running only
  SQL services for tenants, called "SQL pods".

  In the common case, there would one `SQLServer` object per
  separate SQL pod, however we also envision to reuse the
  dynamic behavior described above to only instantiate
  that `SQLServer` late, only at the time it is truly needed.

We intend to support timeseries access in both cases:

- to *query* timeseries data, trivially because the `ts.DB` and
  `ts.Server` objects, responsible for handling `Query`, are bound per
  `SQLServer` already. There would be `ts.DB` / `ts.Server` instances
  in every process where a `SQLServer` object are used.

- to *store* timeseries data, we would see it happen:

  - naturally, in KV nodes because that's where `NodeMetricRecorder`
    is instantiated and there would be a `(*ts.DB).PollSource()`
    running on it.

  - for every `SQLServer`, either in-process with KV nodes or on
    separate process, because that's where `MetricRecorder` is
    instantiated and there would be a separate `(*ts.DB).PollSource()`
    running on it.

This architecture effectively makes the tsdb logic agnostic of whether
tenants run in-process with KV or in separate processes.

It also introduces the option (but not the mandate) to make separate
SQL pods able to record their timeseries in KV, via the new capability
WRITE_TENANT_TIMESERIES.

## Drawbacks

N/A

## Rationale and Alternatives

The main alternative is “do nothing”, but this will result in a UX
regression when a CC Dedicated/SH user migrates to multi-tenancy.

# Explain it to folk outside of your team

There would be no user-visible change in DB Console with this proposal.

(Indeed: this proposal exists solely to *minimize user-visible
changes* in DB Console resulting from the introduction of
multi-tenancy.)

For users of 3rd-party monitoring tools, they will see the metrics
exported by CockroachDB (e.g. `/_status/vars`) now contain a label
that indicate which tenant the metric is sampled for.

# Unresolved questions

N/A

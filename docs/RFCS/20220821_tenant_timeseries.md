- Feature Name: Timeseries access for secondary tenants
- Status: draft
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
        - [Splitting registries inside the metric recorder](#splitting-registries-inside-the-metric-recorder)
        - [Changing the `DataSource` behavior](#changing-the-datasource-behavior)
        - [Instant metric exporter changes](#instant-metric-exporter-changes)
        - [Metric metadata server changes](#metric-metadata-server-changes)
        - [Changes to `GenerateNodeStatus()`](#changes-to-generatenodestatus)
        - [`WriteNodeStatus()` - no changes](#writenodestatus---no-changes)
        - [Splitting the responsibilities of `MetricRecorder`](#splitting-the-responsibilities-of-metricrecorder)
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

**Out of scope**: users with access to the system/admin tenant DB
Console have an option to "merge" SQL-level timeseries across the
system/admin tenant and the secondary tenant(s). This is kept out of
scope because this would require more intricate changes to DB
Console. Also, users can still access their SQL metrics for each
tenant by switching the tenant of their DB Console screen.

# Technical design

At a high-level we will perform the following changes:

- we will introduce a tenant ID prefix for only those timeseries that
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

  1) **extend the encoding of the metric name to be prefixed by the tenant ID.**
  2) **change the source field to become SQL instance ID instead of node ID.**

As a result, we will see:

- for metrics like `cr.node.gossip.infos.sent`,
  `cr.node.liveness.livenodes`, etc, the encoding remains unchanged:
  `/System/tsd/cr.node.gossip.infos.sent/...`.

- for metrics like `cr.node.sql.copy.count`, `cr.node.sql.distsql.exec.latency-count`, etc,
  the encoding of the name will be prefixed by the tenant ID; for example:

  - `/System/tsd/<1>cr.node.sql.copy.count/...` for the tenant with ID 1;
  - `/System/tsd/<2>cr.node.sql.copy.count/...` for the tenant with ID 2,
  - etc.

  They will also be suffixed with the SQL instance ID, for example:

  - `/System/tsd/<1>cr.node.sql.copy.count/.../1` for the tenant with ID 1, instance ID 1
  - `/System/tsd/<1>cr.node.sql.copy.count/.../2` for the tenant with ID 1, instance ID 2
  - `/System/tsd/<1>cr.node.sql.copy.count/.../3` for the tenant with ID 1, instance ID 3
  - etc.

In the case of Dedicated/SH deployments where there's just 1 SQL instance per KV instance,
the SQL instance ID and node ID will match exactly and there will be no change in cardinality.

## Automatic classification of metrics

We will not need to manually review the list of metrics to determine
which are KV-scoped and which are those that are SQL-scoped.

Instead, we will leverage the existing `metric.Registry` infrastructure:

- the `server.Server` object, which is used for KV nodes, already
  instantiates one `metric.Registry` object to collect metric object
  for all the KV-level structures (`Node`, `Engine` etc.), plus
  one registry per `Store`.

- (NEW) the `server.SQLServer` object, which is used for the SQL layer and
  all other tenant-scoped services, will instantiate a *separate*
  `metric.Registry` object for the SQL-level structures (`sql.Server`,
  `pgwire.Server`, `server.httpServer`, etc).

We will introduce the ID as prefix only for metrics defined in the 2nd
registry (as explained below), leaving those from the 1st registry unchanged.

## `MetricRecorder` changes

A single `status.MetricsRecorder` component inside CockroachDB is
currently responsible for all the following features simultaneously,
given a set of `metric.Registry` instances:

- serve the `ts.DataSource` interface (via its `GetTimeSeriesData()`),
  which is used by the async ts.DB poller to obtain the values
  that need to be stored in the tsdb.

- periodically capture an instant observation of all the metrics
  inside the current process, to serve via the Prometheus pull
  endpoint `/_status/vars` (via its `PrintAsText()` method), the
  Graphite export task (via its `ExportToGraphite()` method) and the
  Prometheus push task (via its `ScrapeIntoPrometheus()` method).

- serve metadata for the metrics, to display as the "chart catalog" in
  the DB Console (via the `/_admin/v1/metricmetadata` and
  `/_admin/v1/chartcatalog` endpoints).

- generate instant synthetic "node statuses" (via the method `GenerateNodeStatus()`), which
  is included in diagnostic reports and used as input to the "health alerts"
  subsystem (`(*server.Node) writeNodeStatus()`)

- write node statuses to KV (via the method `WriteNodeStatus()`).

All these services are extended as explained in the following sections.

### Splitting registries inside the metric recorder

The metric recorder already distinguishes the node metric registry
(`nodeRegistry`), responsible for Node/Engine/etc metrics; and the
per-store metric registries (`storeRegistries`), one per store.

We are going to extend this with a new list of SQL-level registries,
`sqlRegistries map[TenantID]*metric.Registry`, one per tenant served
in the current process.

(The concept of "tenants served in the current process" will be discussed in the
section "Deployment styles" below.)

### Changing the `DataSource` behavior

The `GetTimeSeriesData()` method for the `ts.DataSource` interface
will be extended to also collect metrics from the `sqlRegistries`:

- for the `nodeRegistry` and `storeRegistres`, it will continue
  to encode metric names as usual, without a tenant ID prefix.

- for the `sqlRegistries` it will include the tenant ID prefix
  for each metric name.

The remainder of the low-level logic in `(*ts.DB).StoreData()` remains
unchanged.

### Instant metric exporter changes

The instant metric exporter logic (`/_status/vars` &
graphite/prometheus push) will be changed as follows:

- node and store metrics will continue to be exported with their name
  as-is, like previously.

- SQL metrics will be exported separately, one per tenant, with the
  tenant *name* (not ID) included as a metric label
  (e.g. `sql_delete_count{tenant=app}` /
  `sql_delete_count{tenant=system}`).

  (As a reminder, the tenant name is a new concept introduced in [this
  proposal](https://github.com/cockroachdb/cockroach/pull/85954), see
  [this
  section](https://github.com/knz/cockroach/blob/20220720-rfc-in-memory-tenants/docs/RFCS/20220720_dynamic_tenant_servers.md#tenant-names).)

The introduction of the label will ensure that existing PromQL queries
over scraped data can continue to aggregate SQL metrics without
changes, while opening the door for per-tenant aggregations.

### Metric metadata server changes

The metric metadata and chart catalog endpoints
will be modified to return metric names across
all the registries, including the SQL registries.

However, no tenant ID will be introduced in the
metric metadata.

This ensures that each tenant's DB Console continues to see the
"simple" (non-extended) metric names in their charts and chart
catalog.

### Changes to `GenerateNodeStatus()`

The node status generator will include metrics from the node and store
registries, like before, and will be extended to also include the
metrics from the system/admin tenant's SQL registry.

In the node status, the SQL metrics from the system/admin tenant
are not prefixed by the tenant ID.

This ensures we preserve previous behavior, where the system/admin
tenant's SQL metrics were included in node status via the node
registry.

### `WriteNodeStatus()` - no changes

This logic is not based on metric registries and thus requires no
changes.

### Splitting the responsibilities of `MetricRecorder`

After / in addition to the changes above, we are going to split
`MetricRecorder` into separate components with separate
responsibilities.

In a nutshell, we want a new architecture where there is one `MetricRecorder` per
tenant SQL server, and where the cross-tenant services are implemented
by new components:

- a NEW `ExternalExporter`, which will be linked to the tenants'
  `MetricRecorder`s, and contain the logic for the instant metric
  export (`/_status/vars`, Prometheus/Graphite export: `PrintAsText`,
  `ExportToGraphite`, `ScrapeToPrometheus`).

  This will gather metrics across all its `MetricRecorder` sources in
  the same process, and introduce the tenant name label for
  tenant-scoped metrics.

- a NEW `NodeMetricRecorder`, containing the node and store-specific
  metric collection and the `GetNodeStatus()` / `WriteNodeStatus()` logic.

  This also implements the `ts.DataSource` interface for the node/store
  metrics, *without* a tenant ID prefix on the metric names.

- a NEW `MetadataExporter`, which will be linked to the tenant-scoped
  `MetricRecorder`s and, optionally, the KV-scoped
  `NodeMetricRecorder`, and collates metric metadata across all of
  them to implement `GetMetricMetadata()`. The KV-scoped metric
  metadata will only be included if the requesting tenant has the
  capability READ_SYSTEM_TIMESERIES.

The remaining `MetricRecorder` only contains 1 `metric.Registry` for
tenant-scoped metrics and implements the `ts.DataSource` for it,
*with* a tenant ID prefix. It will also evolve to report the
current tenant SQL instance ID as source, instead of the KV node ID.

## Which tenant get their SQL timeseries persisted to KV?

As stated in the objectives, we want that the application tenant
in Dedicated/SH deployments gets its SQL timeseries persisted
(so they can be queried in DB Console).

However, so far, secondary tenants do not get metric persistence.

We are going to change this as follows:

- we will call `(*ts.DB).PollSource()` on the `NodeMetricRecorder`,
  which now handles KV-only metrics, in each `server.Server` (i.e. KV
  nodes).

  This will take care of persisting KV-only metrics.

- we will call `(*ts.DB).PollSource()` for each secondary tenant
  `MetricRecorder`. Remember, as per the changes above now
  `MetricRecorder` is per `server.SQLServer`, i.e. per tenant.

  Each of the tenant `PollSource()` tasks will be responsible for
  persisting only the tenant-scoped metrics.

  In the typical case, there will be a maximum of two such tasks
  running concurrently inside a CockroachDB server process (one for
  the system/admin tenant and one for the application tenant).

- the call to `(*ts.DB).PollSource()` on a tenant's `MetricRecorder`
  will be conditional on a (new) tenant capability: WRITE_TENANT_TIMESERIES.

- the KV tenant connector will verify that `Merge` requests from a
  tenant are only allowed if the requesting tenant has the
  WRITE_TENANT_TIMESERIES capability *and* the client tenant ID
  matches the tenant ID prefix in the timeseries name.

The capability WRITE_TENANT_TIMESERIES will not be granted for CC
Serverless tenants, but will be granted for CC Dedicated / Self-hosted
application tenants.

## Making the tsdb Query endpoint tenant-scoped

Currently, the `/ts/db` query endpoint is defined for KV nodes, and
connected to a single `ts.Server` instance in memory under
`server.Server`.

We are going to extend this as follows:

- `ts.Server` will learn the tenant ID that it is serving
  requests for.

- there will be one `ts.Server` instance per SQL service
  in memory (under `server.SQLServer`).

- each `ts.Server` instance will *introduce a tenant scope*
  into its `Query` endpoint:

  - for each `tspb.Query` object included in a
    `TimeSeriesQueryRequest`, it will check whether the timeseries
    name is SQL-level or not. If it is, it will inject the tenant ID
    of the current tenant into the query. Otherwise (KV-level
    queries), it will not. Then it will forward the query
	to the internal `(*ts.DB) Query()` as usual, which will
	remain unchanged.

- on the KV side, the tenant connector will start accepting
  requests for timeseries for tenants:

  - requests for un-prefixed timeseries (e.g. `/System/tsd/cr.node.gossip.infos.sent`) will be
    allowed only if the requesting tenant has the READ_SYSTEM_TIMESERIES capability.
  - requests for prefixed timeseries (e.g. `/System/tsd/<1>cr.node.sql.copy.count`) will
    be allowed only if the requesting tenant ID is the one also included in the request key.

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

- `pkg/server/status.MetricRecorder`:
  - split the existing logic into `NodeMetricRecorder`, `MetadataExporter`, `ExternalExporter` and `MetricRecorder`.
  - `MetricRecorder`: in `GetTimeSeriesData()` (`ts.DataSource`), introduce tenant ID prefix for SQL metrics and
    the current tenant SQL instance ID as source.
  - `ExternalExporter`: introduce tenant name label in `/_status/vars` and other instant metric exporters for
    SQL-bound metrics; and no label for KV-level metrics.
  - `MetadataExporter`: include KV-level metrics in `GetMetricsMetadata()` only if the
    requesting tenant has the (new) READ_SYSTEM_TIMESERIES capability.
  - `NodeMetricRecorder`: new `GetTimeSeriesData()` (`ts.DataSource`) that collects node/store metrics.
  - (optional? TBD) `NodeMetricRecorder`: also include system/admin tenant SQL metrics in `GenerateNodeStatus()`.

- KV tenant connector, authorization rules:
  - Define new capabilities READ_SYSTEM_TIMESERIES and WRITE_TENANT_TIMESERIES.
    (see [this proposal](https://github.com/cockroachdb/cockroach/pull/85954))
  - Allow `Scan` requests from tenants to non-prefixed
    timeseries via the new READ_SYSTEM_TIMESERIES capability.
  - Allow `Scan` requests from tenants to prefixed timeseries, if the tenant ID
    matches.
  - Allow `Merge`/`DeleteRange` requests to non-prefixed
    timeseries only if the request was issued from the same process or with
    the identity of a KV node.
  - Allow `Merge`/`DeleteRagne` requests from tenants to prefixed
    timeseries, if the WRITE_TENANT_TIMESERIES capability is present
    and the tenant ID matches.

- `pkg/server`:
  - for recording metrics:
    - introduce one `metric.Registry` instance per tenant SQL server.
    - bind `MetricRecorder` to each `SQLServer`, not `Server`, each
      with its tenant ID.
	- make the `ts.DB` object hang off the  `server.SQLServer` (i.e.
	  one per tenant) instead of `server.Server`, each with
	  its KV client bound to the specific tenant ID.
	- call `(*ts.DB).PollSource()` on each tenant's `MetricRecorder`.
    - instantiate `NodeMetricRecorder` on KV nodes only. Call
      `(*ts.DB).PollSource()` on it separately.

  - for querying the tsdb:
    - make the `ts.Server` object hang off `server.SQLServer` (i.e. one
      per tenant) instead of `server.Server`, each with its parent
      SQLServer's tenant ID. Bind the `/ts/db` endpoint for each tenant
      HTTP server to it.
    - instantiate `MetadataExporter` and `ExternalExporter`,
      one per tenant HTTP server. Connect the tenant HTTP service's
	  `/_admin/v1/metricmetadata` / `_admin/v1/chartcatalog` to it.

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

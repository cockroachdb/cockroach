# The internal time series database (tsdb)

Alongside the SQL layer exposed to client apps, CockroachDB also implements
another database engine on top of the KV layer: a native, embedded
*time series database*.

This timeseries database exists solely for the purpose of storing
metrics efficiently inside CockroachDB and serving graphs on the DB
Console, without introducing a dependency on 3rd party technology. It
features:

- an efficient, compact columnar storage for timeseries.
- an automatic rollup algorithm (so that older values take less space on disk).
- a featureful query engine, featuring:
  - aggregation across sources (typically nodes in a cluster),
  - optional downsampling of data,
  - optional, on-the-fly derivative metrics.

The tsdb is layered "on top" of CockroachDB KV, that is, it stores its
data using the same transaction and replication layer as SQL. Only the
data storage layout is different (see section below).

<!-- markdown-toc start - Don't edit this section. Run M-x markdown-toc-refresh-toc -->
**Table of Contents**

- [The internal time series database (tsdb)](#the-internal-time-series-database-tsdb)
    - [Data model](#data-model)
    - [Resolution and data compression](#resolution-and-data-compression)
    - [Data storage](#data-storage)
    - [Low-level KV operations](#low-level-kv-operations)
    - [Data collection](#data-collection)
    - [Meta-Metrics](#meta-metrics)
    - [Low-level query engine](#low-level-query-engine)
    - [High-level query engine](#high-level-query-engine)
    - [Integration in CockroachDB](#integration-in-cockroachdb)
        - [Metric collection](#metric-collection)
        - [Queries, DB Console and other clients](#queries-db-console-and-other-clients)
    - [Previous writeups on the topic of timeseries](#previous-writeups-on-the-topic-of-timeseries)

<!-- markdown-toc end -->


## Data model

From the tsdb perspective, a *data timeseries* is a set of measurements
of a single named variable at multiple points in time.

The variable under each timeseries is identified by a *name* (the
"metric" in the remainder of the CockroachDB source).

Data points are identified by a unix timestamp (expressed in
nanoseconds), a 64-bit floating-point value, and a *source* (a string) where the
value was measured.

(For example, in versions of CockroachDB up to and including v22.2,
the source is the node ID where the data is coming from. Starting in
v23.1 we see more diverse uses of the source field.)

## Resolution and data compression

A single timeseries is stored as multiple groups of KV pairs, one per
*storage resolution*.

Storage resolutions represent the granularity at which values are
kept. To simplify, "finer resolution" means more data points are kept
by unit of time. For example, the "10s" resolution keeps 1 data item
per 10 seconds; the "30mn" resolution keeps 1 data item per 30 minutes.

Under each resolution, data points are stored in a columnar fashion,
with multiple values per keys. A group of data points under
the same KV key is called a *slab*. Each source has its different set of slabs.

Each resolution uses a different slab size:
- 10s resolution uses a 1 hour slab (each key storing 1 hour worth of data, i.e. max 360 values)
- 30mn resolution uses a 24 hour slab (each key storing 24 hours of data, i.e. max 48 values)

(There are 4 resolutions currently implemented in the code. 10s and
30mn are used for real-world deployments; 1ns and 50ns resolutions
are also implemented for testing.)

For each resolution, there is also a configurable *maximum age of
slabs*: when that maximum is reached, every time a new slab is added,
the oldest slab is "rolled up" into the newest slab of a coarser
resolution. Rolling up means multiple consecutive data points are
averaged/interpolated together and only the result is stored. This is
the algorithm that compresses the data over time.

These configuration knobs are cluster settings:

- `timeseries.storage.resolution_10s.ttl` is the maximum age of values
  stored at the 10s resolution. Data older than this is subject to
  rollup to the 30mn resolution.

  The default is 10 days.

- `timeseries.storage.resolution_30m.ttl` is the maximum age of values
  stored at the 30mn resolution. Data older than this is subject
  to deletion (because we don't have a coarser resolution implemented).

  The default is 90 days.

## Data storage

Each timeseries is stored as a sequence of KV pairs, as defined by the algorithm above:

`/System/tsd/<name>/<resolution>/<start_timestamp>/<source>`

(Note: this is the low-level encoding. When using `cockroach debug keys`, the *pretty-printed* output
inverts the order of the key, placing the `<source>` component prior to the resolution
in the output. However, this is just a display artifact. In storage, the source is at the end.)

For example, the metric `cr.node.sql.new_conns` is stored under `/System/tsd/cr.node.sql.new_conns/...`.

If we look at the keyspace with all configurations set to defaults, we will see:

- the 10s resolution data, split into 1-hour slabs, up to 240 pairs (1 per hour over 10 days); for example:

        //System/tsd/cr.node.sql.new_conns/10s/2022-08-11T10:00:00Z/1
        //System/tsd/cr.node.sql.new_conns/10s/2022-08-11T10:00:00Z/2
        //System/tsd/cr.node.sql.new_conns/10s/2022-08-11T10:00:00Z/3
        //System/tsd/cr.node.sql.new_conns/10s/2022-08-11T11:00:00Z/1
        //System/tsd/cr.node.sql.new_conns/10s/2022-08-11T11:00:00Z/2
        //System/tsd/cr.node.sql.new_conns/10s/2022-08-11T11:00:00Z/3
        //System/tsd/cr.node.sql.new_conns/10s/2022-08-11T12:00:00Z/1
        //System/tsd/cr.node.sql.new_conns/10s/2022-08-11T12:00:00Z/2
        //System/tsd/cr.node.sql.new_conns/10s/2022-08-11T12:00:00Z/3
        ...
        //System/tsd/cr.node.sql.new_conns/10s/2022-08-21T08:00:00Z/1
        //System/tsd/cr.node.sql.new_conns/10s/2022-08-21T08:00:00Z/2
        //System/tsd/cr.node.sql.new_conns/10s/2022-08-21T08:00:00Z/3
        //System/tsd/cr.node.sql.new_conns/10s/2022-08-21T09:00:00Z/1
        //System/tsd/cr.node.sql.new_conns/10s/2022-08-21T09:00:00Z/2
        //System/tsd/cr.node.sql.new_conns/10s/2022-08-21T09:00:00Z/3

  Notice: for each timestamp, there's one key per source (node).

  Under each of these keys, there are up to 360 data points
  (1 per 10 second interval, up to 1 hour). The actual data storage is
  a bit more complex than a simple array of floats, see the doc on
  `roachpb.InternalTimeSeriesData` for details.

- the 30m resolution data, split into 24-hour slabs, up to 90 pairs (1 per day over 90 days); for example:

       //System/tsd/cr.node.sql.new_conns/30m/2022-06-11T00:00:00Z/1
       //System/tsd/cr.node.sql.new_conns/30m/2022-06-11T00:00:00Z/2
       //System/tsd/cr.node.sql.new_conns/30m/2022-06-11T00:00:00Z/3
       ...
       //System/tsd/cr.node.sql.new_conns/30m/2022-08-10T00:00:00Z/1
       //System/tsd/cr.node.sql.new_conns/30m/2022-08-10T00:00:00Z/2
       //System/tsd/cr.node.sql.new_conns/30m/2022-08-10T00:00:00Z/3

   Under each of these keys there are up to 48 data points (1 per 30m
   interval, up to 1 day).

## Low-level KV operations

The ts writes are not subject to MVCC logic otherwise afforded to SQL
writes.

Instead, the KV layer offers a specialized operation, `Merge`, which performs
high-performance append-only behavior to stored data points.

These writes are replicated (for fault tolerance), and offer
protection against replay (so they are idempotent and can be
re-applied during transient faults), but they are *not transactional*;
in particular they cannot be rolled back. This affords the tsdb
extremely efficient reads, unhindered by MVCC intents and deletion
tombstones.

Data deletion (for rollups) does use the standard KV `DeleteRange`
operation. At a low level, `DeleteRange` has a special path
for timeseries KV pairs, which makes it more efficient.

## Data collection

The tsdb is implemented in a way mostly independent from
the rest of CockroachDB.

It operates over an abstract `DataSource`, defining a method
`GetTimeSeriesData()` returning `tsdb.TimeSeriesData` (a struct
containing series name, source and data points).

The client of the tsdb library is then intended to call
`PollSource()`. This method starts an asynchronous task that,
periodically (at at 10s interval) calls `StoreData()`. This method, in
turn, scrapes the data points from the `DataSource` and stores them in
KV according to the rules spelled out above.

How this is used in CockroachDB is explained in a separate section
below.

## Meta-Metrics

The tsdb maintains the following metrics about its own functioning:

- `timeseries.write.samples`: Total number of metric samples written to disk.
- `timeseries.write.bytes`: Total size in bytes of metric samples written to disk.
- `timeseries.write.errors`: Total errors encountered while attempting to write metrics to KV.

## Low-level query engine

The tsdb query engine (`Query()` method in code) is the component which,
given a timeseries query, computes a result array of `tsdb.TimeSeriesDatapoint`
suitable for e.g. plotting on a graph using the currently stored KV data.

The parameters of a ts query include:

- the name of the time series to query. (e.g. `cr.node.sql.new_conns`)
- an optional list of sources to restrict the query to (by default,
  all available sources are queried).
- the desired time span, which constrains the range of timestamps in
  the result. (e.g. "2022-08-09T02:03:00Z to 2022-08-09T05:00:00Z")
- the desired resolution for the results. (typically 10s, i.e. the
  finest available)
- an optional derivation function to convert data points into
  rate-of-change measurements. (available: none, derivative,
  non-negative-derivative).
- an optional aggregation function to compute synthetic data points
  across all queried sources. (available: avg, sum, min, max, first,
  last, variance)
- an optional aggregation function to compute synthetic data points
  when the available data points have a finer resolution than the
  desired output resolution.

The query engine uses state of the art formulas internally to avoid
data rounding artifacts, discontinuities and anomalies at the rollup
boundaries. All in all, it may not be as advanced as Prometheus or
Grafana, but it is not simplistic either.

To read data from the KV layer, it uses regular `Scan` operations.

## High-level query engine

The tsdb Go package also offers a `Server` component, which
offers two gRPC methods: `(*Server).Query` and `(*Server).Dump`.

This `ts.Server` component translates calls from client to
calls to the low-level `(*ts.DB).Query` method:

- `(*ts.Server).Query` takes requests as `tspb.TimeSeriesQueryRequest`,
  and each request can contain zero or more low-level `tspb.Query` object.

  `(*ts.Server).Query` is responsible for *parallelizing* calls to the
  low-level `(*ts.DB).Query`, one call per `tspb.Query` in
  `tspb.TimeSeriesQueryRequest`.

  This endpoint is meant for use for data display as graphs.

- `(*ts.Server).Dump` takes request as `tspb.DumpRequest`, and
  collects all the timeseries data within the specified interval,
  for the given names and resolutions.

  This endpoint is meant for use to dump the low-level tsdb data.

## Integration in CockroachDB

The tsdb main components (`ts.DB` and `ts.Server`) are mostly
stateless: they contains merely a few static configuration parameters,
an interface to the underlying KV database (`kv.DB`, like the one used
by the SQL layer), and an interface to the cluster settings for
dynamic tuning knobs.

As of this writing, one `ts.DB` and one `ts.Server` are instantiated
per KV node, in the `(*server.Server).PreStart()` method.

### Metric collection

Separately from the tsdb engine, CockroachDB implements a component
called "metrics recorder" (`server/status/recorder.go`). This component
is mainly responsible for periodically scanning all the
in-memory metric objects (via `metric.Registry`) and keeping
a copy of their values for clients of the Prometheus
status endpoint (`/_status/vars`).

In addition to this responsibility, the recorder also implements
the `ts.DataSource` interface: it can serve scraping requests
by a `ts.DB`'s `PollSource`.

Upon startup, a KV node calls `PollSource` on its ts.DB and
points it to the metric recorder.

This ensures that all the metrics in the `metric.Registry` get
periodically saved into the tsdb.

### Queries, DB Console and other clients

The high-level query engine in `ts.Server` is exposed
to CockroachDB's external gRPC and HTTP endpoints.

The only HTTP endpoint is `/ts/db`, pointing to `(*ts.Server).Query`.
This is used by DB Console to display metric graphs, together with the
(independently defined) `/_admin/v1/metricmetadata` and
`/_admin/v1/chartcatalog`.

The gRPC endpoints are `Dump`, `DumpRaw` and `Query`, pointing
to the respective methods of `(*ts.Server)`:

- Dump / DumpRaw are used by the CLI command `cockroach debug tsdump`.
- Query is only used in unit tests. (Clients should use the HTTP endpoint instead.)

None of these APIs are currently documented as available to end-users.

## Previous writeups on the topic of timeseries

- [2016: RFC on Time Series Culling](../RFCS/20160901_time_series_culling.md)
- [2016: ts package documentation](../../pkg/ts/doc.go)

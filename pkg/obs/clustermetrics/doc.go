// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clustermetrics

// OVERVIEW
//
// Metrics in the classic sense, can be thought of as variables in the
// system. In traditional software deployments, a metric is represented by
// an in memory variable, and its value is periodically saved to a time
// series database for post-hoc analysis. For simplicity and verbosity, this
// means that each process in a distributed system keeps track of its own
// value for a metric.
//
// Let's use an example to clarify the notion, let's say that we have a 3
// node cluster, and want to keep track of the number of queries in the
// system. We can create a classic metric "sql.count" of which each node
// keeps track of a value. Node 1 receives 3 queries, node 2 receives 5, and
// node 3 receives 1. When those metrics are saved to the time series
// database, each node will have its own value, eg:
//
// sql.count:n1 - 3
// sql.count:n2 - 5
// sql.count:n3 - 1
//
// In some use cases however, having a value for every node is more confusing
// than useful. It's for these reasons that we are adding the cluster metrics.
// In this layout, instead of each node reporting its own copy of a metric, the
// metric has a single, cluster wide value. Using the same example from above,
// let's say we added a "cluster.sql.count" metric, a cluster metric equivalent
// of the above "sql.count" metric. When the system is scraped and the data
// stored in a time series database, only a single value for the entire
// cluster is ever stored, eg:
//
// cluster.sql.count - 9
//
// The value of this metric, instead of being tracked by each node in memory,
// will be kept in a system table, system.cluster_metrics, which can be thought
// of as a collection of eventually consistent cluster-wide variables.
//
// More information about the motivation for this feature can be found in
// the below design and github issue:
// - design https://docs.google.com/document/d/1uVA5XhXLI1q-PjcT3V1bFYpjXWO-3LMXvWZep7QY2ck/edit?tab=t.0
// - issue https://github.com/cockroachdb/cockroach/issues/154466
//
// Cluster metrics have a few core requirements:
// - Each node should be able to update a metric.
// - Only a single value for the metric should be store each interval in tsdb.
// - Only one node should report that metric on a prometheus-style scrape.
//
// DESIGN
//
//            ┌──────────────────────┐                                  ┌──────┐
//        ┌───►system.cluster_metrics│◄─────────────────────┐           │tsdb  │
//        │   └──────────────────────┘◄───┐                 │           └─▲────┘
//        │                               │                 │             │
//        │write                          │write            │read         │
//        │                               │                 │             │
//  ┌─────┼───────────────────┐     ┌─────┼──────────┬──────┼─────────────┼──────────┐
//  │ ┌───┴───────┐           │     │ ┌───┴───────┐  │ ┌────┴──────┐    ┌─┴────────┐ │
//  │ │task       │           │     │ │task       │  │ │job        │    │ts.db     │ │
//  │ │CM.Write   │           │     │ │CM.Write   │  │ │CM.Read    │    │Writer    │ │
//  │ └───┬───────┘           │     │ └───┬───────┘  │ └────┬──────┘    └─┬────────┘ │
//  │     │                   │     │     │          │      │             │          │
//  │     │                   │     │     │          │ ┌────▼──────┐      │          │
//  │    ┌▼─────────┐         │     │    ┌▼─────────┐│ │registry   │◄─────┘          │
//  │   ┌┴─────────┐│         │     │   ┌┴─────────┐││ │[cluster m]│◄─────┐          │
//  │ ┌─┴─────────┐││         │     │ ┌─┴─────────┐│││ └───┬───────┘      │          │
//  │ │Metric     │├┘         │     │ │Metric     │├┘│     │              │          │
//  │ │thing.cnt  ├┘          │     │ │thing.cnt  ├┘ │    ┌▼─────────┐    │          │
//  │ └───────────┘           │     │ └───────────┘  │   ┌┴─────────┐│    │          │
//  │      ▲    other metrics │     │      ▲         │ ┌─┴─────────┐││  ┌─┴────────┐ │
//  │      │                  │     │      │         │ │Metric     │├┘  │Prometheus│ │
//  │ ┌────┴──────┐           │     │ ┌────┴──────┐  │ │thing.cnt  ├┘   │Exporter  │ │
//  │ │thing      │           │     │ │thing      │  │ └───────────┘    └──────────┘ │
//  │ │           │           │     │ │           │  │                    ▲          │
//  │ └───────────┘           │     │ └───────────┘  │                    │          │
//  └─────────────────────────┘     └────────────────┴────────────────────┼──────────┘
//   Node                               Node [Reporter]                   │
//                                      May contain duplicate Metrics     │
//                                      between reader and writer*.       │
//                                                                        │ /metrics
//                                                                        │
//
// To achieve these requirements, we will keep the write path and the read
// path disjoint, and they will not interact, even on a node in which both
// are happening.
//
// On the read path, a job is started, so that only one node acts as a reader.
// This node has the informal role of "Cluster Metrics Reporter" and is
// responsible both for reporting cluster metrics on scrape, as well as storing
// these metrics to tsdb. The reader periodically polls the
// system.cluster_metrics table for updates, and adds those values as
// metric.Metrics to its own managed registry. This registry is accessible
// by tsdb and prometheus export, and therefore allows this node to report on
// cluster metrics.
//
// On the write path, each node is running an async task which uses a
// registered metric.Metric to accumulate updates, and then flushes
// them on some cadance to the system.cluster_metrics table. Registered
// metrics are NOT added to any registries which are used by tsdb or
// prometheus scrape.
//
// Because the paths are disjoint, its likely that two metric.Metric instances
// exist for the same theoretical metric. Conider again our above example of
// "cluster.sql.count". Let's say that some node, node 1 acts as the Reporter.
// On startup, it creates a metric.Metric for writing to "cluster.sql.count",
// let's call it SQLCountA. It will periodically accumulate Metric.Inc() calls
// on that first reference, SQLCountA, until a flush occurs, at which point the
// value is reset to zero. Then, as this node is the reporter, when it first
// reads the "cluster.sql.count" value from the system.cluster_metrics table,
// it creates a second metric reference, SQLCountB, which has the most recently
// updated value of the metric from the system table. It is this object which
// is used in tsdb write, and prometheus scrape.

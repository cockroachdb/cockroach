// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package cmreader watches the system.cluster_metrics table and
// materializes its rows into a local metric registry so that they
// appear alongside node-level metrics in TSDB and Prometheus scrapes.
//
// # Architecture
//
// The package has two main components:
//
//   - registry: a package-private wrapper around metric.Registry
//     that is exposed externally only as a metric.RegistryReader
//     via NewRegistryReader. This prevents callers outside the
//     cmreader package from mutating the registry directly; only
//     the registrySyncer may add or remove metrics.
//
//   - registrySyncer: subscribes to the system.cluster_metrics table via a
//     cmwatcher.Watcher rangefeed and keeps the registry in sync with
//     the table contents. Each row in the table corresponds to a single
//     metric (gauge, counter, or their vector variants). When a row is
//     inserted or updated, the registrySyncer creates or updates the
//     corresponding metric object in the registry. When a row is
//     deleted, the metric (or vector label set) is removed.
//
// # Unlabeled vs Labeled Metrics
//
// Cluster metrics come in two flavors:
//
// Unlabeled (scalar) metrics have no labels and are created as plain
// metric.Gauge or metric.Counter instances. Because they are ordinary
// scalar metrics, they are reported to both TSDB and Prometheus.
//
// Labeled (vector) metrics carry a set of key-value label pairs and
// are created as metric.GaugeVec or metric.CounterVec instances
// (exported variants). These are only reported to Prometheus, not
// TSDB, since TSDB does not support label dimensions.
//
// # Metadata Registration
//
// Before a metric row from the rangefeed can be materialized into the
// registry, its metadata must be registered in the cmmetrics package
// via RegisterClusterMetric or RegisterLabeledClusterMetric. If a row
// arrives whose name is not found in the metadata registry, the
// registrySyncer logs an error and skips the row.
package cmreader

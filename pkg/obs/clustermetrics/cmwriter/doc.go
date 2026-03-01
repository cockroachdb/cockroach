// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package cmwriter provides the write-side implementation for cluster metrics.
// It periodically flushes registered metrics to a MetricStore.
//
// The Writer is initialized on SQL server startup and uses an SQLStore
// to persist metrics to system.cluster_metrics.
package cmwriter

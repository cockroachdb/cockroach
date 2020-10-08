// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package raftentry

import "github.com/cockroachdb/cockroach/pkg/util/metric"

var (
	metaEntryCacheSize = metric.Metadata{
		Name:        "raft.entrycache.size",
		Help:        "Number of Raft entries in the Raft entry cache",
		Measurement: "Entry Count",
		Unit:        metric.Unit_COUNT,
	}
	metaEntryCacheBytes = metric.Metadata{
		Name:        "raft.entrycache.bytes",
		Help:        "Aggregate size of all Raft entries in the Raft entry cache",
		Measurement: "Entry Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaEntryCacheAccesses = metric.Metadata{
		Name:        "raft.entrycache.accesses",
		Help:        "Number of cache lookups in the Raft entry cache",
		Measurement: "Accesses",
		Unit:        metric.Unit_COUNT,
	}
	metaEntryCacheHits = metric.Metadata{
		Name:        "raft.entrycache.hits",
		Help:        "Number of successful cache lookups in the Raft entry cache",
		Measurement: "Hits",
		Unit:        metric.Unit_COUNT,
	}
)

// Metrics is the set of metrics for the raft entry cache.
type Metrics struct {
	// NB: the values in the gauges are updated asynchronously and may hold stale
	// values in the face of concurrent updates.
	Size     *metric.Gauge
	Bytes    *metric.Gauge
	Accesses *metric.Counter
	Hits     *metric.Counter
}

func makeMetrics() Metrics {
	return Metrics{
		Size:     metric.NewGauge(metaEntryCacheSize),
		Bytes:    metric.NewGauge(metaEntryCacheBytes),
		Accesses: metric.NewCounter(metaEntryCacheAccesses),
		Hits:     metric.NewCounter(metaEntryCacheHits),
	}
}

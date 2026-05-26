// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	metaEntryCacheReadBytes = metric.Metadata{
		Name:        "raft.entrycache.read_bytes",
		Help:        "Counter of bytes in entries returned from the Raft entry cache",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
)

// Metrics is the set of metrics for the raft entry cache.
type Metrics struct {
	// NB: the values in the gauges are updated asynchronously and may hold stale
	// values in the face of concurrent updates.
	Size      *metric.Gauge
	Bytes     *metric.Gauge
	Accesses  *metric.Counter
	Hits      *metric.Counter
	ReadBytes *metric.Counter
}

func makeMetrics() Metrics {
	return Metrics{
		Size:      metric.NewGauge(metaEntryCacheSize),
		Bytes:     metric.NewGauge(metaEntryCacheBytes),
		Accesses:  metric.NewCounter(metaEntryCacheAccesses),
		Hits:      metric.NewCounter(metaEntryCacheHits),
		ReadBytes: metric.NewCounter(metaEntryCacheReadBytes),
	}
}

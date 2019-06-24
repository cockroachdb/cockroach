// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package compactor

import "github.com/cockroachdb/cockroach/pkg/util/metric"

// Metrics holds all metrics relating to a Compactor.
type Metrics struct {
	BytesQueued         *metric.Gauge
	BytesSkipped        *metric.Counter
	BytesCompacted      *metric.Counter
	CompactionSuccesses *metric.Counter
	CompactionFailures  *metric.Counter
	CompactingNanos     *metric.Counter
}

// MetricStruct implements the metrics.Struct interface.
func (Metrics) MetricStruct() {}

var _ metric.Struct = Metrics{}

var (
	metaBytesQueued = metric.Metadata{
		Name:        "compactor.suggestionbytes.queued",
		Help:        "Number of logical bytes in suggested compactions in the queue",
		Measurement: "Logical Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaBytesSkipped = metric.Metadata{
		Name:        "compactor.suggestionbytes.skipped",
		Help:        "Number of logical bytes in suggested compactions which were not compacted",
		Measurement: "Logical Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaBytesCompacted = metric.Metadata{
		Name:        "compactor.suggestionbytes.compacted",
		Help:        "Number of logical bytes compacted from suggested compactions",
		Measurement: "Logical Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaCompactionSuccesses = metric.Metadata{
		Name:        "compactor.compactions.success",
		Help:        "Number of successful compaction requests sent to the storage engine",
		Measurement: "Compaction Requests",
		Unit:        metric.Unit_COUNT,
	}
	metaCompactionFailures = metric.Metadata{
		Name:        "compactor.compactions.failure",
		Help:        "Number of failed compaction requests sent to the storage engine",
		Measurement: "Compaction Requests",
		Unit:        metric.Unit_COUNT,
	}
	metaCompactingNanos = metric.Metadata{
		Name:        "compactor.compactingnanos",
		Help:        "Number of nanoseconds spent compacting ranges",
		Measurement: "Processing Time",
		Unit:        metric.Unit_NANOSECONDS,
	}
)

// makeMetrics returns a Metrics struct.
func makeMetrics() Metrics {
	return Metrics{
		BytesQueued:         metric.NewGauge(metaBytesQueued),
		BytesSkipped:        metric.NewCounter(metaBytesSkipped),
		BytesCompacted:      metric.NewCounter(metaBytesCompacted),
		CompactionSuccesses: metric.NewCounter(metaCompactionSuccesses),
		CompactionFailures:  metric.NewCounter(metaCompactionFailures),
		CompactingNanos:     metric.NewCounter(metaCompactingNanos),
	}
}

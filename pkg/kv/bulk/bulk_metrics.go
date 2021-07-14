// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bulk

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// Metrics contains pointers to the metrics for
// monitoring bulk operations.
type Metrics struct {
	MaxBytesHist  *metric.Histogram
	CurBytesCount *metric.Gauge

	WrittenBytes *metric.Counter

	RangeFlushes  *metric.Counter
	SizeFlushes   *metric.Counter
	ManualFlushes *metric.Counter
}

// MetricStruct implements the metrics.Struct interface.
func (Metrics) MetricStruct() {}

var _ metric.Struct = Metrics{}

var (
	metaMemMaxBytes = metric.Metadata{
		Name:        "sql.mem.bulk.max",
		Help:        "Memory usage per sql statement for bulk operations",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	metaMemCurBytes = metric.Metadata{
		Name:        "sql.mem.bulk.current",
		Help:        "Current sql statement memory usage for bulk operations",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}

	metaWrittenBytes = metric.Metadata{
		Name:        "bulk.written_bytes",
		Help:        "Number of bytes written by bulk",
		Measurement: "Written bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaRangeFlushes = metric.Metadata{
		Name:        "bulk.sst_flushes.range",
		Help:        "Number of flushes due to encountering a range boundary",
		Measurement: "Number of flushes",
		Unit:        metric.Unit_COUNT,
	}
	metaSizeFlushes = metric.Metadata{
		Name:        "bulk.sst_flushes.size",
		Help:        "Number of flushes due to reaching size threshold",
		Measurement: "Number of flushes",
		Unit:        metric.Unit_COUNT,
	}
	metaManualFlushes = metric.Metadata{
		Name:        "bulk.sst_flushes.manual",
		Help:        "Number of flushes due to manual flushing by bulk subsystems",
		Measurement: "Number of flushes",
		Unit:        metric.Unit_COUNT,
	}
)

// See pkg/sql/mem_metrics.go
// log10int64times1000 = log10(math.MaxInt64) * 1000, rounded up somewhat
const log10int64times1000 = 19 * 1000

// MakeBulkMetrics instantiates the metrics holder for bulk operation monitoring.
func MakeBulkMetrics(histogramWindow time.Duration) Metrics {
	return Metrics{
		MaxBytesHist:  metric.NewHistogram(metaMemMaxBytes, histogramWindow, log10int64times1000, 3),
		CurBytesCount: metric.NewGauge(metaMemCurBytes),
		WrittenBytes:  metric.NewCounter(metaWrittenBytes),
		RangeFlushes:  metric.NewCounter(metaRangeFlushes),
		SizeFlushes:   metric.NewCounter(metaSizeFlushes),
		ManualFlushes: metric.NewCounter(metaManualFlushes),
	}
}

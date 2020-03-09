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
)

// See pkg/sql/mem_metrics.go
// log10int64times1000 = log10(math.MaxInt64) * 1000, rounded up somewhat
const log10int64times1000 = 19 * 1000

// MakeBulkMetrics instantiates the metrics holder for bulk operation monitoring.
func MakeBulkMetrics(histogramWindow time.Duration) Metrics {
	return Metrics{
		MaxBytesHist:  metric.NewHistogram(metaMemMaxBytes, histogramWindow, log10int64times1000, 3),
		CurBytesCount: metric.NewGauge(metaMemCurBytes),
	}
}

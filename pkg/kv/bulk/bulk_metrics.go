// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulk

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// Metrics contains pointers to the metrics for
// monitoring bulk operations.
type Metrics struct {
	MaxBytesHist  metric.IHistogram
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
		MaxBytesHist: metric.NewHistogram(metric.HistogramOptions{
			Metadata:     metaMemMaxBytes,
			Duration:     histogramWindow,
			MaxVal:       log10int64times1000,
			SigFigs:      3,
			BucketConfig: metric.MemoryUsage64MBBuckets,
		}),
		CurBytesCount: metric.NewGauge(metaMemCurBytes),
	}
}

type sz int64

func (b sz) String() string { return string(humanizeutil.IBytes(int64(b))) }
func (b sz) SafeValue()     {}

type timing time.Duration

func (t timing) String() string { return time.Duration(t).Round(time.Second).String() }
func (t timing) SafeValue()     {}

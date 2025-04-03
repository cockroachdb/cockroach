// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package checkpoint

import (
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
)

var (
	metaCreateNanos = metric.Metadata{
		Name:        "changefeed.checkpoint.create_nanos",
		Help:        "Time it takes to create a changefeed checkpoint",
		Unit:        metric.Unit_NANOSECONDS,
		Measurement: "Nanoseconds",
	}

	metaTotalBytes = metric.Metadata{
		Name:        "changefeed.checkpoint.total_bytes",
		Help:        "Total size of a changefeed checkpoint",
		Unit:        metric.Unit_BYTES,
		Measurement: "Bytes",
	}

	metaSpanCount = metric.Metadata{
		Name:        "changefeed.checkpoint.span_count",
		Help:        "Number of spans in a changefeed checkpoint",
		Unit:        metric.Unit_COUNT,
		Measurement: "Spans",
	}
)

type AggMetrics struct {
	CreateNanos *aggmetric.AggHistogram
	TotalBytes  *aggmetric.AggHistogram
	SpanCount   *aggmetric.AggHistogram
}

func NewAggMetrics(b aggmetric.Builder) *AggMetrics {
	return &AggMetrics{
		CreateNanos: b.Histogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePrometheus,
			Metadata:     metaCreateNanos,
			Duration:     base.DefaultHistogramWindowInterval(),
			BucketConfig: metric.IOLatencyBuckets,
		}),
		TotalBytes: b.Histogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePrometheus,
			Metadata:     metaTotalBytes,
			Duration:     base.DefaultHistogramWindowInterval(),
			BucketConfig: metric.MemoryUsage64MBBuckets,
		}),
		SpanCount: b.Histogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePrometheus,
			Metadata:     metaSpanCount,
			Duration:     base.DefaultHistogramWindowInterval(),
			BucketConfig: metric.DataCount16MBuckets,
		}),
	}
}

func (a *AggMetrics) AddChild(labelVals ...string) *Metrics {
	return &Metrics{
		CreateNanos: a.CreateNanos.AddChild(labelVals...),
		TotalBytes:  a.TotalBytes.AddChild(labelVals...),
		SpanCount:   a.SpanCount.AddChild(labelVals...),
	}
}

// MetricStruct implements the metric.Struct interface.
func (*AggMetrics) MetricStruct() {}

var _ metric.Struct = (*AggMetrics)(nil)

type Metrics struct {
	CreateNanos *aggmetric.Histogram
	TotalBytes  *aggmetric.Histogram
	SpanCount   *aggmetric.Histogram
}

// MetricStruct implements the metric.Struct interface.
func (*Metrics) MetricStruct() {}

var _ metric.Struct = (*Metrics)(nil)

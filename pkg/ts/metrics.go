// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ts

import "github.com/cockroachdb/cockroach/pkg/util/metric"

var (
	// Storage metrics.
	metaWriteSamples = metric.Metadata{
		Name:        "timeseries.write.samples",
		Help:        "Total number of metric samples written to disk",
		Measurement: "Metric Samples",
		Unit:        metric.Unit_COUNT,
	}
	metaWriteBytes = metric.Metadata{
		Name:        "timeseries.write.bytes",
		Help:        "Total size in bytes of metric samples written to disk",
		Measurement: "Storage",
		Unit:        metric.Unit_BYTES,
	}
	metaWriteErrors = metric.Metadata{
		Name:        "timeseries.write.errors",
		Help:        "Total errors encountered while attempting to write metrics to disk",
		Measurement: "Errors",
		Unit:        metric.Unit_COUNT,
	}
)

// TimeSeriesMetrics contains metrics relevant to the time series system.
type TimeSeriesMetrics struct {
	WriteSamples *metric.Counter
	WriteBytes   *metric.Counter
	WriteErrors  *metric.Counter
}

// NewTimeSeriesMetrics creates a new instance of TimeSeriesMetrics.
func NewTimeSeriesMetrics() *TimeSeriesMetrics {
	return &TimeSeriesMetrics{
		WriteSamples: metric.NewCounter(metaWriteSamples),
		WriteBytes:   metric.NewCounter(metaWriteBytes),
		WriteErrors:  metric.NewCounter(metaWriteErrors),
	}
}

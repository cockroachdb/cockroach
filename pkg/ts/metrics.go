// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package ts

import "github.com/cockroachdb/cockroach/pkg/util/metric"

var (
	// Storage metrics.
	metaWriteSamples = metric.Metadata{
		Name: "timeseries.write.samples",
		Help: "Total number of metric samples written to disk",
	}
	metaWriteBytes = metric.Metadata{
		Name: "timeseries.write.bytes",
		Help: "Total size in bytes of metric samples written to disk",
	}
	metaWriteErrors = metric.Metadata{
		Name: "timeseries.write.errors",
		Help: "Total errors encountered while attempting to write metrics to disk",
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

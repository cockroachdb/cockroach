// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvevent

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

var (
	metaChangefeedBufferEntriesIn = metric.Metadata{
		Name:        "changefeed.buffer_entries.in",
		Help:        "Total entries entering the buffer between raft and changefeed sinks",
		Measurement: "Entries",
		Unit:        metric.Unit_COUNT,
	}
	metaChangefeedBufferEntriesOut = metric.Metadata{
		Name:        "changefeed.buffer_entries.out",
		Help:        "Total entries leaving the buffer between raft and changefeed sinks",
		Measurement: "Entries",
		Unit:        metric.Unit_COUNT,
	}
)

// Metrics is a metric.Struct for kvfeed metrics.
type Metrics struct {
	BufferEntriesIn  *metric.Counter
	BufferEntriesOut *metric.Counter
}

// MakeMetrics constructs a Metrics struct with the provided histogram window.
func MakeMetrics(histogramWindow time.Duration) Metrics {
	return Metrics{
		BufferEntriesIn:  metric.NewCounter(metaChangefeedBufferEntriesIn),
		BufferEntriesOut: metric.NewCounter(metaChangefeedBufferEntriesOut),
	}
}

var _ (metric.Struct) = (*Metrics)(nil)

// MetricStruct makes Metrics a metric.Struct.
func (m Metrics) MetricStruct() {}

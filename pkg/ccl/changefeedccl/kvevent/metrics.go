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
	metaChangefeedBufferEntriesReleased = metric.Metadata{
		Name:        "changefeed.buffer_entries.released",
		Help:        "Total entries processed, emitted and acknowledged by the sinks",
		Measurement: "Entries",
		Unit:        metric.Unit_COUNT,
	}
	metaChangefeedBufferMemAcquired = metric.Metadata{
		Name:        "changefeed.buffer_entries_mem.acquired",
		Help:        "Total amount of memory acquired for entries as they enter the system",
		Measurement: "Entries",
		Unit:        metric.Unit_COUNT,
	}
	metaChangefeedBufferMemReleased = metric.Metadata{
		Name:        "changefeed.buffer_entries_mem.released",
		Help:        "Total amount of memory released by the entries after they have been emitted",
		Measurement: "Entries",
		Unit:        metric.Unit_COUNT,
	}
	metaChangefeedBufferPushbackNanos = metric.Metadata{
		Name:        "changefeed.buffer_pushback_nanos",
		Help:        "Total time spent waiting while the buffer was full",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
)

// Metrics is a metric.Struct for kvfeed metrics.
type Metrics struct {
	BufferEntriesIn          *metric.Counter
	BufferEntriesOut         *metric.Counter
	BufferEntriesReleased    *metric.Counter
	BufferPushbackNanos      *metric.Counter
	BufferEntriesMemAcquired *metric.Counter
	BufferEntriesMemReleased *metric.Counter
}

// MakeMetrics constructs a Metrics struct with the provided histogram window.
func MakeMetrics(histogramWindow time.Duration) Metrics {
	return Metrics{
		BufferEntriesIn:          metric.NewCounter(metaChangefeedBufferEntriesIn),
		BufferEntriesOut:         metric.NewCounter(metaChangefeedBufferEntriesOut),
		BufferEntriesReleased:    metric.NewCounter(metaChangefeedBufferEntriesReleased),
		BufferEntriesMemAcquired: metric.NewCounter(metaChangefeedBufferMemAcquired),
		BufferEntriesMemReleased: metric.NewCounter(metaChangefeedBufferMemReleased),
		BufferPushbackNanos:      metric.NewCounter(metaChangefeedBufferPushbackNanos),
	}
}

var _ (metric.Struct) = (*Metrics)(nil)

// MetricStruct makes Metrics a metric.Struct.
func (m Metrics) MetricStruct() {}

// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvfeed

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

const pollRequestNanosHistMaxLatency = time.Hour

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
	metaChangefeedPollRequestNanos = metric.Metadata{
		Name:        "changefeed.poll_request_nanos",
		Help:        "Time spent fetching changes",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
)

// Metrics is a metric.Struct for kvfeed metrics.
//
// TODO(ajwerner): Make these metrics more reasonable given the removal of the
// poller and polling in general.
type Metrics struct {
	BufferEntriesIn      *metric.Counter
	BufferEntriesOut     *metric.Counter
	PollRequestNanosHist *metric.Histogram
}

// MakeMetrics constructs a Metrics struct with the provided histogram window.
func MakeMetrics(histogramWindow time.Duration) Metrics {
	return Metrics{
		BufferEntriesIn:  metric.NewCounter(metaChangefeedBufferEntriesIn),
		BufferEntriesOut: metric.NewCounter(metaChangefeedBufferEntriesOut),
		// Metrics for changefeed performance debugging: - PollRequestNanos and
		// PollRequestNanosHist, things are first
		//   fetched with some limited concurrency. We're interested in both the
		//   total amount of time fetching as well as outliers, so we need both
		//   the counter and the histogram.
		// - N/A. Each change is put into a buffer. Right now nothing measures
		//   this since the buffer doesn't actually buffer and so it just tracks
		//   the poll sleep time.
		// - ProcessingNanos. Everything from the buffer until the SQL row is
		//   about to be emitted. This includes TableMetadataNanos, which is
		//   dependent on network calls, so also tracked in case it's ever the
		//   cause of a ProcessingNanos blowup.
		// - EmitNanos and FlushNanos. All of our interactions with the sink.
		PollRequestNanosHist: metric.NewHistogram(
			metaChangefeedPollRequestNanos, histogramWindow,
			pollRequestNanosHistMaxLatency.Nanoseconds(), 1),
	}
}

var _ (metric.Struct) = (*Metrics)(nil)

// MetricStruct makes Metrics a metric.Struct.
func (m Metrics) MetricStruct() {}

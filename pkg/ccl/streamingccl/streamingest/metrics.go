// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingest

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

var (
	metaStreamingEventsIngested = metric.Metadata{
		Name:        "streaming.events_ingested",
		Help:        "Events ingested by all ingestion jobs",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
	metaStreamingResolvedEventsIngested = metric.Metadata{
		Name:        "streaming.resolved_events_ingested",
		Help:        "Resolved events ingested by all ingestion jobs",
		Measurement: "Events",
		Unit:        metric.Unit_COUNT,
	}
	metaStreamingIngestedBytes = metric.Metadata{
		Name:        "streaming.ingested_bytes",
		Help:        "Bytes ingested by all ingestion jobs",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaStreamingFlushes = metric.Metadata{
		Name:        "streaming.flushes",
		Help:        "Total flushes across all ingestion jobs",
		Measurement: "Flushes",
		Unit:        metric.Unit_COUNT,
	}
)

// Metrics are for production monitoring of stream ingestion jobs.
type Metrics struct {
	IngestedEvents *metric.Counter
	IngestedBytes  *metric.Counter
	Flushes        *metric.Counter
	ResolvedEvents *metric.Counter
}

// MetricStruct implements the metric.Struct interface.
func (*Metrics) MetricStruct() {}

// MakeMetrics makes the metrics for stream ingestion job monitoring.
func MakeMetrics(histogramWindow time.Duration) metric.Struct {
	m := &Metrics{
		IngestedEvents: metric.NewCounter(metaStreamingEventsIngested),
		IngestedBytes:  metric.NewCounter(metaStreamingIngestedBytes),
		Flushes:        metric.NewCounter(metaStreamingFlushes),
		ResolvedEvents: metric.NewCounter(metaStreamingResolvedEventsIngested),
	}
	return m
}

func init() {
	jobs.MakeStreamIngestMetricsHook = MakeMetrics
}

// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemafeed

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

var metaChangefeedTableMetadataNanos = metric.Metadata{
	Name:        "changefeed.schemafeed.table_metadata_nanos",
	Help:        "Time blocked while verifying table metadata histories",
	Measurement: "Nanoseconds",
	Unit:        metric.Unit_NANOSECONDS,
}

var metaChangefeedTableHistoryScans = metric.Metadata{
	Name:        "changefeed.schemafeed.table_history_scans",
	Help:        "The number of table history scans during polling",
	Measurement: "Counts",
	Unit:        metric.Unit_COUNT,
}

// Metrics is a metric.Struct for schemafeed metrics.
type Metrics struct {
	TableMetadataNanos *metric.Counter
	TableHistoryScans  *metric.Counter
}

// MetricStruct implements the metric.Struct interface.
func (Metrics) MetricStruct() {}

// MakeMetrics constructs a Metrics struct with the provided histogram window.
func MakeMetrics(histogramWindow time.Duration) Metrics {
	return Metrics{
		TableMetadataNanos: metric.NewCounter(metaChangefeedTableMetadataNanos),
		TableHistoryScans:  metric.NewCounter(metaChangefeedTableHistoryScans),
	}
}

var _ (metric.Struct) = (*Metrics)(nil)

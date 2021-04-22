// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package schemafeed

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

var metaChangefeedTableMetadataNanos = metric.Metadata{
	Name:        "changefeed.table_metadata_nanos",
	Help:        "Time blocked while verifying table metadata histories",
	Measurement: "Nanoseconds",
	Unit:        metric.Unit_NANOSECONDS,
}

// Metrics is a metric.Struct for schemafeed metrics.
type Metrics struct {
	TableMetadataNanos *metric.Counter
}

// MetricStruct implements the metric.Struct interface.
func (Metrics) MetricStruct() {}

// MakeMetrics constructs a Metrics struct with the provided histogram window.
func MakeMetrics(histogramWindow time.Duration) Metrics {
	return Metrics{
		TableMetadataNanos: metric.NewCounter(metaChangefeedTableMetadataNanos),
	}
}

var _ (metric.Struct) = (*Metrics)(nil)

// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowinfra

import "github.com/cockroachdb/cockroach/pkg/util/metric"

var (
	// MetaMaxRowSizeLog is metadata for the
	// sql.guardrails.max_row_size_log.count{.internal} metrics.
	MetaMaxRowSizeLog = metric.Metadata{
		Name:        "sql.guardrails.max_row_size_log.count",
		Help:        "Number of rows observed violating sql.guardrails.max_row_size_log",
		Measurement: "Rows",
		Unit:        metric.Unit_COUNT,
	}
	// MetaMaxRowSizeErr is metadata for the
	// sql.guardrails.max_row_size_err.count{.internal} metrics.
	MetaMaxRowSizeErr = metric.Metadata{
		Name:        "sql.guardrails.max_row_size_err.count",
		Help:        "Number of rows observed violating sql.guardrails.max_row_size_err",
		Measurement: "Rows",
		Unit:        metric.Unit_COUNT,
	}
)

// Metrics holds metrics measuring calls into the KV layer by various parts of
// the SQL layer, including by queries, schema changes, and bulk IO.
type Metrics struct {
	MaxRowSizeLogCount *metric.Counter
	MaxRowSizeErrCount *metric.Counter
}

var _ metric.Struct = Metrics{}

// MetricStruct is part of the metric.Struct interface.
func (Metrics) MetricStruct() {}

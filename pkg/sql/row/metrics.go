// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package row

import "github.com/cockroachdb/cockroach/pkg/util/metric"

var (
	MetaMaxRowSizeLog = metric.Metadata{
		Name:        "sql.guardrails.max_row_size_log.count",
		Help:        "Number of rows observed violating sql.guardrails.max_row_size_log",
		Measurement: "Rows",
		Unit:        metric.Unit_COUNT,
	}
	MetaMaxRowSizeErr = metric.Metadata{
		Name:        "sql.guardrails.max_row_size_err.count",
		Help:        "Number of rows observed violating sql.guardrails.max_row_size_err",
		Measurement: "Rows",
		Unit:        metric.Unit_COUNT,
	}
)

type RowMetrics struct {
	MaxRowSizeLogCount *metric.Counter
	MaxRowSizeErrCount *metric.Counter
}

var _ metric.Struct = RowMetrics{}

// MetricStruct is part of the metric.Struct interface.
func (RowMetrics) MetricStruct() {}

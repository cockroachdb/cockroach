// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import "github.com/cockroachdb/cockroach/pkg/util/metric"

// TODO(ajwerner): Add many more metrics.

var (
	metaRunning = metric.Metadata{
		Name:        "sql.schema_changer.running",
		Help:        "Gauge of currently running schema changes",
		Measurement: "Schema changes",
		Unit:        metric.Unit_COUNT,
	}
	metaSuccesses = metric.Metadata{
		Name:        "sql.schema_changer.successes",
		Help:        "Counter of the number of schema changer resumes which succeed",
		Measurement: "Schema changes",
		Unit:        metric.Unit_COUNT,
	}
	metaRetryErrors = metric.Metadata{
		Name:        "sql.schema_changer.retry_errors",
		Help:        "Counter of the number of retriable errors experienced by the schema changer",
		Measurement: "Errors",
		Unit:        metric.Unit_COUNT,
	}
	metaPermanentErrors = metric.Metadata{
		Name:        "sql.schema_changer.permanent_errors",
		Help:        "Counter of the number of permanent errors experienced by the schema changer",
		Measurement: "Errors",
		Unit:        metric.Unit_COUNT,
	}
)

// SchemaChangerMetrics are metrics corresponding to the schema changer.
type SchemaChangerMetrics struct {
	RunningSchemaChanges *metric.Gauge
	Successes            *metric.Counter
	RetryErrors          *metric.Counter
	PermanentErrors      *metric.Counter
}

// MetricStruct makes SchemaChangerMetrics a metric.Struct.
func (s *SchemaChangerMetrics) MetricStruct() {}

var _ metric.Struct = (*SchemaChangerMetrics)(nil)

// NewSchemaChangerMetrics constructs a new SchemaChangerMetrics.
func NewSchemaChangerMetrics() *SchemaChangerMetrics {
	return &SchemaChangerMetrics{
		RunningSchemaChanges: metric.NewGauge(metaRunning),
		Successes:            metric.NewCounter(metaSuccesses),
		RetryErrors:          metric.NewCounter(metaRetryErrors),
		PermanentErrors:      metric.NewCounter(metaPermanentErrors),
	}
}

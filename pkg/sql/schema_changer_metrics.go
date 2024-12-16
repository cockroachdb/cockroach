// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// TODO(ajwerner): Add many more metrics.

// SchemaChangerMetrics are metrics corresponding to the schema changer.
type SchemaChangerMetrics struct {
	ConstraintErrors    telemetry.Counter
	UncategorizedErrors telemetry.Counter
}

// MetricStruct makes SchemaChangerMetrics a metric.Struct.
func (s *SchemaChangerMetrics) MetricStruct() {}

var _ metric.Struct = (*SchemaChangerMetrics)(nil)

// NewSchemaChangerMetrics constructs a new SchemaChangerMetrics.
func NewSchemaChangerMetrics() *SchemaChangerMetrics {
	return &SchemaChangerMetrics{
		ConstraintErrors:    sqltelemetry.SchemaChangeErrorCounter("constraint_violation"),
		UncategorizedErrors: sqltelemetry.SchemaChangeErrorCounter("uncategorized"),
	}
}

// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// TODO(ajwerner): Add many more metrics.

var (
	metaObjects = metric.Metadata{
		Name:        "sql.schema_changer.object_count",
		Help:        "Counter of the number of objects in the cluster",
		Measurement: "Objects",
		Unit:        metric.Unit_COUNT,
	}
)

// SchemaChangerMetrics are metrics corresponding to the schema changer.
type SchemaChangerMetrics struct {
	ConstraintErrors    telemetry.Counter
	UncategorizedErrors telemetry.Counter
	ObjectCount         *metric.Gauge
}

// MetricStruct makes SchemaChangerMetrics a metric.Struct.
func (s *SchemaChangerMetrics) MetricStruct() {}

var _ metric.Struct = (*SchemaChangerMetrics)(nil)

// NewSchemaChangerMetrics constructs a new SchemaChangerMetrics.
func NewSchemaChangerMetrics() *SchemaChangerMetrics {
	return &SchemaChangerMetrics{
		ConstraintErrors:    sqltelemetry.SchemaChangeErrorCounter("constraint_violation"),
		UncategorizedErrors: sqltelemetry.SchemaChangeErrorCounter("uncategorized"),
		ObjectCount:         metric.NewGauge(metaObjects),
	}
}

// UpdateDescriptorCount updates our sql.schema_changer.object_count gauge with
// a fresh count of objects in the system.descriptor table.
func UpdateDescriptorCount(
	ctx context.Context, execCfg *ExecutorConfig, metric *SchemaChangerMetrics,
) error {
	return execCfg.InternalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		desc, err := txn.Descriptors().ByIDWithLeased(txn.KV()).Get().Table(ctx, keys.DescriptorTableID)
		if err != nil {
			return err
		}
		tableStats, err := execCfg.TableStatsCache.GetFreshTableStats(ctx, desc, nil /* typeResolver */)
		if err != nil {
			return err
		}
		if len(tableStats) > 0 {
			// Use the row count from the most recent statistic.
			metric.ObjectCount.Update(int64(tableStats[0].RowCount))
		}
		return nil
	})
}

// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var (
	metaDatabaseCount = metric.Metadata{
		Name:        "sql.schema.database_count",
		Help:        "The number of databases hosted by this cluster",
		Measurement: "Databases",
		Unit:        metric.Unit_COUNT,
	}

	metaTableCount = metric.Metadata{
		Name:        "sql.schema.table_count",
		Help:        "The number of tables hosted by this cluster",
		Measurement: "Tables",
		Unit:        metric.Unit_COUNT,
	}

	metaUpdatedAt = metric.Metadata{
		Name:        "sql.schema.updated_at",
		Help:        "The unix timestamp of the most recent update to schema metrics",
		Measurement: "Update Time",
		Unit:        metric.Unit_TIMESTAMP_NS,
	}

	internalDatabaseNames = map[string]struct{}{
		"system":   {},
		"postgres": {},
	}
)

// SchemaMetrics holds gauges counting the number of databases and tables in the cluster.
type SchemaMetrics struct {
	DatabaseCountExternal *metric.Gauge
	DatabaseCountInternal *metric.Gauge
	TableCountExternal    *metric.Gauge
	TableCountInternal    *metric.Gauge
	UpdatedAt             *metric.Gauge
}

// MetricStruct makes SchemaMetrics a metric.Struct.
func (s *SchemaMetrics) MetricStruct() {}

var _ metric.Struct = (*SchemaMetrics)(nil)

// NewSchemaMetrics constructs a new SchemaMetrics.
func NewSchemaMetrics() *SchemaMetrics {
	return &SchemaMetrics{
		DatabaseCountExternal: metric.NewGauge(labelInternal(metaDatabaseCount, false)),
		DatabaseCountInternal: metric.NewGauge(labelInternal(metaDatabaseCount, true)),
		TableCountExternal:    metric.NewGauge(labelInternal(metaTableCount, false)),
		TableCountInternal:    metric.NewGauge(labelInternal(metaTableCount, true)),
		UpdatedAt:             metric.NewGauge(metaUpdatedAt),
	}
}

func labelInternal(metadata metric.Metadata, internal bool) metric.Metadata {
	metadata.AddLabel("internal", strconv.FormatBool(internal))
	return metadata
}

// Refresh runs a SQL query to update our gauges.
func (s *SchemaMetrics) Refresh(ctx context.Context, ie *InternalExecutor) (retErr error) {
	it, err := ie.QueryIterator(
		ctx,
		"refresh-schema-metrics",
		nil,
		`
			SELECT
				d.name, count(t.name)
			FROM
				crdb_internal.databases AS d
				LEFT JOIN crdb_internal.tables AS t ON
						d.name = t.database_name
			WHERE
				t.state IS NULL OR t.state != 'DROP'
			GROUP BY
				d.name`,
	)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.CombineErrors(retErr, it.Close()) }()

	var externalDatabaseCount, externalTableCount int64
	var internalDatabaseCount, internalTableCount int64

	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		databaseName := string(*it.Cur()[0].(*tree.DString))
		tableCount := int64(*it.Cur()[1].(*tree.DInt))

		if _, ok := internalDatabaseNames[databaseName]; ok {
			internalDatabaseCount++
			internalTableCount += tableCount
		} else {
			externalDatabaseCount++
			externalTableCount += tableCount
		}
	}

	if err != nil {
		return err
	}

	s.DatabaseCountExternal.Update(externalDatabaseCount)
	s.DatabaseCountInternal.Update(internalDatabaseCount)
	s.TableCountExternal.Update(externalTableCount)
	s.TableCountInternal.Update(internalTableCount)
	s.UpdatedAt.Update(timeutil.Now().UnixNano())

	return nil
}

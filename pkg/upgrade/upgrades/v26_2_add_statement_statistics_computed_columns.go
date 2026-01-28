// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// Compute expression components - must match exactly what's in systemschema/system.go.
const (
	execStatsExecCountExpr   = `((statistics->'execution_statistics':::STRING)->'cnt':::STRING)::INT8`
	executionCountExpr       = `((statistics->'statistics':::STRING)->'cnt':::STRING)::INT8`
	serviceLatencyExpr       = `(((statistics->'statistics':::STRING)->'svcLat':::STRING)->'mean':::STRING)::FLOAT8`
	cpuSqlNanosExpr          = `(((statistics->'execution_statistics':::STRING)->'cpuSQLNanos':::STRING)->'mean':::STRING)::FLOAT8`
	contentionTimeExpr       = `(((statistics->'execution_statistics':::STRING)->'contentionTime':::STRING)->'mean':::STRING)::FLOAT8`
	svcLatSqDiffExpr         = `(((statistics->'statistics':::STRING)->'svcLat':::STRING)->'sqDiff':::STRING)::FLOAT8`
	cpuSqlNanosSqDiffExpr    = `(((statistics->'execution_statistics':::STRING)->'cpuSQLNanos':::STRING)->'sqDiff':::STRING)::FLOAT8`
	contentionTimeSqDiffExpr = `(((statistics->'execution_statistics':::STRING)->'contentionTime':::STRING)->'sqDiff':::STRING)::FLOAT8`
	kvCpuTimeNanosExpr       = `(((statistics->'statistics':::STRING)->'kvCPUTimeNanos':::STRING)->'mean':::STRING)::FLOAT8`
	kvCpuTimeNanosSqDiffExpr = `(((statistics->'statistics':::STRING)->'kvCPUTimeNanos':::STRING)->'sqDiff':::STRING)::FLOAT8`
	admissionWaitTimeExpr    = `(((statistics->'execution_statistics':::STRING)->'admissionWaitTime':::STRING)->'mean':::STRING)::FLOAT8`
	admissionWaitTimeSqDiff  = `(((statistics->'execution_statistics':::STRING)->'admissionWaitTime':::STRING)->'sqDiff':::STRING)::FLOAT8`
	rowsReadExpr             = `(((statistics->'statistics':::STRING)->'rowsRead':::STRING)->'mean':::STRING)::FLOAT8`
	rowsWrittenExpr          = `(((statistics->'statistics':::STRING)->'rowsWritten':::STRING)->'mean':::STRING)::FLOAT8`
	bytesReadExpr            = `(((statistics->'statistics':::STRING)->'bytesRead':::STRING)->'mean':::STRING)::FLOAT8`
	bytesReadSqDiffExpr      = `(((statistics->'statistics':::STRING)->'bytesRead':::STRING)->'sqDiff':::STRING)::FLOAT8`
)

const addStatementStatisticsComputedColumns = `
ALTER TABLE system.statement_statistics
	ADD COLUMN IF NOT EXISTS exec_sample_count INT8 AS (` + execStatsExecCountExpr + `) STORED,
	ADD COLUMN IF NOT EXISTS svc_lat_sum FLOAT8 AS (` + serviceLatencyExpr + ` * ` + executionCountExpr + `::FLOAT8) STORED,
	ADD COLUMN IF NOT EXISTS cpu_sql_nanos_sum FLOAT8 AS (` + cpuSqlNanosExpr + ` * ` + execStatsExecCountExpr + `::FLOAT8) STORED,
	ADD COLUMN IF NOT EXISTS contention_time_sum FLOAT8 AS (` + contentionTimeExpr + ` * ` + execStatsExecCountExpr + `::FLOAT8) STORED,
	ADD COLUMN IF NOT EXISTS svc_lat_sum_sq FLOAT8 AS (` + svcLatSqDiffExpr + ` + ` + executionCountExpr + `::FLOAT8 * ` + serviceLatencyExpr + ` * ` + serviceLatencyExpr + `) STORED,
	ADD COLUMN IF NOT EXISTS cpu_sql_nanos_sum_sq FLOAT8 AS (` + cpuSqlNanosSqDiffExpr + ` + ` + execStatsExecCountExpr + `::FLOAT8 * ` + cpuSqlNanosExpr + ` * ` + cpuSqlNanosExpr + `) STORED,
	ADD COLUMN IF NOT EXISTS contention_time_sum_sq FLOAT8 AS (` + contentionTimeSqDiffExpr + ` + ` + execStatsExecCountExpr + `::FLOAT8 * ` + contentionTimeExpr + ` * ` + contentionTimeExpr + `) STORED,
	ADD COLUMN IF NOT EXISTS kv_cpu_time_nanos FLOAT8 AS (` + kvCpuTimeNanosExpr + `) STORED,
	ADD COLUMN IF NOT EXISTS kv_cpu_time_nanos_sum FLOAT8 AS (` + kvCpuTimeNanosExpr + ` * ` + executionCountExpr + `::FLOAT8) STORED,
	ADD COLUMN IF NOT EXISTS kv_cpu_time_nanos_sum_sq FLOAT8 AS (` + kvCpuTimeNanosSqDiffExpr + ` + ` + executionCountExpr + `::FLOAT8 * ` + kvCpuTimeNanosExpr + ` * ` + kvCpuTimeNanosExpr + `) STORED,
	ADD COLUMN IF NOT EXISTS admission_wait_time_sum FLOAT8 AS (` + admissionWaitTimeExpr + ` * ` + execStatsExecCountExpr + `::FLOAT8) STORED,
	ADD COLUMN IF NOT EXISTS admission_wait_time_sum_sq FLOAT8 AS (` + admissionWaitTimeSqDiff + ` + ` + execStatsExecCountExpr + `::FLOAT8 * ` + admissionWaitTimeExpr + ` * ` + admissionWaitTimeExpr + `) STORED,
	ADD COLUMN IF NOT EXISTS rows_read_sum FLOAT8 AS (` + rowsReadExpr + ` * ` + executionCountExpr + `::FLOAT8) STORED,
	ADD COLUMN IF NOT EXISTS rows_written_sum FLOAT8 AS (` + rowsWrittenExpr + ` * ` + executionCountExpr + `::FLOAT8) STORED,
	ADD COLUMN IF NOT EXISTS bytes_read_sum FLOAT8 AS (` + bytesReadExpr + ` * ` + executionCountExpr + `::FLOAT8) STORED,
	ADD COLUMN IF NOT EXISTS bytes_read_sum_sq FLOAT8 AS (` + bytesReadSqDiffExpr + ` + ` + executionCountExpr + `::FLOAT8 * ` + bytesReadExpr + ` * ` + bytesReadExpr + `) STORED
`

const addStatementStatisticsCoveringIndex = `
CREATE INDEX IF NOT EXISTS stmt_fp_ts_cov_counts
	ON system.statement_statistics (fingerprint_id, aggregated_ts DESC)
	STORING (execution_count, exec_sample_count, svc_lat_sum, cpu_sql_nanos_sum,
		contention_time_sum, kv_cpu_time_nanos_sum, svc_lat_sum_sq, cpu_sql_nanos_sum_sq,
		contention_time_sum_sq, kv_cpu_time_nanos_sum_sq, admission_wait_time_sum,
		admission_wait_time_sum_sq, rows_read_sum, rows_written_sum, bytes_read_sum,
		bytes_read_sum_sq)
	WHERE app_name NOT LIKE '$ internal%'
`

// statementStatisticsComputedColumnsMigration adds new computed columns to
// the system.statement_statistics table for variance/stddev calculations and
// creates a covering index to support efficient aggregation queries.
func statementStatisticsComputedColumnsMigration(
	ctx context.Context, version clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	// First, add the new computed columns.
	// We use columnExists instead of hasColumn because the SQL parser may
	// normalize computed expressions differently (adding parentheses), which
	// would cause hasColumn to fail even though the columns are semantically
	// identical.
	op := operation{
		name: "add-computed-columns-to-statement-statistics",
		schemaList: []string{
			"exec_sample_count", "svc_lat_sum", "cpu_sql_nanos_sum", "contention_time_sum",
			"svc_lat_sum_sq", "cpu_sql_nanos_sum_sq", "contention_time_sum_sq",
			"kv_cpu_time_nanos", "kv_cpu_time_nanos_sum", "kv_cpu_time_nanos_sum_sq",
			"admission_wait_time_sum", "admission_wait_time_sum_sq",
			"rows_read_sum", "rows_written_sum", "bytes_read_sum", "bytes_read_sum_sq",
		},
		query:          addStatementStatisticsComputedColumns,
		schemaExistsFn: columnExists,
	}
	if err := migrateTable(ctx, version, deps, op, keys.StatementStatisticsTableID,
		systemschema.StatementStatisticsTable); err != nil {
		return err
	}

	// Then, add the covering index.
	// We use indexExists instead of hasIndex because the SQL parser may
	// populate CompositeColumnIDs differently than the expected descriptor.
	op = operation{
		name:           "add-covering-index-to-statement-statistics",
		schemaList:     []string{"stmt_fp_ts_cov_counts"},
		query:          addStatementStatisticsCoveringIndex,
		schemaExistsFn: indexExists,
	}
	if err := migrateTable(ctx, version, deps, op, keys.StatementStatisticsTableID,
		systemschema.StatementStatisticsTable); err != nil {
		return err
	}

	return nil
}

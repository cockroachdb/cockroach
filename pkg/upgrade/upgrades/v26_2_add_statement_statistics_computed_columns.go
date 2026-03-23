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
	"github.com/cockroachdb/redact"
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
	maxRetriesExpr           = `((statistics->'statistics':::STRING)->'maxRetries':::STRING)::INT8`
	commitLatExpr            = `(((statistics->'statistics':::STRING)->'commitLat':::STRING)->'mean':::STRING)::FLOAT8`
	commitLatSqDiffExpr      = `(((statistics->'statistics':::STRING)->'commitLat':::STRING)->'sqDiff':::STRING)::FLOAT8`
)

// statement_statistics migration SQL statements.
const addStatementStatisticsComputedColumns = `
ALTER TABLE system.statement_statistics
	ADD COLUMN IF NOT EXISTS exec_sample_count INT8 AS (` + execStatsExecCountExpr + `) STORED FAMILY "primary",
	ADD COLUMN IF NOT EXISTS svc_lat_sum FLOAT8 AS (` + serviceLatencyExpr + ` * ` + executionCountExpr + `::FLOAT8) STORED FAMILY "primary",
	ADD COLUMN IF NOT EXISTS cpu_sql_nanos_sum FLOAT8 AS (` + cpuSqlNanosExpr + ` * ` + execStatsExecCountExpr + `::FLOAT8) STORED FAMILY "primary",
	ADD COLUMN IF NOT EXISTS contention_time_sum FLOAT8 AS (` + contentionTimeExpr + ` * ` + execStatsExecCountExpr + `::FLOAT8) STORED FAMILY "primary",
	ADD COLUMN IF NOT EXISTS svc_lat_sum_sq FLOAT8 AS (` + svcLatSqDiffExpr + ` + ` + executionCountExpr + `::FLOAT8 * ` + serviceLatencyExpr + ` * ` + serviceLatencyExpr + `) STORED FAMILY "primary",
	ADD COLUMN IF NOT EXISTS cpu_sql_nanos_sum_sq FLOAT8 AS (` + cpuSqlNanosSqDiffExpr + ` + ` + execStatsExecCountExpr + `::FLOAT8 * ` + cpuSqlNanosExpr + ` * ` + cpuSqlNanosExpr + `) STORED FAMILY "primary",
	ADD COLUMN IF NOT EXISTS contention_time_sum_sq FLOAT8 AS (` + contentionTimeSqDiffExpr + ` + ` + execStatsExecCountExpr + `::FLOAT8 * ` + contentionTimeExpr + ` * ` + contentionTimeExpr + `) STORED FAMILY "primary",
	ADD COLUMN IF NOT EXISTS kv_cpu_time_nanos_sum FLOAT8 AS (` + kvCpuTimeNanosExpr + ` * ` + executionCountExpr + `::FLOAT8) STORED FAMILY "primary",
	ADD COLUMN IF NOT EXISTS kv_cpu_time_nanos_sum_sq FLOAT8 AS (` + kvCpuTimeNanosSqDiffExpr + ` + ` + executionCountExpr + `::FLOAT8 * ` + kvCpuTimeNanosExpr + ` * ` + kvCpuTimeNanosExpr + `) STORED FAMILY "primary",
	ADD COLUMN IF NOT EXISTS admission_wait_time_sum FLOAT8 AS (` + admissionWaitTimeExpr + ` * ` + execStatsExecCountExpr + `::FLOAT8) STORED FAMILY "primary",
	ADD COLUMN IF NOT EXISTS admission_wait_time_sum_sq FLOAT8 AS (` + admissionWaitTimeSqDiff + ` + ` + execStatsExecCountExpr + `::FLOAT8 * ` + admissionWaitTimeExpr + ` * ` + admissionWaitTimeExpr + `) STORED FAMILY "primary",
	ADD COLUMN IF NOT EXISTS rows_read_sum FLOAT8 AS (` + rowsReadExpr + ` * ` + executionCountExpr + `::FLOAT8) STORED FAMILY "primary",
	ADD COLUMN IF NOT EXISTS rows_written_sum FLOAT8 AS (` + rowsWrittenExpr + ` * ` + executionCountExpr + `::FLOAT8) STORED FAMILY "primary",
	ADD COLUMN IF NOT EXISTS bytes_read_sum FLOAT8 AS (` + bytesReadExpr + ` * ` + executionCountExpr + `::FLOAT8) STORED FAMILY "primary",
	ADD COLUMN IF NOT EXISTS bytes_read_sum_sq FLOAT8 AS (` + bytesReadSqDiffExpr + ` + ` + executionCountExpr + `::FLOAT8 * ` + bytesReadExpr + ` * ` + bytesReadExpr + `) STORED FAMILY "primary",
	ADD COLUMN IF NOT EXISTS max_retries INT8 AS (` + maxRetriesExpr + `) STORED FAMILY "primary"
`

const addStatementStatisticsCoveringIndex = `
CREATE INDEX IF NOT EXISTS stmt_fp_ts_cov_counts
	ON system.statement_statistics (fingerprint_id, aggregated_ts DESC)
	STORING (execution_count, exec_sample_count, svc_lat_sum, cpu_sql_nanos_sum,
		contention_time_sum, kv_cpu_time_nanos_sum, svc_lat_sum_sq, cpu_sql_nanos_sum_sq,
		contention_time_sum_sq, kv_cpu_time_nanos_sum_sq, admission_wait_time_sum,
		admission_wait_time_sum_sq, rows_read_sum, rows_written_sum, bytes_read_sum,
		bytes_read_sum_sq, max_retries)
	WHERE app_name NOT LIKE '$ internal%'
`

const dropStmtExecutionCountIdx = `DROP INDEX IF EXISTS system.statement_statistics@execution_count_idx`
const dropStmtServiceLatencyIdx = `DROP INDEX IF EXISTS system.statement_statistics@service_latency_idx`
const dropStmtCpuSqlNanosIdx = `DROP INDEX IF EXISTS system.statement_statistics@cpu_sql_nanos_idx`
const dropStmtContentionTimeIdx = `DROP INDEX IF EXISTS system.statement_statistics@contention_time_idx`
const dropStmtTotalEstExecTimeIdx = `DROP INDEX IF EXISTS system.statement_statistics@total_estimated_execution_time_idx`
const dropStmtP99LatencyIdx = `DROP INDEX IF EXISTS system.statement_statistics@p99_latency_idx`

// transaction_statistics migration SQL statements.
const addTransactionStatisticsComputedColumns = `
ALTER TABLE system.transaction_statistics
	ADD COLUMN IF NOT EXISTS exec_sample_count INT8 AS (` + execStatsExecCountExpr + `) STORED FAMILY "primary",
	ADD COLUMN IF NOT EXISTS svc_lat_sum FLOAT8 AS (` + serviceLatencyExpr + ` * ` + executionCountExpr + `::FLOAT8) STORED FAMILY "primary",
	ADD COLUMN IF NOT EXISTS cpu_sql_nanos_sum FLOAT8 AS (` + cpuSqlNanosExpr + ` * ` + execStatsExecCountExpr + `::FLOAT8) STORED FAMILY "primary",
	ADD COLUMN IF NOT EXISTS contention_time_sum FLOAT8 AS (` + contentionTimeExpr + ` * ` + execStatsExecCountExpr + `::FLOAT8) STORED FAMILY "primary",
	ADD COLUMN IF NOT EXISTS svc_lat_sum_sq FLOAT8 AS (` + svcLatSqDiffExpr + ` + ` + executionCountExpr + `::FLOAT8 * ` + serviceLatencyExpr + ` * ` + serviceLatencyExpr + `) STORED FAMILY "primary",
	ADD COLUMN IF NOT EXISTS cpu_sql_nanos_sum_sq FLOAT8 AS (` + cpuSqlNanosSqDiffExpr + ` + ` + execStatsExecCountExpr + `::FLOAT8 * ` + cpuSqlNanosExpr + ` * ` + cpuSqlNanosExpr + `) STORED FAMILY "primary",
	ADD COLUMN IF NOT EXISTS contention_time_sum_sq FLOAT8 AS (` + contentionTimeSqDiffExpr + ` + ` + execStatsExecCountExpr + `::FLOAT8 * ` + contentionTimeExpr + ` * ` + contentionTimeExpr + `) STORED FAMILY "primary",
	ADD COLUMN IF NOT EXISTS kv_cpu_time_nanos_sum FLOAT8 AS (` + kvCpuTimeNanosExpr + ` * ` + executionCountExpr + `::FLOAT8) STORED FAMILY "primary",
	ADD COLUMN IF NOT EXISTS kv_cpu_time_nanos_sum_sq FLOAT8 AS (` + kvCpuTimeNanosSqDiffExpr + ` + ` + executionCountExpr + `::FLOAT8 * ` + kvCpuTimeNanosExpr + ` * ` + kvCpuTimeNanosExpr + `) STORED FAMILY "primary",
	ADD COLUMN IF NOT EXISTS admission_wait_time_sum FLOAT8 AS (` + admissionWaitTimeExpr + ` * ` + execStatsExecCountExpr + `::FLOAT8) STORED FAMILY "primary",
	ADD COLUMN IF NOT EXISTS admission_wait_time_sum_sq FLOAT8 AS (` + admissionWaitTimeSqDiff + ` + ` + execStatsExecCountExpr + `::FLOAT8 * ` + admissionWaitTimeExpr + ` * ` + admissionWaitTimeExpr + `) STORED FAMILY "primary",
	ADD COLUMN IF NOT EXISTS rows_read_sum FLOAT8 AS (` + rowsReadExpr + ` * ` + executionCountExpr + `::FLOAT8) STORED FAMILY "primary",
	ADD COLUMN IF NOT EXISTS rows_written_sum FLOAT8 AS (` + rowsWrittenExpr + ` * ` + executionCountExpr + `::FLOAT8) STORED FAMILY "primary",
	ADD COLUMN IF NOT EXISTS bytes_read_sum FLOAT8 AS (` + bytesReadExpr + ` * ` + executionCountExpr + `::FLOAT8) STORED FAMILY "primary",
	ADD COLUMN IF NOT EXISTS bytes_read_sum_sq FLOAT8 AS (` + bytesReadSqDiffExpr + ` + ` + executionCountExpr + `::FLOAT8 * ` + bytesReadExpr + ` * ` + bytesReadExpr + `) STORED FAMILY "primary",
	ADD COLUMN IF NOT EXISTS max_retries INT8 AS (` + maxRetriesExpr + `) STORED FAMILY "primary",
	ADD COLUMN IF NOT EXISTS commit_lat_sum FLOAT8 AS (` + commitLatExpr + ` * ` + executionCountExpr + `::FLOAT8) STORED FAMILY "primary",
	ADD COLUMN IF NOT EXISTS commit_lat_sum_sq FLOAT8 AS (` + commitLatSqDiffExpr + ` + ` + executionCountExpr + `::FLOAT8 * ` + commitLatExpr + ` * ` + commitLatExpr + `) STORED FAMILY "primary"
`

const addTransactionStatisticsCoveringIndex = `
CREATE INDEX IF NOT EXISTS txn_fp_ts_cov_counts
	ON system.transaction_statistics (fingerprint_id, aggregated_ts DESC)
	STORING (execution_count, exec_sample_count, svc_lat_sum, cpu_sql_nanos_sum,
		contention_time_sum, kv_cpu_time_nanos_sum, svc_lat_sum_sq, cpu_sql_nanos_sum_sq,
		contention_time_sum_sq, kv_cpu_time_nanos_sum_sq, admission_wait_time_sum,
		admission_wait_time_sum_sq, rows_read_sum, rows_written_sum, bytes_read_sum,
		bytes_read_sum_sq, max_retries, commit_lat_sum, commit_lat_sum_sq)
	WHERE app_name NOT LIKE '$ internal%'
`

const dropTxnExecutionCountIdx = `DROP INDEX IF EXISTS system.transaction_statistics@execution_count_idx`
const dropTxnServiceLatencyIdx = `DROP INDEX IF EXISTS system.transaction_statistics@service_latency_idx`
const dropTxnCpuSqlNanosIdx = `DROP INDEX IF EXISTS system.transaction_statistics@cpu_sql_nanos_idx`
const dropTxnContentionTimeIdx = `DROP INDEX IF EXISTS system.transaction_statistics@contention_time_idx`
const dropTxnTotalEstExecTimeIdx = `DROP INDEX IF EXISTS system.transaction_statistics@total_estimated_execution_time_idx`
const dropTxnP99LatencyIdx = `DROP INDEX IF EXISTS system.transaction_statistics@p99_latency_idx`

// statementStatisticsComputedColumnsMigration adds new computed columns to
// the system.statement_statistics table for variance/stddev calculations,
// creates a covering index to support efficient aggregation queries, and
// drops the old metric indexes.
func statementStatisticsComputedColumnsMigration(
	ctx context.Context, version clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	// Add the new computed columns.
	op := operation{
		name: "add-computed-columns-to-statement-statistics",
		schemaList: []string{
			"exec_sample_count", "svc_lat_sum", "cpu_sql_nanos_sum", "contention_time_sum",
			"svc_lat_sum_sq", "cpu_sql_nanos_sum_sq", "contention_time_sum_sq",
			"kv_cpu_time_nanos_sum", "kv_cpu_time_nanos_sum_sq",
			"admission_wait_time_sum", "admission_wait_time_sum_sq",
			"rows_read_sum", "rows_written_sum", "bytes_read_sum", "bytes_read_sum_sq",
			"max_retries",
		},
		query:          addStatementStatisticsComputedColumns,
		schemaExistsFn: columnExists,
	}
	if err := migrateTable(ctx, version, deps, op, keys.StatementStatisticsTableID,
		systemschema.StatementStatisticsTable); err != nil {
		return err
	}

	// Add the covering index.
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

	// Drop old metric indexes, one at a time (each must be a single statement).
	stmtOldIndexes := []struct {
		name  redact.RedactableString
		idx   string
		query string
	}{
		{"drop-execution-count-idx-from-stmt-stats", "execution_count_idx", dropStmtExecutionCountIdx},
		{"drop-service-latency-idx-from-stmt-stats", "service_latency_idx", dropStmtServiceLatencyIdx},
		{"drop-cpu-sql-nanos-idx-from-stmt-stats", "cpu_sql_nanos_idx", dropStmtCpuSqlNanosIdx},
		{"drop-contention-time-idx-from-stmt-stats", "contention_time_idx", dropStmtContentionTimeIdx},
		{"drop-total-est-exec-time-idx-from-stmt-stats", "total_estimated_execution_time_idx", dropStmtTotalEstExecTimeIdx},
		{"drop-p99-latency-idx-from-stmt-stats", "p99_latency_idx", dropStmtP99LatencyIdx},
	}
	for _, idx := range stmtOldIndexes {
		op = operation{
			name:           idx.name,
			schemaList:     []string{idx.idx},
			query:          idx.query,
			schemaExistsFn: doesNotHaveIndex,
		}
		if err := migrateTable(ctx, version, deps, op, keys.StatementStatisticsTableID,
			systemschema.StatementStatisticsTable); err != nil {
			return err
		}
	}

	return nil
}

// transactionStatisticsComputedColumnsMigration adds new computed columns to
// the system.transaction_statistics table for variance/stddev calculations,
// creates a covering index to support efficient aggregation queries, and
// drops the old metric indexes.
func transactionStatisticsComputedColumnsMigration(
	ctx context.Context, version clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	// Add the new computed columns.
	op := operation{
		name: "add-computed-columns-to-transaction-statistics",
		schemaList: []string{
			"exec_sample_count", "svc_lat_sum", "cpu_sql_nanos_sum", "contention_time_sum",
			"svc_lat_sum_sq", "cpu_sql_nanos_sum_sq", "contention_time_sum_sq",
			"kv_cpu_time_nanos_sum", "kv_cpu_time_nanos_sum_sq",
			"admission_wait_time_sum", "admission_wait_time_sum_sq",
			"rows_read_sum", "rows_written_sum", "bytes_read_sum", "bytes_read_sum_sq",
			"max_retries", "commit_lat_sum", "commit_lat_sum_sq",
		},
		query:          addTransactionStatisticsComputedColumns,
		schemaExistsFn: columnExists,
	}
	if err := migrateTable(ctx, version, deps, op, keys.TransactionStatisticsTableID,
		systemschema.TransactionStatisticsTable); err != nil {
		return err
	}

	// Add the covering index.
	op = operation{
		name:           "add-covering-index-to-transaction-statistics",
		schemaList:     []string{"txn_fp_ts_cov_counts"},
		query:          addTransactionStatisticsCoveringIndex,
		schemaExistsFn: indexExists,
	}
	if err := migrateTable(ctx, version, deps, op, keys.TransactionStatisticsTableID,
		systemschema.TransactionStatisticsTable); err != nil {
		return err
	}

	// Drop old metric indexes, one at a time (each must be a single statement).
	txnOldIndexes := []struct {
		name  redact.RedactableString
		idx   string
		query string
	}{
		{"drop-execution-count-idx-from-txn-stats", "execution_count_idx", dropTxnExecutionCountIdx},
		{"drop-service-latency-idx-from-txn-stats", "service_latency_idx", dropTxnServiceLatencyIdx},
		{"drop-cpu-sql-nanos-idx-from-txn-stats", "cpu_sql_nanos_idx", dropTxnCpuSqlNanosIdx},
		{"drop-contention-time-idx-from-txn-stats", "contention_time_idx", dropTxnContentionTimeIdx},
		{"drop-total-est-exec-time-idx-from-txn-stats", "total_estimated_execution_time_idx", dropTxnTotalEstExecTimeIdx},
		{"drop-p99-latency-idx-from-txn-stats", "p99_latency_idx", dropTxnP99LatencyIdx},
	}
	for _, idx := range txnOldIndexes {
		op = operation{
			name:           idx.name,
			schemaList:     []string{idx.idx},
			query:          idx.query,
			schemaExistsFn: doesNotHaveIndex,
		}
		if err := migrateTable(ctx, version, deps, op, keys.TransactionStatisticsTableID,
			systemschema.TransactionStatisticsTable); err != nil {
			return err
		}
	}

	return nil
}

// Copyright 2023 The Cockroach Authors.
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

const addExecutionCountComputedColStmtStats = `
ALTER TABLE system.statement_statistics
ADD COLUMN IF NOT EXISTS "execution_count" INT8 FAMILY "primary" 
AS ((statistics->'statistics'->'cnt')::INT8) STORED
`

const addIndexOnExecutionCountComputedColStmtStats = `
CREATE INDEX execution_count_idx ON system.statement_statistics (
aggregated_ts, app_name, execution_count DESC) WHERE app_name NOT LIKE 
'$ internal%'
`

const addServiceLatencyComputedColStmtStats = `
ALTER TABLE system.statement_statistics
ADD COLUMN IF NOT EXISTS "service_latency" FLOAT FAMILY "primary" 
AS ((statistics->'statistics'->'svcLat'->'mean')::FLOAT) STORED
`

const addIndexOnServiceLatencyComputedColStmtStats = `
CREATE INDEX service_latency_idx ON system.statement_statistics (
aggregated_ts, app_name, service_latency DESC) WHERE app_name NOT LIKE 
'$ internal%'
`

const addCpuSqlNanosComputedColStmtStats = `
ALTER TABLE system.statement_statistics
ADD COLUMN IF NOT EXISTS "cpu_sql_nanos" FLOAT FAMILY "primary" 
AS ((statistics->'execution_statistics'->'cpuSQLNanos'->'mean')::FLOAT) STORED
`

const addIndexOnCpuSqlNanosComputedColStmtStats = `
CREATE INDEX cpu_sql_nanos_idx ON system.statement_statistics (
aggregated_ts, app_name, cpu_sql_nanos DESC) WHERE app_name NOT LIKE 
'$ internal%'
`

const addContentionTimeComputedColStmtStats = `
ALTER TABLE system.statement_statistics
ADD COLUMN IF NOT EXISTS "contention_time" FLOAT FAMILY "primary" 
AS ((statistics->'execution_statistics'->'contentionTime'->'mean')::FLOAT) STORED
`

const addIndexOnContentionTimeComputedColStmtStats = `
CREATE INDEX contention_time_idx ON system.statement_statistics (
aggregated_ts, app_name, contention_time DESC) WHERE app_name NOT LIKE 
'$ internal%'
`

const addTotalEstimatedExecutionTimeComputedColStmtStats = `
ALTER TABLE system.statement_statistics
ADD COLUMN IF NOT EXISTS "total_estimated_execution_time" FLOAT FAMILY "primary" 
AS ((statistics->'statistics'->>'cnt')::FLOAT * (statistics->'statistics'->'svcLat'->>'mean')::FLOAT) STORED
`

const addIndexOnTotalEstimatedExecutionTimeComputedColStmtStats = `
CREATE INDEX total_estimated_execution_time_idx ON system.statement_statistics (
aggregated_ts, app_name, total_estimated_execution_time DESC) WHERE app_name NOT LIKE 
'$ internal%'
`

const addP99LatencyComputedColStmtStats = `
ALTER TABLE system.statement_statistics
ADD COLUMN IF NOT EXISTS "p99_latency" FLOAT FAMILY "primary" 
AS ((statistics->'statistics'->'latencyInfo'->'p99')::FLOAT) STORED
`

const addIndexOnP99LatencyComputedColStmtStats = `
CREATE INDEX p99_latency_idx ON system.statement_statistics (
aggregated_ts, app_name, p99_latency DESC) WHERE app_name NOT LIKE 
'$ internal%'
`

// transaction_statistics
const addExecutionCountComputedColTxnStats = `
ALTER TABLE system.transaction_statistics
ADD COLUMN IF NOT EXISTS "execution_count" INT8 FAMILY "primary"
AS ((statistics->'statistics'->'cnt')::INT8) STORED
`

const addIndexOnExecutionCountComputedColTxnStats = `
CREATE INDEX execution_count_idx ON system.transaction_statistics (
aggregated_ts, app_name, execution_count DESC) WHERE app_name NOT LIKE 
'$ internal%'
`

const addServiceLatencyComputedColTxnStats = `
ALTER TABLE system.transaction_statistics
ADD COLUMN IF NOT EXISTS "service_latency" FLOAT FAMILY "primary" 
AS ((statistics->'statistics'->'svcLat'->'mean')::FLOAT) STORED
`

const addIndexOnServiceLatencyComputedColTxnStats = `
CREATE INDEX service_latency_idx ON system.transaction_statistics (
aggregated_ts, app_name, service_latency DESC) WHERE app_name NOT LIKE 
'$ internal%'
`

const addCpuSqlNanosComputedColTxnStats = `
ALTER TABLE system.transaction_statistics
ADD COLUMN IF NOT EXISTS "cpu_sql_nanos" FLOAT FAMILY "primary" 
AS ((statistics->'execution_statistics'->'cpuSQLNanos'->'mean')::FLOAT) STORED
`

const addIndexOnCpuSqlNanosComputedColTxnStats = `
CREATE INDEX cpu_sql_nanos_idx ON system.transaction_statistics (
aggregated_ts, app_name, cpu_sql_nanos DESC) WHERE app_name NOT LIKE 
'$ internal%'
`

const addContentionTimeComputedColTxnStats = `
ALTER TABLE system.transaction_statistics
ADD COLUMN IF NOT EXISTS "contention_time" FLOAT FAMILY "primary" 
AS ((statistics->'execution_statistics'->'contentionTime'->'mean')::FLOAT) STORED
`

const addIndexOnContentionTimeComputedColTxnStats = `
CREATE INDEX contention_time_idx ON system.transaction_statistics (
aggregated_ts, app_name, contention_time DESC) WHERE app_name NOT LIKE 
'$ internal%'
`
const addTotalEstimatedExecutionTimeComputedColTxnStats = `
ALTER TABLE system.transaction_statistics
ADD COLUMN IF NOT EXISTS "total_estimated_execution_time" FLOAT FAMILY "primary" 
AS ((statistics->'statistics'->>'cnt')::FLOAT * (
statistics->'statistics'->'svcLat'->>'mean')::FLOAT) STORED
`

const addIndexOnTotalEstimatedExecutionTimeComputedColTxnStats = `
CREATE INDEX total_estimated_execution_time_idx ON system.transaction_statistics (
aggregated_ts, app_name, total_estimated_execution_time DESC) WHERE app_name NOT LIKE 
'$ internal%'
`

const addP99LatencyComputedColTxnStats = `
ALTER TABLE system.transaction_statistics
ADD COLUMN IF NOT EXISTS "p99_latency" FLOAT FAMILY "primary" 
AS ((statistics->'statistics'->'latencyInfo'->'p99')::FLOAT) STORED
`

const addIndexOnP99LatencyComputedColTxnStats = `
CREATE INDEX p99_latency_idx ON system.transaction_statistics (
aggregated_ts, app_name, p99_latency DESC) WHERE app_name NOT LIKE 
'$ internal%'
`

func createComputedIndexesOnSystemSQLStatistics(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	for _, op := range []operation{
		{
			name:           "create-execution-count-computed-col-stmt-stats",
			schemaList:     []string{"execution_count"},
			query:          addExecutionCountComputedColStmtStats,
			schemaExistsFn: hasColumn,
		},
		{
			name:           "create-execution-count-idx-stmt-stats",
			schemaList:     []string{"execution_count_idx"},
			query:          addIndexOnExecutionCountComputedColStmtStats,
			schemaExistsFn: hasIndex,
		},
		{
			name:           "create-service-latency-computed-col-stmt-stats",
			schemaList:     []string{"service_latency"},
			query:          addServiceLatencyComputedColStmtStats,
			schemaExistsFn: hasColumn,
		},
		{
			name:           "create-service-latency-idx-stmt-stats",
			schemaList:     []string{"service_latency_idx"},
			query:          addIndexOnServiceLatencyComputedColStmtStats,
			schemaExistsFn: hasIndex,
		},
		{
			name:           "create-cpu-sql-nanos-computed-col-stmt-stats",
			schemaList:     []string{"cpu_sql_nanos"},
			query:          addCpuSqlNanosComputedColStmtStats,
			schemaExistsFn: hasColumn,
		},
		{
			name:           "create-cpu-sql-nanos-idx-stmt-stats",
			schemaList:     []string{"cpu_sql_nanos_idx"},
			query:          addIndexOnCpuSqlNanosComputedColStmtStats,
			schemaExistsFn: hasIndex,
		},
		{
			name:           "create-contention-time-computed-col-stmt-stats",
			schemaList:     []string{"contention_time"},
			query:          addContentionTimeComputedColStmtStats,
			schemaExistsFn: hasColumn,
		},
		{
			name:           "create-contention-time-idx-stmt-stats",
			schemaList:     []string{"contention_time_idx"},
			query:          addIndexOnContentionTimeComputedColStmtStats,
			schemaExistsFn: hasIndex,
		},
		{
			name:           "create-total-estimated-execution-time-computed-col-stmt-stats",
			schemaList:     []string{"total_estimated_execution_time"},
			query:          addTotalEstimatedExecutionTimeComputedColStmtStats,
			schemaExistsFn: hasColumn,
		},
		{
			name:           "create-total-estimated-execution-time-idx-stmt-stats",
			schemaList:     []string{"total_estimated_execution_time_idx"},
			query:          addIndexOnTotalEstimatedExecutionTimeComputedColStmtStats,
			schemaExistsFn: hasIndex,
		},
		{
			name:           "create-p99-latency-computed-col-stmt-stats",
			schemaList:     []string{"p99_latency"},
			query:          addP99LatencyComputedColStmtStats,
			schemaExistsFn: hasColumn,
		},
		{
			name:           "create-p99-latency-idx-stmt-stats",
			schemaList:     []string{"p99_latency_idx"},
			query:          addIndexOnP99LatencyComputedColStmtStats,
			schemaExistsFn: hasIndex,
		},
	} {
		if err := migrateTable(ctx, cs, d, op, keys.StatementStatisticsTableID, systemschema.StatementStatisticsTable); err != nil {
			return err
		}
	}

	for _, op := range []operation{
		{
			name:           "create-execution-count-computed-col-txn-stats",
			schemaList:     []string{"execution_count"},
			query:          addExecutionCountComputedColTxnStats,
			schemaExistsFn: hasColumn,
		},
		{
			name:           "create-execution-count-idx-txn-stats",
			schemaList:     []string{"execution_count_idx"},
			query:          addIndexOnExecutionCountComputedColTxnStats,
			schemaExistsFn: hasIndex,
		},
		{
			name:           "create-service-latency-computed-col-txn-stats",
			schemaList:     []string{"service_latency"},
			query:          addServiceLatencyComputedColTxnStats,
			schemaExistsFn: hasColumn,
		},
		{
			name:           "create-service-latency-idx-txn-stats",
			schemaList:     []string{"service_latency_idx"},
			query:          addIndexOnServiceLatencyComputedColTxnStats,
			schemaExistsFn: hasIndex,
		},
		{
			name:           "create-cpu-sql-nanos-computed-col-txn-stats",
			schemaList:     []string{"cpu_sql_nanos"},
			query:          addCpuSqlNanosComputedColTxnStats,
			schemaExistsFn: hasColumn,
		},
		{
			name:           "create-cpu-sql-nanos-idx-txn-stats",
			schemaList:     []string{"cpu_sql_nanos_idx"},
			query:          addIndexOnCpuSqlNanosComputedColTxnStats,
			schemaExistsFn: hasIndex,
		},
		{
			name:           "create-contention-time-computed-col-txn-stats",
			schemaList:     []string{"contention_time"},
			query:          addContentionTimeComputedColTxnStats,
			schemaExistsFn: hasColumn,
		},
		{
			name:           "create-contention-time-idx-txn-stats",
			schemaList:     []string{"contention_time_idx"},
			query:          addIndexOnContentionTimeComputedColTxnStats,
			schemaExistsFn: hasIndex,
		},
		{
			name:           "create-total-estimated-execution-time-computed-col-txn-stats",
			schemaList:     []string{"total_estimated_execution_time"},
			query:          addTotalEstimatedExecutionTimeComputedColTxnStats,
			schemaExistsFn: hasColumn,
		},
		{
			name:           "create-total-estimated-execution-time-idx-txn-stats",
			schemaList:     []string{"total_estimated_execution_time_idx"},
			query:          addIndexOnTotalEstimatedExecutionTimeComputedColTxnStats,
			schemaExistsFn: hasIndex,
		},
		{
			name:           "create-p99-latency-computed-col-txn-stats",
			schemaList:     []string{"p99_latency"},
			query:          addP99LatencyComputedColTxnStats,
			schemaExistsFn: hasColumn,
		},
		{
			name:           "create-p99-latency-idx-txn-stats",
			schemaList:     []string{"p99_latency_idx"},
			query:          addIndexOnP99LatencyComputedColTxnStats,
			schemaExistsFn: hasIndex,
		},
	} {
		if err := migrateTable(ctx, cs, d, op, keys.TransactionStatisticsTableID,
			systemschema.TransactionStatisticsTable); err != nil {
			return err
		}
	}
	return nil
}

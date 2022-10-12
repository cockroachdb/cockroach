// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlstats

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

// StmtStatsEnable determines whether to collect per-statement statistics.
var StmtStatsEnable = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.metrics.statement_details.enabled", "collect per-statement query statistics", true,
).WithPublic()

// TxnStatsNumStmtFingerprintIDsToRecord limits the number of statementFingerprintIDs stored in
// transactions statistics for a single transaction. This defaults to 1000, and
// currently is non-configurable (hidden setting).
var TxnStatsNumStmtFingerprintIDsToRecord = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.metrics.transaction_details.max_statement_ids",
	"max number of statement fingerprint IDs to store for transaction statistics",
	1000,
	settings.PositiveInt,
)

// TxnStatsEnable determines whether to collect per-application transaction
// statistics.
var TxnStatsEnable = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.metrics.transaction_details.enabled", "collect per-application transaction statistics", true,
).WithPublic()

// StatsCollectionLatencyThreshold specifies the minimum amount of time
// consumed by a SQL statement before it is collected for statistics reporting.
var StatsCollectionLatencyThreshold = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"sql.metrics.statement_details.threshold",
	"minimum execution time to cause statement statistics to be collected. "+
		"If configured, no transaction stats are collected.",
	0,
).WithPublic()

// DumpStmtStatsToLogBeforeReset specifies whether we dump the statements
// statistics to logs before being reset.
var DumpStmtStatsToLogBeforeReset = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.metrics.statement_details.dump_to_logs",
	"dump collected statement statistics to node logs when periodically cleared",
	false,
).WithPublic()

// SampleLogicalPlans specifies whether we periodically sample the logical plan
// for each fingerprint.
var SampleLogicalPlans = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.metrics.statement_details.plan_collection.enabled",
	"periodically save a logical plan for each fingerprint",
	false,
).WithPublic()

// LogicalPlanCollectionPeriod specifies the interval between collections of
// logical plans for each fingerprint.
var LogicalPlanCollectionPeriod = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"sql.metrics.statement_details.plan_collection.period",
	"the time until a new logical plan is collected",
	5*time.Minute,
	settings.NonNegativeDuration,
).WithPublic()

// MaxMemSQLStatsStmtFingerprints specifies the maximum of unique statement
// fingerprints we store in memory.
var MaxMemSQLStatsStmtFingerprints = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.metrics.max_mem_stmt_fingerprints",
	"the maximum number of statement fingerprints stored in memory",
	100000,
).WithPublic()

// MaxMemSQLStatsTxnFingerprints specifies the maximum of unique transaction
// fingerprints we store in memory.
var MaxMemSQLStatsTxnFingerprints = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.metrics.max_mem_txn_fingerprints",
	"the maximum number of transaction fingerprints stored in memory",
	100000,
).WithPublic()

// MaxMemReportedSQLStatsStmtFingerprints specifies the maximum of unique statement
// fingerprints we store in memory.
var MaxMemReportedSQLStatsStmtFingerprints = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.metrics.max_mem_reported_stmt_fingerprints",
	"the maximum number of reported statement fingerprints stored in memory",
	100000,
).WithPublic()

// MaxMemReportedSQLStatsTxnFingerprints specifies the maximum of unique transaction
// fingerprints we store in memory.
var MaxMemReportedSQLStatsTxnFingerprints = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.metrics.max_mem_reported_txn_fingerprints",
	"the maximum number of reported transaction fingerprints stored in memory",
	100000,
).WithPublic()

// MaxSQLStatsStmtFingerprintsPerExplicitTxn specifies the maximum of unique statement
// fingerprints we store for each explicit transaction.
//
// This limit is introduced because when SQL Stats starts to record statement
// statistics for statements inside an explicit transaction, the transaction
// fingerprint ID is not known until the transaction is finished. SQL Stats
// holds those statement statistics in a temporary container until the explicit
// transaction finishes, then SQL Stats will upsert the statistics held in the
// temporary container into its in-memory store. However, the temporary
// container cannot inherit the node-level statement statistics fingerprint limit
// (that is: sql.metrics.max_mem_stmt_fingerprints). This is because if we count
// statement fingerprints inside the temporary container towards the total
// fingerprint count, we would be over-counting the statement fingerprint.
//
// For example: let's suppose we execute the following transaction:
// * BEGIN; SELECT 1; SELECT 1, 1; COMMIT;
// This results in 4 statement fingerprints and 1 txn fingerprint.
// Let's suppose currently our statement fingerprint limit is 6.
// If we are to execute the same statement again:
//   - BEGIN; <- this increments current statement fingerprint count to 5
//     since we hold statement stats for explicit transaction in a
//     temporary container before we can perform the upsert.
//   - SELECT 1; <- this increments the count to 6
//   - SELECT 1, 1; <- ERR: this causes the count to exceed our stmt fingerprint
//     limit before we can perform the upsert.
//
// The total amount of memory consumed will still be constrained by the
// top-level memory monitor created for SQL Stats.
var MaxSQLStatsStmtFingerprintsPerExplicitTxn = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.metrics.max_stmt_fingerprints_per_explicit_txn",
	"the maximum number of statement fingerprints stored per explicit transaction",
	2000,
)

// MaxSQLStatReset is the cluster setting that controls at what interval SQL
// statement statistics must be flushed within.
var MaxSQLStatReset = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"diagnostics.forced_sql_stat_reset.interval",
	"interval after which the reported SQL Stats are reset even "+
		"if not collected by telemetry reporter. It has a max value of 24H.",
	time.Hour*2,
	settings.NonNegativeDurationWithMaximum(time.Hour*24),
).WithPublic()

// SampleIndexRecommendation specifies whether we generate an index recommendation
// for each fingerprint ID.
var SampleIndexRecommendation = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.metrics.statement_details.index_recommendation_collection.enabled",
	"generate an index recommendation for each fingerprint ID",
	true,
).WithPublic()

// MaxMemReportedSampleIndexRecommendations specifies the maximum of unique index
// recommendations info we store in memory.
var MaxMemReportedSampleIndexRecommendations = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.metrics.statement_details.max_mem_reported_idx_recommendations",
	"the maximum number of reported index recommendation info stored in memory",
	5000,
).WithPublic()

// GatewayNodeEnabled specifies whether we save the gateway node id for each fingerprint
// during sql stats collection, otherwise the value will be set to 0.
var GatewayNodeEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.metrics.statement_details.gateway_node.enabled",
	"save the gateway node for each statement fingerprint. If false, the value will "+
		"be stored as 0.",
	true,
).WithPublic()

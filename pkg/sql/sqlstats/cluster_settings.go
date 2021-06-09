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
	"sql.metrics.statement_details.enabled", "collect per-statement query statistics", true,
).WithPublic()

// TxnStatsNumStmtFingerprintIDsToRecord limits the number of statementFingerprintIDs stored in
// transactions statistics for a single transaction. This defaults to 1000, and
// currently is non-configurable (hidden setting).
var TxnStatsNumStmtFingerprintIDsToRecord = settings.RegisterIntSetting(
	"sql.metrics.transaction_details.max_statement_ids",
	"max number of statement fingerprint IDs to store for transaction statistics",
	1000,
	settings.PositiveInt,
)

// TxnStatsEnable determines whether to collect per-application transaction
// statistics.
var TxnStatsEnable = settings.RegisterBoolSetting(
	"sql.metrics.transaction_details.enabled", "collect per-application transaction statistics", true,
).WithPublic()

// StatsCollectionLatencyThreshold specifies the minimum amount of time
// consumed by a SQL statement before it is collected for statistics reporting.
var StatsCollectionLatencyThreshold = settings.RegisterDurationSetting(
	"sql.metrics.statement_details.threshold",
	"minimum execution time to cause statement statistics to be collected. "+
		"If configured, no transaction stats are collected.",
	0,
).WithPublic()

// DumpStmtStatsToLogBeforeReset specifies whether we dump the statements
// statistics to logs before being reset.
var DumpStmtStatsToLogBeforeReset = settings.RegisterBoolSetting(
	"sql.metrics.statement_details.dump_to_logs",
	"dump collected statement statistics to node logs when periodically cleared",
	false,
).WithPublic()

// SampleLogicalPlans specifies whether we periodically sample the logical plan
// for each fingerprint.
var SampleLogicalPlans = settings.RegisterBoolSetting(
	"sql.metrics.statement_details.plan_collection.enabled",
	"periodically save a logical plan for each fingerprint",
	true,
).WithPublic()

// LogicalPlanCollectionPeriod specifies the interval between collections of
// logical plans for each fingerprint.
var LogicalPlanCollectionPeriod = settings.RegisterDurationSetting(
	"sql.metrics.statement_details.plan_collection.period",
	"the time until a new logical plan is collected",
	5*time.Minute,
	settings.NonNegativeDuration,
).WithPublic()

// MaxMemSQLStatsStmtFingerprints specifies the maximum of unique statement
// fingerprints we store in memory.
var MaxMemSQLStatsStmtFingerprints = settings.RegisterIntSetting(
	"sql.metrics.max_mem_stmt_fingerprints",
	"the maximum number of statement fingerprints stored in memory",
	100000,
).WithPublic()

// MaxMemSQLStatsTxnFingerprints specifies the maximum of unique transaction
// fingerprints we store in memory.
var MaxMemSQLStatsTxnFingerprints = settings.RegisterIntSetting(
	"sql.metrics.max_mem_txn_fingerprints",
	"the maximum number of transaction fingerprints stored in memory",
	100000,
).WithPublic()

// MaxMemReportedSQLStatsStmtFingerprints specifies the maximum of unique statement
// fingerprints we store in memory.
var MaxMemReportedSQLStatsStmtFingerprints = settings.RegisterIntSetting(
	"sql.metrics.max_mem_reported_stmt_fingerprints",
	"the maximum number of reported statement fingerprints stored in memory",
	100000,
).WithPublic()

// MaxMemReportedSQLStatsTxnFingerprints specifies the maximum of unique transaction
// fingerprints we store in memory.
var MaxMemReportedSQLStatsTxnFingerprints = settings.RegisterIntSetting(
	"sql.metrics.max_mem_reported_txn_fingerprints",
	"the maximum number of reported transaction fingerprints stored in memory",
	100000,
).WithPublic()

// SQLStatReset is the cluster setting that controls at what interval SQL
// statement statistics should be reset.
var SQLStatReset = settings.RegisterDurationSetting(
	"diagnostics.sql_stat_reset.interval",
	"interval controlling how often SQL statement statistics should "+
		"be reset (should be less than diagnostics.forced_sql_stat_reset.interval). It has a max value of 24H.",
	time.Hour,
	settings.NonNegativeDurationWithMaximum(time.Hour*24),
).WithPublic()

// MaxSQLStatReset is the cluster setting that controls at what interval SQL
// statement statistics must be flushed within.
var MaxSQLStatReset = settings.RegisterDurationSetting(
	"diagnostics.forced_sql_stat_reset.interval",
	"interval after which SQL statement statistics are refreshed even "+
		"if not collected (should be more than diagnostics.sql_stat_reset.interval). It has a max value of 24H.",
	time.Hour*2, // 2 x diagnostics.sql_stat_reset.interval
	settings.NonNegativeDurationWithMaximum(time.Hour*24),
).WithPublic()

// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlstats

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

// StmtStatsEnable determines whether to collect per-statement statistics.
// TODO(117690): Unify StmtStatsEnable and TxnStatsEnable into a single cluster setting.
var StmtStatsEnable = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.metrics.statement_details.enabled", "collect per-statement query statistics", true,
	settings.WithPublic)

// TxnStatsNumStmtFingerprintIDsToRecord limits the number of statementFingerprintIDs stored in
// transactions statistics for a single transaction. This defaults to 1000, and
// currently is non-configurable (hidden setting).
var TxnStatsNumStmtFingerprintIDsToRecord = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.metrics.transaction_details.max_statement_ids",
	"max number of statement fingerprint IDs to store for transaction statistics",
	1000,
	settings.PositiveInt,
)

// TxnStatsEnable determines whether to collect per-application transaction
// statistics.
// TODO(117690): Unify StmtStatsEnable and TxnStatsEnable into a single cluster setting.
var TxnStatsEnable = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.metrics.transaction_details.enabled", "collect per-application transaction statistics", true,
	settings.WithPublic)

// StatsCollectionLatencyThreshold specifies the minimum amount of time
// consumed by a SQL statement before it is collected for statistics reporting.
var StatsCollectionLatencyThreshold = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"sql.metrics.statement_details.threshold",
	"minimum execution time to cause statement statistics to be collected. "+
		"If configured, no transaction stats are collected.",
	0,
	settings.WithPublic)

// DumpStmtStatsToLogBeforeReset specifies whether we dump the statements
// statistics to logs before being reset.
var DumpStmtStatsToLogBeforeReset = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.metrics.statement_details.dump_to_logs",
	"dump collected statement statistics to node logs when periodically cleared",
	false,
	settings.WithName("sql.metrics.statement_details.dump_to_logs.enabled"),
	settings.WithPublic)

// MaxMemSQLStatsStmtFingerprints specifies the maximum of unique statement
// fingerprints we store in memory.
var MaxMemSQLStatsStmtFingerprints = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.metrics.max_mem_stmt_fingerprints",
	"the maximum number of statement fingerprints stored in memory",
	7500,
	settings.WithPublic)

// MaxMemSQLStatsTxnFingerprints specifies the maximum of unique transaction
// fingerprints we store in memory.
var MaxMemSQLStatsTxnFingerprints = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.metrics.max_mem_txn_fingerprints",
	"the maximum number of transaction fingerprints stored in memory",
	7500,
	settings.WithPublic)

// MaxMemReportedSQLStatsStmtFingerprints specifies the maximum of unique statement
// fingerprints we store in memory.
var MaxMemReportedSQLStatsStmtFingerprints = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.metrics.max_mem_reported_stmt_fingerprints",
	"the maximum number of reported statement fingerprints stored in memory",
	100000,
	settings.WithPublic)

// MaxMemReportedSQLStatsTxnFingerprints specifies the maximum of unique transaction
// fingerprints we store in memory.
var MaxMemReportedSQLStatsTxnFingerprints = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.metrics.max_mem_reported_txn_fingerprints",
	"the maximum number of reported transaction fingerprints stored in memory",
	100000,
	settings.WithPublic)

// MaxSQLStatReset is the cluster setting that controls at what interval SQL
// statement statistics must be flushed within.
var MaxSQLStatReset = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"diagnostics.forced_sql_stat_reset.interval",
	"interval after which the reported SQL Stats are reset even "+
		"if not collected by telemetry reporter. It has a max value of 24H.",
	time.Hour*2,
	settings.NonNegativeDurationWithMaximum(time.Hour*24),
	settings.WithPublic)

// SampleIndexRecommendation specifies whether we generate an index recommendation
// for each fingerprint ID.
var SampleIndexRecommendation = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.metrics.statement_details.index_recommendation_collection.enabled",
	"generate an index recommendation for each fingerprint ID",
	true,
	settings.WithPublic)

// MaxMemReportedSampleIndexRecommendations specifies the maximum of unique index
// recommendations info we store in memory.
var MaxMemReportedSampleIndexRecommendations = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.metrics.statement_details.max_mem_reported_idx_recommendations",
	"the maximum number of reported index recommendation info stored in memory",
	5000,
	settings.WithPublic)

// GatewayNodeEnabled specifies whether we save the gateway node id for each fingerprint
// during sql stats collection, otherwise the value will be set to 0.
var GatewayNodeEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.metrics.statement_details.gateway_node.enabled",
	"save the gateway node for each statement fingerprint. If false, the value will "+
		"be stored as 0.",
	false,
	settings.WithPublic)

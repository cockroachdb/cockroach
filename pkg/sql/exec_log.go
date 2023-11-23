// Copyright 2018 The Cockroach Authors.
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
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/redact"
)

// This file contains facilities to report SQL activities to separate
// log channels.
//
// See the detailed log sink and format documentation
// (e.g. auto-generated files in docs/generated) for details about the
// general format of log entries.
//
// By default, the facilities in this file produce query logs
// using structured events. The payload of structured events
// is also auto-documented; see the corresponding event definitions
// for details.

// logStatementsExecuteEnabled causes the Executor to log executed
// statements and, if any, resulting errors.
var logStatementsExecuteEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.trace.log_statement_execute",
	"set to true to enable logging of all executed statements",
	false,
	settings.WithName("sql.log.all_statements.enabled"),
	settings.WithPublic)

var slowQueryLogThreshold = settings.RegisterDurationSettingWithExplicitUnit(
	settings.ApplicationLevel,
	"sql.log.slow_query.latency_threshold",
	"when set to non-zero, log statements whose service latency exceeds "+
		"the threshold to a secondary logger on each node",
	0,
	settings.NonNegativeDuration,
	settings.WithPublic,
)

var slowInternalQueryLogEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.log.slow_query.internal_queries.enabled",
	"when set to true, internal queries which exceed the slow query log threshold "+
		"are logged to a separate log. Must have the slow query log enabled for this "+
		"setting to have any effect.",
	false,
	settings.WithPublic)

var slowQueryLogFullTableScans = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.log.slow_query.experimental_full_table_scans.enabled",
	"when set to true, statements that perform a full table/index scan will be logged to the "+
		"slow query log even if they do not meet the latency threshold. Must have the slow query "+
		"log enabled for this setting to have any effect.",
	false,
	settings.WithPublic)

var adminAuditLogEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.log.admin_audit.enabled",
	"when set, log SQL queries that are executed by a user with admin privileges",
	false,
)

var telemetryLoggingEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.telemetry.query_sampling.enabled",
	"when set to true, executed queries will emit an event on the telemetry logging channel",
	// Note: Usage of an env var here makes it possible to set a default without
	// the execution of a cluster setting SQL query. This is particularly advantageous
	// when cluster setting queries would be too inefficient or to slow to use. For
	// example, in multi-tenant setups in CC, it is impractical to enable this
	// setting directly after tenant creation without significant overhead in terms
	// of time and code.
	envutil.EnvOrDefaultBool("COCKROACH_SQL_TELEMETRY_QUERY_SAMPLING_ENABLED", false),
	settings.WithPublic)

type executorType int

const (
	executorTypeExec executorType = iota
	executorTypeInternal
)

// vLevel returns the vmodule log level at which logs from the given executor
// should be written to the logs.
func (s executorType) vLevel() log.Level { return log.Level(s) + 2 }

var logLabels = []string{"exec", "exec-internal"}

// logLabel returns the log label for the given executor type.
func (s executorType) logLabel() string { return logLabels[s] }

// maybeLogStatement conditionally records the current statement
// (p.curPlan) to the exec / audit logs.
func (p *planner) maybeLogStatement(
	ctx context.Context,
	execType executorType,
	numRetries, txnCounter, rows, stmtCount int,
	bulkJobId uint64,
	err error,
	queryReceived time.Time,
	hasAdminRoleCache *HasAdminRoleCache,
	telemetryLoggingMetrics *TelemetryLoggingMetrics,
	stmtFingerprintID appstatspb.StmtFingerprintID,
	queryStats *topLevelQueryStats,
	statsCollector sqlstats.StatsCollector,
) {
	p.maybeAuditRoleBasedAuditEvent(ctx, execType)
	p.maybeLogStatementInternal(ctx, execType, numRetries, txnCounter,
		rows, stmtCount, bulkJobId, err, queryReceived, hasAdminRoleCache,
		telemetryLoggingMetrics, stmtFingerprintID, queryStats, statsCollector,
	)
}

func (p *planner) maybeLogStatementInternal(
	ctx context.Context,
	execType executorType,
	numRetries, txnCounter, rows, stmtCount int,
	bulkJobId uint64,
	err error,
	startTime time.Time,
	hasAdminRoleCache *HasAdminRoleCache,
	telemetryMetrics *TelemetryLoggingMetrics,
	stmtFingerprintID appstatspb.StmtFingerprintID,
	topLevelQueryStats *topLevelQueryStats,
	statsCollector sqlstats.StatsCollector,
) {
	// Note: if you find the code below crashing because p.execCfg == nil,
	// do not add a test "if p.execCfg == nil { do nothing }" !
	// Instead, make the logger work. This is critical for auditing - we
	// can't miss any statement.
	logV := log.V(2)
	logExecuteEnabled := logStatementsExecuteEnabled.Get(&p.execCfg.Settings.SV)
	slowLogThreshold := slowQueryLogThreshold.Get(&p.execCfg.Settings.SV)
	slowLogFullTableScans := slowQueryLogFullTableScans.Get(&p.execCfg.Settings.SV)
	slowQueryLogEnabled := slowLogThreshold != 0
	slowInternalQueryLogEnabled := slowInternalQueryLogEnabled.Get(&p.execCfg.Settings.SV)
	auditEventsDetected := len(p.curPlan.auditEventBuilders) != 0
	logConsoleQuery := telemetryInternalConsoleQueriesEnabled.Get(&p.execCfg.Settings.SV) &&
		strings.HasPrefix(p.SessionData().ApplicationName, "$ internal-console")

	// We only consider non-internal SQL statements for telemetry logging unless
	// the telemetryInternalQueriesEnabled is true.
	telemetryLoggingEnabled := telemetryLoggingEnabled.Get(&p.execCfg.Settings.SV) &&
		(execType == executorTypeExec || telemetryInternalQueriesEnabled.Get(&p.execCfg.Settings.SV) || logConsoleQuery)

	// If hasAdminRoleCache IsSet is true iff AdminAuditLog is enabled.
	shouldLogToAdminAuditLog := hasAdminRoleCache.IsSet && hasAdminRoleCache.HasAdminRole

	// Only log to adminAuditLog if the statement is executed by
	// a user and the user has admin privilege (is directly or indirectly a
	// member of the admin role).

	if !logV && !logExecuteEnabled && !auditEventsDetected && !slowQueryLogEnabled &&
		!shouldLogToAdminAuditLog && !telemetryLoggingEnabled {
		// Shortcut: avoid the expense of computing anything log-related
		// if logging is not enabled by configuration.
		return
	}

	// Compute the pieces of data that are going to be included in logged events.

	// The duration of the query so far. Age is the duration expressed in milliseconds.
	queryDuration := timeutil.Since(startTime)
	age := float32(queryDuration.Nanoseconds()) / 1e6
	// The text of the error encountered, if the query did in fact end
	// in error.
	var execErrStr redact.RedactableString
	if err != nil {
		execErrStr = redact.Sprint(err)
	}
	// The type of execution context (execute/prepare).
	lbl := execType.logLabel()

	// New logging format in v21.1.
	sqlErrState := ""
	if err != nil {
		sqlErrState = pgerror.GetPGCode(err).String()
	}

	execDetails := eventpb.CommonSQLExecDetails{
		// Note: the current statement, application name, etc, are
		// automatically populated by the shared logic in event_log.go.
		ExecMode:      lbl,
		SQLSTATE:      sqlErrState,
		ErrorText:     execErrStr,
		Age:           age,
		NumRetries:    uint32(numRetries),
		FullTableScan: p.curPlan.flags.IsSet(planFlagContainsFullTableScan),
		FullIndexScan: p.curPlan.flags.IsSet(planFlagContainsFullIndexScan),
		TxnCounter:    uint32(txnCounter),
		StmtPosInTxn:  uint32(stmtCount),
	}

	// Note that for bulk job query (IMPORT, BACKUP and RESTORE), we don't
	// print out the number of changed rows along with the sampled query event.
	// We emit it when the job succeeds in a recovery_event.
	switch p.stmt.AST.(type) {
	case *tree.Import, *tree.Restore, *tree.Backup:
		execDetails.BulkJobId = bulkJobId
	default:
		execDetails.NumRows = int64(rows)
	}

	if auditEventsDetected {
		// TODO(knz): re-add the placeholders and age into the logging event.
		entries := make([]logpb.EventPayload, len(p.curPlan.auditEventBuilders))
		for idx, builder := range p.curPlan.auditEventBuilders {
			auditEvent := builder.BuildAuditEvent(ctx, p, eventpb.CommonSQLEventDetails{}, execDetails)
			entries[idx] = auditEvent
		}
		p.logEventsOnlyExternally(ctx, entries...)
	}

	if slowQueryLogEnabled && (
	// Did the user request pumping queries into the slow query log when
	// the logical plan has full scans?
	(slowLogFullTableScans && (execDetails.FullTableScan || execDetails.FullIndexScan)) ||
		// Is the query actually slow?
		queryDuration > slowLogThreshold) {
		switch {
		case execType == executorTypeExec:
			// Non-internal queries are always logged to the slow query log.
			p.logEventsOnlyExternally(ctx, &eventpb.SlowQuery{CommonSQLExecDetails: execDetails})

		case execType == executorTypeInternal && slowInternalQueryLogEnabled:
			// Internal queries that surpass the slow query log threshold should only
			// be logged to the slow-internal-only log if the cluster setting dictates.
			p.logEventsOnlyExternally(ctx, &eventpb.SlowQueryInternal{CommonSQLExecDetails: execDetails})
		}
	}

	if logExecuteEnabled || logV {
		// The API contract for logEventsWithOptions() is that it returns
		// no error when system.eventlog is not written to.
		_ = p.logEventsWithOptions(ctx,
			1, /* depth */
			eventLogOptions{
				// We pass LogToDevChannelIfVerbose because we have a log.V
				// request for this file, which means the operator wants to
				// see a copy of the execution on the DEV Channel.
				dst:               LogExternally | LogToDevChannelIfVerbose,
				verboseTraceLevel: execType.vLevel(),
			},
			&eventpb.QueryExecute{CommonSQLExecDetails: execDetails})
	}

	if shouldLogToAdminAuditLog {
		p.logEventsOnlyExternally(ctx, &eventpb.AdminQuery{CommonSQLExecDetails: execDetails})
	}

	if telemetryLoggingEnabled && !p.SessionData().TroubleshootingMode {
		// We only log to the telemetry channel if enough time has elapsed from
		// the last event emission.
		tracingEnabled := telemetryMetrics.isTracing(p.curPlan.instrumentation.Tracing())

		isStmtMode := telemetrySamplingMode.Get(&p.execCfg.Settings.SV) == telemetryModeStatement

		// Always sample if one of the scenarios is true:
		// - on 'statement' sampling and the current statement is not of type DML
		// - on 'transaction' sampling mode and the current statement is not of type DML and is not a COMMIT
		// - tracing is enabled for this statement
		// - this is a query emitted by our console (application_name starts with `$ internal-console`) and
		// the cluster setting to log console queries is enabled
		forceLog := (p.stmt.AST.StatementType() != tree.TypeDML &&
			(isStmtMode || p.stmt.AST.StatementTag() != "COMMIT")) ||
			tracingEnabled || logConsoleQuery

		var txnID string
		// p.txn can be nil for COPY.
		if p.txn != nil {
			txnID = p.txn.ID().String()
		}

		if telemetryMetrics.shouldEmitStatementLog(telemetryMetrics.timeNow(), txnID, forceLog, stmtCount) {
			var queryLevelStats execstats.QueryLevelStats
			if stats, ok := p.instrumentation.GetQueryLevelStats(); ok {
				queryLevelStats = *stats
			}

			queryLevelStats = telemetryMetrics.getQueryLevelStats(queryLevelStats)
			indexRecs := make([]string, 0, len(p.curPlan.instrumentation.indexRecs))
			for _, rec := range p.curPlan.instrumentation.indexRecs {
				indexRecs = append(indexRecs, rec.SQL)
			}

			phaseTimes := statsCollector.PhaseTimes()

			// Collect the statistics.
			idleLatRaw := phaseTimes.GetIdleLatency(statsCollector.PreviousPhaseTimes())
			idleLatNanos := idleLatRaw.Nanoseconds()
			runLatRaw := phaseTimes.GetRunLatency()
			runLatNanos := runLatRaw.Nanoseconds()
			parseLatNanos := phaseTimes.GetParsingLatency().Nanoseconds()
			planLatNanos := phaseTimes.GetPlanningLatency().Nanoseconds()
			// We want to exclude any overhead to reduce possible confusion.
			svcLatRaw := phaseTimes.GetServiceLatencyNoOverhead()
			svcLatNanos := svcLatRaw.Nanoseconds()

			// processing latency: contributing towards SQL results.
			processingLatNanos := parseLatNanos + planLatNanos + runLatNanos

			// overhead latency: txn/retry management, error checking, etc
			execOverheadNanos := svcLatNanos - processingLatNanos

			skippedQueries := telemetryMetrics.resetSkippedQueryCount()

			var sqlInstanceIDs []int32
			if len(queryLevelStats.SqlInstanceIds) > 0 {
				sqlInstanceIDs = make([]int32, 0, len(queryLevelStats.SqlInstanceIds))
				for sqlId := range queryLevelStats.SqlInstanceIds {
					sqlInstanceIDs = append(sqlInstanceIDs, int32(sqlId))
				}
				sort.Slice(sqlInstanceIDs, func(i, j int) bool {
					return sqlInstanceIDs[i] < sqlInstanceIDs[j]
				})
			}

			sampledQuery := eventpb.SampledQuery{
				CommonSQLExecDetails:                  execDetails,
				SkippedQueries:                        skippedQueries,
				CostEstimate:                          p.curPlan.instrumentation.costEstimate,
				Distribution:                          p.curPlan.instrumentation.distribution.String(),
				PlanGist:                              p.curPlan.instrumentation.planGist.String(),
				SessionID:                             p.extendedEvalCtx.SessionID.String(),
				Database:                              p.CurrentDatabase(),
				StatementID:                           p.stmt.QueryID.String(),
				TransactionID:                         txnID,
				StatementFingerprintID:                uint64(stmtFingerprintID),
				MaxFullScanRowsEstimate:               p.curPlan.instrumentation.maxFullScanRows,
				TotalScanRowsEstimate:                 p.curPlan.instrumentation.totalScanRows,
				OutputRowsEstimate:                    p.curPlan.instrumentation.outputRows,
				StatsAvailable:                        p.curPlan.instrumentation.statsAvailable,
				NanosSinceStatsCollected:              int64(p.curPlan.instrumentation.nanosSinceStatsCollected),
				BytesRead:                             topLevelQueryStats.bytesRead,
				RowsRead:                              topLevelQueryStats.rowsRead,
				RowsWritten:                           topLevelQueryStats.rowsWritten,
				InnerJoinCount:                        int64(p.curPlan.instrumentation.joinTypeCounts[descpb.InnerJoin]),
				LeftOuterJoinCount:                    int64(p.curPlan.instrumentation.joinTypeCounts[descpb.LeftOuterJoin]),
				FullOuterJoinCount:                    int64(p.curPlan.instrumentation.joinTypeCounts[descpb.FullOuterJoin]),
				SemiJoinCount:                         int64(p.curPlan.instrumentation.joinTypeCounts[descpb.LeftSemiJoin]),
				AntiJoinCount:                         int64(p.curPlan.instrumentation.joinTypeCounts[descpb.LeftAntiJoin]),
				IntersectAllJoinCount:                 int64(p.curPlan.instrumentation.joinTypeCounts[descpb.IntersectAllJoin]),
				ExceptAllJoinCount:                    int64(p.curPlan.instrumentation.joinTypeCounts[descpb.ExceptAllJoin]),
				HashJoinCount:                         int64(p.curPlan.instrumentation.joinAlgorithmCounts[exec.HashJoin]),
				CrossJoinCount:                        int64(p.curPlan.instrumentation.joinAlgorithmCounts[exec.CrossJoin]),
				IndexJoinCount:                        int64(p.curPlan.instrumentation.joinAlgorithmCounts[exec.IndexJoin]),
				LookupJoinCount:                       int64(p.curPlan.instrumentation.joinAlgorithmCounts[exec.LookupJoin]),
				MergeJoinCount:                        int64(p.curPlan.instrumentation.joinAlgorithmCounts[exec.MergeJoin]),
				InvertedJoinCount:                     int64(p.curPlan.instrumentation.joinAlgorithmCounts[exec.InvertedJoin]),
				ApplyJoinCount:                        int64(p.curPlan.instrumentation.joinAlgorithmCounts[exec.ApplyJoin]),
				ZigZagJoinCount:                       int64(p.curPlan.instrumentation.joinAlgorithmCounts[exec.ZigZagJoin]),
				ContentionNanos:                       queryLevelStats.ContentionTime.Nanoseconds(),
				Regions:                               queryLevelStats.Regions,
				SQLInstanceIDs:                        sqlInstanceIDs,
				NetworkBytesSent:                      queryLevelStats.NetworkBytesSent,
				MaxMemUsage:                           queryLevelStats.MaxMemUsage,
				MaxDiskUsage:                          queryLevelStats.MaxDiskUsage,
				KVBytesRead:                           queryLevelStats.KVBytesRead,
				KVPairsRead:                           queryLevelStats.KVPairsRead,
				KVRowsRead:                            queryLevelStats.KVRowsRead,
				KvTimeNanos:                           queryLevelStats.KVTime.Nanoseconds(),
				KvGrpcCalls:                           queryLevelStats.KVBatchRequestsIssued,
				NetworkMessages:                       queryLevelStats.NetworkMessages,
				CpuTimeNanos:                          queryLevelStats.CPUTime.Nanoseconds(),
				IndexRecommendations:                  indexRecs,
				Indexes:                               p.curPlan.instrumentation.indexesUsed,
				ScanCount:                             int64(p.curPlan.instrumentation.scanCounts[exec.ScanCount]),
				ScanWithStatsCount:                    int64(p.curPlan.instrumentation.scanCounts[exec.ScanWithStatsCount]),
				ScanWithStatsForecastCount:            int64(p.curPlan.instrumentation.scanCounts[exec.ScanWithStatsForecastCount]),
				TotalScanRowsWithoutForecastsEstimate: p.curPlan.instrumentation.totalScanRowsWithoutForecasts,
				NanosSinceStatsForecasted:             int64(p.curPlan.instrumentation.nanosSinceStatsForecasted),
				IdleLatencyNanos:                      idleLatNanos,
				ServiceLatencyNanos:                   svcLatNanos,
				RunLatencyNanos:                       runLatNanos,
				PlanLatencyNanos:                      planLatNanos,
				ParseLatencyNanos:                     parseLatNanos,
				OverheadLatencyNanos:                  execOverheadNanos,
				MvccBlockBytes:                        queryLevelStats.MvccBlockBytes,
				MvccBlockBytesInCache:                 queryLevelStats.MvccBlockBytesInCache,
				MvccKeyBytes:                          queryLevelStats.MvccKeyBytes,
				MvccPointCount:                        queryLevelStats.MvccPointCount,
				MvccPointsCoveredByRangeTombstones:    queryLevelStats.MvccPointsCoveredByRangeTombstones,
				MvccRangeKeyContainedPoints:           queryLevelStats.MvccRangeKeyContainedPoints,
				MvccRangeKeyCount:                     queryLevelStats.MvccRangeKeyCount,
				MvccRangeKeySkippedPoints:             queryLevelStats.MvccRangeKeySkippedPoints,
				MvccSeekCountInternal:                 queryLevelStats.MvccSeeksInternal,
				MvccSeekCount:                         queryLevelStats.MvccSeeks,
				MvccStepCountInternal:                 queryLevelStats.MvccStepsInternal,
				MvccStepCount:                         queryLevelStats.MvccSteps,
				MvccValueBytes:                        queryLevelStats.MvccValueBytes,
				SchemaChangerMode:                     p.curPlan.instrumentation.schemaChangerMode.String(),
			}

			p.logOperationalEventsOnlyExternally(ctx, &sampledQuery)
		} else {
			telemetryMetrics.incSkippedQueryCount()
		}
	}
}

func (p *planner) logEventsOnlyExternally(ctx context.Context, entries ...logpb.EventPayload) {
	// The API contract for logEventsWithOptions() is that it returns
	// no error when system.eventlog is not written to.
	_ = p.logEventsWithOptions(ctx,
		2, /* depth: we want to use the caller location */
		eventLogOptions{dst: LogExternally},
		entries...)
}

// logOperationalEventsOnlyExternally is a helper that sets redaction
// options to omit SQL Name redaction. This is used when logging to
// the telemetry channel when we want additional metadata available.
func (p *planner) logOperationalEventsOnlyExternally(
	ctx context.Context, entries ...logpb.EventPayload,
) {
	// The API contract for logEventsWithOptions() is that it returns
	// no error when system.eventlog is not written to.
	_ = p.logEventsWithOptions(ctx,
		2, /* depth: we want to use the caller location */
		eventLogOptions{dst: LogExternally, rOpts: redactionOptions{omitSQLNameRedaction: true}},
		entries...)
}

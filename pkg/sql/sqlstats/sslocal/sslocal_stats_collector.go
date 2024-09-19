// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sslocal

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionphase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/insights"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/ssmemstorage"
	"github.com/cockroachdb/redact"
)

// StatsCollector is used to collect statistics for transactions and
// statements for the entire lifetime of a session. It must be closed
// with Close() when the session is done.
type StatsCollector struct {

	// currentTransactionStatementStats contains the current transaction's statement
	// statistics. They will be flushed to flushTarget when the transaction is done
	// so that we can include the transaction fingerprint ID as part of the
	// statement's key. This container is local per stats collector and
	// is cleared for reuse after every transaction.
	currentTransactionStatementStats sqlstats.ApplicationStats

	// stmtFingerprintID is the fingerprint ID of the current statement we are
	// recording. Note that we don't observe sql stats for all statements (e.g. COMMIT).
	// If no stats have been attempted to be recorded yet for the current statement,
	// this value will be 0.
	stmtFingerprintID appstatspb.StmtFingerprintID

	// Allows StatsCollector to send statement and transaction stats to the
	// insights system. Set to nil to disable persistence of insights.
	insightsWriter *insights.ConcurrentBufferIngester

	// phaseTimes tracks session-level phase times.
	phaseTimes *sessionphase.Times

	// previousPhaseTimes tracks the session-level phase times for the previous
	// query. This enables the `SHOW LAST QUERY STATISTICS` observer statement.
	previousPhaseTimes *sessionphase.Times

	// sendInsights is true if we should send statement and transaction stats to
	// the insights system for the current transaction. This value is reset for
	// every new transaction.
	// It is important that if we send statement insights, we also send transaction
	// insights, as the transaction insight event will free the memory used by
	// statement insights.
	sendInsights bool

	// flushTarget is the sql stats container for the current application.
	// This is the target where the statement stats are flushed to upon
	// transaction completion. Note that these are the global stats for the
	// application.
	flushTarget sqlstats.ApplicationStats

	// uniqueServerCounts is a pointer to the statement and transaction
	// fingerprint counters tracked per server.
	uniqueServerCounts *ssmemstorage.SQLStatsAtomicCounters

	st    *cluster.Settings
	knobs *sqlstats.TestingKnobs
}

// NewStatsCollector returns an instance of StatsCollector.
func NewStatsCollector(
	st *cluster.Settings,
	appStats sqlstats.ApplicationStats,
	insights *insights.ConcurrentBufferIngester,
	phaseTime *sessionphase.Times,
	uniqueServerCounts *ssmemstorage.SQLStatsAtomicCounters,
	underOuterTxn bool,
	knobs *sqlstats.TestingKnobs,
) *StatsCollector {
	// See #124935 for more details. If underOuterTxn is true, the
	// executor owning the stats collector is not responsible for
	// starting or committing the transaction. Since the statements
	// are merged into flushTarget on EndTransaction, in this case the
	// container would never be merged into the flushTarget. Instead
	// we'll write directly to the flushTarget when we're collecting
	// stats for a conn exec belonging to an outer transaction.
	currentTransactionStatementStats := appStats
	if !underOuterTxn {
		currentTransactionStatementStats = appStats.NewApplicationStatsWithInheritedOptions()
	}
	return &StatsCollector{
		flushTarget:                      appStats,
		currentTransactionStatementStats: currentTransactionStatementStats,
		insightsWriter:                   insights,
		phaseTimes:                       phaseTime.Clone(),
		uniqueServerCounts:               uniqueServerCounts,
		st:                               st,
		knobs:                            knobs,
	}
}

func (s *StatsCollector) SetStatementFingerprintID(fingerprintID appstatspb.StmtFingerprintID) {
	s.stmtFingerprintID = fingerprintID
}

// StatementFingerprintID returns the fingerprint ID for the current statement.
func (s *StatsCollector) StatementFingerprintID() appstatspb.StmtFingerprintID {
	return s.stmtFingerprintID
}

// PhaseTimes returns the sessionphase.Times that this StatsCollector is
// currently tracking.
func (s *StatsCollector) PhaseTimes() *sessionphase.Times {
	return s.phaseTimes
}

// PreviousPhaseTimes returns the sessionphase.Times that this StatsCollector
// was previously tracking before being Reset.
func (s *StatsCollector) PreviousPhaseTimes() *sessionphase.Times {
	return s.previousPhaseTimes
}

// Reset resets the StatsCollector with a new flushTarget and a new copy
// of the sessionphase.Times.
func (s *StatsCollector) Reset(appStats sqlstats.ApplicationStats, phaseTime *sessionphase.Times) {
	previousPhaseTime := s.phaseTimes
	s.flushTarget = appStats

	s.previousPhaseTimes = previousPhaseTime
	s.phaseTimes = phaseTime.Clone()
	s.stmtFingerprintID = 0
}

// Close frees any local memory used by the stats collector and
// any memory allocated by underlying sql stats systems for the session
// that owns this stats collector.
func (s *StatsCollector) Close(ctx context.Context, sessionID clusterunique.ID) {
	// For stats collectors for executors with outer transactions,
	// the currentTransactionStatementStats is the flush target.
	// We should make sure we're never freeing the flush target,
	// since that container exists beyond the stats collector.
	if s.currentTransactionStatementStats != s.flushTarget {
		s.currentTransactionStatementStats.Free(ctx)
	}
	if s.insightsWriter != nil {
		s.insightsWriter.ClearSession(sessionID)
	}
}

// StartTransaction sets up the StatsCollector for a new transaction.
func (s *StatsCollector) StartTransaction() {
	s.sendInsights = s.shouldObserveInsights()
}

// EndTransaction informs the StatsCollector that the current txn has
// finished execution. (Either COMMITTED or ABORTED). This means the txn's
// fingerprint ID is now available. StatsCollector will now go back to update
// the transaction fingerprint ID field of all the statement statistics for that
// txn.
func (s *StatsCollector) EndTransaction(
	ctx context.Context, transactionFingerprintID appstatspb.TransactionFingerprintID,
) (discardedStats int64) {
	// We possibly ignore the transactionFingerprintID, for situations where
	// grouping by it would otherwise result in collecting higher-cardinality
	// data in the system tables than the cleanup job is able to keep up with.
	// See #78338.
	if !AssociateStmtWithTxnFingerprint.Get(&s.st.SV) {
		transactionFingerprintID = appstatspb.InvalidTransactionFingerprintID
	}

	discardedStats += int64(s.flushTarget.MergeApplicationStatementStats(
		ctx, s.currentTransactionStatementStats, transactionFingerprintID,
	))

	// Avoid taking locks if no stats are discarded.
	if discardedStats > 0 {
		s.flushTarget.MaybeLogDiscardMessage(ctx)
	}

	s.currentTransactionStatementStats.Clear(ctx)

	return discardedStats
}

// ShouldSample returns two booleans, the first one indicates whether we
// ever sampled (i.e. collected statistics for) the given combination of
// statement metadata, and the second one whether we should save the logical
// plan description for it.
func (s *StatsCollector) ShouldSample(
	fingerprint string, implicitTxn bool, database string,
) (previouslySampled bool, savePlanForStats bool) {

	return s.flushTarget.ShouldSample(fingerprint, implicitTxn, database)
}

// UpgradeImplicitTxn informs the StatsCollector that the current txn has been
// upgraded to an explicit transaction, thus all previously recorded statements
// should be updated accordingly.
func (s *StatsCollector) UpgradeImplicitTxn(ctx context.Context) error {
	err := s.currentTransactionStatementStats.IterateStatementStats(ctx, sqlstats.IteratorOptions{},
		func(_ context.Context, statistics *appstatspb.CollectedStatementStatistics) error {
			statistics.Key.ImplicitTxn = false
			return nil
		})

	return err
}

func getInsightStatus(statementError error) insights.Statement_Status {
	if statementError == nil {
		return insights.Statement_Completed
	}

	return insights.Statement_Failed
}

func (s *StatsCollector) shouldObserveInsights() bool {
	return sqlstats.StmtStatsEnable.Get(&s.st.SV) && sqlstats.TxnStatsEnable.Get(&s.st.SV)
}

// ObserveStatement sends the recorded statement stats to the insights system
// for further processing.
func (s *StatsCollector) ObserveStatement(
	stmtFingerprintID appstatspb.StmtFingerprintID, value sqlstats.RecordedStmtStats,
) {
	if !s.sendInsights {
		return
	}

	var autoRetryReason string
	if value.AutoRetryReason != nil {
		autoRetryReason = value.AutoRetryReason.Error()
	}

	var contention *time.Duration
	var cpuSQLNanos int64
	if value.ExecStats != nil {
		contention = &value.ExecStats.ContentionTime
		cpuSQLNanos = value.ExecStats.CPUTime.Nanoseconds()
	}

	var errorCode string
	var errorMsg redact.RedactableString
	if value.StatementError != nil {
		errorCode = pgerror.GetPGCode(value.StatementError).String()
		errorMsg = redact.Sprint(value.StatementError)
	}

	insight := insights.Statement{
		ID:                   value.StatementID,
		FingerprintID:        stmtFingerprintID,
		LatencyInSeconds:     value.ServiceLatencySec,
		Query:                value.Query,
		Status:               getInsightStatus(value.StatementError),
		StartTime:            value.StartTime,
		EndTime:              value.EndTime,
		FullScan:             value.FullScan,
		PlanGist:             value.PlanGist,
		Retries:              int64(value.AutoRetryCount),
		AutoRetryReason:      autoRetryReason,
		RowsRead:             value.RowsRead,
		RowsWritten:          value.RowsWritten,
		Nodes:                value.Nodes,
		KVNodeIDs:            value.KVNodeIDs,
		Contention:           contention,
		IndexRecommendations: value.IndexRecommendations,
		Database:             value.Database,
		CPUSQLNanos:          cpuSQLNanos,
		ErrorCode:            errorCode,
		ErrorMsg:             errorMsg,
	}
	if s.insightsWriter != nil {
		s.insightsWriter.ObserveStatement(value.SessionID, &insight)
	}
}

// ObserveTransaction sends the recorded transaction stats to the insights system
// for further processing.
func (s *StatsCollector) ObserveTransaction(
	ctx context.Context,
	txnFingerprintID appstatspb.TransactionFingerprintID,
	value sqlstats.RecordedTxnStats,
) {
	if !s.sendInsights {
		return
	}

	var retryReason string
	if value.AutoRetryReason != nil {
		retryReason = value.AutoRetryReason.Error()
	}

	var cpuSQLNanos int64
	if value.ExecStats.CPUTime.Nanoseconds() >= 0 {
		cpuSQLNanos = value.ExecStats.CPUTime.Nanoseconds()
	}

	var errorCode string
	var errorMsg redact.RedactableString
	if value.TxnErr != nil {
		errorCode = pgerror.GetPGCode(value.TxnErr).String()
		errorMsg = redact.Sprint(value.TxnErr)
	}

	status := insights.Transaction_Failed
	if value.Committed {
		status = insights.Transaction_Completed
	}

	insight := insights.Transaction{
		ID:              value.TransactionID,
		FingerprintID:   txnFingerprintID,
		UserPriority:    value.Priority.String(),
		ImplicitTxn:     value.ImplicitTxn,
		Contention:      &value.ExecStats.ContentionTime,
		StartTime:       value.StartTime,
		EndTime:         value.EndTime,
		User:            value.SessionData.User().Normalized(),
		ApplicationName: value.SessionData.ApplicationName,
		RowsRead:        value.RowsRead,
		RowsWritten:     value.RowsWritten,
		RetryCount:      value.RetryCount,
		AutoRetryReason: retryReason,
		CPUSQLNanos:     cpuSQLNanos,
		LastErrorCode:   errorCode,
		LastErrorMsg:    errorMsg,
		Status:          status,
	}
	if s.insightsWriter != nil {
		s.insightsWriter.ObserveTransaction(value.SessionID, &insight)
	}
}

// StatementsContainerFull returns true if the current statement
// container is at capacity.
func (s *StatsCollector) StatementsContainerFull() bool {
	return s.uniqueServerCounts.GetStatementCount() >= s.uniqueServerCounts.UniqueStmtFingerprintLimit.Get(&s.st.SV)
}

// RecordStatement records the statistics of a statement.
func (s *StatsCollector) RecordStatement(
	ctx context.Context, key appstatspb.StatementStatisticsKey, value sqlstats.RecordedStmtStats,
) (appstatspb.StmtFingerprintID, error) {
	return s.currentTransactionStatementStats.RecordStatement(ctx, key, value)
}

// RecordTransaction records the statistics of a transaction.
// Transaction stats are always recorded directly on the flushTarget.
func (s *StatsCollector) RecordTransaction(
	ctx context.Context, key appstatspb.TransactionFingerprintID, value sqlstats.RecordedTxnStats,
) error {
	return s.flushTarget.RecordTransaction(ctx, key, value)
}

func (s *StatsCollector) RecordStatementExecStats(
	key appstatspb.StatementStatisticsKey, stats execstats.QueryLevelStats,
) error {
	return s.currentTransactionStatementStats.RecordStatementExecStats(key, stats)
}

func (s *StatsCollector) IterateStatementStats(
	ctx context.Context, opts sqlstats.IteratorOptions, f sqlstats.StatementVisitor,
) error {
	return s.flushTarget.IterateStatementStats(ctx, opts, f)
}

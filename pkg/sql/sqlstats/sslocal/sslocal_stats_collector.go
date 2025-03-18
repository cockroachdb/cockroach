// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sslocal

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionphase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/insights"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/ssmemstorage"
)

type bufferedStmtStats []sqlstats.RecordedStmtStats

// StatsCollector is used to collect statistics for transactions and
// statements for the entire lifetime of a session. It must be closed
// with Close() when the session is done.
// It interfaces with 2 subsystems:
//
//  1. The in-memory sql stats subsystem (flushTarget) which is the
//     sql stats container for the current application. The collection
//     process is currently synchronous and uses the following steps:
//     - RecordStatement is called to either buffer the statement stats
//     for the current transaction, or write them directly to the flushTarget
//     if we belong to an "outer" transaction.
//     - EndTransaction is called to flush the buffered statement stats
//     and transaction to the flushTarget. This is where we also update
//     the transaction fingerprint ID of the buffered statements.
//
//  2. The insights subsystem (insightsWriter) which is used to
//     persist statement and transaction insights to an in-memory cache.
//     Events are sent to the insights subsystem for async processing.
type StatsCollector struct {

	// stmtBuf contains the current transaction's statement
	// statistics. They will be flushed to flushTarget when the transaction is done
	// so that we can include the transaction fingerprint ID as part of the
	// statement's key. This buffer is cleared for reuse after every transaction.
	stmtBuf bufferedStmtStats

	// If writeDirectlyToFlushTarget is set to true, the stmtBuf
	// will be written directly to the flushTarget instead of being buffered to be written
	// at the end of the transaction.
	// See #124935 for more details. When we have a statement from an outer txn,
	// the executor owning the stats collector is not responsible for
	// starting or committing the transaction. Since the statements
	// are merged into flushTarget on EndTransaction, in this case the
	// container would never be merged into the flushTarget. Instead
	// we'll write directly to the flushTarget when we're collecting
	// stats for a conn exec belonging to an outer transaction.
	writeDirectlyToFlushTarget bool

	// stmtFingerprintID is the fingerprint ID of the current statement we are
	// recording. Note that we don't observe sql stats for all statements (e.g. COMMIT).
	// If no stats have been attempted to be recorded yet for the current statement,
	// this value will be 0.
	stmtFingerprintID appstatspb.StmtFingerprintID

	// Allows StatsCollector to send statement and transaction stats to the
	// insights system. Set to nil to disable persistence of insights.
	insightsWriter *insights.ConcurrentBufferIngester

	// phaseTimes tracks session-level phase times.
	phaseTimes sessionphase.Times

	// previousPhaseTimes tracks the session-level phase times for the previous
	// query. This enables the `SHOW LAST QUERY STATISTICS` observer statement.
	previousPhaseTimes sessionphase.Times

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
	flushTarget *ssmemstorage.Container

	// uniqueServerCounts is a pointer to the statement and transaction
	// fingerprint counters tracked per server.
	uniqueServerCounts *ssmemstorage.SQLStatsAtomicCounters

	st    *cluster.Settings
	knobs *sqlstats.TestingKnobs
}

// NewStatsCollector returns an instance of StatsCollector.
func NewStatsCollector(
	st *cluster.Settings,
	appStats *ssmemstorage.Container,
	insights *insights.ConcurrentBufferIngester,
	phaseTime *sessionphase.Times,
	uniqueServerCounts *ssmemstorage.SQLStatsAtomicCounters,
	underOuterTxn bool,
	knobs *sqlstats.TestingKnobs,
) *StatsCollector {
	return &StatsCollector{
		flushTarget:                appStats,
		stmtBuf:                    make(bufferedStmtStats, 0, 1),
		writeDirectlyToFlushTarget: underOuterTxn,
		insightsWriter:             insights,
		phaseTimes:                 *phaseTime,
		uniqueServerCounts:         uniqueServerCounts,
		st:                         st,
		knobs:                      knobs,
	}
}

// StatementFingerprintID returns the fingerprint ID for the current statement.
func (s *StatsCollector) StatementFingerprintID() appstatspb.StmtFingerprintID {
	return s.stmtFingerprintID
}

// PhaseTimes returns the sessionphase.Times that this StatsCollector is
// currently tracking.
func (s *StatsCollector) PhaseTimes() *sessionphase.Times {
	return &s.phaseTimes
}

// PreviousPhaseTimes returns the sessionphase.Times that this StatsCollector
// was previously tracking before being Reset.
func (s *StatsCollector) PreviousPhaseTimes() *sessionphase.Times {
	return &s.previousPhaseTimes
}

// Reset resets the StatsCollector with a new flushTarget (the session's current
// application stats), and a new copy of the sessionphase.Times.
func (s *StatsCollector) Reset(appStats *ssmemstorage.Container, phaseTime *sessionphase.Times) {
	s.flushTarget = appStats
	s.stmtFingerprintID = 0
	s.previousPhaseTimes = s.phaseTimes
	s.phaseTimes = *phaseTime
}

// Close frees any local memory used by the stats collector and
// any memory allocated by underlying sql stats systems for the session
// that owns this stats collector.
func (s *StatsCollector) Close(_ctx context.Context, sessionID clusterunique.ID) {
	s.stmtBuf = nil
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

	for _, stmt := range s.stmtBuf {
		stmt.TransactionFingerprintID = transactionFingerprintID
		if err := s.flushTarget.RecordStatement(ctx, stmt); err != nil {
			discardedStats++
		}
	}

	// Avoid taking locks if no stats are discarded.
	if discardedStats > 0 {
		s.flushTarget.MaybeLogDiscardMessage(ctx)
	}

	s.stmtBuf = make(bufferedStmtStats, 0, len(s.stmtBuf)/2)

	return discardedStats
}

// ShouldSampleNewStatement returns true if the statement is a new statement
// and we should sample its execution statistics.
func (s *StatsCollector) ShouldSampleNewStatement(
	fingerprint string, implicitTxn bool, database string,
) bool {
	if s.uniqueServerCounts.GetStatementCount() >= s.uniqueServerCounts.UniqueStmtFingerprintLimit.Get(&s.st.SV) {
		// The container is full. Since we can't insert more statements
		// into the sql stats container, there's no point in sampling this
		// statement.
		return false
	}
	return s.flushTarget.TrySetStatementSampled(fingerprint, implicitTxn, database)
}

func (s *StatsCollector) SetStatementSampled(
	fingerprint string, implicitTxn bool, database string,
) {
	s.flushTarget.TrySetStatementSampled(fingerprint, implicitTxn, database)
}

func (s *StatsCollector) shouldObserveInsights() bool {
	return sqlstats.StmtStatsEnable.Get(&s.st.SV) && sqlstats.TxnStatsEnable.Get(&s.st.SV)
}

// RecordStatement records the statistics of a statement.
func (s *StatsCollector) RecordStatement(
	ctx context.Context, value sqlstats.RecordedStmtStats,
) error {
	if s.sendInsights && s.insightsWriter != nil {
		insight := insights.MakeStmtInsight(value)
		s.insightsWriter.ObserveStatement(value.SessionID, insight)
	}

	// TODO(xinhaoz): This isn't the best place to set this, but we'll clean this up
	// when we refactor the stats collection code to send the stats to an ingester.
	s.stmtFingerprintID = value.FingerprintID

	if s.writeDirectlyToFlushTarget {
		err := s.flushTarget.RecordStatement(ctx, value)
		return err
	}

	s.stmtBuf = append(s.stmtBuf, value)
	return nil
}

// RecordTransaction records the statistics of a transaction.
// Transaction stats are always recorded directly on the flushTarget.
func (s *StatsCollector) RecordTransaction(
	ctx context.Context, value sqlstats.RecordedTxnStats,
) error {
	if s.sendInsights && s.insightsWriter != nil {
		insight := insights.MakeTxnInsight(value)
		s.insightsWriter.ObserveTransaction(value.SessionID, insight)
	}

	// TODO(117690): Unify StmtStatsEnable and TxnStatsEnable into a single cluster setting.
	if !sqlstats.TxnStatsEnable.Get(&s.st.SV) {
		return nil
	}
	// Do not collect transaction statistics if the stats collection latency
	// threshold is set, since our transaction UI relies on having stats for every
	// statement in the transaction.
	t := sqlstats.StatsCollectionLatencyThreshold.Get(&s.st.SV)
	if t > 0 {
		return nil
	}
	return s.flushTarget.RecordTransaction(ctx, value)
}

// UpgradeToExplicitTransaction is called by the connExecutor when the current
// transaction is upgraded from an implicit transaction to explicit. Since this
// property is part of the statement fingerprint ID, we need to update the
// fingerprint ID of all the statements in the current transaction.
func (s *StatsCollector) UpgradeToExplicitTransaction() {
	for i := range s.stmtBuf {
		stmt := &s.stmtBuf[i]
		// Recalculate stmt fingerprint id.
		stmt.ImplicitTxn = false
		stmt.FingerprintID = appstatspb.ConstructStatementFingerprintID(stmt.Query, false /* implicit */, stmt.Database)
	}
}

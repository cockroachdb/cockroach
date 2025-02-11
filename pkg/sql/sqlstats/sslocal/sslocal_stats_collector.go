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

// StatsCollector is used to collect statistics for transactions and
// statements for the entire lifetime of a session. It must be closed
// with Close() when the session is done.
type StatsCollector struct {
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

	// uniqueServerCounts is a pointer to the statement and transaction
	// fingerprint counters tracked per server.
	uniqueServerCounts *ssmemstorage.SQLStatsAtomicCounters

	statsIngester *SQLConcurrentBufferIngester

	// flushTarget is the sql stats container for the current application.
	// This is the target where the statement stats are flushed to upon
	// transaction completion. Note that these are the global stats for the
	// application.
	appStats *ssmemstorage.Container

	st    *cluster.Settings
	knobs *sqlstats.TestingKnobs
}

// NewStatsCollector returns an instance of StatsCollector.
func NewStatsCollector(
	st *cluster.Settings,
	appStats *ssmemstorage.Container,
	insights *insights.ConcurrentBufferIngester,
	ingester *SQLConcurrentBufferIngester,
	phaseTime *sessionphase.Times,
	uniqueServerCounts *ssmemstorage.SQLStatsAtomicCounters,
	underOuterTxn bool,
	knobs *sqlstats.TestingKnobs,
) *StatsCollector {
	return &StatsCollector{
		phaseTimes:         phaseTime.Clone(),
		uniqueServerCounts: uniqueServerCounts,
		appStats:           appStats,
		statsIngester:      ingester,
		st:                 st,
		knobs:              knobs,
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
func (s *StatsCollector) Reset(appStats *ssmemstorage.Container, phaseTime *sessionphase.Times) {
	s.appStats = appStats
	previousPhaseTime := s.phaseTimes
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
	s.statsIngester.ClearSession(sessionID)
}

// StartTransaction sets up the StatsCollector for a new transaction.
func (s *StatsCollector) StartTransaction() {
	s.sendInsights = s.shouldObserveInsights()
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
	previouslySampled := s.appStats.StatementSampled(fingerprint, implicitTxn, database)
	if previouslySampled {
		return false
	}
	return s.appStats.TrySetStatementSampled(fingerprint, implicitTxn, database)
}

func (s *StatsCollector) SetStatementSampled(
	fingerprint string, implicitTxn bool, database string,
) {
	s.appStats.TrySetStatementSampled(fingerprint, implicitTxn, database)
}

func (s *StatsCollector) shouldObserveInsights() bool {
	return sqlstats.StmtStatsEnable.Get(&s.st.SV) && sqlstats.TxnStatsEnable.Get(&s.st.SV)
}

// StatementsContainerFull returns true if the current statement
// container is at capacity.
func (s *StatsCollector) StatementsContainerFull() bool {
	return s.uniqueServerCounts.GetStatementCount() >= s.uniqueServerCounts.UniqueStmtFingerprintLimit.Get(&s.st.SV)
}

// RecordStatement records the statistics of a statement.
func (s *StatsCollector) RecordStatement(
	ctx context.Context, value *sqlstats.RecordedStmtStats,
) (appstatspb.StmtFingerprintID, error) {
	s.statsIngester.IngestStatement(value)
	return 0, nil
}

// RecordTransaction records the statistics of a transaction.
// Transaction stats are always recorded directly on the flushTarget.
func (s *StatsCollector) RecordTransaction(
	ctx context.Context, key appstatspb.TransactionFingerprintID, value sqlstats.RecordedTxnStats,
) error {
	s.statsIngester.IngestTransaction(&value)
	return nil
}

func (s *StatsCollector) IterateStatementStats(
	ctx context.Context, opts sqlstats.IteratorOptions, f sqlstats.StatementVisitor,
) error {
	return nil
}

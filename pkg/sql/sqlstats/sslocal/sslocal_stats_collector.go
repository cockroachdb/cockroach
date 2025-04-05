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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/ssmemstorage"
)

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
	// stmtFingerprintID is the fingerprint ID of the current statement we are
	// recording. Note that we don't observe sql stats for all statements (e.g. COMMIT).
	// If no stats have been attempted to be recorded yet for the current statement,
	// this value will be 0.
	stmtFingerprintID appstatspb.StmtFingerprintID

	// phaseTimes tracks session-level phase times.
	phaseTimes sessionphase.Times

	// previousPhaseTimes tracks the session-level phase times for the previous
	// query. This enables the `SHOW LAST QUERY STATISTICS` observer statement.
	previousPhaseTimes sessionphase.Times

	// sendStats is true if we should send statement and transaction stats to
	// the insights system for the current transaction. This value is reset for
	// every new transaction.
	sendStats bool

	// flushTarget is the sql stats container for the current application.
	// This is the target where the statement stats are flushed to upon
	// transaction completion. Note that these are the global stats for the
	// application.
	flushTarget *ssmemstorage.Container

	// uniqueServerCounts is a pointer to the statement and transaction
	// fingerprint counters tracked per server.
	uniqueServerCounts *ssmemstorage.SQLStatsAtomicCounters

	statsIngester *SQLStatsIngester

	st    *cluster.Settings
	knobs *sqlstats.TestingKnobs
}

// NewStatsCollector returns an instance of StatsCollector.
func NewStatsCollector(
	st *cluster.Settings,
	appStats *ssmemstorage.Container,
	ingester *SQLStatsIngester,
	phaseTime *sessionphase.Times,
	uniqueServerCounts *ssmemstorage.SQLStatsAtomicCounters,
	knobs *sqlstats.TestingKnobs,
) *StatsCollector {
	return &StatsCollector{
		flushTarget:        appStats,
		phaseTimes:         *phaseTime,
		uniqueServerCounts: uniqueServerCounts,
		statsIngester:      ingester,
		st:                 st,
		knobs:              knobs,
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
	if s.statsIngester != nil {
		s.statsIngester.ClearSession(sessionID)
	}
}

// StartTransaction sets up the StatsCollector for a new transaction.
func (s *StatsCollector) StartTransaction() {
	s.sendStats = s.enabled()
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

func (s *StatsCollector) enabled() bool {
	return sqlstats.StmtStatsEnable.Get(&s.st.SV) && sqlstats.TxnStatsEnable.Get(&s.st.SV)
}

// RecordStatement records the statistics of a statement.
func (s *StatsCollector) RecordStatement(
	_ctx context.Context, value *sqlstats.RecordedStmtStats,
) error {
	if !s.sendStats {
		return nil
	}
	s.statsIngester.IngestStatement(value)
	return nil
}

// RecordTransaction records the statistics of a transaction.
// Transaction stats are always recorded directly on the flushTarget.
func (s *StatsCollector) RecordTransaction(
	_ctx context.Context, value *sqlstats.RecordedTxnStats,
) error {
	if !s.sendStats {
		return nil
	}

	s.statsIngester.IngestTransaction(value)
	return nil
}

func (s *StatsCollector) Enabled() bool {
	return s.sendStats
}

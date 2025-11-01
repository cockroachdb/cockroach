// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sslocal

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
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
//     - RecordStatement is called to to send the statement stats to the
//     stats ingester. The stats ingester will eventually record the
//     statement stats to the flushTarget.
//     - RecordTransaction is called to send the transaction stats to
//     the stats ingester. The stats ingester will eventually record the
//     transaction stats to the flushTarget.
//
//  2. The insights subsystem (insightsWriter) which is used to
//     persist statement and transaction insights to an in-memory cache.
//     Events are sent to the insights subsystem for async processing.
type StatsCollector struct {
	// phaseTimes tracks session-level phase times.
	phaseTimes sessionphase.Times

	// previousPhaseTimes tracks the session-level phase times for the previous
	// query. This enables the `SHOW LAST QUERY STATISTICS` observer statement.
	previousPhaseTimes sessionphase.Times

	// sendStats is true if we should send statement and transaction stats to
	// the sql stats ingester for the transaction. For sql stats, we decide to send
	// all or no execution events to the ingester as the ingester depends on transaction
	// events to clear any of the current session's previously buffered events.
	// This value is reset for every new transaction.
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
	s := &StatsCollector{
		flushTarget:        appStats,
		phaseTimes:         *phaseTime,
		uniqueServerCounts: uniqueServerCounts,
		statsIngester:      ingester,
		st:                 st,
		knobs:              knobs,
	}

	s.sendStats = s.enabled()

	return s
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
// TODO(alyshan): Session Phase Times are set throughout the conn executor, it is
// a tedious process to track when and where these times are set.
// Found a bug again? Consider refactoring.
func (s *StatsCollector) Reset(appStats *ssmemstorage.Container, phaseTime *sessionphase.Times) {
	s.flushTarget = appStats
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
func (s *StatsCollector) RecordStatement(_ctx context.Context, value *sqlstats.RecordedStmtStats) {
	s.statsIngester.RecordStatement(value)
}

// RecordTransaction sends the transaction statistics to the stats ingester.
func (s *StatsCollector) RecordTransaction(_ctx context.Context, value *sqlstats.RecordedTxnStats) {
	s.statsIngester.RecordTransaction(value)
}

func (s *StatsCollector) EnabledForTransaction() bool {
	return s.sendStats
}

// CurrentApplicationName returns the name of the current application
// that this StatsCollector is collecting information for. This method
// is used when creating structs containing the per execution stats for
// a statement or transaction. At that time, we can't read the app name
// that's on the connection executor since a `SET application_name` may
// have mutated the state already. For set application_name, the statement
// is still run under hte previous application name.
func (s *StatsCollector) CurrentApplicationName() string {
	return s.flushTarget.ApplicationName()
}

func (s *StatsCollector) RunLatency() time.Duration {
	return s.PhaseTimes().GetRunLatency()
}

func (s *StatsCollector) IdleLatency() time.Duration {
	return s.PhaseTimes().GetIdleLatency(s.PreviousPhaseTimes())
}

func (s *StatsCollector) ServiceLatency() time.Duration {
	return s.PhaseTimes().GetServiceLatencyNoOverhead()
}

func (s *StatsCollector) ParsingLatency() time.Duration {
	return s.PhaseTimes().GetParsingLatency()
}

func (s *StatsCollector) PlanningLatency() time.Duration {
	return s.PhaseTimes().GetPlanningLatency()
}

func (s *StatsCollector) ProcessingLatency() time.Duration {
	return s.ParsingLatency() + s.PlanningLatency() + s.RunLatency()
}

func (s *StatsCollector) ExecOverheadLatency() time.Duration {
	return s.ServiceLatency() - s.ProcessingLatency()
}

func (s *StatsCollector) StartTime() time.Time {
	return s.PhaseTimes().GetSessionPhaseTime(sessionphase.PlannerStartExecStmt).ToUTC()
}

func (s *StatsCollector) EndTime() time.Time {
	return s.StartTime().Add(s.ServiceLatency())
}

var _ sqlstats.StatementLatencyRecorder = &StatsCollector{}

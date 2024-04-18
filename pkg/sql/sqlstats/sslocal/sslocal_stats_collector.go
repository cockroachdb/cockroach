// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sslocal

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionphase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/insights"
)

// StatsCollector is used to collect statistics for transactions and
// statements for the entire lifetime of a session.
type StatsCollector struct {
	sqlstats.ApplicationStats

	// Allows StatsCollector to send statement and transaction stats to the insights system.
	insightsWriter insights.Writer

	// phaseTimes tracks session-level phase times.
	phaseTimes *sessionphase.Times

	// previousPhaseTimes tracks the session-level phase times for the previous
	// query. This enables the `SHOW LAST QUERY STATISTICS` observer statement.
	previousPhaseTimes *sessionphase.Times

	flushTarget sqlstats.ApplicationStats
	st          *cluster.Settings
	knobs       *sqlstats.TestingKnobs
}

var _ sqlstats.ApplicationStats = &StatsCollector{}

// NewStatsCollector returns an instance of StatsCollector.
func NewStatsCollector(
	st *cluster.Settings,
	appStats sqlstats.ApplicationStats,
	insights insights.Writer,
	phaseTime *sessionphase.Times,
	knobs *sqlstats.TestingKnobs,
) *StatsCollector {
	sc := &StatsCollector{
		ApplicationStats: appStats,
		insightsWriter:   insights,
		phaseTimes:       phaseTime.Clone(),
		st:               st,
		knobs:            knobs,
	}
	return sc
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

// Reset resets the StatsCollector with a new ApplicationStats and a new copy
// of the sessionphase.Times.
func (s *StatsCollector) Reset(appStats sqlstats.ApplicationStats, phaseTime *sessionphase.Times) {
	previousPhaseTime := s.phaseTimes
	s.flushTarget = appStats

	s.previousPhaseTimes = previousPhaseTime
	s.phaseTimes = phaseTime.Clone()
}

// StartTransaction sets up the StatsCollector for a new transaction.
// The current application stats are reset for the new transaction.
func (s *StatsCollector) StartTransaction() {
	s.flushTarget = s.ApplicationStats
	s.ApplicationStats = s.flushTarget.NewApplicationStatsWithInheritedOptions()
}

// EndTransaction informs the StatsCollector that the current txn has
// finished execution. (Either COMMITTED or ABORTED). This means the txn's
// fingerprint ID is now available. StatsCollector will now go back to update
// the transaction fingerprint ID field of all the statement statistics for that
// txn.
func (s *StatsCollector) EndTransaction(
	ctx context.Context, transactionFingerprintID appstatspb.TransactionFingerprintID,
) {
	// We possibly ignore the transactionFingerprintID, for situations where
	// grouping by it would otherwise result in collecting higher-cardinality
	// data in the system tables than the cleanup job is able to keep up with.
	// See #78338.
	if !AssociateStmtWithTxnFingerprint.Get(&s.st.SV) {
		transactionFingerprintID = appstatspb.InvalidTransactionFingerprintID
	}

	var discardedStats uint64
	discardedStats += s.flushTarget.MergeApplicationStatementStats(
		ctx, s.ApplicationStats, transactionFingerprintID,
	)

	discardedStats += s.flushTarget.MergeApplicationTransactionStats(
		ctx,
		s.ApplicationStats,
	)

	// Avoid taking locks if no stats are discarded.
	if discardedStats > 0 {
		s.flushTarget.MaybeLogDiscardMessage(ctx)
	}

	s.ApplicationStats.Free(ctx)
	s.ApplicationStats = s.flushTarget
	s.flushTarget = nil
}

// ShouldSample returns two booleans, the first one indicates whether we
// ever sampled (i.e. collected statistics for) the given combination of
// statement metadata, and the second one whether we should save the logical
// plan description for it.
func (s *StatsCollector) ShouldSample(
	fingerprint string, implicitTxn bool, database string,
) (previouslySampled bool, savePlanForStats bool) {
	sampledInFlushTarget := false
	savePlanForStatsInFlushTarget := true

	if s.flushTarget != nil {
		sampledInFlushTarget, savePlanForStatsInFlushTarget = s.flushTarget.ShouldSample(fingerprint, implicitTxn, database)
	}

	sampledInAppStats, savePlanForStatsInAppStats := s.ApplicationStats.ShouldSample(fingerprint, implicitTxn, database)
	previouslySampled = sampledInFlushTarget || sampledInAppStats
	savePlanForStats = savePlanForStatsInFlushTarget && savePlanForStatsInAppStats
	return previouslySampled, savePlanForStats
}

// UpgradeImplicitTxn informs the StatsCollector that the current txn has been
// upgraded to an explicit transaction, thus all previously recorded statements
// should be updated accordingly.
func (s *StatsCollector) UpgradeImplicitTxn(ctx context.Context) error {
	err := s.ApplicationStats.IterateStatementStats(ctx, sqlstats.IteratorOptions{},
		func(_ context.Context, statistics *appstatspb.CollectedStatementStatistics) error {
			statistics.Key.ImplicitTxn = false
			return nil
		})

	return err
}

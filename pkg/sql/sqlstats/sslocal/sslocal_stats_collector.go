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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionphase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// StatsCollector is used to collect statement and transaction statistics
// from connExecutor.
type StatsCollector struct {
	sqlstats.ApplicationStats

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

// NewStatsCollector returns an instance of sqlstats.StatsCollector.
func NewStatsCollector(
	st *cluster.Settings,
	appStats sqlstats.ApplicationStats,
	phaseTime *sessionphase.Times,
	knobs *sqlstats.TestingKnobs,
) *StatsCollector {
	return &StatsCollector{
		ApplicationStats: appStats,
		phaseTimes:       phaseTime.Clone(),
		st:               st,
		knobs:            knobs,
	}
}

// PhaseTimes implements sqlstats.StatsCollector interface.
func (s *StatsCollector) PhaseTimes() *sessionphase.Times {
	return s.phaseTimes
}

// PreviousPhaseTimes implements sqlstats.StatsCollector interface.
func (s *StatsCollector) PreviousPhaseTimes() *sessionphase.Times {
	return s.previousPhaseTimes
}

// Reset implements sqlstats.StatsCollector interface.
func (s *StatsCollector) Reset(appStats sqlstats.ApplicationStats, phaseTime *sessionphase.Times) {
	previousPhaseTime := s.phaseTimes
	if s.isInExplicitTransaction() {
		s.flushTarget = appStats
	} else {
		s.ApplicationStats = appStats
	}

	s.previousPhaseTimes = previousPhaseTime
	s.phaseTimes = phaseTime.Clone()
}

// StartExplicitTransaction implements sqlstats.StatsCollector interface.
func (s *StatsCollector) StartExplicitTransaction() {
	s.flushTarget = s.ApplicationStats
	s.ApplicationStats = s.flushTarget.NewApplicationStatsWithInheritedOptions()
}

// EndExplicitTransaction implements sqlstats.StatsCollector interface.
func (s *StatsCollector) EndExplicitTransaction(
	ctx context.Context, transactionFingerprintID roachpb.TransactionFingerprintID,
) {
	var discardedStats uint64
	discardedStats += s.flushTarget.MergeApplicationStatementStats(
		ctx,
		s.ApplicationStats,
		func(statistics *roachpb.CollectedStatementStatistics,
		) {
			statistics.Key.TransactionFingerprintID = transactionFingerprintID
		},
	)

	discardedStats += s.flushTarget.MergeApplicationTransactionStats(
		ctx,
		s.ApplicationStats,
	)

	if discardedStats > 0 {
		log.Warningf(ctx, "%d statement statistics discarded due to memory limit", discardedStats)
	}

	s.ApplicationStats.Free(ctx)
	s.ApplicationStats = s.flushTarget
	s.flushTarget = nil
}

// ShouldSaveLogicalPlanDesc implements sqlstats.StatsCollector interface.
func (s *StatsCollector) ShouldSaveLogicalPlanDesc(
	fingerprint string, implicitTxn bool, database string,
) bool {
	if s.isInExplicitTransaction() {
		return s.flushTarget.ShouldSaveLogicalPlanDesc(fingerprint, implicitTxn, database) &&
			s.ApplicationStats.ShouldSaveLogicalPlanDesc(fingerprint, implicitTxn, database)
	}

	return s.ApplicationStats.ShouldSaveLogicalPlanDesc(fingerprint, implicitTxn, database)
}

func (s *StatsCollector) isInExplicitTransaction() bool {
	return s.flushTarget != nil
}

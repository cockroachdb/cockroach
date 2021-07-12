// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package persistedsqlstats

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/ssmemstorage"
	"github.com/cockroachdb/errors"
)

// StatsWriter is a sqlstats.Writer that wraps a in-memory node-local stats
// writer. StatsWriter signals the subsystem when it encounters memory pressure
// which will triggers the flush operation.
type StatsWriter struct {
	// local in-memory storage.
	memWriter sqlstats.Writer

	// Use to signal the stats writer is experiencing memory pressure.
	memoryPressureSignal chan struct{}
}

var _ sqlstats.Writer = &StatsWriter{}

// RecordStatement implements sqlstats.Writer interface.
func (s *StatsWriter) RecordStatement(
	ctx context.Context, key roachpb.StatementStatisticsKey, value sqlstats.RecordedStmtStats,
) (roachpb.StmtFingerprintID, error) {
	stmtFingerprintID, err := s.memWriter.RecordStatement(ctx, key, value)
	if errors.Is(err, ssmemstorage.ErrFingerprintLimitReached) || errors.Is(err, ssmemstorage.ErrMemoryPressure) {
		select {
		case s.memoryPressureSignal <- struct{}{}:
			// If we successfully signaled that we are experiencing memory pressure,
			// then our job is done. However, if we fail to send the signal, that
			// means we are already experiencing memory pressure and the
			// stats-flush-worker has already started to handle the flushing. We
			// don't need to do anything here at this point. The default case of the
			// select allows this operation to be non-blocking.
		default:
		}
	}
	return stmtFingerprintID, err
}

// RecordStatementExecStats implements sqlstats.Writer interface.
func (s *StatsWriter) RecordStatementExecStats(
	key roachpb.StatementStatisticsKey, stats execstats.QueryLevelStats,
) error {
	return s.memWriter.RecordStatementExecStats(key, stats)
}

// ShouldSaveLogicalPlanDesc implements sqlstats.Writer interface.
func (s *StatsWriter) ShouldSaveLogicalPlanDesc(
	fingerprint string, implicitTxn bool, database string,
) bool {
	return s.memWriter.ShouldSaveLogicalPlanDesc(fingerprint, implicitTxn, database)
}

// RecordTransaction implements sqlstats.Writer interface and saves
// per-transaction statistics.
func (s *StatsWriter) RecordTransaction(
	ctx context.Context, key roachpb.TransactionFingerprintID, value sqlstats.RecordedTxnStats,
) error {
	err := s.memWriter.RecordTransaction(ctx, key, value)
	if errors.Is(err, ssmemstorage.ErrFingerprintLimitReached) || errors.Is(err, ssmemstorage.ErrMemoryPressure) {
		select {
		case s.memoryPressureSignal <- struct{}{}:
		default:
		}
	}
	return err
}

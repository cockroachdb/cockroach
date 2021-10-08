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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/ssmemstorage"
	"github.com/cockroachdb/errors"
)

// ApplicationStats is a sqlstats.ApplicationStats that wraps an in-memory
// node-local ApplicationStats. ApplicationStats signals the subsystem when it
// encounters memory pressure which will triggers the flush operation.
type ApplicationStats struct {
	// local in-memory storage.
	sqlstats.ApplicationStats

	// Use to signal the stats writer is experiencing memory pressure.
	memoryPressureSignal chan struct{}
}

var _ sqlstats.ApplicationStats = &ApplicationStats{}

// RecordStatement implements sqlstats.ApplicationStats interface.
func (s *ApplicationStats) RecordStatement(
	ctx context.Context, key roachpb.StatementStatisticsKey, value sqlstats.RecordedStmtStats,
) (roachpb.StmtFingerprintID, error) {
	var fingerprintID roachpb.StmtFingerprintID
	err := s.recordStatsOrSendMemoryPressureSignal(func() (err error) {
		fingerprintID, err = s.ApplicationStats.RecordStatement(ctx, key, value)
		return err
	})
	return fingerprintID, err
}

// ShouldSaveLogicalPlanDesc implements sqlstats.ApplicationStats interface.
func (s *ApplicationStats) ShouldSaveLogicalPlanDesc(
	fingerprint string, implicitTxn bool, database string,
) bool {
	return s.ApplicationStats.ShouldSaveLogicalPlanDesc(fingerprint, implicitTxn, database)
}

// RecordTransaction implements sqlstats.ApplicationStats interface and saves
// per-transaction statistics.
func (s *ApplicationStats) RecordTransaction(
	ctx context.Context, key roachpb.TransactionFingerprintID, value sqlstats.RecordedTxnStats,
) error {
	return s.recordStatsOrSendMemoryPressureSignal(func() error {
		return s.ApplicationStats.RecordTransaction(ctx, key, value)
	})
}

func (s *ApplicationStats) recordStatsOrSendMemoryPressureSignal(fn func() error) error {
	err := fn()
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
		// We have already handled the memory pressure error. We don't have to
		// bubble up the error any further.
		return nil
	}
	return err
}

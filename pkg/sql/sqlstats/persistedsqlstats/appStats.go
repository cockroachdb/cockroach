// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package persistedsqlstats

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
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
	ctx context.Context, key appstatspb.StatementStatisticsKey, value sqlstats.RecordedStmtStats,
) (appstatspb.StmtFingerprintID, error) {
	var fingerprintID appstatspb.StmtFingerprintID
	err := s.recordStatsOrSendMemoryPressureSignal(func() (err error) {
		fingerprintID, err = s.ApplicationStats.RecordStatement(ctx, key, value)
		return err
	})
	return fingerprintID, err
}

// ShouldSample implements sqlstats.ApplicationStats interface.
func (s *ApplicationStats) ShouldSample(
	fingerprint string, implicitTxn bool, database string,
) (bool, bool) {
	return s.ApplicationStats.ShouldSample(fingerprint, implicitTxn, database)
}

// RecordTransaction implements sqlstats.ApplicationStats interface and saves
// per-transaction statistics.
func (s *ApplicationStats) RecordTransaction(
	ctx context.Context, key appstatspb.TransactionFingerprintID, value sqlstats.RecordedTxnStats,
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

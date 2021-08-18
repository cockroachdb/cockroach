// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//
// persistedsqlstats is a subsystem that is responsible for flushing node-local
// in-memory stats into persisted system tables.

package persistedsqlstats

import (
	"context"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/sslocal"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Config is a configuration struct for the persisted SQL stats subsystem.
type Config struct {
	Settings         *cluster.Settings
	InternalExecutor sqlutil.InternalExecutor
	KvDB             *kv.DB
	SQLIDContainer   *base.SQLIDContainer
	Knobs            *TestingKnobs
	FlushCounter     *metric.Counter
	FlushDuration    *metric.Histogram
	FailureCounter   *metric.Counter
}

// PersistedSQLStats is a sqlstats.Provider that wraps a node-local in-memory
// sslocal.SQLStats. It behaves similar to a sslocal.SQLStats. However, it
// periodically writes the in-memory SQL stats into system table for
// persistence. It also performs the flush operation if it detects memory
// pressure.
type PersistedSQLStats struct {
	*sslocal.SQLStats

	cfg *Config

	// memoryPressureSignal is used by the persistedsqlstats.StatsWriter to signal
	// memory pressure during stats recording. A signal is emitted through this
	// channel either if the fingerprint limit or the memory limit has been
	// exceeded.
	memoryPressureSignal chan struct{}

	lastFlushStarted time.Time
}

var _ sqlstats.Provider = &PersistedSQLStats{}

// New returns a new instance of the PersistedSQLStats.
func New(cfg *Config, memSQLStats *sslocal.SQLStats) *PersistedSQLStats {
	return &PersistedSQLStats{
		SQLStats:             memSQLStats,
		cfg:                  cfg,
		memoryPressureSignal: make(chan struct{}),
	}
}

// Start implements sqlstats.Provider interface.
func (s *PersistedSQLStats) Start(ctx context.Context, stopper *stop.Stopper) {
	s.SQLStats.Start(ctx, stopper)
	s.startSQLStatsFlushLoop(ctx, stopper)
}

func (s *PersistedSQLStats) startSQLStatsFlushLoop(ctx context.Context, stopper *stop.Stopper) {
	_ = stopper.RunAsyncTask(ctx, "sql-stats-worker", func(ctx context.Context) {
		var resetIntervalChanged = make(chan struct{}, 1)

		SQLStatsFlushInterval.SetOnChange(&s.cfg.Settings.SV, func(ctx context.Context) {
			select {
			case resetIntervalChanged <- struct{}{}:
			default:
			}
		})

		for timer := timeutil.NewTimer(); ; timer.Reset(s.nextFlushInterval()) {
			select {
			case <-timer.C:
				timer.Read = true
			case <-s.memoryPressureSignal:
				// We are experiencing memory pressure, so we flush SQL stats to disk
				// immediately, rather than waiting the full flush interval, in an
				// attempt to relieve some of that pressure
			case <-resetIntervalChanged:
				// In this case, we would restart the loop without performing any flush
				// and recalculate the flush interval in the for-loop's post statement.
				continue
			case <-stopper.ShouldQuiesce():
				return
			}

			enabled := SQLStatsFlushEnabled.Get(&s.cfg.Settings.SV)
			if enabled {
				s.Flush(ctx)
			}
		}
	})
}

// nextFlushInterval calculates the wait interval that is between:
// [(1 - SQLStatsFlushJitter) * SQLStatsFlushInterval),
//  (1 + SQLStatsFlushJitter) * SQLStatsFlushInterval)]
func (s *PersistedSQLStats) nextFlushInterval() time.Duration {
	baseInterval := SQLStatsFlushInterval.Get(&s.cfg.Settings.SV)

	jitter := SQLStatsFlushJitter.Get(&s.cfg.Settings.SV)
	frac := 1 + (2*rand.Float64()-1)*jitter

	flushInterval := time.Duration(frac * float64(baseInterval.Nanoseconds()))
	return flushInterval
}

// GetWriterForApplication implements sqlstats.Provider interface.
func (s *PersistedSQLStats) GetWriterForApplication(appName string) sqlstats.Writer {
	writer := s.SQLStats.GetWriterForApplication(appName)
	return &StatsWriter{
		memWriter:            writer,
		memoryPressureSignal: s.memoryPressureSignal,
	}
}

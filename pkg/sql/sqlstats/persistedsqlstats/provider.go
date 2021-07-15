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
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/sslocal"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// TODO(azhng): currently we do not have the ability to compute a hash for
//  query plan. This is currently being worked on by the SQL Queries team.
//  Once we are able get consistent hash value from a query plan, we should
//  update this.
const dummyPlanHash = int64(0)

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

// flushState represents the current flushState of the PersistedSQLStats.
type flushState int

const (
	idle flushState = iota
	flushing
)

// PersistedSQLStats is a sqlstats.Provider that wraps a node-local in-memory
// sslocal.SQLStats. It behaves similar to a sslocal.SQLStats. However, it
// periodically writes the in-memory SQL stats into system table for
// persistence. It also performs the flush operation if it detects memory
// pressure.
type PersistedSQLStats struct {
	*sslocal.SQLStats

	cfg *Config

	memoryPressureSignal chan struct{}

	errChan chan error

	mu struct {
		syncutil.RWMutex
		state       flushState
		lastFlushed time.Time
	}
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
	_ = stopper.RunAsyncTask(ctx, "sql-stats-flusher", func(ctx context.Context) {
		var (
			timer                timeutil.Timer
			deadline             time.Time
			resetIntervalChanged = make(chan struct{}, 1)
		)

		SQLStatsFlushInterval.SetOnChange(&s.cfg.Settings.SV, func(ctx context.Context) {
			select {
			case resetIntervalChanged <- struct{}{}:
			default:
			}
		})

		maybeResetTimer := func() {
			var newDeadline time.Time
			resetInterval := SQLStatsFlushInterval.Get(&s.cfg.Settings.SV)

			now := s.getTimeNow()
			newDeadline = now.Add(resetInterval)

			if deadline.IsZero() || !deadline.Equal(newDeadline) {
				deadline = newDeadline
				timer.Reset(deadline.Sub(now))
			}
		}

		for {
			maybeResetTimer()
			select {
			case <-resetIntervalChanged:
				// Rerun the loop because the reset interval may have changed.
				continue
			case <-timer.C:
				timer.Read = true
			case <-s.memoryPressureSignal:
			case err := <-s.errChan:
				if log.V(1 /* level */) {
					log.Errorf(ctx, "failed to flush sql stats: %s", err)
				}
			case <-stopper.ShouldQuiesce():
				return
			}

			enabled := SQLStatsFlushEnabled.Get(&s.cfg.Settings.SV)
			if enabled {
				s.Flush(ctx, stopper)
			}
		}
	})
}

// GetWriterForApplication implements sqlstats.Provider interface.
func (s *PersistedSQLStats) GetWriterForApplication(appName string) sqlstats.Writer {
	writer := s.SQLStats.GetWriterForApplication(appName)
	return &StatsWriter{
		memWriter:            writer,
		memoryPressureSignal: s.memoryPressureSignal,
	}
}

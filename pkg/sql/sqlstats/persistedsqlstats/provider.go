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
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/sslocal"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// ErrConcurrentSQLStatsCompaction is reported when two sql stats compaction
// jobs are issued concurrently. This is a sentinel error.
var ErrConcurrentSQLStatsCompaction = errors.New("another sql stats compaction job is already running")

// Config is a configuration struct for the persisted SQL stats subsystem.
type Config struct {
	Settings         *cluster.Settings
	InternalExecutor sqlutil.InternalExecutor
	KvDB             *kv.DB
	SQLIDContainer   *base.SQLIDContainer
	JobRegistry      *jobs.Registry

	// Metrics.
	FlushCounter   *metric.Counter
	FlushDuration  *metric.Histogram
	FailureCounter *metric.Counter

	// Testing knobs.
	Knobs *sqlstats.TestingKnobs
}

// PersistedSQLStats is a sqlstats.Provider that wraps a node-local in-memory
// sslocal.SQLStats. It behaves similar to a sslocal.SQLStats. However, it
// periodically writes the in-memory SQL stats into system table for
// persistence. It also performs the flush operation if it detects memory
// pressure.
type PersistedSQLStats struct {
	*sslocal.SQLStats

	cfg *Config

	// memoryPressureSignal is used by the persistedsqlstats.ApplicationStats to signal
	// memory pressure during stats recording. A signal is emitted through this
	// channel either if the fingerprint limit or the memory limit has been
	// exceeded.
	memoryPressureSignal chan struct{}

	lastFlushStarted time.Time
	jobMonitor       jobMonitor

	atomic struct {
		nextFlushAt atomic.Value
	}
}

var _ sqlstats.Provider = &PersistedSQLStats{}

// New returns a new instance of the PersistedSQLStats.
func New(cfg *Config, memSQLStats *sslocal.SQLStats) *PersistedSQLStats {
	p := &PersistedSQLStats{
		SQLStats:             memSQLStats,
		cfg:                  cfg,
		memoryPressureSignal: make(chan struct{}),
	}

	p.jobMonitor = jobMonitor{
		st:           cfg.Settings,
		ie:           cfg.InternalExecutor,
		db:           cfg.KvDB,
		scanInterval: defaultScanInterval,
		jitterFn:     p.jitterInterval,
	}

	return p
}

// Start implements sqlstats.Provider interface.
func (s *PersistedSQLStats) Start(ctx context.Context, stopper *stop.Stopper) {
	s.startSQLStatsFlushLoop(ctx, stopper)
	s.jobMonitor.start(ctx, stopper)
}

// GetController returns the controller of the PersistedSQLStats.
func (s *PersistedSQLStats) GetController(server serverpb.SQLStatusServer) *Controller {
	return NewController(s, server, s.cfg.KvDB, s.cfg.InternalExecutor)
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

		initialDelay := s.nextFlushInterval()
		timer := timeutil.NewTimer()
		timer.Reset(initialDelay)

		log.Infof(ctx, "starting sql-stats-worker with initial delay: %s", initialDelay)
		for {
			waitInterval := s.nextFlushInterval()
			timer.Reset(waitInterval)

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

// GetLocalMemProvider returns a sqlstats.Provider that can only be used to
// access local in-memory sql statistics.
func (s *PersistedSQLStats) GetLocalMemProvider() sqlstats.Provider {
	return s.SQLStats
}

// GetNextFlushAt returns the time next flush is going to happen.
func (s *PersistedSQLStats) GetNextFlushAt() time.Time {
	return s.atomic.nextFlushAt.Load().(time.Time)
}

// nextFlushInterval calculates the wait interval that is between:
// [(1 - SQLStatsFlushJitter) * SQLStatsFlushInterval),
//  (1 + SQLStatsFlushJitter) * SQLStatsFlushInterval)]
func (s *PersistedSQLStats) nextFlushInterval() time.Duration {
	baseInterval := SQLStatsFlushInterval.Get(&s.cfg.Settings.SV)
	waitInterval := s.jitterInterval(baseInterval)

	nextFlushAt := s.getTimeNow().Add(waitInterval)
	s.atomic.nextFlushAt.Store(nextFlushAt)

	return waitInterval
}

func (s *PersistedSQLStats) jitterInterval(interval time.Duration) time.Duration {
	jitter := SQLStatsFlushJitter.Get(&s.cfg.Settings.SV)
	frac := 1 + (2*rand.Float64()-1)*jitter

	jitteredInterval := time.Duration(frac * float64(interval.Nanoseconds()))
	return jitteredInterval
}

// GetApplicationStats implements sqlstats.Provider interface.
func (s *PersistedSQLStats) GetApplicationStats(appName string) sqlstats.ApplicationStats {
	appStats := s.SQLStats.GetApplicationStats(appName)
	return &ApplicationStats{
		ApplicationStats:     appStats,
		memoryPressureSignal: s.memoryPressureSignal,
	}
}

// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//
// persistedsqlstats is a subsystem that is responsible for flushing node-local
// in-memory stats into persisted system tables.

package persistedsqlstats

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/sslocal"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// Config is a configuration struct for the persisted SQL stats subsystem.
type Config struct {
	Settings                *cluster.Settings
	InternalExecutorMonitor *mon.BytesMonitor
	DB                      isql.DB
	ClusterID               func() uuid.UUID
	SQLIDContainer          *base.SQLIDContainer
	JobRegistry             *jobs.Registry

	// Metrics.
	FlushesSuccessful       *metric.Counter
	FlushLatency            metric.IHistogram
	FlushDoneSignalsIgnored *metric.Counter
	FlushesFailed           *metric.Counter
	FlushedFingerprintCount *metric.Counter

	// Testing knobs.
	Knobs *sqlstats.TestingKnobs
}

// PersistedSQLStats wraps a node-local in-memory sslocal.SQLStats. It
// behaves similar to a sslocal.SQLStats. However, it periodically
// writes the in-memory SQL stats into system table for persistence. It
// also performs the flush operation if it detects memory pressure.
type PersistedSQLStats struct {
	*sslocal.SQLStats

	cfg *Config

	// Used to signal the flush completed.
	flushDoneMu struct {
		syncutil.Mutex
		signalCh chan<- struct{}
	}

	lastFlushStarted time.Time
	jobMonitor       jobMonitor
	atomic           struct {
		nextFlushAt atomic.Value
	}

	// drain is closed when a graceful drain is initiated.
	drain       chan struct{}
	setDraining sync.Once
	// tasksDoneWG is used to wait for all background tasks to finish.
	tasksDoneWG sync.WaitGroup

	// The last time the size was checked before doing a flush.
	lastSizeCheck time.Time
}

// New returns a new instance of the PersistedSQLStats.
func New(cfg *Config, memSQLStats *sslocal.SQLStats) *PersistedSQLStats {
	p := &PersistedSQLStats{
		SQLStats: memSQLStats,
		cfg:      cfg,
		drain:    make(chan struct{}),
	}

	p.jobMonitor = jobMonitor{
		st:           cfg.Settings,
		clusterID:    cfg.ClusterID,
		db:           cfg.DB,
		scanInterval: defaultScanInterval,
		jitterFn:     p.jitterInterval,
	}
	if cfg.Knobs != nil {
		p.jobMonitor.testingKnobs.updateCheckInterval = cfg.Knobs.JobMonitorUpdateCheckInterval
	}

	return p
}

func (s *PersistedSQLStats) Start(ctx context.Context, stopper *stop.Stopper) {
	s.startSQLStatsFlushLoop(ctx, stopper)
	s.jobMonitor.start(ctx, stopper, s.drain, &s.tasksDoneWG)
	stopper.AddCloser(stop.CloserFn(func() {
		// TODO(knz,yahor): This really should be just Stop(), but there
		// is a leak somewhere and would cause a panic when a hard stop
		// catches up with a graceful drain.
		// See: https://github.com/cockroachdb/cockroach/issues/101297
		s.cfg.InternalExecutorMonitor.EmergencyStop(ctx)
	}))
}

// Stop stops the background tasks. This is used during graceful drain
// to quiesce just the SQL activity.
func (s *PersistedSQLStats) Stop(ctx context.Context) {
	log.Infof(ctx, "stopping persisted SQL stats tasks")
	defer log.Infof(ctx, "persisted SQL stats tasks successfully shut down")
	s.setDraining.Do(func() {
		close(s.drain)
	})
	s.tasksDoneWG.Wait()
}

// SetFlushDoneSignalCh sets the channel to signal each time a flush has been completed.
func (s *PersistedSQLStats) SetFlushDoneSignalCh(sigCh chan<- struct{}) {
	s.flushDoneMu.Lock()
	defer s.flushDoneMu.Unlock()
	s.flushDoneMu.signalCh = sigCh
}

// GetController returns the controller of the PersistedSQLStats.
func (s *PersistedSQLStats) GetController(server serverpb.SQLStatusServer) *Controller {
	return NewController(s, server, s.cfg.DB)
}

func (s *PersistedSQLStats) startSQLStatsFlushLoop(ctx context.Context, stopper *stop.Stopper) {
	s.tasksDoneWG.Add(1)
	err := stopper.RunAsyncTask(ctx, "sql-stats-worker", func(ctx context.Context) {
		defer s.tasksDoneWG.Done()
		var resetIntervalChanged = make(chan struct{}, 1)

		SQLStatsFlushInterval.SetOnChange(&s.cfg.Settings.SV, func(ctx context.Context) {
			select {
			case resetIntervalChanged <- struct{}{}:
			default:
			}
		})

		initialDelay := s.nextFlushInterval()
		var timer timeutil.Timer
		timer.Reset(initialDelay)

		log.Infof(ctx, "starting sql-stats-worker with initial delay: %s", initialDelay)
		for {
			waitInterval := s.nextFlushInterval()
			timer.Reset(waitInterval)

			select {
			case <-timer.C:
				timer.Read = true
			case <-resetIntervalChanged:
				// In this case, we would restart the loop without performing any flush
				// and recalculate the flush interval in the for-loop's post statement.
				continue
			case <-s.drain:
				return
			case <-stopper.ShouldQuiesce():
				return
			}

			flushed := s.MaybeFlush(ctx, stopper)

			if !flushed {
				// If the flush did not do any work, don't signal flush completion.
				continue
			}

			// Tell the local activity translator job, if any, that we've
			// performed a round of flush.
			if sigCh := func() chan<- struct{} {
				s.flushDoneMu.Lock()
				defer s.flushDoneMu.Unlock()
				return s.flushDoneMu.signalCh
			}(); sigCh != nil {
				select {
				case sigCh <- struct{}{}:
				case <-stopper.ShouldQuiesce():
					return
				case <-s.drain:
					return
				default:
					// Don't block the flush loop if the sql activity update job is not
					// ready to receive. We should at least continue to collect and flush
					// stats for this node.
					s.cfg.FlushDoneSignalsIgnored.Inc(1)
					if log.V(1) {
						log.Warning(ctx, "sql-stats-worker: unable to signal flush completion")
					}
				}
			}
		}
	})
	if err != nil {
		s.tasksDoneWG.Done()
		log.Warningf(ctx, "failed to start sql-stats-worker: %v", err)
	}
}

// GetNextFlushAt returns the time next flush is going to happen.
func (s *PersistedSQLStats) GetNextFlushAt() time.Time {
	return s.atomic.nextFlushAt.Load().(time.Time)
}

// GetSQLInstanceID returns the SQLInstanceID.
func (s *PersistedSQLStats) GetSQLInstanceID() base.SQLInstanceID {
	return s.cfg.SQLIDContainer.SQLInstanceID()
}

// GetEnabledSQLInstanceID returns the SQLInstanceID when gateway node is enabled,
// and zero otherwise.
func (s *PersistedSQLStats) GetEnabledSQLInstanceID() base.SQLInstanceID {
	if sqlstats.GatewayNodeEnabled.Get(&s.cfg.Settings.SV) {
		return s.cfg.SQLIDContainer.SQLInstanceID()
	}
	return 0
}

// nextFlushInterval calculates the wait interval that is between:
// [(1 - SQLStatsFlushJitter) * SQLStatsFlushInterval),
//
//	(1 + SQLStatsFlushJitter) * SQLStatsFlushInterval)]
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

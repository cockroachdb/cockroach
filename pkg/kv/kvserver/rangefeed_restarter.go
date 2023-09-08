// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var rangefeedRestartInterval = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kv.rangefeed.restart_interval",
	"minimum interval between rangefeed restarts to conform to new settings",
	50*time.Millisecond,
	settings.WithVisibility(settings.Reserved),
)

var rangefeedRestartAvailablePercent = settings.RegisterIntSetting(
	settings.SystemOnly,
	"kv.rangefeed.restart_quota_headroom",
	"minimum percentage of rangefeed catchup quota that still allow processor "+
		"reconfiguration restarts to proceed. set to >100 to disable eager restarts on "+
		"processor type change",
	50,
	settings.WithVisibility(settings.Reserved),
	settings.WithValidateInt(func(i int64) error {
		if i < 0 {
			return errors.New("kv.rangefeed.restart_headroom must be equal or greater than zero")
		}
		return nil
	}),
)

type RangefeedRestarter struct {
	// Number of active catch up scans running.
	// Note this value is updated synchronously with corresponding store limiter.
	activeCatchups int64
	s              *Store
	st             *cluster.Settings
	cfgChangedC    chan interface{}

	// Cancel previous restart job context.
	cancelJob func()
}

func NewRangefeedRestarter(s *Store, st *cluster.Settings) *RangefeedRestarter {
	return &RangefeedRestarter{
		s:           s,
		st:          st,
		cfgChangedC: make(chan interface{}, 1),
		cancelJob:   func() {},
	}
}

func (p *RangefeedRestarter) Start(ctx context.Context, stopper *stop.Stopper) {
	RangeFeedUseScheduler.SetOnChange(&p.st.SV, func(context.Context) {
		useScheduler := RangeFeedUseScheduler.Get(&p.st.SV)
		p.start(ctx, useScheduler, stopper)
	})
	pacingChanged := func(ctx2 context.Context) {
		select {
		case p.cfgChangedC <- struct{}{}:
		default:
		}
	}
	concurrentRangefeedItersLimit.SetOnChange(&p.st.SV, pacingChanged)
	rangefeedRestartInterval.SetOnChange(&p.st.SV, pacingChanged)
	rangefeedRestartAvailablePercent.SetOnChange(&p.st.SV, pacingChanged)
}

// Cancel previous job and start new.
func (p *RangefeedRestarter) start(ctx context.Context, toScheduler bool, stopper *stop.Stopper) {
	p.cancelJob()
	ctx, p.cancelJob = stopper.WithCancelOnQuiesce(ctx)
	_ = stopper.RunAsyncTask(ctx, "restart range feed processors", func(ctx context.Context) {
		run(ctx, p.s, toScheduler,
			func(ctx context.Context) error {
				return waitRestartQuota(ctx, timeutil.DefaultTimeSource{}, &p.activeCatchups, p.st, p.cfgChangedC)
			},
			func(r *Replica, p rangefeed.Processor) {
				r.disconnectRangefeedWithErr(p,
					kvpb.NewError(kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_REPLICA_REMOVED)))
			})
	})
}

// startCatchup bumps number of active catch-ups counter and returns a function
// thant must be called when catchup finished to release quota.
func (p *RangefeedRestarter) startCatchup() (done func()) {
	atomic.AddInt64(&p.activeCatchups, 1)
	once := sync.Once{}
	return func() {
		once.Do(func() {
			atomic.AddInt64(&p.activeCatchups, -1)
		})
	}
}

// run performs a gradual restart of processors that don't match desired type.
func run(
	ctx context.Context,
	s *Store,
	scheduler bool,
	waitRestartQuota func(ctx context.Context) error,
	restartFn func(*Replica, rangefeed.Processor),
) {
	log.VInfof(ctx, 2, "attempting to update processors to new type of %t", scheduler)
	var restartedProcessors int
	defer func() {
		log.VInfof(ctx, 2, "restarted processors with different type: %d", restartedProcessors)
	}()

	ids := findProcessorsOfType(s, !scheduler)
	log.VInfof(ctx, 2, "found rangefeed processors with non matching config to restart %d", len(ids))
	for _, id := range ids {
		r := s.GetReplicaIfExists(id)
		if r == nil {
			continue
		}
		// Skip removed processors without waiting.
		if r.getRangefeedProcessor() == nil {
			continue
		}
		if err := waitRestartQuota(ctx); err != nil {
			// This is only the case of context cancelled and store shutting down or
			// when settings were changed to disable gradual restart completely.
			log.VInfof(ctx, 2, "stopping gradual processor restart: %s", err)
			return
		}
		// Get and recheck processor after waiting because it might have been
		// removed or replaced.
		proc := r.getRangefeedProcessor()
		if proc == nil {
			continue
		}
		_, scheduled := proc.(*rangefeed.ScheduledProcessor)
		if scheduled != scheduler {
			restartFn(r, proc)
			restartedProcessors++
		}
	}
}

// findProcessorsOfType finds range ids of replicas in the store that have
// rangefeed processor that match desired scheduler configuration filter.
func findProcessorsOfType(s *Store, scheduler bool) []roachpb.RangeID {
	var ids []roachpb.RangeID
	s.rangefeedReplicas.Lock()
	defer s.rangefeedReplicas.Unlock()
	for rangeID, schedulerID := range s.rangefeedReplicas.m {
		if !scheduler && schedulerID == 0 || scheduler && schedulerID > 0 {
			ids = append(ids, rangeID)
		}
	}
	return ids
}

// waitRestartQuota waits until enough catchup quota is available but not less
// than pacing interval.
// cfgChangedC is monitored for configuration changes
// necessary catchup quota threshold is determined as the difference between
// activeCatchups and concurrentCatchupLimit * minQuotaPercent to ensure
// restarted rangefeeds not hitting store catchup limiter.
func waitRestartQuota(
	ctx context.Context,
	t timeutil.TimeSource,
	activeCatchups *int64,
	st *cluster.Settings,
	cfgChangedC <-chan interface{},
) error {
	var delay time.Duration
	var maxRunningCatchups int64
	refreshConfig := func() error {
		delay = rangefeedRestartInterval.Get(&st.SV)
		concurrentCatchupLimit := concurrentRangefeedItersLimit.Get(&st.SV)
		minQuotaPercent := rangefeedRestartAvailablePercent.Get(&st.SV)
		minCatchups := float64(concurrentCatchupLimit*(100-minQuotaPercent)) / 100
		if minCatchups < 0 {
			return errors.New("processor restarts disabled")
		}
		maxRunningCatchups = int64(math.Round(minCatchups))
		return nil
	}

	wait := t.NewTimer()
	defer wait.Stop()
	if err := refreshConfig(); err != nil {
		return err
	}
	for {
		wait.Reset(delay)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wait.Ch():
			wait.MarkRead()
			if atomic.LoadInt64(activeCatchups) <= maxRunningCatchups {
				return nil
			}
		case <-cfgChangedC:
			if err := refreshConfig(); err != nil {
				return err
			}
		}
	}
}

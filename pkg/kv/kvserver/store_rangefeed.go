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
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/limit"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var rangefeedRestartInterval = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kv.rangefeed.restarter.interval",
	"minimum interval between rangefeed restarts to reconfigure processors "+
		"(set to 0 to disable automatic restart)",
	50*time.Millisecond,
	settings.WithVisibility(settings.Reserved),
)

var rangefeedRestartAvailableFraction = settings.RegisterFloatSetting(
	settings.SystemOnly,
	"kv.rangefeed.restarter.min_concurrent_catchup_fraction",
	"minimum available fraction of rangefeed catchup quota that allows restarts "+
		"to reconfigure processors (quota is set by kv.rangefeed.concurrent_catchup_iterators)",
	0.5,
	settings.WithVisibility(settings.Reserved),
	settings.FloatInRange(0, 1),
)

// rangeFeedUpdaterConf provides configuration for the rangefeed updater job,
// and allows watching for when it is updated.
type rangeFeedUpdaterConf struct {
	settings *cluster.Settings
	changed  <-chan struct{}
}

// newRangeFeedUpdaterConf creates the config reading from and watching the
// given cluster settings.
func newRangeFeedUpdaterConf(st *cluster.Settings) rangeFeedUpdaterConf {
	confCh := make(chan struct{}, 1)
	confChanged := func(ctx context.Context) {
		select {
		case confCh <- struct{}{}:
		default:
		}
	}
	closedts.SideTransportCloseInterval.SetOnChange(&st.SV, confChanged)
	RangeFeedRefreshInterval.SetOnChange(&st.SV, confChanged)
	RangeFeedSmearInterval.SetOnChange(&st.SV, confChanged)
	return rangeFeedUpdaterConf{settings: st, changed: confCh}
}

// get returns a pair of (refresh interval, smear interval) which determines
// pacing of the rangefeed closed timestamp updater job.
func (r rangeFeedUpdaterConf) get() (time.Duration, time.Duration) {
	refresh := RangeFeedRefreshInterval.Get(&r.settings.SV)
	if refresh <= 0 {
		refresh = closedts.SideTransportCloseInterval.Get(&r.settings.SV)
	}
	if refresh <= 0 {
		return 0, 0
	}
	smear := RangeFeedSmearInterval.Get(&r.settings.SV)
	if smear <= 0 || smear > refresh {
		smear = refresh
	}
	return refresh, smear
}

// wait blocks until it receives a valid rangefeed closed timestamp pacing
// configuration, and returns it.
func (r rangeFeedUpdaterConf) wait(ctx context.Context) (time.Duration, time.Duration, error) {
	for {
		if refresh, sched := r.get(); refresh != 0 && sched != 0 {
			return refresh, sched, nil
		}
		select {
		case <-r.changed:
			// Loop back around and check if the config is good now.
		case <-ctx.Done():
			return 0, 0, ctx.Err()
		}
	}
}

// rangeFeedUpdaterPace returns the number of work items to do (out of workLeft)
// within a quantum of time, and a suggested deadline for completing this work.
// It assumes that work can be done at constant speed and uniformly fill the
// remaining time between now and the deadline.
//
// See TestRangeFeedUpdaterPace for an example of how this function can/should
// be used for scheduling work.
func rangeFeedUpdaterPace(
	now, deadline time.Time, quantum time.Duration, workLeft int,
) (todo int, by time.Time) {
	timeLeft := deadline.Sub(now)
	if workLeft <= 0 || timeLeft <= 0 { // ran out of work or time
		return workLeft, now
	} else if timeLeft <= quantum { // time is running out
		return workLeft, deadline
	}
	// Otherwise, we have workLeft >= 1, and at least a full quantum of time.
	// Assume we can complete work at uniform speed.
	todo = int(float64(workLeft) * quantum.Seconds() / timeLeft.Seconds())
	by = now.Add(quantum)
	if todo > workLeft { // should never happen, but just in case float64 has quirks
		return workLeft, by
	} else if todo == 0 {
		return 1, by // always do some work
	}
	return todo, by
}

// rangefeedRestarter is a service responsible for restarting rangefeed
// processors if their type is different from type prescribed by
// kv.rangefeed.scheduler.enabled option.
//
// rangefeedRestarter is listening for settings changes and initiates rescan
// of replicas to find processors that doesn't match desired state. for matching
// processors, service triggers processor stop to force rangefeed restarts with
// new options.
//
// restart is controlled by two settings:
//   - pacing interval - control how fast processors could be restarted
//   - minimum quota - delay restarts if too many other rangefeeds are
//     performing catchup scans
type rangefeedRestarter struct {
	st             *cluster.Settings
	catchupLimiter limit.ConcurrentRequestLimiter
	// findRangesWithProcType returns RangeIDs or replicas that run rangefeed
	// processor of type defined by argument. Provided function typically visits
	// underlying store but is injected to break dependency on store and allow
	// testing.
	findRangesWithProcType func(scheduled bool) []roachpb.RangeID
	// getReplica retrieves replica by RangeID if it exists, otherwise it returns
	// nil. Provided function typically retrieves replica from underlying store
	// but is injected to break dependency on store and allow testing.
	getReplica func(rangeID roachpb.RangeID) *Replica

	cfgChangedC chan interface{}

	// Cancel previous restart job context.
	mu struct {
		syncutil.Mutex
		cancelJob func()
	}
}

func newRangefeedRestarter(s *Store) *rangefeedRestarter {
	r := &rangefeedRestarter{
		st:             s.cfg.Settings,
		catchupLimiter: s.limiters.ConcurrentRangefeedIters,
		findRangesWithProcType: func(scheduled bool) []roachpb.RangeID {
			return findProcessorsOfType(s, scheduled)
		},
		getReplica: func(rangeID roachpb.RangeID) *Replica {
			return s.GetReplicaIfExists(rangeID)
		},
		cfgChangedC: make(chan interface{}, 1),
	}
	r.mu.cancelJob = func() {}
	return r
}

func (rr *rangefeedRestarter) start(ctx context.Context, stopper *stop.Stopper) {
	RangeFeedUseScheduler.SetOnChange(&rr.st.SV, func(context.Context) {
		useScheduler := RangeFeedUseScheduler.Get(&rr.st.SV)
		rr.startJob(ctx, useScheduler, stopper)
	})
	pacingChanged := func(ctx2 context.Context) {
		select {
		case rr.cfgChangedC <- struct{}{}:
		default:
		}
	}
	concurrentRangefeedItersLimit.SetOnChange(&rr.st.SV, pacingChanged)
	rangefeedRestartInterval.SetOnChange(&rr.st.SV, pacingChanged)
	rangefeedRestartAvailableFraction.SetOnChange(&rr.st.SV, pacingChanged)
}

// Cancel previous job and start new.
func (rr *rangefeedRestarter) startJob(
	ctx context.Context, enableScheduler bool, stopper *stop.Stopper,
) {
	ctx, cancelJob := stopper.WithCancelOnQuiesce(ctx)
	// Swap context cancel functions under lock and cancel previous.
	rr.mu.Lock()
	cancelJob, rr.mu.cancelJob = rr.mu.cancelJob, cancelJob
	rr.mu.Unlock()
	// cancelJob is never nil by design.
	cancelJob()

	_ = stopper.RunAsyncTask(ctx, "restart range feed processors", func(ctx context.Context) {
		rr.run(ctx, enableScheduler, timeutil.DefaultTimeSource{},
			func(r *Replica, p rangefeed.Processor) {
				// Stop processor with retryable error. This will disconnect all its
				// registrations with the error provided and trigger client side restart.
				cause := kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED
				if !rr.st.Version.IsActive(ctx, clusterversion.V23_2) {
					cause = kvpb.RangeFeedRetryError_REASON_REPLICA_REMOVED
				}
				r.disconnectRangefeedWithErr(p,
					kvpb.NewError(kvpb.NewRangeFeedRetryError(cause)))
			})
	})
}

// run performs a gradual restart of processors that don't match desired type.
func (rr *rangefeedRestarter) run(
	ctx context.Context,
	enableScheduler bool,
	t timeutil.TimeSource,
	restartFn func(*Replica, rangefeed.Processor),
) {
	log.VInfof(ctx, 2, "attempting to update processors to new type of %t", enableScheduler)
	var restartedProcessors int
	defer func() {
		log.VInfof(ctx, 2, "restarted processors with different type: %d", restartedProcessors)
	}()

	getMatchingProcessor := func(r *Replica) rangefeed.Processor {
		proc := r.getRangefeedProcessor()
		if proc == nil {
			return nil
		}

		if _, scheduler := proc.(*rangefeed.ScheduledProcessor); scheduler == enableScheduler {
			return nil
		}
		return proc
	}

	var delay time.Duration
	var minAvailableCatchups uint64
	refreshConfig := func() error {
		delay = rangefeedRestartInterval.Get(&rr.st.SV)
		if delay == 0 {
			return errors.New("processor restarts disabled")
		}
		// Note that we read catchup limiter config directly here as we can't track
		// its config updates consistently. It is possible that this callback is
		// triggered before limiter is updated and if we read its value directly
		// we may end up with incorrect fraction value.
		concurrentCatchupLimit := concurrentRangefeedItersLimit.Get(&rr.st.SV)
		minAvailableQuotaFraction := rangefeedRestartAvailableFraction.Get(&rr.st.SV)
		minAvailableCatchups = computeMinCatchupLimit(concurrentCatchupLimit, minAvailableQuotaFraction)
		return nil
	}

	wait := t.NewTimer()
	defer wait.Stop()
	waitRestartQuota := func() error {
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
				if avail := rr.catchupLimiter.Available(); avail >= minAvailableCatchups {
					return nil
				}
			case <-rr.cfgChangedC:
				if err := refreshConfig(); err != nil {
					return err
				}
			}
		}
	}

	ids := rr.findRangesWithProcType(!enableScheduler)
	log.VInfof(ctx, 2, "found rangefeed processors with non matching config to restart %d", len(ids))
	for _, id := range ids {
		r := rr.getReplica(id)
		if r == nil {
			continue
		}
		// Skip removed or changed processors without waiting.
		if getMatchingProcessor(r) == nil {
			continue
		}
		if err := waitRestartQuota(); err != nil {
			// This is only the case of context cancelled and store shutting down or
			// when settings were changed to disable gradual restart completely.
			log.VInfof(ctx, 2, "stopping gradual processor restart: %s", err)
			return
		}
		// Get and recheck processor after waiting because it might have been
		// removed or replaced.
		proc := getMatchingProcessor(r)
		if proc == nil {
			continue
		}
		restartFn(r, proc)
		restartedProcessors++
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

// computeMinCatchupLimit compute available quota that still allows restarts
// to proceed. if quota fraction is non-zero we must be at least at 1 to respect
// some throttling.
func computeMinCatchupLimit(limit int64, fraction float64) uint64 {
	return uint64(math.Ceil(float64(limit) * fraction))
}

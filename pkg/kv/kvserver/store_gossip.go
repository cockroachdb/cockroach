// Copyright 2022 The Cockroach Authors.
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
	fmt "fmt"
	math "math"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigstore"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

const (
	// GossipWhenCapacityDeltaExceedsFraction specifies the fraction from the
	// last gossiped store capacity values which need be exceeded before the
	// store will gossip immediately without waiting for the periodic gossip
	// interval.
	defaultGossipWhenCapacityDeltaExceedsFraction = 0.05

	// systemDataGossipInterval is the interval at which range lease
	// holders verify that the most recent system data is gossiped.
	// This ensures that system data is always eventually gossiped, even
	// if a range lease holder experiences a failure causing a missed
	// gossip update.
	systemDataGossipInterval = 1 * time.Minute
)

var errPeriodicGossipsDisabled = errors.New("periodic gossip is disabled")

// startGossip runs an infinite loop in a goroutine which regularly checks
// whether the store has a first range or config replica and asks those ranges
// to gossip accordingly.
func (s *Store) startGossip() {
	wakeReplica := func(ctx context.Context, repl *Replica) error {
		// Acquire the range lease, which in turn triggers system data gossip
		// functions (e.g. MaybeGossipSystemConfig or MaybeGossipNodeLiveness).
		_, pErr := repl.getLeaseForGossip(ctx)
		return pErr.GoError()
	}
	gossipFns := []struct {
		key         roachpb.Key
		fn          func(context.Context, *Replica) error
		description redact.SafeString
		interval    time.Duration
	}{
		{
			key: roachpb.KeyMin,
			fn: func(ctx context.Context, repl *Replica) error {
				// The first range is gossiped by all replicas, not just the lease
				// holder, so wakeReplica is not used here.
				return repl.maybeGossipFirstRange(ctx).GoError()
			},
			description: "first range descriptor",
			interval:    s.cfg.SentinelGossipTTL() / 2,
		},
		{
			key:         keys.NodeLivenessSpan.Key,
			fn:          wakeReplica,
			description: "node liveness",
			interval:    systemDataGossipInterval,
		},
	}

	cannotGossipEvery := log.Every(time.Minute)
	cannotGossipEvery.ShouldLog() // only log next time after waiting out the delay

	// Periodic updates run in a goroutine and signal a WaitGroup upon completion
	// of their first iteration.
	s.initComplete.Add(len(gossipFns))
	for _, gossipFn := range gossipFns {
		gossipFn := gossipFn // per-iteration copy
		bgCtx := s.AnnotateCtx(context.Background())
		if err := s.stopper.RunAsyncTask(bgCtx, "store-gossip", func(ctx context.Context) {
			ticker := time.NewTicker(gossipFn.interval)
			defer ticker.Stop()
			for first := true; ; {
				// Retry in a backoff loop until gossipFn succeeds. The gossipFn might
				// temporarily fail (e.g. because node liveness hasn't initialized yet
				// making it impossible to get an epoch-based range lease), in which
				// case we want to retry quickly.
				retryOptions := base.DefaultRetryOptions()
				retryOptions.Closer = s.stopper.ShouldQuiesce()
				for r := retry.Start(retryOptions); r.Next(); {
					if repl := s.LookupReplica(roachpb.RKey(gossipFn.key)); repl != nil {
						annotatedCtx := repl.AnnotateCtx(ctx)
						if err := gossipFn.fn(annotatedCtx, repl); err != nil {
							if cannotGossipEvery.ShouldLog() {
								log.Infof(annotatedCtx, "could not gossip %s: %v", gossipFn.description, err)
							}
							if !errors.Is(err, errPeriodicGossipsDisabled) {
								continue
							}
						}
					}
					break
				}
				if first {
					first = false
					s.initComplete.Done()
				}
				select {
				case <-ticker.C:
				case <-s.stopper.ShouldQuiesce():
					return
				}
			}
		}); err != nil {
			s.initComplete.Done()
		}
	}
}

var errSysCfgUnavailable = errors.New("system config not available in gossip")

// systemGossipUpdate is a callback for gossip updates to
// the system config which affect range split boundaries.
func (s *Store) systemGossipUpdate(sysCfg *config.SystemConfig) {
	if !s.cfg.SpanConfigsDisabled && spanconfigstore.EnabledSetting.Get(&s.ClusterSettings().SV) {
		return // nothing to do
	}

	ctx := s.AnnotateCtx(context.Background())
	s.computeInitialMetrics.Do(func() {
		// Metrics depend in part on the system config. Compute them as soon as we
		// get the first system config, then periodically in the background
		// (managed by the Node).
		if err := s.ComputeMetrics(ctx); err != nil {
			log.Infof(ctx, "%s: failed initial metrics computation: %s", s, err)
		}
		log.Event(ctx, "computed initial metrics")
	})

	// We'll want to offer all replicas to the split and merge queues. Be a little
	// careful about not spawning too many individual goroutines.
	shouldQueue := s.systemConfigUpdateQueueRateLimiter.AdmitN(1)

	// For every range, update its zone config and check if it needs to
	// be split or merged.
	now := s.cfg.Clock.NowAsClockTimestamp()
	newStoreReplicaVisitor(s).Visit(func(repl *Replica) bool {
		key := repl.Desc().StartKey
		conf, err := sysCfg.GetSpanConfigForKey(ctx, key)
		if err != nil {
			if log.V(1) {
				log.Infof(context.TODO(), "failed to get span config for key %s", key)
			}
			conf = s.cfg.DefaultSpanConfig
		}

		if s.cfg.SpanConfigsDisabled ||
			!spanconfigstore.EnabledSetting.Get(&s.ClusterSettings().SV) {
			repl.SetSpanConfig(conf)
		}

		if shouldQueue {
			s.splitQueue.Async(ctx, "gossip update", true /* wait */, func(ctx context.Context, h queueHelper) {
				h.MaybeAdd(ctx, repl, now)
			})
			s.mergeQueue.Async(ctx, "gossip update", true /* wait */, func(ctx context.Context, h queueHelper) {
				h.MaybeAdd(ctx, repl, now)
			})
		}
		return true // more
	})
}

func (s *Store) asyncGossipStore(ctx context.Context, reason string, useCached bool) {
	if err := s.stopper.RunAsyncTask(
		ctx, fmt.Sprintf("storage.Store: gossip on %s", reason),
		func(ctx context.Context) {
			if err := s.GossipStore(ctx, useCached); err != nil {
				log.Warningf(ctx, "error gossiping on %s: %+v", reason, err)
			}
		}); err != nil {
		log.Warningf(ctx, "unable to gossip on %s: %+v", reason, err)
	}
}

// GossipStore broadcasts the store on the gossip network.
func (s *Store) GossipStore(ctx context.Context, useCached bool) error {
	// Temporarily indicate that we're gossiping the store capacity to avoid
	// recursively triggering a gossip of the store capacity.
	syncutil.StoreFloat64(&s.gossipQueriesPerSecondVal, -1)
	syncutil.StoreFloat64(&s.gossipWritesPerSecondVal, -1)

	storeDesc, err := s.Descriptor(ctx, useCached)
	if err != nil {
		return errors.Wrapf(err, "problem getting store descriptor for store %+v", s.Ident)
	}

	// Set countdown target for re-gossiping capacity to be large enough that
	// it would only occur when there has been significant changes. We
	// currently gossip every 10 seconds, meaning that unless significant
	// redistribution occurs we do not wish to gossip again to avoid wasting
	// bandwidth and racing with local storepool estimations.
	// TODO(kvoli): Reconsider what triggers gossip here and possibly limit to
	// only significant workload changes (load), rather than lease or range
	// count. Previoulsy, this was not as much as an issue as the gossip
	// interval was 60 seconds, such that gossiping semi-frequently on changes
	// was required.
	rangeCountdown := float64(storeDesc.Capacity.RangeCount) * s.cfg.TestingKnobs.GossipWhenCapacityDeltaExceedsFraction
	atomic.StoreInt32(&s.gossipRangeCountdown, int32(math.Ceil(math.Max(rangeCountdown, 10))))
	leaseCountdown := float64(storeDesc.Capacity.LeaseCount) * s.cfg.TestingKnobs.GossipWhenCapacityDeltaExceedsFraction
	atomic.StoreInt32(&s.gossipLeaseCountdown, int32(math.Ceil(math.Max(leaseCountdown, 10))))
	syncutil.StoreFloat64(&s.gossipQueriesPerSecondVal, storeDesc.Capacity.QueriesPerSecond)
	syncutil.StoreFloat64(&s.gossipWritesPerSecondVal, storeDesc.Capacity.WritesPerSecond)

	// Unique gossip key per store.
	gossipStoreKey := gossip.MakeStoreDescKey(storeDesc.StoreID)
	// Gossip store descriptor.
	if fn := s.cfg.TestingKnobs.StoreGossipIntercept; fn != nil {
		// Give the interceptor a chance to see and/or mutate the descriptor we're about
		// to gossip.
		fn(storeDesc)
	}
	return s.cfg.Gossip.AddInfoProto(gossipStoreKey, storeDesc, gossip.StoreTTL)
}

type capacityChangeEvent int

const (
	rangeAddEvent capacityChangeEvent = iota
	rangeRemoveEvent
	leaseAddEvent
	leaseRemoveEvent
)

// maybeGossipOnCapacityChange decrements the countdown on range
// and leaseholder counts. If it reaches 0, then we trigger an
// immediate gossip of this store's descriptor, to include updated
// capacity information.
func (s *Store) maybeGossipOnCapacityChange(ctx context.Context, cce capacityChangeEvent) {
	if s.cfg.TestingKnobs.DisableLeaseCapacityGossip && (cce == leaseAddEvent || cce == leaseRemoveEvent) {
		return
	}

	// Incrementally adjust stats to keep them up to date even if the
	// capacity is gossiped, but isn't due yet to be recomputed from scratch.
	s.cachedCapacity.Lock()
	switch cce {
	case rangeAddEvent:
		s.cachedCapacity.RangeCount++
	case rangeRemoveEvent:
		s.cachedCapacity.RangeCount--
	case leaseAddEvent:
		s.cachedCapacity.LeaseCount++
	case leaseRemoveEvent:
		s.cachedCapacity.LeaseCount--
	}
	s.cachedCapacity.Unlock()

	if ((cce == rangeAddEvent || cce == rangeRemoveEvent) && atomic.AddInt32(&s.gossipRangeCountdown, -1) == 0) ||
		((cce == leaseAddEvent || cce == leaseRemoveEvent) && atomic.AddInt32(&s.gossipLeaseCountdown, -1) == 0) {
		// Reset countdowns to avoid unnecessary gossiping.
		atomic.StoreInt32(&s.gossipRangeCountdown, 0)
		atomic.StoreInt32(&s.gossipLeaseCountdown, 0)
		s.asyncGossipStore(ctx, "capacity change", true /* useCached */)
	}
}

// recordNewPerSecondStats takes recently calculated values for the number of
// queries and key writes the store is handling and decides whether either has
// changed enough to justify re-gossiping the store's capacity.
func (s *Store) recordNewPerSecondStats(newQPS, newWPS float64) {
	oldQPS := syncutil.LoadFloat64(&s.gossipQueriesPerSecondVal)
	oldWPS := syncutil.LoadFloat64(&s.gossipWritesPerSecondVal)
	if oldQPS == -1 || oldWPS == -1 {
		// Gossiping of store capacity is already ongoing.
		return
	}

	const minAbsoluteChange = 100
	updateForQPS := (newQPS < oldQPS*.5 || newQPS > oldQPS*1.5) && math.Abs(newQPS-oldQPS) > minAbsoluteChange
	updateForWPS := (newWPS < oldWPS*.5 || newWPS > oldWPS*1.5) && math.Abs(newWPS-oldWPS) > minAbsoluteChange

	if !updateForQPS && !updateForWPS {
		return
	}

	var message string
	if updateForQPS && updateForWPS {
		message = "queries-per-second and writes-per-second change"
	} else if updateForQPS {
		message = "queries-per-second change"
	} else {
		message = "writes-per-second change"
	}
	// TODO(a-robinson): Use the provided values to avoid having to recalculate
	// them in GossipStore.
	s.asyncGossipStore(context.TODO(), message, false /* useCached */)
}

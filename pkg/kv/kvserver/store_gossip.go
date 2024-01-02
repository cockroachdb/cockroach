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
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

const (
	// defaultGossipWhenCapacityDeltaExceedsFraction specifies the fraction from the
	// last gossiped store capacity values which need be exceeded before the
	// store will gossip immediately without waiting for the periodic gossip
	// interval.
	defaultGossipWhenCapacityDeltaExceedsFraction = 0.05

	//  gossipWhenLeaseCountDeltaExceeds specifies the absolute change from the
	//  last gossiped store capacity lease count which needs to be exceeded
	//  before the store will gossip immediately without waiting for the periodic
	//  gossip interval.
	gossipWhenLeaseCountDeltaExceeds = 5

	// GossipWhenRangeCountDeltaExceeds specifies the absolute change from the
	// last gossiped store capacity range count which needs to be exceeded
	// before the store will gossip immediately without waiting for the
	// periodic gossip interval.
	GossipWhenRangeCountDeltaExceeds = 5

	// gossipWhenLoadDeltaExceedsFraction specifies the fraction from the last
	// gossiped store capacity load which needs to be exceeded before the store
	// will gossip immediately without waiting for the periodic gossip interval.
	gossipWhenLoadDeltaExceedsFraction = 0.5

	// gossipMinAbsoluteDelta is the minimum delta in load that is required to
	// trigger gossip. This stops frequent re-gossiping when load values
	// fluctuate but are insignificant in absolute terms.
	gossipMinAbsoluteDelta = 100

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
//
// TODO(kvoli): Refactor this function to sit on the StoreGossip struct,
// rather than on the store.
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

var errSpanConfigsUnavailable = errors.New("span configs not available")

// systemGossipUpdate is a callback for gossip updates to
// the system config which affect range split boundaries.
//
// TODO(kvoli): Refactor this function to sit on the store gossip struct,
// rather than on the store.
func (s *Store) systemGossipUpdate(sysCfg *config.SystemConfig) {
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

// cachedCapacity caches information on store capacity to prevent
// expensive recomputations in case leases or replicas are rapidly
// rebalancing.
type cachedCapacity struct {
	syncutil.Mutex
	cached, lastGossiped roachpb.StoreCapacity
}

// StoreGossip is responsible for gossiping the store descriptor. It maintains
// state for cached information to gosip and countdown to gossip more
// frequently on updates.
type StoreGossip struct {
	// Ident is the identity of the store this store gossip is associated with.
	// This field is set after initialization, at store Start().
	Ident   roachpb.StoreIdent
	stopper *stop.Stopper
	knobs   StoreGossipTestingKnobs
	// cachedCapacity caches information on store capacity to prevent
	// expensive recomputations in case leases or replicas are rapidly
	// rebalancing.
	cachedCapacity *cachedCapacity
	// gossipOngoing indicates whether there is currently a triggered gossip,
	// to avoid recursively re-triggering gossip.
	gossipOngoing syncutil.AtomicBool
	// gossiper is used for adding information to gossip.
	gossiper InfoGossiper
	// descriptorGetter is used for getting an up to date or cached store
	// descriptor to gossip.
	descriptorGetter StoreDescriptorProvider
}

// StoreGossipTestingKnobs defines the testing knobs specific to StoreGossip.
type StoreGossipTestingKnobs struct {
	// OverrideGossipWhenCapacityDeltaExceedsFraction specifies the fraction
	// from the last gossiped store capacity values which need be exceeded
	// before the store will gossip immediately without waiting for the
	// periodic gossip interval. This is ignored unless set to a value > 0.
	OverrideGossipWhenCapacityDeltaExceedsFraction float64
	// This method, if set, gets to see (and mutate, if desired) any local
	// StoreDescriptor before it is being sent out on the Gossip network.
	StoreGossipIntercept func(descriptor *roachpb.StoreDescriptor)
	// DisableLeaseCapacityGossip disables the ability of a changing number of
	// leases to trigger the store to gossip its capacity. With this enabled,
	// only changes in the number of replicas can cause the store to gossip its
	// capacity.
	DisableLeaseCapacityGossip bool
	// AsyncDisabled indicates that asyncGossipStore should not be treated as
	// async.
	AsyncDisabled bool
}

// NewStoreGossip returns a new StoreGosip which may be used for gossiping the
// store descriptor: both proactively, calling Gossip() and reacively on
// capacity/load changes.
func NewStoreGossip(
	gossiper InfoGossiper, descGetter StoreDescriptorProvider, testingKnobs StoreGossipTestingKnobs,
) *StoreGossip {
	return &StoreGossip{
		cachedCapacity:   &cachedCapacity{},
		gossiper:         gossiper,
		descriptorGetter: descGetter,
		knobs:            testingKnobs,
	}
}

// CachedCapacity returns the current cached capacity.
func (s *StoreGossip) CachedCapacity() roachpb.StoreCapacity {
	s.cachedCapacity.Lock()
	defer s.cachedCapacity.Unlock()

	return s.cachedCapacity.cached
}

// UpdateCachedCapacity updates the cached capacity to be equal to the capacity
// given.
func (s *StoreGossip) UpdateCachedCapacity(capacity roachpb.StoreCapacity) {
	s.cachedCapacity.Lock()
	defer s.cachedCapacity.Unlock()

	s.cachedCapacity.cached = capacity
}

// StoreDescriptorProvider provides a method to access the store descriptor.
type StoreDescriptorProvider interface {
	// Descriptor returns a StoreDescriptor including current store
	// capacity information.
	Descriptor(ctx context.Context, cached bool) (*roachpb.StoreDescriptor, error)
}

var _ StoreDescriptorProvider = &Store{}

// InfoGossiper provides a method to add a message to gossip.
type InfoGossiper interface {
	// AddInfoProto adds or updates an info object in gossip. Returns an error
	// if info couldn't be added.
	AddInfoProto(key string, msg protoutil.Message, ttl time.Duration) error
}

var _ InfoGossiper = &gossip.Gossip{}

// asyncGossipStore asynchronously gossips the store descriptor, for a given
// reason. A cached descriptor is used if specified, otherwise the store
// descriptor is updated and capacities recalculated.
func (s *StoreGossip) asyncGossipStore(ctx context.Context, reason string, useCached bool) {
	gossipFn := func(ctx context.Context) {
		log.VEventf(ctx, 2, "gossiping on %s", reason)
		if err := s.GossipStore(ctx, useCached); err != nil {
			log.Warningf(ctx, "error gossiping on %s: %+v", reason, err)
		}
	}

	// If async is disabled, then gossip immediately rather than running the
	// gossipFn in a task.
	if s.knobs.AsyncDisabled {
		gossipFn(ctx)
		return
	}

	if err := s.stopper.RunAsyncTask(
		ctx, fmt.Sprintf("storage.Store: gossip on %s", reason), gossipFn,
	); err != nil {
		log.Warningf(ctx, "unable to gossip on %s: %+v", reason, err)
	}
}

// GossipStore broadcasts the store on the gossip network.
func (s *StoreGossip) GossipStore(ctx context.Context, useCached bool) error {
	// Temporarily indicate that we're gossiping the store capacity to avoid
	// recursively triggering a gossip of the store capacity. This doesn't
	// block direct calls to GossipStore, rather capacity triggered gossip
	// outlined in the methods below.
	s.gossipOngoing.Set(true)
	defer s.gossipOngoing.Set(false)

	storeDesc, err := s.descriptorGetter.Descriptor(ctx, useCached)
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
	s.cachedCapacity.Lock()
	s.cachedCapacity.lastGossiped = storeDesc.Capacity
	s.cachedCapacity.Unlock()

	// Unique gossip key per store.
	gossipStoreKey := gossip.MakeStoreDescKey(storeDesc.StoreID)
	// Gossip store descriptor.
	if fn := s.knobs.StoreGossipIntercept; fn != nil {
		// Give the interceptor a chance to see and/or mutate the descriptor we're about
		// to gossip.
		fn(storeDesc)
	}

	return s.gossiper.AddInfoProto(gossipStoreKey, storeDesc, gossip.StoreTTL)
}

// CapacityChangeEvent represents a change in a store's capacity for either
// leases or replicas.
type CapacityChangeEvent int

const (
	RangeAddEvent CapacityChangeEvent = iota
	RangeRemoveEvent
	LeaseAddEvent
	LeaseRemoveEvent
)

// maybeGossipOnCapacityChange decrements the countdown on range
// and leaseholder counts. If it reaches 0, then we trigger an
// immediate gossip of this store's descriptor, to include updated
// capacity information.
func (s *StoreGossip) MaybeGossipOnCapacityChange(ctx context.Context, cce CapacityChangeEvent) {

	// Incrementally adjust stats to keep them up to date even if the
	// capacity is gossiped, but isn't due yet to be recomputed from scratch.
	s.cachedCapacity.Lock()
	switch cce {
	case RangeAddEvent:
		s.cachedCapacity.cached.RangeCount++
	case RangeRemoveEvent:
		s.cachedCapacity.cached.RangeCount--
	case LeaseAddEvent:
		s.cachedCapacity.cached.LeaseCount++
	case LeaseRemoveEvent:
		s.cachedCapacity.cached.LeaseCount--
	}
	s.cachedCapacity.Unlock()

	if shouldGossip, reason := s.shouldGossipOnCapacityDelta(); shouldGossip {
		s.asyncGossipStore(context.TODO(), reason, true /* useCached */)
	}
}

// recordNewPerSecondStats takes recently calculated values for the number of
// queries and key writes the store is handling and decides whether either has
// changed enough to justify re-gossiping the store's capacity.
func (s *StoreGossip) RecordNewPerSecondStats(newQPS, newWPS float64) {
	// Overwrite stats to keep them up to date even if the capacity is
	// gossiped, but isn't due yet to be recomputed from scratch.
	s.cachedCapacity.Lock()
	s.cachedCapacity.cached.QueriesPerSecond = newQPS
	s.cachedCapacity.cached.WritesPerSecond = newWPS
	s.cachedCapacity.Unlock()

	if shouldGossip, reason := s.shouldGossipOnCapacityDelta(); shouldGossip {
		// TODO(a-robinson): Use the provided values to avoid having to recalculate
		// them in GossipStore.
		s.asyncGossipStore(context.TODO(), reason, false /* useCached */)
	}
}

// shouldGossipOnCapacityDelta determines whether the difference between the
// last gossiped store capacity and the currently cached capacity is large
// enough that gossiping immediately is required to avoid poor allocation
// decisions by stores in the cluster. The difference must be large enough in
// both absolute and relative terms in order to trigger gossip.
func (s *StoreGossip) shouldGossipOnCapacityDelta() (should bool, reason string) {
	// If there is an ongoing gossip attempt, then there is no need to regossip
	// immediately as we will already be gossiping an up to date (cached) capacity.
	if s.gossipOngoing.Get() {
		return
	}

	gossipWhenCapacityDeltaExceedsFraction := defaultGossipWhenCapacityDeltaExceedsFraction
	if overrideCapacityDeltaFraction := s.knobs.OverrideGossipWhenCapacityDeltaExceedsFraction; overrideCapacityDeltaFraction > 0 {
		gossipWhenCapacityDeltaExceedsFraction = overrideCapacityDeltaFraction
	}

	s.cachedCapacity.Lock()
	updateForQPS, deltaQPS := deltaExceedsThreshold(
		s.cachedCapacity.lastGossiped.QueriesPerSecond, s.cachedCapacity.cached.QueriesPerSecond,
		gossipMinAbsoluteDelta, gossipWhenLoadDeltaExceedsFraction)
	updateForWPS, deltaWPS := deltaExceedsThreshold(
		s.cachedCapacity.lastGossiped.WritesPerSecond, s.cachedCapacity.cached.WritesPerSecond,
		gossipMinAbsoluteDelta, gossipWhenLoadDeltaExceedsFraction)
	updateForRangeCount, deltaRangeCount := deltaExceedsThreshold(
		float64(s.cachedCapacity.lastGossiped.RangeCount), float64(s.cachedCapacity.cached.RangeCount),
		GossipWhenRangeCountDeltaExceeds, gossipWhenCapacityDeltaExceedsFraction)
	updateForLeaseCount, deltaLeaseCount := deltaExceedsThreshold(
		float64(s.cachedCapacity.lastGossiped.LeaseCount), float64(s.cachedCapacity.cached.LeaseCount),
		gossipWhenLeaseCountDeltaExceeds, gossipWhenCapacityDeltaExceedsFraction)
	s.cachedCapacity.Unlock()

	if s.knobs.DisableLeaseCapacityGossip {
		updateForLeaseCount = false
	}

	if updateForQPS {
		reason += fmt.Sprintf("queries-per-second(%.1f) ", deltaQPS)
	}
	if updateForWPS {
		reason += fmt.Sprintf("writes-per-second(%.1f) ", deltaWPS)
	}
	if updateForRangeCount {
		reason += fmt.Sprintf("range-count(%.1f) ", deltaRangeCount)
	}
	if updateForLeaseCount {
		reason += fmt.Sprintf("lease-count(%.1f) ", deltaLeaseCount)
	}
	if reason != "" {
		should = true
		reason += "change"
	}
	return should, reason
}

// shouldGossipOnLoadDelta takes in old gossiped load value and a new one,
// returning true if the delta exceeds the threshold to gossip.
func deltaExceedsThreshold(
	old, cur, requiredMinDelta, requiredDeltaFraction float64,
) (exceeds bool, delta float64) {
	delta = cur - old
	deltaAbsolute := math.Abs(cur - old)
	deltaFraction := 10e9
	// If the old value was not zero, then calculate the fractional delta.
	// Otherwise it is undefined and we defer to the absolute check by setting
	// it to a high number.
	if old != 0 {
		deltaFraction = deltaAbsolute / old
	}
	exceeds = deltaAbsolute >= requiredMinDelta && deltaFraction >= requiredDeltaFraction
	return exceeds, delta
}

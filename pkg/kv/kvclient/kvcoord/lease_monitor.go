// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord

import (
	"context"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// LeaseMonitor monitors ranges for lease movement. It tracks in-flight requests
// by replica, and periodically checks the range lease. If the lease expired, it
// probes the range for a new lease. If the lease moves to a different replica
// while a request is in flight, it cancels the request's context such that the
// DistSender will retry the request on the new leaseholder. This prevents
// requests getting stuck on a broken replica, e.g. in the case of disk stalls,
// replica stalls, partial network partitions, etc.
type LeaseMonitor struct {
	stopper          *stop.Stopper
	clock            *hlc.Clock
	transportFactory TransportFactory
	nodeDescs        NodeDescStore
	ranges           syncutil.IntMap // rangeID -> *rangeMonitor

	// Injected during Start().
	liveness   *liveness.NodeLiveness
	rangeCache *rangecache.RangeCache
	metrics    DistSenderMetrics
}

func NewLeaseMonitor(
	stopper *stop.Stopper,
	clock *hlc.Clock,
	transportFactory TransportFactory,
	nodeDescs NodeDescStore,
) *LeaseMonitor {
	return &LeaseMonitor{
		stopper:          stopper,
		clock:            clock,
		transportFactory: transportFactory,
		nodeDescs:        nodeDescs,
	}
}

// Start starts the lease monitor. Several dependencies are injected here to
// avoid circular dependencies during construction.
func (lm *LeaseMonitor) Start(
	ctx context.Context,
	liveness *liveness.NodeLiveness,
	rangeCache *rangecache.RangeCache,
	metrics DistSenderMetrics,
) error {
	lm.liveness = liveness
	lm.rangeCache = rangeCache
	lm.metrics = metrics

	return lm.stopper.RunAsyncTask(ctx, "lease-monitor", func(ctx context.Context) {
		ctx, cancel := lm.stopper.WithCancelOnQuiesce(ctx)
		defer cancel()

		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
			case <-ctx.Done():
				return
			}
			lm.ranges.Range(func(_ int64, v unsafe.Pointer) bool {
				rm := (*rangeMonitor)(v)
				if err := rm.MaybeProbeAndRedirect(ctx); err != nil {
					log.Errorf(ctx, "failed to probe r%d lease: %v", rm.rangeID, err)
				}
				return true
			})
		}
	})
}

// Track tracks a request. It returns a context that should be used for the
// DistSender send, and a cancel function that should be called when the request
// has completed to untrack it.
//
// If the lease moves elsewhere while the requests is still in flight, the
// returned context will be cancelled. The caller can disambiguate this from
// a client context cancellation by checking if the passed-in context cancelled.
//
// TODO: consider returning a tracking token instead, wrapping the context.
func (lm *LeaseMonitor) Track(
	ctx context.Context, desc roachpb.RangeDescriptor, ba *kvpb.BatchRequest,
) (context.Context, func()) {
	if ba.RangeID == 0 {
		log.Fatalf(ctx, "BatchRequest without range ID: %s", ba)
	}
	if ba.Replica.ReplicaID == 0 {
		log.Fatalf(ctx, "BatchRequest without replica ID: %s", ba)
	}

	return lm.getOrCreateRangeMonitor(ba.RangeID, desc.StartKey).Track(ctx, ba)
}

// getOrCreateRangeMonitor looks up a rangeMonitor, or creates it if it does
// not already exist.
//
// TODO: we'll need to unregister these as ranges are removed too.
func (lm *LeaseMonitor) getOrCreateRangeMonitor(
	rangeID roachpb.RangeID, key roachpb.RKey,
) *rangeMonitor {
	// Fast path.
	if v, ok := lm.ranges.Load(int64(rangeID)); ok {
		return (*rangeMonitor)(v)
	}

	rm := &rangeMonitor{
		lm:      lm,
		rangeID: rangeID,
		key:     key, // TODO: RangeDescriptor.StartKey is immutable, but maybe clone anyway?
	}
	rm.mu.cancelFns = map[roachpb.ReplicaID]map[*kvpb.BatchRequest]func(){}

	if v, loaded := lm.ranges.LoadOrStore(int64(rangeID), unsafe.Pointer(rm)); loaded {
		return (*rangeMonitor)(v) // raced with concurrent store
	}
	return rm
}

// rangeMonitor tracks the lease for a single range.
type rangeMonitor struct {
	lm      *LeaseMonitor
	rangeID roachpb.RangeID
	key     roachpb.RKey // range start key

	mu struct {
		syncutil.RWMutex
		// TODO: We have to track the lease ourselves for now. It would be better to
		// store it in the range cache, but it only looks at the lease sequence when
		// updating, not the epoch or expiration.
		lease roachpb.Lease

		// cancelFns contains the context cancellation functions for in-flight
		// requests, by replica ID.
		cancelFns map[roachpb.ReplicaID]map[*kvpb.BatchRequest]func()
	}
}

// isValidLease returns whether the given lease is valid.
func (rm *rangeMonitor) isValidLease(lease roachpb.Lease) bool {
	if lease.Empty() {
		return false
	}

	now := rm.lm.clock.Now()

	switch lease.Type() {
	case roachpb.LeaseExpiration:
		if lease.Expiration == nil {
			return false
		}
		return now.Less(*lease.Expiration)

	case roachpb.LeaseEpoch:
		l, ok := rm.lm.liveness.GetLiveness(lease.Replica.NodeID)
		if !ok {
			return false
		}
		return lease.Epoch == l.Epoch && now.Less(l.Expiration.ToTimestamp())

	default:
		log.Fatalf(context.Background(), "unknown lease type %s", lease.Type())
		return false
	}
}

// hasValidLease returns whether the currently tracked lease is valid.
func (rm *rangeMonitor) hasValidLease() bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.isValidLease(rm.mu.lease)
}

// maybeUpdateLease updates the tracked lease if the given lease is newer. It
// says nothing about whether the lease is actually valid, see hasValidLease().
//
// TODO: move this to Lease.IsNewer().
func (rm *rangeMonitor) maybeUpdateLease(lease roachpb.Lease) bool {
	shouldUpdate := func() bool {
		if lease.Sequence > rm.mu.lease.Sequence {
			return true
		} else if rm.mu.lease.Start.Less(lease.Start) {
			return true
		} else if t := lease.Type(); t != rm.mu.lease.Type() {
			return t == roachpb.LeaseEpoch // upgrade
		} else if t == roachpb.LeaseEpoch {
			return lease.Epoch > rm.mu.lease.Epoch
		} else if t == roachpb.LeaseExpiration {
			return rm.mu.lease.Expiration.Less(*lease.Expiration)
		} else {
			return false
		}
	}()

	if shouldUpdate {
		rm.mu.lease = lease
	}
	return shouldUpdate
}

// track tracks a range request.
func (rm *rangeMonitor) Track(
	ctx context.Context, ba *kvpb.BatchRequest,
) (context.Context, func()) {
	trackCtx, trackCancel := context.WithCancel(context.Background())

	// The returned cancel function also needs to untrack the request.
	cancel := func() {
		rm.mu.Lock()
		defer rm.mu.Unlock()
		if cancelFns, ok := rm.mu.cancelFns[ba.Replica.ReplicaID]; ok {
			if cancelFn, ok := cancelFns[ba]; ok {
				delete(cancelFns, ba)
				cancelFn() // TODO consider calling outside of lock, or by closing over trackCancel()
			}
		}
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()
	if _, ok := rm.mu.cancelFns[ba.Replica.ReplicaID]; !ok {
		rm.mu.cancelFns[ba.Replica.ReplicaID] = map[*kvpb.BatchRequest]func(){}
	}
	rm.mu.cancelFns[ba.Replica.ReplicaID][ba] = trackCancel

	return trackCtx, cancel
}

// MaybeProbeAndRedirect will probe the range for a valid lease, unless we
// already know about one. If the lease has moved, we'll cancel in-flight
// requests to other replicas.
func (rm *rangeMonitor) MaybeProbeAndRedirect(ctx context.Context) error {
	if err := rm.MaybeProbe(ctx); err != nil {
		return err
	}
	if err := rm.MaybeRedirect(ctx); err != nil {
		return err
	}
	return nil
}

// MaybeProbe will probe the range for a valid lease, unless we already
// know about one.
func (rm *rangeMonitor) MaybeProbe(ctx context.Context) error {
	// We already know about a valid lease.
	if rm.hasValidLease() {
		return nil
	}

	// If there are no tracked requests, don't probe for a lease.
	hasTrackedRequests := func() bool {
		rm.mu.RLock()
		defer rm.mu.RUnlock()

		for _, cancelFns := range rm.mu.cancelFns {
			if len(cancelFns) > 0 {
				return true
			}
		}
		return false
	}()
	if !hasTrackedRequests {
		return nil
	}

	// Fetch the descriptor and lease from the range cache.
	//
	// TODO: The range cache only cares about the identity of the leaseholder, not
	// the validity of the lease. Fix this, so we can use the range cache to track
	// the lease.
	tok, err := rm.lm.rangeCache.LookupWithEvictionToken(
		ctx, rm.key, rangecache.EvictionToken{}, false)
	if err != nil {
		return err
	}
	if lease := tok.Lease(); lease != nil {
		if rm.maybeUpdateLease(*lease) && rm.hasValidLease() {
			// The range cache contained a valid lease, we're done.
			return nil
		}
	}
	rm.mu.RLock()
	lease := rm.mu.lease
	rm.mu.RUnlock()

	desc := tok.Desc()
	if desc == nil {
		return nil // TODO consider doing something better
	}

	// Probe the replicas.
	replicas, err := NewReplicaSlice(ctx, rm.lm.nodeDescs, desc, nil, AllExtantReplicas)
	if err != nil {
		return err
	}
	opts := SendOptions{
		class:   rpc.SystemClass,
		metrics: &rm.lm.metrics,
	}
	transport, err := rm.lm.transportFactory(opts, replicas)
	if err != nil {
		return err
	}
	defer transport.Release()
	transport.MoveToFront(lease.Replica)

	for !transport.IsExhausted() {
		var ba kvpb.BatchRequest
		ba.Replica = transport.NextReplica()
		ba.RangeID = desc.RangeID
		ba.Add(&kvpb.LeaseInfoRequest{
			RequestHeader: kvpb.RequestHeader{
				Key: desc.StartKey.AsRawKey(),
			},
		})

		sendCtx, cancel := context.WithTimeout(ctx, time.Second)
		br, err := transport.SendNext(sendCtx, &ba)
		cancel()

		if err := ctx.Err(); err != nil {
			return err
		}
		if err == nil && br.Error != nil {
			err = br.Error.GoError()
		}
		// TODO: we're going to have to handle various errors here, like NLHE, range
		// splits, replica removals, etc. Maybe better if we can use the DistSender?
		if err != nil {
			log.Errorf(ctx, "XXX failed to probe lease on %s: %s", ba.Replica, err)
			continue
		}
		resp := br.Responses[0].GetLeaseInfo()
		lease := resp.Lease
		if resp.CurrentLease != nil {
			lease = *resp.CurrentLease
		}
		log.Infof(ctx, "XXX found new lease: %s", lease)
		rm.maybeUpdateLease(lease) // the lease is valid if GetLeaseInfo() returned success
		return nil
	}
	return errors.Errorf("couldn't detect valid lease on r%d", rm.rangeID)
}

// MaybeRedirect will cancel in-flight requests to non-leaseholder replicas.
// Does nothing it we don't know about a valid lease.
//
// TODO: this can be turned into a DistSender circuit breaker, by cancelling all
// requests when we haven't seen a valid lease for some time.
func (rm *rangeMonitor) MaybeRedirect(ctx context.Context) error {
	if !rm.hasValidLease() {
		return nil
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	for replicaID, cancelFns := range rm.mu.cancelFns {
		if replicaID == rm.mu.lease.Replica.ReplicaID {
			continue
		}
		for ba, cancel := range cancelFns {
			log.Warningf(ctx, "XXX cancelling request %v", ba)
			cancel()
		}
		delete(rm.mu.cancelFns, replicaID)
	}

	return nil
}

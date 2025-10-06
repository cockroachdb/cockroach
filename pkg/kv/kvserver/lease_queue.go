// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/plan"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	// leaseQueueTimerDuration is the duration between (potential) lease transfers of queued
	// replicas. Use a zero duration to process lease transfers greedily.
	leaseQueueTimerDuration = 0
	// leaseQueuePurgatoryCheckInterval is the interval at which replicas in
	// the lease queue purgatory are re-attempted.
	leaseQueuePurgatoryCheckInterval = 10 * time.Second
)

// MinLeaseTransferInterval controls how frequently leases can be transferred
// for rebalancing. It does not prevent transferring leases in order to allow a
// replica to be removed from a range or moving from a non-preferred
// leaseholder to a preferred leaseholder.
//
// TODO(kvoli): Revisit this rate limiting mechanism, a setting which scales
// with cluster or store size is preferable #119155.
var MinLeaseTransferInterval = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kv.allocator.min_lease_transfer_interval",
	"controls how frequently leases can be transferred for rebalancing. "+
		"It does not prevent transferring leases in order to allow a "+
		"replica to be removed from a range.",
	1*time.Second,
	settings.NonNegativeDuration,
)

// MinIOOverloadLeaseShedInterval controls how frequently a store may decide to
// shed all leases due to becoming IO overloaded.
var MinIOOverloadLeaseShedInterval = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kv.allocator.min_io_overload_lease_shed_interval",
	"controls how frequently all leases can be shed from a node "+
		"due to the node becoming IO overloaded",
	30*time.Second,
	settings.NonNegativeDuration,
)

type leaseQueue struct {
	planner           plan.ReplicationPlanner
	allocator         allocatorimpl.Allocator
	storePool         storepool.AllocatorStorePool
	purgCh            <-chan time.Time
	lastLeaseTransfer atomic.Value // read and written by scanner & queue goroutines
	*baseQueue
}

var _ queueImpl = &leaseQueue{}

func newLeaseQueue(store *Store, allocator allocatorimpl.Allocator) *leaseQueue {
	var storePool storepool.AllocatorStorePool
	if store.cfg.StorePool != nil {
		storePool = store.cfg.StorePool
	}
	lq := &leaseQueue{
		planner:   plan.NewLeasePlanner(allocator, storePool),
		allocator: allocator,
		storePool: storePool,
		purgCh:    time.NewTicker(leaseQueuePurgatoryCheckInterval).C,
	}

	lq.baseQueue = newBaseQueue("lease", lq, store,
		queueConfig{
			maxSize:              defaultQueueMaxSize,
			needsLease:           true,
			needsSpanConfigs:     true,
			acceptsUnsplitRanges: false,
			successes:            store.metrics.LeaseQueueSuccesses,
			failures:             store.metrics.LeaseQueueFailures,
			pending:              store.metrics.LeaseQueuePending,
			processingNanos:      store.metrics.LeaseQueueProcessingNanos,
			purgatory:            store.metrics.LeaseQueuePurgatory,
			disabledConfig:       kvserverbase.LeaseQueueEnabled,
		})

	return lq
}

func (lq *leaseQueue) shouldQueue(
	ctx context.Context, now hlc.ClockTimestamp, repl *Replica, confReader spanconfig.StoreReader,
) (shouldQueue bool, priority float64) {
	conf, _, err := confReader.GetSpanConfigForKey(ctx, repl.startKey)
	if err != nil {
		return false, 0
	}
	desc := repl.Desc()
	return lq.planner.ShouldPlanChange(ctx, now, repl, desc, &conf, plan.PlannerOptions{
		CanTransferLease: lq.canTransferLeaseFrom(ctx, repl, &conf),
	})
}

func (lq *leaseQueue) process(
	ctx context.Context, repl *Replica, confReader spanconfig.StoreReader,
) (processed bool, err error) {
	if tokenErr := repl.allocatorToken.TryAcquire(ctx, lq.name); tokenErr != nil {
		return false, tokenErr
	}
	defer repl.allocatorToken.Release(ctx)

	conf, _, err := confReader.GetSpanConfigForKey(ctx, repl.startKey)
	if err != nil {
		return false, err
	}
	desc := repl.Desc()
	change, err := lq.planner.PlanOneChange(ctx, repl, desc, &conf, plan.PlannerOptions{
		CanTransferLease: lq.canTransferLeaseFrom(ctx, repl, &conf),
	})
	if err != nil {
		log.KvDistribution.Infof(ctx, "err=%v", err)
		return false, err
	}

	if transferOp, ok := change.Op.(plan.AllocationTransferLeaseOp); ok {
		log.KvDistribution.Infof(ctx, "transferring lease to s%d usage=%v", transferOp.Target, transferOp.Usage)
		lq.lastLeaseTransfer.Store(timeutil.Now())
		if err := repl.AdminTransferLease(ctx, transferOp.Target, false /* bypassSafetyChecks */); err != nil {
			return false, errors.Wrapf(err, "%s: unable to transfer lease to s%d", repl, transferOp.Target)
		}
		change.Op.ApplyImpact(lq.storePool)
	}

	return true, nil
}

func (lq *leaseQueue) postProcessScheduled(
	ctx context.Context, replica replicaInQueue, priority float64,
) {
}

func (lq *leaseQueue) timer(time.Duration) time.Duration {
	return leaseQueueTimerDuration
}

func (lq *leaseQueue) purgatoryChan() <-chan time.Time {
	return lq.purgCh
}

func (lq *leaseQueue) updateChan() <-chan time.Time {
	// TODO(kvoli): implement an update callback on gossiped store descriptors,
	// similar to what is done in the replicate queue.
	return nil
}

// canTransferLeaseFrom checks is a lease can be transferred from the specified
// replica. It considers two factors if the replica is in-conformance with
// lease preferences and the last time a transfer occurred to avoid thrashing.
func (lq *leaseQueue) canTransferLeaseFrom(
	ctx context.Context, repl *Replica, conf *roachpb.SpanConfig,
) bool {
	// If there are preferred leaseholders and the current leaseholder is not in
	// this group, then always allow the lease to transfer without delay.
	if lq.allocator.LeaseholderShouldMoveDueToPreferences(
		ctx,
		lq.storePool,
		conf,
		repl,
		repl.Desc().Replicas().VoterDescriptors(),
		false, /* excludeReplsInNeedOfSnap */
	) {
		return true
	}
	// If the local store is IO overloaded, then always allow transferring the
	// lease away from the replica.
	if !lq.store.existingLeaseCheckIOOverload(ctx) {
		return true
	}
	if lastLeaseTransfer := lq.lastLeaseTransfer.Load(); lastLeaseTransfer != nil {
		minInterval := MinLeaseTransferInterval.Get(&lq.store.cfg.Settings.SV)
		return timeutil.Since(lastLeaseTransfer.(time.Time)) > minInterval
	}
	return true
}

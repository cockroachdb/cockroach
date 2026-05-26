// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/mmaintegration"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type replicaToApplyChanges interface {
	RangeUsageInfo() allocator.RangeUsageInfo
	AdminTransferLease(ctx context.Context, target roachpb.StoreID, bypassSafetyChecks bool) error
	changeReplicasImpl(
		ctx context.Context,
		desc *roachpb.RangeDescriptor,
		senderName kvserverpb.SnapshotRequest_QueueName,
		senderQueuePriority float64,
		reason kvserverpb.RangeLogEventReason,
		details string,
		chgs kvpb.ReplicationChanges,
	) (updatedDesc *roachpb.RangeDescriptor, _ error)
	Desc() *roachpb.RangeDescriptor
}

// mmaStoreRebalancer is the main struct that implements the mma store
// rebalancer. It takes store leaseholder messages from Store and store load
// messages from Gossip as input. It computes the changes using the mma allocator
// and applies the changes.
// TODO(wenyihu6): add allocator sync which coordinates with replicate queue
// and store rebalancer and store pool.
type mmaStoreRebalancer struct {
	store              *mmaStore
	mma                mmaprototype.Allocator
	st                 *cluster.Settings
	as                 *mmaintegration.AllocatorSync
	processTimeoutFunc queueProcessTimeoutFunc
}

func newMMAStoreRebalancer(
	s *Store, mma mmaprototype.Allocator, st *cluster.Settings,
) *mmaStoreRebalancer {
	// Initialize disk utilization thresholds from cluster settings.
	opts := allocatorimpl.MakeDiskCapacityOptions(&st.SV)
	mma.SetDiskUtilThresholds(opts.RebalanceToThreshold, opts.ShedAndBlockAllThreshold)
	as := s.cfg.AllocatorSync
	if as == nil {
		// Production code (server.go) is expected to populate AllocatorSync on
		// the StoreConfig. A handful of test entry points (TestStoreConfig and
		// the assorted store_rebalancer_test setups) populate StorePool but not
		// AllocatorSync; fall back to constructing one here so those tests don't
		// trip a nil-receiver panic from the background rebalancer goroutine.
		if !buildutil.CrdbTestBuild {
			panic(errors.AssertionFailedf("AllocatorSync must be set on StoreConfig outside of test builds"))
		}
		as = mmaintegration.NewAllocatorSync(s.cfg.StorePool, mma, st, nil /* knobs */)
	}
	return &mmaStoreRebalancer{
		store:              (*mmaStore)(s),
		mma:                mma,
		st:                 st,
		as:                 as,
		processTimeoutFunc: makeRateLimitedTimeoutFunc(rebalanceSnapshotRate),
	}
}

// run loops in a loop and rebalances the store periodically. It doesn't return
// until the context is done or the stopper is quiesced.
func (m *mmaStoreRebalancer) run(ctx context.Context, stopper *stop.Stopper) {
	timer := time.NewTicker(jitteredInterval(allocator.LoadBasedRebalanceInterval.Get(&m.st.SV)))
	defer timer.Stop()
	log.KvDistribution.Infof(ctx, "starting multi-metric store rebalancer with mode=%v", kvserverbase.GetLoadBasedRebalancingMode(ctx, m.st))

	for {
		select {
		case <-ctx.Done():
			return
		case <-stopper.ShouldQuiesce():
			return
		case <-timer.C:
			// Wait out the first tick before doing anything since the store is still
			// starting up and we might as well wait for some stats to accumulate.
			timer.Reset(jitteredInterval(allocator.LoadBasedRebalanceInterval.Get(&m.st.SV)))
			if !kvserverbase.LoadBasedRebalancingModeIsMMA(ctx, m.st) {
				continue
			}

			// Keeps rebalancing until no changes are computed. Then exit and await
			// for the next interval.
			periodicCall := true
			for {
				attemptedChanges := m.rebalance(ctx, periodicCall)
				if !attemptedChanges {
					break
				}
				periodicCall = false
			}
		}
	}
}

// TODO(mma): We should add an integration struct (see server.go gossip
// callback), which will be responsible for:
//   - registering the gossip callback (see server.go)
//   - Allocator.SetStore() upon a new store being seen (triggered via
//     gossip callback presumably).
//   - Allocator.UpdateFailureDetectionSummary() upon node liveness
//     status changing. (draining|dead|live|unavailable).
//
// In the future, it would also be responsible for:
//   - Allocator.UpdateStoreMembership() upon a store being marked as
//     decommissioning. We can defer this for now, since we don't need to
//     necessarily support decommissioning stores in the prototype
//   - updating the StorePool with enacted changes made by this rebalancer and
//     vice-versa for the replicate queue, lease queue and store rebalancer

// start launches the mmaStoreRebalancer.run in the background. It continues
// running until the context is done or the stopper is quiesced.
func (m *mmaStoreRebalancer) start(ctx context.Context, stopper *stop.Stopper) {
	_ = stopper.RunAsyncTask(ctx, "mma-store-rebalancer", func(ctx context.Context) {
		m.run(ctx, stopper)
	})
}

// rebalance computes the changes using the mma allocator and applies the
// changes to the store. It returns true if any changes were computed as a
// signal to the caller that it should continue calling rebalance. Note that
// rebalance may return true if errors happen in the process and fail to apply
// the changes successfully.
func (m *mmaStoreRebalancer) rebalance(ctx context.Context, periodicCall bool) bool {
	opts := allocatorimpl.MakeDiskCapacityOptions(&m.st.SV)
	m.mma.SetDiskUtilThresholds(opts.RebalanceToThreshold, opts.ShedAndBlockAllThreshold)
	m.mma.UpdateStoresStatuses(ctx, m.as.GetMMAStoreStatuses())
	knownStoresByMMA := m.mma.KnownStores()
	storeLeaseholderMsg, numIgnoredRanges := m.store.MakeStoreLeaseholderMsg(ctx, knownStoresByMMA)
	if numIgnoredRanges > 0 {
		log.KvDistribution.Infof(ctx, "mma rebalancer: ignored %d ranges since the allocator does not know all stores",
			numIgnoredRanges)
	}

	changes := m.mma.ComputeChanges(ctx, &storeLeaseholderMsg, mmaprototype.ChangeOptions{
		LocalStoreID: m.store.StoreID(),
		PeriodicCall: periodicCall,
	})

	// TODO(wenyihu6): add allocator sync and post apply here
	for _, change := range changes {
		if err := m.applyChange(ctx, change); err != nil {
			log.KvDistribution.VInfof(ctx, 1, "failed to apply change for range %d: %v", change.RangeID, err)
		}
	}

	return len(changes) > 0
}

// applyChange safely applies a single change to the store. It handles the case
// where the replica might not exist and provides proper error handling.
func (m *mmaStoreRebalancer) applyChange(
	ctx context.Context, change mmaprototype.ExternalRangeChange,
) error {
	repl := m.store.GetReplicaIfExists(change.RangeID)
	if repl == nil {
		m.as.MarkChangeAsFailed(ctx, change)
		return errors.Errorf("replica not found for range %d", change.RangeID)
	}
	changeID := m.as.MMAPreApply(ctx, repl.RangeUsageInfo(), change)
	var err error
	switch {
	case change.IsPureTransferLease():
		err = m.applyLeaseTransfer(ctx, repl, change)
	case change.IsChangeReplicas():
		err = m.applyReplicaChanges(ctx, repl, change)
	default:
		return errors.Errorf("unknown change type for range %d", change.RangeID)
	}
	// Inform allocator sync that the change has been applied which applies
	// changes to store pool and inform mma.
	m.as.PostApply(ctx, changeID, err == nil /*success*/)
	return err
}

// processTimeout computes the timeout for applying a change to a replica,
// using the same rate-limited timeout function as the replicate queue and the
// old store rebalancer.
func (m *mmaStoreRebalancer) processTimeout(repl replicaToApplyChanges) time.Duration {
	return m.processTimeoutFunc(m.st, repl.(*Replica))
}

// applyLeaseTransfer applies a lease transfer change.
func (m *mmaStoreRebalancer) applyLeaseTransfer(
	ctx context.Context, repl replicaToApplyChanges, change mmaprototype.ExternalRangeChange,
) error {
	timeout := m.processTimeout(repl)
	return timeutil.RunWithTimeout(ctx, "mma transfer lease", timeout,
		func(ctx context.Context) error {
			return repl.AdminTransferLease(
				ctx,
				change.LeaseTransferTarget(),
				false, /* bypassSafetyChecks */
			)
		})
}

// applyReplicaChanges applies replica membership changes.
func (m *mmaStoreRebalancer) applyReplicaChanges(
	ctx context.Context, repl replicaToApplyChanges, change mmaprototype.ExternalRangeChange,
) error {
	// TODO(wenyihu6): store rebalancer uses RelocateRange
	timeout := m.processTimeout(repl)
	return timeutil.RunWithTimeout(ctx, "mma change replicas", timeout,
		func(ctx context.Context) error {
			_, err := repl.changeReplicasImpl(
				ctx,
				repl.Desc(),
				kvserverpb.SnapshotRequest_REPLICATE_QUEUE,
				0,
				kvserverpb.ReasonRebalance,
				"todo: this is the rebalance detail for the range log",
				change.ReplicationChanges(),
			)
			return err
		})
}

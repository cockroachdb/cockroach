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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

type replicaToApplyChanges interface {
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
	store *mmaStore
	mma   mmaprototype.Allocator
	st    *cluster.Settings
	sp    *storepool.StorePool
	// TODO(wenyihu6): add allocator sync
}

func newMMAStoreRebalancer(
	s *Store, mma mmaprototype.Allocator, st *cluster.Settings, sp *storepool.StorePool,
) *mmaStoreRebalancer {
	return &mmaStoreRebalancer{
		store: (*mmaStore)(s),
		mma:   mma,
		st:    st,
		sp:    sp,
	}
}

// run loops in a loop and rebalances the store periodically. It doesn't return
// until the context is done or the stopper is quiesced.
func (m *mmaStoreRebalancer) run(ctx context.Context, stopper *stop.Stopper) {
	timer := time.NewTicker(jitteredInterval(allocator.LoadBasedRebalanceInterval.Get(&m.st.SV)))
	defer timer.Stop()
	log.Infof(ctx, "starting multi-metric store rebalancer with mode=%v", LoadBasedRebalancingMode.Get(&m.st.SV))

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
			if LoadBasedRebalancingMode.Get(&m.st.SV) != LBRebalancingMultiMetric {
				continue
			}

			// Keeps rebalancing until no changes are computed. Then exit and await
			// for the next interval.
			for {
				attemptedChanges := m.rebalance(ctx)
				if !attemptedChanges {
					break
				}
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
func (m *mmaStoreRebalancer) rebalance(ctx context.Context) bool {
	knownStoresByMMA := m.mma.KnownStores()
	storeLeaseholderMsg, numIgnoredRanges := m.store.MakeStoreLeaseholderMsg(ctx, knownStoresByMMA)
	if numIgnoredRanges > 0 {
		log.Infof(ctx, "mma rebalancer: ignored %d ranges since the allocator does not know all stores",
			numIgnoredRanges)
	}

	changes := m.mma.ComputeChanges(ctx, &storeLeaseholderMsg, mmaprototype.ChangeOptions{
		LocalStoreID: m.store.StoreID(),
	})

	// TODO(wenyihu6): add allocator sync and post apply here
	for _, change := range changes {
		if err := m.applyChange(ctx, change); err != nil {
			log.VInfof(ctx, 1, "failed to apply change for range %d: %v", change.RangeID, err)
		}
	}

	return len(changes) > 0
}

// applyChange safely applies a single change to the store. It handles the case
// where the replica might not exist and provides proper error handling.
func (m *mmaStoreRebalancer) applyChange(
	ctx context.Context, change mmaprototype.PendingRangeChange,
) error {
	repl := m.store.GetReplicaIfExists(change.RangeID)
	if repl == nil {
		return errors.Errorf("replica not found for range %d", change.RangeID)
	}
	if change.IsTransferLease() {
		return m.applyLeaseTransfer(ctx, repl, change)
	} else if change.IsChangeReplicas() {
		return m.applyReplicaChanges(ctx, repl, change)
	}

	return errors.Errorf("unknown change type for range %d", change.RangeID)
}

// applyLeaseTransfer applies a lease transfer change.
func (m *mmaStoreRebalancer) applyLeaseTransfer(
	ctx context.Context, repl replicaToApplyChanges, change mmaprototype.PendingRangeChange,
) error {
	return repl.AdminTransferLease(
		ctx,
		change.LeaseTransferTarget(),
		false, /* bypassSafetyChecks */
	)
}

// applyReplicaChanges applies replica membership changes.
func (m *mmaStoreRebalancer) applyReplicaChanges(
	ctx context.Context, repl replicaToApplyChanges, change mmaprototype.PendingRangeChange,
) error {
	// TODO(mma): We should be setting a timeout on the ctx here, in the case
	// where rebalancing takes a long time (stuck behind other snapshots).
	// See replicateQueue.processTimeoutFunc.
	// TODO(wenyihu6): store rebalancer uses RelocateRange
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
}

// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mma"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type multiMetricStoreRebalancer struct {
	// TODO: Spec out a minimal interface required from the Store, we should not
	// be using the Store directly.
	store     *Store
	metrics   StoreRebalancerMetrics
	as        *AllocatorSync
	allocator mma.Allocator
	st        *cluster.Settings
}

// TODO(kvoli): We should add an integration struct (see server.go gossip
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
func (m *multiMetricStoreRebalancer) start(ctx context.Context, stopper *stop.Stopper) {
	_ = stopper.RunAsyncTask(ctx, "multi-metric-store-rebalancer", func(ctx context.Context) {
		var timer timeutil.Timer
		defer timer.Stop()
		timer.Reset(jitteredInterval(allocator.LoadBasedRebalanceInterval.Get(&m.st.SV)))
		log.Infof(ctx, "starting multi-metric store rebalancer with mode=%v", LoadBasedRebalancingMode.Get(&m.st.SV))
		for {
			// Wait out the first tick before doing anything since the store is still
			// starting up and we might as well wait for some stats to accumulate.
			select {
			case <-stopper.ShouldQuiesce():
				return
			case <-timer.C:
				timer.Read = true
				timer.Reset(jitteredInterval(allocator.LoadBasedRebalanceInterval.Get(&m.st.SV)))
			}
			if LoadBasedRebalancingMode.Get(&m.st.SV) != LBRebalancingMultiMetric {
				continue
			}
			// Loop until no more changes are attempted.
			for {
				attemptedChanges := m.rebalance(ctx)
				if !attemptedChanges {
					break
				}
			}
		}
	})
}

func (m *multiMetricStoreRebalancer) rebalance(ctx context.Context) (attemptedChanges bool) {
	// We first construct a message containing the up-to-date store range
	// information before any allocator pass. This ensures up to date
	// allocator state.
	knownStores := m.allocator.KnownStores()
	storeLeaseholderMsg, numIgnoredRanges := m.store.MakeStoreLeaseholderMsg(knownStores)
	if numIgnoredRanges > 0 {
		log.Infof(ctx, "mma rebalancer: ignored %d ranges since the allocator does not know all stores",
			numIgnoredRanges)
	}
	changes := m.allocator.ComputeChanges(ctx, &storeLeaseholderMsg, mma.ChangeOptions{
		LocalStoreID: m.store.StoreID(),
	})

	for _, change := range changes {
		success := true
		repl := m.store.GetReplicaIfExists(change.RangeID)

		if repl == nil {
			log.VInfof(ctx, 1, "skipping pending change for r%d, replica not found", change.RangeID)
			success = false
			// TODO(sumeer): this is not clean, since we are bypassing the
			// AllocatorSync, but we do need to tell MMA that this change is not
			// going to be applied.
			m.allocator.AdjustPendingChangesDisposition(change.ChangeIDs(), success)
		} else {
			changeID := m.as.MMAPreApply(repl.RangeUsageInfo(), change)
			if change.IsTransferLease() {
				if err := repl.AdminTransferLease(
					ctx,
					change.LeaseTransferTarget(),
					false, /* bypassSafetyChecks */
				); err != nil {
					log.VInfof(ctx, 1, "failed to transfer lease for range %d: %v", change.RangeID, err)
					success = false
				}
				if success {
					m.metrics.LeaseTransferCount.Inc(1)
				}
			} else if change.IsChangeReplicas() {
				// TODO(kvoli): We should be setting a timeout on the ctx here, in
				// the case where rebalancing takes  a long time (stuck behind
				// other snapshots). See replicateQueue.processTimeoutFunc.
				if _, err := repl.changeReplicasImpl(
					ctx,
					repl.Desc(),
					kvserverpb.SnapshotRequest_REPLICATE_QUEUE,
					0,
					kvserverpb.ReasonRebalance,
					"todo: this is the rebalance detail for the range log",
					change.ReplicationChanges(),
				); err != nil {
					log.VInfof(ctx, 1, "failed to change replicas for r%d: %v", change.RangeID, err)
					success = false
				}
				if success {
					m.metrics.RangeRebalanceCount.Inc(1)
				}
			}
			m.as.PostApply(ctx, changeID, success, MMA)
		}
	}
	return len(changes) > 0
}

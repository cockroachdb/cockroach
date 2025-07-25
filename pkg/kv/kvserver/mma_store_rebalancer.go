// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

type mmaStore interface {
	MakeStoreLeaseholderMsg(context.Context, map[roachpb.StoreID]struct{}) (mmaprototype.StoreLeaseholderMsg, int)
	StoreID() roachpb.StoreID
	GetReplicaIfExists(id roachpb.RangeID) *Replica
}

var _ mmaStore = &Store{}

type mmaStoreRebalancer struct {
	store mmaStore
	mma   mmaprototype.Allocator
	st    *cluster.Settings
	sp    *storepool.StorePool
	// TODO(wenyihu6): add allocator sync
}

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
			timer.Reset(jitteredInterval(allocator.LoadBasedRebalanceInterval.Get(&m.st.SV)))
			if LoadBasedRebalancingMode.Get(&m.st.SV) != LBRebalancingMultiMetric {
				continue
			}

			for {
				attemptedChanges := m.rebalance(ctx)
				if !attemptedChanges {
					break
				}
			}
		}
	}
}

func (m *mmaStoreRebalancer) start(ctx context.Context, stopper *stop.Stopper) {
	_ = stopper.RunAsyncTask(ctx, "mma-store-rebalancer", func(ctx context.Context) {
		m.run(ctx, stopper)
	})
}

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

	for _, change := range changes {
		repl := m.store.GetReplicaIfExists(change.RangeID)
		if change.IsTransferLease() {
			if err := repl.AdminTransferLease(
				ctx,
				change.LeaseTransferTarget(),
				false, /* bypassSafetyChecks */
			); err != nil {
				log.VInfof(ctx, 1, "failed to transfer lease for range %d: %v", change.RangeID, err)
			}
		} else if change.IsChangeReplicas() {
			// TODO(mma): We should be setting a timeout on the ctx here, in the case
			// where rebalancing takes  a long time (stuck behind other snapshots).
			// See replicateQueue.processTimeoutFunc.
			// TODO(wenyihu6): store rebalancer uses RelocateRange
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
			}
		}
	}

	return len(changes) > 0
}

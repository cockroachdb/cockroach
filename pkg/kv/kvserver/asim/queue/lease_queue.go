// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package queue

import (
	"container/heap"
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototypehelpers"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/plan"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type leaseQueue struct {
	baseQueue
	plan.ReplicaPlanner
	storePool        storepool.AllocatorStorePool
	planner          plan.ReplicationPlanner
	clock            *hlc.Clock
	settings         *config.SimulationSettings
	as               *mmaprototypehelpers.AllocatorSync
	lastSyncChangeID mmaprototypehelpers.SyncChangeID
}

// NewLeaseQueue returns a new lease queue.
func NewLeaseQueue(
	storeID state.StoreID,
	nodeID state.NodeID,
	stateChanger state.Changer,
	settings *config.SimulationSettings,
	allocator allocatorimpl.Allocator,
	allocatorSync *mmaprototypehelpers.AllocatorSync,
	storePool storepool.AllocatorStorePool,
	start time.Time,
) RangeQueue {
	lq := leaseQueue{
		baseQueue: baseQueue{
			AmbientContext: log.MakeTestingAmbientCtxWithNewTracer(),
			priorityQueue:  priorityQueue{items: make([]*replicaItem, 0, 1)},
			storeID:        storeID,
			stateChanger:   stateChanger,
			next:           start,
		},
		settings:  settings,
		planner:   plan.NewLeasePlanner(allocator, storePool),
		storePool: storePool,
		clock:     storePool.Clock(),
		as:        allocatorSync,
	}
	lq.AddLogTag("lease", nil)
	lq.AddLogTag(fmt.Sprintf("n%ds%d", nodeID, storeID), "")
	return &lq
}

// MaybeAdd proposes a replica for inclusion into the LeaseQueue, if it
// meets the criteria it is enqueued. The criteria is currently if the
// allocator returns a lease transfer.
func (lq *leaseQueue) MaybeAdd(ctx context.Context, replica state.Replica, s state.State) bool {
	if !lq.settings.LeaseQueueEnabled {
		// Nothing to do, disabled.
		return false
	}

	repl := NewSimulatorReplica(replica, s)
	lq.AddLogTag("r", repl.repl.Descriptor())
	lq.AnnotateCtx(ctx)

	desc := repl.Desc()
	conf, err := repl.SpanConfig()
	if err != nil {
		log.Fatalf(ctx, "conf not found err=%v", err)
	}
	log.VEventf(ctx, 1, "maybe add replica=%s, config=%s", desc, conf)
	shouldPlanChange, priority := lq.planner.ShouldPlanChange(
		ctx,
		lq.clock.NowAsClockTimestamp(),
		repl,
		desc,
		conf,
		plan.PlannerOptions{
			CanTransferLease: true,
		},
	)

	if !shouldPlanChange {
		return false
	}

	heap.Push(
		lq,
		&replicaItem{
			rangeID:   roachpb.RangeID(replica.Range()),
			replicaID: replica.Descriptor().ReplicaID,
			priority:  priority,
		},
	)
	return true
}

// Tick processes updates in the LeaseQueue. Only one replica is processed at a
// time. Replicas in the queue are processed in order of priority, then in FIFO
// order on ties.
func (lq *leaseQueue) Tick(ctx context.Context, tick time.Time, s state.State) {
	lq.AddLogTag("tick", tick)
	ctx = lq.ResetAndAnnotateCtx(ctx)
	// TODO(wenyihu6): it is unclear why next tick is forwarded to last tick
	// here (see #149904 for more details).
	if lq.lastTick.After(lq.next) {
		lq.next = lq.lastTick
	}

	if !tick.Before(lq.next) && lq.lastSyncChangeID.IsValid() {
		lq.as.PostApply(ctx, lq.lastSyncChangeID, true /* success */)
		lq.lastSyncChangeID = mmaprototypehelpers.InvalidSyncChangeID
	}

	for !tick.Before(lq.next) && lq.priorityQueue.Len() != 0 {
		item := heap.Pop(lq).(*replicaItem)
		if item == nil {
			return
		}

		rng, ok := s.Range(state.RangeID(item.rangeID))
		if !ok {
			panic("range missing which is unexpected")
		}

		replica, ok := rng.Replica(lq.storeID)
		if !ok {
			// The replica may have been removed from the store by another change
			// (store rebalancer, replicate queue). In which case, we just ignore it
			// and proceed.
			continue
		}

		repl := NewSimulatorReplica(replica, s)
		desc := repl.Desc()
		conf, err := repl.SpanConfig()
		if err != nil {
			panic(err)
		}

		// The replica needs to hold a valid lease.
		if !repl.OwnsValidLease(ctx, hlc.ClockTimestamp{}) {
			continue
		}

		change, err := lq.planner.PlanOneChange(ctx, repl, desc, conf, plan.PlannerOptions{
			CanTransferLease: true,
		})
		if err != nil {
			log.Errorf(ctx, "error planning change %s", err.Error())
			continue
		}

		lq.next, lq.lastSyncChangeID = pushReplicateChange(
			ctx, change, repl, tick, lq.settings.ReplicaChangeDelayFn(), lq.baseQueue.stateChanger, lq.as, "lease queue")
	}

	lq.lastTick = tick
}

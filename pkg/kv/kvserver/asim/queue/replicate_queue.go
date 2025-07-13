// Copyright 2022 The Cockroach Authors.
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

type replicateQueue struct {
	baseQueue
	planner          plan.ReplicationPlanner
	clock            *hlc.Clock
	settings         *config.SimulationSettings
	as               *mmaprototypehelpers.AllocatorSync
	lastSyncChangeID mmaprototypehelpers.SyncChangeID
}

// NewReplicateQueue returns a new replicate queue.
func NewReplicateQueue(
	storeID state.StoreID,
	nodeID state.NodeID,
	stateChanger state.Changer,
	settings *config.SimulationSettings,
	allocator allocatorimpl.Allocator,
	allocatorSync *mmaprototypehelpers.AllocatorSync,
	storePool storepool.AllocatorStorePool,
	start time.Time,
) RangeQueue {
	rq := replicateQueue{
		baseQueue: baseQueue{
			AmbientContext: log.MakeTestingAmbientCtxWithNewTracer(),
			priorityQueue:  priorityQueue{items: make([]*replicaItem, 0, 1)},
			storeID:        storeID,
			stateChanger:   stateChanger,
			next:           start,
		},
		settings: settings,
		planner: plan.NewReplicaPlanner(
			allocator, storePool, plan.ReplicaPlannerTestingKnobs{}),
		clock: storePool.Clock(),
		as:    allocatorSync,
	}
	rq.AddLogTag("replica", nil)
	rq.AddLogTag(fmt.Sprintf("n%ds%d", nodeID, storeID), "")
	return &rq
}

// MaybeAdd proposes a replica for inclusion into the ReplicateQueue, if it
// meets the criteria it is enqueued. The criteria is currently if the
// allocator returns a non-noop, then the replica is added.
func (rq *replicateQueue) MaybeAdd(ctx context.Context, replica state.Replica, s state.State) bool {
	if !rq.settings.ReplicateQueueEnabled {
		// Nothing to do, disabled.
		return false
	}

	repl := NewSimulatorReplica(replica, s)
	rq.AddLogTag("r", repl.Desc().RangeID)
	rq.AnnotateCtx(ctx)

	desc := repl.Desc()
	conf, err := repl.SpanConfig()
	if err != nil {
		log.Fatalf(ctx, "conf not found err=%v", err)
	}
	log.VEventf(ctx, 1, "maybe add replica=%s, config=%s", desc, conf)

	shouldPlanChange, priority := rq.planner.ShouldPlanChange(
		ctx,
		rq.clock.NowAsClockTimestamp(),
		repl,
		desc,
		conf,
		plan.PlannerOptions{},
	)

	if !shouldPlanChange {
		return false
	}

	heap.Push(
		rq,
		&replicaItem{
			rangeID:   roachpb.RangeID(replica.Range()),
			replicaID: replica.Descriptor().ReplicaID,
			priority:  priority,
		},
	)
	return true
}

// Tick processes updates in the ReplicateQueue. Only one replica is
// processed at a time and the duration taken to process a replica depends
// on the action taken. Replicas in the queue are processed in order of
// priority, then in FIFO order on ties. The Tick function currently only
// supports processing ConsiderRebalance actions on replicas.
func (rq *replicateQueue) Tick(ctx context.Context, tick time.Time, s state.State) {
	rq.AddLogTag("tick", tick)
	ctx = rq.ResetAndAnnotateCtx(ctx)
	// TODO(wenyihu6): it is unclear why next tick is forwarded to last tick
	// here (see #149904 for more details).
	if rq.lastTick.After(rq.next) {
		rq.next = rq.lastTick
	}

	if !tick.Before(rq.next) && rq.lastSyncChangeID.IsValid() {
		rq.as.PostApply(ctx, rq.lastSyncChangeID, true /* success */)
		rq.lastSyncChangeID = mmaprototypehelpers.InvalidSyncChangeID
	}

	for !tick.Before(rq.next) && rq.priorityQueue.Len() != 0 {
		item := heap.Pop(rq).(*replicaItem)
		if item == nil {
			return
		}

		rng, ok := s.Range(state.RangeID(item.rangeID))
		if !ok {
			panic("range missing which is unexpected")
		}

		replica, ok := rng.Replica(rq.storeID)
		if !ok {
			// The replica may have been removed from the store by another change
			// (store rebalancer). In which case, we just ignore it and proceed.
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
		change, err := rq.planner.PlanOneChange(ctx, repl, desc, conf, plan.PlannerOptions{})
		if err != nil {
			log.Errorf(ctx, "error planning change %s", err.Error())
			continue
		}

		rq.next, rq.lastSyncChangeID = pushReplicateChange(
			ctx, change, repl, tick, rq.settings.ReplicaChangeDelayFn(), rq.baseQueue.stateChanger, rq.as, "replicate queue")
	}

	rq.lastTick = tick
}

func pushReplicateChange(
	ctx context.Context,
	change plan.ReplicateChange,
	repl *SimulatorReplica,
	tick time.Time,
	delayFn func(int64, bool) time.Duration,
	stateChanger state.Changer,
	as *mmaprototypehelpers.AllocatorSync,
	queueName string,
) (time.Time, mmaprototypehelpers.SyncChangeID) {
	var stateChange state.Change
	var changeID mmaprototypehelpers.SyncChangeID
	next := tick
	switch op := change.Op.(type) {
	case plan.AllocationNoop:
		// Nothing to do.
		return next, mmaprototypehelpers.InvalidSyncChangeID
	case plan.AllocationFinalizeAtomicReplicationOp:
		panic("unimplemented finalize atomic replication op")
	case plan.AllocationTransferLeaseOp:
		if as != nil {
			// as may be nil in some tests.
			changeID = as.NonMMAPreTransferLease(
				ctx,
				repl.Desc(),
				repl.RangeUsageInfo(),
				op.Source,
				op.Target,
				mmaprototypehelpers.ReplicateQueue,
			)
		}
		stateChange = &state.LeaseTransferChange{
			RangeID:        state.RangeID(change.Replica.GetRangeID()),
			TransferTarget: state.StoreID(op.Target.StoreID),
			Author:         state.StoreID(op.Source.StoreID),
			Wait:           delayFn(repl.rng.Size(), false /* add */),
		}
	case plan.AllocationChangeReplicasOp:
		if as != nil {
			// as may be nil in some tests.
			changeID = as.NonMMAPreChangeReplicas(
				ctx,
				repl.Desc(),
				repl.RangeUsageInfo(),
				op.Chgs,
				repl.StoreID(), /* leaseholder */
			)
		}
		log.VEventf(ctx, 1, "pushing state change for range=%s, details=%s changeIDs=%v coming from %s", repl.rng, op.Details, changeID, queueName)
		stateChange = &state.ReplicaChange{
			RangeID: state.RangeID(change.Replica.GetRangeID()),
			Changes: op.Chgs,
			Author:  state.StoreID(op.LeaseholderStore),
			Wait:    delayFn(repl.rng.Size(), true /* add */),
		}
	default:
		panic(fmt.Sprintf("Unknown operation %+v, unable to create state change", op))
	}

	if completeAt, ok := stateChanger.Push(tick, stateChange); ok {
		log.VEventf(ctx, 1, "pushing state change succeeded, complete at %s (cur %s)", completeAt, tick)
		next = completeAt
	} else {
		log.VEventf(ctx, 1, "pushing state change failed")
		as.PostApply(ctx, changeID, false /* success */)
		changeID = mmaprototypehelpers.InvalidSyncChangeID
	}
	return next, changeID
}

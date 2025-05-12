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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mma"
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
	planner       plan.ReplicationPlanner
	clock         *hlc.Clock
	settings      *config.SimulationSettings
	lastChangeIDs []mma.ChangeID
	as            *kvserver.AllocatorSync
}

// NewReplicateQueue returns a new replicate queue.
func NewReplicateQueue(
	storeID state.StoreID,
	stateChanger state.Changer,
	settings *config.SimulationSettings,
	allocator allocatorimpl.Allocator,
	allocatorSync *kvserver.AllocatorSync,
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
	rq.AddLogTag("r", repl.repl.Descriptor())
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
	if rq.lastTick.After(rq.next) {
		rq.next = rq.lastTick
	}

	if !tick.Before(rq.next) && rq.lastChangeIDs != nil {
		rq.as.PostApply(rq.lastChangeIDs, true /* success */)
		rq.lastChangeIDs = nil
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

		rq.lastChangeIDs = pushReplicateChange(
			ctx, change, repl, tick, rq.settings.ReplicaChangeDelayFn(), rq.baseQueue, rq.as)
	}

	rq.lastTick = tick
}

func pushReplicateChange(
	ctx context.Context,
	change plan.ReplicateChange,
	repl *SimulatorReplica,
	tick time.Time,
	delayFn func(int64, bool) time.Duration,
	queue baseQueue,
	as *kvserver.AllocatorSync,
) []mma.ChangeID {
	var stateChange state.Change
	var changeIDs []mma.ChangeID
	switch op := change.Op.(type) {
	case plan.AllocationNoop:
		// Nothing to do.
		return nil
	case plan.AllocationFinalizeAtomicReplicationOp:
		panic("unimplemented finalize atomic replication op")
	case plan.AllocationTransferLeaseOp:
		if as != nil {
			// as may be nil in some tests.
			changeIDs = as.NonMMAPreTransferLease(
				repl.Desc(),
				repl.RangeUsageInfo(),
				op.Source,
				op.Target,
			)
		}
		stateChange = &state.LeaseTransferChange{
			RangeID:        state.RangeID(change.Replica.GetRangeID()),
			TransferTarget: state.StoreID(op.Target.StoreID),
			Author:         state.StoreID(op.Source.StoreID),
			// TODO(mma): Should this be add? I don't think so since it will assume
			// it takes as long as adding a replica. Will need to regenerate the
			// tests and check the output when changing this.
			Wait: delayFn(repl.rng.Size(), false /* add */),
		}
	case plan.AllocationChangeReplicasOp:
		if as != nil {
			// as may be nil in some tests.
			changeIDs = as.NonMMAPreChangeReplicas(
				repl.Desc(),
				repl.RangeUsageInfo(),
				op.Chgs,
			)
		}
		log.VEventf(ctx, 1, "pushing state change for range=%s, details=%s", repl.rng, op.Details)
		stateChange = &state.ReplicaChange{
			RangeID: state.RangeID(change.Replica.GetRangeID()),
			Changes: op.Chgs,
			Author:  state.StoreID(op.LeaseholderStore),
			Wait:    delayFn(repl.rng.Size(), true /* add */),
		}
	default:
		panic(fmt.Sprintf("Unknown operation %+v, unable to create state change", op))
	}

	if completeAt, ok := queue.stateChanger.Push(tick, stateChange); ok {
		queue.next = completeAt
		log.VEventf(ctx, 1, "pushing state change succeeded, complete at %s (cur %s)", completeAt, tick)
	} else {
		log.VEventf(ctx, 1, "pushing state change failed")
		as.PostApply(changeIDs, false /* success */)
		changeIDs = nil
	}
	return changeIDs
}

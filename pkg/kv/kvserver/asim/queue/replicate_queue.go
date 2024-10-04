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
	planner  plan.ReplicationPlanner
	clock    *hlc.Clock
	settings *config.SimulationSettings
}

// NewReplicateQueue returns a new replicate queue.
func NewReplicateQueue(
	storeID state.StoreID,
	stateChanger state.Changer,
	settings *config.SimulationSettings,
	allocator allocatorimpl.Allocator,
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
	}
	rq.AddLogTag("rq", nil)
	return &rq
}

func simCanTransferleaseFrom(
	ctx context.Context, repl plan.LeaseCheckReplica, conf *roachpb.SpanConfig,
) bool {
	return true
}

// MaybeAdd proposes a replica for inclusion into the ReplicateQueue, if it
// meets the criteria it is enqueued. The criteria is currently if the
// allocator returns a non-noop, then the replica is added.
func (rq *replicateQueue) MaybeAdd(ctx context.Context, replica state.Replica, s state.State) bool {
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
		simCanTransferleaseFrom,
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
		change, err := rq.planner.PlanOneChange(ctx, repl, desc, conf, simCanTransferleaseFrom, false /* scatter */)
		if err != nil {
			log.Errorf(ctx, "error planning change %s", err.Error())
			continue
		}

		log.VEventf(ctx, 1, "conf=%+v", rng.SpanConfig())

		rq.applyChange(ctx, change, rng, tick)
	}

	rq.lastTick = tick
}

// applyChange applies a range allocation change. It is responsible only for
// application and returns an error if unsuccessful.
//
// TODO(kvoli): Currently applyChange is only called by the replicate queue. It
// is desirable to funnel all allocation changes via one function. Move this
// application phase onto a separate struct that will be used by both the
// replicate queue and the store rebalancer and specifically for operations
// rather than changes.
func (rq *replicateQueue) applyChange(
	ctx context.Context, change plan.ReplicateChange, rng state.Range, tick time.Time,
) {
	var stateChange state.Change
	switch op := change.Op.(type) {
	case plan.AllocationNoop:
		// Nothing to do.
		return
	case plan.AllocationFinalizeAtomicReplicationOp:
		panic("unimplemented finalize atomic replication op")
	case plan.AllocationTransferLeaseOp:
		stateChange = &state.LeaseTransferChange{
			RangeID:        state.RangeID(change.Replica.GetRangeID()),
			TransferTarget: state.StoreID(op.Target),
			Author:         rq.storeID,
			Wait:           rq.settings.ReplicaChangeDelayFn()(0, false),
		}
	case plan.AllocationChangeReplicasOp:
		log.VEventf(ctx, 1, "pushing state change for range=%s, details=%s", rng, op.Details)
		stateChange = &state.ReplicaChange{
			RangeID: state.RangeID(change.Replica.GetRangeID()),
			Changes: op.Chgs,
			Author:  rq.storeID,
			Wait:    rq.settings.ReplicaChangeDelayFn()(rng.Size(), true),
		}
	default:
		panic(fmt.Sprintf("Unknown operation %+v, unable to apply replicate queue change", op))
	}

	if completeAt, ok := rq.stateChanger.Push(tick, stateChange); ok {
		rq.next = completeAt
		log.VEventf(ctx, 1, "pushing state change succeeded, complete at %s (cur %s)", completeAt, tick)
	} else {
		log.VEventf(ctx, 1, "pushing state change failed")
	}
}

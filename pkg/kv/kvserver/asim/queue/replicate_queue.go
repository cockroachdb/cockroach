// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package queue

import (
	"container/heap"
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type replicateQueue struct {
	baseQueue
	allocator allocatorimpl.Allocator
	storePool storepool.AllocatorStorePool
	delay     func(rangeSize int64, add bool) time.Duration
}

// NewReplicateQueue returns a new replicate queue.
func NewReplicateQueue(
	storeID state.StoreID,
	stateChanger state.Changer,
	delay func(rangeSize int64, add bool) time.Duration,
	allocator allocatorimpl.Allocator,
	storePool storepool.AllocatorStorePool,
	start time.Time,
) RangeQueue {
	return &replicateQueue{
		baseQueue: baseQueue{
			priorityQueue: priorityQueue{items: make([]*replicaItem, 0, 1)},
			storeID:       storeID,
			stateChanger:  stateChanger,
			next:          start,
		},
		delay:     delay,
		allocator: allocator,
		storePool: storePool,
	}
}

// MaybeAdd proposes a replica for inclusion into the ReplicateQueue, if it
// meets the criteria it is enqueued. The criteria is currently if the
// allocator returns a non-noop, then the replica is added.
func (rq *replicateQueue) MaybeAdd(
	ctx context.Context, replica state.Replica, state state.State,
) bool {
	rng, ok := state.Range(replica.Range())
	if !ok {
		return false
	}

	action, priority := rq.allocator.ComputeAction(ctx, rq.storePool, rng.SpanConfig(), rng.Descriptor())
	if action == allocatorimpl.AllocatorNoop {
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
// TODO(kvoli,lidorcarmel): Support taking additional actions, beyond consider
// rebalance.
func (rq *replicateQueue) Tick(ctx context.Context, tick time.Time, s state.State) {
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
			return
		}

		action, _ := rq.allocator.ComputeAction(ctx, rq.storePool, rng.SpanConfig(), rng.Descriptor())

		switch action {
		case allocatorimpl.AllocatorConsiderRebalance:
			rq.considerRebalance(ctx, rq.next, rng, s)
		case allocatorimpl.AllocatorNoop:
			return
		default:
			log.Infof(ctx, "s%d: allocator action %s for range %s is unsupported by the simulator "+
				"replicate queue, ignoring.", rq.storeID, action, rng)
			return
		}
	}

	rq.lastTick = tick
}

// considerRebalance simulates the logic of the replicate queue when given a
// considerRebalance action. It will first ask the allocator for add and remove
// targets for a range. It will then enqueue the replica change into the state
// changer and update the time to process the next replica, with the completion
// time returned.
func (rq *replicateQueue) considerRebalance(
	ctx context.Context, tick time.Time, rng state.Range, s state.State,
) {
	add, remove, _, ok := rq.allocator.RebalanceVoter(
		ctx,
		rq.storePool,
		rng.SpanConfig(),
		nil, /* raftStatus */
		rng.Descriptor().Replicas().VoterDescriptors(),
		rng.Descriptor().Replicas().NonVoterDescriptors(),
		s.ReplicaLoad(rng.RangeID(), rq.storeID).Load(),
		storepool.StoreFilterNone,
		rq.allocator.ScorerOptions(ctx),
	)

	// We were unable to find a rebalance target for the range.
	if !ok {
		return
	}

	// Enqueue the change to be processed and update when the next replica
	// processing can occur with the completion time of the change.
	// 	NB: This limits concurrency to at most one change at a time per
	// 	ReplicateQueue.
	change := state.ReplicaChange{
		RangeID: state.RangeID(rng.Descriptor().RangeID),
		Add:     state.StoreID(add.StoreID),
		Remove:  state.StoreID(remove.StoreID),
		Wait:    rq.delay(rng.Size(), true),
		Author:  rq.storeID,
	}
	if completeAt, ok := rq.stateChanger.Push(tick, &change); ok {
		rq.next = completeAt
	}
}

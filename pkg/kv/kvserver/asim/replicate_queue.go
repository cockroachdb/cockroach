// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package asim

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

// ReplicateQueue presents an interface to interact with a single consumer
// queue, which processes replicas for replication updates if they meet the
// criteria.
type ReplicateQueue interface {
	// MaybeAdd proposes a replica for inclusion into the ReplicateQueue, if it
	// meets the criteria it is enqueued.
	MaybeAdd(ctx context.Context, repl state.Replica, state state.State) bool
	// Tick proceses updates in the ReplicateQueue. Only one replica is
	// processed at a time and the duration taken to process a replica depends
	// on the action taken. Replicas in the queue are processed in order of
	// priority, then in FIFO order on ties.
	Tick(ctx context.Context, tick time.Time, state state.State)
}

// replicaItem represents an item in the replica queue.
type replicaItem struct {
	rangeID   roachpb.RangeID
	replicaID roachpb.ReplicaID
	// Enforce FIFO order for equal priorities.
	seq int
	// Fields used when a replicaItem is enqueued in a priority queue.
	priority float64
	// The index of the item in the heap, maintained by the heap.Interface
	// methods.
	index int
}

type priorityQueue struct {
	seqGen int
	items  []*replicaItem
}

// Len is part of the container.Heap interface.
func (pq priorityQueue) Len() int { return len(pq.items) }

// Less is part of the container.Heap interface.
func (pq priorityQueue) Less(i, j int) bool {
	a, b := pq.items[i], pq.items[j]
	if a.priority == b.priority {
		// When priorities are equal, we want the lower sequence number to show
		// up first (FIFO).
		return a.seq < b.seq
	}
	// We want Pop to give us the highest, not lowest, priority so we use
	// greater than here.
	return a.priority > b.priority
}

// Swap is part of the container.Heap interface.
func (pq priorityQueue) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].index, pq.items[j].index = i, j
}

// Push is part of the container.Heap interface.
func (pq *priorityQueue) Push(x interface{}) {
	n := len(pq.items)
	item := x.(*replicaItem)
	item.index = n
	pq.seqGen++
	item.seq = pq.seqGen
	pq.items = append(pq.items, item)
}

// Pop is part of the container.Heap interface.
func (pq *priorityQueue) Pop() interface{} {
	old := pq.items
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	old[n-1] = nil  // for gc
	pq.items = old[0 : n-1]
	return item
}

// replicateQueue is an implementation of the ReplicateQueue interface.
type replicateQueue struct {
	priorityQueue
	allocator    allocatorimpl.Allocator
	storeID      state.StoreID
	stateChanger state.Changer
	next         time.Time
	// TOOD(kvoli): Delay is used as a fixed delay we pass for all replica
	// changes. In the future we should pass in a delay function which returns
	// a duration given the size of the range, snapshot rates and overload.
	delay time.Duration
}

// NewReplicateQueue returns a new replicate queue.
func NewReplicateQueue(
	storeID state.StoreID,
	stateChanger state.Changer,
	delay time.Duration,
	allocator allocatorimpl.Allocator,
) ReplicateQueue {
	return &replicateQueue{
		priorityQueue: priorityQueue{items: make([]*replicaItem, 0, 1)},
		allocator:     allocator,
		storeID:       storeID,
		stateChanger:  stateChanger,
		delay:         delay,
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

	action, priority := rq.allocator.ComputeAction(ctx, rng.SpanConfig(), rng.Descriptor())
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

// Tick proceses updates in the ReplicateQueue. Only one replica is
// processed at a time and the duration taken to process a replica depends
// on the action taken. Replicas in the queue are processed in order of
// priority, then in FIFO order on ties. The Tick function currently only
// supports processing ConsiderRebalance actions on replicas.
// TODO(kvoli,lidorcarmel): Support taking additional actions, beyond consider rebalance.
func (rq *replicateQueue) Tick(ctx context.Context, tick time.Time, s state.State) {
	if tick.Before(rq.next) || rq.priorityQueue.Len() == 0 {
		return
	}

	item := heap.Pop(rq).(*replicaItem)
	if item == nil {
		return
	}

	rng, ok := s.Range(state.RangeID(item.rangeID))
	if !ok {
		return
	}

	action, _ := rq.allocator.ComputeAction(ctx, rng.SpanConfig(), rng.Descriptor())

	switch action {
	case allocatorimpl.AllocatorConsiderRebalance:
		rq.considerRebalance(ctx, tick, rng, s)
	case allocatorimpl.AllocatorNoop:
		return
	default:
		log.Infof(ctx, "allocator action %s for range %s is unsupported by the simulator "+
			"replicate queue, ignoring.", action, rng)
		return
	}
}

// considerRebalance simulates the logic of the replicate queue when given a
// considerRebalance action. It will first ask the allocator for add and remove
// targets for a range. It will then enqueue the replica change into the state
// changer and update the time to process the next replica, with the completion
// time returned.
func (rq *replicateQueue) considerRebalance(
	ctx context.Context, tick time.Time, rng state.Range, s state.State,
) {
	add, remove, details, ok := rq.allocator.RebalanceVoter(
		ctx,
		rng.SpanConfig(),
		nil, /* raftStatus */
		rng.Descriptor().Replicas().VoterDescriptors(),
		rng.Descriptor().Replicas().NonVoterDescriptors(),
		s.UsageInfo(rng.RangeID()),
		storepool.StoreFilterNone,
		rq.allocator.ScorerOptions(ctx),
	)

	// We were unable to find a rebalance target for the range.
	if !ok {
		log.Warningf(ctx, "%s", details)
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
		Wait:    rq.delay,
	}
	if completeAt, ok := rq.stateChanger.Push(tick, &change); ok {
		rq.next = completeAt
	}
}

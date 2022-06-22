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

// RangeQueue presents an interface to interact with a single consumer
// queue, which processes replicas for replication updates if they meet the
// criteria.
// TODO(kvoli): When replicas are enqueued into multiple queues, and they are
// processed in the same Tick() - both pushing state updates to the state
// changer, then only one will be successful, as there may be at most one
// pending change per range. The split queue currently goes first and therefore
// has priority over the replication queue. We should implement a wait or retry
// next tick mechanism to address this, to match the real code.
type RangeQueue interface {
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

// baseQueue is an implementation of the ReplicateQueue interface.
type baseQueue struct {
	priorityQueue
	storeID        state.StoreID
	stateChanger   state.Changer
	next, lastTick time.Time
}

type replicateQueue struct {
	baseQueue
	allocator allocatorimpl.Allocator
	delay     func(rangeSize int64, add bool) time.Duration
}

type splitQueue struct {
	baseQueue
	splitThreshold int64
	delay          func() time.Duration
}

// NewReplicateQueue returns a new replicate queue.
func NewReplicateQueue(
	storeID state.StoreID,
	stateChanger state.Changer,
	delay func(rangeSize int64, add bool) time.Duration,
	allocator allocatorimpl.Allocator,
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

		action, _ := rq.allocator.ComputeAction(ctx, rng.SpanConfig(), rng.Descriptor())

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
	}
	if completeAt, ok := rq.stateChanger.Push(tick, &change); ok {
		rq.next = completeAt
	}
}

// NewSplitQueue returns a new split queue, implementing the range queue
// interface.
func NewSplitQueue(
	storeID state.StoreID,
	stateChanger state.Changer,
	delay func() time.Duration,
	splitThreshold int64,
	start time.Time,
) RangeQueue {
	return &splitQueue{
		baseQueue: baseQueue{
			priorityQueue: priorityQueue{items: make([]*replicaItem, 0, 1)},
			storeID:       storeID,
			stateChanger:  stateChanger,
			next:          start,
		},
		delay:          delay,
		splitThreshold: splitThreshold,
	}
}

// MaybeAdd proposes a range for being split. If it meets the criteria it is
// enqueued.
func (sq *splitQueue) MaybeAdd(ctx context.Context, replica state.Replica, state state.State) bool {
	priority := sq.shouldSplit(sq.lastTick, replica.Range(), state)
	if priority < 1 {
		return false
	}

	rng, _ := state.Range(replica.Range())

	heap.Push(sq, &replicaItem{
		rangeID:   roachpb.RangeID(replica.Range()),
		replicaID: replica.Descriptor().ReplicaID,
		priority:  float64(rng.Size()) / float64(sq.splitThreshold),
	})
	return true
}

// Tick processes updates in the split queue. Only one range is processed at a
// time and the duration taken to process a replica depends on the action
// taken. Replicas in the queue are processed in order of priority, then in
// FIFO order on ties. The tick currently only considers size based range
// splitting.
func (sq *splitQueue) Tick(ctx context.Context, tick time.Time, s state.State) {
	if sq.lastTick.After(sq.next) {
		sq.next = sq.lastTick
	}

	for !tick.Before(sq.next) && sq.priorityQueue.Len() != 0 {
		item := heap.Pop(sq).(*replicaItem)
		if item == nil {
			return
		}

		rng, ok := s.Range(state.RangeID(item.rangeID))
		if !ok {
			return
		}

		// Check whether the range satisfies the split criteria, since it may have
		// changed since it was enqueued.
		if sq.shouldSplit(tick, rng.RangeID(), s) < 1 {
			return
		}

		splitKey, ok := sq.findKeySpanSplit(tick, s, rng.RangeID())
		if !ok {
			return
		}

		change := state.RangeSplitChange{
			RangeID:     state.RangeID(rng.Descriptor().RangeID),
			Leaseholder: sq.storeID,
			SplitKey:    splitKey,
			Wait:        sq.delay(),
		}

		if completeAt, ok := sq.stateChanger.Push(sq.next, &change); ok {
			sq.next = completeAt
		}
	}

	sq.lastTick = tick
}

// shouldSplit returns whether a range should be split into two. When the
// floating point number returned is greater than or equal to 1, it should be
// split with that priority, else it shouldn't.
func (sq *splitQueue) shouldSplit(tick time.Time, rangeID state.RangeID, s state.State) float64 {
	rng, ok := s.Range(rangeID)
	if !ok {
		return 0
	}

	// Check whether we should split this range based on load.
	if _, ok := s.LoadSplitterFor(sq.storeID).SplitKey(tick, rangeID); ok {
		return 2.0
	}

	// Check whether we should split this range based on size.
	overfullBytesThreshold := float64(rng.Size()) / float64(sq.splitThreshold)

	return overfullBytesThreshold
}

// findKeySpanSplit returns a key that may be used for splitting a range into
// two. It will return the key that divides the range into an equal number of
// keys on the lhs and rhs.
func (sq *splitQueue) findKeySpanSplit(
	tick time.Time, s state.State, rangeID state.RangeID,
) (state.Key, bool) {
	// Try and use the split key suggested by the load based splitter, if one
	// exists.
	if loadSplitKey, ok := s.LoadSplitterFor(sq.storeID).SplitKey(tick, rangeID); ok {
		return loadSplitKey, true
	}

	start, end, ok := s.RangeSpan(rangeID)
	if !ok {
		return start, false
	}

	delta := end - start
	// The range is not splittable, it contains only a single key already. e.g.
	// [0, 1) is not splittable, whilst [0, 2) may be split into [0,1) [1,2).
	if delta < 2 {
		return start, false
	}

	splitKey := start + delta/2
	return splitKey, true
}

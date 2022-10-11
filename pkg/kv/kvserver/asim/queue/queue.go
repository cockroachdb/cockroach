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
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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
	settings       *config.SimulationSettings
	storeID        state.StoreID
	stateChanger   state.Changer
	next, lastTick time.Time
}

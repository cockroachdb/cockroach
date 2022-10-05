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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type splitQueue struct {
	baseQueue
	splitThreshold int64
	delay          func() time.Duration
}

// NewSplitQueue returns a new split queue, implementing the range queue
// interface.
func NewSplitQueue(
	storeID state.StoreID,
	stateChanger state.Changer,
	delay func() time.Duration,
	splitThreshold int64,
	start time.Time,
	settings *config.SimulationSettings,
) RangeQueue {
	return &splitQueue{
		baseQueue: baseQueue{
			priorityQueue: priorityQueue{items: make([]*replicaItem, 0, 1)},
			storeID:       storeID,
			stateChanger:  stateChanger,
			next:          start,
			settings:      settings,
		},
		delay:          delay,
		splitThreshold: splitThreshold,
	}
}

// MaybeAdd proposes a range for being split. If it meets the criteria it is
// enqueued.
func (sq *splitQueue) MaybeAdd(ctx context.Context, replica state.Replica, state state.State) bool {
	if !sq.settings.SplitQueueEnabled {
		return false
	}

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
	if !sq.settings.SplitQueueEnabled {
		return
	}

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
			Author:      sq.storeID,
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

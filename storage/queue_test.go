// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"container/heap"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
)

// testQueueImpl implements queueImpl with a closure for shouldQueue.
type testQueueImpl struct {
	shouldQueueFn func(proto.Timestamp, *Range) (bool, float64)
	processed     int32
	duration      time.Duration
}

func (tq *testQueueImpl) shouldQueue(now proto.Timestamp, r *Range) (bool, float64) {
	return tq.shouldQueueFn(now, r)
}

func (tq *testQueueImpl) process(now proto.Timestamp, r *Range) error {
	atomic.AddInt32(&tq.processed, 1)
	return nil
}

func (tq *testQueueImpl) timer() time.Duration {
	if tq.duration != 0 {
		return tq.duration
	}
	return time.Millisecond
}

// TestQueuePriorityQueue verifies priority queue implementation.
func TestQueuePriorityQueue(t *testing.T) {
	// Create a priority queue, put the items in it, and
	// establish the priority queue (heap) invariants.
	const count = 3
	expRanges := make([]*Range, count+1)
	pq := make(priorityQueue, count)
	for i := 0; i < count; {
		pq[i] = &rangeItem{
			value:    &Range{},
			priority: float64(i),
			index:    i,
		}
		expRanges[3-i] = pq[i].value
		i++
	}
	heap.Init(&pq)

	// Insert a new item and then modify its priority.
	item := &rangeItem{
		value:    &Range{},
		priority: 1.0,
	}
	heap.Push(&pq, item)
	pq.update(item, 4.0)
	expRanges[0] = item.value

	// Take the items out; they should arrive in decreasing priority order.
	for i := 0; pq.Len() > 0; i++ {
		item := heap.Pop(&pq).(*rangeItem)
		if item.value != expRanges[i] {
			t.Errorf("%d: unexpected range with priority %f", i, item.priority)
		}
	}
}

// TestBaseQueueAddUpdateAndRemove verifies basic operation with base
// queue including adding ranges which both should and shouldn't be
// queued, updating an existing range, and removing a range.
func TestBaseQueueAddUpdateAndRemove(t *testing.T) {
	r1 := &Range{}
	r1.SetDesc(&proto.RangeDescriptor{RaftID: 1})
	r2 := &Range{}
	r2.SetDesc(&proto.RangeDescriptor{RaftID: 2})
	shouldAddMap := map[*Range]bool{
		r1: true,
		r2: true,
	}
	priorityMap := map[*Range]float64{
		r1: 1.0,
		r2: 2.0,
	}
	testQueue := &testQueueImpl{
		shouldQueueFn: func(now proto.Timestamp, r *Range) (shouldQueue bool, priority float64) {
			return shouldAddMap[r], priorityMap[r]
		},
	}
	bq := newBaseQueue("test", testQueue, 2)
	bq.MaybeAdd(r1, proto.ZeroTimestamp)
	bq.MaybeAdd(r2, proto.ZeroTimestamp)
	if bq.Length() != 2 {
		t.Fatalf("expected length 2; got %d", bq.Length())
	}
	if bq.pop() != r2 {
		t.Error("expected r2")
	}
	if bq.pop() != r1 {
		t.Error("expected r1")
	}
	if r := bq.pop(); r != nil {
		t.Errorf("expected empty queue; got %v", r)
	}

	// Add again, but this time r2 shouldn't add.
	shouldAddMap[r2] = false
	bq.MaybeAdd(r1, proto.ZeroTimestamp)
	bq.MaybeAdd(r2, proto.ZeroTimestamp)
	if bq.Length() != 1 {
		t.Errorf("expected length 1; got %d", bq.Length())
	}

	// Try adding same range twice.
	bq.MaybeAdd(r1, proto.ZeroTimestamp)
	if bq.Length() != 1 {
		t.Errorf("expected length 1; got %d", bq.Length())
	}

	// Re-add r2 and update priority of r1.
	shouldAddMap[r2] = true
	priorityMap[r1] = 3.0
	bq.MaybeAdd(r1, proto.ZeroTimestamp)
	bq.MaybeAdd(r2, proto.ZeroTimestamp)
	if bq.Length() != 2 {
		t.Fatalf("expected length 2; got %d", bq.Length())
	}
	if bq.pop() != r1 {
		t.Error("expected r1")
	}
	if bq.pop() != r2 {
		t.Error("expected r2")
	}
	if r := bq.pop(); r != nil {
		t.Errorf("expected empty queue; got %v", r)
	}

	// Set !shouldAdd for r2 and add it; this has effect of removing it.
	bq.MaybeAdd(r1, proto.ZeroTimestamp)
	bq.MaybeAdd(r2, proto.ZeroTimestamp)
	shouldAddMap[r2] = false
	bq.MaybeAdd(r2, proto.ZeroTimestamp)
	if bq.Length() != 1 {
		t.Fatalf("expected length 1; got %d", bq.Length())
	}
	if bq.pop() != r1 {
		t.Errorf("expected r1")
	}
}

// TestBaseQueueProcess verifies that items from the queue are
// processed according to the timer function.
func TestBaseQueueProcess(t *testing.T) {
	r1 := &Range{}
	r1.SetDesc(&proto.RangeDescriptor{RaftID: 1})
	r2 := &Range{}
	r2.SetDesc(&proto.RangeDescriptor{RaftID: 2})
	testQueue := &testQueueImpl{
		shouldQueueFn: func(now proto.Timestamp, r *Range) (shouldQueue bool, priority float64) {
			shouldQueue = true
			priority = float64(r.Desc().RaftID)
			return
		},
		duration: 5 * time.Millisecond,
	}
	bq := newBaseQueue("test", testQueue, 2)
	stopper := util.NewStopper(0)
	mc := hlc.NewManualClock(0)
	clock := hlc.NewClock(mc.UnixNano)
	bq.Start(clock, stopper)
	defer stopper.Stop()

	bq.MaybeAdd(r1, proto.ZeroTimestamp)
	bq.MaybeAdd(r2, proto.ZeroTimestamp)
	if pc := atomic.LoadInt32(&testQueue.processed); pc != 0 {
		t.Errorf("expected no processed ranges; got %d", pc)
	}

	time.Sleep(5 * time.Millisecond)
	if err := util.IsTrueWithin(func() bool {
		return atomic.LoadInt32(&testQueue.processed) == 1
	}, 5*time.Millisecond); err != nil {
		t.Error(err)
	}

	time.Sleep(5 * time.Millisecond)
	if err := util.IsTrueWithin(func() bool {
		return atomic.LoadInt32(&testQueue.processed) == 2
	}, 5*time.Millisecond); err != nil {
		t.Error(err)
	}
}

// TestBaseQueueAddRemove adds then removes a range; ensure range is not processed.
func TestBaseQueueAddRemove(t *testing.T) {
	r := &Range{}
	r.SetDesc(&proto.RangeDescriptor{RaftID: 1})
	testQueue := &testQueueImpl{
		shouldQueueFn: func(now proto.Timestamp, r *Range) (shouldQueue bool, priority float64) {
			shouldQueue = true
			priority = 1.0
			return
		},
	}
	bq := newBaseQueue("test", testQueue, 2)
	stopper := util.NewStopper(0)
	mc := hlc.NewManualClock(0)
	clock := hlc.NewClock(mc.UnixNano)
	bq.Start(clock, stopper)
	defer stopper.Stop()

	bq.MaybeAdd(r, proto.ZeroTimestamp)
	bq.MaybeRemove(r)

	time.Sleep(5 * time.Millisecond)
	if pc := atomic.LoadInt32(&testQueue.processed); pc > 0 {
		t.Errorf("expected processed count of 0; got %d", pc)
	}
}

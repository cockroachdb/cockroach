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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"container/heap"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// testQueueImpl implements queueImpl with a closure for shouldQueue.
type testQueueImpl struct {
	shouldQueueFn func(hlc.Timestamp, *Replica) (bool, float64)
	processed     int32
	duration      time.Duration
	blocker       chan struct{} // timer() blocks on this if not nil
	pChan         chan struct{}
	err           error // always returns this error on process
}

func (tq *testQueueImpl) shouldQueue(
	now hlc.Timestamp, r *Replica, _ config.SystemConfig,
) (bool, float64) {
	return tq.shouldQueueFn(now, r)
}

func (tq *testQueueImpl) process(
	_ context.Context, now hlc.Timestamp, r *Replica, _ config.SystemConfig,
) error {
	atomic.AddInt32(&tq.processed, 1)
	return tq.err
}

func (tq *testQueueImpl) getProcessed() int {
	return int(atomic.LoadInt32(&tq.processed))
}

func (tq *testQueueImpl) timer() time.Duration {
	if tq.blocker != nil {
		<-tq.blocker
	}
	if tq.duration != 0 {
		return tq.duration
	}
	return 0
}

func (tq *testQueueImpl) purgatoryChan() <-chan struct{} {
	return tq.pChan
}

func makeTestBaseQueue(
	name string, impl queueImpl, store *Store, gossip *gossip.Gossip, cfg queueConfig,
) *baseQueue {
	cfg.successes = metric.NewCounter(metric.Metadata{Name: "processed"})
	cfg.failures = metric.NewCounter(metric.Metadata{Name: "failures"})
	cfg.pending = metric.NewGauge(metric.Metadata{Name: "pending"})
	cfg.processingNanos = metric.NewCounter(metric.Metadata{Name: "processingnanos"})
	cfg.purgatory = metric.NewGauge(metric.Metadata{Name: "purgatory"})
	return newBaseQueue(context.TODO(), name, impl, store, gossip, cfg)
}

// TestQueuePriorityQueue verifies priority queue implementation.
func TestQueuePriorityQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Create a priority queue, put the items in it, and
	// establish the priority queue (heap) invariants.
	const count = 3
	expRanges := make([]roachpb.RangeID, count+1)
	pq := make(priorityQueue, count)
	for i := 0; i < count; {
		pq[i] = &replicaItem{
			value:    roachpb.RangeID(i),
			priority: float64(i),
			index:    i,
		}
		expRanges[3-i] = pq[i].value
		i++
	}
	heap.Init(&pq)

	// Insert a new item and then modify its priority.
	priorityItem := &replicaItem{
		value:    -1,
		priority: 1.0,
	}
	heap.Push(&pq, priorityItem)
	pq.update(priorityItem, 4.0)
	expRanges[0] = priorityItem.value

	// Take the items out; they should arrive in decreasing priority order.
	for i := 0; pq.Len() > 0; i++ {
		item := heap.Pop(&pq).(*replicaItem)
		if item.value != expRanges[i] {
			t.Errorf("%d: unexpected range with priority %f", i, item.priority)
		}
	}
}

// TestBaseQueueAddUpdateAndRemove verifies basic operation with base
// queue including adding ranges which both should and shouldn't be
// queued, updating an existing range, and removing a range.
func TestBaseQueueAddUpdateAndRemove(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	// Remove replica for range 1 since it encompasses the entire keyspace.
	rng1, err := tc.store.GetReplica(1)
	if err != nil {
		t.Error(err)
	}
	if err := tc.store.RemoveReplica(rng1, *rng1.Desc(), true); err != nil {
		t.Error(err)
	}

	r1 := createReplica(tc.store, 1001, roachpb.RKey("1001"), roachpb.RKey("1001/end"))
	if err := tc.store.AddReplica(r1); err != nil {
		t.Fatal(err)
	}
	r2 := createReplica(tc.store, 1002, roachpb.RKey("1002"), roachpb.RKey("1002/end"))
	if err := tc.store.AddReplica(r2); err != nil {
		t.Fatal(err)
	}

	shouldAddMap := map[*Replica]bool{
		r1: true,
		r2: true,
	}
	priorityMap := map[*Replica]float64{
		r1: 1.0,
		r2: 2.0,
	}
	testQueue := &testQueueImpl{
		shouldQueueFn: func(now hlc.Timestamp, r *Replica) (shouldQueue bool, priority float64) {
			return shouldAddMap[r], priorityMap[r]
		},
	}
	bq := makeTestBaseQueue("test", testQueue, tc.store, tc.gossip, queueConfig{maxSize: 2})

	bq.MaybeAdd(r1, hlc.ZeroTimestamp)
	bq.MaybeAdd(r2, hlc.ZeroTimestamp)
	if bq.Length() != 2 {
		t.Fatalf("expected length 2; got %d", bq.Length())
	}
	if v := bq.pending.Value(); v != 2 {
		t.Errorf("expected 2 pending replicas; got %d", v)
	}
	if bq.pop() != r2 {
		t.Error("expected r2")
	}
	if v := bq.pending.Value(); v != 1 {
		t.Errorf("expected 1 pending replicas; got %d", v)
	}
	if bq.pop() != r1 {
		t.Error("expected r1")
	}
	if v := bq.pending.Value(); v != 0 {
		t.Errorf("expected 0 pending replicas; got %d", v)
	}
	if r := bq.pop(); r != nil {
		t.Errorf("expected empty queue; got %v", r)
	}

	// Add again, but this time r2 shouldn't add.
	shouldAddMap[r2] = false
	bq.MaybeAdd(r1, hlc.ZeroTimestamp)
	bq.MaybeAdd(r2, hlc.ZeroTimestamp)
	if bq.Length() != 1 {
		t.Errorf("expected length 1; got %d", bq.Length())
	}

	// Try adding same replica twice.
	bq.MaybeAdd(r1, hlc.ZeroTimestamp)
	if bq.Length() != 1 {
		t.Errorf("expected length 1; got %d", bq.Length())
	}

	// Re-add r2 and update priority of r1.
	shouldAddMap[r2] = true
	priorityMap[r1] = 3.0
	bq.MaybeAdd(r1, hlc.ZeroTimestamp)
	bq.MaybeAdd(r2, hlc.ZeroTimestamp)
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
	bq.MaybeAdd(r1, hlc.ZeroTimestamp)
	bq.MaybeAdd(r2, hlc.ZeroTimestamp)
	shouldAddMap[r2] = false
	bq.MaybeAdd(r2, hlc.ZeroTimestamp)
	if bq.Length() != 1 {
		t.Fatalf("expected length 1; got %d", bq.Length())
	}
	if v := bq.pending.Value(); v != 1 {
		t.Errorf("expected 1 pending replicas; got %d", v)
	}
	if bq.pop() != r1 {
		t.Errorf("expected r1")
	}
	if v := bq.pending.Value(); v != 0 {
		t.Errorf("expected 0 pending replicas; got %d", v)
	}
}

// TestBaseQueueAdd verifies that calling Add() directly overrides the
// ShouldQueue method.
func TestBaseQueueAdd(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	r, err := tc.store.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	testQueue := &testQueueImpl{
		shouldQueueFn: func(now hlc.Timestamp, r *Replica) (shouldQueue bool, priority float64) {
			return false, 0.0
		},
	}
	bq := makeTestBaseQueue("test", testQueue, tc.store, tc.gossip, queueConfig{maxSize: 1})
	bq.MaybeAdd(r, hlc.ZeroTimestamp)
	if bq.Length() != 0 {
		t.Fatalf("expected length 0; got %d", bq.Length())
	}
	if added, err := bq.Add(r, 1.0); err != nil || !added {
		t.Fatalf("expected Add to succeed: %t, %s", added, err)
	}
	// Add again and verify it's not actually added (it's already there).
	if added, err := bq.Add(r, 1.0); err != nil || added {
		t.Fatalf("expected Add to succeed: %t, %s", added, err)
	}
	if bq.Length() != 1 {
		t.Fatalf("expected length 1; got %d", bq.Length())
	}
}

// TestBaseQueueProcess verifies that items from the queue are
// processed according to the timer function.
func TestBaseQueueProcess(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tsc := TestStoreConfig()
	tc := testContext{}
	tc.StartWithStoreConfig(t, tsc)
	defer tc.Stop()

	// Remove replica for range 1 since it encompasses the entire keyspace.
	rng1, err := tc.store.GetReplica(1)
	if err != nil {
		t.Error(err)
	}
	if err := tc.store.RemoveReplica(rng1, *rng1.Desc(), true); err != nil {
		t.Error(err)
	}

	r1 := createReplica(tc.store, 1001, roachpb.RKey("1001"), roachpb.RKey("1001/end"))
	if err := tc.store.AddReplica(r1); err != nil {
		t.Fatal(err)
	}
	r2 := createReplica(tc.store, 1002, roachpb.RKey("1002"), roachpb.RKey("1002/end"))
	if err := tc.store.AddReplica(r2); err != nil {
		t.Fatal(err)
	}

	testQueue := &testQueueImpl{
		blocker: make(chan struct{}, 1),
		shouldQueueFn: func(now hlc.Timestamp, r *Replica) (shouldQueue bool, priority float64) {
			shouldQueue = true
			priority = float64(r.RangeID)
			return
		},
	}
	bq := makeTestBaseQueue("test", testQueue, tc.store, tc.gossip, queueConfig{maxSize: 2})
	bq.Start(tc.clock, tc.stopper)

	bq.MaybeAdd(r1, hlc.ZeroTimestamp)
	bq.MaybeAdd(r2, hlc.ZeroTimestamp)
	if pc := testQueue.getProcessed(); pc != 0 {
		t.Errorf("expected no processed ranges; got %d", pc)
	}
	if v := bq.successes.Count(); v != 0 {
		t.Errorf("expected 0 processed replicas; got %d", v)
	}
	if v := bq.pending.Value(); v != 2 {
		t.Errorf("expected 2 pending replicas; got %d", v)
	}

	testQueue.blocker <- struct{}{}
	util.SucceedsSoon(t, func() error {
		if pc := testQueue.getProcessed(); pc != 1 {
			return errors.Errorf("expected 1 processed replicas; got %d", pc)
		}
		if v := bq.successes.Count(); v != 1 {
			return errors.Errorf("expected 1 processed replicas; got %d", v)
		}
		if v := bq.pending.Value(); v != 1 {
			return errors.Errorf("expected 1 pending replicas; got %d", v)
		}
		return nil
	})

	testQueue.blocker <- struct{}{}
	util.SucceedsSoon(t, func() error {
		if pc := testQueue.getProcessed(); pc < 2 {
			return errors.Errorf("expected >= %d processed replicas; got %d", 2, pc)
		}
		if v := bq.successes.Count(); v != 2 {
			return errors.Errorf("expected 2 processed replicas; got %d", v)
		}
		if v := bq.pending.Value(); v != 0 {
			return errors.Errorf("expected 0 pending replicas; got %d", v)
		}
		return nil
	})

	// Ensure the test queue is not blocked on a stray call to
	// testQueueImpl.timer().
	close(testQueue.blocker)
}

// TestBaseQueueAddRemove adds then removes a range; ensure range is
// not processed.
func TestBaseQueueAddRemove(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	r, err := tc.store.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	testQueue := &testQueueImpl{
		blocker: make(chan struct{}, 1),
		shouldQueueFn: func(now hlc.Timestamp, r *Replica) (shouldQueue bool, priority float64) {
			shouldQueue = true
			priority = 1.0
			return
		},
	}
	bq := makeTestBaseQueue("test", testQueue, tc.store, tc.gossip, queueConfig{maxSize: 2})
	mc := hlc.NewManualClock(0)
	clock := hlc.NewClock(mc.UnixNano)
	bq.Start(clock, tc.stopper)

	bq.MaybeAdd(r, hlc.ZeroTimestamp)
	bq.MaybeRemove(r.RangeID)

	// Wake the queue
	close(testQueue.blocker)

	// Make sure the queue has actually run through a few times
	for i := 0; i < cap(bq.incoming)+1; i++ {
		bq.incoming <- struct{}{}
	}

	if pc := testQueue.getProcessed(); pc > 0 {
		t.Errorf("expected processed count of 0; got %d", pc)
	}
}

// TestAcceptsUnsplitRanges verifies that ranges that need to split are properly
// rejected when the queue has 'acceptsUnsplitRanges = false'.
func TestAcceptsUnsplitRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, stopper := createTestStore(t)
	defer stopper.Stop()

	dataMaxAddr, err := keys.Addr(keys.SystemConfigTableDataMax)
	if err != nil {
		t.Fatal(err)
	}

	// Remove replica for range 1 since it encompasses the entire keyspace.
	rng1, err := s.GetReplica(1)
	if err != nil {
		t.Error(err)
	}
	if err := s.RemoveReplica(rng1, *rng1.Desc(), true); err != nil {
		t.Error(err)
	}

	// This range can never be split due to zone configs boundaries.
	neverSplits := createReplica(s, 2, roachpb.RKeyMin, dataMaxAddr)
	if err := s.AddReplica(neverSplits); err != nil {
		t.Fatal(err)
	}

	// This range will need to be split after user db/table entries are created.
	willSplit := createReplica(s, 3, dataMaxAddr, roachpb.RKeyMax)
	if err := s.AddReplica(willSplit); err != nil {
		t.Fatal(err)
	}

	testQueue := &testQueueImpl{
		shouldQueueFn: func(now hlc.Timestamp, r *Replica) (shouldQueue bool, priority float64) {
			// Always queue ranges if they make it past the base queue's logic.
			return true, float64(r.RangeID)
		},
	}

	bq := makeTestBaseQueue("test", testQueue, s, s.cfg.Gossip, queueConfig{maxSize: 2})
	bq.Start(s.cfg.Clock, stopper)

	// Check our config.
	var sysCfg config.SystemConfig
	util.SucceedsSoon(t, func() error {
		var ok bool
		sysCfg, ok = s.cfg.Gossip.GetSystemConfig()
		if !ok {
			return errors.New("system config not yet present")
		}
		return nil
	})
	neverSplitsDesc := neverSplits.Desc()
	if sysCfg.NeedsSplit(neverSplitsDesc.StartKey, neverSplitsDesc.EndKey) {
		t.Fatal("System config says range needs to be split")
	}
	willSplitDesc := willSplit.Desc()
	if sysCfg.NeedsSplit(willSplitDesc.StartKey, willSplitDesc.EndKey) {
		t.Fatal("System config says range needs to be split")
	}

	// There are no user db/table entries, everything should be added and
	// processed as usual.
	bq.MaybeAdd(neverSplits, hlc.ZeroTimestamp)
	bq.MaybeAdd(willSplit, hlc.ZeroTimestamp)

	util.SucceedsSoon(t, func() error {
		if pc := testQueue.getProcessed(); pc != 2 {
			return errors.Errorf("expected %d processed replicas; got %d", 2, pc)
		}
		// Check metrics.
		if v := bq.successes.Count(); v != 2 {
			return errors.Errorf("expected 2 processed replicas; got %d", v)
		}
		if v := bq.pending.Value(); v != 0 {
			return errors.Errorf("expected 0 pending replicas; got %d", v)
		}
		return nil
	})

	// Now add a user object, it will trigger a split.
	// The range willSplit starts at the beginning of the user data range,
	// which means keys.MaxReservedDescID+1.
	config.TestingSetZoneConfig(keys.MaxReservedDescID+2, config.ZoneConfig{RangeMaxBytes: 1 << 20})

	// Check our config.
	neverSplitsDesc = neverSplits.Desc()
	if sysCfg.NeedsSplit(neverSplitsDesc.StartKey, neverSplitsDesc.EndKey) {
		t.Fatal("System config says range needs to be split")
	}
	willSplitDesc = willSplit.Desc()
	if !sysCfg.NeedsSplit(willSplitDesc.StartKey, willSplitDesc.EndKey) {
		t.Fatal("System config says range does not need to be split")
	}

	bq.MaybeAdd(neverSplits, hlc.ZeroTimestamp)
	bq.MaybeAdd(willSplit, hlc.ZeroTimestamp)

	util.SucceedsSoon(t, func() error {
		if pc := testQueue.getProcessed(); pc != 3 {
			return errors.Errorf("expected %d processed replicas; got %d", 3, pc)
		}
		// Check metrics.
		if v := bq.successes.Count(); v != 3 {
			return errors.Errorf("expected 3 processed replicas; got %d", v)
		}
		if v := bq.pending.Value(); v != 0 {
			return errors.Errorf("expected 0 pending replicas; got %d", v)
		}
		return nil
	})
}

type testError struct{}

func (*testError) Error() string {
	return "test error"
}

func (*testError) purgatoryErrorMarker() {
}

// TestBaseQueuePurgatory verifies that if error is set on the test
// queue, items are added to the purgatory. Verifies that sending on
// the purgatory channel causes the replicas to be reprocessed.
func TestBaseQueuePurgatory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tsc := TestStoreConfig()
	tc := testContext{}
	tc.StartWithStoreConfig(t, tsc)
	defer tc.Stop()

	testQueue := &testQueueImpl{
		duration: time.Nanosecond,
		shouldQueueFn: func(now hlc.Timestamp, r *Replica) (shouldQueue bool, priority float64) {
			shouldQueue = true
			priority = float64(r.RangeID)
			return
		},
		pChan: make(chan struct{}, 1),
		err:   &testError{},
	}

	// Remove replica for range 1 since it encompasses the entire keyspace.
	rng1, err := tc.store.GetReplica(1)
	if err != nil {
		t.Error(err)
	}
	if err := tc.store.RemoveReplica(rng1, *rng1.Desc(), true); err != nil {
		t.Error(err)
	}

	replicaCount := 10
	bq := makeTestBaseQueue("test", testQueue, tc.store, tc.gossip, queueConfig{maxSize: replicaCount})
	bq.Start(tc.clock, tc.stopper)

	for i := 1; i <= replicaCount; i++ {
		r := createReplica(tc.store, roachpb.RangeID(i+1000),
			roachpb.RKey(fmt.Sprintf("%d", i)), roachpb.RKey(fmt.Sprintf("%d/end", i)))
		if err := tc.store.AddReplica(r); err != nil {
			t.Fatal(err)
		}
		bq.MaybeAdd(r, hlc.ZeroTimestamp)
	}

	util.SucceedsSoon(t, func() error {
		if pc := testQueue.getProcessed(); pc != replicaCount {
			return errors.Errorf("expected %d processed replicas; got %d", replicaCount, pc)
		}
		// We have to loop checking the following conditions because the increment
		// of testQueue.processed does not happen atomically with the replica being
		// placed in purgatory.
		// Verify that the size of the purgatory map is correct.
		if l := bq.PurgatoryLength(); l != replicaCount {
			return errors.Errorf("expected purgatory size of %d; got %d", replicaCount, l)
		}
		// ...and priorityQ should be empty.
		if l := bq.Length(); l != 0 {
			return errors.Errorf("expected empty priorityQ; got %d", l)
		}
		// Check metrics.
		if v := bq.successes.Count(); v != 0 {
			return errors.Errorf("expected 0 processed replicas; got %d", v)
		}
		if v := bq.failures.Count(); v != int64(replicaCount) {
			return errors.Errorf("expected %d failed replicas; got %d", replicaCount, v)
		}
		if v := bq.pending.Value(); v != 0 {
			return errors.Errorf("expected 0 pending replicas; got %d", v)
		}
		if v := bq.purgatory.Value(); v != int64(replicaCount) {
			return errors.Errorf("expected %d purgatory replicas; got %d", replicaCount, v)
		}
		return nil
	})

	// Now, signal that purgatoried replicas should retry.
	testQueue.pChan <- struct{}{}

	util.SucceedsSoon(t, func() error {
		if pc := testQueue.getProcessed(); pc != replicaCount*2 {
			return errors.Errorf("expected %d processed replicas; got %d", replicaCount*2, pc)
		}
		// We have to loop checking the following conditions because the increment
		// of testQueue.processed does not happen atomically with the replica being
		// placed in purgatory.
		// Verify the replicas are still in purgatory.
		if l := bq.PurgatoryLength(); l != replicaCount {
			return errors.Errorf("expected purgatory size of %d; got %d", replicaCount, l)
		}
		// ...and priorityQ should be empty.
		if l := bq.Length(); l != 0 {
			return errors.Errorf("expected empty priorityQ; got %d", l)
		}
		// Check metrics.
		if v := bq.successes.Count(); v != 0 {
			return errors.Errorf("expected 0 processed replicas; got %d", v)
		}
		if v := bq.failures.Count(); v != int64(replicaCount*2) {
			return errors.Errorf("expected %d failed replicas; got %d", replicaCount*2, v)
		}
		if v := bq.pending.Value(); v != 0 {
			return errors.Errorf("expected 0 pending replicas; got %d", v)
		}
		if v := bq.purgatory.Value(); v != int64(replicaCount) {
			return errors.Errorf("expected %d purgatory replicas; got %d", replicaCount, v)
		}
		return nil
	})

	// Remove error and reprocess.
	testQueue.err = nil
	testQueue.pChan <- struct{}{}

	util.SucceedsSoon(t, func() error {
		if pc := testQueue.getProcessed(); pc != replicaCount*3 {
			return errors.Errorf("expected %d processed replicas; got %d", replicaCount*3, pc)
		}
		// Check metrics.
		if v := bq.successes.Count(); v != int64(replicaCount) {
			return errors.Errorf("expected %d processed replicas; got %d", replicaCount, v)
		}
		if v := bq.failures.Count(); v != int64(replicaCount*2) {
			return errors.Errorf("expected %d failed replicas; got %d", replicaCount*2, v)
		}
		if v := bq.pending.Value(); v != 0 {
			return errors.Errorf("expected 0 pending replicas; got %d", v)
		}
		if v := bq.purgatory.Value(); v != 0 {
			return errors.Errorf("expected 0 purgatory replicas; got %d", v)
		}
		return nil
	})

	// Verify the replicas are no longer in purgatory.
	if l := bq.PurgatoryLength(); l != 0 {
		t.Errorf("expected purgatory size of 0; got %d", l)
	}
	// ...and priorityQ should be empty.
	if l := bq.Length(); l != 0 {
		t.Errorf("expected empty priorityQ; got %d", l)
	}
}

type processTimeoutQueueImpl struct {
	testQueueImpl
}

func (pq *processTimeoutQueueImpl) process(
	ctx context.Context, now hlc.Timestamp, r *Replica, _ config.SystemConfig,
) error {
	<-ctx.Done()
	atomic.AddInt32(&pq.processed, 1)
	return ctx.Err()
}

func TestBaseQueueProcessTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	r, err := tc.store.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	ptQueue := &processTimeoutQueueImpl{
		testQueueImpl: testQueueImpl{
			blocker: make(chan struct{}, 1),
			shouldQueueFn: func(now hlc.Timestamp, r *Replica) (shouldQueue bool, priority float64) {
				return true, 1.0
			},
		},
	}
	bq := makeTestBaseQueue("test", ptQueue, tc.store, tc.gossip,
		queueConfig{maxSize: 1, processTimeout: 1 * time.Millisecond})
	bq.Start(tc.clock, tc.stopper)
	bq.MaybeAdd(r, hlc.ZeroTimestamp)

	if l := bq.Length(); l != 1 {
		t.Errorf("expected one queued replica; got %d", l)
	}

	ptQueue.blocker <- struct{}{}
	util.SucceedsSoon(t, func() error {
		if pc := ptQueue.getProcessed(); pc != 1 {
			return errors.Errorf("expected 1 processed replicas; got %d", pc)
		}
		if v := bq.failures.Count(); v != 1 {
			return errors.Errorf("expected 1 failed replicas; got %d", v)
		}
		return nil
	})
}

// processTimeQueueImpl spends 5ms on each process request.
type processTimeQueueImpl struct {
	testQueueImpl
}

func (pq *processTimeQueueImpl) process(
	_ context.Context, _ hlc.Timestamp, _ *Replica, _ config.SystemConfig,
) error {
	time.Sleep(5 * time.Millisecond)
	return nil
}

func TestBaseQueueTimeMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	r, err := tc.store.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	ptQueue := &processTimeQueueImpl{
		testQueueImpl: testQueueImpl{
			shouldQueueFn: func(now hlc.Timestamp, r *Replica) (shouldQueue bool, priority float64) {
				return true, 1.0
			},
		},
	}
	bq := makeTestBaseQueue("test", ptQueue, tc.store, tc.gossip,
		queueConfig{maxSize: 1, processTimeout: 1 * time.Millisecond})
	bq.Start(tc.clock, tc.stopper)
	bq.MaybeAdd(r, hlc.ZeroTimestamp)

	util.SucceedsSoon(t, func() error {
		if v := bq.successes.Count(); v != 1 {
			return errors.Errorf("expected 1 processed replicas; got %d", v)
		}
		if min, v := 5*time.Millisecond, bq.processingNanos.Count(); v < min.Nanoseconds() {
			return errors.Errorf("expected >= %s in processing time; got %d", min, v)
		}
		return nil
	})
}

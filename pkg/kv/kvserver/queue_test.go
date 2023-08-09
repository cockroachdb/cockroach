// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"container/heap"
	"context"
	"fmt"
	"runtime/pprof"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

// testQueueImpl implements queueImpl with a closure for shouldQueue.
type testQueueImpl struct {
	shouldQueueFn func(hlc.ClockTimestamp, *Replica) (bool, float64)
	processed     int32 // accessed atomically
	duration      time.Duration
	blocker       chan struct{} // timer() blocks on this if not nil
	pChan         chan time.Time
	err           error // always returns this error on process
	noop          bool  // if enabled, process will return false
}

var _ queueImpl = &testQueueImpl{}

func (tq *testQueueImpl) shouldQueue(
	_ context.Context, now hlc.ClockTimestamp, r *Replica,
) (bool, float64) {
	return tq.shouldQueueFn(now, r)
}

func (tq *testQueueImpl) process(_ context.Context, _ *Replica) (bool, error) {
	atomic.AddInt32(&tq.processed, 1)
	if tq.err != nil {
		return false, tq.err
	}
	return !tq.noop, nil
}

func (tq *testQueueImpl) getProcessed() int {
	return int(atomic.LoadInt32(&tq.processed))
}

func (*testQueueImpl) postProcessScheduled(
	ctx context.Context, replica replicaInQueue, priority float64,
) {
}

func (tq *testQueueImpl) timer(_ time.Duration) time.Duration {
	if tq.blocker != nil {
		<-tq.blocker
	}
	if tq.duration != 0 {
		return tq.duration
	}
	return 0
}

func (tq *testQueueImpl) purgatoryChan() <-chan time.Time {
	return tq.pChan
}

func (tq *testQueueImpl) updateChan() <-chan time.Time {
	return nil
}

func makeTestBaseQueue(name string, impl queueImpl, store *Store, cfg queueConfig) *baseQueue {
	cfg.successes = metric.NewCounter(metric.Metadata{Name: "processed"})
	cfg.failures = metric.NewCounter(metric.Metadata{Name: "failures"})
	cfg.pending = metric.NewGauge(metric.Metadata{Name: "pending"})
	cfg.processingNanos = metric.NewCounter(metric.Metadata{Name: "processingnanos"})
	cfg.purgatory = metric.NewGauge(metric.Metadata{Name: "purgatory"})
	cfg.disabledConfig = &settings.BoolSetting{}
	return newBaseQueue(name, impl, store, cfg)
}

func createReplicas(t *testing.T, tc *testContext, num int) []*Replica {
	t.Helper()

	// Remove replica for range 1 since it encompasses the entire keyspace.
	repl1, err := tc.store.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := tc.store.RemoveReplica(context.Background(), repl1, repl1.Desc().NextReplicaID, RemoveOptions{
		DestroyData: true,
	}); err != nil {
		t.Fatal(err)
	}

	repls := make([]*Replica, num)
	for i := 0; i < num; i++ {
		id := roachpb.RangeID(1000 + i)
		key := roachpb.RKey(strconv.Itoa(int(id)))
		endKey := roachpb.RKey(string(key) + "/end")
		r := createReplica(tc.store, id, key, endKey)
		if err := tc.store.AddReplica(r); err != nil {
			t.Fatal(err)
		}
		repls[i] = r
	}
	return repls
}

// TestQueuePriorityQueue verifies priority queue implementation.
func TestQueuePriorityQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// Create a priority queue, put the items in it, and
	// establish the priority queue (heap) invariants.
	const count = 3
	expRanges := make([]roachpb.RangeID, count+1)
	pq := priorityQueue{}
	pq.sl = make([]*replicaItem, count)
	for i := 0; i < count; {
		pq.sl[i] = &replicaItem{
			rangeID:  roachpb.RangeID(i),
			priority: float64(i),
			index:    i,
		}
		expRanges[3-i] = pq.sl[i].rangeID
		i++
	}
	heap.Init(&pq)

	// Insert a new item and then modify its priority.
	priorityItem := &replicaItem{
		rangeID:  -1,
		priority: 1.0,
	}
	heap.Push(&pq, priorityItem)
	pq.update(priorityItem, 4.0)
	expRanges[0] = priorityItem.rangeID

	// Take the items out; they should arrive in decreasing priority order.
	for i := 0; pq.Len() > 0; i++ {
		item := heap.Pop(&pq).(*replicaItem)
		if item.rangeID != expRanges[i] {
			t.Errorf("%d: unexpected range with priority %f", i, item.priority)
		}
	}
}

// TestBaseQueueAddUpdateAndRemove verifies basic operation with base
// queue including adding ranges which both should and shouldn't be
// queued, updating an existing range, and removing a range.
func TestBaseQueueAddUpdateAndRemove(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := testContext{}
	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	repls := createReplicas(t, &tc, 2)
	r1, r2 := repls[0], repls[1]

	shouldAddMap := map[*Replica]bool{
		r1: true,
		r2: true,
	}
	priorityMap := map[*Replica]float64{
		r1: 1.0,
		r2: 2.0,
	}
	testQueue := &testQueueImpl{
		shouldQueueFn: func(now hlc.ClockTimestamp, r *Replica) (shouldQueue bool, priority float64) {
			return shouldAddMap[r], priorityMap[r]
		},
	}
	bq := makeTestBaseQueue("test", testQueue, tc.store, queueConfig{maxSize: 2})

	bq.maybeAdd(ctx, r1, hlc.ClockTimestamp{})
	bq.maybeAdd(ctx, r2, hlc.ClockTimestamp{})
	if bq.Length() != 2 {
		t.Fatalf("expected length 2; got %d", bq.Length())
	}
	if v := bq.pending.Value(); v != 2 {
		t.Errorf("expected 2 pending replicas; got %d", v)
	}
	if r, _ := bq.pop(); r != r2 {
		t.Error("expected r2")
	} else {
		bq.finishProcessingReplica(ctx, stopper, r2, nil)
	}
	if v := bq.pending.Value(); v != 1 {
		t.Errorf("expected 1 pending replicas; got %d", v)
	}
	if r, _ := bq.pop(); r != r1 {
		t.Error("expected r1")
	} else {
		bq.finishProcessingReplica(ctx, stopper, r1, nil)
	}
	if v := bq.pending.Value(); v != 0 {
		t.Errorf("expected 0 pending replicas; got %d", v)
	}
	if r, _ := bq.pop(); r != nil {
		t.Errorf("expected empty queue; got %v", r)
	}

	// Add again, but this time r2 shouldn't add.
	shouldAddMap[r2] = false
	bq.maybeAdd(ctx, r1, hlc.ClockTimestamp{})
	bq.maybeAdd(ctx, r2, hlc.ClockTimestamp{})
	if bq.Length() != 1 {
		t.Errorf("expected length 1; got %d", bq.Length())
	}

	// Try adding same replica twice.
	bq.maybeAdd(ctx, r1, hlc.ClockTimestamp{})
	if bq.Length() != 1 {
		t.Errorf("expected length 1; got %d", bq.Length())
	}

	// Re-add r2 and update priority of r1.
	shouldAddMap[r2] = true
	priorityMap[r1] = 3.0
	bq.maybeAdd(ctx, r1, hlc.ClockTimestamp{})
	bq.maybeAdd(ctx, r2, hlc.ClockTimestamp{})
	if bq.Length() != 2 {
		t.Fatalf("expected length 2; got %d", bq.Length())
	}
	if r, _ := bq.pop(); r != r1 {
		t.Error("expected r1")
	} else {
		bq.finishProcessingReplica(ctx, stopper, r1, nil)
	}
	if r, _ := bq.pop(); r != r2 {
		t.Error("expected r2")
	} else {
		bq.finishProcessingReplica(ctx, stopper, r2, nil)
	}
	if r, _ := bq.pop(); r != nil {
		t.Errorf("expected empty queue; got %v", r)
	}

	// Verify that priorities aren't lowered by a later MaybeAdd.
	bq.maybeAdd(ctx, r1, hlc.ClockTimestamp{})
	bq.maybeAdd(ctx, r2, hlc.ClockTimestamp{})
	priorityMap[r1] = 1.0
	bq.maybeAdd(ctx, r1, hlc.ClockTimestamp{})
	if bq.Length() != 2 {
		t.Fatalf("expected length 2; got %d", bq.Length())
	}
	if r, _ := bq.pop(); r != r1 {
		t.Error("expected r1")
	} else {
		bq.finishProcessingReplica(ctx, stopper, r1, nil)
	}
	if r, _ := bq.pop(); r != r2 {
		t.Error("expected r2")
	} else {
		bq.finishProcessingReplica(ctx, stopper, r2, nil)
	}
	if r, _ := bq.pop(); r != nil {
		t.Errorf("expected empty queue; got %v", r)
	}

	// Try removing a replica.
	bq.maybeAdd(ctx, r1, hlc.ClockTimestamp{})
	bq.maybeAdd(ctx, r2, hlc.ClockTimestamp{})
	bq.MaybeRemove(r2.RangeID)
	if bq.Length() != 1 {
		t.Fatalf("expected length 1; got %d", bq.Length())
	}
	if v := bq.pending.Value(); v != 1 {
		t.Errorf("expected 1 pending replicas; got %d", v)
	}
	if r, _ := bq.pop(); r != r1 {
		t.Errorf("expected r1")
	} else {
		bq.finishProcessingReplica(ctx, stopper, r1, nil)
	}
	if v := bq.pending.Value(); v != 0 {
		t.Errorf("expected 0 pending replicas; got %d", v)
	}
}

// TestBaseQueueSamePriorityFIFO verifies that if multiple items are queued at
// the same priority, they will be processes in first-in-first-out order.
// This avoids starvation scenarios, in particular in the Raft snapshot queue.
//
// See:
// https://github.com/cockroachdb/cockroach/issues/31947#issuecomment-434383267
func TestBaseQueueSamePriorityFIFO(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := testContext{}
	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	repls := createReplicas(t, &tc, 5)

	testQueue := &testQueueImpl{
		shouldQueueFn: func(now hlc.ClockTimestamp, r *Replica) (shouldQueue bool, priority float64) {
			t.Fatal("unexpected call to shouldQueue")
			return false, 0.0
		},
	}

	bq := makeTestBaseQueue("test", testQueue, tc.store, queueConfig{maxSize: 100})

	for _, repl := range repls {
		added, err := bq.testingAdd(ctx, repl, 0.0)
		if err != nil {
			t.Fatalf("%s: %v", repl, err)
		}
		if !added {
			t.Fatalf("%v not added", repl)
		}
	}
	for _, expRepl := range repls {
		actRepl, _ := bq.pop()
		if actRepl != expRepl {
			t.Fatalf("expected %v, got %v", expRepl, actRepl)
		}
	}
}

// TestBaseQueueAdd verifies that calling Add() directly overrides the
// ShouldQueue method.
func TestBaseQueueAdd(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := testContext{}
	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	r, err := tc.store.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	testQueue := &testQueueImpl{
		shouldQueueFn: func(now hlc.ClockTimestamp, r *Replica) (shouldQueue bool, priority float64) {
			return false, 0.0
		},
	}
	bq := makeTestBaseQueue("test", testQueue, tc.store, queueConfig{maxSize: 1})
	bq.maybeAdd(context.Background(), r, hlc.ClockTimestamp{})
	if bq.Length() != 0 {
		t.Fatalf("expected length 0; got %d", bq.Length())
	}
	if added, err := bq.testingAdd(ctx, r, 1.0); err != nil || !added {
		t.Fatalf("expected Add to succeed: %t, %s", added, err)
	}
	// Add again and verify it's not actually added (it's already there).
	if added, err := bq.testingAdd(ctx, r, 1.0); err != nil || added {
		t.Fatalf("expected Add to succeed: %t, %s", added, err)
	}
	if bq.Length() != 1 {
		t.Fatalf("expected length 1; got %d", bq.Length())
	}
}

// TestBaseQueueNoop verifies that only successful processes
// are counted as successes, while no-ops are not.
func TestBaseQueueNoop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tsc := TestStoreConfig(nil)
	tc := testContext{}
	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)
	tc.StartWithStoreConfig(ctx, t, stopper, tsc)

	repls := createReplicas(t, &tc, 2)
	r1, r2 := repls[0], repls[1]

	testQueue := &testQueueImpl{
		blocker: make(chan struct{}, 1),
		shouldQueueFn: func(now hlc.ClockTimestamp, r *Replica) (shouldQueue bool, priority float64) {
			shouldQueue = true
			priority = float64(r.RangeID)
			return
		},
		noop: false,
	}
	bq := makeTestBaseQueue("test", testQueue, tc.store, queueConfig{maxSize: 2})
	bq.Start(stopper)
	bq.maybeAdd(ctx, r1, hlc.ClockTimestamp{})
	testQueue.blocker <- struct{}{}
	testutils.SucceedsSoon(t, func() error {
		if pc := testQueue.getProcessed(); pc != 1 {
			return errors.Errorf("expected 1 processed replica; got %d", pc)
		}
		if v := bq.successes.Count(); v != 1 {
			return errors.Errorf("expected 1 successfully processed replicas; got %d", v)
		}
		return nil
	})

	// Ensure that when process is a no-op, the success count
	// is not incremented
	testQueue.noop = true
	bq.maybeAdd(ctx, r2, hlc.ClockTimestamp{})
	testQueue.blocker <- struct{}{}
	testutils.SucceedsSoon(t, func() error {
		if pc := testQueue.getProcessed(); pc != 2 {
			return errors.Errorf("expected 2 processed replicas; got %d", pc)
		}
		if v := bq.successes.Count(); v != 1 {
			return errors.Errorf("expected 1 successfully processed replica; got %d", v)
		}
		return nil
	})
	close(testQueue.blocker)
}

// TestBaseQueueProcess verifies that items from the queue are
// processed according to the timer function.
func TestBaseQueueProcess(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tsc := TestStoreConfig(nil)
	tc := testContext{}
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.StartWithStoreConfig(ctx, t, stopper, tsc)

	repls := createReplicas(t, &tc, 2)
	r1, r2 := repls[0], repls[1]

	testQueue := &testQueueImpl{
		blocker: make(chan struct{}, 1),
		shouldQueueFn: func(now hlc.ClockTimestamp, r *Replica) (shouldQueue bool, priority float64) {
			shouldQueue = true
			priority = float64(r.RangeID)
			return
		},
	}
	bq := makeTestBaseQueue("test", testQueue, tc.store, queueConfig{maxSize: 2})
	bq.Start(stopper)

	bq.maybeAdd(ctx, r1, hlc.ClockTimestamp{})
	bq.maybeAdd(ctx, r2, hlc.ClockTimestamp{})
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
	testutils.SucceedsSoon(t, func() error {
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
	testutils.SucceedsSoon(t, func() error {
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
	defer log.Scope(t).Close(t)
	tc := testContext{}
	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	r, err := tc.store.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	testQueue := &testQueueImpl{
		blocker: make(chan struct{}, 1),
		shouldQueueFn: func(now hlc.ClockTimestamp, r *Replica) (shouldQueue bool, priority float64) {
			shouldQueue = true
			priority = 1.0
			return
		},
	}
	bq := makeTestBaseQueue("test", testQueue, tc.store, queueConfig{maxSize: 2})
	bq.Start(stopper)

	bq.maybeAdd(ctx, r, hlc.ClockTimestamp{})
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

// BaseQueueLabel verifies that the queue name tag exists during maybeAdd but
// does not exist before and after maybeAdd.
func TestBaseQueueLabel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := testContext{}
	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)
	sc := TestStoreConfig(nil)
	sc.TestingKnobs.BaseQueueInterceptor = func(ctx context.Context, bq *baseQueue) {
		_, labelDoesExist := pprof.Label(ctx, bq.name)
		require.True(t, labelDoesExist)
	}
	tc.StartWithStoreConfig(ctx, t, stopper, sc)
	r, err := tc.store.GetReplica(1)
	require.NoError(t, err)

	testQueue := &testQueueImpl{
		shouldQueueFn: func(now hlc.ClockTimestamp, r *Replica) (shouldQueue bool, _ float64) {
			shouldQueue = true
			return
		},
	}
	bq := makeTestBaseQueue("test", testQueue, tc.store, queueConfig{})

	bq.Start(stopper)
	bq.maybeAdd(ctx, r, hlc.ClockTimestamp{})

	_, labelDoesExist := pprof.Label(ctx, bq.name)
	require.False(t, labelDoesExist)
}

// TestNeedsSystemConfig verifies that queues that don't need the system config
// are able to process replicas when the system config isn't available.
func TestNeedsSystemConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)

	tc := testContext{}
	cfg := TestStoreConfig(nil)
	// Configure a gossip instance that won't have the system config available in it.
	cfg.TestingKnobs.MakeSystemConfigSpanUnavailableToQueues = true
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	{
		confReader, err := tc.store.GetConfReader(ctx)
		require.Nil(t, confReader)
		require.True(t, errors.Is(err, errSpanConfigsUnavailable))
	}

	r, err := tc.store.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	queueFnCalled := 0
	testQueue := &testQueueImpl{
		shouldQueueFn: func(now hlc.ClockTimestamp, r *Replica) (bool, float64) {
			queueFnCalled++
			return true, 1.0
		},
	}

	// bqNeedsSysCfg will not add the replica or process it without a system config.
	bqNeedsSysCfg := makeTestBaseQueue("test", testQueue, tc.store, queueConfig{
		needsSpanConfigs:     true,
		acceptsUnsplitRanges: true,
		maxSize:              1,
	})

	bqNeedsSysCfg.Start(stopper)
	bqNeedsSysCfg.maybeAdd(ctx, r, hlc.ClockTimestamp{})
	if queueFnCalled != 0 {
		t.Fatalf("expected shouldQueueFn not to be called without valid system config, got %d calls", queueFnCalled)
	}

	// Manually add a replica and ensure that the process method doesn't get run.
	if added, err := bqNeedsSysCfg.testingAdd(ctx, r, 1.0); err != nil || !added {
		t.Fatalf("expected Add to succeed: %t, %s", added, err)
	}
	// Make sure the queue has actually run through a few times
	for i := 0; i < cap(bqNeedsSysCfg.incoming)+1; i++ {
		bqNeedsSysCfg.incoming <- struct{}{}
	}
	if pc := testQueue.getProcessed(); pc > 0 {
		t.Errorf("expected processed count of 0 for queue that needs system config; got %d", pc)
	}

	// Now check that a queue which doesn't require the system config can
	// successfully add and process a replica.
	bqNoSysCfg := makeTestBaseQueue("test", testQueue, tc.store, queueConfig{
		needsSpanConfigs:     false,
		acceptsUnsplitRanges: true,
		maxSize:              1,
	})
	bqNoSysCfg.Start(stopper)
	bqNoSysCfg.maybeAdd(context.Background(), r, hlc.ClockTimestamp{})
	if queueFnCalled != 1 {
		t.Fatalf("expected shouldQueueFn to be called even without valid system config, got %d calls", queueFnCalled)
	}
	testutils.SucceedsSoon(t, func() error {
		if pc := testQueue.getProcessed(); pc != 1 {
			return errors.Errorf("expected 1 processed replica even without system config; got %d", pc)
		}
		if v := bqNoSysCfg.successes.Count(); v != 1 {
			return errors.Errorf("expected 1 processed replica even without system config; got %d", v)
		}
		return nil
	})
}

// TestAcceptsUnsplitRanges verifies that ranges that need to split are properly
// rejected when the queue has 'acceptsUnsplitRanges = false'.
func TestAcceptsUnsplitRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	s, _ := createTestStore(ctx, t,
		testStoreOpts{
			// This test was written before test stores could start with more than one
			// range and was not adapted.
			createSystemRanges: false,
		},
		stopper)

	maxWontSplitAddr, err := keys.Addr(keys.SystemPrefix)
	if err != nil {
		t.Fatal(err)
	}
	minWillSplitAddr, err := keys.Addr(keys.TableDataMin)
	if err != nil {
		t.Fatal(err)
	}

	// Remove replica for range 1 since it encompasses the entire keyspace.
	repl1, err := s.GetReplica(1)
	if err != nil {
		t.Error(err)
	}
	if err := s.RemoveReplica(context.Background(), repl1, repl1.Desc().NextReplicaID, RemoveOptions{
		DestroyData: true,
	}); err != nil {
		t.Error(err)
	}

	// This range can never be split due to zone configs boundaries.
	neverSplits := createReplica(s, 2, roachpb.RKeyMin, maxWontSplitAddr)
	if err := s.AddReplica(neverSplits); err != nil {
		t.Fatal(err)
	}

	// This range will need to be split after user db/table entries are created.
	willSplit := createReplica(s, 3, minWillSplitAddr, roachpb.RKeyMax)
	if err := s.AddReplica(willSplit); err != nil {
		t.Fatal(err)
	}

	testQueue := &testQueueImpl{
		shouldQueueFn: func(now hlc.ClockTimestamp, r *Replica) (shouldQueue bool, priority float64) {
			// Always queue ranges if they make it past the base queue's logic.
			return true, float64(r.RangeID)
		},
	}

	bq := makeTestBaseQueue("test", testQueue, s, queueConfig{maxSize: 2})
	bq.Start(stopper)

	// Check our config.
	var cfg spanconfig.StoreReader
	testutils.SucceedsSoon(t, func() error {
		cfg, err = bq.store.GetConfReader(ctx)
		require.NoError(t, err)
		if cfg == nil {
			return errors.New("system config not yet present")
		}
		return nil
	})
	neverSplitsDesc := neverSplits.Desc()
	needsSplit, err := cfg.NeedsSplit(ctx, neverSplitsDesc.StartKey, neverSplitsDesc.EndKey)
	require.NoError(t, err)
	if needsSplit {
		t.Fatal("System config says range needs to be split")
	}
	willSplitDesc := willSplit.Desc()
	needsSplit, err = cfg.NeedsSplit(ctx, willSplitDesc.StartKey, willSplitDesc.EndKey)
	require.NoError(t, err)
	if needsSplit {
		t.Fatal("System config says range needs to be split")
	}

	// There are no user db/table entries, everything should be added and
	// processed as usual.
	bq.maybeAdd(ctx, neverSplits, hlc.ClockTimestamp{})
	bq.maybeAdd(ctx, willSplit, hlc.ClockTimestamp{})

	testutils.SucceedsSoon(t, func() error {
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
	zoneConfig := zonepb.DefaultZoneConfig()
	zoneConfig.RangeMaxBytes = proto.Int64(1 << 20)
	config.TestingSetZoneConfig(config.ObjectID(bootstrap.TestingUserDescID(1)), zoneConfig)

	// Check our config.
	neverSplitsDesc = neverSplits.Desc()
	needsSplit, err = cfg.NeedsSplit(ctx, neverSplitsDesc.StartKey, neverSplitsDesc.EndKey)
	require.NoError(t, err)
	if needsSplit {
		t.Fatal("System config says range needs to be split")
	}
	willSplitDesc = willSplit.Desc()
	needsSplit, err = cfg.NeedsSplit(ctx, willSplitDesc.StartKey, willSplitDesc.EndKey)
	require.NoError(t, err)
	if !needsSplit {
		t.Fatal("System config says range does not need to be split")
	}

	bq.maybeAdd(ctx, neverSplits, hlc.ClockTimestamp{})
	bq.maybeAdd(ctx, willSplit, hlc.ClockTimestamp{})

	testutils.SucceedsSoon(t, func() error {
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

type testPurgatoryError struct{}

func (*testPurgatoryError) Error() string {
	return "test purgatory error"
}

func (*testPurgatoryError) PurgatoryErrorMarker() {
}

// TestBaseQueuePurgatory verifies that if error is set on the test
// queue, items are added to the purgatory. Verifies that sending on
// the purgatory channel causes the replicas to be reprocessed.
func TestBaseQueuePurgatory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tsc := TestStoreConfig(nil)
	tc := testContext{}
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.StartWithStoreConfig(ctx, t, stopper, tsc)

	testQueue := &testQueueImpl{
		duration: time.Nanosecond,
		shouldQueueFn: func(now hlc.ClockTimestamp, r *Replica) (shouldQueue bool, priority float64) {
			shouldQueue = true
			priority = float64(r.RangeID)
			return
		},
		pChan: make(chan time.Time, 1),
		err:   &testPurgatoryError{},
	}

	const replicaCount = 10
	repls := createReplicas(t, &tc, replicaCount)

	bq := makeTestBaseQueue("test", testQueue, tc.store, queueConfig{maxSize: replicaCount})
	bq.Start(stopper)

	for _, r := range repls {
		bq.maybeAdd(context.Background(), r, hlc.ClockTimestamp{})
	}

	testutils.SucceedsSoon(t, func() error {
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
	testQueue.pChan <- timeutil.Now()

	testutils.SucceedsSoon(t, func() error {
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
	testQueue.pChan <- timeutil.Now()

	testutils.SucceedsSoon(t, func() error {
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

var _ queueImpl = &processTimeoutQueueImpl{}

func (pq *processTimeoutQueueImpl) process(
	ctx context.Context, r *Replica,
) (processed bool, err error) {
	<-ctx.Done()
	atomic.AddInt32(&pq.processed, 1)
	err = ctx.Err()
	return err == nil, err
}

func TestBaseQueueProcessTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := testContext{}
	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	r, err := tc.store.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	ptQueue := &processTimeoutQueueImpl{
		testQueueImpl: testQueueImpl{
			blocker: make(chan struct{}, 1),
			shouldQueueFn: func(now hlc.ClockTimestamp, r *Replica) (shouldQueue bool, priority float64) {
				return true, 1.0
			},
		},
	}
	bq := makeTestBaseQueue("test", ptQueue, tc.store,
		queueConfig{
			maxSize:              1,
			processTimeoutFunc:   constantTimeoutFunc(time.Millisecond),
			acceptsUnsplitRanges: true,
		})
	bq.Start(stopper)
	bq.maybeAdd(ctx, r, hlc.ClockTimestamp{})

	if l := bq.Length(); l != 1 {
		t.Errorf("expected one queued replica; got %d", l)
	}

	ptQueue.blocker <- struct{}{}
	testutils.SucceedsSoon(t, func() error {
		if pc := ptQueue.getProcessed(); pc != 1 {
			return errors.Errorf("expected 1 processed replicas; got %d", pc)
		}
		if v := bq.failures.Count(); v != 1 {
			return errors.Errorf("expected 1 failed replicas; got %d", v)
		}
		return nil
	})
}

type mvccStatsReplicaInQueue struct {
	replicaInQueue
	size int64
}

func (r mvccStatsReplicaInQueue) GetMVCCStats() enginepb.MVCCStats {
	return enginepb.MVCCStats{ValBytes: r.size}
}

func TestQueueRateLimitedTimeoutFunc(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	type testCase struct {
		guaranteedProcessingTime time.Duration
		rebalanceSnapshotRate    int64 // bytes/s
		replicaSize              int64 // bytes
		expectedTimeout          time.Duration
	}
	makeTest := func(tc testCase) (string, func(t *testing.T)) {
		return fmt.Sprintf("%+v", tc), func(t *testing.T) {
			st := cluster.MakeTestingClusterSettings()
			queueGuaranteedProcessingTimeBudget.Override(ctx, &st.SV, tc.guaranteedProcessingTime)
			rebalanceSnapshotRate.Override(ctx, &st.SV, tc.rebalanceSnapshotRate)
			tf := makeRateLimitedTimeoutFunc(rebalanceSnapshotRate)
			repl := mvccStatsReplicaInQueue{
				size: tc.replicaSize,
			}
			require.Equal(t, tc.expectedTimeout, tf(st, repl))
		}
	}
	for _, tc := range []testCase{
		{
			guaranteedProcessingTime: time.Minute,
			rebalanceSnapshotRate:    1 << 20,
			replicaSize:              1 << 20,
			expectedTimeout:          time.Minute, // the minimum timeout (guaranteedProcessingTime).
		},
		{
			guaranteedProcessingTime: time.Minute,
			rebalanceSnapshotRate:    1 << 20, // minimum rate for timeout calculation.
			replicaSize:              100 << 20,
			expectedTimeout:          100 * time.Second * permittedRangeScanSlowdown,
		},
		{
			guaranteedProcessingTime: time.Hour,
			rebalanceSnapshotRate:    1 << 20, // minimum rate for timeout calculation.
			replicaSize:              100 << 20,
			expectedTimeout:          time.Hour, // the minimum timeout (guaranteedProcessingTime).
		},
		{
			guaranteedProcessingTime: time.Minute,
			rebalanceSnapshotRate:    1 << 10,
			replicaSize:              100 << 20,
			expectedTimeout:          100 * (1 << 10) * time.Second * permittedRangeScanSlowdown,
		},
		{
			guaranteedProcessingTime: time.Minute,
			rebalanceSnapshotRate:    1 << 10, // minimum rate for timeout calculation.
			replicaSize:              100 << 20,
			expectedTimeout:          100 * (1 << 10) * time.Second * permittedRangeScanSlowdown,
		},
	} {
		t.Run(makeTest(tc))
	}
}

// processTimeQueueImpl spends 5ms on each process request.
type processTimeQueueImpl struct {
	testQueueImpl
}

var _ queueImpl = &processTimeQueueImpl{}

func (pq *processTimeQueueImpl) process(_ context.Context, _ *Replica) (processed bool, err error) {
	time.Sleep(5 * time.Millisecond)
	return true, nil
}

func TestBaseQueueTimeMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := testContext{}
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	r, err := tc.store.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	ptQueue := &processTimeQueueImpl{
		testQueueImpl: testQueueImpl{
			shouldQueueFn: func(now hlc.ClockTimestamp, r *Replica) (shouldQueue bool, priority float64) {
				return true, 1.0
			},
		},
	}
	bq := makeTestBaseQueue("test", ptQueue, tc.store,
		queueConfig{
			maxSize:              1,
			processTimeoutFunc:   constantTimeoutFunc(time.Millisecond),
			acceptsUnsplitRanges: true,
		})
	bq.Start(stopper)
	bq.maybeAdd(context.Background(), r, hlc.ClockTimestamp{})

	testutils.SucceedsSoon(t, func() error {
		if v := bq.successes.Count(); v != 1 {
			return errors.Errorf("expected 1 processed replicas; got %d", v)
		}
		if min, v := bq.queueConfig.processTimeoutFunc(nil, nil), bq.processingNanos.Count(); v < min.Nanoseconds() {
			return errors.Errorf("expected >= %s in processing time; got %s", min, time.Duration(v))
		}
		return nil
	})
}

func TestBaseQueueShouldQueueAgain(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCases := []struct {
		now, last   hlc.Timestamp
		minInterval time.Duration
		expQueue    bool
		expPriority float64
	}{
		{makeTS(1, 0), makeTS(1, 0), 0, true, 0},
		{makeTS(100, 0), makeTS(0, 0), 100, true, 0},
		{makeTS(100, 0), makeTS(100, 0), 100, false, 0},
		{makeTS(101, 0), makeTS(100, 0), 100, false, 0},
		{makeTS(200, 0), makeTS(100, 0), 100, true, 1},
		{makeTS(200, 1), makeTS(100, 0), 100, true, 1},
		{makeTS(201, 0), makeTS(100, 0), 100, true, 1.01},
		{makeTS(201, 0), makeTS(100, 1), 100, true, 1.01},
		{makeTS(1100, 0), makeTS(100, 1), 100, true, 10},
	}

	for i, tc := range testCases {
		sq, pri := shouldQueueAgain(tc.now, tc.last, tc.minInterval)
		if sq != tc.expQueue {
			t.Errorf("case %d: expected shouldQueue %t; got %t", i, tc.expQueue, sq)
		}
		if pri != tc.expPriority {
			t.Errorf("case %d: expected priority %f; got %f", i, tc.expPriority, pri)
		}
	}
}

// TestBaseQueueDisable verifies that disabling a queue prevents calls
// to both shouldQueue and process.
func TestBaseQueueDisable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := testContext{}
	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	r, err := tc.store.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	shouldQueueCalled := false
	testQueue := &testQueueImpl{
		blocker: make(chan struct{}, 1),
		shouldQueueFn: func(now hlc.ClockTimestamp, r *Replica) (bool, float64) {
			shouldQueueCalled = true
			return true, 1.0
		},
	}
	bq := makeTestBaseQueue("test", testQueue, tc.store, queueConfig{maxSize: 2})
	bq.Start(stopper)

	bq.SetDisabled(true)
	bq.maybeAdd(context.Background(), r, hlc.ClockTimestamp{})
	if shouldQueueCalled {
		t.Error("shouldQueue should not have been called")
	}

	// Add the range directly, bypassing shouldQueue.
	if _, err := bq.testingAdd(ctx, r, 1.0); !errors.Is(err, errQueueDisabled) {
		t.Fatal(err)
	}

	// Wake the queue.
	close(testQueue.blocker)

	// Make sure the queue has actually run through a few times.
	for i := 0; i < cap(bq.incoming)+1; i++ {
		bq.incoming <- struct{}{}
	}

	if pc := testQueue.getProcessed(); pc > 0 {
		t.Errorf("expected processed count of 0; got %d", pc)
	}
}

// TestQueueDisable verifies that setting the set of queue.enabled cluster
// settings actually disables the base queue. This test works alongside
// TestBaseQueueDisable to verify the entire disable workflow.
func TestQueueDisable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := testContext{}
	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	testCases := []struct {
		name           string
		clusterSetting *settings.BoolSetting
		queue          *baseQueue
	}{
		{
			name:           "Merge Queue",
			clusterSetting: kvserverbase.MergeQueueEnabled,
			queue:          tc.store.mergeQueue.baseQueue,
		},
		{
			name:           "Replicate Queue",
			clusterSetting: kvserverbase.ReplicateQueueEnabled,
			queue:          tc.store.replicateQueue.baseQueue,
		},
		{
			name:           "Replica GC Queue",
			clusterSetting: kvserverbase.ReplicaGCQueueEnabled,
			queue:          tc.store.replicaGCQueue.baseQueue,
		},
		{
			name:           "Raft Log Queue",
			clusterSetting: kvserverbase.RaftLogQueueEnabled,
			queue:          tc.store.raftLogQueue.baseQueue,
		},
		{
			name:           "Raft Snapshot Queue",
			clusterSetting: kvserverbase.RaftSnapshotQueueEnabled,
			queue:          tc.store.raftSnapshotQueue.baseQueue,
		},
		{
			name:           "Consistency Queue",
			clusterSetting: kvserverbase.ConsistencyQueueEnabled,
			queue:          tc.store.consistencyQueue.baseQueue,
		},
		{
			name:           "Split Queue",
			clusterSetting: kvserverbase.SplitQueueEnabled,
			queue:          tc.store.splitQueue.baseQueue,
		},
		{
			name:           "MVCC GC Queue",
			clusterSetting: kvserverbase.MVCCGCQueueEnabled,
			queue:          tc.store.mvccGCQueue.baseQueue,
		},
	}

	if tc.store.tsMaintenanceQueue != nil {
		testCases = append(testCases, struct {
			name           string
			clusterSetting *settings.BoolSetting
			queue          *baseQueue
		}{
			name:           "Timeseries Maintenance Queue",
			clusterSetting: kvserverbase.TimeSeriesMaintenanceQueueEnabled,
			queue:          tc.store.tsMaintenanceQueue.baseQueue,
		})
	}

	// Disable and verify all queues are disabled
	for _, testCase := range testCases {
		testCase.clusterSetting.Override(ctx, &tc.store.ClusterSettings().SV, false)
		if testCase.queue == nil {
			continue
		}
		testCase.queue.mu.Lock()
		disabled := testCase.queue.mu.disabled
		testCase.queue.mu.Unlock()
		if disabled != true {
			t.Errorf("%s should be disabled", testCase.name)
		}
	}
}

type parallelQueueImpl struct {
	testQueueImpl
	processBlocker chan struct{}
	processing     int32 // accessed atomically
}

var _ queueImpl = &parallelQueueImpl{}

func (pq *parallelQueueImpl) process(
	ctx context.Context, repl *Replica,
) (processed bool, err error) {
	atomic.AddInt32(&pq.processing, 1)
	if pq.processBlocker != nil {
		<-pq.processBlocker
	}
	processed, err = pq.testQueueImpl.process(ctx, repl)
	atomic.AddInt32(&pq.processing, -1)
	return processed, err
}

func (pq *parallelQueueImpl) getProcessing() int {
	return int(atomic.LoadInt32(&pq.processing))
}

func TestBaseQueueProcessConcurrently(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := testContext{}
	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	repls := createReplicas(t, &tc, 3)
	r1, r2, r3 := repls[0], repls[1], repls[2]

	pQueue := &parallelQueueImpl{
		testQueueImpl: testQueueImpl{
			blocker: make(chan struct{}, 1),
			shouldQueueFn: func(now hlc.ClockTimestamp, r *Replica) (shouldQueue bool, priority float64) {
				return true, 1
			},
		},
		processBlocker: make(chan struct{}, 1),
	}
	bq := makeTestBaseQueue("test", pQueue, tc.store,
		queueConfig{
			maxSize:        3,
			maxConcurrency: 2,
		},
	)
	bq.Start(stopper)

	bq.maybeAdd(ctx, r1, hlc.ClockTimestamp{})
	bq.maybeAdd(ctx, r2, hlc.ClockTimestamp{})
	bq.maybeAdd(ctx, r3, hlc.ClockTimestamp{})

	if exp, l := 3, bq.Length(); l != exp {
		t.Errorf("expected %d queued replica; got %d", exp, l)
	}

	assertProcessedAndProcessing := func(expProcessed, expProcessing int) {
		t.Helper()
		testutils.SucceedsSoon(t, func() error {
			if p := pQueue.getProcessed(); p != expProcessed {
				return errors.Errorf("expected %d processed replicas; got %d", expProcessed, p)
			}
			if p := pQueue.getProcessing(); p != expProcessing {
				return errors.Errorf("expected %d processing replicas; got %d", expProcessing, p)
			}
			return nil
		})
	}

	close(pQueue.blocker)
	assertProcessedAndProcessing(0, 2)

	pQueue.processBlocker <- struct{}{}
	assertProcessedAndProcessing(1, 2)

	pQueue.processBlocker <- struct{}{}
	assertProcessedAndProcessing(2, 1)

	pQueue.processBlocker <- struct{}{}
	assertProcessedAndProcessing(3, 0)
}

// TestBaseQueueReplicaChange ensures that if a replica is added to the queue
// with a non-zero replica ID then it is only processed if the retrieved replica
// from the getReplica() function has the same replica ID.
func TestBaseQueueChangeReplicaID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// The testContext exists only to construct the baseQueue.
	tc := testContext{}
	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)
	testQueue := &testQueueImpl{
		shouldQueueFn: func(now hlc.ClockTimestamp, r *Replica) (shouldQueue bool, priority float64) {
			return true, 1.0
		},
	}
	bq := makeTestBaseQueue("test", testQueue, tc.store, queueConfig{
		maxSize:              defaultQueueMaxSize,
		acceptsUnsplitRanges: true,
	})
	r := &fakeReplica{rangeID: 1, replicaID: 1}
	bq.mu.Lock()
	bq.getReplica = func(rangeID roachpb.RangeID) (replicaInQueue, error) {
		if rangeID != 1 {
			panic(fmt.Errorf("expected range id 1, got %d", rangeID))
		}
		return r, nil
	}
	bq.mu.Unlock()
	require.Equal(t, 0, testQueue.getProcessed())
	bq.maybeAdd(ctx, r, tc.store.Clock().NowAsClockTimestamp())
	bq.DrainQueue(tc.store.Stopper())
	require.Equal(t, 1, testQueue.getProcessed())
	bq.maybeAdd(ctx, r, tc.store.Clock().NowAsClockTimestamp())
	r.replicaID = 2
	bq.DrainQueue(tc.store.Stopper())
	require.Equal(t, 1, testQueue.getProcessed())
	require.Equal(t, 0, bq.Length())
	require.Equal(t, 0, bq.PurgatoryLength())
	bq.mu.Lock()
	defer bq.mu.Unlock()
	_, exists := bq.mu.replicas[1]
	require.False(t, exists, bq.mu.replicas)
}

func TestBaseQueueRequeue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := testContext{}
	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	repls := createReplicas(t, &tc, 1)
	r1 := repls[0]

	var shouldQueueCount int64 // accessed atomically
	pQueue := &parallelQueueImpl{
		testQueueImpl: testQueueImpl{
			blocker: make(chan struct{}, 1),
			shouldQueueFn: func(now hlc.ClockTimestamp, r *Replica) (shouldQueue bool, priority float64) {
				if atomic.AddInt64(&shouldQueueCount, 1) <= 4 {
					return true, 1
				}
				return false, 1
			},
		},
		processBlocker: make(chan struct{}, 1),
	}
	bq := makeTestBaseQueue("test", pQueue, tc.store,
		queueConfig{
			maxSize:        3,
			maxConcurrency: 2,
		},
	)
	bq.Start(stopper)

	assertShouldQueueCount := func(expShouldQueueCount int) {
		t.Helper()
		testutils.SucceedsSoon(t, func() error {
			if count := int(atomic.LoadInt64(&shouldQueueCount)); count != expShouldQueueCount {
				return errors.Errorf("expected %d calls to ShouldQueue; found %d",
					expShouldQueueCount, count)
			}
			return nil
		})
	}
	assertProcessedAndProcessing := func(expProcessed, expProcessing int) {
		t.Helper()
		testutils.SucceedsSoon(t, func() error {
			if p := pQueue.getProcessed(); p != expProcessed {
				return errors.Errorf("expected %d processed replicas; got %d", expProcessed, p)
			}
			if p := pQueue.getProcessing(); p != expProcessing {
				return errors.Errorf("expected %d processing replicas; got %d", expProcessing, p)
			}
			return nil
		})
	}
	// MaybeAdd a replica. Should queue after checking ShouldQueue.
	bq.maybeAdd(ctx, r1, hlc.ClockTimestamp{})
	assertShouldQueueCount(1)
	if exp, l := 1, bq.Length(); l != exp {
		t.Errorf("expected %d queued replica; got %d", exp, l)
	}

	// Let the first processing attempt run.
	close(pQueue.blocker)
	assertProcessedAndProcessing(0, 1)

	// MaybeAdd the same replica. Should requeue after checking ShouldQueue.
	bq.maybeAdd(ctx, r1, hlc.ClockTimestamp{})
	assertShouldQueueCount(2)

	// Let the first processing attempt finish.
	// Should begin processing second attempt after checking ShouldQueue again.
	pQueue.processBlocker <- struct{}{}
	assertShouldQueueCount(3)
	assertProcessedAndProcessing(1, 1)

	// MaybeAdd the same replica. Should requeue after checking ShouldQueue.
	bq.maybeAdd(ctx, r1, hlc.ClockTimestamp{})
	assertShouldQueueCount(4)

	// Let the second processing attempt finish.
	// Should NOT processing third attempt after checking ShouldQueue again.
	pQueue.processBlocker <- struct{}{}
	assertShouldQueueCount(5)
	assertProcessedAndProcessing(2, 0)

	// MaybeAdd the same replica. Should NOT queue after checking ShouldQueue.
	bq.maybeAdd(ctx, r1, hlc.ClockTimestamp{})
	assertShouldQueueCount(6)
	assertProcessedAndProcessing(2, 0)
}

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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/google/btree"
)

func makeAmbCtx() log.AmbientContext {
	return log.AmbientContext{Tracer: tracing.NewTracer()}
}

// Test implementation of a range set backed by btree.BTree.
type testRangeSet struct {
	syncutil.Mutex
	replicasByKey *btree.BTree
	visited       int
}

// newTestRangeSet creates a new range set that has the count number of ranges.
func newTestRangeSet(count int, t *testing.T) *testRangeSet {
	rs := &testRangeSet{replicasByKey: btree.New(64 /* degree */)}
	for i := 0; i < count; i++ {
		desc := &roachpb.RangeDescriptor{
			RangeID:  roachpb.RangeID(i),
			StartKey: roachpb.RKey(fmt.Sprintf("%03d", i)),
			EndKey:   roachpb.RKey(fmt.Sprintf("%03d", i+1)),
		}
		// Initialize the range stat so the scanner can use it.
		repl := &Replica{
			RangeID: desc.RangeID,
		}
		repl.mu.state.Stats = &enginepb.MVCCStats{
			KeyBytes:  1,
			ValBytes:  2,
			KeyCount:  1,
			LiveCount: 1,
		}
		repl.mu.state.Desc = desc
		repl.startKey = desc.StartKey // actually used by replicasByKey
		if exRngItem := rs.replicasByKey.ReplaceOrInsert((*btreeReplica)(repl)); exRngItem != nil {
			t.Fatalf("failed to insert range %s", repl)
		}
	}
	return rs
}

func (rs *testRangeSet) Visit(visitor func(*Replica) bool) {
	rs.Lock()
	defer rs.Unlock()
	rs.visited = 0
	rs.replicasByKey.Ascend(func(i btree.Item) bool {
		rs.visited++
		rs.Unlock()
		defer rs.Lock()
		return visitor((*Replica)(i.(*btreeReplica)))
	})
}

func (rs *testRangeSet) EstimatedCount() int {
	rs.Lock()
	defer rs.Unlock()
	count := rs.replicasByKey.Len() - rs.visited
	if count < 1 {
		count = 1
	}
	return count
}

// removeRange removes the i-th range from the range set.
func (rs *testRangeSet) remove(index int, t *testing.T) *Replica {
	endKey := roachpb.Key(fmt.Sprintf("%03d", index+1))
	rs.Lock()
	defer rs.Unlock()
	repl := rs.replicasByKey.Delete((rangeBTreeKey)(endKey))
	if repl == nil {
		t.Fatalf("failed to delete range of end key %s", endKey)
	}
	return (*Replica)(repl.(*btreeReplica))
}

// Test implementation of a range queue which adds range to an
// internal slice.
type testQueue struct {
	syncutil.Mutex // Protects ranges, done & processed count
	ranges         []*Replica
	done           bool
	processed      int
	disabled       bool
}

// setDisabled suspends processing of items from the queue.
func (tq *testQueue) setDisabled(d bool) {
	tq.Lock()
	defer tq.Unlock()
	tq.disabled = d
}

func (tq *testQueue) Start(stopper *stop.Stopper) {
	done := func() {
		tq.Lock()
		tq.done = true
		tq.Unlock()
	}

	if err := stopper.RunAsyncTask(context.Background(), "testqueue", func(context.Context) {
		for {
			select {
			case <-time.After(1 * time.Millisecond):
				tq.Lock()
				if !tq.disabled && len(tq.ranges) > 0 {
					tq.ranges = tq.ranges[1:]
					tq.processed++
				}
				tq.Unlock()
			case <-stopper.ShouldQuiesce():
				done()
				return
			}
		}
	}); err != nil {
		done()
	}
}

// NB: MaybeAddAsync on a testQueue is actually synchronous.
func (tq *testQueue) MaybeAddAsync(
	ctx context.Context, replI replicaInQueue, now hlc.ClockTimestamp,
) {
	repl := replI.(*Replica)

	tq.Lock()
	defer tq.Unlock()
	if index := tq.indexOf(repl.RangeID); index == -1 {
		tq.ranges = append(tq.ranges, repl)
	}
}

func (tq *testQueue) MaybeRemove(rangeID roachpb.RangeID) {
	tq.Lock()
	defer tq.Unlock()
	if index := tq.indexOf(rangeID); index != -1 {
		tq.ranges = append(tq.ranges[:index], tq.ranges[index+1:]...)
	}
}

func (tq *testQueue) Name() string {
	return "testQueue"
}

func (tq *testQueue) NeedsLease() bool {
	return false
}

func (tq *testQueue) count() int {
	tq.Lock()
	defer tq.Unlock()
	return len(tq.ranges)
}

func (tq *testQueue) indexOf(rangeID roachpb.RangeID) int {
	for i, repl := range tq.ranges {
		if repl.RangeID == rangeID {
			return i
		}
	}
	return -1
}

func (tq *testQueue) isDone() bool {
	tq.Lock()
	defer tq.Unlock()
	return tq.done
}

// TestScannerAddToQueues verifies that ranges are added to and
// removed from multiple queues.
func TestScannerAddToQueues(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const count = 3
	ranges := newTestRangeSet(count, t)
	q1, q2 := &testQueue{}, &testQueue{}
	// We don't want to actually consume entries from the queues during this test.
	q1.setDisabled(true)
	q2.setDisabled(true)
	mc := hlc.NewManualClock(123)
	clock := hlc.NewClock(mc.UnixNano, time.Nanosecond)
	s := newReplicaScanner(makeAmbCtx(), clock, 1*time.Millisecond, 0, 0, ranges)
	s.AddQueues(q1, q2)
	s.stopper = stop.NewStopper()

	// Start scanner and verify that all ranges are added to both queues.
	s.Start()
	testutils.SucceedsSoon(t, func() error {
		if q1.count() != count || q2.count() != count {
			return errors.Errorf("q1 or q2 count != %d; got %d, %d", count, q1.count(), q2.count())
		}
		return nil
	})

	// Remove first range and verify it does not exist in either range.
	rng := ranges.remove(0, t)
	testutils.SucceedsSoon(t, func() error {
		// This is intentionally inside the loop, otherwise this test races as
		// our removal of the range may be processed before a stray re-queue.
		// Removing on each attempt makes sure we clean this up as we retry.
		s.RemoveReplica(rng)
		c1 := q1.count()
		c2 := q2.count()
		if c1 != count-1 || c2 != count-1 {
			return errors.Errorf("q1 or q2 count != %d; got %d, %d", count-1, c1, c2)
		}
		return nil
	})

	// Stop scanner and verify both queues are stopped.
	s.stopper.Stop(context.Background())
	if !q1.isDone() || !q2.isDone() {
		t.Errorf("expected all queues to stop; got %t, %t", q1.isDone(), q2.isDone())
	}
}

// TestScannerTiming verifies that ranges are scanned, regardless
// of how many, to match scanInterval.
func TestScannerTiming(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const count = 3
	const runTime = 100 * time.Millisecond
	const maxError = 7500 * time.Microsecond
	durations := []time.Duration{
		15 * time.Millisecond,
		25 * time.Millisecond,
	}
	for i, duration := range durations {
		testutils.SucceedsSoon(t, func() error {
			ranges := newTestRangeSet(count, t)
			q := &testQueue{}
			mc := hlc.NewManualClock(123)
			clock := hlc.NewClock(mc.UnixNano, time.Nanosecond)
			s := newReplicaScanner(makeAmbCtx(), clock, duration, 0, 0, ranges)
			s.AddQueues(q)
			s.stopper = stop.NewStopper()
			s.Start()
			time.Sleep(runTime)
			s.stopper.Stop(context.Background())

			avg := s.avgScan()
			log.Infof(context.Background(), "%d: average scan: %s", i, avg)
			if avg.Nanoseconds()-duration.Nanoseconds() > maxError.Nanoseconds() ||
				duration.Nanoseconds()-avg.Nanoseconds() > maxError.Nanoseconds() {
				return errors.Errorf("expected %s, got %s: exceeds max error of %s", duration, avg, maxError)
			}
			return nil
		})
	}
}

// TestScannerPaceInterval tests that paceInterval returns the correct interval.
func TestScannerPaceInterval(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const count = 3
	durations := []time.Duration{
		30 * time.Millisecond,
		60 * time.Millisecond,
		500 * time.Millisecond,
	}
	// function logs an error when the actual value is not close
	// to the expected value
	logErrorWhenNotCloseTo := func(expected, actual time.Duration) {
		delta := 1 * time.Millisecond
		if actual < expected-delta || actual > expected+delta {
			t.Errorf("Expected duration %s, got %s", expected, actual)
		}
	}
	for _, duration := range durations {
		startTime := timeutil.Now()
		ranges := newTestRangeSet(count, t)
		s := newReplicaScanner(makeAmbCtx(), nil, duration, 0, 0, ranges)
		interval := s.paceInterval(startTime, startTime)
		logErrorWhenNotCloseTo(duration/count, interval)
		// The range set is empty
		ranges = newTestRangeSet(0, t)
		s = newReplicaScanner(makeAmbCtx(), nil, duration, 0, 0, ranges)
		interval = s.paceInterval(startTime, startTime)
		logErrorWhenNotCloseTo(duration, interval)
		ranges = newTestRangeSet(count, t)
		s = newReplicaScanner(makeAmbCtx(), nil, duration, 0, 0, ranges)
		// Move the present to duration time into the future
		interval = s.paceInterval(startTime, startTime.Add(duration))
		logErrorWhenNotCloseTo(0, interval)
	}
}

// TestScannerMinMaxIdleTime verifies that the pace interval will not
// be less than the specified min idle time or greater than the
// specified max idle time.
func TestScannerMinMaxIdleTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const targetInterval = 100 * time.Millisecond
	const minIdleTime = 10 * time.Millisecond
	const maxIdleTime = 15 * time.Millisecond
	for count := range []int{1, 10, 20, 100} {
		startTime := timeutil.Now()
		ranges := newTestRangeSet(count, t)
		s := newReplicaScanner(makeAmbCtx(), nil, targetInterval, minIdleTime, maxIdleTime, ranges)
		if interval := s.paceInterval(startTime, startTime); interval < minIdleTime || interval > maxIdleTime {
			t.Errorf("expected interval %s <= %s <= %s", minIdleTime, interval, maxIdleTime)
		}
	}
}

// TestScannerDisabled verifies that disabling a scanner prevents
// replicas from being added to queues.
func TestScannerDisabled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const count = 3
	ranges := newTestRangeSet(count, t)
	q := &testQueue{}
	mc := hlc.NewManualClock(123)
	clock := hlc.NewClock(mc.UnixNano, time.Nanosecond)
	s := newReplicaScanner(makeAmbCtx(), clock, 1*time.Millisecond, 0, 0, ranges)
	s.AddQueues(q)
	s.stopper = stop.NewStopper()
	defer s.stopper.Stop(context.Background())
	s.Start()

	// Verify queue gets all ranges.
	testutils.SucceedsSoon(t, func() error {
		if q.count() != count {
			return errors.Errorf("expected %d replicas; have %d", count, q.count())
		}
		if s.scanCount() == 0 {
			return errors.Errorf("expected scanner count to increment")
		}
		return nil
	})

	lastWaitEnabledCount := s.waitEnabledCount()

	// Now, disable the scanner.
	s.SetDisabled(true)
	testutils.SucceedsSoon(t, func() error {
		if s.waitEnabledCount() == lastWaitEnabledCount {
			return errors.Errorf("expected scanner to stop when disabled")
		}
		return nil
	})

	lastScannerCount := s.scanCount()

	// Remove the replicas and verify the scanner still removes them while disabled.
	ranges.Visit(func(repl *Replica) bool {
		s.RemoveReplica(repl)
		return true
	})

	testutils.SucceedsSoon(t, func() error {
		if qc := q.count(); qc != 0 {
			return errors.Errorf("expected queue to be empty after replicas removed from scanner; got %d", qc)
		}
		return nil
	})
	if sc := s.scanCount(); sc != lastScannerCount {
		t.Errorf("expected scanner count to not increment: %d != %d", sc, lastScannerCount)
	}
}

func TestScannerDisabledWithZeroInterval(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ranges := newTestRangeSet(1, t)
	s := newReplicaScanner(makeAmbCtx(), nil, 0*time.Millisecond, 0, 0, ranges)
	if !s.GetDisabled() {
		t.Errorf("expected scanner to be disabled")
	}
}

// TestScannerEmptyRangeSet verifies that an empty range set doesn't busy loop.
func TestScannerEmptyRangeSet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ranges := newTestRangeSet(0, t)
	q := &testQueue{}
	mc := hlc.NewManualClock(123)
	clock := hlc.NewClock(mc.UnixNano, time.Nanosecond)
	s := newReplicaScanner(makeAmbCtx(), clock, time.Hour, 0, 0, ranges)
	s.AddQueues(q)
	s.stopper = stop.NewStopper()
	defer s.stopper.Stop(context.Background())
	s.Start()
	time.Sleep(time.Millisecond) // give it some time to (not) busy loop
	if count := s.scanCount(); count > 1 {
		t.Errorf("expected at most one loop, but got %d", count)
	}
}

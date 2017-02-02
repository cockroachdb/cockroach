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
	"fmt"
	"testing"
	"time"

	"github.com/google/btree"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

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
		repl.mu.timedMutex = makeTimedMutex(defaultMuLogger)
		repl.cmdQMu.timedMutex = makeTimedMutex(defaultMuLogger)
		repl.mu.state.Stats = enginepb.MVCCStats{
			KeyBytes:  1,
			ValBytes:  2,
			KeyCount:  1,
			LiveCount: 1,
		}

		if err := repl.setDesc(desc); err != nil {
			t.Fatal(err)
		}
		if exRngItem := rs.replicasByKey.ReplaceOrInsert(repl); exRngItem != nil {
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
		return visitor(i.(*Replica))
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
	return repl.(*Replica)
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

func (tq *testQueue) Start(clock *hlc.Clock, stopper *stop.Stopper) {
	stopper.RunWorker(func() {
		for {
			select {
			case <-time.After(1 * time.Millisecond):
				tq.Lock()
				if !tq.disabled && len(tq.ranges) > 0 {
					tq.ranges = tq.ranges[1:]
					tq.processed++
				}
				tq.Unlock()
			case <-stopper.ShouldStop():
				tq.Lock()
				tq.done = true
				tq.Unlock()
				return
			}
		}
	})
}

func (tq *testQueue) MaybeAdd(repl *Replica, now hlc.Timestamp) {
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
	const count = 3
	ranges := newTestRangeSet(count, t)
	q1, q2 := &testQueue{}, &testQueue{}
	// We don't want to actually consume entries from the queues during this test.
	q1.setDisabled(true)
	q2.setDisabled(true)
	s := newReplicaScanner(log.AmbientContext{}, 1*time.Millisecond, 0, ranges)
	s.AddQueues(q1, q2)
	mc := hlc.NewManualClock(123)
	clock := hlc.NewClock(mc.UnixNano, time.Nanosecond)
	stopper := stop.NewStopper()

	// Start scanner and verify that all ranges are added to both queues.
	s.Start(clock, stopper)
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
	stopper.Stop()
	if !q1.isDone() || !q2.isDone() {
		t.Errorf("expected all queues to stop; got %t, %t", q1.isDone(), q2.isDone())
	}
}

// TestScannerTiming verifies that ranges are scanned, regardless
// of how many, to match scanInterval.
func TestScannerTiming(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
			s := newReplicaScanner(log.AmbientContext{}, duration, 0, ranges)
			s.AddQueues(q)
			mc := hlc.NewManualClock(123)
			clock := hlc.NewClock(mc.UnixNano, time.Nanosecond)
			stopper := stop.NewStopper()
			s.Start(clock, stopper)
			time.Sleep(runTime)
			stopper.Stop()

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
		s := newReplicaScanner(log.AmbientContext{}, duration, 0, ranges)
		interval := s.paceInterval(startTime, startTime)
		logErrorWhenNotCloseTo(duration/count, interval)
		// The range set is empty
		ranges = newTestRangeSet(0, t)
		s = newReplicaScanner(log.AmbientContext{}, duration, 0, ranges)
		interval = s.paceInterval(startTime, startTime)
		logErrorWhenNotCloseTo(duration, interval)
		ranges = newTestRangeSet(count, t)
		s = newReplicaScanner(log.AmbientContext{}, duration, 0, ranges)
		// Move the present to duration time into the future
		interval = s.paceInterval(startTime, startTime.Add(duration))
		logErrorWhenNotCloseTo(0, interval)
	}
}

// TestScannerDisabled verifies that disabling a scanner prevents
// replicas from being added to queues.
func TestScannerDisabled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const count = 3
	ranges := newTestRangeSet(count, t)
	q := &testQueue{}
	s := newReplicaScanner(log.AmbientContext{}, 1*time.Millisecond, 0, ranges)
	s.AddQueues(q)
	mc := hlc.NewManualClock(123)
	clock := hlc.NewClock(mc.UnixNano, time.Nanosecond)
	stopper := stop.NewStopper()
	s.Start(clock, stopper)
	defer stopper.Stop()

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
	ranges := newTestRangeSet(1, t)
	s := newReplicaScanner(log.AmbientContext{}, 0*time.Millisecond, 0, ranges)
	if !s.GetDisabled() {
		t.Errorf("expected scanner to be disabled")
	}
}

// TestScannerEmptyRangeSet verifies that an empty range set doesn't busy loop.
func TestScannerEmptyRangeSet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ranges := newTestRangeSet(0, t)
	q := &testQueue{}
	s := newReplicaScanner(log.AmbientContext{}, time.Hour, 0, ranges)
	s.AddQueues(q)
	mc := hlc.NewManualClock(123)
	clock := hlc.NewClock(mc.UnixNano, time.Nanosecond)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	s.Start(clock, stopper)
	time.Sleep(time.Millisecond) // give it some time to (not) busy loop
	if count := s.scanCount(); count > 1 {
		t.Errorf("expected at most one loop, but got %d", count)
	}
}

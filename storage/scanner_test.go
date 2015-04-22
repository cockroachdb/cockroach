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
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
)

// Test implementation of a range iterator which cycles through
// a slice of ranges.
type testIterator struct {
	index  int
	ranges []Range
	count  int
	start  time.Time
	total  time.Duration
	sync.Mutex
}

func newTestIterator(count int) *testIterator {
	ti := &testIterator{
		start: time.Now(),
	}
	ti.ranges = make([]Range, count)
	// Initialize range stats for each range so the scanner can use them.
	for i := range ti.ranges {
		ti.ranges[i].stats = &rangeStats{
			raftID: int64(i),
			MVCCStats: proto.MVCCStats{
				KeyBytes:  1,
				ValBytes:  2,
				KeyCount:  1,
				LiveCount: 1,
			},
		}
	}
	return ti
}

func (ti *testIterator) Next() *Range {
	ti.Lock()
	defer ti.Unlock()
	if ti.index >= len(ti.ranges) {
		return nil
	}
	oldIndex := ti.index
	ti.index++
	return &ti.ranges[oldIndex]
}

func (ti *testIterator) EstimatedCount() int {
	ti.Lock()
	defer ti.Unlock()
	return len(ti.ranges) - ti.index
}

func (ti *testIterator) Reset() {
	ti.Lock()
	defer ti.Unlock()
	ti.index = 0
	now := time.Now()
	ti.total += now.Sub(ti.start)
	ti.start = now
	ti.count++
}

func (ti *testIterator) remove(index int) *Range {
	ti.Lock()
	defer ti.Unlock()
	var rng *Range
	if index < len(ti.ranges) {
		rng = &ti.ranges[index]
		ti.ranges = append(ti.ranges[:index], ti.ranges[index+1:]...)
	}
	return rng
}

func (ti *testIterator) avgScan() time.Duration {
	ti.Lock()
	defer ti.Unlock()
	return time.Duration(ti.total.Nanoseconds() / int64(ti.count))
}

// Test implementation of a range queue which adds range to an
// internal slice.
type testQueue struct {
	sync.Mutex // Protects ranges, done & processed count
	ranges     []*Range
	done       bool
	processed  int
	disabled   bool
}

// setDisabled suspends processing of items from the queue.
func (tq *testQueue) setDisabled(d bool) {
	tq.Lock()
	defer tq.Unlock()
	tq.disabled = d
}

func (tq *testQueue) Start(clock *hlc.Clock, stopper *util.Stopper) {
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

func (tq *testQueue) MaybeAdd(rng *Range, now proto.Timestamp) {
	tq.Lock()
	defer tq.Unlock()
	if index := tq.indexOf(rng); index == -1 {
		tq.ranges = append(tq.ranges, rng)
	}
}

func (tq *testQueue) MaybeRemove(rng *Range) {
	tq.Lock()
	defer tq.Unlock()
	if index := tq.indexOf(rng); index != -1 {
		tq.ranges = append(tq.ranges[:index], tq.ranges[index+1:]...)
	}
}

func (tq *testQueue) count() int {
	tq.Lock()
	defer tq.Unlock()
	return len(tq.ranges)
}

func (tq *testQueue) indexOf(rng *Range) int {
	for i, r := range tq.ranges {
		if r == rng {
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
	defer leaktest.AfterTest(t)
	const count = 3
	iter := newTestIterator(count)
	q1, q2 := &testQueue{}, &testQueue{}
	// We don't want to actually consume entries from the queues during this test.
	q1.setDisabled(true)
	q2.setDisabled(true)
	s := newRangeScanner(1*time.Millisecond, iter, nil)
	s.AddQueues(q1, q2)
	mc := hlc.NewManualClock(0)
	clock := hlc.NewClock(mc.UnixNano)
	stopper := util.NewStopper()

	// Start queue and verify that all ranges are added to both queues.
	s.Start(clock, stopper)
	if err := util.IsTrueWithin(func() bool {
		return q1.count() == count && q2.count() == count
	}, 50*time.Millisecond); err != nil {
		t.Error(err)
	}

	// Remove first range and verify it does not exist in either range.
	rng := iter.remove(0)
	s.RemoveRange(rng)
	if err := util.IsTrueWithin(func() bool {
		return q1.count() == count-1 && q2.count() == count-1
	}, 10*time.Millisecond); err != nil {
		t.Error(err)
	}

	// Stop scanner and verify both queues are stopped.
	stopper.Stop()
	if !q1.isDone() || !q2.isDone() {
		t.Errorf("expected all queues to stop; got %t, %t", q1.isDone(), q2.isDone())
	}
}

// TestScannerTiming verifies that ranges are scanned, regardless
// of how many, to match scanInterval.
func TestScannerTiming(t *testing.T) {
	defer leaktest.AfterTest(t)
	const count = 3
	const runTime = 100 * time.Millisecond
	const maxError = 7500 * time.Microsecond
	durations := []time.Duration{
		10 * time.Millisecond,
		25 * time.Millisecond,
	}
	for i, duration := range durations {
		iter := newTestIterator(count)
		q := &testQueue{}
		s := newRangeScanner(duration, iter, nil)
		s.AddQueues(q)
		mc := hlc.NewManualClock(0)
		clock := hlc.NewClock(mc.UnixNano)
		stopper := util.NewStopper()
		defer stopper.Stop()
		s.Start(clock, stopper)
		time.Sleep(runTime)

		avg := iter.avgScan()
		log.Infof("%d: average scan: %s\n", i, avg)
		if avg.Nanoseconds()-duration.Nanoseconds() > maxError.Nanoseconds() ||
			duration.Nanoseconds()-avg.Nanoseconds() > maxError.Nanoseconds() {
			t.Errorf("expected %s, got %s: exceeds max error of %s", duration, avg, maxError)
		}
	}
}

// TestScannerEmptyIterator verifies that an empty iterator doesn't busy loop.
func TestScannerEmptyIterator(t *testing.T) {
	defer leaktest.AfterTest(t)
	iter := newTestIterator(0)
	q := &testQueue{}
	s := newRangeScanner(1*time.Millisecond, iter, nil)
	s.AddQueues(q)
	mc := hlc.NewManualClock(0)
	clock := hlc.NewClock(mc.UnixNano)
	stopper := util.NewStopper()
	defer stopper.Stop()
	s.Start(clock, stopper)
	time.Sleep(3 * time.Millisecond)
	if count := s.Count(); count > 3 {
		t.Errorf("expected three loops; got %d", count)
	}
}

// TestScannerStats verifies that stats accumulate from all ranges.
func TestScannerStats(t *testing.T) {
	defer leaktest.AfterTest(t)
	const count = 3
	iter := newTestIterator(count)
	q := &testQueue{}
	stopper := util.NewStopper()
	defer stopper.Stop()
	s := newRangeScanner(1*time.Millisecond, iter, nil)
	s.AddQueues(q)
	mc := hlc.NewManualClock(0)
	clock := hlc.NewClock(mc.UnixNano)
	// At start, scanner stats should be blank for MVCC, but have accurate number of ranges.
	if rc := s.Stats().RangeCount; rc != count {
		t.Errorf("range count expected %d; got %d", count, rc)
	}
	if vb := s.Stats().MVCC.ValBytes; vb != 0 {
		t.Errorf("value bytes expected %d; got %d", 0, vb)
	}
	s.Start(clock, stopper)
	// We expect a full run to accumulate stats from all ranges.
	if err := util.IsTrueWithin(func() bool {
		if rc := s.Stats().RangeCount; rc != count {
			return false
		}
		if vb := s.Stats().MVCC.ValBytes; vb != count*2 {
			return false
		}
		return true
	}, 100*time.Millisecond); err != nil {
		t.Error(err)
	}
}

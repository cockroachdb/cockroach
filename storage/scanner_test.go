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

	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
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
			MVCCStats: engine.MVCCStats{
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
	return time.Duration(ti.total.Nanoseconds() / int64(ti.count))
}

// Test implementation of a range queue which adds range to an
// internal slice.
type testQueue struct {
	sync.Mutex // Protects ranges, done & processed count
	ranges     []*Range
	done       bool
	processed  int
}

func (tq *testQueue) Start(clock *hlc.Clock, stopper *util.Stopper) {
	stopper.Add(1)
	go func() {
		for {
			select {
			case <-time.After(1 * time.Millisecond):
				tq.Lock()
				if len(tq.ranges) > 0 {
					tq.ranges = tq.ranges[1:]
					tq.processed++
				}
				tq.Unlock()
			case <-stopper.ShouldStop():
				stopper.SetStopped()
				tq.done = true
				return
			}
		}
	}()
}

func (tq *testQueue) MaybeAdd(rng *Range) {
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
	const count = 3
	iter := newTestIterator(count)
	q1, q2 := &testQueue{}, &testQueue{}
	s := newRangeScanner(1*time.Millisecond, iter, []rangeQueue{q1, q2})
	mc := hlc.NewManualClock(0)
	clock := hlc.NewClock(mc.UnixNano)

	// Start queue and verify that all ranges are added to both queues.
	s.Start(clock)
	if err := util.IsTrueWithin(func() bool {
		return q1.count() == count && q2.count() == count
	}, 10*time.Millisecond); err != nil {
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
	s.Stop()
	if !q1.isDone() || !q2.isDone() {
		t.Errorf("expected all queues to stop; got %t, %t", q1.isDone(), q2.isDone())
	}
}

// TestScannerTiming verifies that ranges are scanned, regardless
// of how many, to match scanInterval.
//
// TODO(spencer): in order to make this test not take too much time,
// we're running these loops at speeds where clock ticks may be
// an issue on virtual machines used for continuous integration.
func TestScannerTiming(t *testing.T) {
	const count = 3
	const runTime = 50 * time.Millisecond
	const maxError = 7500 * time.Microsecond
	durations := []time.Duration{
		5 * time.Millisecond,
		12500 * time.Microsecond,
	}
	for i, duration := range durations {
		iter := newTestIterator(count)
		q := &testQueue{}
		s := newRangeScanner(duration, iter, []rangeQueue{q})
		mc := hlc.NewManualClock(0)
		clock := hlc.NewClock(mc.UnixNano)
		s.Start(clock)
		time.Sleep(runTime)
		s.Stop()

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
	iter := newTestIterator(0)
	q := &testQueue{}
	s := newRangeScanner(1*time.Millisecond, iter, []rangeQueue{q})
	mc := hlc.NewManualClock(0)
	clock := hlc.NewClock(mc.UnixNano)
	s.Start(clock)
	time.Sleep(3 * time.Millisecond)
	s.Stop()
	if count := s.Count(); count > 3 {
		t.Errorf("expected three loops; got %d", count)
	}
}

// TestScannerStats verifies that stats accumulate from all ranges.
func TestScannerStats(t *testing.T) {
	const count = 3
	iter := newTestIterator(count)
	q := &testQueue{}
	s := newRangeScanner(1*time.Millisecond, iter, []rangeQueue{q})
	mc := hlc.NewManualClock(0)
	clock := hlc.NewClock(mc.UnixNano)
	// At start, scanner stats should be blank for MVCC, but have accurate number of ranges.
	if rc := s.Stats().RangeCount; rc != count {
		t.Errorf("range count expected %d; got %d", count, rc)
	}
	if vb := s.Stats().MVCC.ValBytes; vb != 0 {
		t.Errorf("value bytes expected %d; got %d", 0, vb)
	}
	s.Start(clock)
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
	s.Stop()
}

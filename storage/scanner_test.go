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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/util"
)

// Test implementation of a range iterator which cycles through
// a slice of ranges.
type testIterator struct {
	index  int
	ranges []*Range
	count  int
	start  time.Time
	total  time.Duration
	sync.Mutex
}

func newTestIterator(count int) *testIterator {
	ti := &testIterator{
		start: time.Now(),
	}
	ti.ranges = make([]*Range, 0, count)
	for i := 0; i < count; i++ {
		ti.ranges = append(ti.ranges, &Range{})
	}
	return ti
}

func (ti *testIterator) Next() (rng *Range, remaining int) {
	ti.Lock()
	defer ti.Unlock()
	if ti.index >= len(ti.ranges) {
		return
	}
	rng = ti.ranges[ti.index]
	remaining = len(ti.ranges) - ti.index
	ti.index++
	return
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
		rng = ti.ranges[index]
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
	ranges []*Range
}

func (tq *testQueue) MaybeAdd(rng *Range) {
	if index := tq.indexOf(rng); index == -1 {
		tq.ranges = append(tq.ranges, rng)
	}
}

func (tq *testQueue) MaybeRemove(rng *Range) {
	if index := tq.indexOf(rng); index != -1 {
		tq.ranges = append(tq.ranges[:index], tq.ranges[index+1:]...)
	}
}

func (tq *testQueue) Clear() {
	tq.ranges = []*Range(nil)
}

func (tq *testQueue) indexOf(rng *Range) int {
	for i, r := range tq.ranges {
		if r == rng {
			return i
		}
	}
	return -1
}

// TestScannerAddToQueues verifies that ranges are added to and
// removed from multiple queues.
func TestScannerAddToQueues(t *testing.T) {
	const count = 3
	iter := newTestIterator(count)
	q1, q2 := &testQueue{}, &testQueue{}
	s := newRangeScanner(iter, []rangeQueue{q1, q2})

	// Start queue and verify that all ranges are added to both queues.
	scanInterval = 1 * time.Millisecond
	s.start()
	if err := util.IsTrueWithin(func() bool {
		return len(q1.ranges) == count && len(q2.ranges) == count
	}, 10*time.Millisecond); err != nil {
		t.Error(err)
	}

	// Remove first range and verify it does not exist in either range.
	rng := iter.remove(0)
	s.removeRange(rng)
	if err := util.IsTrueWithin(func() bool {
		return len(q1.ranges) == count-1 && len(q2.ranges) == count-1
	}, 10*time.Millisecond); err != nil {
		t.Error(err)
	}

	// Stop queue and verify all ranges are removed from both queues.
	s.stop()
	if len(q1.ranges) != 0 || len(q2.ranges) != 0 {
		t.Errorf("expected all ranges to have been removed on stop; got %d, %d", len(q1.ranges), len(q2.ranges))
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
	const runTime = 10 * time.Millisecond
	const maxError = 500 * time.Microsecond
	durations := []time.Duration{
		1 * time.Millisecond,
		2 * time.Millisecond,
	}
	for i, duration := range durations {
		scanInterval = duration
		iter := newTestIterator(count)
		q := &testQueue{}
		s := newRangeScanner(iter, []rangeQueue{q})
		s.start()
		time.Sleep(runTime)
		s.stop()

		avg := iter.avgScan()
		fmt.Printf("%d: average scan: %s\n", i, avg)
		if avg.Nanoseconds()-duration.Nanoseconds() > maxError.Nanoseconds() ||
			duration.Nanoseconds()-avg.Nanoseconds() > maxError.Nanoseconds() {
			t.Errorf("expected %s, got %s: exceeds max error of %s", duration, avg, maxError)
		}
	}
}

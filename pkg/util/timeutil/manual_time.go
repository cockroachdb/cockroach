// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package timeutil

import (
	"container/heap"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// ManualTime is a testing implementation of TimeSource.
type ManualTime struct {
	mu struct {
		syncutil.Mutex
		now    time.Time
		timers manualTimerQueue
	}
}

// NewManualTime constructs a new ManualTime.
func NewManualTime(initialTime time.Time) *ManualTime {
	mt := ManualTime{}
	mt.mu.now = initialTime
	mt.mu.timers = manualTimerQueue{
		m: make(map[*manualTimer]int),
	}
	return &mt
}

var _ TimeSource = (*ManualTime)(nil)

// Now returns the current time.
func (m *ManualTime) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mu.now
}

// NewTimer constructs a new Timer.
func (m *ManualTime) NewTimer() TimerI {
	return &manualTimer{m: m}
}

// Advance forwards the current time by the given duration.
func (m *ManualTime) Advance(duration time.Duration) {
	m.AdvanceTo(m.Now().Add(duration))
}

// AdvanceTo advances the current time to t. If t is earlier than the current
// time then AdvanceTo is a no-op.
func (m *ManualTime) AdvanceTo(t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !t.After(m.mu.now) {
		return
	}
	m.mu.now = t
	for m.mu.timers.Len() > 0 {
		next := m.mu.timers.heap[0]
		if next.at.After(m.mu.now) {
			break
		}
		next.ch <- next.at
		heap.Pop(&m.mu.timers)
	}
}

func (m *ManualTime) add(mt *manualTimer) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !mt.at.After(m.mu.now) {
		mt.ch <- mt.at
	} else {
		heap.Push(&m.mu.timers, mt)
	}
}

func (m *ManualTime) remove(mt *manualTimer) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if idx, ok := m.mu.timers.m[mt]; ok {
		heap.Remove(&m.mu.timers, idx)
		return true
	}
	return false
}

// Timers returns a snapshot of the timestamps of the pending timers.
func (m *ManualTime) Timers() []time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	timers := make([]time.Time, m.mu.timers.Len())
	for i, t := range m.mu.timers.heap {
		timers[i] = t.at
	}
	sort.Slice(timers, func(i, j int) bool {
		return timers[i].Before(timers[j])
	})
	return timers
}

type manualTimerQueue struct {
	// m maintains the index for a timer in heap.
	m    map[*manualTimer]int
	heap []*manualTimer
}

var _ heap.Interface = (*manualTimerQueue)(nil)

func (m *manualTimerQueue) Len() int {
	return len(m.heap)
}

func (m *manualTimerQueue) Less(i, j int) bool {
	return m.heap[i].at.Before(m.heap[j].at)
}

func (m *manualTimerQueue) Swap(i, j int) {
	m.heap[i], m.heap[j] = m.heap[j], m.heap[i]
	m.m[m.heap[i]] = i
	m.m[m.heap[j]] = j
}

func (m *manualTimerQueue) Push(x interface{}) {
	mt := x.(*manualTimer)
	m.m[mt] = len(m.heap)
	m.heap = append(m.heap, mt)
}

func (m *manualTimerQueue) Pop() interface{} {
	lastIdx := len(m.heap) - 1
	ret := m.heap[lastIdx]
	delete(m.m, ret)
	m.heap = m.heap[:lastIdx]
	return ret
}

type manualTimer struct {
	m  *ManualTime
	at time.Time
	ch chan time.Time
}

var _ TimerI = (*manualTimer)(nil)

func (m *manualTimer) Reset(duration time.Duration) {
	m.Stop()
	m.at = m.m.Now().Add(duration)
	m.ch = make(chan time.Time, 1)
	m.m.add(m)
}

func (m *manualTimer) Stop() bool {
	removed := m.m.remove(m)
	m.ch = nil
	m.at = time.Time{}
	return removed
}

func (m *manualTimer) Ch() <-chan time.Time {
	return m.ch
}

func (m *manualTimer) MarkRead() {}

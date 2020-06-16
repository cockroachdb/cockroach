// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package quotapool

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
func (m *ManualTime) NewTimer() Timer {
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
	for len(m.mu.timers) > 0 {
		next := m.mu.timers[0]
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
	// TODO(ajwerner): Consider making this a O(log N) operation instead of an
	// O(n) operation.
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, imt := range m.mu.timers {
		if imt == mt {
			heap.Remove(&m.mu.timers, i)
			return true
		}
	}
	return false
}

// Timers returns a snapshot of the timestamps of the pending timers.
func (m *ManualTime) Timers() []time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	timers := make([]time.Time, len(m.mu.timers))
	for i := range m.mu.timers {
		timers[i] = m.mu.timers[i].at
	}
	sort.Slice(timers, func(i, j int) bool {
		return timers[i].Before(timers[j])
	})
	return timers
}

type manualTimerQueue []*manualTimer

var _ heap.Interface = (*manualTimerQueue)(nil)

func (m *manualTimerQueue) Len() int {
	return len(*m)
}

func (m *manualTimerQueue) Less(i, j int) bool {
	return (*m)[i].at.Before((*m)[j].at)
}

func (m *manualTimerQueue) Swap(i, j int) {
	(*m)[i], (*m)[j] = (*m)[j], (*m)[i]
}

func (m *manualTimerQueue) Push(x interface{}) {
	*m = append(*m, x.(*manualTimer))
}

func (m *manualTimerQueue) Pop() interface{} {
	ret := (*m)[m.Len()-1]
	*m = (*m)[:m.Len()-1]
	return ret
}

type manualTimer struct {
	m  *ManualTime
	at time.Time
	ch chan time.Time
}

var _ Timer = (*manualTimer)(nil)

func (m *manualTimer) Reset(duration time.Duration) {
	m.Stop()
	m.at = m.m.Now().Add(duration)
	m.ch = make(chan time.Time, 1)
	m.m.add(m)
}

func (m *manualTimer) Ch() <-chan time.Time {
	return m.ch
}

func (m *manualTimer) Stop() bool {
	removed := m.m.remove(m)
	m.ch = nil
	m.at = time.Time{}
	return removed
}

func (m *manualTimer) MarkRead() {}

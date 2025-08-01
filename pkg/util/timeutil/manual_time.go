// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package timeutil

import (
	"container/heap"
	"container/list"
	"fmt"
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
		// tickers is a list with element type *manualTicker.
		tickers list.List
	}
}

// NewManualTime constructs a new ManualTime.
func NewManualTime(initialTime time.Time) *ManualTime {
	mt := ManualTime{}
	mt.mu.now = initialTime
	mt.mu.timers = manualTimerQueue{
		m: make(map[*manualTimer]int),
	}
	mt.mu.tickers.Init()
	return &mt
}

var _ TimeSource = (*ManualTime)(nil)

// Now returns the current time.
func (m *ManualTime) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mu.now
}

// Since implements TimeSource interface
func (m *ManualTime) Since(t time.Time) time.Duration {
	return m.Now().Sub(t)
}

// NewTimer constructs a new timer.
func (m *ManualTime) NewTimer() TimerI {
	return &manualTimer{m: m}
}

// NewTicker creates a new ticker.
func (m *ManualTime) NewTicker(duration time.Duration) TickerI {
	if duration <= 0 {
		panic("non-positive interval for NewTicker")
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	t := &manualTicker{
		m:        m,
		duration: duration,
		nextTick: m.mu.now.Add(duration),
		// We allocate a big buffer so that sending a tick never blocks.
		ch: make(chan time.Time, 10000),
	}
	t.element = m.mu.tickers.PushBack(t)
	return t
}

// Advance forwards the current time by the given duration.
func (m *ManualTime) Advance(duration time.Duration) {
	m.AdvanceTo(m.Now().Add(duration))
}

// Backwards moves the clock back by duration. Duration is expected to be
// positive, and it will be subtracted from the current time.
func (m *ManualTime) Backwards(duration time.Duration) {
	if duration < 0 {
		panic("invalid negative duration")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// No timers fire when the clock goes backwards.
	m.mu.now = m.mu.now.Add(-duration)
}

// AdvanceTo advances the current time to t. If t is earlier than the current
// time then AdvanceTo is a no-op.
func (m *ManualTime) AdvanceTo(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.advanceToLocked(now)
}

// MustAdvanceTo is like AdvanceTo, except it panics if now is below m's current time.
func (m *ManualTime) MustAdvanceTo(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if now.Before(m.mu.now) {
		panic(fmt.Sprintf("attempting to move ManualTime backwards from %s to %s", m.mu.now, now))
	}
	m.advanceToLocked(now)
}

func (m *ManualTime) advanceToLocked(now time.Time) {
	if !now.After(m.mu.now) {
		return
	}
	m.mu.now = now

	// Fire off any timers.
	for m.mu.timers.Len() > 0 {
		next := m.mu.timers.heap[0]
		if next.at.After(now) {
			break
		}
		next.ch <- next.at
		heap.Pop(&m.mu.timers)
	}

	// Fire off any tickers.
	for e := m.mu.tickers.Front(); e != nil; e = e.Next() {
		t := e.Value.(*manualTicker)
		for !t.nextTick.After(now) {
			select {
			case t.ch <- t.nextTick:
			default:
				panic("ticker channel full")
			}
			t.nextTick = t.nextTick.Add(t.duration)
		}
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

func (m *ManualTime) removeTimer(mt *manualTimer) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if idx, ok := m.mu.timers.m[mt]; ok {
		heap.Remove(&m.mu.timers, idx)
		return true
	}
	return false
}

func (m *ManualTime) removeTicker(t *manualTicker) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if t.element != nil {
		m.mu.tickers.Remove(t.element)
		t.element = nil
	}
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
	removed := m.m.removeTimer(m)
	m.ch = nil
	m.at = time.Time{}
	return removed
}

func (m *manualTimer) Ch() <-chan time.Time {
	return m.ch
}

func (m *manualTimer) MarkRead() {}

type manualTicker struct {
	m       *ManualTime
	element *list.Element

	duration time.Duration
	nextTick time.Time
	ch       chan time.Time
}

// Reset is part of the TickerI interface.
func (t *manualTicker) Reset(duration time.Duration) {
	panic("not implemented")
}

// Stop is part of the TickerI interface.
func (t *manualTicker) Stop() {
	t.m.removeTicker(t)
}

// Ch is part of the TickerI interface.
func (t *manualTicker) Ch() <-chan time.Time {
	return t.ch
}

// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package replica_rac2

import (
	"container/heap"
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type streamCloseScheduler struct {
	stopper   *stop.Stopper
	clock     *hlc.Clock
	scheduler RaftScheduler

	mu struct {
		syncutil.Mutex
		scheduled scheduledQueue
	}
}

type scheduledCloseEvent struct {
	rangeID roachpb.RangeID
	at      time.Time
}

// scheduledQueue implements the heap.Interface.
type scheduledQueue struct {
	items []scheduledCloseEvent
}

func newStreamCloseScheduler(
	stopper *stop.Stopper, clock *hlc.Clock, scheduler RaftScheduler,
) *streamCloseScheduler {
	return &streamCloseScheduler{stopper: stopper, scheduler: scheduler, clock: clock}
}

// ScheduleSendStreamCloseRaftMuLocked schedules a callback with a raft event
// after the given delay.
//
// Requires raftMu to be held.
func (s *streamCloseScheduler) ScheduleSendStreamCloseRaftMuLocked(
	ctx context.Context, delay time.Duration, rangeID roachpb.RangeID,
) {
	s.mu.Lock()
	defer s.mu.Unlock()

	heap.Push(&s.mu.scheduled, scheduledCloseEvent{
		rangeID: rangeID,
		at:      s.clock.PhysicalTime().Add(delay),
	})
	if s.mu.scheduled.Len() == 1 {
		// This is the first item in the queue, so start the scheduler loop.
		_ = s.stopper.RunAsyncTask(ctx,
			"flow-control-stream-close-scheduler", s.start)
	}
}

func (s *streamCloseScheduler) start(ctx context.Context) {
	var (
		delay time.Duration
		done  bool
		now   = s.clock.PhysicalTime()
	)
	if delay, done = s.nextDelay(now); done {
		// Nothing to do.
		return
	}
	// There is something to do, create a ticker and start looping until there
	// are no more events to process.
	ticker := time.NewTicker(delay)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopper.ShouldQuiesce():
			return
		case <-ticker.C:
			now = s.clock.PhysicalTime()
			for _, event := range s.readyEvents(now) {
				s.scheduler.EnqueueRaftReady(event.rangeID)
			}
			delay, done = s.nextDelay(now)
			if done {
				return
			}
			ticker.Reset(delay)
		}
	}
}

// nextDelay returns the time to wait until the next event is ready or true if
// there are no more events to wait for processing.
func (s *streamCloseScheduler) nextDelay(now time.Time) (delay time.Duration, done bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mu.scheduled.Len() > 0 {
		next := heap.Pop(&s.mu.scheduled).(scheduledCloseEvent)
		delay = next.at.Sub(now)
		heap.Push(&s.mu.scheduled, next)
	}
	return delay, s.mu.scheduled.Len() == 0
}

// readyEventsLocked returns a slice scheduled events which are ready.
func (s *streamCloseScheduler) readyEvents(now time.Time) []scheduledCloseEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	var events []scheduledCloseEvent
	for s.mu.scheduled.Len() > 0 {
		next := heap.Pop(&s.mu.scheduled).(scheduledCloseEvent)
		if next.at.Before(now) {
			heap.Push(&s.mu.scheduled, next)
			break
		}
		events = append(events, next)
	}
	return events
}

// Len returns the number of items in the priority queue.
func (pq *scheduledQueue) Len() int {
	return len(pq.items)
}

// Less reports whether the element with index i should sort before the element
// with index j.
func (pq *scheduledQueue) Less(i, j int) bool {
	return pq.items[i].at.Before(pq.items[j].at)
}

// Swap swaps the elements with indexes i and j.
func (pq *scheduledQueue) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
}

// Push adds x as an element to the priority queue.
func (pq *scheduledQueue) Push(x interface{}) {
	item := x.(scheduledCloseEvent)
	pq.items = append(pq.items, item)
}

// Pop removes and returns the minimum element (according to Less) from the
// priority queue.
func (pq *scheduledQueue) Pop() interface{} {
	old := pq.items
	n := len(old)
	item := old[n-1]
	pq.items = old[0 : n-1]
	return item
}

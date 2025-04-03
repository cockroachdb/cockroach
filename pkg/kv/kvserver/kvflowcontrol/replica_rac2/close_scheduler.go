// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package replica_rac2

import (
	"container/heap"
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/rac2"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type streamCloseScheduler struct {
	clock     timeutil.TimeSource
	scheduler RaftScheduler
	// nonEmptyCh is used to signal the scheduler that there are events to
	// process. When the heap is empty, the scheduler will wait for the next
	// event to be added before processing, by waiting on this channel.
	nonEmptyCh chan struct{}

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

func NewStreamCloseScheduler(
	clock timeutil.TimeSource, scheduler RaftScheduler,
) *streamCloseScheduler {
	return &streamCloseScheduler{scheduler: scheduler, clock: clock}
}

func (s *streamCloseScheduler) Start(ctx context.Context, stopper *stop.Stopper) error {
	s.nonEmptyCh = make(chan struct{}, 1)
	return stopper.RunAsyncTask(ctx, "flow-control-stream-close-scheduler",
		func(ctx context.Context) { s.run(ctx, stopper) })
}

// streamCloseScheduler implements the rac2.ProbeToCloseTimerScheduler
// interface.
var _ rac2.ProbeToCloseTimerScheduler = &streamCloseScheduler{}

// ScheduleSendStreamCloseRaftMuLocked schedules a callback with a raft event
// after the given delay.
//
// Requires raftMu to be held.
func (s *streamCloseScheduler) ScheduleSendStreamCloseRaftMuLocked(
	ctx context.Context, rangeID roachpb.RangeID, delay time.Duration,
) {
	now := s.clock.Now()
	event := scheduledCloseEvent{
		rangeID: rangeID,
		at:      now.Add(delay),
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	curLen := s.mu.scheduled.Len()
	recalcDelay := (curLen > 0 && s.mu.scheduled.items[0].at.After(event.at)) || curLen == 0
	heap.Push(&s.mu.scheduled, event)
	if recalcDelay {
		// This is the first item in the queue, or this item is scheduled ahead of
		// the first currently scheduled item, so signal the scheduler to
		// recalculate the delay.
		select {
		case s.nonEmptyCh <- struct{}{}:
		default:
		}
	}
}

// maxStreamCloserDelay is the maximum time the stream closer will wait before
// checking for the next event. When there are no events to process, this
// constant is used to avoid the timer from signaling.
const maxStreamCloserDelay = 24 * time.Hour

func (s *streamCloseScheduler) run(_ context.Context, stopper *stop.Stopper) {
	timer := s.clock.NewTimer()
	timer.Reset(s.nextDelay(s.clock.Now()))
	defer timer.Stop()

	for {
		// When there are no more events to wait for, the timer is set to the
		// maxStreamCloserDelay. When an event is added, the nonEmptyCh will be
		// signaled and the timer will be reset to the next event's delay.
		select {
		case <-stopper.ShouldQuiesce():
			return
		case <-s.nonEmptyCh:
		case <-timer.Ch():
			timer.MarkRead()
		}

		now := s.clock.Now()
		for _, event := range s.readyEvents(now) {
			s.scheduler.EnqueueRaftReady(event.rangeID)
		}
		now = s.clock.Now()
		nextDelay := s.nextDelay(now)
		timer.Reset(nextDelay)
	}
}

// nextDelay returns the time to wait until the next event is ready to be
// processed, or if there are no events, returns a long duration.
func (s *streamCloseScheduler) nextDelay(now time.Time) (delay time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delay = maxStreamCloserDelay
	if s.mu.scheduled.Len() > 0 {
		next := heap.Pop(&s.mu.scheduled).(scheduledCloseEvent)
		if delay = next.at.Sub(now); delay == 0 {
			// A non-positive delay will cause the timer to error, so we set it to a
			// small value instead which will occur immediately.
			delay = time.Nanosecond
		}
		heap.Push(&s.mu.scheduled, next)
	}

	return delay
}

// readyEventsLocked returns a slice scheduled events which are ready.
func (s *streamCloseScheduler) readyEvents(now time.Time) []scheduledCloseEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	var events []scheduledCloseEvent
	for s.mu.scheduled.Len() > 0 {
		next := s.mu.scheduled.items[0]
		if next.at.After(now) {
			break
		}
		events = append(events, heap.Pop(&s.mu.scheduled).(scheduledCloseEvent))
	}

	return events
}

func (s scheduledCloseEvent) Less(other scheduledCloseEvent) bool {
	if s.at.Equal(other.at) {
		return s.rangeID < other.rangeID
	}
	return s.at.Before(other.at)
}

// Len returns the number of items in the priority queue.
func (pq *scheduledQueue) Len() int {
	return len(pq.items)
}

// Less reports whether the element with index i should sort before the element
// with index j.
func (pq *scheduledQueue) Less(i, j int) bool {
	return pq.items[i].Less(pq.items[j])
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

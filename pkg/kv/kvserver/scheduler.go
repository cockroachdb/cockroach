// Copyright 2016 The Cockroach Authors.
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
	"container/list"
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const rangeIDChunkSize = 1000

type rangeIDChunk struct {
	// Valid contents are buf[rd:wr], read at buf[rd], write at buf[wr].
	buf    [rangeIDChunkSize]roachpb.RangeID
	rd, wr int
}

func (c *rangeIDChunk) PushBack(id roachpb.RangeID) bool {
	if c.WriteCap() == 0 {
		return false
	}
	c.buf[c.wr] = id
	c.wr++
	return true
}

func (c *rangeIDChunk) PopFront() (roachpb.RangeID, bool) {
	if c.Len() == 0 {
		return 0, false
	}
	id := c.buf[c.rd]
	c.rd++
	return id, true
}

func (c *rangeIDChunk) WriteCap() int {
	return len(c.buf) - c.wr
}

func (c *rangeIDChunk) Len() int {
	return c.wr - c.rd
}

// rangeIDQueue is a chunked queue of range IDs. Instead of a separate list
// element for every range ID, it uses a rangeIDChunk to hold many range IDs,
// amortizing the allocation/GC cost. Using a chunk queue avoids any copying
// that would occur if a slice were used (the copying would occur on slice
// reallocation).
//
// The queue has a naive understanding of priority and fairness. For the most
// part, it implements a FIFO queueing policy with no prioritization of some
// ranges over others. However, the queue can be configured with up to one
// high-priority range, which will always be placed at the front when added.
type rangeIDQueue struct {
	len int

	// Default priority.
	chunks list.List

	// High priority.
	priorityID     roachpb.RangeID
	priorityQueued bool
}

func (q *rangeIDQueue) Push(id roachpb.RangeID) {
	q.len++
	if q.priorityID == id {
		q.priorityQueued = true
		return
	}
	if q.chunks.Len() == 0 || q.back().WriteCap() == 0 {
		q.chunks.PushBack(&rangeIDChunk{})
	}
	if !q.back().PushBack(id) {
		panic(fmt.Sprintf(
			"unable to push rangeID to chunk: len=%d, cap=%d",
			q.back().Len(), q.back().WriteCap()))
	}
}

func (q *rangeIDQueue) PopFront() (roachpb.RangeID, bool) {
	if q.len == 0 {
		return 0, false
	}
	q.len--
	if q.priorityQueued {
		q.priorityQueued = false
		return q.priorityID, true
	}
	frontElem := q.chunks.Front()
	front := frontElem.Value.(*rangeIDChunk)
	id, ok := front.PopFront()
	if !ok {
		panic("encountered empty chunk")
	}
	if front.Len() == 0 && front.WriteCap() == 0 {
		q.chunks.Remove(frontElem)
	}
	return id, true
}

func (q *rangeIDQueue) Len() int {
	return q.len
}

func (q *rangeIDQueue) SetPriorityID(id roachpb.RangeID) {
	if q.priorityID != 0 && q.priorityID != id &&
		// This assertion is temporarily disabled, see:
		// https://github.com/cockroachdb/cockroach/issues/75939
		false {
		panic(fmt.Sprintf(
			"priority range ID already set: old=%d, new=%d",
			q.priorityID, id))
	}
	q.priorityID = id
}

func (q *rangeIDQueue) back() *rangeIDChunk {
	return q.chunks.Back().Value.(*rangeIDChunk)
}

type raftProcessor interface {
	// Process a raft.Ready struct containing entries and messages that are
	// ready to read, be saved to stable storage, committed, or sent to other
	// peers.
	//
	// This method does not take a ctx; the implementation is expected to use a
	// ctx annotated with the range information, according to RangeID.
	processReady(roachpb.RangeID)
	// Process all queued messages for the specified range.
	// Return true if the range should be queued for ready processing.
	processRequestQueue(context.Context, roachpb.RangeID) bool
	// Process a raft tick for the specified range.
	// Return true if the range should be queued for ready processing.
	processTick(context.Context, roachpb.RangeID) bool
}

type raftScheduleFlags int

const (
	stateQueued raftScheduleFlags = 1 << iota
	stateRaftReady
	stateRaftRequest
	stateRaftTick
)

type raftScheduleState struct {
	flags raftScheduleFlags
	begin int64 // nanoseconds
}

type raftScheduler struct {
	ambientContext log.AmbientContext
	processor      raftProcessor
	latency        *metric.Histogram
	numWorkers     int

	mu struct {
		syncutil.Mutex
		cond    *sync.Cond
		queue   rangeIDQueue
		state   map[roachpb.RangeID]raftScheduleState
		stopped bool
	}

	done sync.WaitGroup
}

func newRaftScheduler(
	ambient log.AmbientContext, metrics *StoreMetrics, processor raftProcessor, numWorkers int,
) *raftScheduler {
	s := &raftScheduler{
		ambientContext: ambient,
		processor:      processor,
		latency:        metrics.RaftSchedulerLatency,
		numWorkers:     numWorkers,
	}
	s.mu.cond = sync.NewCond(&s.mu.Mutex)
	s.mu.state = make(map[roachpb.RangeID]raftScheduleState)
	return s
}

func (s *raftScheduler) Start(stopper *stop.Stopper) {
	ctx := s.ambientContext.AnnotateCtx(context.Background())
	waitQuiesce := func(context.Context) {
		<-stopper.ShouldQuiesce()
		s.mu.Lock()
		s.mu.stopped = true
		s.mu.Unlock()
		s.mu.cond.Broadcast()
	}
	if err := stopper.RunAsyncTaskEx(ctx,
		stop.TaskOpts{
			TaskName: "raftsched-wait-quiesce",
			// This task doesn't reference a parent because it runs for the server's
			// lifetime.
			SpanOpt: stop.SterileRootSpan,
		},
		waitQuiesce); err != nil {
		waitQuiesce(ctx)
	}

	s.done.Add(s.numWorkers)
	for i := 0; i < s.numWorkers; i++ {
		if err := stopper.RunAsyncTaskEx(ctx,
			stop.TaskOpts{
				TaskName: "raft-worker",
				// This task doesn't reference a parent because it runs for the server's
				// lifetime.
				SpanOpt: stop.SterileRootSpan,
			},
			s.worker); err != nil {
			s.done.Done()
		}
	}
}

func (s *raftScheduler) Wait(context.Context) {
	s.done.Wait()
}

// SetPriorityID configures the single range that the scheduler will prioritize
// above others. Once set, callers are not permitted to change this value.
func (s *raftScheduler) SetPriorityID(id roachpb.RangeID) {
	s.mu.Lock()
	s.mu.queue.SetPriorityID(id)
	s.mu.Unlock()
}

func (s *raftScheduler) PriorityID() roachpb.RangeID {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.queue.priorityID
}

func (s *raftScheduler) worker(ctx context.Context) {
	defer s.done.Done()

	// We use a sync.Cond for worker notification instead of a buffered
	// channel. Buffered channels have internal overhead for maintaining the
	// buffer even when the elements are empty. And the buffer isn't necessary as
	// the raftScheduler work is already buffered on the internal queue. Lastly,
	// signaling a sync.Cond is significantly faster than selecting and sending
	// on a buffered channel.

	s.mu.Lock()
	for {
		var id roachpb.RangeID
		for {
			if s.mu.stopped {
				s.mu.Unlock()
				return
			}
			var ok bool
			if id, ok = s.mu.queue.PopFront(); ok {
				break
			}
			s.mu.cond.Wait()
		}

		// Grab and clear the existing state for the range ID. Note that we leave
		// the range ID marked as "queued" so that a concurrent Enqueue* will not
		// queue the range ID again.
		state := s.mu.state[id]
		s.mu.state[id] = raftScheduleState{flags: stateQueued}
		s.mu.Unlock()

		// Record the scheduling latency for the range.
		lat := nowNanos() - state.begin
		s.latency.RecordValue(lat)

		// Process requests first. This avoids a scenario where a tick and a
		// "quiesce" message are processed in the same iteration and intervening
		// raft ready processing unquiesces the replica because the tick triggers
		// an election.
		if state.flags&stateRaftRequest != 0 {
			// processRequestQueue returns true if the range should perform ready
			// processing. Do not reorder this below the call to processReady.
			if s.processor.processRequestQueue(ctx, id) {
				state.flags |= stateRaftReady
			}
		}
		if state.flags&stateRaftTick != 0 {
			// processRaftTick returns true if the range should perform ready
			// processing. Do not reorder this below the call to processReady.
			if s.processor.processTick(ctx, id) {
				state.flags |= stateRaftReady
			}
		}
		if state.flags&stateRaftReady != 0 {
			s.processor.processReady(id)
		}

		s.mu.Lock()
		state = s.mu.state[id]
		if state.flags == stateQueued {
			// No further processing required by the range ID, clear it from the
			// state map.
			delete(s.mu.state, id)
		} else {
			// There was a concurrent call to one of the Enqueue* methods. Queue
			// the range ID for further processing.
			//
			// Even though the Enqueue* method did not signal after detecting
			// that the range was being processed, there also is no need for us
			// to signal the condition variable. This is because this worker
			// goroutine will loop back around and continue working without ever
			// going back to sleep.
			//
			// We can prove this out through a short derivation.
			// - For optimal concurrency, we want:
			//     awake_workers = min(max_workers, num_ranges)
			// - The condition variable / mutex structure ensures that:
			//     awake_workers = cur_awake_workers + num_signals
			// - So we need the following number of signals for optimal concurrency:
			//     num_signals = min(max_workers, num_ranges) - cur_awake_workers
			// - If we re-enqueue a range that's currently being processed, the
			//   num_ranges does not change once the current iteration completes
			//   and the worker does not go back to sleep between the current
			//   iteration and the next iteration, so no change to num_signals
			//   is needed.
			s.mu.queue.Push(id)
		}
	}
}

func (s *raftScheduler) enqueue1Locked(
	addFlags raftScheduleFlags, id roachpb.RangeID, now int64,
) int {
	prevState := s.mu.state[id]
	if prevState.flags&addFlags == addFlags {
		return 0
	}
	var queued int
	newState := prevState
	newState.flags = newState.flags | addFlags
	if newState.flags&stateQueued == 0 {
		newState.flags |= stateQueued
		queued++
		s.mu.queue.Push(id)
	}
	if newState.begin == 0 {
		newState.begin = now
	}
	s.mu.state[id] = newState
	return queued
}

func (s *raftScheduler) enqueue1(addFlags raftScheduleFlags, id roachpb.RangeID) int {
	now := nowNanos()
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.enqueue1Locked(addFlags, id, now)
}

func (s *raftScheduler) enqueueN(addFlags raftScheduleFlags, ids ...roachpb.RangeID) int {
	// Enqueue the ids in chunks to avoid hold raftScheduler.mu for too long.
	const enqueueChunkSize = 128

	// Avoid locking for 0 new ranges.
	if len(ids) == 0 {
		return 0
	}

	now := nowNanos()
	s.mu.Lock()
	var count int
	for i, id := range ids {
		count += s.enqueue1Locked(addFlags, id, now)
		if (i+1)%enqueueChunkSize == 0 {
			s.mu.Unlock()
			now = nowNanos()
			s.mu.Lock()
		}
	}
	s.mu.Unlock()
	return count
}

func (s *raftScheduler) signal(count int) {
	if count >= s.numWorkers {
		s.mu.cond.Broadcast()
	} else {
		for i := 0; i < count; i++ {
			s.mu.cond.Signal()
		}
	}
}

func (s *raftScheduler) EnqueueRaftReady(id roachpb.RangeID) {
	s.signal(s.enqueue1(stateRaftReady, id))
}

func (s *raftScheduler) EnqueueRaftRequest(id roachpb.RangeID) {
	s.signal(s.enqueue1(stateRaftRequest, id))
}

func (s *raftScheduler) EnqueueRaftRequests(ids ...roachpb.RangeID) {
	s.signal(s.enqueueN(stateRaftRequest, ids...))
}

func (s *raftScheduler) EnqueueRaftTicks(ids ...roachpb.RangeID) {
	s.signal(s.enqueueN(stateRaftTick, ids...))
}

func nowNanos() int64 {
	return timeutil.Now().UnixNano()
}

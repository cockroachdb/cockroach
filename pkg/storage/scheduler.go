// Copyright 2016 The Cockroach Authors.
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

package storage

import (
	"container/list"
	"fmt"
	"sync"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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
type rangeIDQueue struct {
	chunks list.List
	len    int
}

func (q *rangeIDQueue) PushBack(id roachpb.RangeID) {
	if q.chunks.Len() == 0 || q.back().WriteCap() == 0 {
		q.chunks.PushBack(&rangeIDChunk{})
	}
	q.len++
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
	frontElem := q.chunks.Front()
	front := frontElem.Value.(*rangeIDChunk)
	id, ok := front.PopFront()
	if !ok {
		panic("encountered empty chunk")
	}
	q.len--
	if front.Len() == 0 && front.WriteCap() == 0 {
		q.chunks.Remove(frontElem)
	}
	return id, true
}

func (q *rangeIDQueue) Len() int {
	return q.len
}

func (q *rangeIDQueue) back() *rangeIDChunk {
	return q.chunks.Back().Value.(*rangeIDChunk)
}

type raftProcessor interface {
	processReady(rangeID roachpb.RangeID)
	processRequestQueue(rangeID roachpb.RangeID)
	// Process a raft tick for the specified range. Return true if the range
	// should be queued for ready processing.
	processTick(rangeID roachpb.RangeID) bool
}

type raftScheduleState int

const (
	stateQueued raftScheduleState = 1 << iota
	stateRaftReady
	stateRaftRequest
	stateRaftTick
)

type raftScheduler struct {
	processor  raftProcessor
	numWorkers int

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
		processor:  processor,
		numWorkers: numWorkers,
	}
	s.mu.cond = sync.NewCond(&s.mu.Mutex)
	s.mu.state = make(map[roachpb.RangeID]raftScheduleState)
	return s
}

func (s *raftScheduler) Start(stopper *stop.Stopper) {
	ctx := context.TODO()
	stopper.RunWorker(ctx, func(context.Context) {
		<-stopper.ShouldStop()
		s.mu.Lock()
		s.mu.stopped = true
		s.mu.Unlock()
		s.mu.cond.Broadcast()
	})

	s.done.Add(s.numWorkers)
	for i := 0; i < s.numWorkers; i++ {
		stopper.RunWorker(ctx, func(context.Context) {
			s.worker(stopper)
		})
	}
}

func (s *raftScheduler) Wait(context.Context) {
	s.done.Wait()
}

func (s *raftScheduler) worker(stopper *stop.Stopper) {
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
		s.mu.state[id] = stateQueued
		s.mu.Unlock()

		if state&stateRaftTick != 0 {
			// processRaftTick returns true if the range should perform ready
			// processing. Do not reorder this below the call to processReady.
			if s.processor.processTick(id) {
				state |= stateRaftReady
			}
		}
		if state&stateRaftReady != 0 {
			s.processor.processReady(id)
		}
		// Process requests last. This avoids a scenario where a tick and a
		// "quiesce" message are processed in the same iteration and intervening
		// raft ready processing unquiesced the replica. Note that request
		// processing could also occur first, it just shouldn't occur in between
		// ticking and ready processing. It is possible for a tick to be enqueued
		// concurrently with the quiescing in which case the replica will
		// unquiesce when the tick is processed, but we'll wake the leader in
		// that case.
		if state&stateRaftRequest != 0 {
			s.processor.processRequestQueue(id)
		}

		s.mu.Lock()
		state = s.mu.state[id]
		if state == stateQueued {
			// No further processing required by the range ID, clear it from the
			// state map.
			delete(s.mu.state, id)
		} else {
			// There was a concurrent call to one of the Enqueue* methods. Queue the
			// range ID for further processing.
			s.mu.queue.PushBack(id)
			s.mu.cond.Signal()
		}
	}
}

func (s *raftScheduler) enqueue1Locked(addState raftScheduleState, id roachpb.RangeID) int {
	prevState := s.mu.state[id]
	if prevState&addState == addState {
		return 0
	}
	var queued int
	newState := prevState | addState
	if newState&stateQueued == 0 {
		newState |= stateQueued
		queued++
		s.mu.queue.PushBack(id)
	}
	s.mu.state[id] = newState
	return queued
}

func (s *raftScheduler) enqueue1(addState raftScheduleState, id roachpb.RangeID) int {
	s.mu.Lock()
	count := s.enqueue1Locked(addState, id)
	s.mu.Unlock()
	return count
}

func (s *raftScheduler) enqueueN(addState raftScheduleState, ids ...roachpb.RangeID) int {
	// Enqueue the ids in chunks to avoid hold raftScheduler.mu for too long.
	const enqueueChunkSize = 128

	var count int
	s.mu.Lock()
	for i, id := range ids {
		count += s.enqueue1Locked(addState, id)
		if (i+1)%enqueueChunkSize == 0 {
			s.mu.Unlock()
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

func (s *raftScheduler) EnqueueRaftTick(ids ...roachpb.RangeID) {
	s.signal(s.enqueueN(stateRaftTick, ids...))
}

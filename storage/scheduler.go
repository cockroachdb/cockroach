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

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/syncutil"
)

// schedulerNoWait is a closed channel which will always return true as a case
// in a select statement.
var schedulerNoWait = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

type processor interface {
	processRaftReady(rangeID roachpb.RangeID)
	processRaftRequestQueue(rangeID roachpb.RangeID)
	// Process a raft tick for the specified range. Return true if the range
	// should be queued for ready processing.
	processRaftTick(rangeID roachpb.RangeID) bool
}

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
// ammortizing the allocation/GC cost. Using a chunk queue avoids any copying
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

type scheduleState int

const (
	stateQueued scheduleState = 1 << iota
	stateRaftReady
	stateRaftRequest
	stateRaftTick
)

type scheduler struct {
	ctx       context.Context
	processor processor

	mu struct {
		syncutil.Mutex
		queue rangeIDQueue
		state map[roachpb.RangeID]scheduleState
	}

	notify chan struct{}
}

func newScheduler(ctx context.Context, processor processor, numWorkers int) *scheduler {
	s := &scheduler{
		ctx:       ctx,
		processor: processor,
		notify:    make(chan struct{}, numWorkers),
	}
	s.mu.state = make(map[roachpb.RangeID]scheduleState)
	return s
}

func (s *scheduler) Start(stopper *stop.Stopper) {
	for i := 0; i < cap(s.notify); i++ {
		stopper.RunWorker(func() {
			s.worker(stopper)
		})
	}
}

func (s *scheduler) worker(stopper *stop.Stopper) {
	notify := s.notify
	for {
		select {
		case <-notify:
			s.mu.Lock()
			id, ok := s.mu.queue.PopFront()
			if !ok {
				s.mu.Unlock()
				// Nothing queued, wait for a notification.
				notify = s.notify
				continue
			}
			// Grab and clear the existing state for the range ID. Note that we leave
			// the range ID marked as "queued" so that a concurrent Enqueue* will not
			// queue the range ID again.
			state := s.mu.state[id]
			s.mu.state[id] = stateQueued
			s.mu.Unlock()

			// Process the range ID.
			if state&stateRaftTick != 0 {
				// processRaftTick returns true if the range should perform ready
				// processing. Do not reorder this below the call to processRaftReady.
				if s.processor.processRaftTick(id) {
					state |= stateRaftReady
				}
			}
			if state&stateRaftRequest != 0 {
				s.processor.processRaftRequestQueue(id)
			}
			if state&stateRaftReady != 0 {
				s.processor.processRaftReady(id)
			}

			var queued bool
			s.mu.Lock()
			state = s.mu.state[id]
			if state == stateQueued {
				// No further processing required by the range ID, clear it from the
				// state map.
				delete(s.mu.state, id)
			} else {
				// There was a concurrent call to one of the Enqueue* methods. Queue the
				// range ID for further processing.
				queued = true
				s.mu.queue.PushBack(id)
			}
			s.mu.Unlock()

			if queued {
				select {
				case s.notify <- struct{}{}:
				default:
				}
			}

			// Loop trying to process another replica.
			notify = schedulerNoWait

		case <-stopper.ShouldStop():
			return
		}
	}
}

func (s *scheduler) enqueue1Locked(addState scheduleState, id roachpb.RangeID) int {
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

func (s *scheduler) enqueue1(addState scheduleState, id roachpb.RangeID) int {
	s.mu.Lock()
	count := s.enqueue1Locked(addState, id)
	s.mu.Unlock()
	return count
}

func (s *scheduler) enqueueN(addState scheduleState, ids ...roachpb.RangeID) int {
	var count int
	s.mu.Lock()
	for _, id := range ids {
		count += s.enqueue1Locked(addState, id)
	}
	s.mu.Unlock()
	return count
}

func (s *scheduler) signal(count int) {
	for i := 0; i < count; i++ {
		select {
		case s.notify <- struct{}{}:
		default:
		}
	}
}

func (s *scheduler) EnqueueRaftReady(id roachpb.RangeID) {
	s.signal(s.enqueue1(stateRaftReady, id))
}

func (s *scheduler) EnqueueRaftRequest(id roachpb.RangeID) {
	s.signal(s.enqueue1(stateRaftRequest, id))
}

func (s *scheduler) EnqueueRaftTick(ids ...roachpb.RangeID) {
	s.signal(s.enqueueN(stateRaftTick, ids...))
}

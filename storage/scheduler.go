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

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/syncutil"
)

type processor interface {
	processRaftReady(rangeID roachpb.RangeID)
	processRaftRequest(rangeID roachpb.RangeID)
	processRaftTick(rangeID roachpb.RangeID)
}

const rangeIDChunkSize = 1000

type rangeIDChunk struct {
	// Valid contents are buf[rd:wr], read at buf[rd], write at buf[wr].
	buf    [rangeIDChunkSize]roachpb.RangeID
	rd, wr int
}

func (c *rangeIDChunk) Push(id roachpb.RangeID) bool {
	if c.Cap() == 0 {
		return false
	}
	c.buf[c.wr] = id
	c.wr++
	return true
}

func (c *rangeIDChunk) Pop() (roachpb.RangeID, bool) {
	if c.Len() == 0 {
		return 0, false
	}
	id := c.buf[c.rd]
	c.rd++
	return id, true
}

func (c *rangeIDChunk) Cap() int {
	return len(c.buf) - c.wr
}

func (c *rangeIDChunk) Len() int {
	return c.wr - c.rd
}

type rangeIDQueue struct {
	chunks list.List
	len    int
}

func (q *rangeIDQueue) Push(id roachpb.RangeID) {
	if q.chunks.Len() == 0 || q.back().Cap() == 0 {
		q.chunks.PushBack(&rangeIDChunk{})
	}
	q.len++
	if !q.back().Push(id) {
		panic("unable to push to new rangeID chunk")
	}
}

func (q *rangeIDQueue) Pop() (roachpb.RangeID, bool) {
	if q.len == 0 {
		return 0, false
	}
	frontElem := q.chunks.Front()
	front := frontElem.Value.(*rangeIDChunk)
	id, ok := front.Pop()
	if !ok {
		panic("encountered empty chunk")
	}
	q.len--
	if front.Len() == 0 && front.Cap() == 0 {
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
	noop   chan struct{}
}

func newScheduler(ctx context.Context, processor processor, numWorkers int) *scheduler {
	s := &scheduler{
		ctx:       ctx,
		processor: processor,
		notify:    make(chan struct{}, numWorkers),
		noop:      make(chan struct{}),
	}
	close(s.noop)
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
			id, ok := s.mu.queue.Pop()
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
			if state&stateRaftRequest != 0 {
				s.processor.processRaftRequest(id)
			}
			if state&stateRaftReady != 0 {
				s.processor.processRaftReady(id)
			}
			if state&stateRaftTick != 0 {
				s.processor.processRaftTick(id)
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
				s.mu.queue.Push(id)
			}
			s.mu.Unlock()

			if queued {
				select {
				case s.notify <- struct{}{}:
				default:
				}
			}

			// Loop trying to process another replica.
			notify = s.noop

		case <-stopper.ShouldStop():
			return
		}
	}
}

func (s *scheduler) enqueueOneLocked(id roachpb.RangeID, newState scheduleState) bool {
	prevState := s.mu.state[id]
	if prevState&newState == newState {
		return false
	}
	var queued bool
	newState |= prevState
	if newState&stateQueued == 0 {
		newState |= stateQueued
		queued = true
		s.mu.queue.Push(id)
	}
	s.mu.state[id] = newState
	return queued
}

func (s *scheduler) enqueueOne(id roachpb.RangeID, newState scheduleState) bool {
	s.mu.Lock()
	queued := s.enqueueOneLocked(id, newState)
	s.mu.Unlock()
	return queued
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
	if s.enqueueOne(id, stateRaftReady) {
		s.signal(1)
	}
}

func (s *scheduler) EnqueueRaftRequest(id roachpb.RangeID) {
	if s.enqueueOne(id, stateRaftRequest) {
		s.signal(1)
	}
}

func (s *scheduler) EnqueueRaftTick(ids ...roachpb.RangeID) {
	var count int
	s.mu.Lock()
	for _, id := range ids {
		if s.enqueueOneLocked(id, stateRaftTick) {
			count++
		}
	}
	s.mu.Unlock()
	s.signal(count)
}

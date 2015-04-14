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
	"container/heap"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
)

// A rangeItem holds a range and its priority for use with a priority queue.
type rangeItem struct {
	value    *Range
	priority float64
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

// A priorityQueue implements heap.Interface and holds rangeItems.
type priorityQueue []*rangeItem

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].priority > pq[j].priority
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index, pq[j].index = i, j
}

func (pq *priorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*rangeItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority of a rangeItem in the queue.
func (pq *priorityQueue) update(item *rangeItem, priority float64) {
	item.priority = priority
	heap.Fix(pq, item.index)
}

type queueImpl interface {
	// needsLeaderLease returns whether this queue requires the leader
	// lease to operate on a range.
	needsLeaderLease() bool

	// shouldQueue accepts current time and a Range and returns whether
	// it should be queued and if so, at what priority.
	shouldQueue(proto.Timestamp, *Range) (shouldQueue bool, priority float64)

	// process accepts current time and a range and executes
	// queue-specific work on it.
	process(proto.Timestamp, *Range) error

	// timer returns a duration to wait between processing the next item
	// from the queue.
	timer() time.Duration
}

// baseQueue is the base implementation of the rangeQueue interface.
// Queue implementations should embed a baseQueue and implement queueImpl.
//
// baseQueue is not thread safe and is intended for usage only from
// the scanner's goroutine.
type baseQueue struct {
	name       string
	impl       queueImpl
	maxSize    int                  // Maximum number of ranges to queue
	incoming   chan *Range          // Channel for ranges to be queued
	sync.Mutex                      // Mutex protects priorityQ and ranges
	priorityQ  priorityQueue        // The priority queue
	ranges     map[int64]*rangeItem // Map from RaftID to rangeItem (for updating priority)
}

// newBaseQueue returns a new instance of baseQueue with the
// specified shouldQ function to determine which ranges to queue
// and maxSize to limit the growth of the queue. Note that
// maxSize doesn't prevent new ranges from being added, it just
// limits the total size. Higher priority ranges can still be
// added; their addition simply removes the lowest priority range.
func newBaseQueue(name string, impl queueImpl, maxSize int) *baseQueue {
	return &baseQueue{
		name:     name,
		impl:     impl,
		maxSize:  maxSize,
		incoming: make(chan *Range, 10),
		ranges:   map[int64]*rangeItem{},
	}
}

// Length returns the current size of the queue.
func (bq *baseQueue) Length() int {
	bq.Lock()
	defer bq.Unlock()
	return bq.priorityQ.Len()
}

// Start launches a goroutine to process entries in the queue. The
// provided stopper is used to finish processing.
func (bq *baseQueue) Start(clock *hlc.Clock, stopper *util.Stopper) {
	bq.processLoop(clock, stopper)
}

// MaybeAdd adds the specified range if bq.shouldQ specifies it should
// be queued. Ranges are added to the queue using the priority
// returned by bq.shouldQ. If the queue is too full, an already-queued
// range with the lowest priority may be dropped.
func (bq *baseQueue) MaybeAdd(rng *Range, now proto.Timestamp) {
	bq.Lock()
	defer bq.Unlock()
	if bq.impl.needsLeaderLease() {
		if held, _ := rng.HasLeaderLease(); !held {
			return
		}
	}
	should, priority := bq.impl.shouldQueue(now, rng)
	item, ok := bq.ranges[rng.Desc().RaftID]
	if !should {
		if ok {
			bq.remove(item.index)
		}
		return
	} else if ok {
		// Range has already been added; update priority.
		bq.priorityQ.update(item, priority)
		return
	}

	log.V(1).Infof("adding range %s to %s queue", rng, bq.name)
	item = &rangeItem{value: rng, priority: priority}
	heap.Push(&bq.priorityQ, item)
	bq.ranges[rng.Desc().RaftID] = item

	// If adding this range has pushed the queue past its maximum size,
	// remove the lowest priority element.
	if pqLen := bq.priorityQ.Len(); pqLen > bq.maxSize {
		bq.remove(pqLen - 1)
	}
	// Signal the processLoop that a range has been added.
	bq.incoming <- rng
}

// MaybeRemove removes the specified range from the queue if enqueued.
func (bq *baseQueue) MaybeRemove(rng *Range) {
	bq.Lock()
	defer bq.Unlock()
	if item, ok := bq.ranges[rng.Desc().RaftID]; ok {
		log.V(1).Infof("removing range %s from %s queue", item.value, bq.name)
		bq.remove(item.index)
	}
}

// process processes the entries in the queue until the provided
// stopper signals exit.
//
// TODO(spencer): current load should factor into range processing timer.
func (bq *baseQueue) processLoop(clock *hlc.Clock, stopper *util.Stopper) {
	stopper.RunWorker(func() {
		// nextTime is set arbitrarily far into the future so that we don't
		// unecessarily check for a range to dequeue if the timer function
		// returns a short duration but the priority queue is empty.
		emptyQueue := true
		nextTime := time.Now().Add(24 * time.Hour)

		for {
			select {
			// Incoming ranges set the next time to process in the event that
			// there were previously no ranges in the queue.
			case <-bq.incoming:
				if emptyQueue {
					emptyQueue = false
					nextTime = time.Now().Add(bq.impl.timer())
				}
			// Process ranges as the timer expires.
			case <-time.After(nextTime.Sub(time.Now())):
				if !stopper.StartTask() {
					continue
				}
				start := time.Now()
				nextTime = start.Add(bq.impl.timer())
				bq.Lock()
				rng := bq.pop()
				bq.Unlock()
				if rng != nil {
					log.V(1).Infof("processing range %s from %s queue...", rng, bq.name)
					if bq.impl.needsLeaderLease() {
						if held, _ := rng.HasLeaderLease(); !held {
							log.V(1).Infof("lost required leader lease; skipping...")
							continue
						}
					}
					if err := bq.impl.process(clock.Now(), rng); err != nil {
						log.Errorf("failure processing range %s from %s queue: %s", rng, bq.name, err)
					}
					log.V(1).Infof("processed range %s from %s queue in %s", rng, bq.name, time.Now().Sub(start))
				}
				if bq.Length() == 0 {
					emptyQueue = true
					nextTime = time.Now().Add(24 * time.Hour)
				}
				stopper.FinishTask()

			// Exit on stopper.
			case <-stopper.ShouldStop():
				bq.Lock()
				bq.ranges = map[int64]*rangeItem{}
				bq.priorityQ = nil
				bq.Unlock()
				return
			}
		}
	})
}

// pop dequeues the highest priority range in the queue. Returns the
// range if not empty; otherwise, returns nil. Expects mutex to be
// locked.
func (bq *baseQueue) pop() *Range {
	if bq.priorityQ.Len() == 0 {
		return nil
	}
	item := heap.Pop(&bq.priorityQ).(*rangeItem)
	delete(bq.ranges, item.value.Desc().RaftID)
	return item.value
}

// remove removes an element from the priority queue by index. Expects
// mutex to be locked.
func (bq *baseQueue) remove(index int) {
	item := heap.Remove(&bq.priorityQ, index).(*rangeItem)
	delete(bq.ranges, item.value.Desc().RaftID)
}

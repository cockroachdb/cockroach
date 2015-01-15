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
	"time"

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

// shouldQueue accepts current time and a Range and returns whether it
// should be queued and if so, at what priority.
type shouldQueueFn func(time.Time, *Range) (shouldQueue bool, priority float64)

// processFn accepts current time and a range and executes
// queue-specific work on it.
type processFn func(time.Time, *Range) error

// baseQueue is the base implementation of the rangeQueue interface.
// Queue implementations should embed a baseQueue and provide it
// with shouldQueueFn.
//
// baseQueue is not thread safe.
type baseQueue struct {
	name      string
	shouldQ   shouldQueueFn        // Should a range be queued?
	process   processFn            // Executes queue-specific work on range
	maxSize   int                  // Maximum number of ranges to queue
	priorityQ priorityQueue        // The priority queue
	ranges    map[int64]*rangeItem // Map from RaftID to rangeItem (for updating priority)
}

// newBaseQueue returns a new instance of baseQueue with the
// specified shouldQ function to determine which ranges to queue
// and maxSize to limit the growth of the queue. Note that
// maxSize doesn't prevent new ranges from being added, it just
// limits the total size. Higher priority ranges can still be
// added; their addition simply removes the lowest priority range.
func newBaseQueue(name string, shouldQ shouldQueueFn, process processFn, maxSize int) *baseQueue {
	return &baseQueue{
		name:    name,
		shouldQ: shouldQ,
		process: process,
		maxSize: maxSize,
		ranges:  map[int64]*rangeItem{},
	}
}

// Length returns the current size of the queue.
func (bq *baseQueue) Length() int {
	return bq.priorityQ.Len()
}

// Pop dequeues and processes the highest priority range in the queue.
// Returns the range if not empty; otherwise, returns nil.
func (bq *baseQueue) Pop() *Range {
	if bq.priorityQ.Len() == 0 {
		return nil
	}
	item := heap.Pop(&bq.priorityQ).(*rangeItem)
	delete(bq.ranges, item.value.Desc.RaftID)
	log.Infof("processing range %d from %s queue with priority %f...",
		item.value.Desc.RaftID, bq.name, item.priority)
	if err := bq.process(time.Now(), item.value); err != nil {
		log.Errorf("failure processing range %d from %s queue: %s",
			item.value.Desc.RaftID, bq.name, err)
	}
	return item.value
}

// MaybeAdd adds the specified range if bq.shouldQ specifies it should
// be queued. Ranges are added to the queue using the priority
// returned by bq.shouldQ. If the queue is too full, an already-queued
// range with the lowest priority may be dropped.
func (bq *baseQueue) MaybeAdd(rng *Range) {
	should, priority := bq.shouldQ(time.Now(), rng)
	item, ok := bq.ranges[rng.Desc.RaftID]
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
	item = &rangeItem{value: rng, priority: priority}
	heap.Push(&bq.priorityQ, item)
	bq.ranges[rng.Desc.RaftID] = item

	// If adding this range has pushed the queue past its maximum size,
	// remove the lowest priority element.
	if pqLen := bq.priorityQ.Len(); pqLen > bq.maxSize {
		bq.remove(pqLen - 1)
	}
}

// MaybeRemove removes the specified range from the queue if enqueued.
func (bq *baseQueue) MaybeRemove(rng *Range) {
	if item, ok := bq.ranges[rng.Desc.RaftID]; ok {
		bq.remove(item.index)
	}
}

// Clear removes all ranges from the queue.
func (bq *baseQueue) Clear() {
	bq.ranges = map[int64]*rangeItem{}
	bq.priorityQ = nil
}

func (bq *baseQueue) remove(index int) {
	item := heap.Remove(&bq.priorityQ, index).(*rangeItem)
	delete(bq.ranges, item.value.Desc.RaftID)
}

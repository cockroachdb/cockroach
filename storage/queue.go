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
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
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

var (
	errQueueDisabled   = errors.New("queue disabled")
	errRangeNotAddable = errors.New("range shouldn't be added to queue")
)

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
	maxSize    int                         // Maximum number of ranges to queue
	incoming   chan struct{}               // Channel signalled when a new range is added to the queue.
	sync.Mutex                             // Mutex protects priorityQ and ranges
	priorityQ  priorityQueue               // The priority queue
	ranges     map[proto.RaftID]*rangeItem // Map from RaftID to rangeItem (for updating priority)
	// Some tests in this package disable queues.
	disabled int32 // updated atomically
}

// newBaseQueue returns a new instance of baseQueue with the
// specified shouldQueue function to determine which ranges to queue
// and maxSize to limit the growth of the queue. Note that
// maxSize doesn't prevent new ranges from being added, it just
// limits the total size. Higher priority ranges can still be
// added; their addition simply removes the lowest priority range.
func newBaseQueue(name string, impl queueImpl, maxSize int) *baseQueue {
	return &baseQueue{
		name:     name,
		impl:     impl,
		maxSize:  maxSize,
		incoming: make(chan struct{}, 1),
		ranges:   map[proto.RaftID]*rangeItem{},
	}
}

// Length returns the current size of the queue.
func (bq *baseQueue) Length() int {
	bq.Lock()
	defer bq.Unlock()
	return bq.priorityQ.Len()
}

// SetDisabled turns queue processing off or on as directed.
func (bq *baseQueue) SetDisabled(disabled bool) {
	if disabled {
		atomic.StoreInt32(&bq.disabled, 1)
	} else {
		atomic.StoreInt32(&bq.disabled, 0)
	}
}

// Start launches a goroutine to process entries in the queue. The
// provided stopper is used to finish processing.
func (bq *baseQueue) Start(clock *hlc.Clock, stopper *stop.Stopper) {
	bq.processLoop(clock, stopper)
}

// Add adds the specified range to the queue, regardless of the return
// value of bq.shouldQueue. The range is added with specified
// priority. If the queue is too full, the range may not be
// added. Returns an error if the range was not added.
func (bq *baseQueue) Add(rng *Range, priority float64) error {
	bq.Lock()
	defer bq.Unlock()
	return bq.addInternal(rng, true, priority)
}

// MaybeAdd adds the specified range if bq.shouldQueue specifies it should
// be queued. Ranges are added to the queue using the priority
// returned by bq.shouldQueue. If the queue is too full, an already-queued
// range with the lowest priority may be dropped.
func (bq *baseQueue) MaybeAdd(rng *Range, now proto.Timestamp) {
	bq.Lock()
	defer bq.Unlock()
	should, priority := bq.impl.shouldQueue(now, rng)
	if err := bq.addInternal(rng, should, priority); err != nil && log.V(1) {
		log.Infof("couldn't add %s to queue %s: %s", rng, bq.name, err)
	}
}

// addInternal adds the range the queue with specified priority. If the
// range is already queued, updates the existing priority. Expects the
// queue lock is held by caller. Returns an error if the range was not
// added.
func (bq *baseQueue) addInternal(rng *Range, should bool, priority float64) error {
	if atomic.LoadInt32(&bq.disabled) == 1 {
		return errQueueDisabled
	}
	item, ok := bq.ranges[rng.Desc().RaftID]
	if !should {
		if ok {
			bq.remove(item.index)
		}
		return errRangeNotAddable
	} else if ok {
		// Range has already been added; update priority.
		bq.priorityQ.update(item, priority)
		return nil
	}

	if log.V(1) {
		log.Infof("adding range %s to %s queue", rng, bq.name)
	}
	item = &rangeItem{value: rng, priority: priority}
	heap.Push(&bq.priorityQ, item)
	bq.ranges[rng.Desc().RaftID] = item

	// If adding this range has pushed the queue past its maximum size,
	// remove the lowest priority element.
	if pqLen := bq.priorityQ.Len(); pqLen > bq.maxSize {
		bq.remove(pqLen - 1)
	}
	// Signal the processLoop that a range has been added.
	select {
	case bq.incoming <- struct{}{}:
	default:
		// No need to signal again.
	}
	return nil
}

// MaybeRemove removes the specified range from the queue if enqueued.
func (bq *baseQueue) MaybeRemove(rng *Range) {
	bq.Lock()
	defer bq.Unlock()
	if item, ok := bq.ranges[rng.Desc().RaftID]; ok {
		if log.V(1) {
			log.Infof("removing range %s from %s queue", item.value, bq.name)
		}
		bq.remove(item.index)
	}
}

// processLoop processes the entries in the queue until the provided
// stopper signals exit.
//
// TODO(spencer): current load should factor into range processing timer.
func (bq *baseQueue) processLoop(clock *hlc.Clock, stopper *stop.Stopper) {

	stopper.RunWorker(func() {
		// nextTime is initially nil; we don't start any timers until the queue
		// becomes non-empty.
		var nextTime <-chan time.Time

		immediately := make(chan time.Time)
		close(immediately)

		for {
			select {
			// Incoming signal sets the next time to process if there were previously
			// no ranges in the queue.
			case <-bq.incoming:
				if nextTime == nil {
					// When a range is added, wake up immediately. This is mainly
					// to facilitate testing without unnecessary sleeps.
					nextTime = immediately

					// In case we're in a test, still block on the impl.
					bq.impl.timer()
				}
			// Process ranges as the timer expires.
			case <-nextTime:
				stopper.RunTask(func() {
					bq.processOne(clock)
				})
				if bq.Length() == 0 {
					nextTime = nil
				} else {
					nextTime = time.After(bq.impl.timer())
				}

			// Exit on stopper.
			case <-stopper.ShouldStop():
				bq.Lock()
				bq.ranges = map[proto.RaftID]*rangeItem{}
				bq.priorityQ = nil
				bq.Unlock()
				return
			}
		}
	})
}

func (bq *baseQueue) processOne(clock *hlc.Clock) {
	start := time.Now()
	bq.Lock()
	rng := bq.pop()
	bq.Unlock()
	if rng != nil {
		now := clock.Now()
		if log.V(1) {
			log.Infof("processing range %s from %s queue...", rng, bq.name)
		}
		// If the queue requires the leader lease to process the
		// range, check whether this replica has leader lease and
		// renew or acquire if necessary.
		if bq.impl.needsLeaderLease() {
			// Create a "fake" get request in order to invoke redirectOnOrAcquireLease.
			args := &proto.GetRequest{RequestHeader: proto.RequestHeader{Timestamp: now}}
			if err := rng.redirectOnOrAcquireLeaderLease(nil /* Trace */, args.Header().Timestamp); err != nil {
				if log.V(1) {
					log.Infof("this replica of %s could not acquire leader lease; skipping...", rng)
				}
				return
			}
		}
		if err := bq.impl.process(now, rng); err != nil {
			log.Errorf("failure processing range %s from %s queue: %s", rng, bq.name, err)
		} else if log.V(2) {
			log.Infof("processed range %s from %s queue in %s", rng, bq.name, time.Now().Sub(start))
		}
	}
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

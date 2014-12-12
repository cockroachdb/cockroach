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
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// A rangeQueue is a prioritized queue of ranges for which work is
// scheduled. For example, there's a GC queue for ranges which are due
// for garbage collection, a rebalance queue to move ranges from full
// or busy stores, a recovery queue for ranges with dead replicas,
// etc.
type rangeQueue interface {
	// maybeAdd adds the range to the queue if the range meets
	// the queue's inclusion criteria and the queue is not already
	// too full, etc.
	maybeAdd(*Range)
	// maybeRemove removes the range from the queue if it is present.
	maybeRemove(*Range)
	// clear clears all ranges from the queue.
	clear()
}

// A rangeIterator provides access to a sequence of ranges to consider
// for inclusion in range queues. There are no requirements for the
// ordering of the iteration.
type rangeIterator interface {
	// next returns the next range in the iteration. Returns nil if
	// there are no more ranges.
	next() *Range
	// estimatedCount returns the number of ranges estimated to remain
	// in the iteration. This value does not need to be exact.
	estimatedCount() int
	// reset restarts the iterator at the beginning.
	reset()
}

// A rangeScanner iterates over ranges at a measured pace in order to
// complete approximately one full scan per interval. Each range is
// tested for inclusion in a sequence of prioritized range queues.
type rangeScanner struct {
	interval time.Duration // Duration interval for scan loop
	iter     rangeIterator // Iterator to implement scan of ranges
	queues   []rangeQueue  // Range queues managed by this scanner
	removed  chan *Range   // Ranges to remove from queues
	count    int64         // Count of times through the scanning loop
	stopper  *util.Stopper
}

// newRangeScanner creates a new range scanner with the provided
// loop interval, range iterator, and range queues.
func newRangeScanner(interval time.Duration, iter rangeIterator, queues []rangeQueue) *rangeScanner {
	return &rangeScanner{
		interval: interval,
		iter:     iter,
		queues:   queues,
		removed:  make(chan *Range, 10),
		stopper:  util.NewStopper(1),
	}
}

// start spins up the scanning loop. Call stop() to exit the loop.
func (rs *rangeScanner) start() {
	go rs.scanLoop()
}

// stop stops the scanning loop.
func (rs *rangeScanner) stop() {
	rs.stopper.Stop()
	for _, q := range rs.queues {
		q.clear()
	}
}

// loopCount returns the number of times the scanner has cycled
// through all ranges.
func (rs *rangeScanner) loopCount() int64 {
	return atomic.LoadInt64(&rs.count)
}

// removeRange removes a range from any range queues the scanner may
// have placed it in. This method should be called by the Store
// when a range is removed (e.g. rebalanced or merged).
func (rs *rangeScanner) removeRange(rng *Range) {
	rs.removed <- rng
}

// scanLoop loops endlessly, scanning through ranges available via
// the range iterator, or until the scanner is stopped. The iteration
// is paced to complete a full scan in approximately the scan interval.
func (rs *rangeScanner) scanLoop() {
	start := time.Now()

	for {
		elapsed := time.Now().Sub(start)
		remainingNanos := rs.interval.Nanoseconds() - elapsed.Nanoseconds()
		if remainingNanos < 0 {
			remainingNanos = 0
		}
		nextIteration := time.Duration(remainingNanos)
		if count := rs.iter.estimatedCount(); count > 0 {
			nextIteration = time.Duration(remainingNanos / int64(count))
		}
		log.V(6).Infof("next range scan iteration in %s", nextIteration)

		select {
		case <-time.After(nextIteration):
			rng := rs.iter.next()
			if rng != nil {
				// Try adding range to all queues.
				for _, q := range rs.queues {
					q.maybeAdd(rng)
				}
			} else {
				// Otherwise, reset iteration and start time.
				rs.iter.reset()
				start = time.Now()
				atomic.AddInt64(&rs.count, 1)
				log.V(6).Infof("reset range scan iteration")
			}

		case rng := <-rs.removed:
			// Remove range from all queues as applicable.
			for _, q := range rs.queues {
				q.maybeRemove(rng)
			}
			log.V(6).Infof("removed range %s", rng)

		case <-rs.stopper.ShouldStop():
			// Exit the loop.
			rs.stopper.SetStopped()
			return
		}
	}
}

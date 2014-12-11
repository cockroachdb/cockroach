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
	"time"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

var (
	// scanInterval is the target for the duration of a single scan
	// through available ranges. The scan is slowed as necessary to
	// approximate this duration.
	scanInterval = 10 * time.Minute
)

// A rangeQueue is a prioritized queue of ranges for which work is
// schedule. For example, there's a GC queue for ranges which are due
// for garbage collection, a rebalance queue to move ranges from full
// or busy stores, a recovery queue for ranges with dead replicas,
// etc.
type rangeQueue interface {
	// MaybeAdd adds the range to the queue if the range meets
	// the queue's inclusion criteria and the queue is not already
	// too full, etc.
	MaybeAdd(*Range)
	// MaybeRemove removes the range from the queue if it is present.
	MaybeRemove(*Range)
	// Clear clears all ranges from the queue.
	Clear()
}

// A rangeIterator provides access to a sequence of ranges to consider
// for inclusion in range queues. There are no requirements for the
// ordering of the iteration.
type rangeIterator interface {
	// Next returns the next range in the iteration and the number of
	// ranges remaining. The number of ranges remaining INCLUDES the
	// range returned with this invocation of Next(). If the returned
	// range is non-nil, but is the last range, remaining will be 1.
	// If there are no more ranges, returns nil, 0.
	Next() (*Range, int)
	// Reset restarts the iterator at the beginning.
	Reset()
}

// A rangeScanner iterates over ranges at a measured pace in order to
// complete one full scan in approximately the scanInterval. Each
// range is tested for inclusion in a sequence of prioritized range
// queues.
type rangeScanner struct {
	iter    rangeIterator // Iterator to implement scan of ranges
	queues  []rangeQueue  // Range queues managed by this scanner
	removed chan *Range   // Ranges to remove from queues
	stopper *util.Stopper
}

// newRangeScanner creates a new range scanner with the provided
// iterator.
func newRangeScanner(iter rangeIterator, queues []rangeQueue) *rangeScanner {
	return &rangeScanner{
		iter:    iter,
		queues:  queues,
		removed: make(chan *Range, 10),
		stopper: util.NewStopper(1),
	}
}

// start spins up the scanning loop. Call stop() to exit the loop.
func (rs *rangeScanner) start() {
	go rs.scanLoop()
}

// stop stops the scanning loop.
func (rs *rangeScanner) stop() {
	rs.stopper.Stop()
	rs.foreachQueue(func(q rangeQueue) { q.Clear() })
}

// removeRange removes a range from any range queues the scanner may
// have placed it in. This method should be called by the Store
// when a range is removed (e.g. rebalanced or merged).
func (rs *rangeScanner) removeRange(rng *Range) {
	rs.removed <- rng
}

// scanLoop loops endlessly, scanning through ranges available via
// the range iterator, or until the scanner is stopped. The iteration
// is paced to complete a full scan in approximately the scanInterval.
func (rs *rangeScanner) scanLoop() {
	start := time.Now()
	rng, remaining := rs.iter.Next()
	nextIteration := time.Duration(scanInterval.Nanoseconds() / int64(remaining+1))

	for {
		select {
		case <-time.After(nextIteration):
			if rng != nil {
				// Try adding range to all queues.
				rs.foreachQueue(func(q rangeQueue) { q.MaybeAdd(rng) })
			} else {
				// Otherwise, reset iteration and start time.
				rs.iter.Reset()
				start = time.Now()
				log.V(6).Infof("reset range scan iteration")
			}
			// Get next range in iteration and compute expected wait time
			// for next iteration.
			rng, remaining = rs.iter.Next()
			elapsed := time.Now().Sub(start)
			remainingNanos := scanInterval.Nanoseconds() - elapsed.Nanoseconds()
			if remainingNanos < 0 {
				remainingNanos = 0
			}
			nextIteration = time.Duration(remainingNanos / int64(remaining+1))
			log.V(6).Infof("next range scan iteration in %s", nextIteration)

		case removeRng := <-rs.removed:
			// Remove range from all queues as applicable.
			rs.foreachQueue(func(q rangeQueue) { q.MaybeRemove(removeRng) })
			log.V(6).Infof("removed range %s", removeRng)

		case <-rs.stopper.ShouldStop():
			// Exit the loop.
			rs.stopper.SetStopped()
			return
		}
	}
}

// foreachQueue iterates over the range queues managed by this scanner
// and executes the provided method for each.
func (rs *rangeScanner) foreachQueue(fn func(q rangeQueue)) {
	for _, q := range rs.queues {
		fn(q)
	}
}

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
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
)

// A rangeQueue is a prioritized queue of ranges for which work is
// scheduled. For example, there's a GC queue for ranges which are due
// for garbage collection, a rebalance queue to move ranges from full
// or busy stores, a recovery queue for ranges with dead replicas,
// etc.
type rangeQueue interface {
	// Start launches a goroutine to process the contents of the queue.
	// The provided stopper is used to signal that the goroutine should exit.
	Start(*hlc.Clock, *stop.Stopper)
	// MaybeAdd adds the range to the queue if the range meets
	// the queue's inclusion criteria and the queue is not already
	// too full, etc.
	MaybeAdd(*Range, proto.Timestamp)
	// MaybeRemove removes the range from the queue if it is present.
	MaybeRemove(*Range)
}

// A rangeSet provides access to a sequence of ranges to consider
// for inclusion in range queues. There are no requirements for the
// ordering of the iteration.
type rangeSet interface {
	// Visit calls the given function for every range in the set btree
	// until the function returns false.
	Visit(func(*Range) bool)
	// EstimatedCount returns the number of ranges estimated to remain
	// in the iteration. This value does not need to be exact.
	EstimatedCount() int
}

// A storeStats holds statistics over the entire store. Stats is an
// aggregation of MVCC stats across all ranges in the store.
type storeStats struct {
	RangeCount int
	MVCC       engine.MVCCStats
}

// A rangeScanner iterates over ranges at a measured pace in order to
// complete approximately one full scan per target interval in a large
// store (in small stores it may complete faster than the target
// interval).  Each range is tested for inclusion in a sequence of
// prioritized range queues.
type rangeScanner struct {
	targetInterval time.Duration // Target duration interval for scan loop
	maxIdleTime    time.Duration // Max idle time for scan loop
	ranges         rangeSet      // Ranges to be scanned
	queues         []rangeQueue  // Range queues managed by this scanner
	removed        chan *Range   // Ranges to remove from queues
	// Count of times and total duration through the scanning loop but locked by the completedScan
	// mutex.
	completedScan *sync.Cond
	count         int64
	total         time.Duration
}

// newRangeScanner creates a new range scanner with the provided loop intervals,
// range set, and range queues.  If scanFn is not nil, after a complete
// loop that function will be called.
func newRangeScanner(targetInterval, maxIdleTime time.Duration, ranges rangeSet) *rangeScanner {
	return &rangeScanner{
		targetInterval: targetInterval,
		maxIdleTime:    maxIdleTime,
		ranges:         ranges,
		removed:        make(chan *Range, 10),
		completedScan:  sync.NewCond(&sync.Mutex{}),
	}
}

// AddQueues adds a variable arg list of queues to the range scanner.
// This method may only be called before Start().
func (rs *rangeScanner) AddQueues(queues ...rangeQueue) {
	rs.queues = append(rs.queues, queues...)
}

// Start spins up the scanning loop. Call Stop() to exit the loop.
func (rs *rangeScanner) Start(clock *hlc.Clock, stopper *stop.Stopper) {
	for _, queue := range rs.queues {
		queue.Start(clock, stopper)
	}
	rs.scanLoop(clock, stopper)
}

// Count returns the number of times the scanner has cycled through
// all ranges.
func (rs *rangeScanner) Count() int64 {
	rs.completedScan.L.Lock()
	defer rs.completedScan.L.Unlock()
	return rs.count
}

// avgScan returns the average scan time of each scan cycle. Used in unittests.
func (rs *rangeScanner) avgScan() time.Duration {
	rs.completedScan.L.Lock()
	defer rs.completedScan.L.Unlock()
	return time.Duration(rs.total.Nanoseconds() / int64(rs.count))
}

// RemoveRange removes a range from any range queues the scanner may
// have placed it in. This method should be called by the Store
// when a range is removed (e.g. rebalanced or merged).
func (rs *rangeScanner) RemoveRange(rng *Range) {
	rs.removed <- rng
}

// WaitForScanCompletion waits until the end of the next scan and returns the
// total number of scans completed so far.
func (rs *rangeScanner) WaitForScanCompletion() int64 {
	rs.completedScan.L.Lock()
	defer rs.completedScan.L.Unlock()
	initalValue := rs.count
	for rs.count == initalValue {
		rs.completedScan.Wait()
	}
	return rs.count
}

// paceInterval returns a duration between iterations to allow us to pace
// the scan.
func (rs *rangeScanner) paceInterval(start, now time.Time) time.Duration {
	elapsed := now.Sub(start)
	remainingNanos := rs.targetInterval.Nanoseconds() - elapsed.Nanoseconds()
	if remainingNanos < 0 {
		remainingNanos = 0
	}
	count := rs.ranges.EstimatedCount()
	if count < 1 {
		count = 1
	}
	interval := time.Duration(remainingNanos / int64(count))
	if rs.maxIdleTime > 0 && interval > rs.maxIdleTime {
		interval = rs.maxIdleTime
	}
	return interval
}

// waitAndProcess waits for the pace interval and processes the range
// if rng is not nil. The method returns true when the scanner needs
// to be stopped. The method also removes a range from queues when it
// is signaled via the removed channel.
func (rs *rangeScanner) waitAndProcess(start time.Time, clock *hlc.Clock, stopper *stop.Stopper,
	rng *Range) bool {
	waitInterval := rs.paceInterval(start, time.Now())
	nextTime := time.After(waitInterval)
	if log.V(6) {
		log.Infof("Wait time interval set to %s", waitInterval)
	}
	for {
		select {
		case <-nextTime:
			if rng == nil {
				return false
			}
			if !stopper.StartTask() {
				return true
			}
			// Try adding range to all queues.
			for _, q := range rs.queues {
				q.MaybeAdd(rng, clock.Now())
			}
			stopper.FinishTask()
			return false
		case rng := <-rs.removed:
			// Remove range from all queues as applicable.
			for _, q := range rs.queues {
				q.MaybeRemove(rng)
			}
			if log.V(6) {
				log.Infof("removed range %s", rng)
			}
		case <-stopper.ShouldStop():
			return true
		}
	}
}

// scanLoop loops endlessly, scanning through ranges available via
// the range set, or until the scanner is stopped. The iteration
// is paced to complete a full scan in approximately the scan interval.
func (rs *rangeScanner) scanLoop(clock *hlc.Clock, stopper *stop.Stopper) {
	stopper.RunWorker(func() {
		start := time.Now()

		for {
			if rs.ranges.EstimatedCount() == 0 {
				// Just wait without processing any range.
				if rs.waitAndProcess(start, clock, stopper, nil) {
					break
				}
			} else {
				shouldStop := true
				rs.ranges.Visit(func(rng *Range) bool {
					shouldStop = rs.waitAndProcess(start, clock, stopper, rng)
					return !shouldStop
				})
				if shouldStop {
					break
				}
			}

			if !stopper.StartTask() {
				// Exit the loop.
				break
			}

			// Increment iteration count.
			rs.completedScan.L.Lock()
			rs.count++
			rs.total += time.Now().Sub(start)
			rs.completedScan.Broadcast()
			rs.completedScan.L.Unlock()
			if log.V(6) {
				log.Infof("reset range scan iteration")
			}

			// Reset iteration and start time.
			start = time.Now()
			stopper.FinishTask()
		}
	})
}

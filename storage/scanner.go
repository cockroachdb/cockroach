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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

// A replicaQueue is a prioritized queue of replicas for which work is
// scheduled. For example, there's a GC queue for replicas which are due
// for garbage collection, a rebalance queue to move replicas from full
// or busy stores, a recovery queue for replicas of ranges with dead replicas,
// etc.
type replicaQueue interface {
	// Start launches a goroutine to process the contents of the queue.
	// The provided stopper is used to signal that the goroutine should exit.
	Start(*hlc.Clock, *stop.Stopper)
	// MaybeAdd adds the replica to the queue if the replica meets
	// the queue's inclusion criteria and the queue is not already
	// too full, etc.
	MaybeAdd(*Replica, roachpb.Timestamp)
	// MaybeRemove removes the replica from the queue if it is present.
	MaybeRemove(*Replica)
}

// A replicaSet provides access to a sequence of replicas to consider
// for inclusion in replica queues. There are no requirements for the
// ordering of the iteration.
type replicaSet interface {
	// Visit calls the given function for every replica in the set btree
	// until the function returns false.
	Visit(func(*Replica) bool)
	// EstimatedCount returns the number of replicas estimated to remain
	// in the iteration. This value does not need to be exact.
	EstimatedCount() int
}

// A replicaScanner iterates over replicas at a measured pace in order to
// complete approximately one full scan per target interval in a large
// store (in small stores it may complete faster than the target
// interval).  Each replica is tested for inclusion in a sequence of
// prioritized replica queues.
type replicaScanner struct {
	targetInterval time.Duration  // Target duration interval for scan loop
	maxIdleTime    time.Duration  // Max idle time for scan loop
	waitTimer      util.Timer     // Shared timer to avoid allocations.
	replicas       replicaSet     // Replicas to be scanned
	queues         []replicaQueue // Replica queues managed by this scanner
	removed        chan *Replica  // Replicas to remove from queues
	// Count of times and total duration through the scanning loop but locked by the completedScan
	// mutex.
	completedScan *sync.Cond
	count         int64
	total         time.Duration
}

// newReplicaScanner creates a new replica scanner with the provided loop intervals,
// replica set, and replica queues.  If scanFn is not nil, after a complete
// loop that function will be called.
func newReplicaScanner(targetInterval, maxIdleTime time.Duration, replicas replicaSet) *replicaScanner {
	if targetInterval <= 0 {
		log.Fatalf("scanner interval must be greater than zero")
	}
	return &replicaScanner{
		targetInterval: targetInterval,
		maxIdleTime:    maxIdleTime,
		replicas:       replicas,
		removed:        make(chan *Replica, 10),
		completedScan:  sync.NewCond(&sync.Mutex{}),
	}
}

// AddQueues adds a variable arg list of queues to the replica scanner.
// This method may only be called before Start().
func (rs *replicaScanner) AddQueues(queues ...replicaQueue) {
	rs.queues = append(rs.queues, queues...)
}

// Start spins up the scanning loop.
func (rs *replicaScanner) Start(clock *hlc.Clock, stopper *stop.Stopper) {
	for _, queue := range rs.queues {
		queue.Start(clock, stopper)
	}
	rs.scanLoop(clock, stopper)
}

// Count returns the number of times the scanner has cycled through
// all replicas.
func (rs *replicaScanner) Count() int64 {
	rs.completedScan.L.Lock()
	defer rs.completedScan.L.Unlock()
	return rs.count
}

// avgScan returns the average scan time of each scan cycle. Used in unittests.
func (rs *replicaScanner) avgScan() time.Duration {
	rs.completedScan.L.Lock()
	defer rs.completedScan.L.Unlock()
	return time.Duration(rs.total.Nanoseconds() / int64(rs.count))
}

// RemoveReplica removes a replica from any replica queues the scanner may
// have placed it in. This method should be called by the Store
// when a replica is removed (e.g. rebalanced or merged).
func (rs *replicaScanner) RemoveReplica(repl *Replica) {
	rs.removed <- repl
}

// paceInterval returns a duration between iterations to allow us to pace
// the scan.
func (rs *replicaScanner) paceInterval(start, now time.Time) time.Duration {
	elapsed := now.Sub(start)
	remainingNanos := rs.targetInterval.Nanoseconds() - elapsed.Nanoseconds()
	if remainingNanos < 0 {
		remainingNanos = 0
	}
	count := rs.replicas.EstimatedCount()
	if count < 1 {
		count = 1
	}
	interval := time.Duration(remainingNanos / int64(count))
	if rs.maxIdleTime > 0 && interval > rs.maxIdleTime {
		interval = rs.maxIdleTime
	}
	return interval
}

// waitAndProcess waits for the pace interval and processes the replica
// if repl is not nil. The method returns true when the scanner needs
// to be stopped. The method also removes a replica from queues when it
// is signaled via the removed channel.
func (rs *replicaScanner) waitAndProcess(start time.Time, clock *hlc.Clock, stopper *stop.Stopper,
	repl *Replica) bool {
	waitInterval := rs.paceInterval(start, timeutil.Now())
	rs.waitTimer.Reset(waitInterval)
	if log.V(6) {
		log.Infof("Wait time interval set to %s", waitInterval)
	}
	for {
		select {
		case <-rs.waitTimer.C:
			rs.waitTimer.Read = true
			if repl == nil {
				return false
			}

			return !stopper.RunTask(func() {
				// Try adding replica to all queues.
				for _, q := range rs.queues {
					q.MaybeAdd(repl, clock.Now())
				}
			})
		case repl := <-rs.removed:
			// Remove replica from all queues as applicable.
			for _, q := range rs.queues {
				q.MaybeRemove(repl)
			}
			if log.V(6) {
				log.Infof("removed replica %s", repl)
			}
		case <-stopper.ShouldStop():
			return true
		}
	}
}

// scanLoop loops endlessly, scanning through replicas available via
// the replica set, or until the scanner is stopped. The iteration
// is paced to complete a full scan in approximately the scan interval.
func (rs *replicaScanner) scanLoop(clock *hlc.Clock, stopper *stop.Stopper) {
	stopper.RunWorker(func() {
		start := timeutil.Now()

		// waitTimer is reset in each call to waitAndProcess.
		defer rs.waitTimer.Stop()

		for {
			var shouldStop bool
			count := 0
			rs.replicas.Visit(func(repl *Replica) bool {
				count++
				shouldStop = rs.waitAndProcess(start, clock, stopper, repl)
				return !shouldStop
			})
			if count == 0 {
				// No replicas processed, just wait.
				shouldStop = rs.waitAndProcess(start, clock, stopper, nil)
			}

			shouldStop = shouldStop || !stopper.RunTask(func() {
				// Increment iteration count.
				rs.completedScan.L.Lock()
				rs.count++
				rs.total += timeutil.Now().Sub(start)
				rs.completedScan.Broadcast()
				rs.completedScan.L.Unlock()
				if log.V(6) {
					log.Infof("reset replica scan iteration")
				}

				// Reset iteration and start time.
				start = timeutil.Now()
			})
			if shouldStop {
				return
			}
		}
	})
}

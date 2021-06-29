// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// A replicaQueue is a prioritized queue of replicas for which work is
// scheduled. For example, there's a GC queue for replicas which are due
// for garbage collection, a rebalance queue to move replicas from full
// or busy stores, a recovery queue for replicas of ranges with dead replicas,
// etc.
type replicaQueue interface {
	// Start launches a goroutine to process the contents of the queue.
	// The provided stopper is used to signal that the goroutine should exit.
	Start(*stop.Stopper)
	// MaybeAdd adds the replica to the queue if the replica meets
	// the queue's inclusion criteria and the queue is not already
	// too full, etc.
	MaybeAddAsync(context.Context, replicaInQueue, hlc.ClockTimestamp)
	// MaybeRemove removes the replica from the queue if it is present.
	MaybeRemove(roachpb.RangeID)
	// Name returns the name of the queue.
	Name() string
	// NeedsLease returns whether the queue requires a replica to be leaseholder.
	NeedsLease() bool
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
	log.AmbientContext
	clock   *hlc.Clock
	stopper *stop.Stopper

	targetInterval time.Duration  // Target duration interval for scan loop
	minIdleTime    time.Duration  // Min idle time for scan loop
	maxIdleTime    time.Duration  // Max idle time for scan loop
	waitTimer      timeutil.Timer // Shared timer to avoid allocations
	replicas       replicaSet     // Replicas to be scanned
	queues         []replicaQueue // Replica queues managed by this scanner
	removed        chan *Replica  // Replicas to remove from queues
	// Count of times and total duration through the scanning loop.
	mu struct {
		syncutil.Mutex
		scanCount        int64
		waitEnabledCount int64
		total            time.Duration
		// Some tests in this package disable scanning.
		disabled bool
	}
	// Used to notify processing loop if the disabled state changes.
	setDisabledCh chan struct{}
}

// newReplicaScanner creates a new replica scanner with the provided
// loop intervals, replica set, and replica queues.  If scanFn is not
// nil, after a complete loop that function will be called. If the
// targetInterval is 0, the scanner is disabled.
func newReplicaScanner(
	ambient log.AmbientContext,
	clock *hlc.Clock,
	targetInterval, minIdleTime, maxIdleTime time.Duration,
	replicas replicaSet,
) *replicaScanner {
	if targetInterval < 0 {
		panic("scanner interval must be greater than or equal to zero")
	}
	rs := &replicaScanner{
		AmbientContext: ambient,
		clock:          clock,
		targetInterval: targetInterval,
		minIdleTime:    minIdleTime,
		maxIdleTime:    maxIdleTime,
		replicas:       replicas,
		removed:        make(chan *Replica),
		setDisabledCh:  make(chan struct{}, 1),
	}
	if targetInterval == 0 {
		rs.SetDisabled(true)
	}
	return rs
}

// AddQueues adds a variable arg list of queues to the replica scanner.
// This method may only be called before Start().
func (rs *replicaScanner) AddQueues(queues ...replicaQueue) {
	rs.queues = append(rs.queues, queues...)
}

// Start spins up the scanning loop.
func (rs *replicaScanner) Start() {
	for _, queue := range rs.queues {
		queue.Start(rs.stopper)
	}
	rs.scanLoop()
}

// scanCount returns the number of times the scanner has cycled through
// all replicas.
func (rs *replicaScanner) scanCount() int64 {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.mu.scanCount
}

// waitEnabledCount returns the number of times the scanner went in the mode of
// waiting to be reenabled.
func (rs *replicaScanner) waitEnabledCount() int64 {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.mu.waitEnabledCount
}

// SetDisabled turns replica scanning off or on as directed. Note that while
// disabled, removals are still processed.
func (rs *replicaScanner) SetDisabled(disabled bool) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.mu.disabled = disabled
	// The select prevents blocking on the channel.
	select {
	case rs.setDisabledCh <- struct{}{}:
	default:
	}
}

func (rs *replicaScanner) GetDisabled() bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.mu.disabled
}

// avgScan returns the average scan time of each scan cycle. Used in unittests.
func (rs *replicaScanner) avgScan() time.Duration {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.mu.scanCount == 0 {
		return 0
	}
	return time.Duration(rs.mu.total.Nanoseconds() / rs.mu.scanCount)
}

// RemoveReplica removes a replica from any replica queues the scanner may
// have placed it in. This method should be called by the Store
// when a replica is removed (e.g. rebalanced or merged).
func (rs *replicaScanner) RemoveReplica(repl *Replica) {
	select {
	case rs.removed <- repl:
	case <-rs.stopper.ShouldQuiesce():
	}
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
	if rs.minIdleTime > 0 && interval < rs.minIdleTime {
		interval = rs.minIdleTime
	}
	if rs.maxIdleTime > 0 && interval > rs.maxIdleTime {
		interval = rs.maxIdleTime
	}
	return interval
}

// waitAndProcess waits for the pace interval and processes the replica
// if repl is not nil. The method returns true when the scanner needs
// to be stopped. The method also removes a replica from queues when it
// is signaled via the removed channel.
func (rs *replicaScanner) waitAndProcess(ctx context.Context, start time.Time, repl *Replica) bool {
	waitInterval := rs.paceInterval(start, timeutil.Now())
	rs.waitTimer.Reset(waitInterval)
	if log.V(6) {
		log.Infof(ctx, "wait timer interval set to %s", waitInterval)
	}
	for {
		select {
		case <-rs.waitTimer.C:
			if log.V(6) {
				log.Infof(ctx, "wait timer fired")
			}
			rs.waitTimer.Read = true
			if repl == nil {
				return false
			}

			if log.V(2) {
				log.Infof(ctx, "replica scanner processing %s", repl)
			}
			for _, q := range rs.queues {
				q.MaybeAddAsync(ctx, repl, rs.clock.NowAsClockTimestamp())
			}
			return false

		case repl := <-rs.removed:
			rs.removeReplica(repl)

		case <-rs.stopper.ShouldQuiesce():
			return true
		}
	}
}

func (rs *replicaScanner) removeReplica(repl *Replica) {
	// Remove replica from all queues as applicable. Note that we still
	// process removals while disabled.
	rangeID := repl.RangeID
	for _, q := range rs.queues {
		q.MaybeRemove(rangeID)
	}
	if log.V(6) {
		ctx := rs.AnnotateCtx(context.TODO())
		log.Infof(ctx, "removed replica %s", repl)
	}
}

// scanLoop loops endlessly, scanning through replicas available via
// the replica set, or until the scanner is stopped. The iteration
// is paced to complete a full scan in approximately the scan interval.
func (rs *replicaScanner) scanLoop() {
	ctx := rs.AnnotateCtx(context.Background())
	_ = rs.stopper.RunAsyncTask(ctx, "scan-loop", func(ctx context.Context) {
		start := timeutil.Now()

		// waitTimer is reset in each call to waitAndProcess.
		defer rs.waitTimer.Stop()

		for {
			if rs.GetDisabled() {
				if done := rs.waitEnabled(); done {
					return
				}
				continue
			}
			var shouldStop bool
			count := 0
			rs.replicas.Visit(func(repl *Replica) bool {
				count++
				shouldStop = rs.waitAndProcess(ctx, start, repl)
				return !shouldStop
			})
			if count == 0 {
				// No replicas processed, just wait.
				shouldStop = rs.waitAndProcess(ctx, start, nil)
			}

			// waitAndProcess returns true when the system is stopping. Note that this
			// means we don't have to check the stopper as well.
			if shouldStop {
				return
			}

			// Increment iteration count.
			func() {
				rs.mu.Lock()
				defer rs.mu.Unlock()
				rs.mu.scanCount++
				rs.mu.total += timeutil.Since(start)
			}()
			if log.V(6) {
				log.Infof(ctx, "reset replica scan iteration")
			}

			// Reset iteration and start time.
			start = timeutil.Now()
		}
	})
}

// waitEnabled loops, removing replicas from the scanner's queues,
// until scanning is enabled or the stopper signals shutdown,
func (rs *replicaScanner) waitEnabled() bool {
	rs.mu.Lock()
	rs.mu.waitEnabledCount++
	rs.mu.Unlock()
	for {
		if !rs.GetDisabled() {
			return false
		}
		select {
		case <-rs.setDisabledCh:
			continue

		case repl := <-rs.removed:
			rs.removeReplica(repl)

		case <-rs.stopper.ShouldQuiesce():
			return true
		}
	}
}

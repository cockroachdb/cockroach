// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package queue

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
)

// ReplicaPacer controls the speed of considering a replica.
type ReplicaPacer interface {
	// Next returns the next replica for the current tick, if exists.
	Next(tick time.Time) state.Replica
}

// ReplicaScanner simulates store scanner replica pacing, iterating over
// replicas at a rate sufficient to complete iteration in the specified scan
// loop interval.
type ReplicaScanner struct {
	// TOOD(kvoli): make this a function which returns the store's current
	// replicas in state.
	nextReplsFn        func() []state.Replica
	repls              []state.Replica
	start              time.Time
	lastLoop           time.Time
	targetLoopInterval time.Duration
	minIterInvterval   time.Duration
	maxIterInvterval   time.Duration
	iterInterval       time.Duration
	visited            int
	shuffler           func(n int, swap func(i, j int))
}

// NewScannerReplicaPacer returns a scanner replica pacer.
func NewScannerReplicaPacer(
	nextReplsFn func() []state.Replica,
	targetLoopInterval, minIterInterval, maxIterInterval time.Duration,
	seed int64,
) *ReplicaScanner {
	return &ReplicaScanner{
		nextReplsFn:        nextReplsFn,
		repls:              make([]state.Replica, 0, 1),
		targetLoopInterval: targetLoopInterval,
		minIterInvterval:   minIterInterval,
		maxIterInvterval:   maxIterInterval,
		shuffler:           state.NewShuffler(seed),
	}
}

// Len implements sort.Interface.
func (rp ReplicaScanner) Len() int { return len(rp.repls) }

// Less implements sort.Interface.
func (rp ReplicaScanner) Less(i, j int) bool {
	return rp.repls[i].Range() < rp.repls[j].Range()
}

// Swap implements sort.Interface.
func (rp ReplicaScanner) Swap(i, j int) {
	rp.repls[i], rp.repls[j] = rp.repls[j], rp.repls[i]
}

// resetPacerLoop collects the current replicas on the store and sets the
// pacing interval to complete iteration over all replicas in exactly target
// loop interval.
func (rp *ReplicaScanner) resetPacerLoop(tick time.Time) {
	rp.repls = rp.nextReplsFn()

	// Avoid the same replicas being processed in the same order in each
	// iteration.
	rp.shuffler(rp.Len(), rp.Swap)

	// Reset the counter and tracker vars.
	rp.visited = 0

	// If there are no replicas, we cannot determine the correct iter interval,
	// instead of waiting for the loop interval return early and try again on next
	// check.
	if len(rp.repls) == 0 {
		return
	}

	iterInterval := time.Duration(rp.targetLoopInterval.Nanoseconds() / int64(len(rp.repls)))

	// Adjust minimum and maximum times according to the min/max interval
	// allowed.
	if iterInterval < rp.minIterInvterval {
		iterInterval = rp.minIterInvterval
	}
	if iterInterval > rp.maxIterInvterval {
		iterInterval = rp.maxIterInvterval
	}

	// Set the start time on first loop, otherwise roll it over from the
	// previous loop.
	if rp.start == rp.lastLoop {
		rp.start = tick
	}

	rp.lastLoop = tick
	rp.iterInterval = iterInterval
}

// maybeResetPacerLoop checks whether we have completed iteration and resets
// the pacing loop if so.
func (rp *ReplicaScanner) maybeResetPacerLoop(tick time.Time) {
	if rp.visited >= len(rp.repls) {
		rp.resetPacerLoop(tick)
	}
}

// Next returns the next replica for the current tick, if exists.
func (rp *ReplicaScanner) Next(tick time.Time) state.Replica {
	rp.maybeResetPacerLoop(tick)

	elapsed := tick.Sub(rp.start)
	if elapsed >= rp.iterInterval && len(rp.repls) > 0 {
		repl := rp.repls[rp.visited]
		rp.visited++
		rp.start = rp.start.Add(rp.iterInterval)
		return repl
	}
	return nil
}

// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package asim

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/shuffle"
)

const (
	defaultLoopInterval     = 10 * time.Minute
	defaultMinInterInterval = 10 * time.Millisecond
	defaultMaxIterInterval  = 1 * time.Second
)

// ReplicaPacer controls the speed of considering a replica.
type ReplicaPacer interface {

	// Next returns the next replica for the current tick, if exists. It also
	// returns whether there are additional replicas for consideration on or
	// before this tick.
	Next(tick time.Time) (bool, *Replica)
}

// ScannerReplicaPacer simulates store scanner replica pacing, iterating over
// replicas at a rate sufficient to complete iteration in the specified scan
// loop interval.
type ScannerReplicaPacer struct {
	// TOOD(kvoli): make this a function which returns the store's current
	// replicas.
	nextReplsFn        func() []*Replica
	repls              []*Replica
	start              time.Time
	lastLoop           time.Time
	targetLoopInterval time.Duration
	minIterInvterval   time.Duration
	maxIterInvterval   time.Duration
	iterInterval       time.Duration
	visited            int
}

// NewScannerReplicaPacer returns a scanner replica pacer.
func NewScannerReplicaPacer(
	nextReplsFn func() []*Replica, targetLoopInterval, minIterInterval, maxIterInterval time.Duration,
) *ScannerReplicaPacer {
	return &ScannerReplicaPacer{
		nextReplsFn:        nextReplsFn,
		repls:              make([]*Replica, 0, 1),
		targetLoopInterval: targetLoopInterval,
		minIterInvterval:   minIterInterval,
		maxIterInvterval:   maxIterInterval,
	}
}

// Len implements sort.Interface.
func (rp ScannerReplicaPacer) Len() int { return len(rp.repls) }

// Less implements sort.Interface.
func (rp ScannerReplicaPacer) Less(i, j int) bool {
	return rp.repls[i].rangeDesc.RangeID < rp.repls[j].rangeDesc.RangeID
}

// Swap implements sort.Interface.
func (rp ScannerReplicaPacer) Swap(i, j int) {
	rp.repls[i], rp.repls[j] = rp.repls[j], rp.repls[i]
}

// resetPacerLoop collects the current replicas on the store and sets the
// pacing interval to complete iteration over all replicas in exactly target
// loop interval.
func (rp *ScannerReplicaPacer) resetPacerLoop(tick time.Time) {
	rp.repls = rp.nextReplsFn()

	// Avoid the same replicas being processed in the same order in each
	// iteration.
	shuffle.Shuffle(rp)

	// Reset the counter and tracker vars.
	rp.visited = 0

	// If there are no replicas, we cannot determine the correct loop interval,
	// instead of waiitng the loop interval return early and try again on next
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
func (rp *ScannerReplicaPacer) maybeResetPacerLoop(tick time.Time) {
	if rp.visited >= len(rp.repls) {
		rp.resetPacerLoop(tick)
	}
}

// Next returns the next replica for the current tick, if exists. It also
// returns whether there are additional replicas for consideration on or
// before this tick.
func (rp *ScannerReplicaPacer) Next(tick time.Time) (bool, *Replica) {
	rp.maybeResetPacerLoop(tick)

	elapsed := tick.Sub(rp.start)
	if elapsed >= rp.iterInterval && len(rp.repls) > 0 {
		repl := rp.repls[rp.visited]
		rp.visited++
		rp.start = rp.start.Add(rp.iterInterval)

		// Return done if updated elapsed duration is less than the iter
		// interval.
		return tick.Sub(rp.start) < rp.iterInterval, repl
	}
	return true, nil
}

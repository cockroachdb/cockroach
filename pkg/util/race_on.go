// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build race

package util

import "runtime"

// RaceEnabled is true if CockroachDB was built with the race build tag.
const RaceEnabled = true

// racePreemptionPoints is set in EnableRacePreemptionPoints.
var racePreemptionPoints = false

// EnableRacePreemptionPoints enables goroutine preemption points declared with
// RacePreempt for builds using the race build tag.
func EnableRacePreemptionPoints() func() {
	racePreemptionPoints = true
	return func() {
		racePreemptionPoints = false
	}
}

// RacePreempt adds a goroutine preemption point if CockroachDB was built with
// the race build tag and preemption points have been enabled. The function is a
// no-op (and should be optimized out through dead code elimination) if the race
// build tag was not used.
func RacePreempt() {
	if racePreemptionPoints {
		runtime.Gosched()
	}
}

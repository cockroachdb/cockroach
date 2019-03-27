// Copyright 2017 The Cockroach Authors.
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

// +build race

package util

import (
	"fmt"
	"runtime"
	"sync/atomic"
)

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

// NoParallelUse is a struct that can be embedded in other structs. It provides
// BeginExclusive and EndExclusive functions which trigger panics in race builds
// if a goroutine calls BeginExclusive before another goroutine calls
// EndExclusive.
type NoParallelUse struct {
	useCount int32
}

// Silence unused warnings.
var _ = NoParallelUse{}

// BeginExclusive marks the beginning of a section where this goroutine is
// assumed to have exclusive ownership of the object.
func (n *NoParallelUse) BeginExclusive() {
	val := atomic.AddInt32(&n.useCount, 1)
	if val != 1 {
		panic(fmt.Sprintf("already in use (val=%d)", val))
	}
}

// EndExclusive marks the end of a section where this goroutine is assumed to
// have exclusive ownership of the object.
func (n *NoParallelUse) EndExclusive() {
	val := atomic.AddInt32(&n.useCount, -1)
	if val != 0 {
		panic(fmt.Sprintf("unexpected value %d after End", val))
	}
}

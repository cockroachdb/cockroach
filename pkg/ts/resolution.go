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

package ts

import (
	"fmt"
	"time"
)

// Resolution is used to enumerate the different resolution values supported by
// Cockroach.
type Resolution int64

func (r Resolution) String() string {
	switch r {
	case Resolution10s:
		return "10s"
	case Resolution30mRUP:
		return "30mRUP"
	case resolution1ns:
		return "1ns"
	case resolution50nsRUP:
		return "50nsRUP"
	case resolutionInvalid:
		return "BAD"
	}
	return fmt.Sprintf("%d", r)
}

// Resolution enumeration values are directly serialized and persisted into
// system keys; these values must never be altered or reordered.
const (
	// Resolution10s stores data with a sample resolution of 10 seconds.
	Resolution10s Resolution = 1
	// Resolution30mRUP stores roll-up data from a higher resolution at a sample
	// resolution of 30 minutes.
	Resolution30mRUP Resolution = 2
	// resolution50nsRUP stores roll-up data from the 1ns resolution at a sample
	// resolution of 50 nanoseconds. Used for testing.
	resolution50nsRUP Resolution = 998
	// resolution1ns stores data with a sample resolution of 1 nanosecond. Used
	// only for testing.
	resolution1ns Resolution = 999
	// resolutionInvalid is an invalid resolution used only for testing. It causes
	// an error to be thrown in certain methods. It is invalid because its sample
	// period is not a divisor of its slab period.
	resolutionInvalid Resolution = 1000
)

// sampleDurationByResolution is a map used to retrieve the sample duration
// corresponding to a Resolution value. Sample durations are expressed in
// nanoseconds.
var sampleDurationByResolution = map[Resolution]int64{
	Resolution10s:     int64(time.Second * 10),
	Resolution30mRUP:  int64(time.Minute * 30),
	resolution1ns:     1,  // 1ns resolution only for tests.
	resolution50nsRUP: 50, // 50ns rollup only for tests.
	resolutionInvalid: 10, // Invalid resolution.
}

// slabDurationByResolution is a map used to retrieve the slab duration
// corresponding to a Resolution value; the slab duration determines how many
// samples are stored at a single Cockroach key/value. Slab durations are
// expressed in nanoseconds.
var slabDurationByResolution = map[Resolution]int64{
	Resolution10s:     int64(time.Hour),
	Resolution30mRUP:  int64(time.Hour * 24),
	resolution1ns:     10,   // 1ns resolution only for tests.
	resolution50nsRUP: 1000, // 50ns rollup only for tests.
	resolutionInvalid: 11,
}

// SampleDuration returns the sample duration corresponding to this resolution
// value, expressed in nanoseconds.
func (r Resolution) SampleDuration() int64 {
	duration, ok := sampleDurationByResolution[r]
	if !ok {
		panic(fmt.Sprintf("no sample duration found for resolution value %v", r))
	}
	return duration
}

// SlabDuration returns the slab duration corresponding to this resolution
// value, expressed in nanoseconds. The slab duration determines how many
// consecutive samples are stored in a single Cockroach key/value.
func (r Resolution) SlabDuration() int64 {
	duration, ok := slabDurationByResolution[r]
	if !ok {
		panic(fmt.Sprintf("no slab duration found for resolution value %v", r))
	}
	return duration
}

// IsRollup returns true if this resolution contains rollup data: statistical
// values about a large number of samples taken over a long period, such as
// the min, max and sum.
func (r Resolution) IsRollup() bool {
	return r == Resolution30mRUP || r == resolution50nsRUP
}

func normalizeToPeriod(timestampNanos int64, period int64) int64 {
	return timestampNanos - timestampNanos%period
}

func (r Resolution) normalizeToSlab(timestampNanos int64) int64 {
	return normalizeToPeriod(timestampNanos, r.SlabDuration())
}

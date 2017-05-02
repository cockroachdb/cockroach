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
// Author: Matt Tracy (matt.r.tracy@gmail.com)

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
	case resolution1ns:
		return "1ns"
	}
	return fmt.Sprintf("%d", r)
}

// Resolution enumeration values are directly serialized and persisted into
// system keys; these values must never be altered or reordered.
const (
	// Resolution10s stores data with a sample resolution of 10 seconds.
	Resolution10s Resolution = 1
	// resolution1ns stores data with a sample resolution of 1 nanosecond. Used
	// only for testing.
	resolution1ns Resolution = 999
)

// sampleDurationByResolution is a map used to retrieve the sample duration
// corresponding to a Resolution value. Sample durations are expressed in
// nanoseconds.
var sampleDurationByResolution = map[Resolution]int64{
	Resolution10s: int64(time.Second * 10),
	resolution1ns: 1, // 1ns resolution only for tests.
}

// slabDurationByResolution is a map used to retrieve the slab duration
// corresponding to a Resolution value; the slab duration determines how many
// samples are stored at a single Cockroach key/value. Slab durations are
// expressed in nanoseconds.
var slabDurationByResolution = map[Resolution]int64{
	Resolution10s: int64(time.Hour),
	resolution1ns: 10, // 1ns resolution only for tests.
}

// pruneAgeByResolution maintains a suggested maximum age per resolution; data
// which is older than the given threshold for a resolution is considered
// eligible for deletion. Thresholds are specified in nanoseconds.
var pruneThresholdByResolution = map[Resolution]int64{
	Resolution10s: (30 * 24 * time.Hour).Nanoseconds(),
	resolution1ns: time.Second.Nanoseconds(),
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

// PruneThreshold returns the pruning threshold duration for this resolution,
// expressed in nanoseconds. This duration determines how old time series data
// must be before it is eligible for pruning.
func (r Resolution) PruneThreshold() int64 {
	threshold, ok := pruneThresholdByResolution[r]
	if !ok {
		panic(fmt.Sprintf("no prune threshold found for resolution value %v", r))
	}
	return threshold
}

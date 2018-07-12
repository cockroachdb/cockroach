// Copyright 2018 The Cockroach Authors.
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

package telemetry

import (
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// Count retrieves and increments the usage counter for the passed feature.
// High-volume callers may want to instead use `GetCounter` and hold on to the
// returned Counter between calls to Inc, to avoid contention in the registry.
func Count(feature string) {
	Inc(GetCounter(feature))
}

// Counter represents the usage counter for a given 'feature'.
type Counter *int32

// Inc increments the counter.
func Inc(c Counter) {
	atomic.AddInt32(c, 1)
}

// GetCounter returns a counter from the global registry.
func GetCounter(feature string) Counter {
	counters.RLock()
	i, ok := counters.m[feature]
	counters.RUnlock()

	if !ok {
		counters.Lock()
		var n int32
		counters.m[feature] = &n
		i = &n
		counters.Unlock()
	}
	return i
}

func init() {
	counters.m = make(map[string]Counter, approxFeatureCount)
}

var approxFeatureCount = 100

// counters stores the registry of feature-usage counts.
// TODO(dt): consider a lock-free map.
var counters struct {
	syncutil.RWMutex
	m map[string]Counter
}

// GetAndResetFeatureCounts returns the current feature usage counts and resets
// the counts for all features back to 0.
func GetAndResetFeatureCounts() map[string]int32 {
	counters.RLock()
	m := make(map[string]int32, approxFeatureCount)
	for k := range counters.m {
		m[k] = atomic.SwapInt32(counters.m[k], 0)
	}
	counters.RUnlock()
	return m
}

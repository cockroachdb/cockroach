// Copyright 2016 The Cockroach Authors.
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
// Author: Tamir Duberstein (tamird@gmail.com)

package timeutil

import (
	"os"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/util/log"
)

const offsetEnvKey = "COCKROACH_SIMULATED_OFFSET"
const monotonicCheckEnableEnvKey = "COCKROACH_ENABLE_CHECK_MONOTONIC_TIME"

var (
	nowFunc = time.Now

	offset time.Duration

	monotonicityCheckEnabled bool
	mu                       struct {
		sync.Mutex
		lastTime                time.Time
		monotonicityErrorsCount int32
	}
)

func initMonotonicityCheck() {
	// TODO(knz) perhaps we should change the default to disabled.
	// However for Beta we always enable.
	monotonicityCheckEnabled = os.Getenv(monotonicCheckEnableEnvKey) != "0"
}

func initFakeTime() {
	if offsetStr := os.Getenv(offsetEnvKey); offsetStr != "" {
		var err error
		if offset, err = time.ParseDuration(offsetStr); err != nil {
			panic(err)
		}
	}
	if offset == 0 {
		nowFunc = time.Now
	} else {
		nowFunc = func() time.Time {
			return time.Now().Add(offset)
		}
	}
}

// Now returns the current local time with an optional offset specified by the
// environment. The offset functionality is guarded by the  "clockoffset" build
// tag - if built with that tag, the clock offset is parsed from the
// "COCKROACH_SIMULATED_OFFSET" environment variable using time.ParseDuration,
// which supports quasi-human values like "1h" or "1m".
// Additionally, the time is checked to be monotonic (no backward
// jumps in time) unless COCKROACH_ENABLE_CHECK_MONOTONIC_TIME is set
// to 0.
func Now() time.Time {
	if monotonicityCheckEnabled {
		mu.Lock()
		lastTime := mu.lastTime
		newTime := nowFunc()

		if !lastTime.IsZero() && newTime.Before(lastTime) {
			mu.monotonicityErrorsCount++
			defer log.Warningf("backward time jump detected: previously %s, now %s (offset %s)", lastTime, newTime, newTime.Sub(lastTime))
		}

		mu.lastTime = newTime
		mu.Unlock()
		return newTime
	}
	return nowFunc()
}

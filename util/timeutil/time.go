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
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/util/log"
)

const offsetEnvKey = "COCKROACH_SIMULATED_OFFSET"
const checkMonotonicEnvKey = "COCKROACH_CHECK_MONOTONIC_TIME"

var (
	nowFunc = time.Now

	offset time.Duration

	tmu      sync.Mutex
	lastTime time.Time
)

func initFakeTime() {
	if offsetStr := os.Getenv(offsetEnvKey); offsetStr != "" {
		var err error
		if offset, err = time.ParseDuration(offsetStr); err != nil {
			panic(err)
		}
	}
	if offset != 0 {
		prevFunc := nowFunc
		nowFunc = func() time.Time {
			return prevFunc().Add(offset)
		}
	}
}

func initMonotonicTimeCheck() {
	if checkStr := os.Getenv(checkMonotonicEnvKey); checkStr != "" {
		doCheck := false
		var err error
		if doCheck, err = strconv.ParseBool(checkStr); err != nil {
			panic(err)
		}
		if doCheck {
			lastTime = nowFunc()
			prevFunc := nowFunc
			nowFunc = func() time.Time {
				tmu.Lock()
				defer tmu.Unlock()
				newTime := prevFunc()
				if newTime.Before(lastTime) {
					log.Warningf("backward time jump detected: previously %s, now %s (offset %s)", lastTime, newTime, newTime.Sub(lastTime))
				}
				lastTime = newTime
				return newTime
			}
		}
	}
}

// Now returns the current local time, with optional tweaks specified by the environment:
// - clock offsets. This is guarded by the  "clockoffset" build
//   tag - if built with that tag, the clock offset is parsed from the
//   "COCKROACH_SIMULATED_OFFSET" environment variable using time.ParseDuration,
//   which supports quasi-human values like "1h" or "1m".
// - time backwards jumps. This is guarded by the "clockmonotonic" build
//   tag - if built with that tag, and the "COCKROACH_CHECK_MONOTONIC_TIME" environment
//   variable is set, the clock is checked for monotonicity.
func Now() time.Time {
	return nowFunc()
}

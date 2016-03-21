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
	"time"

	"github.com/cockroachdb/cockroach/util/envutil"
)

var (
	nowFunc = time.Now
)

// SetTimeOffset configures a fixed offset to reported time samples.
func SetTimeOffset(offset time.Duration) {
	if offset == 0 {
		nowFunc = time.Now
	} else {
		nowFunc = func() time.Time {
			return time.Now().Add(offset)
		}
	}
}

func initFakeTime() {
	offset := envutil.EnvOrDefaultDuration("simulated_offset", 0)
	SetTimeOffset(offset)
}

// Now returns the current local time with an optional offset specified by the
// environment. The offset functionality is guarded by the  "clockoffset" build
// tag - if built with that tag, the clock offset is parsed from the
// "COCKROACH_SIMULATED_OFFSET" environment variable using time.ParseDuration,
// which supports quasi-human values like "1h" or "1m".
func Now() time.Time {
	return nowFunc()
}

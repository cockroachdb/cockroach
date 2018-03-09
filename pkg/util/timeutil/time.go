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

package timeutil

import (
	"math"
	"time"
)

// ClocklessMaxOffset is a special-cased value that is used when the cluster
// runs in "clockless" mode. In that (experimental) mode, we operate without
// assuming any bound on the clock drift.
const ClocklessMaxOffset = math.MaxInt64

// Since returns the time elapsed since t.
// It is shorthand for Now().Sub(t).
func Since(t time.Time) time.Duration {
	return Now().Sub(t)
}

// UnixEpoch represents the Unix epoch, January 1, 1970 UTC.
var UnixEpoch = time.Unix(0, 0).UTC()

// FromUnixMicros returns the UTC time.Time corresponding to the given Unix
// time, usec microseconds since UnixEpoch. In Go's current time.Time
// implementation, all possible values for us can be represented as a time.Time.
func FromUnixMicros(us int64) time.Time {
	return time.Unix(us/1e6, (us%1e6)*1e3).UTC()
}

// ToUnixMicros returns t as the number of microseconds elapsed since UnixEpoch.
// Fractional microseconds are rounded, half up, using time.Round. Similar to
// time.Time.UnixNano, the result is undefined if the Unix time in microseconds
// cannot be represented by an int64.
func ToUnixMicros(t time.Time) int64 {
	return t.Unix()*1e6 + int64(t.Round(time.Microsecond).Nanosecond())/1e3
}

// Unix wraps time.Unix ensuring that the result is in UTC instead of Local.
func Unix(sec, nsec int64) time.Time {
	return time.Unix(sec, nsec).UTC()
}

// SleepUntil sleeps until the given time. The current time is
// refreshed every second in case there was a clock jump
//
// untilNanos is the target time to sleep till in epoch nanoseconds
// currentTimeNanos is a function returning current time in epoch nanoseconds
func SleepUntil(untilNanos int64, currentTimeNanos func() int64) {
	for {
		d := time.Duration(untilNanos - currentTimeNanos())
		if d <= 0 {
			break
		}
		if d > time.Second {
			d = time.Second
		}
		time.Sleep(d)
	}
}

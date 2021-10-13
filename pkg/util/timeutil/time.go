// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package timeutil

import (
	"strings"
	"time"
)

// LibPQTimePrefix is the prefix lib/pq prints time-type datatypes with.
const LibPQTimePrefix = "0000-01-01"

// Now returns the current time with the UTC location.
//
// Note: The UTC location is applied because we're afraid of timestamps coming
// from now leading to Datums, and we want our Datums UTC. We also want
// timestamps to be UTC when we print them. The UTC conversion, though, clears
// the monontonic part of the timestamp, which makes Since(t) calls more
// expensive. If all you want to do with this timestamp is pass it into Since(t)
// later, then consider using NowMonotonic()/SinceMonotonic().
func Now() time.Time {
	return time.Now().UTC()
}

// MonotonicTime represents a time instant t that's only to be used for
// timeutil.Since(t) and t1.Sub(t2) - i.e. only for measuring time durations -
// and not for printing (since it hasn't been converted to UTC and we like our
// timestamps printed as UTC).
type MonotonicTime time.Time

// ToTime converts a MonotonicTime to a time.Time by converting it to UTC.
func (t MonotonicTime) ToTime() time.Time {
	return time.Time(t).UTC()
}

// Sub is like time.Sub - returns the delta (t - other).
func (t MonotonicTime) Sub(other MonotonicTime) time.Duration {
	return time.Time(t).Sub(time.Time(other))
}

// NowMonotonic returns a timestamp `t` suitable for using with
// SinceMonotonic(t) later. SinceMonotonic is more efficient than Since, but in
// order to do anything else with `t` you have to call t.ToTime() to turn it
// into a (UTC) timestamp.
func NowMonotonic() MonotonicTime {
	return MonotonicTime(time.Now())
}

// SinceMonotonic is like Since, but operates on Monotonic time and is more
// efficient.
func SinceMonotonic(t MonotonicTime) time.Duration {
	return time.Since(time.Time(t))
}

// Since returns the time elapsed since t.
// It is shorthand for Now().Sub(t), but more efficient.
func Since(t time.Time) time.Duration {
	return time.Since(t)
}

// Until returns the duration until t.
// It is shorthand for t.Sub(Now()), but more efficient.
func Until(t time.Time) time.Duration {
	return time.Until(t)
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

// ReplaceLibPQTimePrefix replaces unparsable lib/pq dates used for timestamps
// (0000-01-01) with timestamps that can be parsed by date libraries.
func ReplaceLibPQTimePrefix(s string) string {
	if strings.HasPrefix(s, LibPQTimePrefix) {
		return "1970-01-01" + s[len(LibPQTimePrefix):]
	}
	return s
}

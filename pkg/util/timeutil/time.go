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

func init() {
	// Set time.Local so that time.Now() returns timestamps localized as UTC.
	// We like our timestamps UTC because:
	// 1) Timestamps might leak into Datums, and we don't want different nodes
	// using different timestamps.
	// 2) We want timestamps to print in the same timezone across the cluster.
	//
	// Setting the time.Local global is a fairly brutal thing to do. Before this,
	// we had timeutil.Now() return time.Now().UTC(). That UTC() call had the
	// unfortunate side effect of stripping the monotonic part of the clock
	// reading, which makes future time.Since(t) calls more expensive.
	//
	// Setting time.Local to nil makes it UTC. We don't set time.Local = time.UTC
	// because that breaks the time library's documented expectation that, in a
	// time.Time, a nil location is only ever represented as nil. I don't think
	// violating that invariant actually breaks anything inside the standard
	// library, but it is a pain for some of our tests that marshall/unmarshall
	// time.Time instances in various ways (yaml, json) and expect them to
	// round-trip. Having two representations for a UTC roundtrip (one with the
	// location set to time.UTC, one with a nil location) confuses these tests.
	time.Local = nil
}

// LibPQTimePrefix is the prefix lib/pq prints time-type datatypes with.
const LibPQTimePrefix = "0000-01-01"

// Now returns the current time.
//
// The timestamp returned will have the locality set to UTC, courtesy of the
// init() above.
func Now() time.Time {
	return time.Now()
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

// StripMonotonic strips the monotonic part(*) of the timestamp. This is useful
// when we want a time.Time that round-trips through marshalling/unmarshalling
// that loses the monotonic part (e.g. json, yaml). In particular, various tests
// check that larger structs containing time.Time round-trip.
//
// (*) Go time.Time can have two clock readings in them: a wall-clock and a
// monotonic clock.
func StripMonotonic(t time.Time) time.Time {
	return t.Round(0)
}

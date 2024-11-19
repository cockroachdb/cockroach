// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package timeutil

import (
	"strings"
	"time"
	"unsafe"
)

// LibPQTimePrefix is the prefix lib/pq prints time-type datatypes with.
const LibPQTimePrefix = "0000-01-01"

// Now returns the current UTC time.
//
// We've decided in times immemorial that always returning UTC is a good policy
// across the cluster so that all the timestamps print uniformly across
// different nodes, and also because we were afraid that timestamps leak into
// SQL Datums, and there the timestamp matters. Years later, it's not clear
// whether this was a good decision since it's forcing the nasty implementation
// below.
func Now() time.Time {
	t := time.Now()
	// HACK: instead of doing t = t.UTC(), we reach inside the
	// struct and set the location manually. UTC() strips the monotonic clock reading
	// from t, for no good reason: https://groups.google.com/g/golang-nuts/c/dyPTdi6oem8
	// Stripping the monotonic part has bad consequences:
	// 1. We lose the benefits of the monotonic clock reading.
	// 2. On OSX, only the monotonic clock seems to have nanosecond resolution. If
	// we strip it, we only get microsecond resolution. Besides generally sucking,
	// microsecond resolution is not enough to guarantee that consecutive
	// timeutil.Now() calls don't return the same instant. This trips up some of
	// our tests, which assume that they can measure any duration of time.
	// 3. time.Since(t) does one less system calls when t has a monotonic reading,
	// making it twice as fast as otherwise:
	// https://cs.opensource.google/go/go/+/refs/tags/go1.17.2:src/time/time.go;l=878;drc=refs%2Ftags%2Fgo1.17.2
	x := (*timeLayout)(unsafe.Pointer(&t))
	x.loc = nil // nil means UTC
	return t
}

// NowNoMono is like Now(), but it strips down the monotonic part of the
// timestamp. This is useful for getting timestamps that rounds-trip through
// various channels that strip out the monotonic part - for example yaml
// marshaling.
func NowNoMono() time.Time {
	// UTC has the side-effect of stripping the nanos.
	return time.Now().UTC()
}

// StripMono returns a copy of t with its monotonic clock reading stripped. This
// is useful for getting a time.Time that compares == with another one that
// might not have the mono part. time.Time is meant to be compared with
// Time.Equal() (which ignores the mono), not with ==, but sometimes we have a
// time.Time in a bigger struct and we want to use require.Equal() or such.
func StripMono(t time.Time) time.Time {
	// UTC() has the side-effect of stripping the mono part.
	return t.UTC()
}

// timeLayout mimics time.Time, exposing all the fields. We do an unsafe cast of
// a time.Time to this in order to set the location.
type timeLayout struct {
	wall uint64
	ext  int64
	loc  *time.Location
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

// FromUnixNanos returns the UTC time.Time corresponding to the given Unix
// time, ns nanoseconds since UnixEpoch. In Go's current time.Time
// implementation, all possible values for ns can be represented as a time.Time.
func FromUnixNanos(ns int64) time.Time {
	return time.Unix(ns/1e9, ns%1e9).UTC()
}

// ToUnixMicros returns t as the number of microseconds elapsed since UnixEpoch.
// Fractional microseconds are rounded, half up, using time.Round. Similar to
// time.Time.UnixNano, the result is undefined if the Unix time in microseconds
// cannot be represented by an int64.
func ToUnixMicros(t time.Time) int64 {
	return t.Unix()*1e6 + int64(t.Round(time.Microsecond).Nanosecond())/1e3
}

// Unix wraps time.Unix ensuring that the result is in UTC instead of Local.
//
// The process of deriving the args to construct a specific time.Time:
//
//	// say we want to construct timestamp "294277-01-01 23:59:59.999999 +0000 UTC"
//	tm := time.Date(294277, 1, 1, 23, 59, 59, 999999000, time.UTC)
//	// get the args of "timeutil.Unix"
//	sec := tm.Unix()
//	nsec := int64(tm.Nanosecond())
//	// verify
//	fmt.Println(tm == time.Unix(sec, nsec).UTC())
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

// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package hlc

import (
	"fmt"
	"math"
	"strconv"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Timestamp constant values.
var (
	// MaxTimestamp is the max value allowed for Timestamp.
	MaxTimestamp = Timestamp{WallTime: math.MaxInt64, Logical: math.MaxInt32}
	// MinTimestamp is the min value allowed for Timestamp.
	MinTimestamp = Timestamp{WallTime: 0, Logical: 1}
)

// Less compares two timestamps.
func (t Timestamp) Less(s Timestamp) bool {
	return t.WallTime < s.WallTime || (t.WallTime == s.WallTime && t.Logical < s.Logical)
}

// String implements the fmt.Formatter interface.
func (t Timestamp) String() string {
	// The following code was originally written as
	//   fmt.Sprintf("%d.%09d,%d", t.WallTime/1e9, t.WallTime%1e9, t.Logical).
	// The main problem with the original code was that it would put
	// a negative sign in the middle (after the decimal point) if
	// the value happened to be negative.
	buf := make([]byte, 0, 12)
	if t.WallTime == 0 {
		// We simplify "0.000000000" to just "0" as this is a common case.
		buf = append(buf, '0')
	} else {
		u := uint64(t.WallTime)
		if t.WallTime < 0 {
			u = -u
			buf = append(buf, '-')
		}

		// Generate the decimal representation of the wall time.
		// expressed in nanoseconds
		timeS := strconv.FormatUint(u, 10)
		if len(timeS) < 10 {
			// If there's less than 10 digits in the representation,
			// that means there's less than one second of time.
			// In that case, print a leading zero (number of seconds).
			buf = append(buf, '0')
		} else {
			// Output all the digits except the last 9.
			buf = append(buf, []byte(timeS[:len(timeS)-9])...)
		}
		// Second-nanosecond separator.
		buf = append(buf, '.')
		// Now on to print the nanoseconds.
		// We'll ignore all the digits except the last 9, since these
		// were emitted above already.
		sz := 9
		if len(timeS) < sz {
			// If there's less than 9 digits worth of nanoseconds, we'll use
			// that.
			sz = len(timeS)
		}
		// Pad the nanosecond part with zeroes on the left. This reduces
		// to no zeroes at all if there were exactly 9 digits already.
		const zeroes = "000000000"
		buf = append(buf, zeroes[:9-sz]...)
		// Output the nanoseconds.
		buf = append(buf, timeS[len(timeS)-sz:]...)
	}
	// Finally, output the logical part.
	buf = append(buf, ',')
	buf = strconv.AppendInt(buf, int64(t.Logical), 10)
	// Convert the byte array to a string without a copy. We follow the
	// examples of strings.Builder here.
	return *(*string)(unsafe.Pointer(&buf))
}

// AsOfSystemTime returns a string to be used in an AS OF SYSTEM TIME query.
func (t Timestamp) AsOfSystemTime() string {
	return fmt.Sprintf("%d.%010d", t.WallTime, t.Logical)
}

// Less compares two timestamps.
func (t LegacyTimestamp) Less(s LegacyTimestamp) bool {
	return Timestamp(t).Less(Timestamp(s))
}

func (t LegacyTimestamp) String() string {
	return Timestamp(t).String()
}

// IsEmpty retruns true if t is an empty Timestamp.
func (t Timestamp) IsEmpty() bool {
	return t == Timestamp{}
}

// Add returns a timestamp with the WallTime and Logical components increased.
// wallTime is expressed in nanos.
func (t Timestamp) Add(wallTime int64, logical int32) Timestamp {
	return Timestamp{
		WallTime: t.WallTime + wallTime,
		Logical:  t.Logical + logical,
	}
}

// Clone return a new timestamp that has the same contents as the receiver.
func (t Timestamp) Clone() *Timestamp {
	return &t
}

// Next returns the timestamp with the next later timestamp.
func (t Timestamp) Next() Timestamp {
	if t.Logical == math.MaxInt32 {
		if t.WallTime == math.MaxInt64 {
			panic("cannot take the next value to a max timestamp")
		}
		return Timestamp{
			WallTime: t.WallTime + 1,
		}
	}
	return Timestamp{
		WallTime: t.WallTime,
		Logical:  t.Logical + 1,
	}
}

// Prev returns the next earliest timestamp.
func (t Timestamp) Prev() Timestamp {
	if t.Logical > 0 {
		return Timestamp{
			WallTime: t.WallTime,
			Logical:  t.Logical - 1,
		}
	} else if t.WallTime > 0 {
		return Timestamp{
			WallTime: t.WallTime - 1,
			Logical:  math.MaxInt32,
		}
	}
	panic("cannot take the previous value to a zero timestamp")
}

// FloorPrev returns a timestamp earlier than the current timestamp. If it
// can subtract a logical tick without wrapping around, it does so. Otherwise
// it subtracts a nanosecond from the walltime.
func (t Timestamp) FloorPrev() Timestamp {
	if t.Logical > 0 {
		return Timestamp{
			WallTime: t.WallTime,
			Logical:  t.Logical - 1,
		}
	} else if t.WallTime > 0 {
		return Timestamp{
			WallTime: t.WallTime - 1,
			Logical:  0,
		}
	}
	panic("cannot take the previous value to a zero timestamp")
}

// Forward updates the timestamp from the one given, if that moves it forwards
// in time. Returns true if the timestamp was adjusted and false otherwise.
func (t *Timestamp) Forward(s Timestamp) bool {
	if t.Less(s) {
		*t = s
		return true
	}
	return false
}

// Backward updates the timestamp from the one given, if that moves it
// backwards in time.
func (t *Timestamp) Backward(s Timestamp) {
	if s.Less(*t) {
		*t = s
	}
}

// GoTime converts the timestamp to a time.Time.
func (t Timestamp) GoTime() time.Time {
	return timeutil.Unix(0, t.WallTime)
}

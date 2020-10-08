// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package timeofday

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// TimeOfDay represents a time of day (no date), stored as microseconds since
// midnight.
type TimeOfDay int64

const (
	// Min is the minimum TimeOfDay value (midnight).
	Min = TimeOfDay(0)

	// Time2400 is a special value to represent the 24:00 input time
	Time2400 = TimeOfDay(microsecondsPerDay)

	// Max is the maximum TimeOfDay value (1 second before midnight)
	Max = Time2400

	microsecondsPerSecond = 1e6
	microsecondsPerMinute = 60 * microsecondsPerSecond
	microsecondsPerHour   = 60 * microsecondsPerMinute
	microsecondsPerDay    = 24 * microsecondsPerHour
	nanosPerMicro         = 1000
	secondsPerDay         = 24 * 60 * 60
)

// New creates a TimeOfDay representing the specified time.
func New(hour, min, sec, micro int) TimeOfDay {
	hours := time.Duration(hour) * time.Hour
	minutes := time.Duration(min) * time.Minute
	seconds := time.Duration(sec) * time.Second
	micros := time.Duration(micro) * time.Microsecond
	return FromInt(int64((hours + minutes + seconds + micros) / time.Microsecond))
}

func (t TimeOfDay) String() string {
	micros := t.Microsecond()
	if micros > 0 {
		s := fmt.Sprintf("%02d:%02d:%02d.%06d", t.Hour(), t.Minute(), t.Second(), micros)
		return strings.TrimRight(s, "0")
	}
	return fmt.Sprintf("%02d:%02d:%02d", t.Hour(), t.Minute(), t.Second())
}

// FromInt constructs a TimeOfDay from an int64, representing microseconds since
// midnight. Inputs outside the range [0, microsecondsPerDay) are modded as
// appropriate.
func FromInt(i int64) TimeOfDay {
	return TimeOfDay(positiveMod(i, microsecondsPerDay))
}

// positive_mod returns x mod y in the range [0, y). (Go's modulo operator
// preserves sign.)
func positiveMod(x, y int64) int64 {
	if x < 0 {
		return x%y + y
	}
	return x % y
}

// FromTime constructs a TimeOfDay from a time.Time, ignoring the date and time zone.
func FromTime(t time.Time) TimeOfDay {
	// Adjust for timezone offset so it won't affect the time. This is necessary
	// at times, like when casting from a TIMESTAMPTZ.
	_, offset := t.Zone()
	unixSeconds := t.Unix() + int64(offset)

	nanos := (unixSeconds%secondsPerDay)*int64(time.Second) + int64(t.Nanosecond())
	return FromInt(nanos / nanosPerMicro)
}

// FromTimeAllow2400 assumes 24:00 time is possible from the given input,
// otherwise falling back to FromTime.
// It assumes time.Time is represented as lib/pq or as unix time.
func FromTimeAllow2400(t time.Time) TimeOfDay {
	if t.Day() != 1 {
		return Time2400
	}
	return FromTime(t)
}

// ToTime converts a TimeOfDay to a time.Time, using the Unix epoch as the date.
func (t TimeOfDay) ToTime() time.Time {
	return timeutil.Unix(0, int64(t)*nanosPerMicro)
}

// Random generates a random TimeOfDay.
func Random(rng *rand.Rand) TimeOfDay {
	return TimeOfDay(rng.Int63n(microsecondsPerDay))
}

// Round takes a TimeOfDay, and rounds it to the given precision.
func (t TimeOfDay) Round(precision time.Duration) TimeOfDay {
	if t == Time2400 {
		return t
	}
	ret := t.ToTime().Round(precision)
	// Rounding Max should give Time2400, not 00:00.
	// To catch this, see if we are comparing against the same day.
	if ret.Day() != t.ToTime().Day() {
		return Time2400
	}
	return FromTime(ret)
}

// Add adds a Duration to a TimeOfDay, wrapping into the next day if necessary.
func (t TimeOfDay) Add(d duration.Duration) TimeOfDay {
	return FromInt(int64(t) + d.Nanos()/nanosPerMicro)
}

// Difference returns the interval between t1 and t2, which may be negative.
func Difference(t1 TimeOfDay, t2 TimeOfDay) duration.Duration {
	return duration.MakeDuration(int64(t1-t2)*nanosPerMicro, 0, 0)
}

// Hour returns the hour specified by t, in the range [0, 24].
func (t TimeOfDay) Hour() int {
	if t == Time2400 {
		return 24
	}
	return int(int64(t)%microsecondsPerDay) / microsecondsPerHour
}

// Minute returns the minute offset within the hour specified by t, in the
// range [0, 59].
func (t TimeOfDay) Minute() int {
	return int(int64(t)%microsecondsPerHour) / microsecondsPerMinute
}

// Second returns the second offset within the minute specified by t, in the
// range [0, 59].
func (t TimeOfDay) Second() int {
	return int(int64(t)%microsecondsPerMinute) / microsecondsPerSecond
}

// Microsecond returns the microsecond offset within the second specified by t,
// in the range [0, 999999].
func (t TimeOfDay) Microsecond() int {
	return int(int64(t) % microsecondsPerSecond)
}

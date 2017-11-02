// Copyright 2017 The Cockroach Authors.
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

package timeofday

import (
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// TimeOfDay represents a time of day (no date), stored as microseconds since
// midnight UTC.
type TimeOfDay int64

const (
	// Min is the minimum TimeOfDay value (midnight).
	Min = TimeOfDay(0)
	// Max is the maximum TimeOfDay value (1 microsecond before midnight).
	Max = TimeOfDay(microsecondsPerDay - 1)

	format             = "15:04:05.999999"
	microsecondsPerDay = int64(24 * time.Hour / time.Microsecond)
	nanosPerMicro      = 1000
)

// NewTimeOfDay creates a TimeOfDay representing the specified time.
func NewTimeOfDay(hour int, min int, sec int, micro int) TimeOfDay {
	hours := time.Duration(hour) * time.Hour
	minutes := time.Duration(min) * time.Minute
	seconds := time.Duration(sec) * time.Second
	micros := time.Duration(micro) * time.Microsecond
	return TimeOfDay(int64((hours + minutes + seconds + micros) / time.Microsecond))
}

func (t TimeOfDay) String() string {
	return ToTime(t).Format(format)
}

// FromTime constructs a TimeOfDay from a time.Time, ignoring the date portion.
func FromTime(t time.Time) TimeOfDay {
	return NewTimeOfDay(t.Hour(), t.Minute(), t.Second(), t.Nanosecond()/nanosPerMicro)
}

// ToTime converts a TimeOfDay to a time.Time, using the Unix epoch as the date.
func ToTime(t TimeOfDay) time.Time {
	return timeutil.Unix(0, int64(t)*nanosPerMicro)
}

// Random generates a random TimeOfDay.
func Random(rng *rand.Rand) TimeOfDay {
	return TimeOfDay(rng.Int63n(microsecondsPerDay))
}

// Add adds a Duration to a TimeOfDay, wrapping into the next day if necessary.
func Add(t TimeOfDay, d duration.Duration) TimeOfDay {
	return FromTime(duration.Add(ToTime(t), d))
}

// Difference returns the interval between t1 and t2, which may be negative.
func Difference(t1 TimeOfDay, t2 TimeOfDay) duration.Duration {
	nanos := ToTime(t1).Sub(ToTime(t2)).Nanoseconds()
	return duration.Duration{Nanos: nanos}
}

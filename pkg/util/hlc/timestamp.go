// Copyright 2014 The Cockroach Authors.
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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package hlc

import (
	"fmt"
	"math"
	"time"
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

func (t Timestamp) String() string {
	return fmt.Sprintf("%d.%09d,%d", t.WallTime/1E9, t.WallTime%1E9, t.Logical)
}

// Add returns a timestamp with the WallTime and Logical components increased.
// wallTime is expressed in nanos.
func (t Timestamp) Add(wallTime int64, logical int32) Timestamp {
	return Timestamp{
		WallTime: t.WallTime + wallTime,
		Logical:  t.Logical + logical,
	}
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
	return time.Unix(0, t.WallTime)
}

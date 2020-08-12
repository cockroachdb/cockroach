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
	"regexp"
	"strconv"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// Timestamp constant values.
var (
	// MaxTimestamp is the max value allowed for Timestamp.
	MaxTimestamp = Timestamp{WallTime: math.MaxInt64, Logical: math.MaxInt32}
	// MinTimestamp is the min value allowed for Timestamp.
	MinTimestamp = Timestamp{WallTime: 0, Logical: 1}
)

// Less returns whether the receiver is less than the parameter.
func (t Timestamp) Less(s Timestamp) bool {
	return t.WallTime < s.WallTime || (t.WallTime == s.WallTime && t.Logical < s.Logical)
}

// LessEq returns whether the receiver is less than or equal to the parameter.
func (t Timestamp) LessEq(s Timestamp) bool {
	return t.Less(s) || t == s
}

// String implements the fmt.Formatter interface.
func (t Timestamp) String() string {
	// The following code was originally written as
	//   fmt.Sprintf("%d.%09d,%d", t.WallTime/1e9, t.WallTime%1e9, t.Logical).
	// The main problem with the original code was that it would put
	// a negative sign in the middle (after the decimal point) if
	// the value happened to be negative.
	buf := make([]byte, 0, 20)

	w := t.WallTime
	if w == 0 {
		buf = append(buf, '0', ',')
	} else {
		if w < 0 {
			w = -w
			buf = append(buf, '-')
		}

		s, ns := uint64(w/1e9), uint64(w%1e9)
		buf = strconv.AppendUint(buf, s, 10)

		prev := len(buf)
		buf = append(buf, '.', '0', '0', '0', '0', '0', '0', '0', '0', '0', ',')
		// The following lines expand to:
		// 		for i := 0; i < 9; i++ {
		// 			buf[len(buf)-2-i] = byte('0' + ns%10)
		// 			ns = ns / 10
		// 		}
		zeroBuf := buf[prev+1 : len(buf)-1]
		zeroBuf[8] = byte('0' + ns%10)
		ns = ns / 10
		zeroBuf[7] = byte('0' + ns%10)
		ns = ns / 10
		zeroBuf[6] = byte('0' + ns%10)
		ns = ns / 10
		zeroBuf[5] = byte('0' + ns%10)
		ns = ns / 10
		zeroBuf[4] = byte('0' + ns%10)
		ns = ns / 10
		zeroBuf[3] = byte('0' + ns%10)
		ns = ns / 10
		zeroBuf[2] = byte('0' + ns%10)
		ns = ns / 10
		zeroBuf[1] = byte('0' + ns%10)
		ns = ns / 10
		zeroBuf[0] = byte('0' + ns%10)
	}
	buf = strconv.AppendInt(buf, int64(t.Logical), 10)

	return *(*string)(unsafe.Pointer(&buf))
}

// SafeValue implements the redact.SafeValue interface.
func (Timestamp) SafeValue() {}

var (
	timestampRegexp = regexp.MustCompile(
		`^(?P<sign>-)?(?P<secs>\d{1,19})(\.(?P<nanos>\d{1,20}))?,(?P<logical>-?\d{1,10})$`)
	signSubexp    = 1
	secsSubexp    = 2
	nanosSubexp   = 4
	logicalSubexp = 5
)

// ParseTimestamp attempts to parse the string generated from
// Timestamp.String().
func ParseTimestamp(str string) (_ Timestamp, err error) {
	matches := timestampRegexp.FindStringSubmatch(str)
	if matches == nil {
		return Timestamp{}, errors.Errorf("failed to parse %q as Timestamp", str)
	}
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "failed to parse %q as Timestamp", str)
		}
	}()
	seconds, err := strconv.ParseInt(matches[secsSubexp], 10, 64)
	if err != nil {
		return Timestamp{}, err
	}
	var nanos int64
	if nanosMatch := matches[nanosSubexp]; nanosMatch != "" {
		nanos, err = strconv.ParseInt(nanosMatch, 10, 64)
		if err != nil {
			return Timestamp{}, err
		}
	}
	logical, err := strconv.ParseInt(matches[logicalSubexp], 10, 32)
	if err != nil {
		return Timestamp{}, err
	}
	wallTime := seconds*time.Second.Nanoseconds() + nanos
	if matches[signSubexp] == "-" {
		wallTime *= -1
	}
	return Timestamp{
		WallTime: wallTime,
		Logical:  int32(logical),
	}, nil
}

// AsOfSystemTime returns a string to be used in an AS OF SYSTEM TIME query.
func (t Timestamp) AsOfSystemTime() string {
	return fmt.Sprintf("%d.%010d", t.WallTime, t.Logical)
}

// Less returns whether the receiver is less than the parameter.
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

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
	"strings"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

// Timestamp constant values.
var (
	// MaxTimestamp is the max value allowed for Timestamp.
	MaxTimestamp = Timestamp{WallTime: math.MaxInt64, Logical: math.MaxInt32}
	// MinTimestamp is the min value allowed for Timestamp.
	MinTimestamp = Timestamp{WallTime: 0, Logical: 1}
)

// EqOrdering returns whether the receiver sorts equally to the parameter.
//
// This method is split from tests of structural equality (Equal and the equals
// operator) because it does not consider differences in flags and only
// considers whether the walltime and logical time differ between the
// timestamps.
func (t Timestamp) EqOrdering(s Timestamp) bool {
	return t.WallTime == s.WallTime && t.Logical == s.Logical
}

// Less returns whether the receiver is less than the parameter.
func (t Timestamp) Less(s Timestamp) bool {
	return t.WallTime < s.WallTime || (t.WallTime == s.WallTime && t.Logical < s.Logical)
}

// LessEq returns whether the receiver is less than or equal to the parameter.
func (t Timestamp) LessEq(s Timestamp) bool {
	return t.WallTime < s.WallTime || (t.WallTime == s.WallTime && t.Logical <= s.Logical)
}

var flagStrings = map[TimestampFlag]string{
	TimestampFlag_SYNTHETIC: "syn",
}
var flagStringsInverted = func() map[string]TimestampFlag {
	m := make(map[string]TimestampFlag)
	for k, v := range flagStrings {
		m[v] = k
	}
	return m
}()

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

	if t.Flags != 0 {
		buf = append(buf, '[')
		comma := false
		for i := 0; i < 8; i++ {
			f := TimestampFlag(1 << i)
			if t.IsFlagSet(f) {
				if comma {
					buf = append(buf, ',')
				}
				comma = true
				buf = append(buf, flagStrings[f]...)
			}
		}
		buf = append(buf, ']')
	}

	return *(*string)(unsafe.Pointer(&buf))
}

// SafeValue implements the redact.SafeValue interface.
func (Timestamp) SafeValue() {}

var (
	timestampRegexp = regexp.MustCompile(
		`^(?P<sign>-)?(?P<secs>\d{1,19})(?:\.(?P<nanos>\d{1,20}))?,(?P<logical>-?\d{1,10})(?:\[(?P<flags>[\w,]+)\])?$`)
	signSubexp    = 1
	secsSubexp    = 2
	nanosSubexp   = 3
	logicalSubexp = 4
	flagsSubexp   = 5
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
	t := Timestamp{
		WallTime: wallTime,
		Logical:  int32(logical),
	}
	if flagsMatch := matches[flagsSubexp]; flagsMatch != "" {
		flagStrs := strings.Split(flagsMatch, ",")
		for _, flagStr := range flagStrs {
			if flagStr == "" {
				return Timestamp{}, errors.Errorf("empty flag provided")
			}
			flagMatch, ok := flagStringsInverted[flagStr]
			if !ok {
				return Timestamp{}, errors.Errorf("unknown flag %q provided", flagStr)
			}
			if t.IsFlagSet(flagMatch) {
				return Timestamp{}, errors.Errorf("duplicate flag %q provided", flagStr)
			}
			t = t.SetFlag(flagMatch)
		}
	}
	return t, nil
}

// AsOfSystemTime returns a string to be used in an AS OF SYSTEM TIME query.
func (t Timestamp) AsOfSystemTime() string {
	return fmt.Sprintf("%d.%010d", t.WallTime, t.Logical)
}

// IsEmpty retruns true if t is an empty Timestamp.
func (t Timestamp) IsEmpty() bool {
	return t == Timestamp{}
}

// IsFlagSet returns whether the specified flag is set on the timestamp.
func (t Timestamp) IsFlagSet(f TimestampFlag) bool {
	return t.Flags&uint32(f) != 0
}

// Add returns a timestamp with the WallTime and Logical components increased.
// wallTime is expressed in nanos.
func (t Timestamp) Add(wallTime int64, logical int32) Timestamp {
	return Timestamp{
		WallTime: t.WallTime + wallTime,
		Logical:  t.Logical + logical,
		Flags:    t.Flags,
	}
}

// SetFlag returns a timestamp with the specified flag set.
func (t Timestamp) SetFlag(f TimestampFlag) Timestamp {
	t.Flags = t.Flags | uint32(f)
	return t
}

// ClearFlag returns a timestamp with the specified flag cleared.
func (t Timestamp) ClearFlag(f TimestampFlag) Timestamp {
	t.Flags = t.Flags &^ uint32(f)
	return t
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
			Flags:    t.Flags,
		}
	}
	return Timestamp{
		WallTime: t.WallTime,
		Logical:  t.Logical + 1,
		Flags:    t.Flags,
	}
}

// Prev returns the next earliest timestamp.
func (t Timestamp) Prev() Timestamp {
	if t.Logical > 0 {
		return Timestamp{
			WallTime: t.WallTime,
			Logical:  t.Logical - 1,
			Flags:    t.Flags,
		}
	} else if t.WallTime > 0 {
		return Timestamp{
			WallTime: t.WallTime - 1,
			Logical:  math.MaxInt32,
			Flags:    t.Flags,
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
			Flags:    t.Flags,
		}
	} else if t.WallTime > 0 {
		return Timestamp{
			WallTime: t.WallTime - 1,
			Logical:  0,
			Flags:    t.Flags,
		}
	}
	panic("cannot take the previous value to a zero timestamp")
}

// Forward replaces the receiver with the argument, if that moves it forwards in
// time. Returns true if the timestamp was adjusted to a larger time and false
// otherwise.
func (t *Timestamp) Forward(s Timestamp) bool {
	if t.Less(s) {
		*t = s
		return true
	} else if t.EqOrdering(s) && onlyLeftSynthetic(*t, s) {
		// If the times are equal but t is synthetic while s is not, remove the
		// synthtic flag but continue to return false.
		*t = t.ClearFlag(TimestampFlag_SYNTHETIC)
	}
	return false
}

// Backward replaces the receiver with the argument, if that moves it backwards
// in time.
func (t *Timestamp) Backward(s Timestamp) {
	if s.Less(*t) {
		// Replace t with s. If s is synthetic while t is not, remove the
		// synthtic flag.
		if onlyLeftSynthetic(s, *t) {
			s = s.ClearFlag(TimestampFlag_SYNTHETIC)
		}
		*t = s
	} else if onlyLeftSynthetic(*t, s) {
		*t = t.ClearFlag(TimestampFlag_SYNTHETIC)
	}
}

func onlyLeftSynthetic(l, r Timestamp) bool {
	return l.IsFlagSet(TimestampFlag_SYNTHETIC) && !r.IsFlagSet(TimestampFlag_SYNTHETIC)
}

// GoTime converts the timestamp to a time.Time.
func (t Timestamp) GoTime() time.Time {
	return timeutil.Unix(0, t.WallTime)
}

// ToLegacyTimestamp converts a Timestamp to a LegacyTimestamp.
func (t Timestamp) ToLegacyTimestamp() LegacyTimestamp {
	var flags *uint32
	if t.Flags != 0 {
		flags = proto.Uint32(t.Flags)
	}
	return LegacyTimestamp{WallTime: t.WallTime, Logical: t.Logical, Flags: flags}
}

// ToTimestamp converts a LegacyTimestamp to a Timestamp.
func (t LegacyTimestamp) ToTimestamp() Timestamp {
	var flags uint32
	if t.Flags != nil {
		flags = *t.Flags
	}
	return Timestamp{WallTime: t.WallTime, Logical: t.Logical, Flags: flags}
}

// EqOrdering returns whether the receiver sorts equally to the parameter.
func (t LegacyTimestamp) EqOrdering(s LegacyTimestamp) bool {
	return t.ToTimestamp().EqOrdering(s.ToTimestamp())
}

// Less returns whether the receiver is less than the parameter.
func (t LegacyTimestamp) Less(s LegacyTimestamp) bool {
	return t.ToTimestamp().Less(s.ToTimestamp())
}

func (t LegacyTimestamp) String() string {
	return t.ToTimestamp().String()
}

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
	// MaxClockTimestamp is the max value allowed for ClockTimestamp.
	MaxClockTimestamp = ClockTimestamp{WallTime: math.MaxInt64, Logical: math.MaxInt32}
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

// String implements the fmt.Stringer interface.
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
		`^(?P<sign>-)?(?P<secs>\d{1,19})(?:\.(?P<nanos>\d{1,20}))?(?:,(?P<logical>-?\d{1,10}))?(?:\[(?P<flags>[\w,]+)\])?$`)
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
	wallTime := seconds*time.Second.Nanoseconds() + nanos
	if matches[signSubexp] == "-" {
		wallTime *= -1
	}
	var logical int64
	if logicalMatch := matches[logicalSubexp]; logicalMatch != "" {
		logical, err = strconv.ParseInt(logicalMatch, 10, 32)
		if err != nil {
			return Timestamp{}, err
		}
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
//
// TODO(nvanbenschoten): consider an AddNanos method that takes a time.Duration.
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

// String implements the fmt.Stringer interface.
func (t LegacyTimestamp) String() string {
	return t.ToTimestamp().String()
}

// ClockTimestamp is a Timestamp with the added capability of being able to
// update a peer's HLC clock. It possesses this capability because the clock
// timestamp itself is guaranteed to have come from an HLC clock somewhere in
// the system. As such, a clock timestamp is an promise that some node in the
// system has a clock with a reading equal to or above its value.
//
// ClockTimestamp is the statically typed version of a Timestamp with its
// Synthetic flag set to false.
type ClockTimestamp Timestamp

// TryToClockTimestamp attempts to downcast a Timestamp into a ClockTimestamp.
// Returns the result and a boolean indicating whether the cast succeeded.
func (t Timestamp) TryToClockTimestamp() (ClockTimestamp, bool) {
	if t.IsFlagSet(TimestampFlag_SYNTHETIC) {
		return ClockTimestamp{}, false
	}
	return ClockTimestamp(t), true
}

// UnsafeToClockTimestamp converts a Timestamp to a ClockTimestamp, regardless
// of whether such a cast would be legal according to the Synthetic flag. The
// method should only be used in tests.
func (t Timestamp) UnsafeToClockTimestamp() ClockTimestamp {
	// TODO(nvanbenschoten): unset the Synthetic flag here.
	return ClockTimestamp(t)
}

// ToTimestamp upcasts a ClockTimestamp into a Timestamp.
func (t ClockTimestamp) ToTimestamp() Timestamp {
	return Timestamp(t)
}

// Less returns whether the receiver is less than the parameter.
func (t ClockTimestamp) Less(s ClockTimestamp) bool { return Timestamp(t).Less(Timestamp(s)) }

// String implements the fmt.Stringer interface.
func (t ClockTimestamp) String() string { return t.ToTimestamp().String() }

// SafeValue implements the redact.SafeValue interface.
func (t ClockTimestamp) SafeValue() {}

// IsEmpty retruns true if t is an empty ClockTimestamp.
func (t ClockTimestamp) IsEmpty() bool { return Timestamp(t).IsEmpty() }

// Forward is like Timestamp.Forward, but for ClockTimestamps.
func (t *ClockTimestamp) Forward(s ClockTimestamp) bool { return (*Timestamp)(t).Forward(Timestamp(s)) }

// Reset implements the protoutil.Message interface.
func (t *ClockTimestamp) Reset() { (*Timestamp)(t).Reset() }

// ProtoMessage implements the protoutil.Message interface.
func (t *ClockTimestamp) ProtoMessage() {}

// MarshalTo implements the protoutil.Message interface.
func (t *ClockTimestamp) MarshalTo(data []byte) (int, error) { return (*Timestamp)(t).MarshalTo(data) }

// Unmarshal implements the protoutil.Message interface.
func (t *ClockTimestamp) Unmarshal(data []byte) error { return (*Timestamp)(t).Unmarshal(data) }

// Size implements the protoutil.Message interface.
func (t *ClockTimestamp) Size() int { return (*Timestamp)(t).Size() }

// Equal is needed for the gogoproto.equal option.
func (t *ClockTimestamp) Equal(that interface{}) bool {
	switch v := that.(type) {
	case nil:
		return t == nil
	case ClockTimestamp:
		return (*Timestamp)(t).Equal((Timestamp)(v))
	case *ClockTimestamp:
		return (*Timestamp)(t).Equal((*Timestamp)(v))
	default:
		return false
	}
}

// NewPopulatedClockTimestamp is needed for the gogoproto.populate option.
func NewPopulatedClockTimestamp(r randyTimestamp, easy bool) *ClockTimestamp {
	return (*ClockTimestamp)(NewPopulatedTimestamp(r, easy))
}

// Ignore unused warnings. The function is called, but by generated functions
// that themselves are unused.
var _ = NewPopulatedClockTimestamp

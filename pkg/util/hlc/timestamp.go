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

// String implements the fmt.Formatter interface.
func (t Timestamp) String() string {
	// The following code was originally written as
	//   fmt.Sprintf("%d.%09d,%d", t.WallTime/1e9, t.WallTime%1e9, t.Logical).
	// The main problem with the original code was that it would put
	// a negative sign in the middle (after the decimal point) if
	// the value happened to be negative.
	buf := make([]byte, 0, 21)

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

	if !t.FromClock && !t.IsEmpty() {
		// 0,0 (an empty timestamp) is always considered to have come from a
		// clock for formatting purposes. All others timestamps that did not
		// come from a clock denote this using a question mark.
		buf = append(buf, '?')
	}

	return *(*string)(unsafe.Pointer(&buf))
}

// SafeValue implements the redact.SafeValue interface.
func (Timestamp) SafeValue() {}

var (
	timestampRegexp = regexp.MustCompile(
		`^(?P<sign>-)?(?P<secs>\d{1,19})(?:\.(?P<nanos>\d{1,20}))?(?:,(?P<logical>-?\d{1,10}))?(?P<clock>\?)?$`)
	signSubexp    = 1
	secsSubexp    = 2
	nanosSubexp   = 3
	logicalSubexp = 4
	clockSubexp   = 5
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
	fromClock := matches[clockSubexp] == ""
	t := Timestamp{
		WallTime:  wallTime,
		Logical:   int32(logical),
		FromClock: fromClock,
	}
	return t, nil
}

// AsOfSystemTime returns a string to be used in an AS OF SYSTEM TIME query.
func (t Timestamp) AsOfSystemTime() string {
	return fmt.Sprintf("%d.%010d", t.WallTime, t.Logical)
}

// IsEmpty retruns true if t is an empty Timestamp. The method ignores the
// FromClock flag.
func (t Timestamp) IsEmpty() bool {
	return t.WallTime == 0 && t.Logical == 0
}

// Add returns a timestamp with the WallTime and Logical components increased.
// wallTime is expressed in nanos.
func (t Timestamp) Add(wallTime int64, logical int32) Timestamp {
	s := Timestamp{
		WallTime:  t.WallTime + wallTime,
		Logical:   t.Logical + logical,
		FromClock: t.FromClock,
	}
	if t.Less(s) {
		// Adding a positive value to a Timestamp removes its FromClock flag.
		s.FromClock = false
	}
	return s
}

// SetFromClock ... WIP
func (t Timestamp) SetFromClock(val bool) Timestamp {
	t.FromClock = val
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
			WallTime:  t.WallTime + 1,
			FromClock: t.FromClock,
		}
	}
	return Timestamp{
		WallTime:  t.WallTime,
		Logical:   t.Logical + 1,
		FromClock: t.FromClock,
	}
}

// Prev returns the next earliest timestamp.
func (t Timestamp) Prev() Timestamp {
	if t.Logical > 0 {
		return Timestamp{
			WallTime:  t.WallTime,
			Logical:   t.Logical - 1,
			FromClock: t.FromClock,
		}
	} else if t.WallTime > 0 {
		return Timestamp{
			WallTime:  t.WallTime - 1,
			Logical:   math.MaxInt32,
			FromClock: t.FromClock,
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
			WallTime:  t.WallTime,
			Logical:   t.Logical - 1,
			FromClock: t.FromClock,
		}
	} else if t.WallTime > 0 {
		return Timestamp{
			WallTime:  t.WallTime - 1,
			Logical:   0,
			FromClock: t.FromClock,
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
	} else if t.EqOrdering(s) {
		t.FromClock = eitherFromClock(*t, s)
	}
	return false
}

// Backward replaces the receiver with the argument, if that moves it backwards
// in time.
func (t *Timestamp) Backward(s Timestamp) {
	fromClock := eitherFromClock(*t, s)
	if s.Less(*t) {
		*t = s
	}
	t.FromClock = fromClock
}

func eitherFromClock(l, r Timestamp) bool {
	return l.FromClock || r.FromClock
}

// GoTime converts the timestamp to a time.Time.
func (t Timestamp) GoTime() time.Time {
	return timeutil.Unix(0, t.WallTime)
}

var trueBool = true

// ToLegacyTimestamp converts a Timestamp to a LegacyTimestamp.
func (t Timestamp) ToLegacyTimestamp() LegacyTimestamp {
	var fromClock *bool
	if t.FromClock {
		fromClock = &trueBool
	}
	return LegacyTimestamp{WallTime: t.WallTime, Logical: t.Logical, FromClock: fromClock}
}

// ToTimestamp converts a LegacyTimestamp to a Timestamp.
func (t LegacyTimestamp) ToTimestamp() Timestamp {
	var fromClock bool
	if t.FromClock != nil {
		fromClock = *t.FromClock
	}
	return Timestamp{WallTime: t.WallTime, Logical: t.Logical, FromClock: fromClock}
}

// EqOrdering returns whether the receiver sorts equally to the parameter.
func (t LegacyTimestamp) EqOrdering(s LegacyTimestamp) bool {
	return t.ToTimestamp().EqOrdering(s.ToTimestamp())
}

// Less returns whether the receiver is less than the parameter.
func (t LegacyTimestamp) Less(s LegacyTimestamp) bool {
	return t.ToTimestamp().Less(s.ToTimestamp())
}

// String implements the fmt.Formatter interface.
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
// FromClock flag set. However, instances do not have their FromClock flag set,
// as this would increase their encoded size with no real benefit.
type ClockTimestamp Timestamp

// TryToClockTimestamp attempts to downcast a Timestamp into a ClockTimestamp.
// Returns the result and a boolean indicating whether the cast succeeded.
//
// TODO(nvanbenschoten): what about the migration in a mixed version cluster? In
// such cases, old nodes will never set the FromClock flag, but will also
// consider all timestamps to be ClockTimestamps from the perspective of being
// able to use them to update HLC clocks. They will also need all timestamps to
// be written as ClockTimestamps to be able to interpret MVCC. But we also can't
// blindly consider all timestamps to be clock timestamps, because a timestamp
// may actually be in the future once v21.1 nodes know that all v20.2 nodes have
// been upgraded.
//
// The migration might look something like the following:
// 1. introduce a new ClockTimestamps cluster version
// 2. add client server version to BatchRequest / grab from RPC handshake.
// 3. mark all timestamps in requests / responses from such nodes as FromClock.
// 4. don't start creating non-clock, future timestamps until this cluster
//    version is active.
//
// Or maybe a long-running migration might help. All we really need is for
// no-one to create non-clock timestamps until everyone is upgraded and everyone
// knows that everyone is upgraded.
func (t Timestamp) TryToClockTimestamp() (ClockTimestamp, bool) {
	if !t.FromClock {
		return ClockTimestamp{}, false
	}
	t.FromClock = false // unset, ClockTimestamps don't carry flag
	return ClockTimestamp(t), true
}

// UnsafeToClockTimestamp converts a Timestamp to a ClockTimestamp, regardless
// of whether such a cast would be legal according to the FromClock flag. The
// method should only be used in tests.
func (t Timestamp) UnsafeToClockTimestamp() ClockTimestamp {
	t.FromClock = false // unset, ClockTimestamps don't carry flag
	return ClockTimestamp(t)
}

// ToTimestamp upcasts a ClockTimestamp into a Timestamp. The method sets the
// timestamp's FromClock flag so that a call to TryToClockTimestamp will succeed
// if the resulting Timestamp is never mutated.
func (t ClockTimestamp) ToTimestamp() Timestamp {
	return Timestamp(t).SetFromClock(true)
}

// Less returns whether the receiver is less than the parameter.
func (t ClockTimestamp) Less(s ClockTimestamp) bool { return Timestamp(t).Less(Timestamp(s)) }

// String implements the fmt.Formatter interface.
func (t ClockTimestamp) String() string { return t.ToTimestamp().String() }

// SafeValue implements the redact.SafeValue interface.
func (t ClockTimestamp) SafeValue() {}

// IsEmpty retruns true if t is an empty ClockTimestamp.
func (t ClockTimestamp) IsEmpty() bool { return Timestamp(t).IsEmpty() }

// Forward is like Timestamp.Forward, but for ClockTimestamps.
func (t *ClockTimestamp) Forward(s ClockTimestamp) bool { return (*Timestamp)(t).Forward(Timestamp(s)) }

// Backward is like Timestamp.Backward, but for ClockTimestamps.
func (t *ClockTimestamp) Backward(s ClockTimestamp) { (*Timestamp)(t).Backward(Timestamp(s)) }

// Reset implements the proto.Message interface.
func (t *ClockTimestamp) Reset() { (*Timestamp)(t).Reset() }

// ProtoMessage implements the proto.Message interface.
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

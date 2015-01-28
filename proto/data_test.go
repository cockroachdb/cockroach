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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package proto

import (
	"bytes"
	"math"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	gogoproto "github.com/gogo/protobuf/proto"
)

// TestKeyNext tests that the method for creating lexicographic
// successors to byte slices works as expected.
func TestKeyNext(t *testing.T) {
	a := Key("a")
	aNext := a.Next()
	if a.Equal(aNext) {
		t.Errorf("expected key not equal to next")
	}
	if !a.Less(aNext) {
		t.Errorf("expected next key to be greater")
	}

	testCases := []struct {
		key  Key
		next Key
	}{
		{nil, Key("\x00")},
		{Key(""), Key("\x00")},
		{Key("test key"), Key("test key\x00")},
		{Key("\xff"), Key("\xff\x00")},
		{Key("xoxo\x00"), Key("xoxo\x00\x00")},
	}
	for i, c := range testCases {
		if !bytes.Equal(c.key.Next(), c.next) {
			t.Errorf("%d: unexpected next bytes for %q: %q", i, c.key, c.key.Next())
		}
	}
}

func TestKeyPrefixEnd(t *testing.T) {
	a := Key("a1")
	aNext := a.Next()
	aEnd := a.PrefixEnd()
	if !a.Less(aEnd) {
		t.Errorf("expected end key to be greater")
	}
	if !aNext.Less(aEnd) {
		t.Errorf("expected end key to be greater than next")
	}

	testCases := []struct {
		key Key
		end Key
	}{
		{Key{}, KeyMax},
		{Key{0}, Key{0x01}},
		{Key{0xff}, Key{0xff}},
		{Key{0xff, 0xff}, Key{0xff, 0xff}},
		{KeyMax, KeyMax},
		{Key{0xff, 0xfe}, Key{0xff, 0xff}},
		{Key{0x00, 0x00}, Key{0x00, 0x01}},
		{Key{0x00, 0xff}, Key{0x01, 0x00}},
		{Key{0x00, 0xff, 0xff}, Key{0x01, 0x00, 0x00}},
	}
	for i, c := range testCases {
		if !bytes.Equal(c.key.PrefixEnd(), c.end) {
			t.Errorf("%d: unexpected prefix end bytes for %q: %q", i, c.key, c.key.PrefixEnd())
		}
	}
}

func TestKeyEqual(t *testing.T) {
	a1 := Key("a1")
	a2 := Key("a2")
	if !a1.Equal(a1) {
		t.Errorf("expected keys equal")
	}
	if a1.Equal(a2) {
		t.Errorf("expected different keys not equal")
	}
}

func TestKeyLess(t *testing.T) {
	testCases := []struct {
		a, b Key
		less bool
	}{
		{nil, Key("\x00"), true},
		{Key(""), Key("\x00"), true},
		{Key("a"), Key("b"), true},
		{Key("a\x00"), Key("a"), false},
		{Key("a\x00"), Key("a\x01"), true},
	}
	for i, c := range testCases {
		if c.a.Less(c.b) != c.less {
			t.Fatalf("%d: unexpected %q < %q: %t", i, c.a, c.b, c.less)
		}
	}
}

func TestKeyCompare(t *testing.T) {
	testCases := []struct {
		a, b    Key
		compare int
	}{
		{nil, nil, 0},
		{nil, Key("\x00"), -1},
		{Key("\x00"), Key("\x00"), 0},
		{Key(""), Key("\x00"), -1},
		{Key("a"), Key("b"), -1},
		{Key("a\x00"), Key("a"), 1},
		{Key("a\x00"), Key("a\x01"), -1},
	}
	for i, c := range testCases {
		if c.a.Compare(c.b) != c.compare {
			t.Fatalf("%d: unexpected %q.Compare(%q): %d", i, c.a, c.b, c.compare)
		}
	}
}

// TestNextKey tests that the method for creating successors of a Key
// works as expected.
func TestNextKey(t *testing.T) {
	testCases := []struct {
		key  Key
		next Key
	}{
		{nil, Key("\x00")},
		{Key(""), Key("\x00")},
		{Key("test key"), Key("test key\x00")},
		{Key("\xff\xff"), Key("\xff\xff\x00")},
		{Key("xoxo\x00"), Key("xoxo\x00\x00")},
	}
	for i, c := range testCases {
		if !c.key.Next().Equal(c.next) {
			t.Fatalf("%d: unexpected next key for %q: %s", i, c.key, c.key.Next())
		}
	}
}

// TestPrevKey tests that the method for creating the predecessor of a Key
// works as expected.
func TestPrevKey(t *testing.T) {
	testCases := []struct {
		key  Key
		prev Key
	}{
		{Key("\x00"), Key("")},
		{Key("test key\x00"), Key("test key")},
		// "test key\x01" -> "test key\x00\xff..."
		{
			Key("test key\x01"),
			Key(strings.Join([]string{
				"test key\x00",
				strings.Repeat("\xff", KeyMaxLength-9)}, "")),
		},
		// "\x01" -> "\x00\xff..."
		{
			Key("\x01"),
			Key(strings.Join([]string{
				"\x00",
				strings.Repeat("\xff", KeyMaxLength-1)}, "")),
		},
		// "\xff...\x01" -> "\xff...\x00"
		{
			Key(strings.Join([]string{
				strings.Repeat("\xff", KeyMaxLength-1),
				"\x01"}, "")),
			Key(strings.Join([]string{
				strings.Repeat("\xff", KeyMaxLength-1), "\x00"}, "")),
		},
		// "\xff..." -> "\xff...\xfe"
		{
			KeyMax,
			Key(strings.Join([]string{
				strings.Repeat("\xff", KeyMaxLength-1), "\xfe"}, "")),
		},
		// "\xff...\x00" -> "\xff..." with the \x00 removed only
		{
			Key(strings.Join([]string{
				strings.Repeat("\xff", KeyMaxLength-1),
				"\x00"}, "")),
			Key(strings.Repeat("\xff", KeyMaxLength-1)),
		},
	}
	for i, c := range testCases {
		if !c.key.Prev().Equal(c.prev) {
			t.Fatalf("%d: unexpected prev key for %d: %d", i, c.key, c.key.Prev())
		}
	}

	defer func() {
		if err := recover(); err == nil {
			t.Error("Should panic when trying to find prev of keymin")
		}
	}()
	KeyMin.Prev()
}

func TestKeyString(t *testing.T) {
	if KeyMax.String() != "\xff..." {
		t.Errorf("expected key max to display a compact version: %s", KeyMax.String())
	}
	if str := Key(append([]byte("foo"), KeyMax...)).String(); str != "foo\xff..." {
		t.Errorf("expected \"foo\xff...\"; got %q", str)
	}
}

func makeTS(walltime int64, logical int32) Timestamp {
	return Timestamp{
		WallTime: walltime,
		Logical:  logical,
	}
}

func TestLess(t *testing.T) {
	a := Timestamp{}
	b := Timestamp{}
	if a.Less(b) || b.Less(a) {
		t.Errorf("expected %+v == %+v", a, b)
	}
	b = makeTS(1, 0)
	if !a.Less(b) {
		t.Errorf("expected %+v < %+v", a, b)
	}
	a = makeTS(1, 1)
	if !b.Less(a) {
		t.Errorf("expected %+v < %+v", b, a)
	}
}

func TestEqual(t *testing.T) {
	a := Timestamp{}
	b := Timestamp{}
	if !a.Equal(b) {
		t.Errorf("expected %+v == %+v", a, b)
	}
	b = makeTS(1, 0)
	if a.Equal(b) {
		t.Errorf("expected %+v < %+v", a, b)
	}
	a = makeTS(1, 1)
	if b.Equal(a) {
		t.Errorf("expected %+v < %+v", b, a)
	}
}

func TestValueBothBytesAndIntegerSet(t *testing.T) {
	k := []byte("key")
	v := Value{Bytes: []byte("a"), Integer: gogoproto.Int64(0)}
	if err := v.Verify(k); err == nil {
		t.Error("expected error with both byte slice and integer fields set")
	}
}

// TestValueZeroIntegerSerialization verifies that a value with
// integer=0 set can be marshalled and unmarshalled successfully.
// This tests exists because gob serialization treats integers
// and pointers to integers as the same and so loses a proto.Value
// which encodes integer=0.
//
// TODO(spencer): change Value type to switch between integer and
//   []byte value types using a mechanism other than nil pointers.
func TestValueZeroIntegerSerialization(t *testing.T) {
	k := Key("key 00")
	v := Value{Integer: gogoproto.Int64(0)}
	v.InitChecksum(k)

	data, err := gogoproto.Marshal(&v)
	if err != nil {
		t.Fatal(err)
	}
	v2 := &Value{}
	if err = gogoproto.Unmarshal(data, v2); err != nil {
		t.Fatal(err)
	}
	if v2.Integer == nil {
		t.Errorf("expected non-nil integer value; got %s", v2)
	} else if v2.GetInteger() != 0 {
		t.Errorf("expected zero integer value; got %d", v2.GetInteger())
	} else if err = v2.Verify(k); err != nil {
		t.Errorf("failed value verification: %s", err)
	}
}

func TestValueChecksumEmpty(t *testing.T) {
	k := []byte("key")
	v := Value{}
	// Before initializing checksum, always works.
	if err := v.Verify(k); err != nil {
		t.Error(err)
	}
	if err := v.Verify([]byte("key2")); err != nil {
		t.Error(err)
	}
	v.InitChecksum(k)
	if err := v.Verify(k); err != nil {
		t.Error(err)
	}
}

func TestValueChecksumWithBytes(t *testing.T) {
	k := []byte("key")
	v := Value{Bytes: []byte("abc")}
	v.InitChecksum(k)
	if err := v.Verify(k); err != nil {
		t.Error(err)
	}
	// Try a different key; should fail.
	if err := v.Verify([]byte("key2")); err == nil {
		t.Error("expected checksum verification failure on different key")
	}
	// Mess with value.
	v.Bytes = []byte("abcd")
	if err := v.Verify(k); err == nil {
		t.Error("expected checksum verification failure on different value")
	}
}

func TestValueChecksumWithInteger(t *testing.T) {
	k := []byte("key")
	testValues := []int64{0, 1, -1, math.MinInt64, math.MaxInt64}
	for _, i := range testValues {
		v := Value{Integer: gogoproto.Int64(i)}
		v.InitChecksum(k)
		if err := v.Verify(k); err != nil {
			t.Error(err)
		}
		// Try a different key; should fail.
		if err := v.Verify([]byte("key2")); err == nil {
			t.Error("expected checksum verification failure on different key")
		}
		// Mess with value.
		v.Integer = gogoproto.Int64(i + 1)
		if err := v.Verify(k); err == nil {
			t.Error("expected checksum verification failure on different value")
		}
	}
}

// TestNodeList verifies that its public methods Add() and Contain()
// operate as expected.
func TestNodeList(t *testing.T) {
	sn := NodeList{}
	items := append([]int{109, 104, 102, 108, 1000}, rand.Perm(100)...)
	for i := range items {
		n := int32(items[i])
		if sn.Contains(n) {
			t.Fatalf("%d: false positive hit for %d on slice %v",
				i, n, sn.GetNodes())
		}
		// Add this item and, for good measure, all the previous ones.
		for j := i; j >= 0; j-- {
			sn.Add(int32(items[j]))
		}
		if nodes := sn.GetNodes(); len(nodes) != i+1 {
			t.Fatalf("%d: missing values or duplicates: %v",
				i, nodes)
		}
		if !sn.Contains(n) {
			t.Fatalf("%d: false negative hit for %d on slice %v",
				i, n, sn.GetNodes())
		}
	}
}

func ts(name string, dps ...*TimeSeriesDatapoint) *TimeSeriesData {
	return &TimeSeriesData{
		Name:       name,
		Datapoints: dps,
	}
}

func tsdpi(ts time.Duration, val int64) *TimeSeriesDatapoint {
	return &TimeSeriesDatapoint{
		TimestampNanos: int64(ts),
		IntValue:       gogoproto.Int64(val),
	}
}

func tsdpf(ts time.Duration, val float32) *TimeSeriesDatapoint {
	return &TimeSeriesDatapoint{
		TimestampNanos: int64(ts),
		FloatValue:     gogoproto.Float32(val),
	}
}

// TestToInternal verifies the conversion of TimeSeriesData to internal storage
// format is correct.
func TestToInternal(t *testing.T) {
	tcases := []struct {
		keyDuration    int64
		sampleDuration int64
		expectsError   bool
		input          *TimeSeriesData
		expected       []*InternalTimeSeriesData
	}{
		{
			time.Minute.Nanoseconds(),
			101,
			true,
			ts("error.series"),
			nil,
		},
		{
			time.Minute.Nanoseconds(),
			time.Hour.Nanoseconds(),
			true,
			ts("error.series"),
			nil,
		},
		{
			time.Hour.Nanoseconds(),
			time.Second.Nanoseconds(),
			true,
			ts("error.series",
				tsdpi((time.Hour*50)+(time.Second*5), 1),
				&TimeSeriesDatapoint{},
			),
			nil,
		},
		{
			time.Hour.Nanoseconds(),
			time.Second.Nanoseconds(),
			true,
			ts("error.series",
				tsdpi((time.Hour*50)+(time.Second*5), 1),
				&TimeSeriesDatapoint{
					IntValue:   gogoproto.Int64(5),
					FloatValue: gogoproto.Float32(5.0),
				},
			),
			nil,
		},
		{
			time.Hour.Nanoseconds(),
			time.Second.Nanoseconds(),
			false,
			ts("test.series",
				tsdpi((time.Hour*50)+(time.Second*5), 1),
				tsdpi((time.Hour*51)+(time.Second*3), 2),
				tsdpi((time.Hour*50)+(time.Second*10), 3),
				tsdpi((time.Hour*53), 4),
				tsdpi((time.Hour*50)+(time.Second*5)+1, 5),
				tsdpi((time.Hour*53)+(time.Second*15), 0),
			),
			[]*InternalTimeSeriesData{
				{
					StartTimestampNanos: int64(time.Hour * 50),
					SampleDurationNanos: int64(time.Second),
					Samples: []*InternalTimeSeriesSample{
						{
							Offset:   5,
							IntCount: 1,
							IntSum:   gogoproto.Int64(1),
						},
						{
							Offset:   10,
							IntCount: 1,
							IntSum:   gogoproto.Int64(3),
						},
						{
							Offset:   5,
							IntCount: 1,
							IntSum:   gogoproto.Int64(5),
						},
					},
				},
				{
					StartTimestampNanos: int64(time.Hour * 51),
					SampleDurationNanos: int64(time.Second),
					Samples: []*InternalTimeSeriesSample{
						{
							Offset:   3,
							IntCount: 1,
							IntSum:   gogoproto.Int64(2),
						},
					},
				},
				{
					StartTimestampNanos: int64(time.Hour * 53),
					SampleDurationNanos: int64(time.Second),
					Samples: []*InternalTimeSeriesSample{
						{
							Offset:   0,
							IntCount: 1,
							IntSum:   gogoproto.Int64(4),
						},
						{
							Offset:   15,
							IntCount: 1,
							IntSum:   gogoproto.Int64(0),
						},
					},
				},
			},
		},
		{
			(time.Hour * 24).Nanoseconds(),
			(time.Minute * 20).Nanoseconds(),
			false,
			ts("test.series",
				tsdpf((time.Hour*5)+(time.Minute*5), 1.0),
				tsdpf((time.Hour*24)+(time.Minute*39), 2.0),
				tsdpf((time.Hour*10)+(time.Minute*10), 3.0),
				tsdpf((time.Hour*48), 4.0),
				tsdpf((time.Hour*15)+(time.Minute*22)+1, 5.0),
				tsdpf((time.Hour*52)+(time.Minute*15), 0.0),
			),
			[]*InternalTimeSeriesData{
				{
					StartTimestampNanos: 0,
					SampleDurationNanos: int64(time.Minute * 20),
					Samples: []*InternalTimeSeriesSample{
						{
							Offset:     15,
							FloatCount: 1,
							FloatSum:   gogoproto.Float32(1.0),
						},
						{
							Offset:     30,
							FloatCount: 1,
							FloatSum:   gogoproto.Float32(3.0),
						},
						{
							Offset:     46,
							FloatCount: 1,
							FloatSum:   gogoproto.Float32(5.0),
						},
					},
				},
				{
					StartTimestampNanos: int64(time.Hour * 24),
					SampleDurationNanos: int64(time.Minute * 20),
					Samples: []*InternalTimeSeriesSample{
						{
							Offset:     1,
							FloatCount: 1,
							FloatSum:   gogoproto.Float32(2.0),
						},
					},
				},
				{
					StartTimestampNanos: int64(time.Hour * 48),
					SampleDurationNanos: int64(time.Minute * 20),
					Samples: []*InternalTimeSeriesSample{
						{
							Offset:     0,
							FloatCount: 1,
							FloatSum:   gogoproto.Float32(4.0),
						},
						{
							Offset:     12,
							FloatCount: 1,
							FloatSum:   gogoproto.Float32(0.0),
						},
					},
				},
			},
		},
	}

	for i, tc := range tcases {
		actual, err := tc.input.ToInternal(tc.keyDuration, tc.sampleDuration)
		if err != nil {
			if !tc.expectsError {
				t.Errorf("unexpected error from case %d: %s", i, err.Error())
			}
			continue
		} else if tc.expectsError {
			t.Errorf("expected error from case %d, none encountered", i)
			continue
		}

		if !reflect.DeepEqual(actual, tc.expected) {
			t.Errorf("case %d fails: ToInternal result was %v, expected %v", i, actual, tc.expected)
		}
	}
}

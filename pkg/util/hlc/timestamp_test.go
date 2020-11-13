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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func makeTS(walltime int64, logical int32) Timestamp {
	return Timestamp{
		WallTime: walltime,
		Logical:  logical,
	}
}

func makeTSWithFlags(walltime int64, logical int32, flags TimestampFlag) Timestamp {
	return Timestamp{
		WallTime: walltime,
		Logical:  logical,
		Flags:    uint32(flags),
	}
}

func TestEqOrdering(t *testing.T) {
	a := Timestamp{}
	b := Timestamp{}
	if !a.EqOrdering(b) {
		t.Errorf("expected %+v == %+v", a, b)
	}
	b = makeTS(1, 0)
	if a.EqOrdering(b) {
		t.Errorf("expected %+v != %+v", a, b)
	}
	a = makeTS(1, 1)
	if a.EqOrdering(b) {
		t.Errorf("expected %+v != %+v", b, a)
	}
	b = makeTSWithFlags(1, 1, 3)
	if !a.EqOrdering(b) {
		t.Errorf("expected %+v == %+v", b, a)
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
	b = makeTSWithFlags(1, 1, 3)
	if a.Less(b) || b.Less(a) {
		t.Errorf("expected %+v == %+v", a, b)
	}
}

func TestLessEq(t *testing.T) {
	a := Timestamp{}
	b := Timestamp{}
	if !a.LessEq(b) || !b.LessEq(a) {
		t.Errorf("expected %+v == %+v", a, b)
	}
	b = makeTS(1, 0)
	if !a.LessEq(b) || b.LessEq(a) {
		t.Errorf("expected %+v < %+v", a, b)
	}
	a = makeTS(1, 1)
	if !b.LessEq(a) || a.LessEq(b) {
		t.Errorf("expected %+v < %+v", b, a)
	}
	b = makeTSWithFlags(1, 1, 3)
	if !a.LessEq(b) || !b.LessEq(a) {
		t.Errorf("expected %+v == %+v", a, b)
	}
}

func TestIsEmpty(t *testing.T) {
	a := Timestamp{}
	assert.True(t, a.IsEmpty())
	a = makeTS(1, 0)
	assert.False(t, a.IsEmpty())
	a = makeTS(0, 1)
	assert.False(t, a.IsEmpty())
	a = makeTSWithFlags(0, 0, 3)
	assert.False(t, a.IsEmpty())
}

func TestSetFlag(t *testing.T) {
	a := Timestamp{}
	assert.False(t, a.IsFlagSet(TimestampFlag_SYNTHETIC))
	a = a.SetFlag(TimestampFlag_UNKNOWN)
	assert.False(t, a.IsFlagSet(TimestampFlag_SYNTHETIC))
	a = a.SetFlag(TimestampFlag_SYNTHETIC)
	assert.True(t, a.IsFlagSet(TimestampFlag_SYNTHETIC))
}

func TestTimestampNext(t *testing.T) {
	testCases := []struct {
		ts, expNext Timestamp
	}{
		{makeTS(1, 2), makeTS(1, 3)},
		{makeTS(1, math.MaxInt32-1), makeTS(1, math.MaxInt32)},
		{makeTS(1, math.MaxInt32), makeTS(2, 0)},
		{makeTS(math.MaxInt32, math.MaxInt32), makeTS(math.MaxInt32+1, 0)},
		{makeTSWithFlags(1, 2, 3), makeTSWithFlags(1, 3, 3)},
		{makeTSWithFlags(1, math.MaxInt32-1, 3), makeTSWithFlags(1, math.MaxInt32, 3)},
		{makeTSWithFlags(1, math.MaxInt32, 3), makeTSWithFlags(2, 0, 3)},
		{makeTSWithFlags(math.MaxInt32, math.MaxInt32, 3), makeTSWithFlags(math.MaxInt32+1, 0, 3)},
	}
	for _, c := range testCases {
		assert.Equal(t, c.expNext, c.ts.Next())
	}
}

func TestTimestampPrev(t *testing.T) {
	testCases := []struct {
		ts, expPrev Timestamp
	}{
		{makeTS(1, 2), makeTS(1, 1)},
		{makeTS(1, 1), makeTS(1, 0)},
		{makeTS(1, 0), makeTS(0, math.MaxInt32)},
		{makeTSWithFlags(1, 2, 3), makeTSWithFlags(1, 1, 3)},
		{makeTSWithFlags(1, 1, 3), makeTSWithFlags(1, 0, 3)},
		{makeTSWithFlags(1, 0, 3), makeTSWithFlags(0, math.MaxInt32, 3)},
	}
	for _, c := range testCases {
		assert.Equal(t, c.expPrev, c.ts.Prev())
	}
}

func TestTimestampFloorPrev(t *testing.T) {
	testCases := []struct {
		ts, expPrev Timestamp
	}{
		{makeTS(2, 0), makeTS(1, 0)},
		{makeTS(1, 2), makeTS(1, 1)},
		{makeTS(1, 1), makeTS(1, 0)},
		{makeTS(1, 0), makeTS(0, 0)},
		{makeTSWithFlags(2, 0, 3), makeTSWithFlags(1, 0, 3)},
		{makeTSWithFlags(1, 2, 3), makeTSWithFlags(1, 1, 3)},
		{makeTSWithFlags(1, 1, 3), makeTSWithFlags(1, 0, 3)},
		{makeTSWithFlags(1, 0, 3), makeTSWithFlags(0, 0, 3)},
	}
	for _, c := range testCases {
		assert.Equal(t, c.expPrev, c.ts.FloorPrev())
	}
}

func TestAsOfSystemTime(t *testing.T) {
	testCases := []struct {
		ts  Timestamp
		exp string
	}{
		{makeTS(145, 0), "145.0000000000"},
		{makeTS(145, 123), "145.0000000123"},
		{makeTS(145, 1123456789), "145.1123456789"},
	}
	for _, c := range testCases {
		assert.Equal(t, c.exp, c.ts.AsOfSystemTime())
	}
}

func TestTimestampString(t *testing.T) {
	testCases := []struct {
		ts  Timestamp
		exp string
	}{
		{makeTS(0, 0), "0,0"},
		{makeTS(0, 123), "0,123"},
		{makeTS(0, -123), "0,-123"},
		{makeTS(1, 0), "0.000000001,0"},
		{makeTS(-1, 0), "-0.000000001,0"},
		{makeTS(1, 123), "0.000000001,123"},
		{makeTS(-1, -123), "-0.000000001,-123"},
		{makeTS(123, 0), "0.000000123,0"},
		{makeTS(-123, 0), "-0.000000123,0"},
		{makeTS(1234567890, 0), "1.234567890,0"},
		{makeTS(-1234567890, 0), "-1.234567890,0"},
		{makeTS(6661234567890, 0), "6661.234567890,0"},
		{makeTS(-6661234567890, 0), "-6661.234567890,0"},
		{makeTSWithFlags(0, 0, TimestampFlag_SYNTHETIC), "0,0[syn]"},
		{makeTSWithFlags(0, 123, TimestampFlag_SYNTHETIC), "0,123[syn]"},
		{makeTSWithFlags(1, 0, TimestampFlag_SYNTHETIC), "0.000000001,0[syn]"},
		{makeTSWithFlags(1, 123, TimestampFlag_SYNTHETIC), "0.000000001,123[syn]"},
	}
	for _, c := range testCases {
		assert.Equal(t, c.exp, c.ts.String())
		parsed, err := ParseTimestamp(c.ts.String())
		assert.NoError(t, err)
		assert.Equal(t, c.ts, parsed)
	}
}

func TestParseTimestamp(t *testing.T) {
	for _, c := range []struct {
		s      string
		expErr string
	}{
		{
			"asdf",
			"failed to parse \"asdf\" as Timestamp",
		},
		{
			"-1.-1,0",
			"failed to parse \"-1.-1,0\" as Timestamp",
		},
		{
			"9999999999999999999,0",
			"failed to parse \"9999999999999999999,0\" as Timestamp: strconv.ParseInt: parsing \"9999999999999999999\": value out of range",
		},
		{
			"1.9999999999999999999,0",
			"failed to parse \"1.9999999999999999999,0\" as Timestamp: strconv.ParseInt: parsing \"9999999999999999999\": value out of range",
		},
		{
			"0,123[]",
			"failed to parse \"0,123\\[\\]\" as Timestamp",
		},
		{
			"0,123[bad]",
			"failed to parse \"0,123\\[bad\\]\" as Timestamp: unknown flag \"bad\" provided",
		},
		{
			"0,123[syn,]",
			"failed to parse \"0,123\\[syn,\\]\" as Timestamp: empty flag provided",
		},
		{
			"0,123[syn,syn]",
			"failed to parse \"0,123\\[syn,syn\\]\" as Timestamp: duplicate flag \"syn\" provided",
		},
	} {
		_, err := ParseTimestamp(c.s)
		if assert.Error(t, err) {
			assert.Regexp(t, c.expErr, err.Error())
		}
	}
}

func BenchmarkTimestampString(b *testing.B) {
	ts := makeTS(-6661234567890, 0)

	for i := 0; i < b.N; i++ {
		_ = ts.String()
	}
}

func BenchmarkTimestampStringWithFlags(b *testing.B) {
	ts := makeTSWithFlags(-6661234567890, 0, TimestampFlag_SYNTHETIC)

	for i := 0; i < b.N; i++ {
		_ = ts.String()
	}
}

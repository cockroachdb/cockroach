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
	return Timestamp{WallTime: walltime, Logical: logical}
}

func makeSynTS(walltime int64, logical int32) Timestamp {
	return makeTS(walltime, logical).WithSynthetic(true)
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
	b = makeSynTS(1, 1)
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
	b = makeSynTS(1, 1)
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
	b = makeSynTS(1, 1)
	if !a.LessEq(b) || !b.LessEq(a) {
		t.Errorf("expected %+v == %+v", a, b)
	}
}

func TestIsEmpty(t *testing.T) {
	a := makeTS(0, 0)
	assert.True(t, a.IsEmpty())
	a = makeTS(1, 0)
	assert.False(t, a.IsEmpty())
	a = makeTS(0, 1)
	assert.False(t, a.IsEmpty())
	a = makeSynTS(0, 0)
	assert.False(t, a.IsEmpty())
}

func TestTimestampNext(t *testing.T) {
	testCases := []struct {
		ts, expNext Timestamp
	}{
		{makeTS(1, 2), makeTS(1, 3)},
		{makeTS(1, math.MaxInt32-1), makeTS(1, math.MaxInt32)},
		{makeTS(1, math.MaxInt32), makeTS(2, 0)},
		{makeTS(math.MaxInt32, math.MaxInt32), makeTS(math.MaxInt32+1, 0)},
		{makeSynTS(1, 2), makeSynTS(1, 3)},
		{makeSynTS(1, math.MaxInt32-1), makeSynTS(1, math.MaxInt32)},
		{makeSynTS(1, math.MaxInt32), makeSynTS(2, 0)},
		{makeSynTS(math.MaxInt32, math.MaxInt32), makeSynTS(math.MaxInt32+1, 0)},
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
		{makeSynTS(1, 2), makeSynTS(1, 1)},
		{makeSynTS(1, 1), makeSynTS(1, 0)},
		{makeSynTS(1, 0), makeSynTS(0, math.MaxInt32)},
	}
	for _, c := range testCases {
		assert.Equal(t, c.expPrev, c.ts.Prev())
	}
}

func TestTimestampFloorPrevWallPrev(t *testing.T) {
	testCases := []struct {
		ts, expPrev, expWallPrev Timestamp
	}{
		{makeTS(2, 0), makeTS(1, 0), makeTS(1, 0)},
		{makeTS(1, 2), makeTS(1, 1), makeTS(0, 0)},
		{makeTS(1, 1), makeTS(1, 0), makeTS(0, 0)},
		{makeTS(1, 0), makeTS(0, 0), makeTS(0, 0)},
		{makeSynTS(2, 0), makeSynTS(1, 0), makeSynTS(1, 0)},
		{makeSynTS(1, 2), makeSynTS(1, 1), makeSynTS(0, 0)},
		{makeSynTS(1, 1), makeSynTS(1, 0), makeSynTS(0, 0)},
		{makeSynTS(1, 0), makeSynTS(0, 0), makeSynTS(0, 0)},
	}
	for _, c := range testCases {
		assert.Equal(t, c.expPrev, c.ts.FloorPrev())
		assert.Equal(t, c.expWallPrev, c.ts.WallPrev())
	}
}

func TestTimestampForward(t *testing.T) {
	testCases := []struct {
		ts, arg   Timestamp
		expFwd    Timestamp
		expFwdRes bool
	}{
		{makeTS(2, 0), makeTS(1, 0), makeTS(2, 0), false},
		{makeTS(2, 0), makeTS(1, 1), makeTS(2, 0), false},
		{makeTS(2, 0), makeTS(2, 0), makeTS(2, 0), false},
		{makeTS(2, 0), makeTS(2, 1), makeTS(2, 1), true},
		{makeTS(2, 0), makeTS(3, 0), makeTS(3, 0), true},
		{makeSynTS(2, 0), makeTS(1, 0), makeSynTS(2, 0), false},
		{makeSynTS(2, 0), makeTS(1, 1), makeSynTS(2, 0), false},
		{makeSynTS(2, 0), makeTS(2, 0), makeTS(2, 0), false},
		{makeSynTS(2, 0), makeTS(2, 1), makeTS(2, 1), true},
		{makeSynTS(2, 0), makeTS(3, 0), makeTS(3, 0), true},
		{makeTS(2, 0), makeSynTS(1, 0), makeTS(2, 0), false},
		{makeTS(2, 0), makeSynTS(1, 1), makeTS(2, 0), false},
		{makeTS(2, 0), makeSynTS(2, 0), makeTS(2, 0), false},
		{makeTS(2, 0), makeSynTS(2, 1), makeSynTS(2, 1), true},
		{makeTS(2, 0), makeSynTS(3, 0), makeSynTS(3, 0), true},
		{makeSynTS(2, 0), makeSynTS(1, 0), makeSynTS(2, 0), false},
		{makeSynTS(2, 0), makeSynTS(1, 1), makeSynTS(2, 0), false},
		{makeSynTS(2, 0), makeSynTS(2, 0), makeSynTS(2, 0), false},
		{makeSynTS(2, 0), makeSynTS(2, 1), makeSynTS(2, 1), true},
		{makeSynTS(2, 0), makeSynTS(3, 0), makeSynTS(3, 0), true},
	}
	for _, c := range testCases {
		ts := c.ts
		assert.Equal(t, c.expFwdRes, ts.Forward(c.arg))
		assert.Equal(t, c.expFwd, ts)
	}
}

func TestTimestampBackward(t *testing.T) {
	testCases := []struct {
		ts, arg, expBwd Timestamp
	}{
		{makeTS(2, 0), makeTS(1, 0), makeTS(1, 0)},
		{makeTS(2, 0), makeTS(1, 1), makeTS(1, 1)},
		{makeTS(2, 0), makeTS(2, 0), makeTS(2, 0)},
		{makeTS(2, 0), makeTS(2, 1), makeTS(2, 0)},
		{makeTS(2, 0), makeTS(3, 0), makeTS(2, 0)},
		{makeSynTS(2, 0), makeTS(1, 0), makeTS(1, 0)},
		{makeSynTS(2, 0), makeTS(1, 1), makeTS(1, 1)},
		{makeSynTS(2, 0), makeTS(2, 0), makeTS(2, 0)},
		{makeSynTS(2, 0), makeTS(2, 1), makeTS(2, 0)},
		{makeSynTS(2, 0), makeTS(3, 0), makeTS(2, 0)},
		{makeTS(2, 0), makeSynTS(1, 0), makeTS(1, 0)},
		{makeTS(2, 0), makeSynTS(1, 1), makeTS(1, 1)},
		{makeTS(2, 0), makeSynTS(2, 0), makeTS(2, 0)},
		{makeTS(2, 0), makeSynTS(2, 1), makeTS(2, 0)},
		{makeTS(2, 0), makeSynTS(3, 0), makeTS(2, 0)},
		{makeSynTS(2, 0), makeSynTS(1, 0), makeSynTS(1, 0)},
		{makeSynTS(2, 0), makeSynTS(1, 1), makeSynTS(1, 1)},
		{makeSynTS(2, 0), makeSynTS(2, 0), makeSynTS(2, 0)},
		{makeSynTS(2, 0), makeSynTS(2, 1), makeSynTS(2, 0)},
		{makeSynTS(2, 0), makeSynTS(3, 0), makeSynTS(2, 0)},
	}
	for _, c := range testCases {
		ts := c.ts
		ts.Backward(c.arg)
		assert.Equal(t, c.expBwd, ts)
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
		{makeSynTS(145, 0), "145.0000000000?"},
		{makeSynTS(145, 123), "145.0000000123?"},
		{makeSynTS(145, 1123456789), "145.1123456789?"},
	}
	for _, c := range testCases {
		assert.Equal(t, c.exp, c.ts.AsOfSystemTime())
	}
}

// TestTimestampFormatParseRoundTrip tests the majority of timestamps that
// round-trip through formatting then parsing.
func TestTimestampFormatParseRoundTrip(t *testing.T) {
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
		{makeSynTS(0, 123), "0,123?"},
		{makeSynTS(1, 0), "0.000000001,0?"},
		{makeSynTS(1, 123), "0.000000001,123?"},
	}
	for _, c := range testCases {
		str := c.ts.String()
		assert.Equal(t, c.exp, str)

		parsed, err := ParseTimestamp(str)
		assert.NoError(t, err)
		assert.Equal(t, c.ts, parsed)
	}
}

// TestTimestampParseFormatNonRoundTrip tests the minority of timestamps that do
// not round-trip through parsing then formatting.
func TestTimestampParseFormatNonRoundTrip(t *testing.T) {
	testCases := []struct {
		s      string
		exp    Timestamp
		expStr string
	}{
		// Logical portion can be omitted.
		{"0", makeTS(0, 0), "0,0"},
		// Fractional portion can be omitted.
		{"99,0", makeTS(99000000000, 0), "99.000000000,0"},
		// Fractional and logical portion can be omitted.
		{"99", makeTS(99000000000, 0), "99.000000000,0"},
		// Negatives can be omitted for portions that are 0.
		{"-0", makeTS(0, 0), "0,0"},
		{"-0.0", makeTS(0, 0), "0,0"},
		{"-0.0,-0", makeTS(0, 0), "0,0"},
		{"1,-0", makeTS(1000000000, 0), "1.000000000,0"},
		{"1.1,-0", makeTS(1000000001, 0), "1.000000001,0"},
		{"-0,1", makeTS(0, 1), "0,1"},
		// Other cases.
		{"0.000000001", makeTS(1, 0), "0.000000001,0"},
		{"99.000000001", makeTS(99000000001, 0), "99.000000001,0"},
		{"0?", makeSynTS(0, 0), "0,0?"},
		{"99?", makeSynTS(99000000000, 0), "99.000000000,0?"},
		{"0.000000001?", makeSynTS(1, 0), "0.000000001,0?"},
		{"99.000000001?", makeSynTS(99000000001, 0), "99.000000001,0?"},
	}
	for _, c := range testCases {
		parsed, err := ParseTimestamp(c.s)
		assert.NoError(t, err)
		assert.Equal(t, c.exp, parsed)

		str := parsed.String()
		assert.Equal(t, c.expStr, str)
	}
}

func TestTimestampParseError(t *testing.T) {
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
			"0,123[?]",
			"failed to parse \"0,123\\[\\?\\]\" as Timestamp",
		},
		{
			"0,123??",
			"failed to parse \"0,123\\?\\?\" as Timestamp",
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

func BenchmarkTimestampStringSynthetic(b *testing.B) {
	ts := makeSynTS(-6661234567890, 0)

	for i := 0; i < b.N; i++ {
		_ = ts.String()
	}
}

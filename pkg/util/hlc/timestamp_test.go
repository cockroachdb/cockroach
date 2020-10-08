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
}

func TestTimestampNext(t *testing.T) {
	testCases := []struct {
		ts, expNext Timestamp
	}{
		{makeTS(1, 2), makeTS(1, 3)},
		{makeTS(1, math.MaxInt32-1), makeTS(1, math.MaxInt32)},
		{makeTS(1, math.MaxInt32), makeTS(2, 0)},
		{makeTS(math.MaxInt32, math.MaxInt32), makeTS(math.MaxInt32+1, 0)},
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

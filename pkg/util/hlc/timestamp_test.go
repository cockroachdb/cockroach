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

func TestTimestampNext(t *testing.T) {
	testCases := []struct {
		ts, expNext Timestamp
	}{
		{makeTS(1, 2), makeTS(1, 3)},
		{makeTS(1, math.MaxInt32-1), makeTS(1, math.MaxInt32)},
		{makeTS(1, math.MaxInt32), makeTS(2, 0)},
		{makeTS(math.MaxInt32, math.MaxInt32), makeTS(math.MaxInt32+1, 0)},
	}
	for i, c := range testCases {
		if next := c.ts.Next(); next != c.expNext {
			t.Errorf("%d: expected %s; got %s", i, c.expNext, next)
		}
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
	for i, c := range testCases {
		if prev := c.ts.Prev(); prev != c.expPrev {
			t.Errorf("%d: expected %s; got %s", i, c.expPrev, prev)
		}
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
	for i, c := range testCases {
		if prev := c.ts.FloorPrev(); prev != c.expPrev {
			t.Errorf("%d: expected %s; got %s", i, c.expPrev, prev)
		}
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
	for i, c := range testCases {
		if exp := c.ts.AsOfSystemTime(); exp != c.exp {
			t.Errorf("%d: expected %s; got %s", i, c.exp, exp)
		}
	}
}

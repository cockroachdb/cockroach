// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package qos

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLevelBuckets(t *testing.T) {
	for _, tc := range []struct {
		l       Level
		encoded uint32
		str     string
		valid   bool
	}{
		{
			Level{ClassHigh, 127},
			0x0000FFFF,
			"h:127",
			true,
		},
		{
			Level{ClassHigh, 126},
			0x0000FFFD,
			"h:126",
			true,
		},
		{
			Level{ClassHigh, 1},
			0x0000FF03,
			"h:1",
			true,
		},
		{
			Level{ClassHigh, 0},
			0x0000FF01,
			"h:0",
			true,
		},
		{
			Level{ClassDefault, 127},
			0x000080FF,
			"d:127",
			true,
		},
		{
			Level{ClassLow, 125},
			0x000001FB,
			"l:125",
			true,
		},
		{
			Level{ClassHigh, 128},
			0x0000FFFF,
			"h:128",
			false,
		},
		{
			Level{17, 2},
			0x0000FF05,
			"17:2",
			false,
		},
	} {
		t.Run(fmt.Sprintf("%s->%x", tc.l, tc.encoded), func(t *testing.T) {
			assert.Equal(t, tc.valid, tc.l.IsValid())
			assert.Equal(t, tc.str, tc.l.String())
			l := tc.l
			if !tc.l.IsValid() {
				l = clampToValid(l)
			}
			assert.Equal(t, tc.encoded, l.Encode())
			assert.Equal(t, l, Decode(tc.encoded))
		})
	}
}

func clampToValid(l Level) Level {
	if l.Class >= NumClasses {
		l.Class = NumClasses - 1
	}
	if l.Shard >= NumShards {
		l.Shard = NumShards - 1
	}
	return l
}

func TestIncDec(t *testing.T) {
	levels := make([]Level, 0, NumClasses*NumShards)
	for c := Class(0); c < NumClasses; c++ {
		for s := Shard(0); s < NumShards; s++ {
			levels = append(levels, Level{Class: c, Shard: s})
		}
	}
	randIncDecTest := func() {
		start := rand.Intn(len(levels))
		var incs, decs int
		l := levels[start]
		const N = 500
		for i := 0; i < N; i++ {
			var next Level
			if inc := rand.Intn(2) == 1; inc {
				if next = l.Inc(); next != l {
					incs++
				}
			} else {
				if next = l.Dec(); next != l {
					decs++
				}
			}
			l = next
			end := start + incs - decs
			assert.Equal(t, levels[end], l)
		}
	}
	const trials = 100
	for i := 0; i < trials; i++ {
		randIncDecTest()
	}
}

// TestOrdering ensures that Level provides a total ordering both using the
// Less method as well as comparing values creates with Encode.
func TestOrdering(t *testing.T) {
	// Each test case is provided as a slice of Levels in sorted order.
	// The test ensure that comparisons are valid both using the Less method
	// as well as using the numerical comparison of encoded values.
	cases := [][]Level{
		{
			{ClassDefault, 127},
			{ClassHigh, 2},
			{ClassHigh, 126},
			{ClassHigh, 127},
		},
		{
			{ClassLow, 0},
			{ClassLow, 127},
			{ClassDefault, 127},
			{ClassHigh, 127},
		},
	}
	// shuffled returns a shuffled copy of l.
	shuffled := func(l []Level) []Level {
		l = append([]Level{}, l...)
		rand.Shuffle(len(l), func(i, j int) {
			l[i], l[j] = l[j], l[i]
		})
		return l
	}
	lessLess := func(a, b Level) bool {
		return a.Less(b)
	}
	lessEncoded := func(a, b Level) bool {
		return a.Encode() < b.Encode()
	}
	test := func(sorted []Level, less func(a, b Level) bool) func(t *testing.T) {
		return func(t *testing.T) {
			l := shuffled(sorted)
			sort.Slice(l, func(i, j int) bool {
				return less(l[i], l[j])
			})
			assert.EqualValues(t, sorted, l)
		}
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%v/Less", tc), test(tc, lessLess))
		t.Run(fmt.Sprintf("%v/Encoded", tc), test(tc, lessEncoded))
	}
}

// TestDecode tests that various encoded values decode as expected.
func TestDecode(t *testing.T) {
	for _, tc := range []struct {
		encoded uint32
		level   Level
	}{
		{0xFF00, Level{ClassHigh, 0}},
		{0xFF01, Level{ClassHigh, 0}},
		{0xFF02, Level{ClassHigh, 1}},
		{0x1234FF02, Level{ClassHigh, 1}},
		{0xF902, Level{ClassHigh, 1}},
		{0xF902, Level{ClassHigh, 1}},
		{0x4202, Level{ClassDefault, 1}},
		{0x8102, Level{ClassDefault, 1}},
		{0xC002, Level{ClassHigh, 1}},
		{0xFFFFFF02, Level{ClassHigh, 1}},
		{0xFFFFFFFE, Level{ClassHigh, 127}},
		{0xFFFFFFFD, Level{ClassHigh, 126}},
		{0xFFFD, Level{ClassHigh, 126}},
	} {
		assert.Equal(t, tc.level, Decode(tc.encoded))
	}
}

// TestInvalidEncodePanics ensures that encoding an invalid Level leads to a
// panic.
func TestInvalidEncodePanics(t *testing.T) {
	for _, tc := range []Level{
		{NumClasses, 0},
		{NumClasses + 1, 0},
		{math.MaxUint8, math.MaxUint8},
		{0, NumShards},
		{0, NumShards + 1},
		{0, math.MaxUint8},
	} {
		assert.Panics(t, func() {
			tc.Encode()
		}, "(%d, %d)", tc.Class, tc.Shard)
	}
}

func TestUnmarshalText(t *testing.T) {
	for _, tc := range []struct {
		in  []byte
		out Level
		err string
	}{
		{
			in:  []byte("ff05"),
			out: Level{ClassHigh, 2},
		},
		{
			in:  []byte(""),
			err: "invalid data length 0, expected 4",
		},
		{
			in:  []byte("000z"),
			err: "encoding/hex: invalid byte:",
		},
		{
			in:  []byte("0000"),
			out: Level{ClassLow, 0},
		},
	} {
		t.Run(fmt.Sprintf("%v", tc), func(t *testing.T) {
			var l Level
			err := l.UnmarshalText(tc.in)
			if tc.err != "" {
				if assert.Error(t, err) {
					assert.Regexp(t, tc.err, err.Error())
				}
			} else {
				assert.Equal(t, tc.out, l)
			}
		})
	}
}

func TestMarshalText(t *testing.T) {
	for _, tc := range []struct {
		l   Level
		out []byte
		err string
	}{
		{
			l:   Level{ClassHigh, 2},
			out: []byte("ff05"),
		},
		{
			l:   Level{4, 2},
			err: "cannot marshal invalid Level",
		},
		{
			l:   Level{ClassLow, math.MaxUint8},
			err: "cannot marshal invalid Level",
		},
	} {
		t.Run(fmt.Sprintf("%v", tc), func(t *testing.T) {
			out, err := tc.l.MarshalText()
			if tc.err != "" {
				if assert.Error(t, err) {
					assert.Regexp(t, tc.err, err.Error())
				}
			} else {
				assert.Equal(t, tc.out, out)
			}
		})
	}
}

func TestContext(t *testing.T) {
	ctx := context.Background()
	got, ok := LevelFromContext(ctx)
	assert.Zero(t, got)
	assert.False(t, ok)
	l := Level{ClassHigh, 12}
	ctx = ContextWithLevel(ctx, l)
	got, ok = LevelFromContext(ctx)
	assert.Equal(t, l, got)
	assert.True(t, ok)
}

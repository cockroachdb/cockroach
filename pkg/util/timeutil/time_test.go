// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package timeutil

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestUnixMicros(t *testing.T) {
	testCases := []struct {
		us      int64
		utcTime time.Time
	}{
		{-1, time.Date(1969, 12, 31, 23, 59, 59, 999999000, time.UTC)},
		{0, time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)},
		{1, time.Date(1970, 1, 1, 0, 0, 0, 1000, time.UTC)},
		{4242424242424242, time.Date(2104, 6, 9, 3, 10, 42, 424242000, time.UTC)},
		{math.MaxInt64, time.Date(294247, 1, 10, 4, 0, 54, 775807000, time.UTC)},
		{-62135596800000000, time.Time{}},
	}
	for i, testCase := range testCases {
		if e, a := testCase.utcTime, FromUnixMicros(testCase.us).UTC(); e != a {
			t.Errorf("%d:FromUnixMicro: expected %v, but got %v", i, e, a)
		}

		if e, a := testCase.us, ToUnixMicros(testCase.utcTime); e != a {
			t.Errorf("%d:ToUnixMicro: expected %v, but got %v", i, e, a)
		}
	}

	for i := 0; i < 32; i++ {
		us := rand.Int63()
		if e, a := us, ToUnixMicros(FromUnixMicros(us)); e != a {
			t.Errorf("%d did not roundtrip; got back %d", e, a)
		}
	}
}

func TestUnixMicrosRounding(t *testing.T) {
	testCases := []struct {
		us      int64
		utcTime time.Time
	}{
		{0, time.Date(1970, 1, 1, 0, 0, 0, 1, time.UTC)},
		{0, time.Date(1970, 1, 1, 0, 0, 0, 499, time.UTC)},
		{1, time.Date(1970, 1, 1, 0, 0, 0, 500, time.UTC)},
		{1, time.Date(1970, 1, 1, 0, 0, 0, 999, time.UTC)},
	}
	for i, testCase := range testCases {
		if e, a := testCase.us, ToUnixMicros(testCase.utcTime); e != a {
			t.Errorf("%d:ToUnixMicro: expected %v, but got %v", i, e, a)
		}
	}
}

func TestReplaceLibPQTimePrefix(t *testing.T) {
	assert.Equal(t, "1970-02-02 11:00", ReplaceLibPQTimePrefix("1970-02-02 11:00"))
	assert.Equal(t, "1970-01-01 11:00", ReplaceLibPQTimePrefix("0000-01-01 11:00"))
}

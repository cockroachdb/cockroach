// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package timeutil

import (
	"math"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// Test that consecutive timeutil.Now() calls don't return the same instant. We
// have tests that rely on all durations being measurable.
func TestTimeIncreasing(t *testing.T) {
	a := Now()
	b := Now()
	require.NotEqual(t, a, b)
	require.NotZero(t, b.Sub(a))
}

func init() {
	// We run our tests with the env var TZ="", which means UTC, in order to make
	// the timezone predictable. This is not how we run in production, though. For
	// this package, let's brutally pretend we're somewhere else by changing the
	// locale, in order to make the testing more general. In particular, without
	// this, TestTimeIsUTC(), which tests that the timeutil library converts
	// timestamps to UTC, would pass regardless of our code because of the testing
	// environment. We need to do this in a package init because otherwise the
	// update races with the runtime reading it.
	loc, err := LoadLocation("Africa/Cairo")
	if err != nil {
		panic(err)
	}
	time.Local = loc
}

// Test that Now() returns times in UTC. We've made a decision that this is a
// good policy across the cluster so that all the timestamps print uniformly
// across different nodes, and also because we were afraid that timestamps leak
// into SQL Datums, and there the timestamp matters.
func TestTimeIsUTC(t *testing.T) {
	require.Equal(t, time.UTC, Now().Location())
	require.Equal(t, time.UTC, Unix(1, 1).Location())
}

// Test that the unsafe cast we do in timeutil.Now() to set the time to UTC is
// sane: check that our hardcoded struct layout corresponds to stdlib.
func TestTimeLayout(t *testing.T) {
	stdlib := reflect.TypeOf((*time.Time)(nil)).Elem()
	ours := reflect.TypeOf((*timeLayout)(nil)).Elem()
	require.Equal(t, stdlib.NumField(), ours.NumField())
	for i := 0; i < stdlib.NumField(); i++ {
		stdlibF, ourF := stdlib.Field(i), ours.Field(i)
		require.Equal(t, stdlibF.Type, ourF.Type)
		require.Equal(t, stdlibF.Offset, ourF.Offset)
	}
}

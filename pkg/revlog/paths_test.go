// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlog

import (
	"math"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestFormatParseTickEndRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		name string
		time time.Time
	}{
		{name: "normal", time: time.Date(2026, 4, 20, 15, 30, 10, 0, time.UTC)},
		{name: "midnight", time: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
		{name: "year boundary", time: time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC)},
		{name: "leap day", time: time.Date(2028, 2, 29, 12, 0, 0, 0, time.UTC)},
		{name: "end of day", time: time.Date(2026, 6, 15, 23, 59, 50, 0, time.UTC)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ts := hlc.Timestamp{WallTime: tc.time.UnixNano()}
			formatted := FormatTickEnd(ts)
			parsed, err := ParseTickEnd(formatted)
			require.NoError(t, err)
			require.Equal(t, ts, parsed)
		})
	}
}

func TestFormatParseHLCNameRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		name string
		ts   hlc.Timestamp
	}{
		{name: "zero", ts: hlc.Timestamp{}},
		{name: "normal", ts: hlc.Timestamp{WallTime: 1714646400000000000, Logical: 5}},
		{name: "max wall", ts: hlc.Timestamp{WallTime: math.MaxInt64}},
		{name: "max logical", ts: hlc.Timestamp{Logical: math.MaxInt32}},
		{name: "max both", ts: hlc.Timestamp{WallTime: math.MaxInt64, Logical: math.MaxInt32}},
		{name: "logical only", ts: hlc.Timestamp{Logical: 42}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			formatted := FormatHLCName(tc.ts)
			parsed, err := ParseHLCName(formatted)
			require.NoError(t, err)
			require.Equal(t, tc.ts, parsed)
		})
	}
}

func TestFormatHLCNameLexSort(t *testing.T) {
	defer leaktest.AfterTest(t)()
	timestamps := []hlc.Timestamp{
		{WallTime: 0, Logical: 0},
		{WallTime: 0, Logical: 1},
		{WallTime: 1, Logical: 0},
		{WallTime: 100, Logical: 0},
		{WallTime: 100, Logical: 5},
		{WallTime: 1714646400000000000, Logical: 0},
		{WallTime: math.MaxInt64, Logical: 0},
		{WallTime: math.MaxInt64, Logical: math.MaxInt32},
	}
	formatted := make([]string, len(timestamps))
	for i, ts := range timestamps {
		formatted[i] = FormatHLCName(ts)
	}
	// Verify the formatted strings are already sorted.
	require.True(t, slices.IsSorted(formatted),
		"formatted HLC names should be in ascending lex order: %v", formatted)

	// Shuffle and re-sort to double-check.
	shuffled := slices.Clone(formatted)
	shuffled[0], shuffled[len(shuffled)-1] = shuffled[len(shuffled)-1], shuffled[0]
	slices.Sort(shuffled)
	require.Equal(t, formatted, shuffled)
}

func TestPathConstruction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	te := hlc.Timestamp{
		WallTime: time.Date(2026, 4, 20, 15, 30, 10, 0, time.UTC).UnixNano(),
	}
	require.Equal(t, "log/resolved/2026-04-20/15-30.10.pb", MarkerPath(te))
	require.Equal(t, "log/data/2026-04-20/15-30.10/", DataDirPath(te))
	require.Equal(t, "log/data/2026-04-20/15-30.10/42.sst", DataFilePath(te, 42))

	hlcTs := hlc.Timestamp{WallTime: 1000000000, Logical: 7}
	require.Equal(t, "log/coverage/0000000001000000000_0000000007", CoveragePath(hlcTs))
	require.Equal(t, "log/schema/descs/0000000001000000000_0000000007/99.pb", SchemaDescPath(hlcTs, 99))
	require.Equal(t, "log/schema/descs/0000000001000000000_0000000007/", SchemaDescDirPath(hlcTs))
}

func TestParseTickEndInvalid(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, input := range []string{
		"",
		"not-a-date",
		"2026-04-20",
		"15-30.10",
		"2026/04/20/15-30.10",
	} {
		_, err := ParseTickEnd(input)
		require.Errorf(t, err, "input: %q", input)
	}
}

func TestParseHLCNameInvalid(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, input := range []string{
		"",
		"not-an-hlc",
		"123_456",
		"0000000000000000001",
		"abc_0000000001",
	} {
		_, err := ParseHLCName(input)
		require.Errorf(t, err, "input: %q", input)
	}
}

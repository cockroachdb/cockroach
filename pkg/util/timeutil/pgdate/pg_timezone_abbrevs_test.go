// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgdate_test

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/stretchr/testify/require"
)

func TestLookupPGTimezoneAbbrev(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectedOK     bool
		expectedOffset int32
		expectedIsDST  bool
	}{
		{name: "EST upper", input: "EST", expectedOK: true, expectedOffset: -18000},
		{name: "EST lower", input: "est", expectedOK: true, expectedOffset: -18000},
		{name: "EST mixed", input: "Est", expectedOK: true, expectedOffset: -18000},
		{name: "PST", input: "PST", expectedOK: true, expectedOffset: -28800},
		{name: "EDT is DST", input: "EDT", expectedOK: true, expectedOffset: -14400, expectedIsDST: true},
		{name: "PDT is DST", input: "PDT", expectedOK: true, expectedOffset: -25200, expectedIsDST: true},
		{name: "EAT non-IANA", input: "EAT", expectedOK: true, expectedOffset: 10800},
		{name: "UTC zero", input: "UTC", expectedOK: true},
		{name: "unknown", input: "XYZ", expectedOK: false},
		{name: "empty", input: "", expectedOK: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := pgdate.LookupPGTimezoneAbbrev(tc.input)
			require.Equal(t, tc.expectedOK, ok)
			if !ok {
				return
			}
			require.Equal(t, tc.expectedOffset, got.UTCOffsetSecs)
			require.Equal(t, tc.expectedIsDST, got.IsDST)
		})
	}
}

func TestPGTimezoneAbbrevsTable(t *testing.T) {
	abbrevs := pgdate.PGTimezoneAbbrevs()
	// PostgreSQL 18.3's tznames/Default contains roughly 145 fixed-offset
	// abbreviations. Guard against accidental truncation while leaving room
	// for future tzdata updates.
	require.GreaterOrEqual(t, len(abbrevs), 100)
}

// CET is also an IANA zone name. In conversions it must use the PostgreSQL
// abbreviation's fixed offset, while full IANA names retain their DST rules.
func TestTimeZoneStringToLocation(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		standard timeutil.TimeZoneStringToLocationStandard
		offsets  [2]int
	}{
		{"CET", "CET", timeutil.TimeZoneStringToLocationPOSIXStandard, [2]int{3600, 3600}},
		{"lowercase", "cet", timeutil.TimeZoneStringToLocationPOSIXStandard, [2]int{3600, 3600}},
		{"CEST", "CEST", timeutil.TimeZoneStringToLocationPOSIXStandard, [2]int{7200, 7200}},
		{"non-IANA abbreviation", "EAT", timeutil.TimeZoneStringToLocationPOSIXStandard, [2]int{10800, 10800}},
		{"IANA region", "Europe/Paris", timeutil.TimeZoneStringToLocationPOSIXStandard, [2]int{3600, 7200}},
		{"UTC-prefixed offset", "UTC+1", timeutil.TimeZoneStringToLocationISO8601Standard, [2]int{-3600, -3600}},
		{"POSIX offset", "+1", timeutil.TimeZoneStringToLocationPOSIXStandard, [2]int{-3600, -3600}},
		{"ISO offset", "+1", timeutil.TimeZoneStringToLocationISO8601Standard, [2]int{3600, 3600}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			loc, err := pgdate.TimeZoneStringToLocation(tc.input, tc.standard)
			require.NoError(t, err)
			for i, month := range []time.Month{time.January, time.July} {
				_, offset := time.Date(2022, month, 1, 0, 0, 0, 0, time.UTC).In(loc).Zone()
				require.Equal(t, tc.offsets[i], offset, "month %s", month)
			}
		})
	}
	_, err := pgdate.TimeZoneStringToLocation("not_a_timezone", timeutil.TimeZoneStringToLocationPOSIXStandard)
	require.Error(t, err)
}

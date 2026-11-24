// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgdate_test

import (
	"testing"

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

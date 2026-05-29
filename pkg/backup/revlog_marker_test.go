// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestFormatJobMarkerName(t *testing.T) {
	defer leaktest.AfterTest(t)()

	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	name := formatJobMarkerName(now, jobspb.JobID(42))
	require.Contains(t, name, "_42.pb")
	require.True(t, len(name) > 23, "name should be at least 19 digit nanos + _ + id + .pb")

	// Verify lex ordering: earlier time < later time.
	earlier := formatJobMarkerName(now.Add(-time.Hour), jobspb.JobID(100))
	later := formatJobMarkerName(now.Add(time.Hour), jobspb.JobID(1))
	require.Less(t, earlier, later,
		"earlier timestamps should sort before later ones")
}

func TestParseJobMarkerJobID(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name       string
		input      string
		expectedID jobspb.JobID
		expectedOK bool
	}{
		{
			name:       "valid marker",
			input:      "0000001715342400000000000_42.pb",
			expectedID: 42,
			expectedOK: true,
		},
		{
			name:       "no underscore",
			input:      "nounderscorehere.pb",
			expectedID: 0,
			expectedOK: false,
		},
		{
			name:       "trailing underscore",
			input:      "123_.pb",
			expectedID: 0,
			expectedOK: false,
		},
		{
			name:       "non-numeric job ID",
			input:      "123_abc.pb",
			expectedID: 0,
			expectedOK: false,
		},
		{
			name:       "empty string",
			input:      "",
			expectedID: 0,
			expectedOK: false,
		},
		{
			name:       "no .pb suffix",
			input:      "123_42",
			expectedID: 42,
			expectedOK: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id, ok := parseJobMarkerJobID(tc.input)
			require.Equal(t, tc.expectedOK, ok)
			if ok {
				require.Equal(t, tc.expectedID, id)
			}
		})
	}
}

func TestFormatParseJobMarkerRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()

	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	ids := []jobspb.JobID{1, 42, 1000, jobspb.JobID(1<<31 - 1)}

	for _, id := range ids {
		name := formatJobMarkerName(now, id)
		parsedID, ok := parseJobMarkerJobID(name)
		require.True(t, ok, "should parse marker name %q", name)
		require.Equal(t, id, parsedID, "round-trip for jobID %d", id)
	}
}

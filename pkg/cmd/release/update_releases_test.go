// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/release"
	"github.com/stretchr/testify/require"
)

func Test_processReleaseData(t *testing.T) {
	testReleases, err := os.ReadFile("testdata/release_data.yaml")
	require.NoError(t, err)

	data, err := parseReleases(testReleases)
	require.NoError(t, err)

	expectedReleaseData := map[string]release.Series{
		"21.2": {
			Latest: "21.2.1",
		},
		"22.1": {
			Latest:      "22.1.12",
			Predecessor: "21.2",
			Withdrawn:   []string{"22.1.12"},
		},
		"22.2": {
			Latest:      "22.2.0",
			Predecessor: "22.1",
		},
		"23.1": {
			Latest:      "23.1.1",
			Predecessor: "22.2",
			Withdrawn:   []string{"23.1.0"},
		},
	}
	require.Equal(t, expectedReleaseData, processReleaseData(data))
}

func Test_parseReleases(t *testing.T) {
	for _, tc := range []struct {
		name string
		blob string
		err  string
	}{
		{
			name: "unknown schema version",
			blob: "schema_version: 2\nreleases: []\n",
			err:  "schema_version 2, want 1",
		},
		{
			name: "missing schema version",
			blob: "releases: []\n",
			err:  "schema_version 0, want 1",
		},
		{
			// The docs releases.yml is a bare list of releases.
			name: "docs releases.yml format",
			blob: "- release_name: v26.3.1\n  major_version: v26.3\n",
			err:  "failed to YAML parse release data",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseReleases([]byte(tc.blob))
			require.ErrorContains(t, err, tc.err)
		})
	}
}

func Test_validateReleaseData(t *testing.T) {
	testCases := []struct {
		name        string
		data        map[string]release.Series
		expectedErr string
	}{
		{
			name: "multiple series without a predecessor",
			data: map[string]release.Series{
				"22.2": {Latest: "22.2.1"},
				"23.1": {Latest: "23.1.12"},
			},
			expectedErr: "two release series without known predecessors",
		},
		{
			name: "missing predecessor information",
			data: map[string]release.Series{
				"22.2": {Latest: "22.2.22"},
				"23.1": {Latest: "23.1.12", Predecessor: "19.2"},
			},
			expectedErr: `predecessor of "23.1" is "19.2", but there is no release information for it`,
		},
		{
			name: "missing latest release",
			data: map[string]release.Series{
				"22.2": {Latest: "22.2.22"},
				"23.1": {Predecessor: "22.2"},
			},
			expectedErr: `release information for series "23.1" is missing the latest release`,
		},
		{
			name: "invalid latest version",
			data: map[string]release.Series{
				"2.1": {Latest: "beta-20160829"},
			},
			expectedErr: `release information for series "2.1" has invalid latest release "beta-20160829"`,
		},
		{
			name: "invalid withdrawn release",
			data: map[string]release.Series{
				"22.2": {Latest: "22.2.22"},
				"23.1": {Latest: "23.1.12", Predecessor: "22.2", Withdrawn: []string{"beta-20160829"}},
			},
			expectedErr: `release information for series "23.1" has invalid withdrawn release "beta-20160829"`,
		},
		{
			name: "all releases withdrawn",
			data: map[string]release.Series{
				"22.2": {Latest: "22.2.22"},
				"23.1": {Latest: "23.1.1", Predecessor: "22.2", Withdrawn: []string{"23.1.0", "23.1.1"}},
			},
			expectedErr: `series "23.1" is invalid: every release has been withdrawn`,
		},
		{
			name: "valid release data",
			data: map[string]release.Series{
				"22.1": {Latest: "22.1.19"},
				"22.2": {Latest: "22.2.22", Predecessor: "22.1"},
				"23.1": {Latest: "23.1.1", Predecessor: "22.2", Withdrawn: []string{"23.1.0"}},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateReleaseData(tc.data)
			if tc.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.Contains(t, err.Error(), tc.expectedErr)
			}
		})
	}
}

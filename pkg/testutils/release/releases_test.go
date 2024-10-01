// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package release

import (
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/stretchr/testify/require"
)

var (
	testReleaseData = map[string]Series{
		"19.2": {
			Latest: "19.2.0",
		},
		"22.1": {
			Latest:      "22.1.13",
			Withdrawn:   []string{"22.1.13"},
			Predecessor: "19.2",
		},
		"22.2": {
			Latest:      "22.2.8",
			Withdrawn:   []string{"22.2.0", "22.2.4"},
			Predecessor: "22.1",
		},
		"23.1": {
			Latest:      "23.1.1",
			Withdrawn:   []string{"23.1.0"},
			Predecessor: "22.2",
		},
		"23.2": {
			Latest:      "23.2.0-beta.1", // test pre-release latest versions
			Predecessor: "23.1",
		},
		"24.1": {
			Predecessor: "23.2",
		},
	}

	// Results rely on this constant seed.
	seed = int64(1)

	// Tests must call `resetRNG` if they use this global rng.
	rng *rand.Rand
)

func resetRNG() {
	rng = rand.New(rand.NewSource(seed))
}

func TestLatestAndRandomPredecessor(t *testing.T) {
	testCases := []struct {
		name           string
		v              string
		expectedErr    string
		expectedLatest string
		expectedRandom string
	}{
		{
			name:        "non-existent release",
			v:           "v50.8.3",
			expectedErr: `no release information for "v50.8.3" ("50.8" series)`,
		},
		{
			name:        "no known predecessor",
			v:           "v19.2.4",
			expectedErr: `no known predecessor for "v19.2.4" ("19.2" series)`,
		},
		{
			name:           "single release",
			v:              "v22.1.8",
			expectedLatest: "19.2.0",
			expectedRandom: "19.2.0",
		},
		{
			name:           "latest is withdrawn",
			v:              "v22.2.3",
			expectedLatest: "22.1.12",
			expectedRandom: "22.1.10",
		},
		{
			name:           "latest is pre-release",
			v:              "v24.1.0",
			expectedLatest: "23.2.0-beta.1",
			expectedRandom: "23.2.0-beta.1",
		},
	}

	oldReleaseData := releaseData
	releaseData = testReleaseData
	defer func() { releaseData = oldReleaseData }()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resetRNG() // deterministic results
			latestPred, latestErr := LatestPredecessor(version.MustParse(tc.v))
			randomPred, randomErr := RandomPredecessor(rng, version.MustParse(tc.v))
			if tc.expectedErr == "" {
				require.NoError(t, latestErr)
				require.NoError(t, randomErr)
			} else {
				require.Contains(t, latestErr.Error(), tc.expectedErr)
				require.Contains(t, randomErr.Error(), tc.expectedErr)
				return
			}

			require.Equal(t, tc.expectedLatest, latestPred)
			require.Equal(t, tc.expectedRandom, randomPred)
		})
	}
}

func TestLatestPredecessorHistory(t *testing.T) {
	testCases := []struct {
		name           string
		v              string
		k              int
		expectedErr    string
		expectedLatest []string
		expectedRandom []string
	}{
		{
			name:        "not enough history",
			v:           "v22.1.8",
			k:           3,
			expectedErr: `no known predecessor for "v19.2.0" ("19.2" series)`,
		},
		{
			name:           "valid history",
			v:              "v23.1.1",
			k:              3,
			expectedLatest: []string{"19.2.0", "22.1.12", "22.2.8"},
			expectedRandom: []string{"19.2.0", "22.1.8", "22.2.8"},
		},
		{
			name:           "with pre-release",
			v:              "v23.1.1-beta.1",
			k:              2,
			expectedLatest: []string{"22.1.12", "22.2.8"},
			expectedRandom: []string{"22.1.8", "22.2.8"},
		},
		{
			name:           "latest is pre-release",
			v:              "v24.1.0",
			k:              2,
			expectedLatest: []string{"23.1.1", "23.2.0-beta.1"},
			expectedRandom: []string{"23.1.1", "23.2.0-beta.1"},
		},
	}

	oldReleaseData := releaseData
	releaseData = testReleaseData
	defer func() { releaseData = oldReleaseData }()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resetRNG() // deterministic results
			latestHistory, latestErr := LatestPredecessorHistory(version.MustParse(tc.v), tc.k)
			randomHistory, randomErr := RandomPredecessorHistory(rng, version.MustParse(tc.v), tc.k)
			if tc.expectedErr == "" {
				require.NoError(t, latestErr)
				require.NoError(t, randomErr)
			} else {
				require.Contains(t, latestErr.Error(), tc.expectedErr)
				require.Contains(t, randomErr.Error(), tc.expectedErr)
				return
			}

			require.Equal(t, tc.expectedLatest, latestHistory)
			require.Equal(t, tc.expectedRandom, randomHistory)
		})
	}
}

func TestMajorReleasesBetween(t *testing.T) {
	oldReleaseData := releaseData
	releaseData = testReleaseData
	defer func() { releaseData = oldReleaseData }()

	testCases := []struct {
		name     string
		v1       string
		v2       string
		expected int
	}{
		{
			name:     "v1 and v2 are on the same release series",
			v1:       "22.2.10",
			v2:       "22.2.14",
			expected: 0,
		},
		{
			name:     "v1 and v2 are one major release apart",
			v1:       "22.2.10",
			v2:       "23.1.0",
			expected: 1,
		},
		{
			name:     "v1 and v2 are two major releases apart",
			v1:       "22.2.10",
			v2:       "23.2.4",
			expected: 2,
		},
		{
			name:     "v1 and v2 are multiple major releases apart",
			v1:       "19.2.3",
			v2:       "24.1.10",
			expected: 5,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			vv1 := version.MustParse("v" + tc.v1)
			vv2 := version.MustParse("v" + tc.v2)

			// We should get the same result regardless of the order of
			// arguments.
			count, err := MajorReleasesBetween(vv1, vv2)
			require.NoError(t, err)
			require.Equal(t, tc.expected, count)

			count, err = MajorReleasesBetween(vv2, vv1)
			require.NoError(t, err)
			require.Equal(t, tc.expected, count)
		})
	}
}

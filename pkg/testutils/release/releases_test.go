// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
			v:           "v23.2.3",
			expectedErr: `no release information for "v23.2.3" ("23.2" series)`,
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

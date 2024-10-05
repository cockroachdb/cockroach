// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mixedversion

import (
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/stretchr/testify/require"
)

func TestClusterVersionAtLeast(t *testing.T) {
	rng := newRand()

	testCases := []struct {
		name           string
		currentVersion string
		minVersion     string
		expectedErr    string
		expected       bool
	}{
		{
			name:           "invalid minVersion",
			currentVersion: "23.1",
			minVersion:     "v23.1",
			expectedErr:    `invalid version v23.1`,
		},
		{
			name:           "cluster version is behind",
			currentVersion: "22.2",
			minVersion:     "23.1",
			expected:       false,
		},
		{
			name:           "cluster version is ahead",
			currentVersion: "23.1",
			minVersion:     "22.2",
			expected:       true,
		},
		{
			name:           "cluster version is equal to minVersion",
			currentVersion: "23.1",
			minVersion:     "23.1",
			expected:       true,
		},
		{
			name:           "cluster version is behind, internal version",
			currentVersion: "22.1-82",
			minVersion:     "23.1",
			expected:       false,
		},
		{
			name:           "cluster version is ahead, internal version",
			currentVersion: "23.1-2",
			minVersion:     "23.1",
			expected:       true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			currentVersion, err := roachpb.ParseVersion(tc.currentVersion)
			require.NoError(t, err)

			var clusterVersions atomic.Value
			clusterVersions.Store([]roachpb.Version{currentVersion})
			runner := testTestRunner()
			runner.systemService.clusterVersions = &clusterVersions

			h := runner.newHelper(ctx, nilLogger, Context{System: &ServiceContext{Finalizing: false}})

			supportedFeature, err := h.ClusterVersionAtLeast(rng, tc.minVersion)
			if tc.expectedErr == "" {
				require.NoError(t, err)
				require.Equal(t, tc.expected, supportedFeature)
			} else {
				require.Error(t, err)
				require.Equal(t, tc.expectedErr, err.Error())
			}
		})
	}
}

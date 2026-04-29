// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spec

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClustersCompatible(t *testing.T) {
	t.Run("spec does not match", func(t *testing.T) {
		s1 := ClusterSpec{NodeCount: 4}
		s2 := ClusterSpec{NodeCount: 5}
		require.False(t, ClustersCompatible(s1, s2, GCE))
	})
	t.Run("spec has different lifetime", func(t *testing.T) {
		s1 := ClusterSpec{NodeCount: 5, Lifetime: 100}
		s2 := ClusterSpec{NodeCount: 5, Lifetime: 200}
		require.True(t, ClustersCompatible(s1, s2, GCE))
	})
	t.Run("spec has different GCE spec with cloud as GCE", func(t *testing.T) {
		s1 := ClusterSpec{NodeCount: 5}
		s2 := ClusterSpec{NodeCount: 5}
		s1.VolumeType = "mock_volume1"
		s2.VolumeType = "mock_volume2"
		require.False(t, ClustersCompatible(s1, s2, GCE))
	})
	t.Run("spec has different GCE spec with cloud as AWS", func(t *testing.T) {
		s1 := ClusterSpec{NodeCount: 5}
		s2 := ClusterSpec{NodeCount: 5}
		s1.VolumeType = "mock_volume1"
		s2.VolumeType = "mock_volume2"
		require.False(t, ClustersCompatible(s1, s2, AWS))
	})
	t.Run("spec has different spec with cloud as AWS", func(t *testing.T) {
		s1 := ClusterSpec{NodeCount: 5}
		s2 := ClusterSpec{NodeCount: 5}
		s1.GCE.MinCPUPlatform = "mock_platform1"
		s2.GCE.MinCPUPlatform = "mock_platform2"
		require.True(t, ClustersCompatible(s1, s2, AWS))
	})
}

func TestMayUseLocalSSD(t *testing.T) {
	tests := []struct {
		name                  string
		spec                  ClusterSpec
		defaultPreferLocalSSD bool
		expected              bool
	}{
		{
			name:                  "default spec with PreferLocalSSD=true",
			defaultPreferLocalSSD: true,
			expected:              true,
		},
		{
			name:                  "default spec with PreferLocalSSD=false",
			defaultPreferLocalSSD: false,
			expected:              false,
		},
		{
			name:                  "explicit non-local-ssd volume type",
			spec:                  ClusterSpec{VolumeType: "gp3"},
			defaultPreferLocalSSD: true,
			expected:              false,
		},
		{
			name:                  "explicit local-ssd volume type",
			spec:                  ClusterSpec{VolumeType: "local-ssd"},
			defaultPreferLocalSSD: false,
			expected:              true,
		},
		{
			name:                  "LocalSSD disabled",
			spec:                  ClusterSpec{LocalSSD: LocalSSDDisable},
			defaultPreferLocalSSD: true,
			expected:              false,
		},
		{
			name:                  "LocalSSD preferred",
			spec:                  ClusterSpec{LocalSSD: LocalSSDPreferOn},
			defaultPreferLocalSSD: false,
			expected:              true,
		},
		{
			name:                  "RandomizeVolumeType enabled",
			spec:                  ClusterSpec{RandomizeVolumeType: true},
			defaultPreferLocalSSD: false,
			expected:              true,
		},
		{
			name:                  "VolumeSize set overrides PreferLocalSSD",
			spec:                  ClusterSpec{VolumeSize: 100},
			defaultPreferLocalSSD: true,
			expected:              false,
		},
		{
			name:                  "VolumeSize set overrides LocalSSDPreferOn",
			spec:                  ClusterSpec{VolumeSize: 100, LocalSSD: LocalSSDPreferOn},
			defaultPreferLocalSSD: true,
			expected:              false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.spec.mayUseLocalSSD(tc.defaultPreferLocalSSD)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestClustersRetainClearedInfo(t *testing.T) {
	// Adding a test in case we switch the ClustersCompatible signature to take
	// pointers to ClusterSpec in the future.
	t.Run("original structs are not modified", func(t *testing.T) {
		s1 := ClusterSpec{
			NodeCount:              5,
			ExposedMetamorphicInfo: map[string]string{"VolumeType": "io2"},
		}
		s2 := ClusterSpec{
			NodeCount:              5,
			ExposedMetamorphicInfo: map[string]string{"VolumeType": "gp3"},
		}

		ClustersCompatible(s1, s2, GCE)

		// Original data should still be there
		require.Equal(t, "io2", s1.ExposedMetamorphicInfo["VolumeType"])
		require.Equal(t, "gp3", s2.ExposedMetamorphicInfo["VolumeType"])
	})
}

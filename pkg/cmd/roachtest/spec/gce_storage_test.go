// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spec

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGCEStorageDecision(t *testing.T) {
	tests := []struct {
		name                  string
		spec                  ClusterSpec
		benchmark             bool
		defaultPreferLocalSSD bool
		expectedKind          GCEStorageKind
		expectedVolumeType    string
		expectedUsesLocalSSD  bool
		expectedRequiresPDSSD bool
	}{
		{
			name:                 "explicit local SSD",
			spec:                 ClusterSpec{VolumeType: "local-ssd"},
			expectedKind:         GCEStorageLocalSSD,
			expectedVolumeType:   "local-ssd",
			expectedUsesLocalSSD: true,
		},
		{
			name:                  "explicit local SSD overrides benchmark remote defaults",
			spec:                  ClusterSpec{VolumeType: "local-ssd", LocalSSD: LocalSSDDisable},
			benchmark:             true,
			defaultPreferLocalSSD: false,
			expectedKind:          GCEStorageLocalSSD,
			expectedVolumeType:    "local-ssd",
			expectedUsesLocalSSD:  true,
		},
		{
			name:                  "explicit pd-ssd",
			spec:                  ClusterSpec{VolumeType: "pd-ssd"},
			expectedKind:          GCEStoragePDSSD,
			expectedVolumeType:    "pd-ssd",
			expectedRequiresPDSSD: true,
		},
		{
			name:                  "explicit pd-ssd overrides local SSD preference",
			spec:                  ClusterSpec{VolumeType: "pd-ssd", LocalSSD: LocalSSDPreferOn},
			defaultPreferLocalSSD: true,
			expectedKind:          GCEStoragePDSSD,
			expectedVolumeType:    "pd-ssd",
			expectedRequiresPDSSD: true,
		},
		{
			name:               "explicit non-pd remote volume",
			spec:               ClusterSpec{VolumeType: "hyperdisk-balanced"},
			expectedKind:       GCEStorageExplicitRemote,
			expectedVolumeType: "hyperdisk-balanced",
		},
		{
			name:               "non-benchmark volume size uses default remote disk",
			spec:               ClusterSpec{VolumeSize: 500},
			expectedKind:       GCEStorageDefaultRemote,
			expectedVolumeType: "",
		},
		{
			name:                  "benchmark volume size preserves pd-ssd",
			spec:                  ClusterSpec{VolumeSize: 500},
			benchmark:             true,
			expectedKind:          GCEStoragePDSSD,
			expectedVolumeType:    "pd-ssd",
			expectedRequiresPDSSD: true,
		},
		{
			name:         "volume size with randomization randomizes remote storage",
			spec:         ClusterSpec{VolumeSize: 500, RandomizeVolumeType: true},
			benchmark:    true,
			expectedKind: GCEStorageRandomized,
		},
		{
			name:                 "local SSD preference",
			spec:                 ClusterSpec{LocalSSD: LocalSSDPreferOn},
			expectedKind:         GCEStorageLocalSSD,
			expectedVolumeType:   "local-ssd",
			expectedUsesLocalSSD: true,
		},
		{
			name:                 "local SSD preference overrides randomization",
			spec:                 ClusterSpec{LocalSSD: LocalSSDPreferOn, RandomizeVolumeType: true},
			expectedKind:         GCEStorageLocalSSD,
			expectedVolumeType:   "local-ssd",
			expectedUsesLocalSSD: true,
		},
		{
			name:         "randomized storage",
			spec:         ClusterSpec{RandomizeVolumeType: true},
			expectedKind: GCEStorageRandomized,
		},
		{
			name:         "local SSD disabled uses default remote disk",
			spec:         ClusterSpec{LocalSSD: LocalSSDDisable},
			expectedKind: GCEStorageDefaultRemote,
		},
		{
			name:                  "benchmark with local SSD disabled preserves pd-ssd",
			spec:                  ClusterSpec{LocalSSD: LocalSSDDisable},
			benchmark:             true,
			expectedKind:          GCEStoragePDSSD,
			expectedVolumeType:    "pd-ssd",
			expectedRequiresPDSSD: true,
		},
		{
			name:                  "default spec follows default local SSD preference",
			defaultPreferLocalSSD: true,
			expectedKind:          GCEStorageLocalSSD,
			expectedVolumeType:    "local-ssd",
			expectedUsesLocalSSD:  true,
		},
		{
			name:                  "benchmark default remote storage preserves pd-ssd",
			benchmark:             true,
			defaultPreferLocalSSD: false,
			expectedKind:          GCEStoragePDSSD,
			expectedVolumeType:    "pd-ssd",
			expectedRequiresPDSSD: true,
		},
		{
			name:                  "non-benchmark default remote storage uses machine default",
			defaultPreferLocalSSD: false,
			expectedKind:          GCEStorageDefaultRemote,
		},
		{
			name:         "unknown local SSD setting uses default remote disk",
			spec:         ClusterSpec{LocalSSD: LocalSSDSetting(99)},
			expectedKind: GCEStorageDefaultRemote,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			decision := tc.spec.GCEStorageDecision(tc.benchmark, tc.defaultPreferLocalSSD)

			require.Equal(t, tc.expectedKind, decision.Kind)
			require.Equal(t, tc.expectedVolumeType, decision.VolumeType)
			require.Equal(t, tc.expectedUsesLocalSSD, decision.UsesLocalSSD())
			require.Equal(t, tc.expectedRequiresPDSSD, decision.RequiresPDSSD())
			require.NotEmpty(t, decision.Reason)
		})
	}
}

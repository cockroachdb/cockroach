// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/stretchr/testify/require"
)

func TestStorageVariantRegs(t *testing.T) {
	tests := []struct {
		name             string
		baseClouds       registry.CloudSet
		skipLocalSSD     bool
		expectedSuffixes []string
	}{
		{
			name:       "all clouds includes all volume types and local-ssd",
			baseClouds: registry.AllClouds,
			expectedSuffixes: []string{
				"",
				"/vol=gp3/fs=ext4", "/vol=gp3/fs=xfs",
				"/vol=io2/fs=ext4", "/vol=io2/fs=xfs",
				"/vol=pd-ssd/fs=ext4", "/vol=pd-ssd/fs=xfs",
				"/vol=premium-ssd/fs=ext4", "/vol=premium-ssd/fs=xfs",
				"/vol=premium-ssd-v2/fs=ext4", "/vol=premium-ssd-v2/fs=xfs",
				"/vol=ultra-disk/fs=ext4", "/vol=ultra-disk/fs=xfs",
				"/local-ssd/fs=ext4", "/local-ssd/fs=xfs",
			},
		},
		{
			name:         "all clouds skip local-ssd",
			baseClouds:   registry.AllClouds,
			skipLocalSSD: true,
			expectedSuffixes: []string{
				"",
				"/vol=gp3/fs=ext4", "/vol=gp3/fs=xfs",
				"/vol=io2/fs=ext4", "/vol=io2/fs=xfs",
				"/vol=pd-ssd/fs=ext4", "/vol=pd-ssd/fs=xfs",
				"/vol=premium-ssd/fs=ext4", "/vol=premium-ssd/fs=xfs",
				"/vol=premium-ssd-v2/fs=ext4", "/vol=premium-ssd-v2/fs=xfs",
				"/vol=ultra-disk/fs=ext4", "/vol=ultra-disk/fs=xfs",
			},
		},
		{
			name:       "single cloud excludes other clouds volume types",
			baseClouds: registry.OnlyAWS,
			expectedSuffixes: []string{
				"",
				"/vol=gp3/fs=ext4", "/vol=gp3/fs=xfs",
				"/vol=io2/fs=ext4", "/vol=io2/fs=xfs",
				"/local-ssd/fs=ext4", "/local-ssd/fs=xfs",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			regs := storageVariantRegs(tc.baseClouds, tc.skipLocalSSD)

			var suffixes []string
			for _, r := range regs {
				suffixes = append(suffixes, r.nameSuffix)
			}
			require.Equal(t, tc.expectedSuffixes, suffixes)

			// The first element is always the base registration.
			require.Empty(t, regs[0].nameSuffix)
			require.Empty(t, regs[0].specOpts)
			require.Equal(t, tc.baseClouds, regs[0].clouds)

			// All non-base variants have spec options including filesystem.
			for _, r := range regs[1:] {
				require.NotEmpty(t, r.specOpts, "variant %s should have spec options", r.nameSuffix)
			}
		})
	}
}

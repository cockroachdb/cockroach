// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
	"github.com/stretchr/testify/require"
)

// TestUniqueVersions tests that the registered upgrades have unique versions.
func TestUniqueVersions(t *testing.T) {
	versions := make(map[roachpb.Version]upgradebase.Upgrade)
	for _, m := range upgrades {
		_, found := versions[m.Version()]
		require.Falsef(t, found, "duplicate version %s", m.Version())
		versions[m.Version()] = m
	}
}

// TestFirstUpgradesAfterPreExistingRelease checks that the first internal
// version following each supported pre-existing release has an firstUpgrade
// registered for it.
func TestFirstUpgradesAfterPreExistingRelease(t *testing.T) {
	// Compute the set of pre-existing releases supported by this binary.
	// This excludes the latest release if the binary version is a release.
	preExistingReleases := make(map[roachpb.Version]struct{})
	minBinaryVersion := clusterversion.ByKey(clusterversion.BinaryMinSupportedVersionKey)
	binaryVersion := clusterversion.ByKey(clusterversion.BinaryVersionKey)
	for _, v := range clusterversion.ListBetween(minBinaryVersion, binaryVersion) {
		preExistingReleases[roachpb.Version{Major: v.Major, Minor: v.Minor}] = struct{}{}
	}
	if binaryVersion.Internal == 0 {
		delete(preExistingReleases, binaryVersion)
	}

	require.NotEmpty(t, preExistingReleases)
	// Check that the first internal version after each pre-existing release has
	// an upgrade registered for it, and that that upgrade is in the
	// firstUpgradesAfterPreExistingReleases slice.
	for r := range preExistingReleases {
		v := roachpb.Version{Major: r.Major, Minor: r.Minor, Internal: 2}
		m, found := registry[v]
		require.True(t, found, "missing upgrade for %s in registry", v)
		require.Contains(t, m.Name(), firstUpgradeDescription(v), "upgrade for %s must use newFirstUpgrade", v)
	}
	// Check that for each registered upgrade for a non-primordial version with
	// internal version 2 is two internal versions ahead of a supported pre-existing
	// release.
	for v := range registry {
		if v.Major == 0 || v.Internal != 2 {
			continue
		}
		r := roachpb.Version{Major: v.Major, Minor: v.Minor}
		_, found := preExistingReleases[r]
		require.True(t, found,
			"registered upgrade for %s but %s is not a supported pre-existing release",
			v, r)
	}
}

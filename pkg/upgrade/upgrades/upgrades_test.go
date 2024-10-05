// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
// version following each supported pre-existing release has an upgrade
// registered for it, which is also in firstUpgradesAfterPreExistingReleases,
// and vice-versa.
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
		require.Contains(t, firstUpgradesAfterPreExistingReleases, m,
			"missing upgrade for %s in slice", v)
	}
	// Check that for each registered upgrade for a non-primordial version with
	// internal version 2 is two internal versions ahead of a supported pre-existing
	// release, and that the upgrade is in the firstUpgradesAfterPreExistingReleases
	// slice.
	for v, m := range registry {
		if v.Major == 0 || v.Internal != 2 {
			continue
		}
		r := roachpb.Version{Major: v.Major, Minor: v.Minor}
		_, found := preExistingReleases[r]
		require.True(t, found,
			"registered upgrade for %s but %s is not a supported pre-existing release",
			v, r)
		require.Contains(t, firstUpgradesAfterPreExistingReleases, m,
			"missing upgrade for %s in slice", v)
	}
	// Check that the firstUpgradesAfterPreExistingReleases slice has no upgrades
	// which aren't registered. This implies there are no duplicates either.
	for _, m := range firstUpgradesAfterPreExistingReleases {
		_, found := registry[m.Version()]
		require.Truef(t, found, "expected upgrade %s to be registered, but isn't", m.Version())
	}
}

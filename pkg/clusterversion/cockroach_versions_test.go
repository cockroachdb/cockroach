// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusterversion

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

// TestVersionTable runs sanity checks on the versions table:
//   - all keys have a non-zero version;
//   - versions are strictly increasing;
//   - we have no odd Internal values.
func TestVersionTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var prev roachpb.Version
	for k := Key(0); k < numKeys; k++ {
		v := versionTable[k]
		require.NotEqual(t, roachpb.Version{}, v)
		// We don't use Patch for cluster versions.
		require.Zero(t, v.Patch)
		// All internal versions must be even (since 23.1).
		require.Zero(t, v.Internal%2)
		if k > 0 {
			require.True(t, prev.Less(v))
		}
		prev = v
	}
}

// TestNoMissingVersions checks that starting with MinSupported, we have no gaps
// in Internal versions (which would happen if a version was incorrectly
// removed).
func TestNoMissingVersions(t *testing.T) {
	// removedVersions lists versions that have been purposely removed during a
	// development cycle.
	//
	// Any additions here should be accompanied by a comment explaining the
	// reason. The versions in the list must be in order.
	removedVersions := []roachpb.Version{
		// In the 23.2 cycle V23_2TTLAllowDescPK and V23_2_PartiallyVisibleIndexes
		// were introduced but then later removed (since they were redundant).
		{Major: 23, Minor: 1, Internal: 4},
		{Major: 23, Minor: 1, Internal: 6},
	}

	for k := MinSupported + 1; k < numKeys; k++ {
		prev := (k - 1).Version()
		v := k.Version()
		if v.Major == prev.Major && v.Minor == prev.Minor {
			expectedInternal := prev.Internal + 2

			// Allow exceptions. This loop assumes the removed versions are increasing
			// (the condition could hit multiple times).
			for _, removed := range removedVersions {
				if (removed.Major == v.Major || removed.Major+DevOffset == v.Major) &&
					removed.Minor == v.Minor &&
					removed.Internal == expectedInternal {
					expectedInternal += 2
				}
			}

			require.Equalf(t, expectedInternal, v.Internal,
				"version gap between %s (%s) and %s (%s)", k-1, prev, k, v,
			)
		}
	}
}

// TestKeyConstants runs sanity checks on version key constants.
func TestKeyConstants(t *testing.T) {
	require.Equal(t, numKeys-1, Latest)
	// MinSupported should be a final release.
	require.True(t, MinSupported.IsFinal())

	supported := SupportedPreviousReleases()
	require.Equal(t, MinSupported, supported[0])
	// Check PreviousRelease.
	require.Equal(t, PreviousRelease, supported[len(supported)-1])
}

func TestFinalVersion(t *testing.T) {
	if finalVersion >= 0 {
		require.False(t, DevelopmentBranch, "final version set but developmentBranch is still set")
		require.Equal(t, Latest, finalVersion, "finalVersion must match the minted latest version")
	} else {
		require.False(t, Latest.IsFinal(), "finalVersion not set but Latest is final")
	}
}

func TestVersionFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	v := ClusterVersion{
		Version: roachpb.Version{
			Major: 1,
			Minor: 2,
			Patch: 3,
		},
	}

	if actual, expected := string(redact.Sprint(v.Version)), `1.2`; actual != expected {
		t.Errorf("expected %q, got %q", expected, actual)
	}

	if actual, expected := v.Version.String(), `1.2`; actual != expected {
		t.Errorf("expected %q, got %q", expected, actual)
	}

	if actual, expected := v.String(), `1.2`; actual != expected {
		t.Errorf("expected %q, got %q", expected, actual)
	}
}

func TestClusterVersionPrettyPrint(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cv := func(major, minor, patch, internal int32) ClusterVersion {
		return ClusterVersion{
			Version: roachpb.Version{
				Major:    major,
				Minor:    minor,
				Patch:    patch,
				Internal: internal,
			},
		}
	}

	tests := []struct {
		cv  ClusterVersion
		exp string
	}{
		{cv(20, 1, 0, 4), "20.1-upgrading-to-20.2-step-004"},
		{cv(20, 2, 0, 7), "20.2-upgrading-to-21.1-step-007(fence)"},
		{cv(20, 2, 0, 4), "20.2-upgrading-to-21.1-step-004"},
		{cv(22, 2, 1, 5), "22.2-upgrading-to-23.1-step-005(fence)"},
		{cv(22, 2, 1, 4), "22.2-upgrading-to-23.1-step-004"},
	}
	for _, test := range tests {
		if actual := test.cv.PrettyPrint().StripMarkers(); actual != test.exp {
			t.Errorf("expected %s, got %q", test.exp, actual)
		}
	}
}

func TestReleaseSeries(t *testing.T) {
	// Verify that the ReleaseSeries call works on all keys.
	for k := Latest; k > 0; k-- {
		if k.Version().Major > 0 {
			require.NotEqual(t, roachpb.ReleaseSeries{}, k.ReleaseSeries())
		} else {
			require.Equal(t, roachpb.ReleaseSeries{}, k.ReleaseSeries())
		}
	}

	// Verify the latest version.
	major, minor := build.BranchReleaseSeries()
	require.Equal(t, fmt.Sprintf("v%s", Latest.ReleaseSeries()), fmt.Sprintf("v%d.%d", major, minor))

	// Verify the ReleaseSeries results down to MinSupported.
	expected := Latest.ReleaseSeries()
	for k := Latest; k >= MinSupported; k-- {
		if k.IsFinal() {
			v := RemoveDevOffset(k.Version())
			expected = roachpb.ReleaseSeries{Major: v.Major, Minor: v.Minor}
		}
		require.Equalf(t, expected, k.ReleaseSeries(), "version: %s", k)
	}
}

func TestStringForPersistence(t *testing.T) {
	testCases := []struct {
		v            roachpb.Version
		minSupported roachpb.Version
		expected     string
	}{
		{
			v:            roachpb.Version{Major: 23, Minor: 2},
			minSupported: roachpb.Version{Major: 23, Minor: 2},
			expected:     "23.2",
		},
		{
			v:            roachpb.Version{Major: 24, Minor: 1},
			minSupported: roachpb.Version{Major: 23, Minor: 2},
			expected:     "24.1",
		},
		{
			v:            roachpb.Version{Major: 24, Minor: 1, Internal: 10},
			minSupported: roachpb.Version{Major: 23, Minor: 2},
			expected:     "24.1-10",
		},
		{
			v:            roachpb.Version{Major: 24, Minor: 1, Internal: 10},
			minSupported: roachpb.Version{Major: 24, Minor: 1},
			expected:     "24.1-upgrading-to-24.2-step-010",
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tc.expected, stringForPersistenceWithMinSupported(tc.v, tc.minSupported))
		})
	}
}

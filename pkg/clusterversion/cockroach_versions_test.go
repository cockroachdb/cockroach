// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusterversion

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/redact"
	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
)

func TestVersionsAreValid(t *testing.T) {
	defer leaktest.AfterTest(t)()

	require.NoError(t, versionsSingleton.Validate())
}

// TestPreserveVersionsForMinBinaryVersion ensures that versions
// at or above binaryMinSupportedVersion are not deleted.
func TestPreserveVersionsForMinBinaryVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()

	prevVersion := keyedVersion{
		Key:     -1,
		Version: roachpb.Version{Major: 1, Minor: 1},
	}
	for _, namedVersion := range versionsSingleton {
		v := namedVersion.Version
		if v.Less(binaryMinSupportedVersion) {
			prevVersion = namedVersion
			continue
		}
		expectedDiff := int32(2)
		if prevVersion.Key == V23_2Start {
			// In 23.2 cycle V23_2TTLAllowDescPK and
			// V23_2_PartiallyVisibleIndexes were introduced but then later
			// removed (since they were redundant), so we exempt them from not
			// being deleted. (There were more versions introduced and then
			// removed, but they were at the tail of 23.2 versions, so they
			// don't require a similar exemption.)
			expectedDiff = 6
		}
		if v.Major == prevVersion.Major && v.Minor == prevVersion.Minor {
			require.Equalf(t, prevVersion.Internal+expectedDiff, v.Internal,
				"version(s) between %s (%s) and %s (%s) is(are) at or above minBinaryVersion (%s) and should not be removed",
				prevVersion.Key, prevVersion.Version,
				namedVersion.Key, namedVersion.Version,
				binaryMinSupportedVersion)
		}
		prevVersion = namedVersion
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
		{cv(19, 2, 1, 5), "19.2-5"},
		{cv(20, 1, 0, 4), "20.1-4"},
		{cv(20, 2, 0, 7), "20.2-7(fence)"},
		{cv(20, 2, 0, 4), "20.2-4"},
		{cv(20, 2, 1, 5), "20.2-5(fence)"},
		{cv(20, 2, 1, 4), "20.2-4"},
	}
	for _, test := range tests {
		if actual := test.cv.PrettyPrint(); actual != test.exp {
			t.Errorf("expected %s, got %q", test.exp, actual)
		}
	}
}

func TestGetVersionsBetween(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Define a list of versions v3..v9
	var vs keyedVersions
	for i := 3; i < 10; i++ {
		vs = append(vs, keyedVersion{
			Key:     Key(42),
			Version: roachpb.Version{Major: int32(i)},
		})
	}
	v := func(major int32) roachpb.Version {
		return roachpb.Version{Major: major}
	}
	list := func(first, last int32) []roachpb.Version {
		var cvs []roachpb.Version
		for i := first; i <= last; i++ {
			cvs = append(cvs, v(i))
		}
		return cvs
	}

	tests := []struct {
		from, to roachpb.Version
		exp      []roachpb.Version
	}{
		{v(5), v(8), list(6, 8)},
		{v(1), v(1), []roachpb.Version{}},
		{v(7), v(7), []roachpb.Version{}},
		{v(1), v(5), list(3, 5)},
		{v(6), v(12), list(7, 9)},
		{v(4), v(5), list(5, 5)},
	}

	for _, test := range tests {
		actual := listBetweenInternal(test.from, test.to, vs)
		if len(actual) != len(test.exp) {
			t.Errorf("expected %d versions, got %d", len(test.exp), len(actual))
		}

		for i := range test.exp {
			if actual[i] != test.exp[i] {
				t.Errorf("%s version incorrect: expected %s, got %s", humanize.Ordinal(i), test.exp[i], actual[i])
			}
		}
	}
}

// TestEnsureConsistentBinaryVersion ensures that BinaryVersionKey maps to a
// version equal to binaryVersion.
func TestEnsureConsistentBinaryVersion(t *testing.T) {
	require.Equal(t, ByKey(BinaryVersionKey), binaryVersion)
}

// TestEnsureConsistentMinBinaryVersion ensures that BinaryMinSupportedVersionKey
// maps to a version equal to binaryMinSupportedVersion.
func TestEnsureConsistentMinBinaryVersion(t *testing.T) {
	require.Equal(t, ByKey(BinaryMinSupportedVersionKey), binaryMinSupportedVersion)
}

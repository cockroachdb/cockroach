// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package version

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetters(t *testing.T) {
	v, err := Parse("v1.2.3-beta.1")
	require.NoError(t, err)

	require.Equal(t, MajorVersion{1, 2}, v.Major())
	require.Equal(t, 3, v.Patch())

	require.True(t, v.IsPrerelease())
	require.False(t, v.IsCloudOnlyBuild())
	require.False(t, v.IsCustomOrNightlyBuild())
}

func TestValid(t *testing.T) {
	testData := []string{
		// a subset of real version strings
		"v19.1.11",
		"v21.1.0-1-g9cbe7c5281",
		"v22.2.10-1-g7b8322d67c-fips",
		"v22.2.10-fips",
		"v23.1.0-alpha.0",
		"v23.1.0-alpha.1-1643-gdf8e73734e-fips",
		"v23.1.0-alpha.4-fips",
		"v23.1.11-cloudonly",
		"v23.1.11-cloudonly2",
		"v23.1.12-cloudonly-rc1",
		"v23.2.0-beta.1-cloudonly-rc1",
		"v23.2.0-alpha.00000000-4376-g7450647f213",

		// we may generate a cloudonly pre-release, for testing purposes
		"v24.3.0-alpha.1-cloudonly.1",

		// some version strings wouldn't be considered valid, but have exist
		// historically existed, so we have to allow them. see the ordering
		// and Compare() tests for more on how these are handled
		"v23.1.0-swenson-mr-4",

		// these may not actually exist, but are parseable
		"v1.1.2-beta.20190101+metadata",
		"v1.2.3-rc1-with-hyphen+metadata-with-hyphen",
	}
	for _, str := range testData {
		t.Run(str, func(t *testing.T) {
			v, err := Parse(str)
			require.NoError(t, err)
			require.Equal(t, str, v.String())
		})
	}
}

func TestInvalid(t *testing.T) {
	testData := []string{
		"v1",
		"v1.2",
		"v1.2-beta",
		"v1x2.3",
		"v1.2x3",
		"1.0.0",
		" v1.0.0",
		"v1.0.0  ",
		"v1.2.beta",
		"v1.2-beta",
		"v1.2.3.beta",
		"v1.2.3-beta$",
		"v1.2.3-bet;a",
		"v1.2.3+metadata%",
		"v01.2.3",
		"v1.02.3",
		"v1.2.03",

		// these were formerly considered valid, and are valid SemVer versions; however
		// CRDB versions are not semantic versions, so these are now considered invalid
		"v0.0.0",
		"v0.0.1",
		"v0.1.0",
		"v1.0.0",
		"v1.0.0-alpha",
		"v1.0.0-beta.20190101",
		"v1.0.0-rc1-with-hyphen",
		"v1.0.0-rc2.dot.dot",
		"v1.2.3+metadata",
		"v1.2.3+metadata-with-hyphen",
		"v1.2.3+metadata.with.dots",
	}
	for _, str := range testData {
		t.Run(str, func(t *testing.T) {
			_, err := Parse(str)
			require.Error(t, err)
		})
	}
}

func TestCompare(t *testing.T) {
	testData := []struct {
		a, b string
		cmp  int
	}{
		// Typical comparison scenarios of common types of versions
		{"v1.1.0", "v1.1.0", 0},
		{"v1.1.0", "v1.1.1", -1},
		{"v1.2.3", "v1.3.0", -1},
		{"v1.2.3", "v2.1.0", -1},
		{"v1.1.0", "v1.1.0-alpha.1", +1},
		{"v1.1.0", "v1.1.0-rc.2", +1},
		{"v1.1.0-alpha.1", "v1.1.0-beta.1", -1},
		{"v1.1.0-beta.1", "v1.1.0-rc.2", -1},
		{"v1.1.0-rc.2", "v1.1.0-rc.10", -1},
		{"v1.1.1", "v1.1.0-alpha.1", +1},

		// When versions have unrecognized custom suffixes, they are compared lexicographically.
		// A version with any unrecognized custom suffix is considered earlier than the same version
		// with no suffix, since these suffixes are most commonly used for odd pre-release cases
		{"v1.2.3", "v1.2.3-foo", -1},
		{"v1.2.3", "v1.2.3-4", -1},
		{"v1.2.3", "v1.2.3-4-foo", -1},
		{"v1.2.3-4", "v1.2.3-4-foo", -1},
	}
	for _, tc := range testData {
		t.Run(fmt.Sprintf("%s vs %s", tc.a, tc.b), func(t *testing.T) {
			a, err := Parse(tc.a)
			require.NoError(t, err)
			b, err := Parse(tc.b)
			require.NoError(t, err)

			require.Equal(t, tc.cmp, a.Compare(b))
			require.Equal(t, -tc.cmp, b.Compare(a))
		})
	}
}

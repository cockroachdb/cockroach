// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package version

import (
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// shuffleStrings returns a new slice containing the values from in
// shuffled into a random order
func shuffleStrings(in []string) []string {
	out := make([]string, len(in))
	for a, b := range rand.Perm(len(in)) {
		out[a] = in[b]
	}
	return out
}

func TestVersion_Empty(t *testing.T) {
	v := Version{}

	require.True(t, v.Empty())
	require.Equal(t, "", v.String())
	require.Equal(t, "", v.DisplayName())
}

func TestVersion_Getters(t *testing.T) {
	v, err := Parse("v1.2.3-beta.1")
	require.NoError(t, err)

	require.Equal(t, MajorVersion{1, 2}, v.Major())
	require.Equal(t, 3, v.Patch())

	require.True(t, v.IsPrerelease())
	require.False(t, v.IsCloudOnlyBuild())
	require.False(t, v.IsCustomOrNightlyBuild())
}

func TestVersion_IsPrerelease(t *testing.T) {
	// Valid pre-release versions
	require.True(t, MustParse("v20.2.0-beta.3").IsPrerelease())
	require.True(t, MustParse("v19.1.0-rc.5").IsPrerelease())
	require.True(t, MustParse("v20.2.0-alpha.1").IsPrerelease())
	require.True(t, MustParse("v21.1.0-rc.2-163-g122c66f436").IsPrerelease())
	require.True(t, MustParse("v21.1.0-beta.5-57-gf05a57face").IsPrerelease())
	require.True(t, MustParse("v21.1.0-alpha.3-2846-g7ae3ac92f7").IsPrerelease())
	require.True(t, MustParse("v23.2.0-rc.2-cloudonly-rc2").IsPrerelease())
	require.True(t, MustParse("v24.3.0-alpha.1-cloudonly.1").IsPrerelease())

	// Valid production versions
	require.False(t, MustParse("v19.2.6").IsPrerelease())
	require.False(t, MustParse("v21.1.0").IsPrerelease())
	require.False(t, MustParse("v21.1.0-247-g5668206478").IsPrerelease())

	// Cloudonly versions are stable, despite the suffix
	require.False(t, MustParse("v23.1.12-cloudonly-rc2").IsPrerelease())

	// 23.2.0 had some quirky cloudonly tag formats; they're all stable, too
	require.False(t, MustParse("v23.2.0-cloudonly-rc2").IsPrerelease())
	require.False(t, MustParse("v23.2.0-cloudonly").IsPrerelease())
	require.False(t, MustParse("v23.2.0-cloudonly.1").IsPrerelease())
	require.False(t, MustParse("v23.2.0-cloudonly2").IsPrerelease())
}

func TestVersion_IsCustomOrNightlyBuild(t *testing.T) {
	// Valid pre-release versions
	require.False(t, MustParse("v20.2.0-beta.3").IsCustomOrNightlyBuild())
	require.False(t, MustParse("v19.1.0-rc.5").IsCustomOrNightlyBuild())
	require.False(t, MustParse("v20.2.0-alpha.1").IsCustomOrNightlyBuild())
	require.True(t, MustParse("v21.1.0-rc.2-163-g122c66f436").IsCustomOrNightlyBuild())
	require.True(t, MustParse("v21.1.0-beta.5-57-gf05a57face").IsCustomOrNightlyBuild())
	require.True(t, MustParse("v21.1.0-alpha.3-2846-g7ae3ac92f7").IsCustomOrNightlyBuild())

	// Valid [cloudonly] pre-release versions
	require.False(t, MustParse("v23.2.0-rc.2-cloudonly-rc2").IsCustomOrNightlyBuild())

	// Valid production versions
	require.False(t, MustParse("v19.2.6").IsCustomOrNightlyBuild())
	require.False(t, MustParse("v21.1.0").IsCustomOrNightlyBuild())
	require.True(t, MustParse("v21.1.0-247-g5668206478").IsCustomOrNightlyBuild())

	// Valid [cloudonly] production versions
	require.False(t, MustParse("v23.1.12-cloudonly-rc2").IsCustomOrNightlyBuild())

	// Valid [cloudonly] v23.2.0 (production) versions may or may not have "rc" after "-cloudonly" suffix.
	require.False(t, MustParse("v23.2.0-cloudonly-rc2").IsCustomOrNightlyBuild())
	require.False(t, MustParse("v23.2.0-cloudonly").IsCustomOrNightlyBuild())
	require.False(t, MustParse("v23.2.0-cloudonly.1").IsCustomOrNightlyBuild())
	require.False(t, MustParse("v23.2.0-cloudonly2").IsCustomOrNightlyBuild())
}

func TestVersion_IsCloudOnlyBuild(t *testing.T) {
	// Valid pre-release versions
	require.False(t, MustParse("v20.2.0-beta.3").IsCloudOnlyBuild())
	require.False(t, MustParse("v19.1.0-rc.5").IsCloudOnlyBuild())
	require.False(t, MustParse("v20.2.0-alpha.1").IsCloudOnlyBuild())
	require.False(t, MustParse("v21.1.0-rc.2-163-g122c66f436").IsCloudOnlyBuild())
	require.False(t, MustParse("v21.1.0-beta.5-57-gf05a57face").IsCloudOnlyBuild())
	require.False(t, MustParse("v21.1.0-alpha.3-2846-g7ae3ac92f7").IsCloudOnlyBuild())

	// Valid [cloudonly] pre-release versions
	require.False(t, MustParse("v23.2.0-rc.2-cloudonly-rc2").IsCloudOnlyBuild())

	// Valid production versions
	require.False(t, MustParse("v19.2.6").IsCloudOnlyBuild())
	require.False(t, MustParse("v21.1.0").IsCloudOnlyBuild())
	require.False(t, MustParse("v21.1.0-247-g5668206478").IsCloudOnlyBuild())

	// Valid [cloudonly] production versions
	require.True(t, MustParse("v23.1.12-cloudonly-rc2").IsCloudOnlyBuild())

	// Valid [cloudonly] v23.2.0 (production) versions may or may not have "rc" after "-cloudonly" suffix.
	require.True(t, MustParse("v23.2.0-cloudonly-rc2").IsCloudOnlyBuild())
	require.True(t, MustParse("v23.2.0-cloudonly").IsCloudOnlyBuild())
	require.True(t, MustParse("v23.2.0-cloudonly.1").IsCloudOnlyBuild())
	require.True(t, MustParse("v23.2.0-cloudonly2").IsCloudOnlyBuild())
}

func TestParse(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		testData := []string{
			// a subset of real version strings from crdb_versions and intrusion_crdb_clusters
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
			v, err := Parse(str)
			require.NoError(t, err)
			require.Equal(t, str, v.String())

			parts := strings.Split(str, ".")
			vMajor := parts[0]
			minor := parts[1]
			series := vMajor + "." + minor
			require.Equal(t, series, v.Major().String())
		}

		// special case
		shaVersion := "sha256:6bbf843734d11db9cc5eb8ea77f6974032e17ad216c91ccecfaf52a4890eaa11:latest-v22.2-build"
		v, err := Parse(shaVersion)
		require.NoError(t, err)
		require.Equal(t, shaVersion, v.String())
		require.Equal(t, MajorVersion{22, 2}, v.Major())
	})

	t.Run("invalid", func(t *testing.T) {
		testData := []string{
			"v0.0.0",
			"v0.0.1",
			"v0.1.0",
			"v1",
			"v1.0.0-alpha",
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
			"v1.0.0-rc1-with-hyphen",
			"v1.0.0-rc2.dot.dot",
			"v1.2.3+metadata",
			"v1.2.3+metadata-with-hyphen",
			"v1.2.3+metadata.with.dots",
		}
		for _, str := range testData {
			_, err := Parse(str)
			require.Errorf(t, err, "expected error parsing '%s'", str)
		}
	})

	t.Run("verify-expected-parsing", func(t *testing.T) {
		for _, tc := range []struct {
			raw  string
			want Version
		}{
			// pre-releases
			{
				raw: "v24.2.0-alpha.1",
				want: Version{
					year:         24,
					ordinal:      2,
					patch:        0,
					phase:        alpha,
					phaseOrdinal: 1,
				},
			},
			{
				raw: "v24.2.0-alpha.1-cloudonly.2",
				want: Version{
					year:            24,
					ordinal:         2,
					patch:           0,
					phase:           alpha,
					phaseOrdinal:    1,
					phaseSubOrdinal: 2,
				},
			},
			{
				raw: "v24.2.0-beta.2",
				want: Version{
					year:         24,
					ordinal:      2,
					patch:        0,
					phase:        beta,
					phaseOrdinal: 2,
				},
			},
			{
				raw: "v24.2.0-beta.2-cloudonly.3",
				want: Version{
					year:            24,
					ordinal:         2,
					patch:           0,
					phase:           beta,
					phaseOrdinal:    2,
					phaseSubOrdinal: 3,
				},
			},
			// cloudonly releases
			{
				raw: "v24.2.0-cloudonly.4",
				want: Version{
					year:         24,
					ordinal:      2,
					patch:        0,
					phase:        cloudonly,
					phaseOrdinal: 4,
				},
			},
			// stable releases
			{
				raw: "v24.2.0",
				want: Version{
					year:    24,
					ordinal: 2,
					patch:   0,
					phase:   stable,
				},
			},
			{
				raw: "v24.2.4",
				want: Version{
					year:    24,
					ordinal: 2,
					patch:   4,
					phase:   stable,
				},
			},
			{
				raw: "v24.2.4-cloudonly.2",
				want: Version{
					year:         24,
					ordinal:      2,
					patch:        4,
					phase:        cloudonly,
					phaseOrdinal: 2,
				},
			},
			// custom releases
			{
				raw: "v24.2.3-12-gabcd1234",
				want: Version{
					year:           24,
					ordinal:        2,
					patch:          3,
					phase:          stable,
					nightlyOrdinal: 12,
				},
			},
		} {
			t.Run(tc.raw, func(t *testing.T) {
				tc.want.raw = tc.raw
				v := MustParse(tc.raw)
				require.Equal(t, tc.want, v)
			})
		}
	})
}

func TestDisplayName(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want string
	}{
		{
			raw:  "v24.2.0",
			want: "v24.2.0",
		},
		{
			raw:  "v24.2.0-alpha.1",
			want: "v24.2.0-alpha.1",
		},
		{
			raw:  "v24.2.0-alpha.1-cloudonly.1",
			want: "v24.2.0-alpha.1",
		},
		{
			raw:  "v24.2.0-beta.2",
			want: "v24.2.0-beta.2",
		},
		{
			raw:  "v24.2.0-rc.3",
			want: "v24.2.0-rc.3",
		},
		{
			raw:  "v24.2.0-cloudonly.4",
			want: "v24.2.0",
		},
		{
			raw:  "v24.2.0-12-gabcd1234",
			want: "v24.2.0",
		},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			v := MustParse(tc.raw)
			require.Equal(t, tc.want, v.DisplayName())
		})
	}
}

func TestVersionCompare(t *testing.T) {
	const aEqualsB = "equal"
	const aLessThanB = "less than"
	const bLessThanA = "greater than"

	testCases := []struct {
		a    string
		b    string
		want string
	}{
		// equality cases
		{
			a:    "v20.2.7",
			b:    "v20.2.7",
			want: aEqualsB,
		},
		{
			a:    "v20.2.7-alpha.1",
			b:    "v20.2.7-alpha.1",
			want: aEqualsB,
		},
		{
			a:    "v20.2.7-beta.1",
			b:    "v20.2.7-beta.1",
			want: aEqualsB,
		},
		{
			a:    "v20.2.7-rc.1",
			b:    "v20.2.7-rc.1",
			want: aEqualsB,
		},
		{
			a:    "v20.2.7-cloudonly.1",
			b:    "v20.2.7-cloudonly.1",
			want: aEqualsB,
		},
		{
			a:    "v20.2.7-rc.1-1-g9cbe7c5281",
			b:    "v20.2.7-rc.1-1-g9cbe7c5281",
			want: aEqualsB,
		},
		{
			a:    "v20.2.7-1-g9cbe7c5281",
			b:    "v20.2.7-1-g9cbe7c5281",
			want: aEqualsB,
		},
		{
			a:    "v23.1.2-customLabel",
			b:    "v23.1.2-customLabel",
			want: aEqualsB,
		},

		// basic dotted version ordering
		{
			a:    "v20.2.7",
			b:    "v20.2.8",
			want: aLessThanB,
		},
		{
			a:    "v20.2.7",
			b:    "v20.2.11",
			want: aLessThanB,
		},
		{
			a:    "v20.2.11",
			b:    "v20.2.100",
			want: aLessThanB,
		},
		{
			a:    "v20.1.0",
			b:    "v20.2.0",
			want: aLessThanB,
		},

		// even if there's a prerelease or nightly tag
		{
			a:    "v20.2.7",
			b:    "v21.1.0-alpha.3",
			want: aLessThanB,
		},
		{
			a:    "v20.2.7",
			b:    "v21.1.0-beta.3",
			want: aLessThanB,
		},
		{
			a:    "v20.2.7",
			b:    "v21.1.0-rc.3",
			want: aLessThanB,
		},
		{
			a:    "v20.2.7",
			b:    "v21.1.0-cloudonly.3",
			want: aLessThanB,
		},
		{
			a:    "v20.2.7",
			b:    "v21.1.0-1-g9cbe7c5281",
			want: aLessThanB,
		},
		{
			a:    "v20.2.7",
			b:    "v21.1.0",
			want: aLessThanB,
		},

		// alpha < beta, beta < rc, rc < cloudonly, cloudonly < .0
		{
			a:    "v20.2.0-alpha.1",
			b:    "v21.1.0-beta.3",
			want: aLessThanB,
		},
		{
			a:    "v21.1.0-beta.3",
			b:    "v21.1.0-rc.2",
			want: aLessThanB,
		},
		{
			a:    "v21.1.0-rc.2",
			b:    "v21.1.0-cloudonly.1",
			want: aLessThanB,
		},
		{
			a:    "v21.1.0-cloudonly.1",
			b:    "v21.1.0",
			want: aLessThanB,
		},

		// nightly & custom builds are greater than "regular" builds, prereleases, and cloudonly
		{
			a:    "v21.1.0-alpha.3",
			b:    "v21.1.0-1-g9cbe7c5281",
			want: aLessThanB,
		},
		{
			a:    "v21.1.0-beta.3",
			b:    "v21.1.0-1-g9cbe7c5281",
			want: aLessThanB,
		},
		{
			a:    "v21.1.0-rc.3",
			b:    "v21.1.0-1-g9cbe7c5281",
			want: aLessThanB,
		},
		{
			a:    "v21.1.0-cloudonly.1",
			b:    "v21.1.0-1-g9cbe7c5281",
			want: aLessThanB,
		},
		{
			a:    "v21.1.0",
			b:    "v21.1.0-1-g9cbe7c5281",
			want: aLessThanB,
		},
		{
			a:    "v21.1.0",
			b:    "v21.1.0-customLabel",
			want: aLessThanB,
		},

		// the "sha256:<whatever>:latest-vX.Y-build" versions are considered slightly later
		// than vX.Y.0, but less than vX.Y.1-alpha.0 (the earliest .1 build possible)
		{
			a:    "v22.2.0",
			b:    "sha256:6bbf843734d11db9cc5eb8ea77f6974032e17ad216c91ccecfaf52a4890eaa11:latest-v22.2-build",
			want: aLessThanB,
		},
		{
			a:    "v22.2.1-alpha.0",
			b:    "sha256:6bbf843734d11db9cc5eb8ea77f6974032e17ad216c91ccecfaf52a4890eaa11:latest-v22.2-build",
			want: bLessThanA,
		},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s %s %s", tc.a, tc.want, tc.b), func(t *testing.T) {
			va := MustParse(tc.a)
			vb := MustParse(tc.b)
			if tc.want == aEqualsB {
				require.Equal(t, 0, va.Compare(vb))
			} else if tc.want == aLessThanB {
				require.Equal(t, -1, va.Compare(vb))
				require.Equal(t, 1, vb.Compare(va))
			} else if tc.want == bLessThanA {
				require.Equal(t, 1, va.Compare(vb))
				require.Equal(t, -1, vb.Compare(va))
			}
		})
	}
}

func TestVersionOrdering(t *testing.T) {
	testCases := []struct {
		name  string
		input []string
		want  []string
	}{
		{
			name:  "sorts semantic versions",
			input: []string{"v20.1.11", "v20.1.100", "v20.1.7"},
			want:  []string{"v20.1.7", "v20.1.11", "v20.1.100"},
		},
		{
			name:  "sorts prereleases before cloudonlys",
			input: []string{"v20.2.10", "v21.1.0", "v21.1.0-rc.1", "v21.1.0-beta.2", "v21.1.0-alpha.1", "v21.1.0-cloudonly.1"},
			want:  []string{"v20.2.10", "v21.1.0-alpha.1", "v21.1.0-beta.2", "v21.1.0-rc.1", "v21.1.0-cloudonly.1", "v21.1.0"},
		},
		{
			name:  "sorts custom builds after corresponding normal version",
			input: []string{"v21.1.0-1-g9cbe7c5281", "v21.1.0", "v21.1.0-rc.1"},
			want:  []string{"v21.1.0-rc.1", "v21.1.0", "v21.1.0-1-g9cbe7c5281"},
		},
		{
			name:  "sorts nonstandard custom builds after corresponding normal version",
			input: []string{"v21.1.0-customLabel", "v21.1.0", "v21.1.0-rc.1"},
			want:  []string{"v21.1.0-rc.1", "v21.1.0", "v21.1.0-customLabel"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			versions := make([]Version, len(tc.input))
			for i, v := range shuffleStrings(tc.input) {
				versions[i] = MustParse(v)
			}
			expectedVersions := make([]Version, len(tc.want))
			for i, v := range tc.want {
				expectedVersions[i] = MustParse(v)

			}

			slices.SortFunc(versions, func(a, b Version) int { return a.Compare(b) })
			require.Equal(t, expectedVersions, versions)
		})
	}
}

func TestAtLeast(t *testing.T) {
	testCases := []struct {
		cockroachVersion string
		minVersion       string
		expected         bool
	}{
		{"v21.2.0-alpha.1", "v21.2.0", false},
		{"v20.2.7", "v21.1.0", false},
		{"v21.1.2", "v21.1.3", false},
		{"v21.1.3", "v21.1.3", true},
		{"v20.2.11", "v20.2.7", true},
		{"v21.1.0-beta.3", "v20.2.7", true},
		{"v21.1.0-beta.3", "v20.2.0-alpha.1", true},
		{"v21.1.0-rc.2", "v21.1.0-beta.3", true},
		{"v21.1.0", "v21.1.0-beta.3", true},
		{"v21.1.0", "v21.1.0-rc.3", true},
	}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Version.AtLeast #%d: %s >= %s; %t", i, tc.cockroachVersion, tc.minVersion, tc.expected), func(t *testing.T) {
			a := MustParse(tc.cockroachVersion)
			b := MustParse(tc.minVersion)
			require.Equal(t, tc.expected, a.AtLeast(b))
		})
	}
}

func TestVersionCanBeAMapKey(t *testing.T) {
	// not a real test, but a reminder that Version needs to be hashable
	_ = make(map[Version]bool)
}

func TestValue(t *testing.T) {
	version := MustParse("v24.2.1-rc.3-cloudonly.1-12-gabcd124")

	str, err := version.Value()
	require.NoError(t, err)
	require.Equal(t, "v24.2.1-rc.3-cloudonly.1-12-gabcd124", str)

	// unlikely but supported
	version = Version{}
	str, err = version.Value()
	require.NoError(t, err)
	require.Equal(t, "", str)
}

func TestScan(t *testing.T) {
	value := "v24.2.1-rc.3-cloudonly.1-12-gabcd124"

	v := Version{}
	err := v.Scan(value)
	require.NoError(t, err)
	require.Equal(t, MustParse("v24.2.1-rc.3-cloudonly.1-12-gabcd124"), v)

	err = v.Scan(nil)
	require.ErrorContains(t, err, "non-nil Version string required")

	err = v.Scan(123) // or any other type
	require.ErrorContains(t, err, "cannot convert int to Version")
}

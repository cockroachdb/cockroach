// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/release"
	"github.com/cockroachdb/version"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func Test_processReleaseData(t *testing.T) {
	testReleases, err := os.ReadFile("testdata/release_data.yaml")
	require.NoError(t, err)

	var data []Release
	require.NoError(t, yaml.Unmarshal(testReleases, &data)) //nolint:yaml

	expectedReleaseData := map[string]release.Series{
		"21.2": {
			Latest: "21.2.1",
		},
		"22.1": {
			Latest:      "22.1.12",
			Predecessor: "21.2",
			Withdrawn:   []string{"22.1.12"},
		},
		"22.2": {
			Latest:      "22.2.0",
			Predecessor: "22.1",
		},
		"23.1": {
			Latest:      "23.1.1",
			Predecessor: "22.2",
			Withdrawn:   []string{"23.1.0"},
		},
	}
	require.Equal(t, expectedReleaseData, processReleaseData(data))
}

func Test_loadReleaseDataFromPath(t *testing.T) {
	t.Run("file does not exist", func(t *testing.T) {
		result, err := loadReleaseDataFromPath(filepath.Join(t.TempDir(), "nonexistent.yaml"))
		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("valid file", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "releases.yaml")
		content := `"26.1":
  latest: 26.1.0
  predecessor: "25.4"
`
		require.NoError(t, os.WriteFile(path, []byte(content), 0644))
		result, err := loadReleaseDataFromPath(path)
		require.NoError(t, err)
		require.Equal(t, map[string]release.Series{
			"26.1": {Latest: "26.1.0", Predecessor: "25.4"},
		}, result)
	})

	t.Run("corrupt YAML", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "releases.yaml")
		require.NoError(t, os.WriteFile(path, []byte("not: [valid: yaml"), 0644))
		_, err := loadReleaseDataFromPath(path)
		require.Error(t, err)
		require.Contains(t, err.Error(), "could not parse existing release data")
	})
}

func Test_preserveUnreleasedSeries(t *testing.T) {
	testCases := []struct {
		name     string
		data     map[string]release.Series
		existing map[string]release.Series
		expected map[string]release.Series
	}{
		{
			name: "preserves unreleased series",
			data: map[string]release.Series{
				"26.1": {Latest: "26.1.0", Predecessor: "25.4"},
				"25.4": {Latest: "25.4.5", Predecessor: "25.3"},
			},
			existing: map[string]release.Series{
				"26.1": {Latest: "26.1.0", Predecessor: "25.4"},
				"26.2": {Predecessor: "26.1"},
			},
			expected: map[string]release.Series{
				"25.4": {Latest: "25.4.5", Predecessor: "25.3"},
				"26.1": {Latest: "26.1.0", Predecessor: "25.4"},
				"26.2": {Predecessor: "26.1"},
			},
		},
		{
			name: "preserves chain of unreleased series",
			data: map[string]release.Series{
				"26.1": {Latest: "26.1.0", Predecessor: "25.4"},
			},
			existing: map[string]release.Series{
				"26.2": {Predecessor: "26.1"},
				"26.3": {Predecessor: "26.2"},
			},
			expected: map[string]release.Series{
				"26.1": {Latest: "26.1.0", Predecessor: "25.4"},
				"26.2": {Predecessor: "26.1"},
				"26.3": {Predecessor: "26.2"},
			},
		},
		{
			name: "does not preserve entries with Latest",
			data: map[string]release.Series{
				"26.1": {Latest: "26.1.0", Predecessor: "25.4"},
			},
			existing: map[string]release.Series{
				"25.4": {Latest: "25.4.3", Predecessor: "25.3"},
			},
			expected: map[string]release.Series{
				"26.1": {Latest: "26.1.0", Predecessor: "25.4"},
			},
		},
		{
			name:     "nil existing data",
			data:     map[string]release.Series{"26.1": {Latest: "26.1.0"}},
			existing: nil,
			expected: map[string]release.Series{"26.1": {Latest: "26.1.0"}},
		},
		{
			name: "downloaded data wins over stale existing entries",
			data: map[string]release.Series{
				"26.1": {Latest: "26.1.2", Predecessor: "25.4"},
			},
			existing: map[string]release.Series{
				"26.1": {Latest: "26.1.0", Predecessor: "25.4"},
			},
			expected: map[string]release.Series{
				"26.1": {Latest: "26.1.2", Predecessor: "25.4"},
			},
		},
		{
			name: "does not preserve orphaned entries",
			data: map[string]release.Series{
				"26.1": {Latest: "26.1.0", Predecessor: "25.4"},
			},
			existing: map[string]release.Series{
				"99.1": {Predecessor: "99.0"},
			},
			expected: map[string]release.Series{
				"26.1": {Latest: "26.1.0", Predecessor: "25.4"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			preserveUnreleasedSeries(tc.data, tc.existing)
			require.Equal(t, tc.expected, tc.data)
		})
	}
}

func Test_addCurrentRelease(t *testing.T) {
	testCases := []struct {
		name           string
		data           map[string]release.Series
		currentVersion string
		expected       map[string]release.Series
	}{
		{
			name: "adds current release with correct predecessor",
			data: map[string]release.Series{
				"26.1": {Latest: "26.1.0", Predecessor: "25.4"},
			},
			currentVersion: "v26.2.0",
			expected: map[string]release.Series{
				"26.1": {Latest: "26.1.0", Predecessor: "25.4"},
				"26.2": {Predecessor: "26.1"},
			},
		},
		{
			name: "uses unreleased series as predecessor",
			data: map[string]release.Series{
				"26.1": {Latest: "26.1.0", Predecessor: "25.4"},
				"26.2": {Predecessor: "26.1"},
			},
			currentVersion: "v26.3.0",
			expected: map[string]release.Series{
				"26.1": {Latest: "26.1.0", Predecessor: "25.4"},
				"26.2": {Predecessor: "26.1"},
				"26.3": {Predecessor: "26.2"},
			},
		},
		{
			name: "no-op if series already exists with releases",
			data: map[string]release.Series{
				"26.1": {Latest: "26.1.0", Predecessor: "25.4"},
			},
			currentVersion: "v26.1.0",
			expected: map[string]release.Series{
				"26.1": {Latest: "26.1.0", Predecessor: "25.4"},
			},
		},
		{
			name: "ignores series higher than current version",
			data: map[string]release.Series{
				"26.1": {Latest: "26.1.0", Predecessor: "25.4"},
				"26.2": {Predecessor: "26.1"},
				"27.1": {Predecessor: "26.2"},
			},
			currentVersion: "v26.3.0",
			expected: map[string]release.Series{
				"26.1": {Latest: "26.1.0", Predecessor: "25.4"},
				"26.2": {Predecessor: "26.1"},
				"26.3": {Predecessor: "26.2"},
				"27.1": {Predecessor: "26.2"},
			},
		},
		{
			name: "overwrites unreleased entry with correct predecessor",
			data: map[string]release.Series{
				"26.1": {Latest: "26.1.0", Predecessor: "25.4"},
				"26.2": {Predecessor: "26.1"},
				"26.3": {Predecessor: "26.1"}, // stale
			},
			currentVersion: "v26.3.0",
			expected: map[string]release.Series{
				"26.1": {Latest: "26.1.0", Predecessor: "25.4"},
				"26.2": {Predecessor: "26.1"},
				"26.3": {Predecessor: "26.2"}, // corrected
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			v := version.MustParse(tc.currentVersion)
			addCurrentRelease(tc.data, &v)
			require.Equal(t, tc.expected, tc.data)
		})
	}
}

func Test_findLatestReleasedPredecessor(t *testing.T) {
	testCases := []struct {
		name     string
		data     map[string]release.Series
		series   string
		expected string
	}{
		{
			name: "direct predecessor has releases",
			data: map[string]release.Series{
				"26.1": {Latest: "26.1.0", Predecessor: "25.4"},
				"26.2": {Predecessor: "26.1"},
			},
			series:   "26.2",
			expected: "26.1.0",
		},
		{
			name: "skips unreleased intermediate series",
			data: map[string]release.Series{
				"26.1": {Latest: "26.1.0", Predecessor: "25.4"},
				"26.2": {Predecessor: "26.1"},
				"26.3": {Predecessor: "26.2"},
			},
			series:   "26.3",
			expected: "26.1.0",
		},
		{
			name: "no released predecessor",
			data: map[string]release.Series{
				"26.2": {Predecessor: "26.1"},
			},
			series:   "26.2",
			expected: "",
		},
		{
			name: "series not in data",
			data: map[string]release.Series{
				"26.1": {Latest: "26.1.0"},
			},
			series:   "99.9",
			expected: "",
		},
		{
			name: "handles cycle in predecessor chain",
			data: map[string]release.Series{
				"26.1": {Predecessor: "26.2"},
				"26.2": {Predecessor: "26.1"},
			},
			series:   "26.2",
			expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := findLatestReleasedPredecessor(tc.data, tc.series)
			require.Equal(t, tc.expected, result)
		})
	}
}

// Test_simulatePR167081 reproduces the exact scenario from PR #167081.
// On master (binary version 26.3), the docs YAML has no 26.2 releases.
// The existing cockroach_releases.yaml has a 26.2 entry (added when
// master was at 26.2). The tool should preserve 26.2, chain
// 26.3 → 26.2 → 26.1, and find the correct released versions for
// the REPOSITORIES.bzl file.
func Test_simulatePR167081(t *testing.T) {
	// Step 1: Simulate processReleaseData output — the docs YAML has
	// releases up to 26.1 but nothing for 26.2.
	downloaded := map[string]release.Series{
		"21.2": {Latest: "21.2.17"},
		"22.1": {Latest: "22.1.22", Predecessor: "21.2"},
		"22.2": {Latest: "22.2.19", Predecessor: "22.1"},
		"23.1": {Latest: "23.1.30", Predecessor: "22.2"},
		"23.2": {Latest: "23.2.29", Predecessor: "23.1"},
		"24.1": {Latest: "24.1.26", Predecessor: "23.2"},
		"24.2": {Latest: "24.2.10", Predecessor: "24.1"},
		"24.3": {Latest: "24.3.28", Predecessor: "24.2"},
		"25.1": {Latest: "25.1.10", Predecessor: "24.3"},
		"25.2": {Latest: "25.2.14", Predecessor: "25.1"},
		"25.3": {Latest: "25.3.7", Predecessor: "25.2"},
		"25.4": {Latest: "25.4.5", Predecessor: "25.3"},
		"26.1": {Latest: "26.1.0", Predecessor: "25.4"},
	}

	// Validate the downloaded data (same as production flow).
	require.NoError(t, validateReleaseData(downloaded))

	// Step 2: Simulate the existing file — it has 26.2 and 26.3 from
	// a previous run.
	existing := map[string]release.Series{
		"26.1": {Latest: "26.1.0", Predecessor: "25.4"},
		"26.2": {Predecessor: "26.1"},
		"26.3": {Predecessor: "26.1"}, // bug: was set to 26.1 before our fix
	}

	// Step 3: Preserve unreleased series.
	preserveUnreleasedSeries(downloaded, existing)

	// 26.2 should be preserved.
	require.Contains(t, downloaded, "26.2", "26.2 entry should be preserved")
	require.Equal(t, "26.1", downloaded["26.2"].Predecessor)

	// Step 4: Add current release (master is at 26.3).
	currentVersion := version.MustParse("v26.3.0")
	addCurrentRelease(downloaded, &currentVersion)

	// 26.3 should exist with predecessor 26.2 (not 26.1).
	require.Contains(t, downloaded, "26.3")
	require.Equal(t, "26.2", downloaded["26.3"].Predecessor,
		"26.3's predecessor should be 26.2, not 26.1")

	// Step 5: Verify findLatestReleasedPredecessor walks past
	// unreleased 26.2 to find 26.1.
	predecessor := findLatestReleasedPredecessor(downloaded, "26.3")
	require.Equal(t, "26.1.0", predecessor,
		"should walk past unreleased 26.2 to find 26.1.0")

	predecessorSeries := version.MustParse("v" + predecessor).Format("%X.%Y")
	prePredecessor := findLatestReleasedPredecessor(downloaded, predecessorSeries)
	require.Equal(t, "25.4.5", prePredecessor,
		"pre-predecessor should be 25.4.5")
}

// Test_collectPredecessorVersions_PR168432 reproduces the scenario from
// PR #168432 where 26.2 gets an RC release, causing the old
// two-predecessor logic to drop the 25.4 binary that
// cockroach-go-testserver-25.4 tests need.
func Test_collectPredecessorVersions_PR168432(t *testing.T) {
	data := map[string]release.Series{
		"25.3": {Latest: "25.3.7", Predecessor: "25.2"},
		"25.4": {Latest: "25.4.8", Predecessor: "25.3"},
		"26.1": {Latest: "26.1.2", Predecessor: "25.4"},
		"26.2": {Latest: "26.2.0-rc.1", Predecessor: "26.1"},
		"26.3": {Predecessor: "26.2"},
	}
	currentVersion := version.MustParse("v26.3.0")

	// With testserver configs for 25.4 and 26.1, the old logic would
	// only produce [26.1.2, 26.2.0-rc.1], missing 25.4.8. The new
	// logic walks back far enough to cover 25.4 and skips 26.2 since
	// no testserver config requires it.
	requiredSeries := map[string]bool{"25.4": true, "26.1": true}
	versions, err := collectPredecessorVersions(data, currentVersion, requiredSeries)
	require.NoError(t, err)
	require.Equal(t, []string{"25.4.8", "26.1.2"}, versions)
}

func Test_collectPredecessorVersions(t *testing.T) {
	data := map[string]release.Series{
		"25.3": {Latest: "25.3.7", Predecessor: "25.2"},
		"25.4": {Latest: "25.4.8", Predecessor: "25.3"},
		"26.1": {Latest: "26.1.2", Predecessor: "25.4"},
		"26.2": {Latest: "26.2.0-rc.1", Predecessor: "26.1"},
		"26.3": {Predecessor: "26.2"},
	}

	testCases := []struct {
		name           string
		currentVersion string
		requiredSeries map[string]bool
		expected       []string
		expectedErr    string
	}{
		{
			name:           "two predecessors without gap",
			currentVersion: "v26.3.0",
			requiredSeries: map[string]bool{"26.1": true, "25.4": true},
			// We walk past 26.2 to find 25.4, but 26.2 is not
			// included because no testserver config requires it.
			expected: []string{"25.4.8", "26.1.2"},
		},
		{
			name:           "single predecessor",
			currentVersion: "v26.3.0",
			requiredSeries: map[string]bool{"26.1": true},
			// Walks past 26.2 (not required) and stops at 26.1.
			expected: []string{"26.1.2"},
		},
		{
			name:           "unreachable series",
			currentVersion: "v26.3.0",
			requiredSeries: map[string]bool{"24.0": true},
			expectedErr:    "could not find released versions for required testserver series",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := collectPredecessorVersions(
				data, version.MustParse(tc.currentVersion), tc.requiredSeries,
			)
			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, result)
			}
		})
	}
}

func Test_validateReleaseData(t *testing.T) {
	testCases := []struct {
		name        string
		data        map[string]release.Series
		expectedErr string
	}{
		{
			name: "multiple series without a predecessor",
			data: map[string]release.Series{
				"22.2": {Latest: "22.2.1"},
				"23.1": {Latest: "23.1.12"},
			},
			expectedErr: "two release series without known predecessors",
		},
		{
			name: "missing predecessor information",
			data: map[string]release.Series{
				"22.2": {Latest: "22.2.22"},
				"23.1": {Latest: "23.1.12", Predecessor: "19.2"},
			},
			expectedErr: `predecessor of "23.1" is "19.2", but there is no release information for it`,
		},
		{
			name: "missing latest release",
			data: map[string]release.Series{
				"22.2": {Latest: "22.2.22"},
				"23.1": {Predecessor: "22.2"},
			},
			expectedErr: `release information for series "23.1" is missing the latest release`,
		},
		{
			name: "invalid latest version",
			data: map[string]release.Series{
				"2.1": {Latest: "beta-20160829"},
			},
			expectedErr: `release information for series "2.1" has invalid latest release "beta-20160829"`,
		},
		{
			name: "invalid withdrawn release",
			data: map[string]release.Series{
				"22.2": {Latest: "22.2.22"},
				"23.1": {Latest: "23.1.12", Predecessor: "22.2", Withdrawn: []string{"beta-20160829"}},
			},
			expectedErr: `release information for series "23.1" has invalid withdrawn release "beta-20160829"`,
		},
		{
			name: "all releases withdrawn",
			data: map[string]release.Series{
				"22.2": {Latest: "22.2.22"},
				"23.1": {Latest: "23.1.1", Predecessor: "22.2", Withdrawn: []string{"23.1.0", "23.1.1"}},
			},
			expectedErr: `series "23.1" is invalid: every release has been withdrawn`,
		},
		{
			name: "valid release data",
			data: map[string]release.Series{
				"22.1": {Latest: "22.1.19"},
				"22.2": {Latest: "22.2.22", Predecessor: "22.1"},
				"23.1": {Latest: "23.1.1", Predecessor: "22.2", Withdrawn: []string{"23.1.0"}},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateReleaseData(tc.data)
			if tc.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.Contains(t, err.Error(), tc.expectedErr)
			}
		})
	}
}

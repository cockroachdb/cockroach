// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package framework

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerateClusterName(t *testing.T) {
	t.Setenv("ROACHPROD_USER", "first.last")

	// Use subtests with specific names to exercise sanitization.
	// generateClusterName uses t.Name(), so the subtest name becomes part of
	// the cluster name.
	tests := []struct {
		subtestName string
		wantPrefix  string // expected prefix before the timestamp
	}{{
		subtestName: "Simple",
		// Dots stripped from username, test name lowercased.
		// t.Name() = "TestGenerateClusterName/Simple"
		// sanitized: "testgenerateclustername-simple" → truncated to 20 chars
		wantPrefix: "firstlast-testgenerateclustern",
	}, {
		subtestName: "With_Underscores",
		// underscores → dashes
		wantPrefix: "firstlast-testgenerateclustern",
	}, {
		subtestName: "With Spaces",
		// spaces → dashes (Go subtests encode spaces as underscores in t.Name())
		wantPrefix: "firstlast-testgenerateclustern",
	}}

	for _, tt := range tests {
		t.Run(tt.subtestName, func(t *testing.T) {
			name := generateClusterName(t)

			// Should start with the sanitized username.
			require.True(t, strings.HasPrefix(name, "firstlast-"),
				"name %q should start with 'firstlast-'", name)

			// Should end with a unix timestamp (digits after the last dash).
			lastDash := strings.LastIndex(name, "-")
			require.Greater(t, lastDash, 0)
			timestamp := name[lastDash+1:]
			require.NotEmpty(t, timestamp)
			for _, c := range timestamp {
				require.True(t, c >= '0' && c <= '9',
					"timestamp portion %q should be all digits", timestamp)
			}

			// The part before the timestamp should match expected prefix.
			prefix := name[:lastDash]
			require.Equal(t, tt.wantPrefix, prefix,
				"prefix mismatch for subtest %q", tt.subtestName)
		})
	}

	// Verify truncation: a long test name should be truncated to 20 chars.
	t.Run("VeryLongTestNameThatExceedsTwentyCharacters", func(t *testing.T) {
		name := generateClusterName(t)
		// Format: username-testname-timestamp
		// testname portion is truncated to 20 chars before appending
		parts := strings.SplitN(name, "-", 2) // split on first dash
		require.GreaterOrEqual(t, len(parts), 2)

		// Remove the timestamp suffix to get the test name portion
		rest := parts[1] // everything after "firstlast-"
		lastDash := strings.LastIndex(rest, "-")
		testNamePart := rest[:lastDash]
		// The sanitized test name is "testgenerateclustername-verylongtestnameth..."
		// truncated to 20 chars
		require.LessOrEqual(t, len(testNamePart), 20,
			"test name portion %q should be at most 20 chars", testNamePart)
	})
}

// TestGenerateClusterNameFormat verifies the overall format with a simple
// username (no dots).
func TestGenerateClusterNameFormat(t *testing.T) {
	t.Setenv("ROACHPROD_USER", "testuser")

	name := generateClusterName(t)

	// Format: username-sanitized_test_name-timestamp
	// t.Name() = "TestGenerateClusterNameFormat"
	// sanitized: "testgenerateclustern" (truncated to 20)
	require.True(t, strings.HasPrefix(name, "testuser-"),
		"name %q should start with 'testuser-'", name)

	// Verify the full format: username-testname-timestamp
	expectedPrefix := "testuser-testgenerateclustern"
	lastDash := strings.LastIndex(name, "-")
	prefix := name[:lastDash]
	require.Equal(t, expectedPrefix, prefix)
}

func TestExtractJSON(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{{
		name:  "clean JSON",
		input: "{\n  \"clusters\": {}\n}",
		want:  "{\n  \"clusters\": {}\n}",
	}, {
		name: "JSON preceded by warnings",
		input: `failed to create IBM provider: some error
2026/01/01 00:00:00 aws.go:1092: Failed to list AWS VMs
{
  "clusters": {}
}`,
		want: "{\n  \"clusters\": {}\n}",
	}, {
		name: "warnings containing braces",
		input: `error: unexpected response {"error": "auth failed"} from server
another warning line
{
  "clusters": {}
}`,
		want: "{\n  \"clusters\": {}\n}",
	}, {
		name: "JSON followed by trailing warnings",
		input: `{
  "clusters": {}
}
some trailing warning
another trailing line`,
		want: "{\n  \"clusters\": {}\n}",
	}, {
		name: "warnings before and after JSON",
		input: `failed to create IBM provider: some error
{
  "clusters": {}
}
trailing warning: cleanup failed`,
		want: "{\n  \"clusters\": {}\n}",
	}, {
		name:    "no JSON",
		input:   "just some error messages\nno json here",
		wantErr: true,
	}, {
		name:    "empty input",
		input:   "",
		wantErr: true,
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := extractJSON(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

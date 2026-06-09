// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSummaryWriterAppend covers the three behaviors the rest of the release
// commands rely on: a no-op when no file is configured, accumulation across
// calls, and best-effort tolerance of an unwritable path (logged, never
// panics or errors).
func TestSummaryWriterAppend(t *testing.T) {
	t.Run("no path is a no-op", func(t *testing.T) {
		// The zero value must be safe to use: commands embed summaryWriter and
		// leave it empty on local/dry runs.
		var s summaryWriter
		require.NotPanics(t, func() { s.append("ignored") })
	})

	t.Run("appends accumulate", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "summary.md")
		s := summaryWriter{path: path}
		s.append("first\n")
		s.append("second\n")
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, "first\nsecond\n", string(data))
	})

	t.Run("unwritable path is logged, not fatal", func(t *testing.T) {
		// A path whose parent is a regular file can't be opened for writing.
		// append must swallow the error rather than panic.
		file := filepath.Join(t.TempDir(), "afile")
		require.NoError(t, os.WriteFile(file, []byte("x"), 0644))
		s := summaryWriter{path: filepath.Join(file, "summary.md")}
		require.NotPanics(t, func() { s.append("block") })
	})
}

func TestShortSHA(t *testing.T) {
	tests := []struct {
		name     string
		sha      string
		expected string
	}{
		{name: "long sha truncated to 12", sha: "abcdef0123456789cafe", expected: "abcdef012345"},
		{name: "exactly 12 returned whole", sha: "abcdef012345", expected: "abcdef012345"},
		{name: "short string returned whole", sha: "abc123", expected: "abc123"},
		{name: "empty", sha: "", expected: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, shortSHA(tc.sha))
		})
	}
}

func TestCommitURL(t *testing.T) {
	require.Equal(t,
		"https://github.com/cockroachdb/cockroach/commit/deadbeef",
		commitURL("cockroachdb/cockroach", "deadbeef"))
}

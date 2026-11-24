// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestParseMochaOutput tests the Mocha output parser with real test output.
func TestParseMochaOutput(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected []mochaTestResult
	}{
		{
			name: "single level hierarchy",
			input: `
  70 passing (17s)
  1 pending
  2 failing

  1) events
       emits acquire every time a client is acquired:
     Uncaught Error: expected 0 to equal 20

  2) pool size of 1
       can only send 1 query at a time:
      Error: expected values to match
`,
			expected: []mochaTestResult{
				{name: "events => emits acquire every time a client is acquired", status: statusFail},
				{name: "pool size of 1 => can only send 1 query at a time", status: statusFail},
			},
		},
		{
			name: "multi-level hierarchy",
			input: `
  70 passing (17s)
  1 pending
  2 failing

  1) pool
       with callbacks
         removes client if it errors in background:
     Error: Timeout of 2000ms exceeded

  2) pool size of 1
       can only send 1 query at a time:
      Error: expected values to match
`,
			expected: []mochaTestResult{
				{name: "pool => with callbacks => removes client if it errors in background", status: statusFail},
				{name: "pool size of 1 => can only send 1 query at a time", status: statusFail},
			},
		},
		{
			name: "passing tests",
			input: `
    ✓ works totally unconfigured (44ms)
    ✓ passes props to clients (43ms)
`,
			expected: []mochaTestResult{
				{name: "works totally unconfigured (44ms)", status: statusPass},
				{name: "passes props to clients (43ms)", status: statusPass},
			},
		},
		{
			name: "skipped tests",
			input: `
    - is returned from the query method
    ✓ verifies a client with a callback (44ms)
`,
			expected: []mochaTestResult{
				{name: "is returned from the query method", status: statusSkip},
				{name: "verifies a client with a callback (44ms)", status: statusPass},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			results := parseMochaOutput(tc.input)
			require.Equal(t, len(tc.expected), len(results), "unexpected number of results")
			for i, expected := range tc.expected {
				require.Equal(t, expected.name, results[i].name, "test name mismatch at index %d", i)
				require.Equal(t, expected.status, results[i].status, "test status mismatch at index %d", i)
			}
		})
	}
}

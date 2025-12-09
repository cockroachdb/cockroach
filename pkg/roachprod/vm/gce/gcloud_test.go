// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gce

import (
	"io"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// At the top of sdk_instances_test.go
func newTestLogger(t *testing.T) *logger.Logger {
	cfg := logger.Config{
		Stdout: io.Discard, // or use a buffer if you want to inspect output
		Stderr: io.Discard,
	}
	l, err := cfg.NewLogger("")
	require.NoError(t, err)
	return l
}

func TestParseMachineType(t *testing.T) {
	tests := []struct {
		machineType     string
		expectedFamily  string
		expectedCores   int
		expectedMemory  int // in MB
		expectedSSDOpts string
	}{
		{
			machineType:    "n1-standard-4",
			expectedFamily: "n1",
			expectedCores:  4,
			expectedMemory: 0,
		},
		{
			machineType:    "n2-highmem-8",
			expectedFamily: "n2",
			expectedCores:  8,
			expectedMemory: 0,
		},
		{
			machineType:    "n2-custom-16-32768",
			expectedFamily: "n2",
			expectedCores:  16,
			expectedMemory: 32768,
		},
		{
			machineType:     "c4a-standard-8-lssd",
			expectedFamily:  "c4a",
			expectedCores:   8,
			expectedMemory:  0,
			expectedSSDOpts: "lssd",
		},
		{
			machineType:     "c4d-standard-384-metal",
			expectedFamily:  "c4d",
			expectedCores:   384,
			expectedMemory:  0,
			expectedSSDOpts: "",
		},
		{
			machineType:     "z3-highmem-176-standardlssd",
			expectedFamily:  "z3",
			expectedCores:   176,
			expectedMemory:  0,
			expectedSSDOpts: "standardlssd",
		},
	}
	for _, tc := range tests {
		t.Run(tc.machineType, func(t *testing.T) {
			family, _, cores, memory, ssdOpts, err := parseMachineType(tc.machineType)
			assert.NoError(t, err)
			assert.EqualValues(t, tc.expectedFamily, family)
			assert.EqualValues(t, tc.expectedCores, cores)
			assert.EqualValues(t, tc.expectedMemory, memory)
			assert.EqualValues(t, tc.expectedSSDOpts, ssdOpts)
		})
	}
}

func TestAllowedLocalSSDCount(t *testing.T) {
	for _, c := range []struct {
		machineType string
		expected    []int
		unsupported bool
	}{
		// N1 has the same ssd counts for all cpu counts.
		{"n1-standard-4", []int{1, 2, 3, 4, 5, 6, 7, 8, 16, 24}, false},
		{"n1-highcpu-64", []int{1, 2, 3, 4, 5, 6, 7, 8, 16, 24}, false},
		{"n1-highmem-96", []int{1, 2, 3, 4, 5, 6, 7, 8, 16, 24}, false},

		{"n2-standard-4", []int{1, 2, 4, 8, 16, 24}, false},
		{"n2-standard-8", []int{1, 2, 4, 8, 16, 24}, false},
		{"n2-standard-16", []int{2, 4, 8, 16, 24}, false},
		// N.B. n2-standard-30 doesn't exist, but we still get the ssd counts based on cpu count.
		{"n2-standard-30", []int{4, 8, 16, 24}, false},
		{"n2-standard-32", []int{4, 8, 16, 24}, false},
		{"n2-standard-48", []int{8, 16, 24}, false},
		{"n2-standard-64", []int{8, 16, 24}, false},
		{"n2-standard-80", []int{8, 16, 24}, false},
		{"n2-standard-96", []int{16, 24}, false},
		{"n2-standard-128", []int{16, 24}, false},

		{"c2-standard-4", []int{1, 2, 4, 8}, false},
		{"c2-standard-8", []int{1, 2, 4, 8}, false},
		{"c2-standard-16", []int{2, 4, 8}, false},
		{"c2-standard-30", []int{4, 8}, false},
		{"c2-standard-60", []int{8}, false},
		// c2-standard-64 doesn't exist and exceed cpu count, so we expect an error.
		{"c2-standard-64", nil, true},
	} {
		t.Run(c.machineType, func(t *testing.T) {
			actual, err := AllowedLocalSSDCount(c.machineType)
			if c.unsupported {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.EqualValues(t, c.expected, actual)
			}
		})
	}
}

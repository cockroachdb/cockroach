// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gce

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseMachineTypeSpecs(t *testing.T) {
	testCases := []struct {
		name    string
		raw     []string
		total   int
		want    []MachineTypeSpec
		wantErr string
	}{{
		name:  "single entry defaults to total",
		raw:   []string{"n2-standard-16"},
		total: 3,
		want: []MachineTypeSpec{
			{MachineType: "n2-standard-16", Count: 3},
		},
	}, {
		name:  "comma separated counts",
		raw:   []string{"n2-standard-16=4,n2-standard-8=2"},
		total: 6,
		want: []MachineTypeSpec{
			{MachineType: "n2-standard-16", Count: 4},
			{MachineType: "n2-standard-8", Count: 2},
		},
	}, {
		name:  "multiple flags",
		raw:   []string{"n2-standard-16=4", "n2-standard-8=2"},
		total: 6,
		want: []MachineTypeSpec{
			{MachineType: "n2-standard-16", Count: 4},
			{MachineType: "n2-standard-8", Count: 2},
		},
	}, {
		name:  "trims whitespace",
		raw:   []string{" n2-standard-16 = 2 "},
		total: 2,
		want: []MachineTypeSpec{
			{MachineType: "n2-standard-16", Count: 2},
		},
	}, {
		name:    "missing counts with multiple entries",
		raw:     []string{"n2-standard-16,n2-standard-8=2"},
		total:   6,
		wantErr: "machine type counts are required",
	}, {
		name:    "count mismatch",
		raw:     []string{"n2-standard-16=4"},
		total:   6,
		wantErr: "cover 4 nodes",
	}, {
		name:    "invalid count",
		raw:     []string{"n2-standard-16=0"},
		total:   1,
		wantErr: "invalid node count",
	}, {
		name:    "empty entry",
		raw:     []string{","},
		total:   1,
		wantErr: "entry cannot be empty",
	}}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			specs, err := ParseMachineTypeSpecs(tt.raw, tt.total)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, specs)
		})
	}
}

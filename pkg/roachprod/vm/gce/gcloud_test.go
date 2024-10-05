// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gce

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAllowedLocalSSDCount(t *testing.T) {
	for i, c := range []struct {
		machineType string
		expected    []int
		unsupported bool
	}{
		// N1 has the same ssd counts for all cpu counts.
		{"n1-standard-4", []int{1, 2, 3, 4, 5, 6, 7, 8, 16, 24}, false},
		{"n1-highcpu-64", []int{1, 2, 3, 4, 5, 6, 7, 8, 16, 24}, false},
		{"n1-higmem-96", []int{1, 2, 3, 4, 5, 6, 7, 8, 16, 24}, false},

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
		t.Run(strconv.Itoa(i), func(t *testing.T) {
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

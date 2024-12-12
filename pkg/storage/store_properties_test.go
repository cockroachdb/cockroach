// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestPathIsInside(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		base, target string
		expected     bool
	}{
		{
			base:     "/",
			target:   "/cockroach/cockroach-data",
			expected: true,
		},
		{
			base:     "/cockroach",
			target:   "/cockroach/cockroach-data",
			expected: true,
		},
		{
			base:     "/cockroach/cockroach-data",
			target:   "/cockroach/cockroach-data",
			expected: true,
		},
		{
			base:     "/cockroach/cockroach-data/foo",
			target:   "/cockroach/cockroach-data",
			expected: false,
		},
		{
			base:     "/cockroach/cockroach-data1",
			target:   "/cockroach/cockroach-data",
			expected: false,
		},
		{
			base:     "/run/user/1001",
			target:   "/cockroach/cockroach-data",
			expected: false,
		},
		{
			base:     "/..foo",
			target:   "/..foo/data",
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			result := pathIsInside(filepath.FromSlash(tc.base), filepath.FromSlash(tc.target))
			if result != tc.expected {
				t.Fatalf("%q, %q: expected %t, got %t", tc.base, tc.target, tc.expected, result)
			}
		})
	}
}

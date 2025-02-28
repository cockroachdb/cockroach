// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package iterutil_test

import (
	"maps"
	"slices"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/stretchr/testify/require"
)

func TestEnumerate(t *testing.T) {
	for name, tc := range map[string]struct {
		input []int
	}{
		"empty": {
			input: nil,
		},
		"one element": {
			input: []int{1},
		},
		"multiple elements": {
			input: []int{1, 4, 9},
		},
	} {
		t.Run(name, func(t *testing.T) {
			expected := maps.Collect(slices.All(tc.input))
			actual := maps.Collect(iterutil.Enumerate(slices.Values(tc.input)))
			require.Equal(t, expected, actual)
		})
	}
}

func TestKeys(t *testing.T) {
	for name, tc := range map[string]struct {
		input map[int]struct{}
	}{
		"empty": {
			input: nil,
		},
		"one element": {
			input: map[int]struct{}{
				1: {},
			},
		},
		"multiple elements": {
			input: map[int]struct{}{
				1: {},
				4: {},
				9: {},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			expected := slices.Collect(maps.Keys(tc.input))
			actual := slices.Collect(iterutil.Keys(maps.All(tc.input)))
			require.ElementsMatch(t, expected, actual)
		})
	}
}

func TestMinFunc(t *testing.T) {
	intCmp := func(a, b int) int {
		return a - b
	}

	for name, tc := range map[string]struct {
		input []int
	}{
		"empty": {
			input: nil,
		},
		"one element": {
			input: []int{1},
		},
		"multiple elements": {
			input: []int{1, 3, 2},
		},
		"multiple elements with zero value": {
			input: []int{1, 0, 3, 2},
		},
	} {
		t.Run(name, func(t *testing.T) {
			m := iterutil.MinFunc(slices.Values(tc.input), intCmp)
			if len(tc.input) == 0 {
				require.Equal(t, 0, m)
			} else {
				require.Equal(t, slices.MinFunc(tc.input, intCmp), m)
			}
		})
	}
}

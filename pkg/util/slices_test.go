// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCombinesUniqueInt64(t *testing.T) {
	for _, tc := range []struct {
		inputA, inputB, expected []int64
	}{{
		inputA:   []int64{1, 2, 4},
		inputB:   []int64{3, 5},
		expected: []int64{1, 2, 3, 4, 5},
	}, {
		inputA:   []int64{1, 2, 4},
		inputB:   []int64{1, 3, 4},
		expected: []int64{1, 2, 3, 4},
	}, {
		inputA:   []int64{1, 2, 3},
		inputB:   []int64{1, 2, 3},
		expected: []int64{1, 2, 3},
	}, {
		inputA:   []int64{},
		inputB:   []int64{1, 3},
		expected: []int64{1, 3},
	}, {
		inputA:   []int64{4, 4},
		inputB:   []int64{1, 1, 4, 4, 4, 5, 5},
		expected: []int64{1, 1, 4, 4, 4, 5, 5},
	}, {
		inputA:   []int64{4, 4, 4},
		inputB:   []int64{1, 1, 4, 4, 5, 5},
		expected: []int64{1, 1, 4, 4, 4, 5, 5},
	},
	} {
		output := CombineUnique(tc.inputA, tc.inputB)
		require.Equal(t, tc.expected, output)
	}
}

func TestCombinesUniqueStrings(t *testing.T) {
	for _, tc := range []struct {
		inputA, inputB, expected []string
	}{{
		inputA:   []string{"a", "b", "d"},
		inputB:   []string{"c", "e"},
		expected: []string{"a", "b", "c", "d", "e"},
	}, {
		inputA:   []string{"a", "b", "d"},
		inputB:   []string{"a", "c", "d"},
		expected: []string{"a", "b", "c", "d"},
	}, {
		inputA:   []string{"a", "b", "c"},
		inputB:   []string{"a", "b", "c"},
		expected: []string{"a", "b", "c"},
	}, {
		inputA:   []string{},
		inputB:   []string{"a", "c"},
		expected: []string{"a", "c"},
	}, {
		inputA:   []string{"a", "a", "a"},
		inputB:   []string{"b", "b", "b"},
		expected: []string{"a", "a", "a", "b", "b", "b"},
	},
	} {
		output := CombineUnique(tc.inputA, tc.inputB)
		require.Equal(t, tc.expected, output)
	}
}

func TestFilter(t *testing.T) {
	numbers := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	evens := Filter(numbers, func(i int) bool {
		return i%2 == 0
	})
	odds := Filter(numbers, func(i int) bool {
		return i%2 == 1
	})

	// Assert filtering works.
	require.Equal(t, []int{1, 3, 5, 7, 9}, odds)
	require.Equal(t, []int{0, 2, 4, 6, 8}, evens)

	// Assert/demonstrate that filtering does not mutate the original slice.
	require.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, numbers)

	// The filtered slices' capacity is correctly set and the original slice's
	// capacity is unmodified.
	require.Equal(t, len(odds), cap(odds))
	require.Equal(t, len(evens), cap(evens))
	require.Equal(t, len(numbers), cap(numbers))

	// And some weird cases.
	require.Equal(t, []int{}, Filter(nil, func(i int) bool {
		return true
	}))
	require.Equal(t, []int{}, Filter([]int{}, func(i int) bool {
		return true
	}))
	require.Equal(t, []int{}, Filter(numbers, func(i int) bool {
		return false
	}))
}

func TestMap(t *testing.T) {
	require.Equal(t, []bool{}, Map(nil, func(i int) bool {
		require.Fail(t, "should not be called")
		return true
	}))
	require.Equal(t, []bool{}, Map([]int{}, func(i int) bool {
		require.Fail(t, "should not be called")
		return true
	}))
	require.Equal(t, []bool{false, true, false, true, false}, Map([]int{1, 2, 3, 4, 5}, func(i int) bool {
		return i%2 == 0
	}))
}

func TestMapFrom(t *testing.T) {
	require.Equal(t, map[int]bool{}, MapFrom(nil, func(i int) (int, bool) {
		require.Fail(t, "should not be called")
		return 0, false
	}))
	require.Equal(t, map[int]bool{}, MapFrom([]int{}, func(i int) (int, bool) {
		require.Fail(t, "should not be called")
		return 0, false
	}))
	require.Equal(t, map[int]int{
		1: 1,
		2: 4,
		3: 9,
		4: 16,
		5: 25,
	}, MapFrom([]int{1, 1, 2, 3, 4, 5, 5}, func(i int) (int, int) {
		return i, i * i
	}))
}

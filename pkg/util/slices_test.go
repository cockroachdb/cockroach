// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package util

import (
	"testing"

	"github.com/cockroachdb/errors"
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

func TestMapErr(t *testing.T) {
	t.Run("empty slice", func(t *testing.T) {
		out, err := MapE(nil, func(i int) (int, error) {
			require.Fail(t, "should not be called")
			return 0, nil
		})
		require.NoError(t, err)
		require.Equal(t, []int{}, out)
	})

	t.Run("map to same type", func(t *testing.T) {
		out, err := MapE([]int{1, 2, 3}, func(i int) (int, error) {
			return i * 2, nil
		})
		require.NoError(t, err)
		require.Equal(t, []int{2, 4, 6}, out)
	})

	t.Run("map to different type", func(t *testing.T) {
		out, err := MapE([]int{1, 2, 3}, func(i int) (string, error) {
			return string(rune('a' + i - 1)), nil
		})
		require.NoError(t, err)
		require.Equal(t, []string{"a", "b", "c"}, out)
	})

	t.Run("error case", func(t *testing.T) {
		out, err := MapE([]int{1, 2, 3}, func(i int) (int, error) {
			if i == 2 {
				return 0, errors.New("error on 2")
			} else if i == 3 {
				require.Fail(t, "should not be called")
			}
			return i * 2, nil
		})
		require.Error(t, err)
		require.Nil(t, out)
	})
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

func TestReduce(t *testing.T) {
	t.Run("empty slice", func(t *testing.T) {
		require.Equal(t, 15, Reduce([]int{}, func(acc, i, _ int) int {
			return acc + i
		}, 15))
	})

	t.Run("reduce to same type", func(t *testing.T) {
		require.Equal(t, 15, Reduce([]int{1, 2, 3, 4, 5}, func(acc, i, _ int) int {
			return acc + i
		}, 0))
	})

	t.Run("reduce to different type", func(t *testing.T) {
		require.Equal(t, 11, Reduce([]string{"hello", "world!"}, func(acc int, i string, _ int) int {
			return acc + len(i)
		}, 0))
	})
}

func TestApplyPermutation(t *testing.T) {
	testcases := []struct {
		name        string
		input       []any
		permutation []int
		expected    []any
	}{
		{
			name:        "empty input",
			input:       []any{},
			permutation: []int{},
			expected:    []any{},
		},
		{
			name:        "single element",
			input:       []any{"a"},
			permutation: []int{0},
			expected:    []any{"a"},
		},
		{
			name:        "reversal",
			input:       []any{"a", "b", "c", "d"},
			permutation: []int{3, 2, 1, 0},
			expected:    []any{"d", "c", "b", "a"},
		},
		{
			name:        "simple swaps, no cycles",
			input:       []any{"a", "b", "c", "d"},
			permutation: []int{1, 0, 3, 2},
			expected:    []any{"b", "a", "d", "c"},
		},
		{
			name:        "permutation with cycles",
			input:       []any{"a", "b", "c", "d", "e"},
			permutation: []int{4, 0, 1, 3, 2},
			expected:    []any{"e", "a", "b", "d", "c"},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			ApplyPermutation(tc.permutation, func(i, j int) {
				tc.input[i], tc.input[j] = tc.input[j], tc.input[i]
			})
			require.Equal(t, tc.expected, tc.input)
		})
	}

}

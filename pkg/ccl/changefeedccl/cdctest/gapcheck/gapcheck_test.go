package gapcheck_test

import (
	"math/rand/v2"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest/gapcheck"

	"github.com/stretchr/testify/require"
)

func TestGapChecker_Add(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		nums []int
		err  bool
	}{
		{"empty", []int{}, false},
		{"no gaps", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, false},
		{"no gaps, reverse order", []int{9, 8, 7, 6, 5, 4, 3, 2, 1, 0}, false},
		{"with gaps, in order", []int{0, 1, 2, 4, 5, 6, 7, 8, 9, 12}, true},
		{"with gaps, reverse order", []int{9, 8, 7, 6, 5, 4, 3, 2, 1, 0}, false},
		{"with gaps, random order", []int{2, 4, 1, 3, 5, 7, 6, 8, 0, 9, 100}, true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := gapcheck.NewGapChecker()
			for _, n := range tc.nums {
				c.Add(n)
			}
			if tc.err {
				require.Error(t, c.Check())
			} else {
				require.NoError(t, c.Check())
			}
		})
	}
}

func TestGapChecker_Random(t *testing.T) {
	t.Parallel()
	const n = 100_000
	nums := make([]int, n)
	for i := 0; i < n; i++ {
		nums[i] = i
	}
	rand.Shuffle(n, func(i, j int) { nums[i], nums[j] = nums[j], nums[i] })

	c := gapcheck.NewGapChecker()
	for _, n := range nums {
		c.Add(n)
	}
	require.NoError(t, c.Check())
}

func BenchmarkGapChecker(b *testing.B) {
	nums := make([]int, b.N)
	for i := 0; i < b.N; i++ {
		nums[i] = rand.IntN(b.N)
	}
	b.ResetTimer()
	c := gapcheck.NewGapChecker()
	for i := 0; i < b.N; i++ {
		c.Add(nums[i])
	}
	_ = c.Check()
}

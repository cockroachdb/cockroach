// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package unique

import (
	"cmp"
	"math"
	"math/bits"
	"math/rand"
	"sort"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestGenerateUniqueIDOrder verifies the expected ordering of
// generateUniqueID.
func TestGenerateUniqueIDOrder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []int64{
		generateUniqueID(0, 0),
		generateUniqueID(1, 0),
		generateUniqueID(2<<15, 0),
		generateUniqueID(0, 1),
		generateUniqueID(0, 10000),
		GenerateUniqueInt(0),
	}
	prev := tests[0]
	for _, tc := range tests[1:] {
		if tc <= prev {
			t.Fatalf("%d > %d", tc, prev)
		}
	}
}

// sorterWithSwapCount implements sort.Interface and wraps a slice of data
// with a counter that tracks the number of swaps performed during sorting.
type sorterWithSwapCount[T cmp.Ordered] struct {
	data      []T
	swapCount int
}

func (s *sorterWithSwapCount[_]) Len() int {
	return len(s.data)
}

func (s *sorterWithSwapCount[_]) Less(i, j int) bool {
	return s.data[i] < s.data[j]
}

func (s *sorterWithSwapCount[_]) Swap(i, j int) {
	s.data[i], s.data[j] = s.data[j], s.data[i]
	s.swapCount += 1
}

var _ sort.Interface = &sorterWithSwapCount[int64]{}

// TestGenerateUniqueUnorderedIDOrder verifies the expected lack of ordering
// for IDs produced by GenerateUniqueUnorderedID.
func TestGenerateUniqueUnorderedIDOrder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Our null hypothesis is that the mean number of swaps required to sort a
	// slice of generated IDs is the same as the mean number of swaps required to
	// sort a slice (of the same length) of random numbers.
	//
	// We determined experimentally (by using the same test as below but with
	// rand.Int63() instead of GenerateUniqueUnorderedID and numTrials = 100000)
	// that the number of swaps required to sort a slice of 101 (numGensPerTrial)
	// random numbers fits a normal distribution with the following mean and
	// standard deviation:
	const (
		distMean   = 271
		distStdDev = 21
	)

	// We test our null hypothesis by running 100 trials and then performing
	// a z-test (https://en.wikipedia.org/wiki/Z-test) for the observed mean
	// number of swaps.
	const (
		numTrials       = 100
		numGensPerTrial = 101
	)

	// We then compare it against the critical value of +-2.58,
	// which corresponds to a confidence level of 99%.
	const (
		confidenceLevel = 0.99
		pValue          = 1 - confidenceLevel
		criticalVal     = 2.58
	)

	// Run trials.
	swapCounts := make([]int, numTrials)
	{
		for i := 0; i < numTrials; i++ {
			gens := make([]int64, numGensPerTrial)
			for i := range gens {
				gens[i] = GenerateUniqueUnorderedID(1 /* instanceID */)
			}
			s := &sorterWithSwapCount[int64]{
				data: gens,
			}
			sort.Sort(s)
			swapCounts[i] = s.swapCount
		}

		t.Logf("swap counts: %#v", swapCounts)
	}

	// Compute z-score.
	var zScore float64
	{
		swapCountsSum := 0
		for _, c := range swapCounts {
			swapCountsSum += c
		}
		swapCountsMean := float64(swapCountsSum) / float64(numTrials)

		t.Logf("mean: %f", swapCountsMean)

		stdErr := float64(distStdDev) / math.Sqrt(numTrials)
		zScore = (swapCountsMean - float64(distMean)) / stdErr

		t.Logf("z-score: %f", zScore)
	}

	// Compare z-score to critical value.
	if zScore < -criticalVal || zScore > criticalVal {
		t.Fatalf("there is evidence that the generated IDs are not unordered (p < %s)",
			strconv.FormatFloat(pValue, 'f' /* fmt */, -1 /* prec */, 64 /* bitSize */))
	}
}

func TestMapToUnorderedUniqueInt(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("preserves number of one bits", func(t *testing.T) {
		for i := 0; i < 30; i++ {
			// RandInput is [0][63 random bits].
			randInput := uint64(rand.Int63())
			output := mapToUnorderedUniqueInt(randInput)

			inputOnesCount := bits.OnesCount64(randInput)
			outputOnesCount := bits.OnesCount64(output)
			require.Equalf(t, inputOnesCount, outputOnesCount, "input: %b, output: "+
				"%b\nExpected: %d, got: %d", randInput, output, inputOnesCount,
				outputOnesCount)
		}
	})

	t.Run("correctly reverses timestamp", func(t *testing.T) {
		for name, tc := range map[string]struct {
			input    uint64
			expected uint64
		}{
			"asymmetrical timestamp": {
				input:    0b0101100000000000000000000000000000000000000000000000000000000001,
				expected: 0b0000000000000000000000000000000000000000000001101000000000000001,
			},
			"symmetrical timestamp": {
				input:    0b0100000000000000000000000000000000000000000000001000000000000101,
				expected: 0b0100000000000000000000000000000000000000000000001000000000000101,
			},
			"max timestamp": {
				input:    0b0111111111111111111111111111111111111111111111111000000000000001,
				expected: 0b0111111111111111111111111111111111111111111111111000000000000001,
			},
		} {
			t.Run(name, func(t *testing.T) {
				actual := mapToUnorderedUniqueInt(tc.input)
				require.Equalf(t,
					tc.expected, actual,
					"actual unordered unique int does not match expected:\n"+
						"%064b (expected)\n"+
						"%064b (actual)",
					tc.expected, actual,
				)
			})
		}
	})
}

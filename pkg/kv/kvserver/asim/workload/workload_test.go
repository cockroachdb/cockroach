// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package workload

import (
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const testingSeed = 1

type summaryStats struct {
	writes, reads, size int
	quartiles           [3]int
}

func quartiles(keyspace []int) [3]int {
	sort.Ints(keyspace)
	size := len(keyspace)
	return [3]int{keyspace[size/4], keyspace[size/2], keyspace[(3*size)/4]}
}

func summary(ops []LoadEvent, cycleLength int) summaryStats {
	writes := 0
	reads := 0
	sizeSum := 0
	distribution := make([]int, 0, 1)
	for _, op := range ops {
		sizeSum += int(op.ReadSize + op.WriteSize)
		writes += int(op.Writes)
		reads += int(op.Reads)
		for i := 0; i < int(op.Reads)+int(op.Writes); i++ {
			distribution = append(distribution, int(op.Key))
		}
	}
	return summaryStats{
		writes:    writes,
		reads:     reads,
		size:      sizeSum,
		quartiles: quartiles(distribution),
	}
}

// TestRandWorkloadGenerator asserts that the randomly generated load is within
// the range of possibility, given the input variables. In addition, the seed
// for each test is fixed to testingSeed, the results are asserted on
// deterministically with this assumption.
func TestRandWorkloadGenerator(t *testing.T) {
	cycleLength := 100
	testCases := []struct {
		keyGenerator      KeyGenerator
		rate              float64
		readRatio         float64
		maxSize           int
		minSize           int
		duration          time.Duration
		expectedQuartiles [3]int
		expectedReadRatio float64
	}{
		{
			keyGenerator:      NewUniformKeyGen(0, int64(cycleLength), rand.New(rand.NewSource(testingSeed))),
			rate:              10,
			readRatio:         0.75,
			maxSize:           1000,
			minSize:           100,
			duration:          1000 * time.Second,
			expectedQuartiles: [3]int{25, 49, 75},
		},
		{
			keyGenerator:      NewUniformKeyGen(100, 100+int64(cycleLength), rand.New(rand.NewSource(testingSeed))),
			rate:              10,
			readRatio:         0.75,
			maxSize:           1000,
			minSize:           100,
			duration:          1000 * time.Second,
			expectedQuartiles: [3]int{125, 149, 175},
		},
		{
			keyGenerator:      NewUniformKeyGen(0, int64(cycleLength), rand.New(rand.NewSource(testingSeed))),
			rate:              10,
			readRatio:         0.5,
			maxSize:           1000,
			minSize:           100,
			duration:          1000 * time.Second,
			expectedQuartiles: [3]int{25, 49, 75},
		},
		{
			keyGenerator:      NewUniformKeyGen(200, 200+int64(cycleLength), rand.New(rand.NewSource(testingSeed))),
			rate:              10,
			readRatio:         0.5,
			maxSize:           1000,
			minSize:           100,
			duration:          1000 * time.Second,
			expectedQuartiles: [3]int{225, 249, 275},
		},
		{
			keyGenerator:      NewZipfianKeyGen(0, int64(cycleLength), 1.1, 1, rand.New(rand.NewSource(testingSeed))),
			rate:              10,
			readRatio:         0.75,
			maxSize:           1000,
			minSize:           100,
			duration:          1000 * time.Second,
			expectedQuartiles: [3]int{1, 4, 20},
		},
		{
			keyGenerator:      NewZipfianKeyGen(300, 300+int64(cycleLength), 1.1, 1, rand.New(rand.NewSource(testingSeed))),
			rate:              10,
			readRatio:         0.75,
			maxSize:           1000,
			minSize:           100,
			duration:          1000 * time.Second,
			expectedQuartiles: [3]int{301, 304, 320},
		},
		{
			keyGenerator:      NewZipfianKeyGen(0, int64(cycleLength), 1.1, 1, rand.New(rand.NewSource(testingSeed))),
			rate:              10,
			readRatio:         0.5,
			maxSize:           1000,
			minSize:           100,
			duration:          1000 * time.Second,
			expectedQuartiles: [3]int{1, 4, 20},
		},
		{
			keyGenerator:      NewZipfianKeyGen(400, 400+int64(cycleLength), 1.1, 1, rand.New(rand.NewSource(testingSeed))),
			rate:              10,
			readRatio:         0.5,
			maxSize:           1000,
			minSize:           100,
			duration:          1000 * time.Second,
			expectedQuartiles: [3]int{401, 404, 420},
		},
	}

	start := time.Date(2022, 03, 21, 11, 0, 0, 0, time.UTC)
	for _, tc := range testCases {
		workLoadGenerator := newRandomGenerator(start, testingSeed, tc.keyGenerator, tc.rate, tc.readRatio, tc.maxSize, tc.minSize)
		workLoadGenerator.lastRun = start
		end := start.Add(tc.duration)

		// Generate the workload events.
		ops := workLoadGenerator.Tick(end)

		stats := summary(ops, cycleLength)
		count := stats.reads + stats.writes
		require.Equal(t, int(tc.rate)*(int(tc.duration)/int(time.Second)), count)
		require.GreaterOrEqual(t, float64(stats.size)/float64(count), float64(tc.minSize))
		require.LessOrEqual(t, stats.size/(count*1.0), tc.maxSize)
		require.Equal(t, tc.expectedQuartiles, stats.quartiles)
		require.Equal(t, math.Round(tc.readRatio*100), math.Round((float64(stats.reads)/float64(stats.reads+stats.writes))*100))
	}
}

// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package workload

import (
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
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
	distribution := make([]int, len(ops))
	for i, op := range ops {
		sizeSum += int(op.Size)
		distribution[i] = int(op.Key)
		if op.IsWrite {
			writes++
		} else {
			reads++
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
			keyGenerator:      NewUniformKeyGen(int64(cycleLength), rand.New(rand.NewSource(testingSeed))),
			rate:              10,
			readRatio:         0.75,
			maxSize:           1000,
			minSize:           100,
			duration:          1000 * time.Second,
			expectedQuartiles: [3]int{25, 49, 75},
		},
		{
			keyGenerator:      NewUniformKeyGen(int64(cycleLength), rand.New(rand.NewSource(testingSeed))),
			rate:              10,
			readRatio:         0.5,
			maxSize:           1000,
			minSize:           100,
			duration:          1000 * time.Second,
			expectedQuartiles: [3]int{25, 49, 75},
		},
		{
			keyGenerator:      NewZipfianKeyGen(int64(cycleLength), 1.1, 1, rand.New(rand.NewSource(testingSeed))),
			rate:              10,
			readRatio:         0.75,
			maxSize:           1000,
			minSize:           100,
			duration:          1000 * time.Second,
			expectedQuartiles: [3]int{1, 4, 20},
		},
		{
			keyGenerator:      NewZipfianKeyGen(int64(cycleLength), 1.1, 1, rand.New(rand.NewSource(testingSeed))),
			rate:              10,
			readRatio:         0.5,
			maxSize:           1000,
			minSize:           100,
			duration:          1000 * time.Second,
			expectedQuartiles: [3]int{1, 4, 20},
		},
	}

	for _, tc := range testCases {
		start := state.TestingStartTime()
		workLoadGenerator := newRandomGenerator(start, testingSeed, tc.keyGenerator, tc.rate, tc.readRatio, tc.maxSize, tc.minSize)
		workLoadGenerator.lastRun = start
		end := start.Add(tc.duration)

		done := false
		ops := make([]LoadEvent, 0)
		next := LoadEvent{}
		for !done {
			done, next = workLoadGenerator.GetNext(end)
			ops = append(ops, next)
		}
		stats := summary(ops, cycleLength)
		count := stats.reads + stats.writes
		require.Equal(t, 1+int(tc.rate)*(int(tc.duration)/int(time.Second)), count)
		require.GreaterOrEqual(t, float64(stats.size)/float64(count), float64(tc.minSize))
		require.LessOrEqual(t, stats.size/(count*1.0), tc.maxSize)
		require.Equal(t, tc.expectedQuartiles, stats.quartiles)
		require.Equal(t, math.Round(tc.readRatio*100), math.Round((float64(stats.reads)/float64(stats.reads+stats.writes))*100))
	}
}

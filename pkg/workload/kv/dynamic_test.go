// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kv

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMovingHotKeyGenerator(t *testing.T) {
	testSeed := 42

	testCases := []struct {
		desc           string
		writes         int
		reads          int
		readSkew       int64
		expectedWrites []int64
		expectedReads  []int64
	}{
		{
			desc:           "2/3 read ratio, no read tail skew",
			writes:         4,
			reads:          8,
			readSkew:       1,
			expectedWrites: []int64{2, 3, 4, 5},
			expectedReads:  []int64{1, 0, 3, 2, 2, 3, 5, 4},
		},
		{
			desc:           "1/3 read ratio, no read tail skew",
			writes:         8,
			reads:          4,
			readSkew:       1,
			expectedWrites: []int64{1, 2, 3, 4, 5, 6, 7, 8},
			expectedReads:  []int64{1, 1, 3, 3},
		},
		{
			desc:           "9/10 read ratio, no read tail skew",
			writes:         1,
			reads:          9,
			readSkew:       1,
			expectedWrites: []int64{9},
			expectedReads:  []int64{7, 2, 8, 5, 1, 7, 9, 7, 7},
		},
		{
			desc:           "1/10 read ratio, no read tail skew",
			writes:         9,
			reads:          1,
			readSkew:       1,
			expectedWrites: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
			expectedReads:  []int64{1},
		},
		{
			desc:           "1/2 read ratio, 10x read tail skew",
			writes:         5,
			reads:          5,
			readSkew:       10,
			expectedWrites: []int64{10, 11, 12, 13, 14},
			expectedReads:  []int64{8, 3, 11, 8, 5},
		},
		{
			desc:           "1/3 read ratio, 10x read tail skew",
			writes:         8,
			reads:          4,
			readSkew:       10,
			expectedWrites: []int64{10, 11, 12, 13, 14, 15, 16, 17},
			expectedReads:  []int64{8, 3, 11, 8},
		},
		{
			desc:           "9/10 read ratio, 10x read tail skew",
			writes:         1,
			reads:          9,
			readSkew:       10,
			expectedWrites: []int64{90},
			expectedReads:  []int64{81, 31, 88, 66, 22, 82, 90, 82, 82},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			readPercent := int(float64(tc.reads) / float64(tc.writes+tc.reads) * 100)
			seq := &sequence{config: &kv{readPercent: readPercent, cycleLength: 10000, readSkew: tc.readSkew, zipfian: true}}
			random := rand.New(rand.NewSource(int64(testSeed)))

			gen := movingHotKeyGenerator(seq, random)

			writeResults := make([]int64, tc.writes)
			readResults := make([]int64, tc.reads)

			for i := range writeResults {
				writeResults[i] = gen.writeKey()
			}

			for i := range readResults {
				readResults[i] = gen.readKey()
			}

			require.Equal(t, tc.expectedWrites, writeResults, "write keys: Expected keys do not the actual keys")
			require.Equal(t, tc.expectedReads, readResults, "read keys: Expected keys do not the actual keys")
		})
	}
}

func TestPeriodicGenerator(t *testing.T) {
	testSeed := 42

	testCases := []struct {
		desc           string
		writes         int
		reads          int
		cycle          int
		interval       time.Duration
		intervals      int
		expectedWrites []int64
		expectedReads  []int64
	}{
		{
			desc:           "2 intervals, [0,2) -> [4,6)",
			writes:         8,
			reads:          8,
			cycle:          8,
			interval:       time.Second,
			intervals:      2,
			expectedWrites: []int64{0, 1, 0, 0, 4, 5, 5, 5},
			expectedReads:  []int64{0, 1, 0, 0, 4, 5, 5, 5},
		},
		{
			desc:           "4 intervals, [0,2) -> [4,6)",
			writes:         8,
			reads:          8,
			cycle:          8,
			interval:       time.Second,
			intervals:      4,
			expectedWrites: []int64{0, 1, 4, 4, 0, 1, 5, 5},
			expectedReads:  []int64{0, 1, 4, 4, 0, 1, 5, 5},
		},
		{
			desc:           "2 intervals, [0,25) -> [50,75)",
			writes:         8,
			reads:          8,
			cycle:          100,
			interval:       time.Second,
			intervals:      2,
			expectedWrites: []int64{8, 8, 24, 16, 51, 72, 66, 50},
			expectedReads:  []int64{8, 8, 24, 16, 51, 72, 66, 50},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			seq := &sequence{config: &kv{cycleLength: int64(tc.cycle), dynamicInterval: tc.interval.String()}}
			start := time.Date(2022, 03, 21, 11, 0, 0, 0, time.UTC)
			random := rand.New(rand.NewSource(int64(testSeed)))

			gen := periodicKeyGenerator(seq, random)

			// Disable wall clock time, we will manually tick time for testing.
			gen.now = start
			gen.testingBlockTime = true
			gen.read.setLastMap(start)
			gen.write.setLastMap(start)

			writeResults := make([]int64, tc.writes)
			readResults := make([]int64, tc.reads)

			totalTime := (tc.interval * time.Duration(tc.intervals))
			writeAdd := totalTime / time.Duration(tc.writes)
			readAdd := totalTime / time.Duration(tc.reads)

			for i := range writeResults {
				gen.now = start.Add(writeAdd * time.Duration(i))
				writeResults[i] = gen.writeKey()
			}

			for i := range readResults {
				gen.now = start.Add(readAdd * time.Duration(i))
				readResults[i] = gen.readKey()
			}

			require.Equal(t, tc.expectedWrites, writeResults, "write keys: Expected keys do not the actual keys")
			require.Equal(t, tc.expectedReads, readResults, "read keys: Expected keys do not the actual keys")
		})
	}
}

func TestShuffleGenerator(t *testing.T) {
	testSeed := 42

	testCases := []struct {
		desc           string
		writes         int
		reads          int
		cycle          int
		chunk          int64
		interval       time.Duration
		intervals      int
		expectedWrites []int64
		expectedReads  []int64
	}{
		{
			desc:           "2 shuffles 4/8 chunks",
			writes:         4,
			reads:          4,
			cycle:          8,
			chunk:          4,
			interval:       time.Second,
			intervals:      2,
			expectedWrites: []int64{4, 7, 6, 4},
			expectedReads:  []int64{4, 7, 6, 4},
		},
		{
			desc:           "4 shuffles 2/8 chunks",
			writes:         4,
			reads:          4,
			cycle:          8,
			chunk:          2,
			interval:       time.Second,
			intervals:      4,
			expectedWrites: []int64{4, 3, 0, 2},
			expectedReads:  []int64{4, 3, 0, 2},
		},
		{
			desc:           "8 shuffles 1/8 chunks",
			writes:         4,
			reads:          4,
			cycle:          8,
			chunk:          1,
			interval:       time.Second,
			intervals:      8,
			expectedWrites: []int64{4, 0, 5, 2},
			expectedReads:  []int64{4, 0, 5, 2},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			seq := &sequence{config: &kv{cycleLength: int64(tc.cycle), dynamicInterval: tc.interval.String(), shuffleChunk: tc.chunk}}
			start := time.Date(2022, 03, 21, 11, 0, 0, 0, time.UTC)
			random := rand.New(rand.NewSource(int64(testSeed)))

			gen := shuffleGenerator(seq, random)

			// Disable wall clock time, we will manually tick time for testing.
			gen.now = start
			gen.testingBlockTime = true
			gen.read.setLastMap(start)
			gen.write.setLastMap(start)

			writeResults := make([]int64, tc.writes)
			readResults := make([]int64, tc.reads)

			totalTime := (tc.interval * time.Duration(tc.intervals))
			writeAdd := totalTime / time.Duration(tc.writes)
			readAdd := totalTime / time.Duration(tc.reads)

			for i := range writeResults {
				gen.now = start.Add(writeAdd * time.Duration(i))
				writeResults[i] = gen.writeKey()
			}

			for i := range readResults {
				gen.now = start.Add(readAdd * time.Duration(i))
				readResults[i] = gen.readKey()
			}

			require.Equal(t, tc.expectedWrites, writeResults, "write keys: Expected keys do not the actual keys")
			require.Equal(t, tc.expectedReads, readResults, "read keys: Expected keys do not the actual keys")
		})
	}
}

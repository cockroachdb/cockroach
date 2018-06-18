// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package heapprofiler

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"time"

	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/assert"
)

func TestPercentSystemMemoryHeuristic(t *testing.T) {
	rssValues := []int64{30, 40, 70, 20, 80, 10, 4, 90}
	expectedScores := []int64{70, 80, 90}
	numProfiles := 0
	mockHeapProfile := func(
		ctx context.Context, dir string, prefix string, suffix string,
	) {
		assert.Equal(t, prefix, "memprof.percent_system_memory.")
		score, err := strconv.ParseInt(strings.Split(suffix, "_")[0], 10, 64)
		assert.Nil(t, err)
		assert.Equal(t, expectedScores[numProfiles], score)
		numProfiles++
	}
	rssChan := make(chan int64)
	hp := heapProfiler{
		stats:                      &stats{systemMemory: 100},
		heuristics:                 []heuristic{percentSystemMemoryHeuristic},
		rssChan:                    rssChan,
		takeHeapProfile:            mockHeapProfile,
		timeDifferenceBetweenDumps: time.Nanosecond,
	}
	stopper := stop.NewStopper()
	stopper.RunWorker(context.Background(), hp.getWorker(stopper))
	for _, rss := range rssValues {
		rssChan <- rss
	}
	stopper.Stop(context.Background())
	assert.Equal(t, numProfiles, len(expectedScores))
}

func TestLastMinuteIncreaseHeuristic(t *testing.T) {
	rssValues := []int64{30, 40, 70, 20, (1 << 32) + 20, 78}
	expectedScores := []int64{1 << 32}
	expectedPrefixes := []string{"memprof.last_minute_increase."}
	numProfiles := 0
	mockHeapProfile := func(
		ctx context.Context, dir string, prefix string, suffix string,
	) {
		assert.Equal(t, prefix, expectedPrefixes[numProfiles])
		score, err := strconv.ParseInt(strings.Split(suffix, "_")[0], 10, 64)
		assert.Nil(t, err)
		assert.Equal(t, expectedScores[numProfiles], score)
		numProfiles++
	}
	rssChan := make(chan int64)
	hp := heapProfiler{
		stats:           &stats{},
		heuristics:      []heuristic{lastMinuteIncreaseHeuristic},
		rssChan:         rssChan,
		takeHeapProfile: mockHeapProfile,
		dir:             "",
		timeDifferenceBetweenDumps: time.Nanosecond,
	}
	stopper := stop.NewStopper()
	stopper.RunWorker(context.Background(), hp.getWorker(stopper))
	for _, rss := range rssValues {
		rssChan <- rss
	}
	stopper.Stop(context.Background())
	assert.Equal(t, numProfiles, len(expectedScores))
}

func TestAllHeuristics(t *testing.T) {
	rssValues := []int64{30, 40, 70, 20 + (1 << 32), 1<<34 + 1<<33, 10, 4, 90}
	expectedScores := []int64{(1 << 32) - 10}
	expectedPrefixes := []string{"memprof.last_minute_increase."}
	numProfiles := 0
	mockHeapProfile := func(
		ctx context.Context, dir string, prefix string, suffix string,
	) {
		assert.Equal(t, prefix, expectedPrefixes[numProfiles])
		score, err := strconv.ParseInt(strings.Split(suffix, "_")[0], 10, 64)
		assert.Nil(t, err)
		assert.Equal(t, expectedScores[numProfiles], score)
		numProfiles++
	}
	rssChan := make(chan int64)
	hp := heapProfiler{
		stats:                      &stats{systemMemory: 1 << 35},
		heuristics:                 []heuristic{percentSystemMemoryHeuristic, lastMinuteIncreaseHeuristic},
		rssChan:                    rssChan,
		takeHeapProfile:            mockHeapProfile,
		timeDifferenceBetweenDumps: timeDifferenceBetweenProfiles,
	}
	stopper := stop.NewStopper()
	stopper.RunWorker(context.Background(), hp.getWorker(stopper))
	for _, rss := range rssValues {
		rssChan <- rss
	}
	stopper.Stop(context.Background())
	assert.Equal(t, numProfiles, len(expectedScores))
}

func TestAllHeuristicsSmallInterval(t *testing.T) {
	rssValues := []int64{30, 40, 70, 20 + (1 << 32), 1<<34 + 1<<33, 10, 4, 1 << 31}
	expectedScores := []int64{(1 << 32) - 10, 1<<34 + 1<<33, 1<<31 - 4}
	lmi := "memprof.last_minute_increase."
	sp := "memprof.percent_system_memory."
	expectedPrefixes := []string{lmi, sp, lmi}
	numProfiles := 0
	mockHeapProfile := func(
		ctx context.Context, dir string, prefix string, suffix string,
	) {
		assert.Equal(t, prefix, expectedPrefixes[numProfiles])
		score, err := strconv.ParseInt(strings.Split(suffix, "_")[0], 10, 64)
		assert.Nil(t, err)
		assert.Equal(t, expectedScores[numProfiles], score)
		numProfiles++
	}
	rssChan := make(chan int64)
	hp := heapProfiler{
		stats:                      &stats{systemMemory: 1 << 35},
		heuristics:                 []heuristic{percentSystemMemoryHeuristic, lastMinuteIncreaseHeuristic},
		rssChan:                    rssChan,
		takeHeapProfile:            mockHeapProfile,
		timeDifferenceBetweenDumps: time.Nanosecond,
	}
	stopper := stop.NewStopper()
	stopper.RunWorker(context.Background(), hp.getWorker(stopper))
	for _, rss := range rssValues {
		rssChan <- rss
	}
	stopper.Stop(context.Background())
	assert.Equal(t, numProfiles, len(expectedScores))
}

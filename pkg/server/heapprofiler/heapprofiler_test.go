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

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/assert"
)

type rssVal struct {
	secs time.Duration // secs is the time at which this rss value was emitted
	rss  int64
}

func testHelper(
	t *testing.T,
	hp *heapProfiler,
	st *cluster.Settings,
	rssValues []rssVal,
	expectedScores []int64,
	expectedPrefixes []string,
) {
	baseTime := time.Time{}
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
	var currentTime time.Time
	var timeLocker syncutil.RWMutex
	now := func() time.Time {
		timeLocker.RLock()
		defer timeLocker.RUnlock()
		return currentTime
	}
	rssChan := make(chan RssVal)
	hp.rssChan = rssChan
	hp.takeHeapProfile = mockHeapProfile
	hp.currentTime = now
	// set a large negative time so that first profile is triggered correctly
	// since we start time from 0 in test.
	// Not needed in main code as time will never be 0.
	hp.lastProfileTime = time.Time{}.Add(-1000 * time.Second)

	stopper := stop.NewStopper()
	stopper.RunWorker(context.Background(), hp.getWorker(stopper, st))

	for _, r := range rssValues {
		v := RssVal{baseTime.Add(time.Second * r.secs), r.rss}
	Sender:
		for {
			timeLocker.Lock()
			select {
			case rssChan <- v:
				currentTime = v.Timestamp
				timeLocker.Unlock()
				break Sender
			default:
			}
			timeLocker.Unlock()
		}
	}
	stopper.Stop(context.Background())
	close(rssChan)
	assert.Equal(t, numProfiles, len(expectedScores))
}

func TestPercentSystemMemoryHeuristic(t *testing.T) {
	rssValues := []rssVal{
		{0, 30}, {20, 40}, // random small values
		{30, 88},            // should trigger
		{120, 89},           // should not trigger as less than 100s before last profile
		{130, 10}, {140, 4}, // random small values
		{150, 90}, // should trigger
		{260, 92}, // should trigger
		{340, 91}, // should not trigger as less than 100s before last profile
		{480, 99}, // should trigger
	}
	expectedScores := []int64{88, 90, 92, 99}
	prefix := "memprof.percent_system_memory."
	expectedPrefixes := []string{prefix, prefix, prefix, prefix}
	hp := &heapProfiler{
		stats: &stats{
			systemMemory:      100,
			numPreviousValues: 7,
		},
		heuristics: []heuristic{percentSystemMemoryHeuristic},
	}
	st := &cluster.Settings{}
	minProfileInterval.Override(&st.SV, time.Second*100)
	percentSystemMemoryThreshold.Override(&st.SV, 85)
	testHelper(t, hp, st, rssValues, expectedScores, expectedPrefixes)
}

func TestLastMinuteIncreaseHeuristic(t *testing.T) {
	rssValues := []rssVal{
		{0, 30}, {20, 40}, // random small values
		{30, 120},           // should not trigger
		{50, 189},           // should trigger with score (189-30) = 159
		{120, 10}, {130, 4}, // random small values
		{140, 190}, // should not trigger as less than 100s before last profile
		{150, 90},  // should not trigger as below threshold
		{260, 192}, // should not trigger as no other data point in last minute
		{450, 91},  // should not trigger as less than 100s before last profile
		{480, 199}, // should trigger with score (199-91) = 108
	}
	expectedScores := []int64{159, 108}
	prefix := "memprof.last_minute_increase."
	expectedPrefixes := []string{prefix, prefix}
	hp := &heapProfiler{
		stats: &stats{
			numPreviousValues: 7,
		},
		heuristics: []heuristic{lastMinuteIncreaseHeuristic},
	}
	st := &cluster.Settings{}
	minProfileInterval.Override(&st.SV, time.Second*100)
	lastMinuteIncreaseThreshold.Override(&st.SV, 100)
	testHelper(t, hp, st, rssValues, expectedScores, expectedPrefixes)
}

func TestMultipleHeuristics(t *testing.T) {
	rssValues := []rssVal{
		{0, 10}, {20, 40}, // random small values
		{30, 180},  // both matching, should trigger only last_minute_increase
		{210, 180}, // should trigger only percent_system_increase
		{270, 40},  // should not trigger
		{320, 150}, // should trigger only last_minute_increase
	}
	expectedScores := []int64{170, 180, 110}
	expectedPrefixes := []string{
		"memprof.last_minute_increase.",
		"memprof.percent_system_memory.",
		"memprof.last_minute_increase.",
	}
	hp := &heapProfiler{
		stats: &stats{
			systemMemory:      200,
			numPreviousValues: 7,
		},
		heuristics: []heuristic{lastMinuteIncreaseHeuristic, percentSystemMemoryHeuristic},
	}
	st := &cluster.Settings{}
	minProfileInterval.Override(&st.SV, time.Second*100)
	percentSystemMemoryThreshold.Override(&st.SV, 85)
	lastMinuteIncreaseThreshold.Override(&st.SV, 100)
	testHelper(t, hp, st, rssValues, expectedScores, expectedPrefixes)
}

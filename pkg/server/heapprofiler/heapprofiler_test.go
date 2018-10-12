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
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/assert"
)

type rssVal struct {
	secs time.Duration // secs is the time at which this rss value was emitted
	rss  int64
}

func testHelper(
	t *testing.T,
	hp *HeapProfiler,
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
	hp.takeHeapProfile = mockHeapProfile
	hp.currentTime = now
	// set a large negative time so that first profile is triggered correctly
	// since we start time from 0 in test.
	// Not needed in main code as time will never be 0.
	hp.lastProfileTime = time.Time{}.Add(-1000 * time.Second)

	ctx := context.TODO()
	for _, r := range rssValues {
		currentTime = baseTime.Add(time.Second * r.secs)
		hp.MaybeTakeProfile(ctx, st, r.rss)
	}
	assert.Equal(t, numProfiles, len(expectedScores))
}

func TestPercentSystemMemoryHeuristic(t *testing.T) {
	rssValues := []rssVal{
		{0, 30}, {20, 40}, // random small values
		{30, 88},            // should trigger
		{80, 89},            // should not trigger as less than 60s before last profile
		{130, 10}, {140, 4}, // random small values
		{150, 90}, // should trigger
		{260, 92}, // should not trigger as continues above threshold
		{290, 30}, // random small value
		{380, 99}, // should trigger
		{390, 30}, // random small value
		{430, 91}, // should not trigger as less than 60s before last profile
		{500, 95}, // should trigger
	}
	expectedScores := []int64{88, 90, 99, 95}
	prefix := "memprof.fraction_system_memory."
	expectedPrefixes := []string{prefix, prefix, prefix, prefix}
	hp := &HeapProfiler{
		stats:      &stats{systemMemory: 100},
		heuristics: []heuristic{fractionSystemMemoryHeuristic},
	}
	st := &cluster.Settings{}
	systemMemoryThresholdFraction.Override(&st.SV, .85)
	testHelper(t, hp, st, rssValues, expectedScores, expectedPrefixes)
}

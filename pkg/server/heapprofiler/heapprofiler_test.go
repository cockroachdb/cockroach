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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/assert"
)

type rssVal struct {
	secs        time.Duration // secs is the time at which this rss value was emitted
	rssBytes    uint64
	goIdleBytes uint64
}

func testHelper(
	ctx context.Context,
	t *testing.T,
	hp *HeapProfiler,
	st *cluster.Settings,
	rssValues []rssVal,
	expectedScores []int64,
) {
	baseTime := time.Time{}
	numProfiles := 0
	mockHeapProfile := func(
		ctx context.Context, dir string, prefix string, suffix string,
	) {
		score, err := strconv.ParseInt(strings.Split(suffix, "_")[0], 10, 64)
		assert.Nil(t, err)
		if len(expectedScores) <= numProfiles {
			t.Fatalf("unexected profile: %d", numProfiles)
		}
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
	hp.now = now
	// set a large negative time so that first profile is triggered correctly
	// since we start time from 0 in test.
	// Not needed in main code as time will never be 0.
	hp.lastProfileTime = time.Time{}.Add(-1000 * time.Second)

	for _, r := range rssValues {
		currentTime = baseTime.Add(time.Second * r.secs)
		ms := base.MemStats{RSSBytes: r.rssBytes}
		ms.Go.GoIdle = r.goIdleBytes
		hp.MaybeTakeProfile(ctx, st, ms)
	}
	assert.Equal(t, numProfiles, len(expectedScores))
}

func TestPercentSystemMemoryHeuristic(t *testing.T) {
	rssValues := []rssVal{
		{0, 30, 0}, {20, 40, 0}, // random small values
		{30, 88, 0},               // should trigger
		{80, 89, 0},               // should not trigger as less than 60s before last profile
		{130, 10, 0}, {140, 4, 0}, // random small values
		{150, 90, 0},   // should trigger
		{290, 30, 0},   // random small value
		{380, 99, 0},   // should trigger
		{390, 30, 0},   // random small value
		{430, 91, 0},   // should not trigger as less than 60s before last profile
		{500, 95, 0},   // should trigger
		{600, 30, 0},   // random small value
		{700, 100, 90}, // should not trigger; large idle heap which is discounted
		{700, 100, 10}, // should trigger; some idle heap but not big enough to matter
	}
	expectedScores := []int64{88, 90, 99, 95, 90}
	hp := &HeapProfiler{
		stats: &stats{systemMemoryBytes: 100},
	}
	st := &cluster.Settings{}
	systemMemoryThresholdFraction.Override(&st.SV, .85)
	testHelper(context.Background(), t, hp, st, rssValues, expectedScores)
}

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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/stretchr/testify/assert"
)

func TestHeapProfiler(t *testing.T) {
	ctx := context.Background()
	type test struct {
		secs         int // The measurement's timestamp.
		goTotalBytes uint64
		goIdleBytes  uint64

		expProfile bool
	}
	tests := []test{
		{0, 30, 0, true},  // we always take the first profile
		{10, 40, 0, true}, // new high-water mark
		{20, 30, 0, false},
		{30, 50, 20, false}, // large GoTotal but also large GoIdle
		{4000, 10, 0, true}, // new hour; should trigger regardless of the usage
	}
	var currentTime time.Time
	now := func() time.Time {
		return currentTime
	}

	var tookProfile bool
	hp, err := NewHeapProfiler("" /* dir */, cluster.MakeTestingClusterSettings())
	if err != nil {
		t.Fatal(err)
	}
	hp.knobs = testingKnobs{
		now:               now,
		dontWriteProfiles: true,
		maybeTakeProfileHook: func(willTakeProfile bool) {
			tookProfile = willTakeProfile
		},
	}

	for i, r := range tests {
		currentTime = (time.Time{}).Add(time.Second * time.Duration(r.secs))
		ms := base.MemStats{}
		ms.Go.GoTotal = r.goTotalBytes
		ms.Go.GoIdle = r.goIdleBytes
		hp.MaybeTakeProfile(ctx, ms)
		assert.Equal(t, r.expProfile, tookProfile, i)
	}
}

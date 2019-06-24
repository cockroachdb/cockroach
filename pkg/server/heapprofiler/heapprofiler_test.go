// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package heapprofiler

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/stretchr/testify/assert"
)

func TestHeapProfiler(t *testing.T) {
	ctx := context.Background()
	type test struct {
		secs      int // The measurement's timestamp.
		heapBytes uint64

		expProfile bool
	}
	tests := []test{
		{0, 30, true},    // we always take the first profile
		{10, 40, true},   // new high-water mark
		{20, 30, false},  // below high-water mark; no profile
		{4000, 10, true}, // new hour; should trigger regardless of the usage
	}
	var currentTime time.Time
	now := func() time.Time {
		return currentTime
	}

	var tookProfile bool
	hp, err := NewHeapProfiler("dummy_dir", cluster.MakeTestingClusterSettings())
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

		// Initialize enough of ms for the purposes of the HeapProfiler.
		var ms runtime.MemStats
		ms.HeapAlloc = r.heapBytes

		hp.MaybeTakeProfile(ctx, ms)
		assert.Equal(t, r.expProfile, tookProfile, i)
	}
}

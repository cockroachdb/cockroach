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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestProfiler(t *testing.T) {
	ctx := context.Background()
	type test struct {
		secs      int // The measurement's timestamp.
		heapBytes int64

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
	hp := profiler{
		knobs: testingKnobs{
			now:               now,
			dontWriteProfiles: true,
			maybeTakeProfileHook: func(willTakeProfile bool) {
				tookProfile = willTakeProfile
			},
		}}

	for i, r := range tests {
		currentTime = (time.Time{}).Add(time.Second * time.Duration(r.secs))

		hp.maybeTakeProfile(ctx, r.heapBytes, nil)
		assert.Equal(t, r.expProfile, tookProfile, i)
	}
}

// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package profiler

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
		{0, 30, true},   // we always take the first profile
		{10, 40, true},  // new high-water mark
		{20, 30, false}, // below high-water mark; no profile
		// NB: resetInterval == 30s.
		{4000, 10, true}, // new hour; should trigger regardless of the usage
	}
	var currentTime time.Time
	now := func() time.Time {
		return currentTime
	}

	var tookProfile bool
	hp := makeProfiler(
		nil, // store
		zeroFloor,
		func() time.Duration { return 30 * time.Second },
	)
	hp.knobs = testingKnobs{
		now:               now,
		dontWriteProfiles: true,
		maybeTakeProfileHook: func(willTakeProfile bool) {
			tookProfile = willTakeProfile
		},
	}

	for i, r := range tests {
		currentTime = (time.Time{}).Add(time.Second * time.Duration(r.secs))

		hp.maybeTakeProfile(ctx, r.heapBytes, nil)
		assert.Equal(t, r.expProfile, tookProfile, i)
	}
}

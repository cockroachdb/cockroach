// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestReplicaGCShouldQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ts := func(t time.Duration) hlc.Timestamp {
		return hlc.Timestamp{WallTime: t.Nanoseconds()}
	}

	base := 2 * (ReplicaGCQueueSuspectTimeout + ReplicaGCQueueInactivityThreshold)
	var (
		z       = ts(0)
		bTS     = ts(base)
		cTS     = ts(base + ReplicaGCQueueSuspectTimeout)
		cTSnext = ts(base + ReplicaGCQueueSuspectTimeout + 1)
		uTSprev = ts(base + ReplicaGCQueueUninitializedTimeout - 1)
		uTS     = ts(base + ReplicaGCQueueUninitializedTimeout)
		uTSnext = ts(base + ReplicaGCQueueUninitializedTimeout + 1)
		iTSprev = ts(base + ReplicaGCQueueInactivityThreshold - 1)
		iTS     = ts(base + ReplicaGCQueueInactivityThreshold)
	)
	cfg := makeReplicaGCQueueConfig(nil)
	for i, test := range []struct {
		now, lastCheck, lastActivity hlc.Timestamp
		isCandidate, isInitialized   bool

		shouldQ  bool
		priority float64
	}{
		// Test outcomes when range is in candidate state.

		// All timestamps current: candidacy plays no role.
		{now: z, lastCheck: z, lastActivity: z, isCandidate: true, isInitialized: true, shouldQ: false, priority: 0},
		// Threshold: no action taken.
		{now: cTS, lastCheck: z, lastActivity: bTS, isCandidate: true, isInitialized: true, shouldQ: false, priority: 0},
		// Queue with priority.
		{now: cTSnext, lastCheck: z, lastActivity: bTS, isCandidate: true, isInitialized: true, shouldQ: true, priority: 1},
		// Last processed recently: candidate still gets processed eagerly.
		{now: cTSnext, lastCheck: bTS, lastActivity: z, isCandidate: true, isInitialized: true, shouldQ: true, priority: 1},
		// Last processed recently: non-candidate stays put.
		{now: cTSnext, lastCheck: bTS, lastActivity: z, isCandidate: false, isInitialized: true, shouldQ: false, priority: 0},
		// Still no effect until iTS reached.
		{now: iTSprev, lastCheck: bTS, lastActivity: z, isCandidate: false, isInitialized: true, shouldQ: false, priority: 0},
		{now: iTS, lastCheck: bTS, lastActivity: z, isCandidate: true, isInitialized: true, shouldQ: true, priority: 1},
		// Verify again that candidacy increases priority.
		{now: iTS, lastCheck: bTS, lastActivity: z, isCandidate: false, isInitialized: true, shouldQ: true, priority: 0},
		// Verify that uninitialized replicas will get cleaned up after lastActivity is old enough.
		{now: uTS, lastCheck: z, lastActivity: bTS, isCandidate: false, isInitialized: false, shouldQ: false, priority: 0},
		{now: uTSnext, lastCheck: z, lastActivity: bTS, isCandidate: false, isInitialized: false, shouldQ: true, priority: 1},
		// Verify that uninitialized replicas aren't cleaned up if lastCheck was too recent.
		{now: uTSprev, lastCheck: bTS, lastActivity: z, isCandidate: false, isInitialized: false, shouldQ: false, priority: 0},
		{now: uTS, lastCheck: bTS, lastActivity: z, isCandidate: false, isInitialized: false, shouldQ: true, priority: 1},
	} {
		if sq, pr := replicaGCShouldQueueImpl(
			cfg, test.now, test.lastCheck, test.lastActivity, test.isCandidate, test.isInitialized,
		); sq != test.shouldQ || pr != test.priority {
			t.Errorf("%d: %+v: got (%t,%f)", i, test, sq, pr)
		}
	}
}

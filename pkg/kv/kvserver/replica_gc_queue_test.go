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
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestReplicaGCShouldQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ts := func(t time.Duration) hlc.Timestamp {
		return hlc.Timestamp{WallTime: t.Nanoseconds()}
	}

	base := 2 * (ReplicaGCQueueSuspectTimeout + ReplicaGCQueueInactivityThreshold)
	var (
		z       = ts(0)
		bTS     = ts(base)
		sTS     = ts(base + ReplicaGCQueueSuspectTimeout)
		sTSnext = ts(base + ReplicaGCQueueSuspectTimeout + 1)
		iTS     = ts(base + ReplicaGCQueueInactivityThreshold)
		iTSnext = ts(base + ReplicaGCQueueInactivityThreshold + 1)
	)

	for i, test := range []struct {
		now, lastCheck, lastActivity hlc.Timestamp
		isSuspect                    bool

		shouldQ  bool
		priority float64
	}{
		// Test outcomes when range is in suspect state.

		// All timestamps current: suspect plays no role.
		{now: z, lastCheck: z, lastActivity: z, isSuspect: true, shouldQ: false, priority: 0},
		// Threshold: no action taken.
		{now: sTS, lastCheck: z, lastActivity: bTS, isSuspect: true, shouldQ: false, priority: 0},
		// Queue with priority.
		{now: sTSnext, lastCheck: z, lastActivity: bTS, isSuspect: true, shouldQ: true, priority: 1},
		// Last processed recently: suspect still gets processed eagerly.
		{now: sTSnext, lastCheck: bTS, lastActivity: z, isSuspect: true, shouldQ: true, priority: 1},
		// Last processed recently: non-suspect stays put.
		{now: sTSnext, lastCheck: bTS, lastActivity: z, isSuspect: false, shouldQ: false, priority: 0},
		// Still no effect until iTS crossed.
		{now: iTS, lastCheck: bTS, lastActivity: z, isSuspect: false, shouldQ: false, priority: 0},
		{now: iTSnext, lastCheck: bTS, lastActivity: z, isSuspect: false, shouldQ: true, priority: 0},
		// Verify again that candidacy increases priority.
		{now: iTSnext, lastCheck: bTS, lastActivity: z, isSuspect: true, shouldQ: true, priority: 1},
	} {
		if sq, pr := replicaGCShouldQueueImpl(
			test.now, test.lastCheck, test.lastActivity, test.isSuspect,
		); sq != test.shouldQ || pr != test.priority {
			t.Errorf("%d: %+v: got (%t,%f)", i, test, sq, pr)
		}
	}
}

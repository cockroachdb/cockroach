// Copyright 2016 The Cockroach Authors.
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
//
// Author: Tobias Schottdorf (tobias@cockroachlabs.com)

package storage

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestReplicaGCShouldQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ts := func(t time.Duration) hlc.Timestamp {
		return hlc.ZeroTimestamp.Add(t.Nanoseconds(), 0)
	}

	base := 2 * (ReplicaGCQueueCandidateTimeout + ReplicaGCQueueInactivityThreshold)
	var (
		z       = ts(0)
		bTS     = ts(base)
		cTS     = ts(base + ReplicaGCQueueCandidateTimeout)
		cTSnext = ts(base + ReplicaGCQueueCandidateTimeout + 1)
		iTSprev = ts(base + ReplicaGCQueueInactivityThreshold - 1)
		iTS     = ts(base + ReplicaGCQueueInactivityThreshold)
	)

	for i, test := range []struct {
		now, lastCheck, lastActivity hlc.Timestamp
		isCandidate                  bool

		shouldQ  bool
		priority float64
	}{
		// Test outcomes when range is in candidate state.

		// All timestamps current: candidacy plays no role.
		{now: z, lastCheck: z, lastActivity: z, isCandidate: true, shouldQ: false, priority: 0},
		// Threshold: no action taken.
		{now: cTS, lastCheck: z, lastActivity: bTS, isCandidate: true, shouldQ: false, priority: 0},
		// Queue with priority.
		{now: cTSnext, lastCheck: z, lastActivity: bTS, isCandidate: true, shouldQ: true, priority: 1},
		// Last processed recently: candidate still gets processed eagerly.
		{now: cTSnext, lastCheck: bTS, lastActivity: z, isCandidate: true, shouldQ: true, priority: 1},
		// Last processed recently: non-candidate stays put.
		{now: cTSnext, lastCheck: bTS, lastActivity: z, isCandidate: false, shouldQ: false, priority: 0},
		// Still no effect until iTS reached.
		{now: iTSprev, lastCheck: bTS, lastActivity: z, isCandidate: false, shouldQ: false, priority: 0},
		{now: iTS, lastCheck: bTS, lastActivity: z, isCandidate: true, shouldQ: true, priority: 1},
		// Verify again that candidacy increases priority.
		{now: iTS, lastCheck: bTS, lastActivity: z, isCandidate: false, shouldQ: true, priority: 0},
	} {
		if sq, pr := replicaGCShouldQueueImpl(
			test.now, test.lastCheck, test.lastActivity, test.isCandidate,
		); sq != test.shouldQ || pr != test.priority {
			t.Errorf("%d: %+v: got (%t,%f)", i, test, sq, pr)
		}
	}
}

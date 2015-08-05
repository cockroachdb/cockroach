// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Kenji Kaneda (kenji.kaneda@gmail.com)

package storage_test

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// TestRangeGCQueueDropReplica verifies that a removed replica is
// immediately cleaned up.
func TestRangeGCQueueDropReplica(t *testing.T) {
	defer leaktest.AfterTest(t)

	mtc := startMultiTestContext(t, 3)
	defer mtc.Stop()

	raftID := proto.RangeID(1)
	mtc.replicateRange(raftID, 0, 1, 2)
	mtc.unreplicateRange(raftID, 0, 1)

	// Make sure the range is removed from the store.
	util.SucceedsWithin(t, time.Second, func() error {
		if _, err := mtc.stores[1].GetRange(raftID); !testutils.IsError(err, "range .* was not found") {
			return util.Error("expected range removal")
		}
		return nil
	})

	// Restart the store to tear down the test cleanly.
	mtc.stopStore(1)
	mtc.restartStore(1)
}

// TestRangeGCQueueDropReplicaOnScan verifies that the range GC queue
// removes a range from a store that no longer should have a replica.
func TestRangeGCQueueDropReplicaGCOnScan(t *testing.T) {
	defer leaktest.AfterTest(t)

	mtc := startMultiTestContext(t, 3)
	defer mtc.Stop()
	// Disable the range gc queue to prevent direct removal of range.
	mtc.stores[1].DisableRangeGCQueue(true)

	raftID := proto.RangeID(1)
	mtc.replicateRange(raftID, 0, 1, 2)
	mtc.unreplicateRange(raftID, 0, 1)

	// Wait long enough for the direct range GC to have had a chance and been
	// discarded because the queue is disabled.
	time.Sleep(10 * time.Millisecond)
	if _, err := mtc.stores[1].GetRange(raftID); err != nil {
		t.Error("unexpected range removal")
	}

	// Enable the queue.
	mtc.stores[1].DisableRangeGCQueue(false)

	// Increment the clock's timestamp to make the range GC queue process the range.
	mtc.manualClock.Increment(int64(storage.RangeGCQueueInactivityThreshold+storage.DefaultLeaderLeaseDuration) + 1)

	// Make sure the range is removed from the store.
	util.SucceedsWithin(t, time.Second, func() error {
		store := mtc.stores[1]
		store.ForceRangeGCScan(t)
		if _, err := store.GetRange(raftID); !testutils.IsError(err, "range .* was not found") {
			return util.Errorf("expected range removal: %s", err)
		}
		return nil
	})

	// Restart the store to tear down the test cleanly.
	mtc.stopStore(1)
	mtc.restartStore(1)
}

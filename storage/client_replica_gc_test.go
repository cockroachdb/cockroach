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
// permissions and limitations under the License.
//
// Author: Kenji Kaneda (kenji.kaneda@gmail.com)

package storage_test

import (
	"errors"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/testutils/storageutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// TestReplicaGCQueueDropReplica verifies that a removed replica is
// immediately cleaned up.
func TestReplicaGCQueueDropReplicaDirect(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := &multiTestContext{}
	const numStores = 3
	rangeID := roachpb.RangeID(1)

	// In this test, the Replica on the second Node is removed, and the test
	// verifies that that Node adds this Replica to its RangeGCQueue. However,
	// the queue does a consistent lookup which will usually be read from
	// Node 1. Hence, if Node 1 hasn't processed the removal when Node 2 has,
	// no GC will take place since the consistent RangeLookup hits the first
	// Node. We use the TestingCommandFilter to make sure that the second Node
	// waits for the first.
	ctx := storage.TestStoreContext()
	mtc.storeContext = &ctx
	mtc.storeContext.TestingKnobs.TestingCommandFilter =
		func(filterArgs storageutils.FilterArgs) *roachpb.Error {
			et, ok := filterArgs.Req.(*roachpb.EndTransactionRequest)
			if !ok || filterArgs.Sid != 2 {
				return nil
			}
			rct := et.InternalCommitTrigger.GetChangeReplicasTrigger()
			if rct == nil || rct.ChangeType != roachpb.REMOVE_REPLICA {
				return nil
			}
			util.SucceedsSoon(t, func() error {
				r, err := mtc.stores[0].GetReplica(rangeID)
				if err != nil {
					return err
				}
				if i, _ := r.Desc().FindReplica(2); i >= 0 {
					return errors.New("expected second node gone from first node's known replicas")
				}
				return nil
			})
			return nil
		}

	mtc.Start(t, numStores)
	defer mtc.Stop()

	mtc.replicateRange(rangeID, 1, 2)
	mtc.unreplicateRange(rangeID, 1)

	// Make sure the range is removed from the store.
	util.SucceedsSoon(t, func() error {
		if _, err := mtc.stores[1].GetReplica(rangeID); !testutils.IsError(err, "range .* was not found") {
			return util.Errorf("expected range removal")
		}
		return nil
	})
}

// TestReplicaGCQueueDropReplicaOnScan verifies that the range GC queue
// removes a range from a store that no longer should have a replica.
func TestReplicaGCQueueDropReplicaGCOnScan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mtc := startMultiTestContext(t, 3)
	defer mtc.Stop()
	// Disable the replica gc queue to prevent direct removal of replica.
	mtc.stores[1].DisableReplicaGCQueue(true)

	rangeID := roachpb.RangeID(1)
	mtc.replicateRange(rangeID, 1, 2)
	mtc.unreplicateRange(rangeID, 1)

	// Wait long enough for the direct replica GC to have had a chance and been
	// discarded because the queue is disabled.
	time.Sleep(10 * time.Millisecond)
	if _, err := mtc.stores[1].GetReplica(rangeID); err != nil {
		t.Error("unexpected range removal")
	}

	// Enable the queue.
	mtc.stores[1].DisableReplicaGCQueue(false)

	// Increment the clock's timestamp to make the replica GC queue process the range.
	mtc.expireLeaderLeases()
	mtc.manualClock.Increment(int64(storage.ReplicaGCQueueInactivityThreshold + 1))

	// Make sure the range is removed from the store.
	util.SucceedsSoon(t, func() error {
		store := mtc.stores[1]
		store.ForceReplicaGCScanAndProcess()
		if _, err := store.GetReplica(rangeID); !testutils.IsError(err, "range .* was not found") {
			return util.Errorf("expected range removal: %s", err)
		}
		return nil
	})
}

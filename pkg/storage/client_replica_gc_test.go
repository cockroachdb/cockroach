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
	"testing"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
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
	cfg := storage.TestStoreConfig(nil)
	mtc.storeConfig = &cfg
	mtc.storeConfig.TestingKnobs.TestingCommandFilter =
		func(filterArgs storagebase.FilterArgs) *roachpb.Error {
			et, ok := filterArgs.Req.(*roachpb.EndTransactionRequest)
			if !ok || filterArgs.Sid != 2 {
				return nil
			}
			crt := et.InternalCommitTrigger.GetChangeReplicasTrigger()
			if crt == nil || crt.ChangeType != roachpb.REMOVE_REPLICA {
				return nil
			}
			testutils.SucceedsSoon(t, func() error {
				r, err := mtc.stores[0].GetReplica(rangeID)
				if err != nil {
					return err
				}
				if _, ok := r.Desc().GetReplicaDescriptor(2); ok {
					return errors.New("expected second node gone from first node's known replicas")
				}
				return nil
			})
			return nil
		}

	defer mtc.Stop()
	mtc.Start(t, numStores)

	mtc.replicateRange(rangeID, 1, 2)
	mtc.unreplicateRange(rangeID, 1)

	// Make sure the range is removed from the store.
	testutils.SucceedsSoon(t, func() error {
		if _, err := mtc.stores[1].GetReplica(rangeID); !testutils.IsError(err, "range .* was not found") {
			return errors.Errorf("expected range removal: %v", err) // NB: errors.Wrapf(nil, ...) returns nil.
		}
		return nil
	})
}

// TestReplicaGCQueueDropReplicaOnScan verifies that the range GC queue
// removes a range from a store that no longer should have a replica.
func TestReplicaGCQueueDropReplicaGCOnScan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mtc := &multiTestContext{}
	defer mtc.Stop()
	mtc.Start(t, 3)
	// Disable the replica gc queue to prevent direct removal of replica.
	mtc.stores[1].SetReplicaGCQueueActive(false)

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
	mtc.stores[1].SetReplicaGCQueueActive(true)

	// Increment the clock's timestamp to make the replica GC queue process the range.
	mtc.advanceClock(context.TODO())
	mtc.manualClock.Increment(int64(storage.ReplicaGCQueueInactivityThreshold + 1))

	// Make sure the range is removed from the store.
	testutils.SucceedsSoon(t, func() error {
		store := mtc.stores[1]
		store.ForceReplicaGCScanAndProcess()
		if _, err := store.GetReplica(rangeID); !testutils.IsError(err, "range .* was not found") {
			return errors.Errorf("expected range removal: %v", err) // NB: errors.Wrapf(nil, ...) returns nil.
		}
		return nil
	})
}

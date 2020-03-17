// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/storagebase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/pkg/errors"
)

// TestReplicaGCQueueDropReplica verifies that a removed replica is
// immediately cleaned up.
func TestReplicaGCQueueDropReplicaDirect(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := &multiTestContext{}
	const numStores = 3
	rangeID := roachpb.RangeID(1)

	// Use actual engines (not in memory) because the in-mem ones don't write
	// to disk. The test would still pass if we didn't do this except it
	// would probably look at an empty sideloaded directory and fail.
	tempDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	cache := storage.NewRocksDBCache(1 << 20)
	defer cache.Release()
	for i := 0; i < 3; i++ {
		eng, err := storage.NewRocksDB(storage.RocksDBConfig{
			StorageConfig: base.StorageConfig{
				Dir: filepath.Join(tempDir, strconv.Itoa(i)),
			},
		}, cache)
		if err != nil {
			t.Fatal(err)
		}
		defer eng.Close()
		mtc.engines = append(mtc.engines, eng)
	}

	// In this test, the Replica on the second Node is removed, and the test
	// verifies that that Node adds this Replica to its RangeGCQueue. However,
	// the queue does a consistent lookup which will usually be read from
	// Node 1. Hence, if Node 1 hasn't processed the removal when Node 2 has,
	// no GC will take place since the consistent RangeLookup hits the first
	// Node. We use the TestingEvalFilter to make sure that the second Node
	// waits for the first.
	cfg := kvserver.TestStoreConfig(nil)
	mtc.storeConfig = &cfg
	mtc.storeConfig.TestingKnobs.EvalKnobs.TestingEvalFilter =
		func(filterArgs storagebase.FilterArgs) *roachpb.Error {
			et, ok := filterArgs.Req.(*roachpb.EndTxnRequest)
			if !ok || filterArgs.Sid != 2 {
				return nil
			}
			crt := et.InternalCommitTrigger.GetChangeReplicasTrigger()
			if crt == nil || crt.DeprecatedChangeType != roachpb.REMOVE_REPLICA {
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

	{
		repl1, err := mtc.stores[1].GetReplica(rangeID)
		if err != nil {
			t.Fatal(err)
		}

		// Put some bogus sideloaded data on the replica which we're about to
		// remove. Then, at the end of the test, check that that sideloaded
		// storage is now empty (in other words, GC'ing the Replica took care of
		// cleanup).
		repl1.RaftLock()
		dir := repl1.SideloadedRaftMuLocked().Dir()
		repl1.RaftUnlock()

		if dir == "" {
			t.Fatal("no sideloaded directory")
		}
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
		if err := ioutil.WriteFile(filepath.Join(dir, "i1000000.t100000"), []byte("foo"), 0644); err != nil {
			t.Fatal(err)
		}

		defer func() {
			if !t.Failed() {
				testutils.SucceedsSoon(t, func() error {
					// Verify that the whole directory for the replica is gone.
					repl1.RaftLock()
					dir := repl1.SideloadedRaftMuLocked().Dir()
					repl1.RaftUnlock()
					_, err := os.Stat(dir)

					if os.IsNotExist(err) {
						return nil
					}
					return errors.Errorf("replica still has sideloaded files despite GC: %v", err)
				})
			}
		}()
	}

	mtc.unreplicateRange(rangeID, 1)

	// Make sure the range is removed from the store.
	testutils.SucceedsSoon(t, func() error {
		if _, err := mtc.stores[1].GetReplica(rangeID); !testutils.IsError(err, "r[0-9]+ was not found") {
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
	cfg := kvserver.TestStoreConfig(nil)
	cfg.TestingKnobs.DisableEagerReplicaRemoval = true
	cfg.Clock = nil // manual clock
	mtc.storeConfig = &cfg

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
	mtc.manualClock.Increment(int64(kvserver.ReplicaGCQueueInactivityThreshold + 1))

	// Make sure the range is removed from the store.
	testutils.SucceedsSoon(t, func() error {
		store := mtc.stores[1]
		store.MustForceReplicaGCScanAndProcess()
		if _, err := store.GetReplica(rangeID); !testutils.IsError(err, "r[0-9]+ was not found") {
			return errors.Errorf("expected range removal: %v", err) // NB: errors.Wrapf(nil, ...) returns nil.
		}
		return nil
	})
}

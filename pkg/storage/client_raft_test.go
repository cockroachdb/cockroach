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
// Author: Ben Darnell

package storage_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// mustGetInt decodes an int64 value from the bytes field of the receiver
// and panics if the bytes field is not 0 or 8 bytes in length.
func mustGetInt(v *roachpb.Value) int64 {
	if v == nil {
		return 0
	}
	i, err := v.GetInt()
	if err != nil {
		panic(err)
	}
	return i
}

// TestStoreRecoverFromEngine verifies that the store recovers all ranges and their contents
// after being stopped and recreated.
func TestStoreRecoverFromEngine(t *testing.T) {
	defer leaktest.AfterTest(t)()
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true

	const rangeID = roachpb.RangeID(1)
	splitKey := roachpb.Key("m")
	key1 := roachpb.Key("a")
	key2 := roachpb.Key("z")

	engineStopper := stop.NewStopper()
	defer engineStopper.Stop()
	eng := engine.NewInMem(roachpb.Attributes{}, 1<<20)
	engineStopper.AddCloser(eng)
	var rangeID2 roachpb.RangeID

	get := func(store *storage.Store, rangeID roachpb.RangeID, key roachpb.Key) int64 {
		args := getArgs(key)
		resp, err := client.SendWrappedWith(context.Background(), rg1(store), roachpb.Header{
			RangeID: rangeID,
		}, &args)
		if err != nil {
			t.Fatal(err)
		}
		return mustGetInt(resp.(*roachpb.GetResponse).Value)
	}
	validate := func(store *storage.Store) {
		if val := get(store, rangeID, key1); val != 13 {
			t.Errorf("key %q: expected 13 but got %v", key1, val)
		}
		if val := get(store, rangeID2, key2); val != 28 {
			t.Errorf("key %q: expected 28 but got %v", key2, val)
		}
	}

	// First, populate the store with data across two ranges. Each range contains commands
	// that both predate and postdate the split.
	func() {
		stopper := stop.NewStopper()
		defer stopper.Stop()
		store := createTestStoreWithEngine(t, eng, true, storeCfg, stopper)

		increment := func(rangeID roachpb.RangeID, key roachpb.Key, value int64) (*roachpb.IncrementResponse, *roachpb.Error) {
			args := incrementArgs(key, value)
			resp, err := client.SendWrappedWith(context.Background(), rg1(store), roachpb.Header{
				RangeID: rangeID,
			}, &args)
			incResp, _ := resp.(*roachpb.IncrementResponse)
			return incResp, err
		}

		if _, err := increment(rangeID, key1, 2); err != nil {
			t.Fatal(err)
		}
		if _, err := increment(rangeID, key2, 5); err != nil {
			t.Fatal(err)
		}
		splitArgs := adminSplitArgs(roachpb.KeyMin, splitKey)
		if _, err := client.SendWrapped(context.Background(), rg1(store), &splitArgs); err != nil {
			t.Fatal(err)
		}
		rangeID2 = store.LookupReplica(roachpb.RKey(key2), nil).RangeID
		if rangeID2 == rangeID {
			t.Fatal("got same range id after split")
		}
		if _, err := increment(rangeID, key1, 11); err != nil {
			t.Fatal(err)
		}
		if _, err := increment(rangeID2, key2, 23); err != nil {
			t.Fatal(err)
		}
		validate(store)
	}()

	// Now create a new store with the same engine and make sure the expected data is present.
	// We must use the same clock because a newly-created manual clock will be behind the one
	// we wrote with and so will see stale MVCC data.
	store := createTestStoreWithEngine(t, eng, false, storeCfg, engineStopper)

	// Raft processing is initialized lazily; issue a no-op write request on each key to
	// ensure that is has been started.
	incArgs := incrementArgs(key1, 0)
	if _, err := client.SendWrapped(context.Background(), rg1(store), &incArgs); err != nil {
		t.Fatal(err)
	}
	incArgs = incrementArgs(key2, 0)
	if _, err := client.SendWrappedWith(context.Background(), rg1(store), roachpb.Header{
		RangeID: rangeID2,
	}, &incArgs); err != nil {
		t.Fatal(err)
	}

	validate(store)
}

// TestStoreRecoverWithErrors verifies that even commands that fail are marked as
// applied so they are not retried after recovery.
func TestStoreRecoverWithErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	storeCfg := storage.TestStoreConfig(nil)
	eng := engine.NewInMem(roachpb.Attributes{}, 1<<20)
	defer eng.Close()

	numIncrements := 0

	func() {
		stopper := stop.NewStopper()
		defer stopper.Stop()
		storeCfg := storeCfg // copy
		storeCfg.TestingKnobs.TestingCommandFilter =
			func(filterArgs storagebase.FilterArgs) *roachpb.Error {
				_, ok := filterArgs.Req.(*roachpb.IncrementRequest)
				if ok && filterArgs.Req.Header().Key.Equal(roachpb.Key("a")) {
					numIncrements++
				}
				return nil
			}
		store := createTestStoreWithEngine(t, eng, true, storeCfg, stopper)

		// Write a bytes value so the increment will fail.
		putArgs := putArgs(roachpb.Key("a"), []byte("asdf"))
		if _, err := client.SendWrapped(context.Background(), rg1(store), &putArgs); err != nil {
			t.Fatal(err)
		}

		// Try and fail to increment the key. It is important for this test that the
		// failure be the last thing in the raft log when the store is stopped.
		incArgs := incrementArgs(roachpb.Key("a"), 42)
		if _, err := client.SendWrapped(context.Background(), rg1(store), &incArgs); err == nil {
			t.Fatal("did not get expected error")
		}
	}()

	if numIncrements != 1 {
		t.Fatalf("expected 1 increments; was %d", numIncrements)
	}

	stopper := stop.NewStopper()
	defer stopper.Stop()

	// Recover from the engine.
	store := createTestStoreWithEngine(t, eng, false, storeCfg, stopper)

	// Issue a no-op write to lazily initialize raft on the range.
	incArgs := incrementArgs(roachpb.Key("b"), 0)
	if _, err := client.SendWrapped(context.Background(), rg1(store), &incArgs); err != nil {
		t.Fatal(err)
	}

	// No additional increments were performed on key A during recovery.
	if numIncrements != 1 {
		t.Fatalf("expected 1 increments; was %d", numIncrements)
	}
}

// TestReplicateRange verifies basic replication functionality by creating two stores
// and a range, replicating the range to the second store, and reading its data there.
func TestReplicateRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := startMultiTestContext(t, 2)
	defer mtc.Stop()

	// Issue a command on the first node before replicating.
	incArgs := incrementArgs([]byte("a"), 5)
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &incArgs); err != nil {
		t.Fatal(err)
	}

	repl, err := mtc.stores[0].GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	if err := repl.ChangeReplicas(
		context.Background(),
		roachpb.ADD_REPLICA,
		roachpb.ReplicaDescriptor{
			NodeID:  mtc.stores[1].Ident.NodeID,
			StoreID: mtc.stores[1].Ident.StoreID,
		},
		repl.Desc(),
	); err != nil {
		t.Fatal(err)
	}
	// Verify no intent remains on range descriptor key.
	key := keys.RangeDescriptorKey(repl.Desc().StartKey)
	desc := roachpb.RangeDescriptor{}
	if ok, err := engine.MVCCGetProto(context.Background(), mtc.stores[0].Engine(), key, mtc.stores[0].Clock().Now(), true, nil, &desc); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatalf("range descriptor key %s was not found", key)
	}
	// Verify that in time, no intents remain on meta addressing
	// keys, and that range descriptor on the meta records is correct.
	util.SucceedsSoon(t, func() error {
		meta2, err := keys.Addr(keys.RangeMetaKey(roachpb.RKeyMax))
		if err != nil {
			t.Fatal(err)
		}
		meta1, err := keys.Addr(keys.RangeMetaKey(meta2))
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range []roachpb.RKey{meta2, meta1} {
			metaDesc := roachpb.RangeDescriptor{}
			if ok, err := engine.MVCCGetProto(context.Background(), mtc.stores[0].Engine(), key.AsRawKey(), mtc.stores[0].Clock().Now(), true, nil, &metaDesc); err != nil {
				return err
			} else if !ok {
				return errors.Errorf("failed to resolve %s", key.AsRawKey())
			}
			if !reflect.DeepEqual(metaDesc, desc) {
				return errors.Errorf("descs not equal: %+v != %+v", metaDesc, desc)
			}
		}
		return nil
	})

	// Verify that the same data is available on the replica.
	util.SucceedsSoon(t, func() error {
		getArgs := getArgs([]byte("a"))
		if reply, err := client.SendWrappedWith(context.Background(), rg1(mtc.stores[1]), roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, &getArgs); err != nil {
			return errors.Errorf("failed to read data: %s", err)
		} else if e, v := int64(5), mustGetInt(reply.(*roachpb.GetResponse).Value); v != e {
			return errors.Errorf("failed to read correct data: expected %d, got %d", e, v)
		}
		return nil
	})
}

// TestRestoreReplicas ensures that consensus group membership is properly
// persisted to disk and restored when a node is stopped and restarted.
func TestRestoreReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sc := storage.TestStoreConfig(nil)
	// Disable periodic gossip activities. The periodic gossiping of the first
	// range can cause spurious lease transfers which cause this test to fail.
	sc.TestingKnobs.DisablePeriodicGossips = true
	// Allow a replica to use the lease it had before a restart; we don't want
	// this test to deal with needing to acquire new leases after the restart.
	sc.TestingKnobs.DontPreventUseOfOldLeaseOnStart = true
	mtc := &multiTestContext{storeConfig: &sc}
	mtc.Start(t, 2)
	defer mtc.Stop()

	firstRng, err := mtc.stores[0].GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	// Perform an increment before replication to ensure that commands are not
	// repeated on restarts.
	incArgs := incrementArgs([]byte("a"), 23)
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &incArgs); err != nil {
		t.Fatal(err)
	}

	if err := firstRng.ChangeReplicas(
		context.Background(),
		roachpb.ADD_REPLICA,
		roachpb.ReplicaDescriptor{
			NodeID:  mtc.stores[1].Ident.NodeID,
			StoreID: mtc.stores[1].Ident.StoreID,
		},
		firstRng.Desc(),
	); err != nil {
		t.Fatal(err)
	}

	// TODO(bdarnell): use the stopper.Quiesce() method. The problem
	// right now is that raft isn't creating a task for high-level work
	// it's creating while snapshotting and catching up. Ideally we'll
	// be able to capture that and then can just invoke
	// mtc.stopper.Quiesce() here.

	// TODO(bdarnell): initial creation and replication needs to be atomic;
	// cutting off the process too soon currently results in a corrupted range.
	time.Sleep(500 * time.Millisecond)

	mtc.restart()

	// Send a command on each store. The original store (the lease holder still)
	// will succeed.
	incArgs = incrementArgs([]byte("a"), 5)
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &incArgs); err != nil {
		t.Fatal(err)
	}
	// The follower will return a not lease holder error, indicating the command
	// should be forwarded to the lease holder.
	incArgs = incrementArgs([]byte("a"), 11)
	{
		_, pErr := client.SendWrapped(context.Background(), rg1(mtc.stores[1]), &incArgs)
		if _, ok := pErr.GetDetail().(*roachpb.NotLeaseHolderError); !ok {
			t.Fatalf("expected not lease holder error; got %s", pErr)
		}
	}
	// Send again, this time to first store.
	if _, pErr := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &incArgs); pErr != nil {
		t.Fatal(pErr)
	}

	util.SucceedsSoon(t, func() error {
		getArgs := getArgs([]byte("a"))
		if reply, err := client.SendWrappedWith(context.Background(), rg1(mtc.stores[1]), roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, &getArgs); err != nil {
			return errors.Errorf("failed to read data: %s", err)
		} else if e, v := int64(39), mustGetInt(reply.(*roachpb.GetResponse).Value); v != e {
			return errors.Errorf("failed to read correct data: expected %d, got %d", e, v)
		}
		return nil
	})

	// Both replicas have a complete list in Desc.Replicas
	for i, store := range mtc.stores {
		repl, err := store.GetReplica(1)
		if err != nil {
			t.Fatal(err)
		}
		desc := repl.Desc()
		if len(desc.Replicas) != 2 {
			t.Fatalf("store %d: expected 2 replicas, found %d", i, len(desc.Replicas))
		}
		if desc.Replicas[0].NodeID != mtc.stores[0].Ident.NodeID {
			t.Errorf("store %d: expected replica[0].NodeID == %d, was %d",
				i, mtc.stores[0].Ident.NodeID, desc.Replicas[0].NodeID)
		}
	}
}

// TODO(bdarnell): more aggressive testing here; especially with
// proposer-evaluated KV, what this test does is much less as it doesn't
// exercise the path in which the replica change fails at *apply* time (we only
// test the failfast path), in which case the replica change isn't even
// proposed.
func TestFailedReplicaChange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var runFilter atomic.Value
	runFilter.Store(true)

	sc := storage.TestStoreConfig(nil)
	sc.TestingKnobs.TestingCommandFilter = func(filterArgs storagebase.FilterArgs) *roachpb.Error {
		if runFilter.Load().(bool) {
			if et, ok := filterArgs.Req.(*roachpb.EndTransactionRequest); ok && et.Commit {
				return roachpb.NewErrorWithTxn(errors.Errorf("boom"), filterArgs.Hdr.Txn)
			}
		}
		return nil
	}
	mtc := &multiTestContext{storeConfig: &sc}
	mtc.Start(t, 2)
	defer mtc.Stop()

	repl, err := mtc.stores[0].GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	if err := repl.ChangeReplicas(
		context.Background(),
		roachpb.ADD_REPLICA,
		roachpb.ReplicaDescriptor{
			NodeID:  mtc.stores[1].Ident.NodeID,
			StoreID: mtc.stores[1].Ident.StoreID,
		},
		repl.Desc(),
	); !testutils.IsError(err, "boom") {
		t.Fatalf("did not get expected error: %v", err)
	}

	// After the aborted transaction, r.Desc was not updated.
	// TODO(bdarnell): expose and inspect raft's internal state.
	if replicas := repl.Desc().Replicas; len(replicas) != 1 {
		t.Fatalf("expected 1 replica, found %v", replicas)
	}

	// The pending config change flag was cleared, so a subsequent attempt
	// can succeed.
	runFilter.Store(false)

	// The first failed replica change has laid down intents. Make sure those
	// are pushable by making the transaction abandoned.
	mtc.manualClock.Increment(10 * base.DefaultHeartbeatInterval.Nanoseconds())

	if err := repl.ChangeReplicas(
		context.Background(),
		roachpb.ADD_REPLICA,
		roachpb.ReplicaDescriptor{
			NodeID:  mtc.stores[1].Ident.NodeID,
			StoreID: mtc.stores[1].Ident.StoreID,
		},
		repl.Desc(),
	); err != nil {
		t.Fatal(err)
	}

	// Wait for the range to sync to both replicas (mainly so leaktest doesn't
	// complain about goroutines involved in the process).
	util.SucceedsSoon(t, func() error {
		for _, store := range mtc.stores {
			rang, err := store.GetReplica(1)
			if err != nil {
				return err
			}
			if replicas := rang.Desc().Replicas; len(replicas) <= 1 {
				return errors.Errorf("expected > 1 replicas; got %v", replicas)
			}
		}
		return nil
	})
}

// Test that the preemptive snapshot has been released before the associated
// transaction for ChangeReplicas operation is run.
func TestPreemptiveSnapshotReleasedAfterApply(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var mtc *multiTestContext
	sc := storage.TestStoreConfig(nil)
	sc.TestingKnobs.TestingCommandFilter = func(filterArgs storagebase.FilterArgs) *roachpb.Error {
		if et, ok := filterArgs.Req.(*roachpb.EndTransactionRequest); ok && et.Commit {
			if !mtc.stores[filterArgs.Sid-1].AcquireRaftSnapshot() {
				// We couldn't acquire the store snapshot which means the preemptive
				// snapshot was not released. Return an error in order to fail the
				// test.
				return roachpb.NewErrorWithTxn(errors.Errorf("boom"), filterArgs.Hdr.Txn)
			}
		}
		return nil
	}
	mtc = &multiTestContext{storeConfig: &sc}
	mtc.Start(t, 2)
	defer mtc.Stop()

	mtc.replicateRange(1, 1)
}

// We can truncate the old log entries and a new replica will be brought up from a snapshot.
func TestReplicateAfterTruncation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := startMultiTestContext(t, 2)
	defer mtc.Stop()

	repl, err := mtc.stores[0].GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	// Issue a command on the first node before replicating.
	incArgs := incrementArgs([]byte("a"), 5)
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &incArgs); err != nil {
		t.Fatal(err)
	}

	// Get that command's log index.
	index, err := repl.GetLastIndex()
	if err != nil {
		t.Fatal(err)
	}

	// Truncate the log at index+1 (log entries < N are removed, so this includes
	// the increment).
	truncArgs := truncateLogArgs(index+1, 1)
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &truncArgs); err != nil {
		t.Fatal(err)
	}

	// Issue a second command post-truncation.
	incArgs = incrementArgs([]byte("a"), 11)
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &incArgs); err != nil {
		t.Fatal(err)
	}

	// Now add the second replica.
	if err := repl.ChangeReplicas(
		context.Background(),
		roachpb.ADD_REPLICA,
		roachpb.ReplicaDescriptor{
			NodeID:  mtc.stores[1].Ident.NodeID,
			StoreID: mtc.stores[1].Ident.StoreID,
		},
		repl.Desc(),
	); err != nil {
		t.Fatal(err)
	}

	// Once it catches up, the effects of both commands can be seen.
	util.SucceedsSoon(t, func() error {
		getArgs := getArgs([]byte("a"))
		if reply, err := client.SendWrappedWith(context.Background(), rg1(mtc.stores[1]), roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, &getArgs); err != nil {
			return errors.Errorf("failed to read data: %s", err)
		} else if e, v := int64(16), mustGetInt(reply.(*roachpb.GetResponse).Value); v != e {
			return errors.Errorf("failed to read correct data: expected %d, got %d", e, v)
		}
		return nil
	})

	repl2, err := mtc.stores[1].GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	util.SucceedsSoon(t, func() error {
		if mvcc, mvcc2 := repl.GetMVCCStats(), repl2.GetMVCCStats(); mvcc2 != mvcc {
			return errors.Errorf("expected stats on new range:\n%+v\not equal old:\n%+v", mvcc2, mvcc)
		}
		return nil
	})

	// Send a third command to verify that the log states are synced up so the
	// new node can accept new commands.
	incArgs = incrementArgs([]byte("a"), 23)
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &incArgs); err != nil {
		t.Fatal(err)
	}

	util.SucceedsSoon(t, func() error {
		getArgs := getArgs([]byte("a"))
		if reply, err := client.SendWrappedWith(context.Background(), rg1(mtc.stores[1]), roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, &getArgs); err != nil {
			return errors.Errorf("failed to read data: %s", err)
		} else if e, v := int64(39), mustGetInt(reply.(*roachpb.GetResponse).Value); v != e {
			return errors.Errorf("failed to read correct data: expected %d, got %d", e, v)
		}
		return nil
	})
}

// TestSnapshotAfterTruncation tests that Raft will properly send a
// non-preemptive snapshot when a node is brought up and the log has been
// truncated.
func TestSnapshotAfterTruncation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := startMultiTestContext(t, 3)
	defer mtc.Stop()
	repl, err := mtc.stores[0].GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	key := roachpb.Key("a")
	incA := int64(5)
	incB := int64(7)
	incAB := incA + incB

	// Set up a key to replicate across the cluster. We're going to modify this
	// key and truncate the raft logs from that command after killing one of the
	// nodes to check that it gets the new value after it comes up.
	incArgs := incrementArgs(key, incA)
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &incArgs); err != nil {
		t.Fatal(err)
	}

	mtc.replicateRange(1, 1, 2)
	mtc.waitForValues(key, []int64{incA, incA, incA})

	// Now kill store 1, increment the key on the other stores and truncate
	// their logs to make sure that when store 1 comes back up it will require a
	// non-preemptive snapshot from Raft.
	mtc.stopStore(1)

	incArgs = incrementArgs(key, incB)
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &incArgs); err != nil {
		t.Fatal(err)
	}

	mtc.waitForValues(key, []int64{incAB, incA, incAB})

	index, err := repl.GetLastIndex()
	if err != nil {
		t.Fatal(err)
	}

	// Truncate the log at index+1 (log entries < N are removed, so this
	// includes the increment).
	truncArgs := truncateLogArgs(index+1, 1)
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &truncArgs); err != nil {
		t.Fatal(err)
	}
	mtc.restartStore(1)

	mtc.waitForValues(key, []int64{incAB, incAB, incAB})
}

type fakeSnapshotStream struct {
	nextReq *storage.SnapshotRequest
	nextErr error
}

// Recv implements the SnapshotResponseStream interface.
func (c fakeSnapshotStream) Recv() (*storage.SnapshotRequest, error) {
	return c.nextReq, c.nextErr
}

// Send implements the SnapshotResponseStream interface.
func (c fakeSnapshotStream) Send(request *storage.SnapshotResponse) error {
	return nil
}

// Context implements the SnapshotResponseStream interface.
func (c fakeSnapshotStream) Context() context.Context {
	return context.Background()
}

// TestFailedSnapshotFillsReservation tests that failing to finish applying an
// incoming snapshot still cleans up the outstanding reservation that was made.
func TestFailedSnapshotFillsReservation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := startMultiTestContext(t, 3)
	defer mtc.Stop()

	rep, err := mtc.stores[0].GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}
	header := storage.SnapshotRequest_Header{
		CanDecline:      true,
		RangeSize:       100,
		RangeDescriptor: *rep.Desc(),
	}
	// Cause this stream to return an error as soon as we ask it for something.
	// This injects an error into HandleSnapshotStream when we try to send the
	// "snapshot accepted" message.
	expectedErr := errors.Errorf("")
	stream := fakeSnapshotStream{nil, expectedErr}
	if err := mtc.stores[1].HandleSnapshot(&header, stream); err != expectedErr {
		t.Fatalf("expected error %s, but found %v", expectedErr, err)
	}
	if n := mtc.stores[1].ReservationCount(); n != 0 {
		t.Fatalf("expected 0 reservations, but found %d", n)
	}
}

// TestConcurrentRaftSnapshots tests that snapshots still work correctly when
// Raft requests multiple non-preemptive snapshots at the same time. This
// situation occurs when two replicas need snapshots at the same time.
func TestConcurrentRaftSnapshots(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := startMultiTestContext(t, 5)
	defer mtc.Stop()
	repl, err := mtc.stores[0].GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	key := roachpb.Key("a")
	incA := int64(5)
	incB := int64(7)
	incAB := incA + incB

	// Set up a key to replicate across the cluster. We're going to modify this
	// key and truncate the raft logs from that command after killing one of the
	// nodes to check that it gets the new value after it comes up.
	incArgs := incrementArgs(key, incA)
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &incArgs); err != nil {
		t.Fatal(err)
	}

	mtc.replicateRange(1, 1, 2, 3, 4)
	mtc.waitForValues(key, []int64{incA, incA, incA, incA, incA})

	// Now kill stores 1 + 2, increment the key on the other stores and
	// truncate their logs to make sure that when store 1 + 2 comes back up
	// they will require a non-preemptive snapshot from Raft.
	mtc.stopStore(1)
	mtc.stopStore(2)

	incArgs = incrementArgs(key, incB)
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &incArgs); err != nil {
		t.Fatal(err)
	}

	mtc.waitForValues(key, []int64{incAB, incA, incA, incAB, incAB})

	index, err := repl.GetLastIndex()
	if err != nil {
		t.Fatal(err)
	}

	// Truncate the log at index+1 (log entries < N are removed, so this
	// includes the increment).
	truncArgs := truncateLogArgs(index+1, 1)
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &truncArgs); err != nil {
		t.Fatal(err)
	}
	mtc.restartStore(1)
	mtc.restartStore(2)

	mtc.waitForValues(key, []int64{incAB, incAB, incAB, incAB, incAB})
}

// Test a scenario where a replica is removed from a down node, the associated
// range is split, the node restarts and we try to replicate the RHS of the
// split range back to the restarted node.
func TestReplicateAfterRemoveAndSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sc := storage.TestStoreConfig(nil)
	// Disable the replica GC queue so that it doesn't accidentally pick up the
	// removed replica and GC it. We'll explicitly enable it later in the test.
	sc.TestingKnobs.DisableReplicaGCQueue = true
	mtc := &multiTestContext{storeConfig: &sc}
	mtc.Start(t, 3)
	defer mtc.Stop()
	rep1, err := mtc.stores[0].GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	mtc.replicateRange(1, 1, 2)

	// Kill store 2.
	mtc.stopStore(2)

	// Remove store 2 from the range to simulate removal of a dead node.
	mtc.unreplicateRange(1, 2)

	// Split the range.
	splitKey := roachpb.Key("m")
	splitArgs := adminSplitArgs(splitKey, splitKey)
	if _, err := rep1.AdminSplit(context.Background(), splitArgs); err != nil {
		t.Fatal(err)
	}

	mtc.expireLeases()

	// Restart store 2.
	mtc.restartStore(2)

	replicateRHS := func() error {
		// Try to up-replicate the RHS of the split to store 2. We can't use
		// replicateRange because this should fail on the first attempt and then
		// eventually succeed.
		startKey := roachpb.RKey(splitKey)

		var desc roachpb.RangeDescriptor
		if err := mtc.dbs[0].GetProto(context.TODO(), keys.RangeDescriptorKey(startKey), &desc); err != nil {
			t.Fatal(err)
		}

		rep2, err := mtc.findMemberStoreLocked(desc).GetReplica(desc.RangeID)
		if err != nil {
			t.Fatal(err)
		}

		return rep2.ChangeReplicas(
			context.Background(),
			roachpb.ADD_REPLICA,
			roachpb.ReplicaDescriptor{
				NodeID:  mtc.stores[2].Ident.NodeID,
				StoreID: mtc.stores[2].Ident.StoreID,
			},
			&desc,
		)
	}

	expected := "snapshot intersects existing range"
	if err := replicateRHS(); !testutils.IsError(err, expected) {
		t.Fatalf("unexpected error %v", err)
	}

	// Enable the replica GC queue so that the next attempt to replicate the RHS
	// to store 2 will cause the obsolete replica to be GC'd allowing a
	// subsequent replication to succeed.
	mtc.stores[2].SetReplicaGCQueueActive(true)

	util.SucceedsSoon(t, replicateRHS)
}

// Test various mechanism for refreshing pending commands.
func TestRefreshPendingCommands(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// In this scenario, three different mechanisms detect the need to repropose
	// commands. Test that each one is sufficient individually. We have this
	// redundancy because some mechanisms respond with lower latency than others,
	// but each has some scenarios (not currently tested) in which it is
	// insufficient on its own. In addition, there is a fourth reproposal
	// mechanism (reasonNewLeaderOrConfigChange) which is not relevant to this
	// scenario.
	testCases := []storage.StoreTestingKnobs{
		{DisableRefreshReasonNewLeader: true, DisableRefreshReasonTicks: true},
		{DisableRefreshReasonSnapshotApplied: true, DisableRefreshReasonTicks: true},
		{DisableRefreshReasonNewLeader: true, DisableRefreshReasonSnapshotApplied: true},
	}
	for _, c := range testCases {
		func() {
			sc := storage.TestStoreConfig(nil)
			sc.TestingKnobs = c
			// Disable periodic gossip tasks which can move the range 1 lease
			// unexpectedly.
			sc.TestingKnobs.DisablePeriodicGossips = true
			mtc := &multiTestContext{storeConfig: &sc}
			mtc.Start(t, 3)
			defer mtc.Stop()

			const rangeID = roachpb.RangeID(1)
			mtc.replicateRange(rangeID, 1, 2)

			// Put some data in the range so we'll have something to test for.
			incArgs := incrementArgs([]byte("a"), 5)
			if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &incArgs); err != nil {
				t.Fatal(err)
			}

			// Wait for all nodes to catch up.
			mtc.waitForValues(roachpb.Key("a"), []int64{5, 5, 5})

			// Stop node 2; while it is down write some more data.
			mtc.stopStore(2)

			if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &incArgs); err != nil {
				t.Fatal(err)
			}

			// Get the last increment's log index.
			repl, err := mtc.stores[0].GetReplica(1)
			if err != nil {
				t.Fatal(err)
			}
			index, err := repl.GetLastIndex()
			if err != nil {
				t.Fatal(err)
			}

			// Truncate the log at index+1 (log entries < N are removed, so this includes
			// the increment).
			truncArgs := truncateLogArgs(index+1, rangeID)
			if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &truncArgs); err != nil {
				t.Fatal(err)
			}

			// Stop and restart node 0 in order to make sure that any in-flight Raft
			// messages have been sent.
			mtc.stopStore(0)
			mtc.restartStore(0)

			// Expire existing leases (i.e. move the clock forward). This allows node
			// 3 to grab the lease later in the test.
			mtc.expireLeases()
			// Drain leases from nodes 0 and 1 to prevent them from grabbing any new
			// leases.
			for i := 0; i < 2; i++ {
				if err := mtc.stores[i].DrainLeases(true); err != nil {
					t.Fatal(err)
				}
			}

			// Restart node 2 and wait for the snapshot to be applied. Note that
			// waitForValues reads directly from the engine and thus isn't executing
			// a Raft command.
			mtc.restartStore(2)
			mtc.waitForValues(roachpb.Key("a"), []int64{10, 10, 10})

			// Send an increment to the restarted node. If we don't refresh pending
			// commands appropriately, the range lease command will not get
			// re-proposed when we discover the new leader.
			if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[2]), &incArgs); err != nil {
				t.Fatal(err)
			}

			mtc.waitForValues(roachpb.Key("a"), []int64{15, 15, 15})
		}()
	}
}

// TestStoreRangeUpReplicate verifies that the replication queue will notice
// under-replicated ranges and replicate them.
func TestStoreRangeUpReplicate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := startMultiTestContext(t, 3)
	defer mtc.Stop()

	mtc.initGossipNetwork()

	// Once we know our peers, trigger a scan.
	mtc.stores[0].ForceReplicationScanAndProcess()

	// The range should become available on every node.
	util.SucceedsSoon(t, func() error {
		for _, s := range mtc.stores {
			r := s.LookupReplica(roachpb.RKey("a"), roachpb.RKey("b"))
			if r == nil {
				return errors.Errorf("expected replica for keys \"a\" - \"b\"")
			}
			if n := s.ReservationCount(); n != 0 {
				return errors.Errorf("expected 0 reservations, but found %d", n)
			}
		}
		return nil
	})

	var generated int64
	var normalApplied int64
	var preemptiveApplied int64
	for _, s := range mtc.stores {
		m := s.Metrics()
		generated += m.RangeSnapshotsGenerated.Count()
		normalApplied += m.RangeSnapshotsNormalApplied.Count()
		preemptiveApplied += m.RangeSnapshotsPreemptiveApplied.Count()
	}
	if generated == 0 {
		t.Fatalf("expected at least 1 snapshot, but found 0")
	}

	if normalApplied != 0 {
		t.Fatalf("expected 0 normal snapshots, but found %d", normalApplied)
	}
	if generated != preemptiveApplied {
		t.Fatalf("expected %d preemptive snapshots, but found %d", generated, preemptiveApplied)
	}
}

// TestStoreRangeCorruptionChangeReplicas verifies that the replication queue
// will notice corrupted replicas and replace them.
func TestStoreRangeCorruptionChangeReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numReplicas = 3
	const extraStores = 3

	sc := storage.TestStoreConfig(nil)
	var corrupt struct {
		syncutil.Mutex
		store *storage.Store
	}
	sc.TestingKnobs.TestingApplyFilter = func(filterArgs storagebase.ApplyFilterArgs) *roachpb.Error {
		corrupt.Lock()
		defer corrupt.Unlock()

		if corrupt.store == nil || filterArgs.StoreID != corrupt.store.StoreID() {
			return nil
		}

		return roachpb.NewError(
			storage.NewReplicaCorruptionError(errors.New("boom")),
		)
	}

	// Don't timeout raft leader.
	sc.RaftElectionTimeoutTicks = 1000000
	mtc := &multiTestContext{
		storeConfig: &sc,
	}
	mtc.Start(t, numReplicas+extraStores)
	defer mtc.Stop()
	mtc.initGossipNetwork()
	store0 := mtc.stores[0]

	for i := 0; i < extraStores; i++ {
		util.SucceedsSoon(t, func() error {
			store0.ForceReplicationScanAndProcess()

			replicas := store0.LookupReplica(roachpb.RKey("a"), roachpb.RKey("b")).Desc().Replicas
			if len(replicas) < numReplicas {
				return errors.Errorf("initial replication: expected len(replicas) = %d, got %+v", numReplicas, replicas)
			}

			corrupt.Lock()
			defer corrupt.Unlock()
			// Pick a random replica to corrupt.
			for corrupt.store = nil; corrupt.store == nil || corrupt.store == store0; {
				storeID := replicas[rand.Intn(len(replicas))].StoreID
				for _, store := range mtc.stores {
					if store.StoreID() == storeID {
						corrupt.store = store
						break
					}
				}
			}
			return nil
		})

		var corruptRep roachpb.ReplicaDescriptor
		util.SucceedsSoon(t, func() error {
			r := corrupt.store.LookupReplica(roachpb.RKey("a"), roachpb.RKey("b"))
			if r == nil {
				return errors.New("replica is not available yet")
			}
			var err error
			corruptRep, err = r.GetReplicaDescriptor()
			return err
		})

		args := putArgs(roachpb.Key("any write"), []byte("should mark as corrupted"))
		if _, err := client.SendWrapped(context.Background(), rg1(store0), &args); err != nil {
			t.Fatal(err)
		}

		// Wait until maybeSetCorrupt has been called. This isn't called immediately
		// since the put command has quorum with the other good node.
		util.SucceedsSoon(t, func() error {
			corrupt.Lock()
			defer corrupt.Unlock()
			replicas := corrupt.store.GetDeadReplicas()
			if len(replicas.Replicas) == 0 {
				return errors.New("expected corrupt store to have a dead replica")
			}
			return nil
		})

		corrupt.Lock()
		err := corrupt.store.GossipDeadReplicas(context.Background())
		corrupt.Unlock()
		if err != nil {
			t.Fatal(err)
		}

		util.SucceedsSoon(t, func() error {
			store0.ForceReplicationScanAndProcess()

			// Should be removed from the corrupt store.
			replicas := store0.LookupReplica(roachpb.RKey("a"), roachpb.RKey("b")).Desc().Replicas
			for _, rep := range replicas {
				if rep == corruptRep {
					return errors.Errorf("expected corrupt replica %+v to be removed from %+v", rep, replicas)
				}
				if rep.StoreID == corruptRep.StoreID {
					return errors.Errorf("expected new replica to not be placed back on same store as corrupt replica")
				}
			}
			if len(replicas) < numReplicas {
				return errors.Errorf("expected len(replicas) = %d, got %+v", numReplicas, replicas)
			}
			return nil
		})
	}
}

// getRangeMetadata retrieves the current range descriptor for the target
// range.
func getRangeMetadata(
	key roachpb.RKey, mtc *multiTestContext, t *testing.T,
) roachpb.RangeDescriptor {
	// Calls to RangeLookup typically use inconsistent reads, but we
	// want to do a consistent read here. This is important when we are
	// considering one of the metadata ranges: we must not do an
	// inconsistent lookup in our own copy of the range.
	b := &client.Batch{}
	b.AddRawRequest(&roachpb.RangeLookupRequest{
		Span: roachpb.Span{
			Key: keys.RangeMetaKey(key),
		},
		MaxRanges: 1,
	})
	var reply *roachpb.RangeLookupResponse
	if err := mtc.dbs[0].Run(context.TODO(), b); err != nil {
		t.Fatalf("error getting range metadata: %s", err)
	} else {
		reply = b.RawResponse().Responses[0].GetInner().(*roachpb.RangeLookupResponse)
	}
	if a, e := len(reply.Ranges), 1; a != e {
		t.Fatalf("expected %d range descriptor, got %d", e, a)
	}
	return reply.Ranges[0]
}

// TestUnreplicateFirstRange verifies that multiTestContext still functions in
// the case where the first range (which contains range metadata) is
// unreplicated from the first store. This situation can arise occasionally in
// tests, as can a similar situation where the first store is no longer the lease holder of
// the first range; this verifies that those tests will not be affected.
func TestUnreplicateFirstRange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mtc := startMultiTestContext(t, 3)
	defer mtc.Stop()

	const rangeID = roachpb.RangeID(1)
	// Replicate the range to store 1.
	mtc.replicateRange(rangeID, 1)
	// Unreplicate the from from store 0.
	mtc.unreplicateRange(rangeID, 0)
	// Replicate the range to store 2. The first range is no longer available on
	// store 1, and this command will fail if that situation is not properly
	// supported.
	mtc.replicateRange(rangeID, 2)
}

// TestChangeReplicasDescriptorInvariant tests that a replica change aborts if
// another change has been made to the RangeDescriptor since it was initiated.
//
// TODO(tschottdorf): If this test is flaky because the snapshot count does not
// increase, it's likely because with proposer-evaluated KV, less gets proposed
// and so sometimes Raft discards the preemptive snapshot (though we count that
// case in stats already) or doesn't produce a Ready.
func TestChangeReplicasDescriptorInvariant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := startMultiTestContext(t, 3)
	defer mtc.Stop()

	repl, err := mtc.stores[0].GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	addReplica := func(storeNum int, desc *roachpb.RangeDescriptor) error {
		return repl.ChangeReplicas(
			context.Background(),
			roachpb.ADD_REPLICA,
			roachpb.ReplicaDescriptor{
				NodeID:  mtc.stores[storeNum].Ident.NodeID,
				StoreID: mtc.stores[storeNum].Ident.StoreID,
			},
			desc,
		)
	}

	// Retain the descriptor for the range at this point.
	origDesc := repl.Desc()

	// Add replica to the second store, which should succeed.
	if err := addReplica(1, origDesc); err != nil {
		t.Fatal(err)
	}
	util.SucceedsSoon(t, func() error {
		r := mtc.stores[1].LookupReplica(roachpb.RKey("a"), roachpb.RKey("b"))
		if r == nil {
			return errors.Errorf("expected replica for keys \"a\" - \"b\"")
		}
		return nil
	})

	before := mtc.stores[2].Metrics().RangeSnapshotsPreemptiveApplied.Count()
	// Attempt to add replica to the third store with the original descriptor.
	// This should fail because the descriptor is stale.
	if err := addReplica(2, origDesc); !testutils.IsError(err, `change replicas of range \d+ failed`) {
		t.Fatalf("got unexpected error: %v", err)
	}

	util.SucceedsSoon(t, func() error {
		after := mtc.stores[2].Metrics().RangeSnapshotsPreemptiveApplied.Count()
		// The failed ChangeReplicas call should have applied a preemptive snapshot.
		if after != before+1 {
			return errors.Errorf(
				"ChangeReplicas call should have applied a preemptive snapshot, before %d after %d",
				before, after)
		}
		return nil
	})

	before = mtc.stores[2].Metrics().RangeSnapshotsPreemptiveApplied.Count()
	// Add to third store with fresh descriptor.
	if err := addReplica(2, repl.Desc()); err != nil {
		t.Fatal(err)
	}

	util.SucceedsSoon(t, func() error {
		after := mtc.stores[2].Metrics().RangeSnapshotsPreemptiveApplied.Count()
		// The failed ChangeReplicas call should have applied a preemptive snapshot.
		if after != before+1 {
			return errors.Errorf(
				"ChangeReplicas call should have applied a preemptive snapshot, before %d after %d",
				before, after)
		}
		r := mtc.stores[2].LookupReplica(roachpb.RKey("a"), roachpb.RKey("b"))
		if r == nil {
			return errors.Errorf("expected replica for keys \"a\" - \"b\"")
		}
		return nil
	})
}

// TestProgressWithDownNode verifies that a surviving quorum can make progress
// with a downed node.
func TestProgressWithDownNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := startMultiTestContext(t, 3)
	defer mtc.Stop()

	const rangeID = roachpb.RangeID(1)
	mtc.replicateRange(rangeID, 1, 2)

	incArgs := incrementArgs([]byte("a"), 5)
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &incArgs); err != nil {
		t.Fatal(err)
	}

	// Verify that the first increment propagates to all the engines.
	verify := func(expected []int64) {
		util.SucceedsSoon(t, func() error {
			values := []int64{}
			for _, eng := range mtc.engines {
				val, _, err := engine.MVCCGet(context.Background(), eng, roachpb.Key("a"), mtc.clock.Now(), true, nil)
				if err != nil {
					return err
				}
				values = append(values, mustGetInt(val))
			}
			if !reflect.DeepEqual(expected, values) {
				return errors.Errorf("expected %v, got %v", expected, values)
			}
			return nil
		})
	}
	verify([]int64{5, 5, 5})

	// Stop one of the replicas and issue a new increment.
	mtc.stopStore(1)
	incArgs = incrementArgs([]byte("a"), 11)
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &incArgs); err != nil {
		t.Fatal(err)
	}

	// The new increment can be seen on both live replicas.
	verify([]int64{16, 5, 16})

	// Once the downed node is restarted, it will catch up.
	mtc.restartStore(1)
	verify([]int64{16, 16, 16})
}

// TestReplicateAddAndRemoveRestart is motivated by issue #8111, which suggests the
// following test (which verifies the ability of a snapshot with a new replica ID
// to overwrite existing data):
//   - replicate a range to three stores
//   - stop a store
//   - remove the stopped store from the range
//   - truncate the logs
//   - re-add the store and restart it
//   - ensure that store can catch up with the rest of the group
func TestReplicateRestartAfterTruncationWithRemoveAndReAdd(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runReplicateRestartAfterTruncation(t, true)
}

// TestReplicateRestartAfterTruncation is a variant of
// TestReplicateRestartAfterTruncationWithRemoveAndReAdd without the remove and
// re-add. Just stop, truncate, and restart. This verifies that a snapshot
// without a new replica ID works correctly.
func TestReplicateRestartAfterTruncation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runReplicateRestartAfterTruncation(t, false)
}

func runReplicateRestartAfterTruncation(t *testing.T, removeBeforeTruncateAndReAdd bool) {
	sc := storage.TestStoreConfig(nil)
	// Don't timeout raft leaders or range leases (see the relation between
	// RaftElectionTimeoutTicks and rangeLeaseActiveDuration). This test expects
	// mtc.stores[0] to hold the range lease for range 1.
	sc.RaftElectionTimeoutTicks = 1000000
	mtc := &multiTestContext{storeConfig: &sc}
	mtc.Start(t, 3)
	defer mtc.Stop()

	key := roachpb.Key("a")

	// Replicate the initial range to all three nodes.
	const rangeID = roachpb.RangeID(1)
	mtc.replicateRange(rangeID, 1, 2)

	// Verify that the first increment propagates to all the engines.
	incArgs := incrementArgs(key, 2)
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &incArgs); err != nil {
		t.Fatal(err)
	}
	mtc.waitForValues(key, []int64{2, 2, 2})

	// Stop a store.
	mtc.stopStore(1)
	if removeBeforeTruncateAndReAdd {
		// remove the stopped store from the range
		mtc.unreplicateRange(rangeID, 1)
	}

	// Truncate the logs.
	{
		// Get the last increment's log index.
		repl, err := mtc.stores[0].GetReplica(rangeID)
		if err != nil {
			t.Fatal(err)
		}
		index, err := repl.GetLastIndex()
		if err != nil {
			t.Fatal(err)
		}
		// Truncate the log at index+1 (log entries < N are removed, so this includes
		// the increment).
		truncArgs := truncateLogArgs(index+1, rangeID)
		if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &truncArgs); err != nil {
			t.Fatal(err)
		}
	}

	// Ensure that store can catch up with the rest of the group.
	incArgs = incrementArgs(key, 3)
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &incArgs); err != nil {
		t.Fatal(err)
	}

	mtc.waitForValues(key, []int64{5, 2, 5})

	// Re-add the store and restart it.
	// TODO(dt): ben originally suggested we also attempt this in the other order.
	// This currently hits an NPE in mtc.replicateRange though when it tries to
	// read the Ident.NodeID field in the specified store, and will become
	// impossible after streaming snapshots.
	mtc.restartStore(1)
	if removeBeforeTruncateAndReAdd {
		// Verify old replica is GC'd. Wait out the replica gc queue
		// inactivity threshold and force a gc scan.
		mtc.manualClock.Increment(int64(storage.ReplicaGCQueueInactivityThreshold + 1))
		mtc.stores[1].ForceReplicaGCScanAndProcess()
		util.SucceedsSoon(t, func() error {
			if true {
				// TODO(spencerkimball): fix the flakiness seen in #8670 and remove.
				return nil
			}
			_, err := mtc.stores[1].GetReplica(rangeID)
			if _, ok := err.(*roachpb.RangeNotFoundError); !ok {
				return errors.Errorf("expected replica to be garbage collected")
			}
			return nil
		})

		mtc.replicateRange(rangeID, 1)
	}

	mtc.waitForValues(key, []int64{5, 5, 5})
}

func testReplicaAddRemove(t *testing.T, addFirst bool) {
	sc := storage.TestStoreConfig(nil)
	// We're gonna want to validate the state of the store before and after the
	// replica GC queue does its work, so we disable the replica gc queue here
	// and run it manually when we're ready.
	sc.TestingKnobs.DisableReplicaGCQueue = true
	mtc := &multiTestContext{storeConfig: &sc}
	mtc.Start(t, 4)
	defer mtc.Stop()

	key := roachpb.Key("a")
	verifyFn := func(expected []int64) func() error {
		return func() error {
			values := make([]int64, len(mtc.engines))
			for i, eng := range mtc.engines {
				val, _, err := engine.MVCCGet(context.Background(), eng, key, mtc.clock.Now(), true, nil)
				if err != nil {
					return err
				}
				values[i] = mustGetInt(val)
			}
			if reflect.DeepEqual(expected, values) {
				return nil
			}
			return errors.Errorf("expected %+v, got %+v", expected, values)
		}
	}

	// Replicate the initial range to three of the four nodes.
	const rangeID = roachpb.RangeID(1)
	mtc.replicateRange(rangeID, 3, 1)

	inc1 := int64(5)
	{
		incArgs := incrementArgs(key, inc1)
		if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &incArgs); err != nil {
			t.Fatal(err)
		}
	}

	// The first increment is visible on all three replicas.
	util.SucceedsSoon(t, verifyFn([]int64{
		inc1,
		inc1,
		0,
		inc1,
	}))

	// Stop a store and replace it.
	mtc.stopStore(1)
	if addFirst {
		mtc.replicateRange(rangeID, 2)
		mtc.unreplicateRange(rangeID, 1)
	} else {
		mtc.unreplicateRange(rangeID, 1)
		mtc.replicateRange(rangeID, 2)
	}
	// The first increment is visible on the new replica.
	util.SucceedsSoon(t, verifyFn([]int64{
		inc1,
		inc1,
		inc1,
		inc1,
	}))

	// Ensure that the rest of the group can make progress.
	inc2 := int64(11)
	{
		incArgs := incrementArgs(key, inc2)
		if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &incArgs); err != nil {
			t.Fatal(err)
		}
	}
	util.SucceedsSoon(t, verifyFn([]int64{
		inc1 + inc2,
		inc1,
		inc1 + inc2,
		inc1 + inc2,
	}))

	// Bring the downed store back up (required for a clean shutdown).
	mtc.restartStore(1)

	// The downed store never sees the increment that was added while it was
	// down. Perform another increment now that it is back up to verify that it
	// doesn't see future activity.
	inc3 := int64(23)
	{
		incArgs := incrementArgs(key, inc3)
		if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &incArgs); err != nil {
			t.Fatal(err)
		}
	}
	util.SucceedsSoon(t, verifyFn([]int64{
		inc1 + inc2 + inc3,
		inc1,
		inc1 + inc2 + inc3,
		inc1 + inc2 + inc3,
	}))

	// Wait out the range lease and the unleased duration to make the replica GC'able.
	mtc.expireLeases()
	mtc.manualClock.Increment(int64(storage.ReplicaGCQueueInactivityThreshold + 1))
	mtc.stores[1].SetReplicaGCQueueActive(true)
	mtc.stores[1].ForceReplicaGCScanAndProcess()

	// The removed store no longer has any of the data from the range.
	util.SucceedsSoon(t, verifyFn([]int64{
		inc1 + inc2 + inc3,
		0,
		inc1 + inc2 + inc3,
		inc1 + inc2 + inc3,
	}))

	desc := mtc.stores[0].LookupReplica(roachpb.RKeyMin, nil).Desc()
	replicaIDsByStore := map[roachpb.StoreID]roachpb.ReplicaID{}
	for _, rep := range desc.Replicas {
		replicaIDsByStore[rep.StoreID] = rep.ReplicaID
	}
	expected := map[roachpb.StoreID]roachpb.ReplicaID{1: 1, 4: 2, 3: 4}
	if !reflect.DeepEqual(expected, replicaIDsByStore) {
		t.Fatalf("expected replica IDs to be %v but got %v", expected, replicaIDsByStore)
	}
}

func TestReplicateAddAndRemove(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testReplicaAddRemove(t, true)
}

func TestReplicateRemoveAndAdd(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testReplicaAddRemove(t, false)
}

// TestRaftHeartbeats verifies that coalesced heartbeats are correctly
// suppressing elections in an idle cluster.
func TestRaftHeartbeats(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mtc := startMultiTestContext(t, 3)
	defer mtc.Stop()
	mtc.replicateRange(1, 1, 2)

	// Capture the initial term and state.
	leaderIdx := -1
	for i, store := range mtc.stores {
		if store.RaftStatus(1).SoftState.RaftState == raft.StateLeader {
			leaderIdx = i
			break
		}
	}
	initialTerm := mtc.stores[leaderIdx].RaftStatus(1).Term

	// Wait for several ticks to elapse.
	ticksToWait := 2 * mtc.makeStoreConfig(0).RaftElectionTimeoutTicks
	ticks := mtc.stores[leaderIdx].Metrics().RaftTicks.Count
	for targetTicks := ticks() + int64(ticksToWait); ticks() < targetTicks; {
		time.Sleep(time.Millisecond)
	}

	status := mtc.stores[leaderIdx].RaftStatus(1)
	if status.SoftState.RaftState != raft.StateLeader {
		t.Errorf("expected node %d to be leader after sleeping but was %s", leaderIdx, status.SoftState.RaftState)
	}
	if status.Term != initialTerm {
		t.Errorf("while sleeping, term changed from %d to %d", initialTerm, status.Term)
	}
}

// TestReportUnreachableHeartbeats tests that if a single transport fails,
// coalesced heartbeats are not stalled out entirely.
func TestReportUnreachableHeartbeats(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mtc := startMultiTestContext(t, 3)
	defer mtc.Stop()

	// Replicate the range onto all three stores
	mtc.replicateRange(1, 1, 2)

	// Send a single increment to ensure the range is up.
	key := roachpb.Key("a")
	incArgs := incrementArgs(key, 2)
	if _, err := client.SendWrapped(context.Background(),
		mtc.distSenders[0], &incArgs); err != nil {
		t.Fatal(err)
	}
	mtc.waitForValues(key, []int64{2, 2, 2})

	leaderIdx := -1
	for i, store := range mtc.stores {
		if store.RaftStatus(1).SoftState.RaftState == raft.StateLeader {
			leaderIdx = i
			break
		}
	}
	initialTerm := mtc.stores[leaderIdx].RaftStatus(1).Term
	// Choose a follower index that is guaranteed to not be the leader
	followerIdx := len(mtc.stores) + 1 - leaderIdx

	// Shut down a raft transport via the circuit breaker, and wait for two
	// election timeouts to trigger an election if reportUnreachable broke
	// heartbeat transmission to the other store.
	cb := mtc.transports[0].GetCircuitBreaker(roachpb.NodeID(followerIdx))
	cb.Break()

	ticksToWait := 2 * mtc.makeStoreConfig(0).RaftElectionTimeoutTicks
	ticks := mtc.stores[leaderIdx].Metrics().RaftTicks.Count
	for targetTicks := ticks() + int64(ticksToWait); ticks() < targetTicks; {
		time.Sleep(time.Millisecond)
	}

	// Ensure that the leadership has not changed, to confirm that heartbeats
	// are sent to the store with a functioning transport.
	status := mtc.stores[leaderIdx].RaftStatus(1)
	if status.SoftState.RaftState != raft.StateLeader {
		t.Errorf("expected node %d to be leader after sleeping but was %s", leaderIdx, status.SoftState.RaftState)
	}
	if status.Term != initialTerm {
		t.Errorf("while sleeping, term changed from %d to %d", initialTerm, status.Term)
	}
}

// TestReplicateAfterSplit verifies that a new replica whose start key
// is not KeyMin replicating to a fresh store can apply snapshots correctly.
func TestReplicateAfterSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := startMultiTestContext(t, 2)
	defer mtc.Stop()

	const rangeID = roachpb.RangeID(1)
	splitKey := roachpb.Key("m")
	key := roachpb.Key("z")

	store0 := mtc.stores[0]
	// Make the split
	splitArgs := adminSplitArgs(roachpb.KeyMin, splitKey)
	if _, err := client.SendWrapped(context.Background(), rg1(store0), &splitArgs); err != nil {
		t.Fatal(err)
	}

	rangeID2 := store0.LookupReplica(roachpb.RKey(key), nil).RangeID
	if rangeID2 == rangeID {
		t.Fatal("got same range id after split")
	}
	// Issue an increment for later check.
	incArgs := incrementArgs(key, 11)
	if _, err := client.SendWrappedWith(context.Background(), rg1(store0), roachpb.Header{
		RangeID: rangeID2,
	}, &incArgs); err != nil {
		t.Fatal(err)
	}
	// Now add the second replica.
	mtc.replicateRange(rangeID2, 1)

	if mtc.stores[1].LookupReplica(roachpb.RKey(key), nil).GetMaxBytes() == 0 {
		t.Error("Range MaxBytes is not set after snapshot applied")
	}
	// Once it catches up, the effects of increment commands can be seen.
	util.SucceedsSoon(t, func() error {
		getArgs := getArgs(key)
		// Reading on non-lease holder replica should use inconsistent read
		if reply, err := client.SendWrappedWith(context.Background(), rg1(mtc.stores[1]), roachpb.Header{
			RangeID:         rangeID2,
			ReadConsistency: roachpb.INCONSISTENT,
		}, &getArgs); err != nil {
			return errors.Errorf("failed to read data: %s", err)
		} else if e, v := int64(11), mustGetInt(reply.(*roachpb.GetResponse).Value); v != e {
			return errors.Errorf("failed to read correct data: expected %d, got %d", e, v)
		}
		return nil
	})
}

// TestReplicaRemovalCampaign verifies that a new replica after a split can be
// transferred away/replaced without campaigning the old one.
func TestReplicaRemovalCampaign(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		remove        bool
		expectAdvance bool
	}{
		{ // Replica removed
			remove:        true,
			expectAdvance: false,
		},
		{ // Default behavior
			remove:        false,
			expectAdvance: true,
		},
	}

	const rangeID = roachpb.RangeID(1)
	splitKey := roachpb.Key("m")
	key2 := roachpb.Key("z")

	for i, td := range testData {
		func() {
			mtc := startMultiTestContext(t, 2)
			defer mtc.Stop()

			// Replicate range to enable raft campaigning.
			mtc.replicateRange(rangeID, 1)
			store0 := mtc.stores[0]

			// Make the split.
			splitArgs := adminSplitArgs(roachpb.KeyMin, splitKey)
			if _, err := client.SendWrapped(context.Background(), rg1(store0), &splitArgs); err != nil {
				t.Fatal(err)
			}

			replica2 := store0.LookupReplica(roachpb.RKey(key2), nil)

			rg2 := func(s *storage.Store) client.Sender {
				return client.Wrap(s, func(ba roachpb.BatchRequest) roachpb.BatchRequest {
					if ba.RangeID == 0 {
						ba.RangeID = replica2.RangeID
					}
					return ba
				})
			}

			// Raft processing is initialized lazily; issue a no-op write request to
			// ensure that the Raft group has been started.
			incArgs := incrementArgs(key2, 0)
			if _, err := client.SendWrapped(context.Background(), rg2(store0), &incArgs); err != nil {
				t.Fatal(err)
			}

			if td.remove {
				// Simulate second replica being transferred by removing it.
				if err := store0.RemoveReplica(context.Background(), replica2, *replica2.Desc(), true); err != nil {
					t.Fatal(err)
				}
			}

			var latestTerm uint64
			if td.expectAdvance {
				util.SucceedsSoon(t, func() error {
					if raftStatus := replica2.RaftStatus(); raftStatus != nil {
						if term := raftStatus.Term; term <= latestTerm {
							return errors.Errorf("%d: raft term has not yet advanced: %d", i, term)
						} else if latestTerm == 0 {
							latestTerm = term
						}
					} else {
						return errors.Errorf("%d: raft group is not yet initialized", i)
					}
					return nil
				})
			} else {
				for start := timeutil.Now(); timeutil.Since(start) < time.Second; time.Sleep(10 * time.Millisecond) {
					if raftStatus := replica2.RaftStatus(); raftStatus != nil {
						if term := raftStatus.Term; term > latestTerm {
							if latestTerm == 0 {
								latestTerm = term
							} else {
								t.Errorf("%d: raft term unexpectedly advanced: %d", i, term)
								break
							}
						}
					}
				}
			}
		}()
	}
}

// TestRaftAfterRemoveRange verifies that the raft state removes
// a remote node correctly after the Replica was removed from the Store.
func TestRaftAfterRemoveRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := startMultiTestContext(t, 3)
	defer mtc.Stop()

	// Make the split.
	splitArgs := adminSplitArgs(roachpb.KeyMin, []byte("b"))
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &splitArgs); err != nil {
		t.Fatal(err)
	}

	const rangeID = roachpb.RangeID(2)
	mtc.replicateRange(rangeID, 1, 2)

	mtc.unreplicateRange(rangeID, 2)
	mtc.unreplicateRange(rangeID, 1)

	// Wait for the removal to be processed.
	util.SucceedsSoon(t, func() error {
		for _, s := range mtc.stores[1:] {
			_, err := s.GetReplica(rangeID)
			if _, ok := err.(*roachpb.RangeNotFoundError); !ok {
				return errors.Wrapf(err, "range %d not yet removed from %s", rangeID, s)
			}
		}
		return nil
	})

	// Test that a coalesced heartbeat is ingested correctly.
	replica1 := roachpb.ReplicaDescriptor{
		ReplicaID: roachpb.ReplicaID(mtc.stores[1].StoreID()),
		NodeID:    roachpb.NodeID(mtc.stores[1].StoreID()),
		StoreID:   mtc.stores[1].StoreID(),
	}
	replica2 := roachpb.ReplicaDescriptor{
		ReplicaID: roachpb.ReplicaID(mtc.stores[2].StoreID()),
		NodeID:    roachpb.NodeID(mtc.stores[2].StoreID()),
		StoreID:   mtc.stores[2].StoreID(),
	}
	mtc.transports[2].SendAsync(&storage.RaftMessageRequest{
		RangeID:     0,
		ToReplica:   replica1,
		FromReplica: replica2,
		Message: raftpb.Message{
			Type: raftpb.MsgHeartbeat,
		},
		Heartbeats: []storage.RaftHeartbeat{
			{
				RangeID:       rangeID,
				FromReplicaID: replica2.ReplicaID,
				ToReplicaID:   replica1.ReplicaID,
			},
		},
	})
	// Execute another replica change to ensure that raft has processed
	// the heartbeat just sent.
	mtc.replicateRange(roachpb.RangeID(1), 1)

	// Expire leases to ensure any remaining intent resolutions can complete.
	// TODO(bdarnell): understand why some tests need this.
	mtc.expireLeases()
}

// TestRaftRemoveRace adds and removes a replica repeatedly in an attempt to
// reproduce a race (see #1911 and #9037).
func TestRaftRemoveRace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := startMultiTestContext(t, 10)
	defer mtc.Stop()

	const rangeID = roachpb.RangeID(1)
	// Up-replicate to a bunch of nodes which stresses a condition where a
	// replica created via a preemptive snapshot receives a message for a
	// previous incarnation of the replica (i.e. has a smaller replica ID) that
	// existed on the same store.
	mtc.replicateRange(rangeID, 1, 2, 3, 4, 5, 6, 7, 8, 9)

	for i := 0; i < 10; i++ {
		mtc.unreplicateRange(rangeID, 2)
		mtc.replicateRange(rangeID, 2)
	}
}

// TestStoreRangeRemoveDead verifies that if a store becomes dead, the
// ReplicateQueue will notice and remove any replicas on it.
func TestStoreRangeRemoveDead(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := &multiTestContext{}
	mtc.timeUntilStoreDead = storage.TestTimeUntilStoreDead
	mtc.Start(t, 3)
	defer mtc.Stop()

	// Replicate the range to all stores.
	replica := mtc.stores[0].LookupReplica(roachpb.RKeyMin, nil)
	mtc.replicateRange(replica.RangeID, 1, 2)

	for _, s := range mtc.stores {
		if err := s.GossipStore(context.Background()); err != nil {
			t.Fatal(err)
		}
	}

	rangeDesc := getRangeMetadata(roachpb.RKeyMin, mtc, t)
	if e, a := 3, len(rangeDesc.Replicas); e != a {
		t.Fatalf("expected %d replicas, only found %d, rangeDesc: %+v", e, a, rangeDesc)
	}

	// This can't use SucceedsSoon as using the backoff mechanic won't work
	// as it requires a specific cadence of re-gossiping the alive stores to
	// maintain their alive status.
	tickerDur := storage.TestTimeUntilStoreDead / 2
	ticker := time.NewTicker(tickerDur)
	defer ticker.Stop()

	maxTime := 5 * time.Second
	maxTimeout := time.After(maxTime)

	for len(getRangeMetadata(roachpb.RKeyMin, mtc, t).Replicas) > 2 {
		select {
		case <-maxTimeout:
			t.Fatalf("Failed to remove the dead replica within %s", maxTime)
		case <-ticker.C:
			mtc.manualClock.Increment(int64(tickerDur))

			// Keep gossiping the alive stores.
			if err := mtc.stores[0].GossipStore(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := mtc.stores[1].GossipStore(context.Background()); err != nil {
				t.Fatal(err)
			}

			// Force the repair queues on all alive stores to run.
			mtc.stores[0].ForceReplicationScanAndProcess()
			mtc.stores[1].ForceReplicationScanAndProcess()
		}
	}
}

// TestReplicateRogueRemovedNode ensures that a rogue removed node
// (i.e. a node that has been removed from the range but doesn't know
// it yet because it was down or partitioned away when it happened)
// cannot cause other removed nodes to recreate their ranges.
func TestReplicateRogueRemovedNode(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mtc := startMultiTestContext(t, 3)
	defer mtc.Stop()

	// First put the range on all three nodes.
	raftID := roachpb.RangeID(1)
	mtc.replicateRange(raftID, 1, 2)

	// Put some data in the range so we'll have something to test for.
	incArgs := incrementArgs([]byte("a"), 5)
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &incArgs); err != nil {
		t.Fatal(err)
	}

	// Wait for all nodes to catch up.
	mtc.waitForValues(roachpb.Key("a"), []int64{5, 5, 5})

	// Stop node 2; while it is down remove the range from nodes 2 and 1.
	mtc.stopStore(2)
	mtc.unreplicateRange(raftID, 2)
	mtc.unreplicateRange(raftID, 1)

	// Make a write on node 0; this will not be replicated because 0 is the only node left.
	incArgs = incrementArgs([]byte("a"), 11)
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &incArgs); err != nil {
		t.Fatal(err)
	}

	// Wait for the replica to be GC'd on node 1.
	// Store 0 has two writes, 1 has erased everything, and 2 still has the first write.
	// A single pass of ForceReplicaGCScanAndProcess is not enough, since the replica
	// may be recreated by a stray raft message, so we run the GC scan inside the loop.
	// TODO(bdarnell): if the call to RemoveReplica in replicaGCQueue.process can be
	// moved under the lock, then the GC scan can be moved out of this loop.
	util.SucceedsSoon(t, func() error {
		mtc.expireLeases()
		mtc.manualClock.Increment(int64(
			storage.ReplicaGCQueueInactivityThreshold) + 1)
		mtc.stores[1].ForceReplicaGCScanAndProcess()

		actual := mtc.readIntFromEngines(roachpb.Key("a"))
		expected := []int64{16, 0, 5}
		if !reflect.DeepEqual(expected, actual) {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	// Bring node 2 back up.
	mtc.restartStore(2)

	// Try to issue a command on node 2. It should not be able to commit
	// (so we add it asynchronously).
	var startWG sync.WaitGroup
	startWG.Add(1)
	var finishWG sync.WaitGroup
	finishWG.Add(1)

	rep, err := mtc.stores[2].GetReplica(raftID)
	if err != nil {
		t.Fatal(err)
	}
	replicaDesc, ok := rep.Desc().GetReplicaDescriptor(mtc.stores[2].StoreID())
	if !ok {
		t.Fatalf("ReplicaID %d not found", raftID)
	}
	go func() {
		incArgs := incrementArgs([]byte("a"), 23)
		startWG.Done()
		defer finishWG.Done()
		_, pErr := client.SendWrappedWith(
			context.Background(),
			mtc.stores[2],
			roachpb.Header{
				Replica:   replicaDesc,
				Timestamp: mtc.stores[2].Clock().Now(),
			}, &incArgs,
		)
		if _, ok := pErr.GetDetail().(*roachpb.RangeNotFoundError); !ok {
			// We're on a goroutine and passing the error out is awkward since
			// it would only surface at shutdown time. A panic ought to be good
			// enough to get visibility.
			panic(fmt.Sprintf("unexpected error: %v", pErr))
		}
	}()
	startWG.Wait()

	// Sleep a bit to let the command proposed on node 2 proceed if it's
	// going to. Prior to the introduction of replica tombstones, this
	// would lead to split-brain: Node 2 would wake up node 1 and they
	// would form a quorum, even though node 0 had removed them both.
	// Now the tombstone on node 1 prevents it from rejoining the rogue
	// copy of the group.
	time.Sleep(100 * time.Millisecond)
	util.SucceedsSoon(t, func() error {
		actual := mtc.readIntFromEngines(roachpb.Key("a"))
		// Normally, replica GC has not happened yet on store 2, so we
		// expect {16, 0, 5}. However, it is possible (on a
		// slow/overloaded machine) for the end of the ChangeReplicas
		// transaction to be queued up inside the raft transport for long
		// enough that it doesn't arrive until after store 2 has been
		// restarted, so it is able to trigger an early GC on the
		// restarted node, resulting in {16, 0, 0}.
		// TODO(bdarnell): When #5789 is fixed, the probabilities flip and
		// {16, 0, 0} becomes the expected case. When this happens
		// we should just combine this check with the following one.
		expected1 := []int64{16, 0, 5}
		expected2 := []int64{16, 0, 0}
		if !reflect.DeepEqual(expected1, actual) && !reflect.DeepEqual(expected2, actual) {
			return errors.Errorf("expected %v or %v, got %v", expected1, expected2, actual)
		}
		return nil
	})

	// Run garbage collection on node 2. The lack of an active lease holder
	// lease will cause GC to do a consistent range lookup, where it
	// will see that the range has been moved and delete the old
	// replica.
	mtc.expireLeases()
	mtc.manualClock.Increment(int64(
		storage.ReplicaGCQueueInactivityThreshold) + 1)
	mtc.stores[2].ForceReplicaGCScanAndProcess()
	mtc.waitForValues(roachpb.Key("a"), []int64{16, 0, 0})

	// Now that the group has been GC'd, the goroutine that was
	// attempting to write has finished (with an error).
	finishWG.Wait()
}

type disruptiveElectionTestHandler chan *roachpb.Error

func (disruptiveElectionTestHandler) HandleRaftRequest(
	_ context.Context, _ *storage.RaftMessageRequest, _ storage.RaftMessageResponseStream,
) *roachpb.Error {
	panic("unimplemented")
}

func (d disruptiveElectionTestHandler) HandleRaftResponse(
	ctx context.Context, resp *storage.RaftMessageResponse,
) error {
	switch val := resp.Union.GetValue().(type) {
	case *roachpb.Error:
		d <- val
	default:
		log.Fatalf(ctx, "unexpected response type %T", val)
	}
	return nil
}

func (disruptiveElectionTestHandler) HandleSnapshot(
	_ *storage.SnapshotRequest_Header, _ storage.SnapshotResponseStream,
) error {
	panic("unimplemented")
}

func TestReplicateRemovedNodeDisruptiveElection(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if storage.ProposerEvaluatedKVEnabled() {
		t.Skip("#10577")
	}

	mtc := startMultiTestContext(t, 4)
	defer mtc.Stop()

	// Move the first range from the first node to the other three.
	const rangeID = roachpb.RangeID(1)
	mtc.replicateRange(rangeID, 1, 2, 3)
	mtc.unreplicateRange(rangeID, 0)
	mtc.expireLeases()

	// Ensure that we have a stable lease and raft leader so we can tell if the
	// removed node causes a disruption. This is a three-step process.

	// 1. Write on the second node, to ensure that a lease has been
	// established after the first node's removal.
	key := roachpb.Key("a")
	value := int64(5)
	incArgs := incrementArgs(key, value)
	if _, err := client.SendWrapped(context.Background(), mtc.distSenders[1], &incArgs); err != nil {
		t.Fatal(err)
	}

	// 2. Wait for all nodes to process the increment (and therefore the
	// new lease).
	mtc.waitForValues(key, []int64{0, value, value, value})

	// 3. Wait for the lease holder to obtain raft leadership too.
	util.SucceedsSoon(t, func() error {
		req := &roachpb.LeaseInfoRequest{
			Span: roachpb.Span{
				Key: roachpb.KeyMin,
			},
		}
		reply, pErr := client.SendWrapped(context.Background(), mtc.distSenders[1], req)
		if pErr != nil {
			return pErr.GoError()
		}
		leaseReplica := reply.(*roachpb.LeaseInfoResponse).Lease.Replica.ReplicaID
		leadReplica := roachpb.ReplicaID(mtc.stores[1].RaftStatus(rangeID).Lead)
		if leaseReplica != leadReplica {
			return errors.Errorf("leaseReplica %s does not match leadReplica %s",
				leaseReplica, leadReplica)
		}

		return nil
	})

	// Save the current term, which is the latest among the live stores.
	findTerm := func() uint64 {
		var term uint64
		for i := 1; i < 4; i++ {
			s := mtc.stores[i].RaftStatus(rangeID)
			if s.Term > term {
				term = s.Term
			}
		}
		return term
	}
	term := findTerm()
	if term == 0 {
		t.Fatalf("expected non-zero term")
	}

	// replica0 is the one that  has been removed; replica1 is a current
	// member of the group.
	replica0 := roachpb.ReplicaDescriptor{
		ReplicaID: roachpb.ReplicaID(mtc.stores[0].StoreID()),
		NodeID:    roachpb.NodeID(mtc.stores[0].StoreID()),
		StoreID:   mtc.stores[0].StoreID(),
	}
	replica1 := roachpb.ReplicaDescriptor{
		ReplicaID: roachpb.ReplicaID(mtc.stores[1].StoreID()),
		NodeID:    roachpb.NodeID(mtc.stores[1].StoreID()),
		StoreID:   mtc.stores[1].StoreID(),
	}

	// Create a new transport for store 0. Error responses are passed
	// back along the same grpc stream as the request so it's ok that
	// there are two (this one and the one actually used by the store).
	transport0 := storage.NewRaftTransport(log.AmbientContext{},
		storage.GossipAddressResolver(mtc.gossips[0]),
		nil, /* grpcServer */
		mtc.rpcContext,
	)
	errChan := disruptiveElectionTestHandler(make(chan *roachpb.Error, 1))
	transport0.Listen(mtc.stores[0].StoreID(), errChan)

	// Simulate an election triggered by the removed node.
	transport0.SendAsync(&storage.RaftMessageRequest{
		RangeID:     rangeID,
		ToReplica:   replica1,
		FromReplica: replica0,
		Message: raftpb.Message{
			From: uint64(replica0.ReplicaID),
			To:   uint64(replica1.ReplicaID),
			Type: raftpb.MsgVote,
			Term: term + 1,
		},
	})

	// The receiver of this message should return an error.
	select {
	case pErr := <-errChan:
		switch pErr.GetDetail().(type) {
		case *roachpb.ReplicaTooOldError:
		default:
			t.Fatalf("unexpected error type %T: %s", pErr, pErr)
		}
	case <-time.After(time.Second):
		t.Fatal("did not get expected error")
	}

	// The message should have been discarded without triggering an
	// election or changing the term.
	newTerm := findTerm()
	if term != newTerm {
		t.Errorf("expected term to be constant, but changed from %v to %v", term, newTerm)
	}
}

func TestReplicaTooOldGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("#10189")

	sc := storage.TestStoreConfig(nil)
	sc.TestingKnobs.DisableScanner = true
	mtc := &multiTestContext{storeConfig: &sc}
	mtc.Start(t, 4)
	defer mtc.Stop()

	// Replicate the first range onto all of the nodes.
	const rangeID = 1
	mtc.replicateRange(rangeID, 1, 2, 3)

	// Put some data in the range so we'll have something to test for.
	incArgs := incrementArgs([]byte("a"), 5)
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &incArgs); err != nil {
		t.Fatal(err)
	}
	// Wait for all nodes to catch up.
	mtc.waitForValues(roachpb.Key("a"), []int64{5, 5, 5, 5})

	// Verify store 3 has the replica.
	if _, err := mtc.stores[3].GetReplica(rangeID); err != nil {
		t.Fatal(err)
	}

	// Stop node 3; while it is down remove the range from it. Since the node is
	// down it won't see the removal and won't clean up its replica.
	mtc.stopStore(3)
	mtc.unreplicateRange(rangeID, 3)

	// Perform another write.
	incArgs = incrementArgs([]byte("a"), 11)
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &incArgs); err != nil {
		t.Fatal(err)
	}
	mtc.waitForValues(roachpb.Key("a"), []int64{16, 16, 16, 5})

	// Restart node 3. The removed replica will start talking to the other
	// replicas and determine it needs to be GC'd.
	mtc.restartStore(3)

	// Because we lazily initialize Raft groups, we have to force the Raft group
	// to get created in order to get the replica talking to the other replicas.
	mtc.stores[3].EnqueueRaftUpdateCheck(rangeID)

	util.SucceedsSoon(t, func() error {
		replica, err := mtc.stores[3].GetReplica(rangeID)
		if err != nil {
			if _, ok := err.(*roachpb.RangeNotFoundError); ok {
				return nil
			}
			return err
		}
		return errors.Errorf("found %s, waiting for it to be GC'd", replica)
	})
}

func TestReplicaLazyLoad(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sc := storage.TestStoreConfig(nil)
	sc.RaftTickInterval = time.Millisecond // safe because there is only a single node
	sc.TestingKnobs.DisableScanner = true
	sc.TestingKnobs.DisablePeriodicGossips = true
	mtc := &multiTestContext{storeConfig: &sc}
	mtc.Start(t, 1)
	defer mtc.Stop()

	mtc.stopStore(0)
	mtc.restartStore(0)

	// Wait for a bunch of raft ticks.
	ticks := mtc.stores[0].Metrics().RaftTicks.Count
	for targetTicks := ticks() + 3; ticks() < targetTicks; {
		time.Sleep(time.Millisecond)
	}

	replica, err := mtc.stores[0].GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}
	if replica.RaftStatus() != nil {
		t.Fatalf("expected replica Raft group to be uninitialized")
	}
}

func TestReplicateReAddAfterDown(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mtc := startMultiTestContext(t, 3)
	defer mtc.Stop()

	downedStoreIdx := 2

	// First put the range on all three nodes.
	raftID := roachpb.RangeID(1)
	mtc.replicateRange(raftID, 1, 2)

	// Put some data in the range so we'll have something to test for.
	incArgs := incrementArgs([]byte("a"), 5)
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &incArgs); err != nil {
		t.Fatal(err)
	}

	// Wait for all nodes to catch up.
	mtc.waitForValues(roachpb.Key("a"), []int64{5, 5, 5})

	// Stop node 2; while it is down remove the range from it. Since the node is
	// down it won't see the removal and clean up its replica.
	mtc.stopStore(downedStoreIdx)
	mtc.unreplicateRange(raftID, 2)

	// Perform another write.
	incArgs = incrementArgs([]byte("a"), 11)
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &incArgs); err != nil {
		t.Fatal(err)
	}
	mtc.waitForValues(roachpb.Key("a"), []int64{16, 16, 5})

	// Bring it back up and re-add the range. There is a race when the
	// store applies its removal and re-addition back to back: the
	// replica may or may not have (asynchronously) garbage collected
	// its data in between. Whether the existing data is reused or the
	// replica gets recreated, the replica ID is changed by this
	// process. An ill-timed GC has been known to cause bugs including
	// https://github.com/cockroachdb/cockroach/issues/2873.
	mtc.restartStore(downedStoreIdx)
	mtc.replicateRange(raftID, downedStoreIdx)

	// The range should be synced back up.
	mtc.waitForValues(roachpb.Key("a"), []int64{16, 16, 16})
}

// TestLeaseHolderRemoveSelf verifies that a lease holder can remove itself
// without panicking and future access to the range returns a
// RangeNotFoundError (not RaftGroupDeletedError, and even before
// the ReplicaGCQueue has run).
func TestLeaseHolderRemoveSelf(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mtc := startMultiTestContext(t, 2)
	defer mtc.Stop()

	leaseHolder := mtc.stores[0]

	// Disable the replica GC queue. This verifies that the replica is
	// considered removed even before the gc queue has run, and also
	// helps avoid a deadlock at shutdown.
	leaseHolder.SetReplicaGCQueueActive(false)
	raftID := roachpb.RangeID(1)
	mtc.replicateRange(raftID, 1)
	// Remove the replica from first store.
	mtc.unreplicateRange(raftID, 0)
	getArgs := getArgs([]byte("a"))

	// Force the read command request a new lease.
	mtc.manualClock.Increment(leaseHolder.LeaseExpiration(mtc.clock))

	// Expect get a RangeNotFoundError.
	_, pErr := client.SendWrappedWith(context.Background(), rg1(leaseHolder), roachpb.Header{}, &getArgs)
	if _, ok := pErr.GetDetail().(*roachpb.RangeNotFoundError); !ok {
		t.Fatalf("expect get RangeNotFoundError, actual get %v ", pErr)
	}
}

// TestRemoveRangeWithoutGC ensures that we do not panic when a
// replica has been removed but not yet GC'd (and therefore
// does not have an active raft group).
func TestRemoveRangeWithoutGC(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sc := storage.TestStoreConfig(nil)
	sc.TestingKnobs.DisableReplicaGCQueue = true
	mtc := &multiTestContext{storeConfig: &sc}
	mtc.Start(t, 2)
	defer mtc.Stop()
	const rangeID roachpb.RangeID = 1
	mtc.replicateRange(rangeID, 1)
	mtc.unreplicateRange(rangeID, 0)

	// Wait for store 0 to process the removal. The in-memory replica
	// object still exists but store 0 is no longer present in the
	// configuration.
	util.SucceedsSoon(t, func() error {
		rep, err := mtc.stores[0].GetReplica(rangeID)
		if err != nil {
			return err
		}
		desc := rep.Desc()
		if len(desc.Replicas) != 1 {
			return errors.Errorf("range has %d replicas", len(desc.Replicas))
		}
		return nil
	})

	// The replica's data is still on disk.
	var desc roachpb.RangeDescriptor
	descKey := keys.RangeDescriptorKey(roachpb.RKeyMin)
	if ok, err := engine.MVCCGetProto(context.Background(), mtc.stores[0].Engine(), descKey,
		mtc.stores[0].Clock().Now(), true, nil, &desc); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected range descriptor to be present")
	}

	// Stop and restart the store. The primary motiviation for this test
	// is to ensure that the store does not panic on restart (as was
	// previously the case).
	mtc.stopStore(0)
	mtc.restartStore(0)

	// Initially, the in-memory Replica object is recreated from the
	// on-disk state.
	if _, err := mtc.stores[0].GetReplica(rangeID); err != nil {
		t.Fatal(err)
	}

	// Re-enable the GC queue to allow the replica to be destroyed
	// (after the simulated passage of time).
	mtc.expireLeases()
	mtc.manualClock.Increment(int64(storage.ReplicaGCQueueInactivityThreshold + 1))
	mtc.stores[0].SetReplicaGCQueueActive(true)
	mtc.stores[0].ForceReplicaGCScanAndProcess()

	util.SucceedsSoon(t, func() error {
		// The Replica object should be removed.
		if _, err := mtc.stores[0].GetReplica(rangeID); !testutils.IsError(err, "range [0-9]+ was not found") {
			return errors.Errorf("expected replica to be missing; got %v", err)
		}

		// And the data should no longer be on disk.
		if ok, err := engine.MVCCGetProto(context.Background(), mtc.stores[0].Engine(), descKey,
			mtc.stores[0].Clock().Now(), true, nil, &desc); err != nil {
			return err
		} else if ok {
			return errors.New("expected range descriptor to be absent")
		}
		return nil
	})
}

// TestCheckConsistencyMultiStore creates a Db with three stores ]
// with three way replication. A value is added to the Db, and a
// consistency check is run.
func TestCheckConsistencyMultiStore(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numStores = 3
	mtc := startMultiTestContext(t, numStores)
	defer mtc.Stop()
	// Setup replication of range 1 on store 0 to stores 1 and 2.
	mtc.replicateRange(1, 1, 2)

	// Write something to the DB.
	putArgs := putArgs([]byte("a"), []byte("b"))
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &putArgs); err != nil {
		t.Fatal(err)
	}

	// Run consistency check.
	checkArgs := roachpb.CheckConsistencyRequest{
		Span: roachpb.Span{
			// span of keys that include "a".
			Key:    []byte("a"),
			EndKey: []byte("aa"),
		},
	}
	if _, err := client.SendWrappedWith(context.Background(), rg1(mtc.stores[0]), roachpb.Header{
		Timestamp: mtc.stores[0].Clock().Now(),
	}, &checkArgs); err != nil {
		t.Fatal(err)
	}
}

func TestCheckInconsistent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sc := storage.TestStoreConfig(nil)
	mtc := &multiTestContext{storeConfig: &sc}
	// Store 0 will report a diff with inconsistent key "e".
	diffKey := []byte("e")
	var diffTimestamp hlc.Timestamp
	notifyReportDiff := make(chan struct{}, 1)
	sc.TestingKnobs.BadChecksumReportDiff =
		func(s roachpb.StoreIdent, diff []storage.ReplicaSnapshotDiff) {
			if s != mtc.Store(0).Ident {
				t.Errorf("BadChecksumReportDiff called from follower (StoreIdent = %s)", s)
				return
			}
			if len(diff) != 1 {
				t.Errorf("diff length = %d, diff = %v", len(diff), diff)
			}
			d := diff[0]
			if d.LeaseHolder || !bytes.Equal(diffKey, d.Key) || !diffTimestamp.Equal(d.Timestamp) {
				t.Errorf("diff = %v", d)
			}
			notifyReportDiff <- struct{}{}
		}
	// Store 0 will panic.
	notifyPanic := make(chan struct{}, 1)
	sc.TestingKnobs.BadChecksumPanic = func(s roachpb.StoreIdent) {
		if s != mtc.Store(0).Ident {
			t.Errorf("BadChecksumPanic called from follower (StoreIdent = %s)", s)
			return
		}
		notifyPanic <- struct{}{}
	}

	const numStores = 3
	mtc.Start(t, numStores)
	defer mtc.Stop()
	// Setup replication of range 1 on store 0 to stores 1 and 2.
	mtc.replicateRange(1, 1, 2)

	// Write something to the DB.
	pArgs := putArgs([]byte("a"), []byte("b"))
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &pArgs); err != nil {
		t.Fatal(err)
	}
	pArgs = putArgs([]byte("c"), []byte("d"))
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &pArgs); err != nil {
		t.Fatal(err)
	}

	// Write some arbitrary data only to store 1. Inconsistent key "e"!
	var val roachpb.Value
	val.SetInt(42)
	diffTimestamp = mtc.stores[1].Clock().Now()
	if err := engine.MVCCPut(
		context.Background(), mtc.stores[1].Engine(), nil, diffKey, diffTimestamp, val, nil,
	); err != nil {
		t.Fatal(err)
	}

	// Run consistency check.
	checkArgs := roachpb.CheckConsistencyRequest{
		Span: roachpb.Span{
			// span of keys that include "a" & "c".
			Key:    []byte("a"),
			EndKey: []byte("z"),
		},
	}
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &checkArgs); err != nil {
		t.Fatal(err)
	}
	select {
	case <-notifyReportDiff:
	case <-time.After(5 * time.Second):
		t.Fatal("CheckConsistency() failed to report a diff as expected")
	}
	select {
	case <-notifyPanic:
	case <-time.After(5 * time.Second):
		t.Fatal("CheckConsistency() failed to panic as expected")
	}
}

func TestTransferRaftLeadership(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numStores = 3
	sc := storage.TestStoreConfig(nil)
	// Suppress timeout-based elections (which also includes a previous
	// leader stepping down due to a quorum check). Running tests on a
	// heavily loaded CPU is enough to reach the raft election timeout
	// and cause leadership to change hands in ways this test doesn't
	// expect.
	sc.RaftElectionTimeoutTicks = 100000
	mtc := &multiTestContext{storeConfig: &sc}
	mtc.Start(t, numStores)
	defer mtc.Stop()
	store0 := mtc.Store(0)
	store1 := mtc.Store(1)

	key := roachpb.Key("a")

	{
		// Split off a range to avoid interacting with the initial splits.
		splitArgs := adminSplitArgs(key, key)
		if _, err := client.SendWrapped(context.Background(), mtc.distSenders[0], &splitArgs); err != nil {
			t.Fatal(err)
		}
	}

	repl := store0.LookupReplica(keys.MustAddr(key), nil)
	if repl == nil {
		t.Fatalf("no replica found for key '%s'", key)
	}
	mtc.replicateRange(repl.RangeID, 1, 2)

	getArgs := getArgs([]byte("a"))
	if _, pErr := client.SendWrappedWith(context.Background(), store0, roachpb.Header{RangeID: repl.RangeID}, &getArgs); pErr != nil {
		t.Fatalf("expect get nil, actual get %v ", pErr)
	}

	status := repl.RaftStatus()
	if status != nil && status.Lead != 1 {
		t.Fatalf("raft leader should be 1, but got status %+v", status)
	}

	// Force a read on Store 2 to request a new lease. Other moving parts in
	// the system could have requested another lease as well, so we
	// expire-request in a loop until we get our foot in the door.
	for {
		mtc.expireLeases()
		if _, pErr := client.SendWrappedWith(
			context.Background(),
			store1,
			roachpb.Header{RangeID: repl.RangeID},
			&getArgs,
		); pErr == nil {
			break
		} else {
			switch pErr.GetDetail().(type) {
			case *roachpb.NotLeaseHolderError, *roachpb.RangeNotFoundError:
			default:
				t.Fatal(pErr)
			}
		}
	}
	// Wait for raft leadership transferring to be finished.
	util.SucceedsSoon(t, func() error {
		status = repl.RaftStatus()
		if status.Lead != 2 {
			return errors.Errorf("expected raft leader be 2; got %d", status.Lead)
		}
		return nil
	})
}

// TestFailedPreemptiveSnapshot verifies that ChangeReplicas is
// aborted if we are unable to send a preemptive snapshot.
func TestFailedPreemptiveSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mtc := startMultiTestContext(t, 2)
	defer mtc.Stop()

	// Replicate a range onto the two stores. This replication is
	// important because if there was only one node to begin with, the
	// ChangeReplicas would fail because it was unable to achieve quorum
	// even if the preemptive snapshot failure were ignored.
	mtc.replicateRange(1, 1)

	// Now try to add a third. It should fail because we cannot send a
	// preemptive snapshot to it.
	rep, err := mtc.stores[0].GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}
	const expErr = "aborted due to failed preemptive snapshot: unknown peer 3"
	if err := rep.ChangeReplicas(context.Background(), roachpb.ADD_REPLICA,
		roachpb.ReplicaDescriptor{NodeID: 3, StoreID: 3},
		rep.Desc()); !testutils.IsError(err, expErr) {
		t.Fatalf("expected %s; got %v", expErr, err)
	} else if !storage.IsPreemptiveSnapshotError(err) {
		t.Fatalf("expected preemptive snapshot failed error; got %T: %v", err, err)
	}
}

// Test that a single blocked replica does not block other replicas.
func TestRaftBlockedReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sc := storage.TestStoreConfig(nil)
	sc.TestingKnobs.DisableScanner = true
	mtc := &multiTestContext{storeConfig: &sc}
	mtc.Start(t, 3)
	defer mtc.Stop()

	// Create 2 ranges by splitting range 1.
	splitArgs := adminSplitArgs(roachpb.KeyMin, []byte("b"))
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &splitArgs); err != nil {
		t.Fatal(err)
	}

	// Replicate range 1 to all 3 nodes. This ensures the usage of the network.
	mtc.replicateRange(1, 1, 2)

	// Lock range 2 for raft processing.
	rep, err := mtc.stores[0].GetReplica(2)
	if err != nil {
		t.Fatal(err)
	}

	// NB: We perform the actual locking on a different goroutine in order to
	// workaround a spurious inconsistent lock order warning when running with
	// TAGS=deadlock. The issue is that we're grabbing Replica 2's raftMu and
	// then later Replica 1's from the same goroutine due to the direct calling
	// of client.SendWrapped down the callstack into the Replica code (via the
	// local RPC optimization).
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		rep.RaftLock()
		wg.Done()
	}()
	wg.Wait()
	defer rep.RaftUnlock()

	// Verify that we're still ticking the non-blocked replica.
	ticks := mtc.stores[0].Metrics().RaftTicks.Count
	for targetTicks := ticks() + 3; ticks() < targetTicks; {
		time.Sleep(time.Millisecond)
	}

	// Verify we can still perform operations on the non-blocked replica.
	incArgs := incrementArgs([]byte("a"), 5)
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &incArgs); err != nil {
		t.Fatal(err)
	}
	mtc.waitForValues(roachpb.Key("a"), []int64{5, 5, 5})
}

// Test that ranges quiesce and if a follower unquiesces the leader is woken
// up.
func TestRangeQuiescence(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sc := storage.TestStoreConfig(nil)
	sc.TestingKnobs.DisableScanner = true
	sc.TestingKnobs.DisablePeriodicGossips = true
	mtc := &multiTestContext{storeConfig: &sc}
	mtc.Start(t, 3)
	defer mtc.Stop()

	stopNodeLivenessHeartbeats(mtc)

	// Replica range 1 to all 3 nodes.
	mtc.replicateRange(1, 1, 2)

	waitForQuiescence := func(rangeID roachpb.RangeID) {
		util.SucceedsSoon(t, func() error {
			for _, s := range mtc.stores {
				rep, err := s.GetReplica(rangeID)
				if err != nil {
					t.Fatal(err)
				}
				if !rep.IsQuiescent() {
					return errors.Errorf("%s not quiescent", rep)
				}
			}
			return nil
		})
	}

	// Wait for the range to quiesce.
	waitForQuiescence(1)

	// Find the leader replica.
	var rep *storage.Replica
	var leaderIdx int
	for leaderIdx = range mtc.stores {
		var err error
		if rep, err = mtc.stores[leaderIdx].GetReplica(1); err != nil {
			t.Fatal(err)
		}
		if rep.RaftStatus().SoftState.RaftState == raft.StateLeader {
			break
		}
	}

	// Unquiesce a follower range, this should "wake the leader" and not result
	// in an election.
	followerIdx := (leaderIdx + 1) % len(mtc.stores)
	mtc.stores[followerIdx].EnqueueRaftUpdateCheck(1)

	// Wait for a bunch of ticks to occur which will allow the follower time to
	// campaign.
	ticks := mtc.stores[followerIdx].Metrics().RaftTicks.Count
	for targetTicks := ticks() + int64(2*sc.RaftElectionTimeoutTicks); ticks() < targetTicks; {
		time.Sleep(time.Millisecond)
	}

	// Wait for the range to quiesce again.
	waitForQuiescence(1)

	// The leadership should not have changed.
	if state := rep.RaftStatus().SoftState.RaftState; state != raft.StateLeader {
		t.Fatalf("%s should be the leader: %s", rep, state)
	}
}

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
	"github.com/kr/pretty"
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
	storeCfg := storage.TestStoreConfig()
	storeCfg.TestingKnobs.DisableSplitQueue = true

	rangeID := roachpb.RangeID(1)
	splitKey := roachpb.Key("m")
	key1 := roachpb.Key("a")
	key2 := roachpb.Key("z")

	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
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
		store := createTestStoreWithEngine(t, eng, clock, true, storeCfg, stopper)

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
	store := createTestStoreWithEngine(t, eng, clock, false, storeCfg, engineStopper)

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
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	engineStopper := stop.NewStopper()
	defer engineStopper.Stop()
	eng := engine.NewInMem(roachpb.Attributes{}, 1<<20)
	engineStopper.AddCloser(eng)

	numIncrements := 0

	func() {
		stopper := stop.NewStopper()
		defer stopper.Stop()
		storeCfg := storage.TestStoreConfig()
		storeCfg.TestingKnobs.TestingCommandFilter =
			func(filterArgs storagebase.FilterArgs) *roachpb.Error {
				_, ok := filterArgs.Req.(*roachpb.IncrementRequest)
				if ok && filterArgs.Req.Header().Key.Equal(roachpb.Key("a")) {
					numIncrements++
				}
				return nil
			}
		store := createTestStoreWithEngine(t, eng, clock, true, storeCfg, stopper)

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

	// Recover from the engine.
	store := createTestStoreWithEngine(
		t, eng, clock, false, storage.TestStoreConfig(), engineStopper)

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

	rng, err := mtc.stores[0].GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	if err := rng.ChangeReplicas(
		context.Background(),
		roachpb.ADD_REPLICA,
		roachpb.ReplicaDescriptor{
			NodeID:  mtc.stores[1].Ident.NodeID,
			StoreID: mtc.stores[1].Ident.StoreID,
		},
		rng.Desc(),
	); err != nil {
		t.Fatal(err)
	}
	// Verify no intent remains on range descriptor key.
	key := keys.RangeDescriptorKey(rng.Desc().StartKey)
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

	sc := storage.TestStoreConfig()
	// Disable periodic gossip activities. The periodic gossiping of the first
	// range can cause spurious lease transfers which cause this test to fail.
	sc.TestingKnobs.DisablePeriodicGossips = true
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
		rng, err := store.GetReplica(1)
		if err != nil {
			t.Fatal(err)
		}
		desc := rng.Desc()
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

	sc := storage.TestStoreConfig()
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

	rng, err := mtc.stores[0].GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	if err := rng.ChangeReplicas(
		context.Background(),
		roachpb.ADD_REPLICA,
		roachpb.ReplicaDescriptor{
			NodeID:  mtc.stores[1].Ident.NodeID,
			StoreID: mtc.stores[1].Ident.StoreID,
		},
		rng.Desc(),
	); !testutils.IsError(err, "boom") {
		t.Fatalf("did not get expected error: %v", err)
	}

	// After the aborted transaction, r.Desc was not updated.
	// TODO(bdarnell): expose and inspect raft's internal state.
	if replicas := rng.Desc().Replicas; len(replicas) != 1 {
		t.Fatalf("expected 1 replica, found %v", replicas)
	}

	// The pending config change flag was cleared, so a subsequent attempt
	// can succeed.
	runFilter.Store(false)

	// The first failed replica change has laid down intents. Make sure those
	// are pushable by making the transaction abandoned.
	mtc.manualClock.Increment(10 * base.DefaultHeartbeatInterval.Nanoseconds())

	if err := rng.ChangeReplicas(
		context.Background(),
		roachpb.ADD_REPLICA,
		roachpb.ReplicaDescriptor{
			NodeID:  mtc.stores[1].Ident.NodeID,
			StoreID: mtc.stores[1].Ident.StoreID,
		},
		rng.Desc(),
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

// We can truncate the old log entries and a new replica will be brought up from a snapshot.
func TestReplicateAfterTruncation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := startMultiTestContext(t, 2)
	defer mtc.Stop()

	rng, err := mtc.stores[0].GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	// Issue a command on the first node before replicating.
	incArgs := incrementArgs([]byte("a"), 5)
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &incArgs); err != nil {
		t.Fatal(err)
	}

	// Get that command's log index.
	index, err := rng.GetLastIndex()
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
	if err := rng.ChangeReplicas(
		context.Background(),
		roachpb.ADD_REPLICA,
		roachpb.ReplicaDescriptor{
			NodeID:  mtc.stores[1].Ident.NodeID,
			StoreID: mtc.stores[1].Ident.StoreID,
		},
		rng.Desc(),
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

	rng2, err := mtc.stores[1].GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	util.SucceedsSoon(t, func() error {
		if mvcc, mvcc2 := rng.GetMVCCStats(), rng2.GetMVCCStats(); mvcc2 != mvcc {
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
	rng, err := mtc.stores[0].GetReplica(1)
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

	index, err := rng.GetLastIndex()
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

// TestConcurrentRaftSnapshots tests that snapshots still work correctly when
// Raft requests multiple non-preemptive snapshots at the same time. This
// situation occurs when two replicas need snapshots at the same time.
func TestConcurrentRaftSnapshots(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := startMultiTestContext(t, 5)
	defer mtc.Stop()
	rng, err := mtc.stores[0].GetReplica(1)
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

	index, err := rng.GetLastIndex()
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
			sc := storage.TestStoreConfig()
			sc.TestingKnobs = c
			// Disable periodic gossip tasks which can move the range 1 lease
			// unexpectedly.
			sc.TestingKnobs.DisablePeriodicGossips = true
			mtc := &multiTestContext{storeConfig: &sc}
			mtc.Start(t, 3)
			defer mtc.Stop()

			rangeID := roachpb.RangeID(1)
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
			rng, err := mtc.stores[0].GetReplica(1)
			if err != nil {
				t.Fatal(err)
			}
			index, err := rng.GetLastIndex()
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
		}
		return nil
	})
}

// TestStoreRangeCorruptionChangeReplicas verifies that the replication queue
// will notice corrupted replicas and replace them.
func TestStoreRangeCorruptionChangeReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numReplicas = 3
	const extraStores = 3

	sc := storage.TestStoreConfig()
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
			storage.NewReplicaCorruptionError(errors.New("test")),
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

		args := putArgs(roachpb.Key("boom"), []byte("value"))
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

	rangeID := roachpb.RangeID(1)
	// Replicate the range to store 1.
	mtc.replicateRange(rangeID, 1)
	// Unreplicate the from from store 0.
	mtc.unreplicateRange(rangeID, 0)
	// Replicate the range to store 2. The first range is no longer available on
	// store 1, and this command will fail if that situation is not properly
	// supported.
	mtc.replicateRange(rangeID, 2)
}

// TestStoreRangeDownReplicate verifies that the replication queue will notice
// over-replicated ranges and remove replicas from them.
func TestStoreRangeDownReplicate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := startMultiTestContext(t, 5)
	defer mtc.Stop()
	mtc.initGossipNetwork()
	store0 := mtc.stores[0]

	// Split off a range from the initial range for testing; there are
	// complications if the metadata ranges are removed from store 1, this
	// simplifies the test.
	splitKey := roachpb.Key("m")
	rightKey := roachpb.Key("z")
	{
		replica := store0.LookupReplica(roachpb.RKeyMin, nil)
		mtc.replicateRange(replica.RangeID, 1, 2)
		desc := replica.Desc()
		splitArgs := adminSplitArgs(splitKey, splitKey)
		if _, err := replica.AdminSplit(context.Background(), splitArgs, desc); err != nil {
			t.Fatal(err)
		}
	}
	// Replicate the new range to all five stores.
	rightKeyAddr, err := keys.Addr(rightKey)
	if err != nil {
		t.Fatal(err)
	}
	replica := store0.LookupReplica(rightKeyAddr, nil)
	desc := replica.Desc()
	mtc.replicateRange(desc.RangeID, 3, 4)

	maxTimeout := time.After(10 * time.Second)
	succeeded := false
	i := 0
	for !succeeded {
		select {
		case <-maxTimeout:
			t.Fatalf("Failed to achieve proper replication within 10 seconds")
		case <-time.After(10 * time.Millisecond):
			rangeDesc := getRangeMetadata(rightKeyAddr, mtc, t)
			if count := len(rangeDesc.Replicas); count < 3 {
				t.Fatalf("Removed too many replicas; expected at least 3 replicas, found %d", count)
			} else if count == 3 {
				succeeded = true
				break
			}

			// Cycle the lease to the next replica (on the next store) if that
			// replica still exists. This avoids the condition in which we try
			// to continuously remove the replica on a store when
			// down-replicating while it also still holds the lease.
			for {
				i++
				if i >= len(mtc.stores) {
					i = 0
				}
				rep := mtc.stores[i].LookupReplica(rightKeyAddr, nil)
				if rep != nil {
					mtc.expireLeases()
					// Force the read command request a new lease.
					getArgs := getArgs(rightKey)
					if _, err := client.SendWrapped(context.Background(), mtc.distSenders[i], &getArgs); err != nil {
						t.Fatal(err)
					}
					mtc.stores[i].ForceReplicationScanAndProcess()
					break
				}
			}
		}
	}

	// Expire range leases one more time, so that any remaining resolutions can
	// get a range lease.
	// TODO(bdarnell): understand why some tests need this.
	mtc.expireLeases()
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

	rangeID := roachpb.RangeID(1)
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
	sc := storage.TestStoreConfig()
	// Don't timeout raft leaders or range leases (see the relation between
	// RaftElectionTimeoutTicks and rangeLeaseActiveDuration). This test expects
	// mtc.stores[0] to hold the range lease for range 1.
	sc.RaftElectionTimeoutTicks = 1000000
	mtc := &multiTestContext{storeConfig: &sc}
	mtc.Start(t, 3)
	defer mtc.Stop()

	key := roachpb.Key("a")

	// Replicate the initial range to all three nodes.
	rangeID := roachpb.RangeID(1)
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
		rng, err := mtc.stores[0].GetReplica(rangeID)
		if err != nil {
			t.Fatal(err)
		}
		index, err := rng.GetLastIndex()
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
	sc := storage.TestStoreConfig()
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
	rangeID := roachpb.RangeID(1)
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

	rangeID := roachpb.RangeID(1)
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

	rangeID := roachpb.RangeID(1)
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
				if err := store0.RemoveReplica(replica2, *replica2.Desc(), true); err != nil {
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

// TestRangeDescriptorSnapshotRace calls Snapshot() repeatedly while
// transactions are performed on the range descriptor.
//
// TODO(tamird): Refactor test so that snapshots are generated in the main
// goroutine and  random splits are in the non-main goroutines. that gives the
// test a more natural stopping condition.
func TestRangeDescriptorSnapshotRace(t *testing.T) {
	defer leaktest.AfterTest(t)()

	errCh := make(chan error, 1)
	defer func() {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}()

	mtc := startMultiTestContext(t, 1)
	defer mtc.Stop()

	stopper := stop.NewStopper()
	defer stopper.Stop()
	// Call Snapshot() in a loop and ensure it never fails.
	work := func(key roachpb.RKey) error {
		rng := mtc.stores[0].LookupReplica(key, nil)
		if rng == nil {
			return errors.Errorf("failed to look up replica for %s", key)
		}
		if _, err := rng.GetSnapshot(context.Background()); err != nil {
			return errors.Wrapf(err, "failed to snapshot range %s: %s", rng, key)
		}
		rng.CloseOutSnap()
		return nil
	}

	stopper.RunWorker(func() {
		defer close(errCh)
		for {
			select {
			case <-stopper.ShouldQuiesce():
				return
			default:
			}
			for _, key := range []roachpb.RKey{roachpb.RKeyMin, roachpb.RKey("Z")} {
				if err := work(key); err != nil {
					errCh <- err
					return
				}
			}
		}
	})

	// Split the range repeatedly, carving chunks off the end of the
	// initial range.  The bug that this test was designed to find
	// usually occurred within the first 5 iterations.
	for i := 20; i > 0; i-- {
		rng := mtc.stores[0].LookupReplica(roachpb.RKeyMin, nil)
		if rng == nil {
			t.Fatal("failed to look up min range")
		}
		desc := rng.Desc()
		args := adminSplitArgs(roachpb.KeyMin, []byte(fmt.Sprintf("A%03d", i)))
		if _, err := rng.AdminSplit(context.Background(), args, desc); err != nil {
			t.Fatal(err)
		}
	}

	// Split again, carving chunks off the beginning of the final range.
	for i := 0; i < 20; i++ {
		rng := mtc.stores[0].LookupReplica(roachpb.RKey("Z"), nil)
		if rng == nil {
			t.Fatal("failed to look up max range")
		}
		desc := rng.Desc()
		args := adminSplitArgs(roachpb.KeyMin, []byte(fmt.Sprintf("B%03d", i)))
		if _, err := rng.AdminSplit(context.Background(), args, desc); err != nil {
			t.Fatal(err)
		}
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

	rangeID := roachpb.RangeID(2)
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

// TestRaftRemoveRace adds and removes a replica repeatedly in an
// attempt to reproduce a race
// (https://github.com/cockroachdb/cockroach/issues/1911). Note that
// 10 repetitions is not enough to reliably reproduce the problem, but
// it's better than any other tests we have for this (increasing the
// number of repetitions adds an unacceptable amount of test runtime).
func TestRaftRemoveRace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := startMultiTestContext(t, 3)
	defer mtc.Stop()

	rangeID := roachpb.RangeID(1)
	mtc.replicateRange(rangeID, 1, 2)

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

// TestStoreRangeRebalance verifies that the replication queue will take
// rebalancing opportunities and add a new replica on another store.
func TestStoreRangeRebalance(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// TODO(peter,bram): clean up this test so this isn't required and unexport
	// storage.RebalanceThreshold.
	defer func(threshold float64) {
		storage.RebalanceThreshold = threshold
	}(storage.RebalanceThreshold)
	storage.RebalanceThreshold = 0

	// Start multiTestContext with replica rebalancing enabled.
	sc := storage.TestStoreConfig()
	sc.AllocatorOptions = storage.AllocatorOptions{
		AllowRebalance: true,
		Deterministic:  true,
	}
	mtc := &multiTestContext{storeConfig: &sc}

	mtc.Start(t, 6)
	defer mtc.Stop()

	splitKey := roachpb.Key("split")
	splitArgs := adminSplitArgs(roachpb.KeyMin, splitKey)
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &splitArgs); err != nil {
		t.Fatal(err)
	}

	// The setup for this test is to have two ranges like so:
	// s1:r1, s2:r1r2, s3:r1, s4:r2, s5:r2, s6:-
	// and to rebalance range 1 away from store 2 to store 6:
	// s1:r1, s2:r2, s3:r1, s4:r2, s5:r2, s6:r1

	replica1 := mtc.stores[0].LookupReplica(roachpb.RKeyMin, nil)
	mtc.replicateRange(replica1.Desc().RangeID, 1, 2)

	replica2Key := roachpb.RKey(splitKey)
	replica2 := mtc.stores[0].LookupReplica(replica2Key, nil)
	mtc.replicateRange(replica2.Desc().RangeID, 1, 3, 4)
	mtc.unreplicateRange(replica2.Desc().RangeID, 0)

	countReplicas := func() map[roachpb.StoreID]int {
		counts := make(map[roachpb.StoreID]int)
		rangeDescA := getRangeMetadata(roachpb.RKeyMin, mtc, t)
		for _, repl := range rangeDescA.Replicas {
			counts[repl.StoreID]++
		}
		rangeDescB := getRangeMetadata(replica2Key, mtc, t)
		for _, repl := range rangeDescB.Replicas {
			counts[repl.StoreID]++
		}
		return counts
	}

	// Check the initial conditions.
	expectedStart := map[roachpb.StoreID]int{
		roachpb.StoreID(1): 1,
		roachpb.StoreID(2): 2,
		roachpb.StoreID(3): 1,
		roachpb.StoreID(4): 1,
		roachpb.StoreID(5): 1,
	}
	actualStart := countReplicas()
	if !reflect.DeepEqual(expectedStart, actualStart) {
		t.Fatalf("replicas are not distributed as expected %s", pretty.Diff(expectedStart, actualStart))
	}

	expected := map[roachpb.StoreID]int{
		roachpb.StoreID(1): 1,
		roachpb.StoreID(2): 1,
		roachpb.StoreID(3): 1,
		roachpb.StoreID(4): 1,
		roachpb.StoreID(5): 1,
		roachpb.StoreID(6): 1,
	}

	mtc.initGossipNetwork()
	util.SucceedsSoon(t, func() error {
		// As of this writing, replicas which hold their range's lease cannot
		// be removed; forcefully transfer the lease for range 1 to another
		// store to allow store 2's replica to be removed.
		if err := mtc.transferLease(replica1.RangeID, mtc.stores[0]); err != nil {
			t.Fatal(err)
		}

		// It takes at least two passes to achieve the final result. In the
		// first pass, we add the replica to store 6. In the second pass, we
		// remove the replica from store 2. Note that it can also take some time
		// for the snapshot to arrive.
		mtc.stores[0].ForceReplicationScanAndProcess()

		// Gossip the stores so that the store pools are up to date. Note that
		// there might be a delay between the call below and the asynchronous
		// update of the store pools.
		mtc.gossipStores()

		// Exit when all stores have a single replica.
		actual := countReplicas()
		if !reflect.DeepEqual(expected, actual) {
			return errors.Errorf("replicas are not distributed as expected %s", pretty.Diff(expected, actual))
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

func TestReplicateRemovedNodeDisruptiveElection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// The change to error reporting means that we can no longer trap
	// transport errors separately from error messages and send them to
	// errChan.
	t.Skip("TODO(bdarnell): flaky (#8308), and needs update for change to raft transport error reporting")

	mtc := startMultiTestContext(t, 4)
	defer mtc.Stop()

	// Move the first range from the first node to the other three.
	rangeID := roachpb.RangeID(1)
	mtc.replicateRange(rangeID, 1, 2, 3)
	mtc.unreplicateRange(rangeID, 0)
	mtc.expireLeases()

	// Write on the second node, to ensure that the other nodes have
	// established a lease after the first node's removal.
	incArgs := incrementArgs([]byte("a"), 5)
	if _, err := client.SendWrapped(context.Background(), mtc.distSenders[1], &incArgs); err != nil {
		t.Fatal(err)
	}

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
	// Simulate an election triggered by the removed node.
	errChan := make(chan error)
	mtc.transports[0].SendAsync(&storage.RaftMessageRequest{
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

	if err := <-errChan; !testutils.IsError(err, "sender replica too old, discarding message") {
		t.Fatalf("got unexpected error: %v", err)
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

	sc := storage.TestStoreConfig()
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

	sc := storage.TestStoreConfig()
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
	mtc.stopStore(2)
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
	mtc.restartStore(2)
	mtc.replicateRange(raftID, 2)

	// The range should be synced back up.
	mtc.waitForValues(roachpb.Key("a"), []int64{16, 16, 16})
}

// TestLeaderRemoveSelf verifies that a lease holder can remove itself
// without panicking and future access to the range returns a
// RangeNotFoundError (not RaftGroupDeletedError, and even before
// the ReplicaGCQueue has run).
func TestLeaderRemoveSelf(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mtc := startMultiTestContext(t, 2)
	defer mtc.Stop()
	// Disable the replica GC queue. This verifies that the replica is
	// considered removed even before the gc queue has run, and also
	// helps avoid a deadlock at shutdown.
	mtc.stores[0].SetReplicaGCQueueActive(false)
	raftID := roachpb.RangeID(1)
	mtc.replicateRange(raftID, 1)
	// Remove the replica from first store.
	mtc.unreplicateRange(raftID, 0)
	getArgs := getArgs([]byte("a"))

	// Force the read command request a new lease.
	clock := mtc.clocks[0]
	header := roachpb.Header{
		Timestamp: clock.Update(
			clock.Now().Add(mtc.stores[0].LeaseExpiration(clock), 0),
		),
	}

	// Expect get a RangeNotFoundError.
	_, pErr := client.SendWrappedWith(context.Background(), rg1(mtc.stores[0]), header, &getArgs)
	if _, ok := pErr.GetDetail().(*roachpb.RangeNotFoundError); !ok {
		t.Fatalf("expect get RangeNotFoundError, actual get %v ", pErr)
	}
}

// TestRemoveRangeWithoutGC ensures that we do not panic when a
// replica has been removed but not yet GC'd (and therefore
// does not have an active raft group).
func TestRemoveRangeWithoutGC(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sc := storage.TestStoreConfig()
	sc.TestingKnobs.DisableScanner = true
	mtc := &multiTestContext{storeConfig: &sc}
	mtc.Start(t, 2)
	defer mtc.Stop()
	const rangeID roachpb.RangeID = 1
	mtc.replicateRange(rangeID, 1)
	mtc.unreplicateRange(rangeID, 0)

	// Wait for store 0 to process the removal.
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

	// The replica's data is still on disk even though the Replica
	// object is removed.
	var desc roachpb.RangeDescriptor
	descKey := keys.RangeDescriptorKey(roachpb.RKeyMin)
	if ok, err := engine.MVCCGetProto(context.Background(), mtc.stores[0].Engine(), descKey,
		mtc.stores[0].Clock().Now(), true, nil, &desc); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected range descriptor to be present")
	}

	// Stop and restart the store to reset the replica's raftGroup
	// pointer to nil. As long as the store has not been restarted it
	// can continue to use its last known replica ID.
	mtc.stopStore(0)
	mtc.restartStore(0)

	util.SucceedsSoon(t, func() error {
		// The Replica object should be removed.
		if _, err := mtc.stores[0].GetReplica(rangeID); err == nil {
			return errors.Errorf("expected replica to be missing")
		}

		// And the data should no longer be on disk.
		if ok, err := engine.MVCCGetProto(context.Background(), mtc.stores[0].Engine(), descKey,
			mtc.stores[0].Clock().Now(), true, nil, &desc); err != nil {
			return err
		} else if ok {
			return errors.Errorf("expected range descriptor to be absent")
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

	sc := storage.TestStoreConfig()
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
	diffTimestamp = mtc.stores[1].Clock().Timestamp()
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
	sc := storage.TestStoreConfig()
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

	rng := store0.LookupReplica(keys.MustAddr(key), nil)
	if rng == nil {
		t.Fatalf("no replica found for key '%s'", key)
	}
	mtc.replicateRange(rng.RangeID, 1, 2)

	getArgs := getArgs([]byte("a"))
	if _, pErr := client.SendWrappedWith(context.Background(), store0, roachpb.Header{RangeID: rng.RangeID}, &getArgs); pErr != nil {
		t.Fatalf("expect get nil, actual get %v ", pErr)
	}

	status := rng.RaftStatus()
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
			roachpb.Header{RangeID: rng.RangeID},
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
		status = rng.RaftStatus()
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
	}
}

// Test that a single blocked replica does not block other replicas.
func TestRaftBlockedReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sc := storage.TestStoreConfig()
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

	sc := storage.TestStoreConfig()
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

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
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/testutils/gossiputil"
	"github.com/cockroachdb/cockroach/testutils/storageutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
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
	defer config.TestingDisableTableSplits()()
	rangeID := roachpb.RangeID(1)
	splitKey := roachpb.Key("m")
	key1 := roachpb.Key("a")
	key2 := roachpb.Key("z")

	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	engineStopper := stop.NewStopper()
	defer engineStopper.Stop()
	eng := engine.NewInMem(roachpb.Attributes{}, 1<<20, engineStopper)
	var rangeID2 roachpb.RangeID

	get := func(store *storage.Store, rangeID roachpb.RangeID, key roachpb.Key) int64 {
		args := getArgs(key)
		resp, err := client.SendWrappedWith(rg1(store), nil, roachpb.Header{
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
		store := createTestStoreWithEngine(t, eng, clock, true, nil, stopper)

		increment := func(rangeID roachpb.RangeID, key roachpb.Key, value int64) (*roachpb.IncrementResponse, *roachpb.Error) {
			args := incrementArgs(key, value)
			resp, err := client.SendWrappedWith(rg1(store), nil, roachpb.Header{
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
		if _, err := client.SendWrapped(rg1(store), nil, &splitArgs); err != nil {
			t.Fatal(err)
		}
		rangeID2 = store.LookupReplica(roachpb.RKey(key2), nil).RangeID
		if rangeID2 == rangeID {
			t.Errorf("got same range id after split")
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
	store := createTestStoreWithEngine(t, eng, clock, false, nil, engineStopper)

	// Raft processing is initialized lazily; issue a no-op write request on each key to
	// ensure that is has been started.
	incArgs := incrementArgs(key1, 0)
	if _, err := client.SendWrapped(rg1(store), nil, &incArgs); err != nil {
		t.Fatal(err)
	}
	incArgs = incrementArgs(key2, 0)
	if _, err := client.SendWrappedWith(rg1(store), nil, roachpb.Header{
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
	eng := engine.NewInMem(roachpb.Attributes{}, 1<<20, engineStopper)

	numIncrements := 0

	func() {
		stopper := stop.NewStopper()
		defer stopper.Stop()
		sCtx := storage.TestStoreContext()
		sCtx.TestingKnobs.TestingCommandFilter =
			func(filterArgs storageutils.FilterArgs) *roachpb.Error {
				_, ok := filterArgs.Req.(*roachpb.IncrementRequest)
				if ok && filterArgs.Req.Header().Key.Equal(roachpb.Key("a")) {
					numIncrements++
				}
				return nil
			}
		store := createTestStoreWithEngine(t, eng, clock, true, &sCtx, stopper)

		// Write a bytes value so the increment will fail.
		putArgs := putArgs(roachpb.Key("a"), []byte("asdf"))
		if _, err := client.SendWrapped(rg1(store), nil, &putArgs); err != nil {
			t.Fatal(err)
		}

		// Try and fail to increment the key. It is important for this test that the
		// failure be the last thing in the raft log when the store is stopped.
		incArgs := incrementArgs(roachpb.Key("a"), 42)
		if _, err := client.SendWrapped(rg1(store), nil, &incArgs); err == nil {
			t.Fatal("did not get expected error")
		}
	}()

	if numIncrements != 1 {
		t.Fatalf("expected 1 increments; was %d", numIncrements)
	}

	// Recover from the engine.
	store := createTestStoreWithEngine(t, eng, clock, false, nil, engineStopper)

	// Issue a no-op write to lazily initialize raft on the range.
	incArgs := incrementArgs(roachpb.Key("b"), 0)
	if _, err := client.SendWrapped(rg1(store), nil, &incArgs); err != nil {
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
	if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &incArgs); err != nil {
		t.Fatal(err)
	}

	rng, err := mtc.stores[0].GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	if err := rng.ChangeReplicas(roachpb.ADD_REPLICA,
		roachpb.ReplicaDescriptor{
			NodeID:  mtc.stores[1].Ident.NodeID,
			StoreID: mtc.stores[1].Ident.StoreID,
		}, rng.Desc()); err != nil {
		t.Fatal(err)
	}
	// Verify no intent remains on range descriptor key.
	key := keys.RangeDescriptorKey(rng.Desc().StartKey)
	desc := roachpb.RangeDescriptor{}
	if ok, err := engine.MVCCGetProto(context.Background(), mtc.stores[0].Engine(), key, mtc.stores[0].Clock().Now(), true, nil, &desc); !ok || err != nil {
		t.Fatalf("fetching range descriptor yielded %t, %s", ok, err)
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
			if ok, err := engine.MVCCGetProto(context.Background(), mtc.stores[0].Engine(), key.AsRawKey(), mtc.stores[0].Clock().Now(), true, nil, &metaDesc); !ok || err != nil {
				return util.Errorf("failed to resolve %s", key.AsRawKey())
			}
			if !reflect.DeepEqual(metaDesc, desc) {
				return util.Errorf("descs not equal: %+v != %+v", metaDesc, desc)
			}
		}
		return nil
	})

	// Verify that the same data is available on the replica.
	util.SucceedsSoon(t, func() error {
		getArgs := getArgs([]byte("a"))
		if reply, err := client.SendWrappedWith(rg1(mtc.stores[1]), nil, roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, &getArgs); err != nil {
			return util.Errorf("failed to read data: %s", err)
		} else if e, v := int64(5), mustGetInt(reply.(*roachpb.GetResponse).Value); v != e {
			return util.Errorf("failed to read correct data: expected %d, got %d", e, v)
		}
		return nil
	})
}

// TestRestoreReplicas ensures that consensus group membership is properly
// persisted to disk and restored when a node is stopped and restarted.
func TestRestoreReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := startMultiTestContext(t, 2)
	defer mtc.Stop()

	firstRng, err := mtc.stores[0].GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	// Perform an increment before replication to ensure that commands are not
	// repeated on restarts.
	incArgs := incrementArgs([]byte("a"), 23)
	if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &incArgs); err != nil {
		t.Fatal(err)
	}

	if err := firstRng.ChangeReplicas(roachpb.ADD_REPLICA,
		roachpb.ReplicaDescriptor{
			NodeID:  mtc.stores[1].Ident.NodeID,
			StoreID: mtc.stores[1].Ident.StoreID,
		}, firstRng.Desc()); err != nil {
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

	// Send a command on each store. The original store (the leader still)
	// will succeed.
	incArgs = incrementArgs([]byte("a"), 5)
	if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &incArgs); err != nil {
		t.Fatal(err)
	}
	// The follower will return a not leader error, indicating the command
	// should be forwarded to the leader.
	incArgs = incrementArgs([]byte("a"), 11)
	{
		_, pErr := client.SendWrapped(rg1(mtc.stores[1]), nil, &incArgs)
		if _, ok := pErr.GetDetail().(*roachpb.NotLeaderError); !ok {
			t.Fatalf("expected not leader error; got %s", pErr)
		}
	}
	// Send again, this time to first store.
	if _, pErr := client.SendWrapped(rg1(mtc.stores[0]), nil, &incArgs); pErr != nil {
		t.Fatal(pErr)
	}

	util.SucceedsSoon(t, func() error {
		getArgs := getArgs([]byte("a"))
		if reply, err := client.SendWrappedWith(rg1(mtc.stores[1]), nil, roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, &getArgs); err != nil {
			return util.Errorf("failed to read data: %s", err)
		} else if e, v := int64(39), mustGetInt(reply.(*roachpb.GetResponse).Value); v != e {
			return util.Errorf("failed to read correct data: expected %d, got %d", e, v)
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

func TestFailedReplicaChange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var runFilter atomic.Value
	runFilter.Store(true)

	ctx := storage.TestStoreContext()
	mtc := &multiTestContext{}
	mtc.storeContext = &ctx
	mtc.storeContext.TestingKnobs.TestingCommandFilter =
		func(filterArgs storageutils.FilterArgs) *roachpb.Error {
			if runFilter.Load().(bool) {
				if et, ok := filterArgs.Req.(*roachpb.EndTransactionRequest); ok && et.Commit {
					return roachpb.NewErrorWithTxn(util.Errorf("boom"), filterArgs.Hdr.Txn)
				}
				return nil
			}
			return nil
		}
	mtc.Start(t, 2)
	defer mtc.Stop()

	rng, err := mtc.stores[0].GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	err = rng.ChangeReplicas(roachpb.ADD_REPLICA,
		roachpb.ReplicaDescriptor{
			NodeID:  mtc.stores[1].Ident.NodeID,
			StoreID: mtc.stores[1].Ident.StoreID,
		}, rng.Desc())
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("did not get expected error: %s", err)
	}

	// After the aborted transaction, r.Desc was not updated.
	// TODO(bdarnell): expose and inspect raft's internal state.
	if len(rng.Desc().Replicas) != 1 {
		t.Fatalf("expected 1 replica, found %d", len(rng.Desc().Replicas))
	}

	// The pending config change flag was cleared, so a subsequent attempt
	// can succeed.
	runFilter.Store(false)

	// The first failed replica change has laid down intents. Make sure those
	// are pushable by making the transaction abandoned.
	mtc.manualClock.Increment(10 * storage.DefaultHeartbeatInterval.Nanoseconds())

	err = rng.ChangeReplicas(roachpb.ADD_REPLICA,
		roachpb.ReplicaDescriptor{
			NodeID:  mtc.stores[1].Ident.NodeID,
			StoreID: mtc.stores[1].Ident.StoreID,
		}, rng.Desc())
	if err != nil {
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
			if lr := len(rang.Desc().Replicas); lr <= 1 {
				return util.Errorf("expected > 1 replicas; got %d", lr)
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
	if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &incArgs); err != nil {
		t.Fatal(err)
	}

	// Get that command's log index.
	index, err := rng.GetLastIndex()
	if err != nil {
		t.Fatal(err)
	}

	// Truncate the log at index+1 (log entries < N are removed, so this includes
	// the increment).
	truncArgs := truncateLogArgs(index + 1)
	if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &truncArgs); err != nil {
		t.Fatal(err)
	}

	// Issue a second command post-truncation.
	incArgs = incrementArgs([]byte("a"), 11)
	if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &incArgs); err != nil {
		t.Fatal(err)
	}

	// Now add the second replica.
	if err := rng.ChangeReplicas(roachpb.ADD_REPLICA,
		roachpb.ReplicaDescriptor{
			NodeID:  mtc.stores[1].Ident.NodeID,
			StoreID: mtc.stores[1].Ident.StoreID,
		}, rng.Desc()); err != nil {
		t.Fatal(err)
	}

	// Once it catches up, the effects of both commands can be seen.
	util.SucceedsSoon(t, func() error {
		getArgs := getArgs([]byte("a"))
		if reply, err := client.SendWrappedWith(rg1(mtc.stores[1]), nil, roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, &getArgs); err != nil {
			return util.Errorf("failed to read data: %s", err)
		} else if e, v := int64(16), mustGetInt(reply.(*roachpb.GetResponse).Value); v != e {
			return util.Errorf("failed to read correct data: expected %d, got %d", e, v)
		}
		return nil
	})

	rng2, err := mtc.stores[1].GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}
	if mvcc, mvcc2 := rng.GetMVCCStats(), rng2.GetMVCCStats(); !reflect.DeepEqual(mvcc, mvcc2) {
		t.Fatalf("expected stats on new range:\n%+v\nto equal old:\n%+v", mvcc2, mvcc)
	}

	// Send a third command to verify that the log states are synced up so the
	// new node can accept new commands.
	incArgs = incrementArgs([]byte("a"), 23)
	if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &incArgs); err != nil {
		t.Fatal(err)
	}

	util.SucceedsSoon(t, func() error {
		getArgs := getArgs([]byte("a"))
		if reply, err := client.SendWrappedWith(rg1(mtc.stores[1]), nil, roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, &getArgs); err != nil {
			return util.Errorf("failed to read data: %s", err)
		} else if e, v := int64(39), mustGetInt(reply.(*roachpb.GetResponse).Value); v != e {
			return util.Errorf("failed to read correct data: expected %d, got %d", e, v)
		}
		return nil
	})
}

// TestStoreRangeUpReplicate verifies that the replication queue will notice
// under-replicated ranges and replicate them.
func TestStoreRangeUpReplicate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := startMultiTestContext(t, 3)
	defer mtc.Stop()

	// Initialize the gossip network.
	var wg sync.WaitGroup
	wg.Add(len(mtc.stores))
	key := gossip.MakePrefixPattern(gossip.KeyStorePrefix)
	mtc.stores[0].Gossip().RegisterCallback(key, func(_ string, _ roachpb.Value) { wg.Done() })
	for _, s := range mtc.stores {
		s.GossipStore()
	}
	wg.Wait()

	// Once we know our peers, trigger a scan.
	mtc.stores[0].ForceReplicationScanAndProcess()

	// The range should become available on every node.
	util.SucceedsSoon(t, func() error {
		for _, s := range mtc.stores {
			r := s.LookupReplica(roachpb.RKey("a"), roachpb.RKey("b"))
			if r == nil {
				return util.Errorf("expected replica for keys \"a\" - \"b\"")
			}
		}
		return nil
	})
}

// getRangeMetadata retrieves the current range descriptor for the target
// range.
func getRangeMetadata(key roachpb.RKey, mtc *multiTestContext, t *testing.T) roachpb.RangeDescriptor {
	// Calls to RangeLookup typically use inconsistent reads, but we
	// want to do a consistent read here. This is important when we are
	// considering one of the metadata ranges: we must not do an
	// inconsistent lookup in our own copy of the range.
	b := &client.Batch{}
	b.InternalAddRequest(&roachpb.RangeLookupRequest{
		Span: roachpb.Span{
			Key: keys.RangeMetaKey(key),
		},
		MaxRanges: 1,
	})
	var reply *roachpb.RangeLookupResponse
	if br, err := mtc.dbs[0].RunWithResponse(b); err != nil {
		t.Fatalf("error getting range metadata: %s", err)
	} else {
		reply = br.Responses[0].GetInner().(*roachpb.RangeLookupResponse)
	}
	if a, e := len(reply.Ranges), 1; a != e {
		t.Fatalf("expected %d range descriptor, got %d", e, a)
	}
	return reply.Ranges[0]
}

// TestUnreplicateFirstRange verifies that multiTestContext still functions in
// the case where the first range (which contains range metadata) is
// unreplicated from the first store. This situation can arise occasionally in
// tests, as can a similar situation where the first store is no longer the leader of
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

	// Initialize the gossip network.
	var wg sync.WaitGroup
	wg.Add(len(mtc.stores))
	key := gossip.MakePrefixPattern(gossip.KeyStorePrefix)
	mtc.stores[0].Gossip().RegisterCallback(key, func(_ string, _ roachpb.Value) { wg.Done() })
	for _, s := range mtc.stores {
		s.GossipStore()
	}
	wg.Wait()

	maxTimeout := time.After(10 * time.Second)
	succeeded := false
	for !succeeded {
		select {
		case <-maxTimeout:
			t.Fatalf("Failed to achieve proper replication within 10 seconds")
		case <-time.After(10 * time.Millisecond):
			mtc.expireLeaderLeases()
			rangeDesc := getRangeMetadata(rightKeyAddr, mtc, t)
			if count := len(rangeDesc.Replicas); count < 3 {
				t.Fatalf("Removed too many replicas; expected at least 3 replicas, found %d", count)
			} else if count == 3 {
				succeeded = true
				break
			}

			// Run replication scans on every store; only the store with the
			// leader lease will actually do anything. If we did not wait
			// for the scan to complete here it could be interrupted by the
			// next call to expireLeaderLeases.
			for _, store := range mtc.stores {
				store.ForceReplicationScanAndProcess()
			}
		}
	}

	// Expire leader leases one more time, so that any remaining resolutions can
	// get a leader lease.
	// TODO(bdarnell): understand why some tests need this.
	mtc.expireLeaderLeases()
}

// TestChangeReplicasDuplicateError tests that a replica change aborts if
// another change has been made to the RangeDescriptor since it was initiated.
func TestChangeReplicasDescriptorInvariant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := startMultiTestContext(t, 3)
	defer mtc.Stop()

	repl, err := mtc.stores[0].GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	addReplica := func(storeNum int, desc *roachpb.RangeDescriptor) error {
		return repl.ChangeReplicas(roachpb.ADD_REPLICA,
			roachpb.ReplicaDescriptor{
				NodeID:  mtc.stores[storeNum].Ident.NodeID,
				StoreID: mtc.stores[storeNum].Ident.StoreID,
			},
			desc)
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
			return util.Errorf("expected replica for keys \"a\" - \"b\"")
		}
		return nil
	})

	// Attempt to add replica to the third store with the original descriptor.
	// This should fail because the descriptor is stale.
	if err := addReplica(2, origDesc); err == nil {
		t.Fatal("Expected error calling ChangeReplicas with stale RangeDescriptor")
	}

	// Both addReplica calls attempted to use origDesc.NextReplicaID.
	// The failed second call should not have overwritten the cached
	// replica descriptor from the successful first call.
	if rd, err := mtc.stores[0].ReplicaDescriptor(origDesc.RangeID, origDesc.NextReplicaID); err != nil {
		t.Fatalf("failed to look up replica %s", origDesc.NextReplicaID)
	} else if a, e := rd.StoreID, mtc.stores[1].Ident.StoreID; a != e {
		t.Fatalf("expected replica %s to point to store %s, but got %s", origDesc.NextReplicaID, a, e)
	}

	// Add to third store with fresh descriptor.
	if err := addReplica(2, repl.Desc()); err != nil {
		t.Fatal(err)
	}
	util.SucceedsSoon(t, func() error {
		r := mtc.stores[2].LookupReplica(roachpb.RKey("a"), roachpb.RKey("b"))
		if r == nil {
			return util.Errorf("expected replica for keys \"a\" - \"b\"")
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
	if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &incArgs); err != nil {
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
				return util.Errorf("expected %v, got %v", expected, values)
			}
			return nil
		})
	}
	verify([]int64{5, 5, 5})

	// Stop one of the replicas and issue a new increment.
	mtc.stopStore(1)
	incArgs = incrementArgs([]byte("a"), 11)
	if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &incArgs); err != nil {
		t.Fatal(err)
	}

	// The new increment can be seen on both live replicas.
	verify([]int64{16, 5, 16})

	// Once the downed node is restarted, it will catch up.
	mtc.restartStore(1)
	verify([]int64{16, 16, 16})
}

func TestReplicateAddAndRemove(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFunc := func(addFirst bool) {
		mtc := startMultiTestContext(t, 4)
		defer mtc.Stop()

		// Replicate the initial range to three of the four nodes.
		rangeID := roachpb.RangeID(1)
		mtc.replicateRange(rangeID, 3, 1)

		incArgs := incrementArgs([]byte("a"), 5)
		if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &incArgs); err != nil {
			t.Fatal(err)
		}

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
					return util.Errorf("addFirst: %t, expected %v, got %v", addFirst, expected, values)
				}
				return nil
			})
		}

		// The first increment is visible on all three replicas.
		verify([]int64{5, 5, 0, 5})

		// Stop a store and replace it.
		mtc.stopStore(1)
		if addFirst {
			mtc.replicateRange(rangeID, 2)
			mtc.unreplicateRange(rangeID, 1)
		} else {
			mtc.unreplicateRange(rangeID, 1)
			mtc.replicateRange(rangeID, 2)
		}
		verify([]int64{5, 5, 5, 5})

		// Ensure that the rest of the group can make progress.
		incArgs = incrementArgs([]byte("a"), 11)
		if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &incArgs); err != nil {
			t.Fatal(err)
		}
		verify([]int64{16, 5, 16, 16})

		// Bring the downed store back up (required for a clean shutdown).
		mtc.restartStore(1)

		// Node 1 never sees the increment that was added while it was
		// down. Perform another increment on the live nodes to verify.
		incArgs = incrementArgs([]byte("a"), 23)
		if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &incArgs); err != nil {
			t.Fatal(err)
		}
		verify([]int64{39, 5, 39, 39})

		// Wait out the leader lease and the unleased duration to make the replica GC'able.
		mtc.expireLeaderLeases()
		mtc.manualClock.Increment(int64(
			storage.ReplicaGCQueueInactivityThreshold + 1))
		mtc.stores[1].ForceReplicaGCScanAndProcess()

		// The removed store no longer has any of the data from the range.
		verify([]int64{39, 0, 39, 39})

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
	// Run the test twice, once adding the replacement before removing
	// the downed node, and once removing the downed node first.
	testFunc(true)
	testFunc(false)
}

// TestRaftHeartbeats verifies that coalesced heartbeats are correctly
// suppressing elections in an idle cluster.
func TestRaftHeartbeats(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mtc := startMultiTestContext(t, 3)
	defer mtc.Stop()
	mtc.replicateRange(1, 1, 2)

	// Capture the initial term and state.
	status := mtc.stores[0].RaftStatus(1)
	initialTerm := status.Term
	if status.SoftState.RaftState != raft.StateLeader {
		t.Errorf("expected node 0 to initially be leader but was %s", status.SoftState.RaftState)
	}

	// Wait for several ticks to elapse.
	time.Sleep(5 * mtc.makeContext(0).RaftTickInterval)
	status = mtc.stores[0].RaftStatus(1)
	if status.SoftState.RaftState != raft.StateLeader {
		t.Errorf("expected node 0 to be leader after sleeping but was %s", status.SoftState.RaftState)
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
	if _, err := client.SendWrapped(rg1(store0), nil, &splitArgs); err != nil {
		t.Fatal(err)
	}

	rangeID2 := store0.LookupReplica(roachpb.RKey(key), nil).RangeID
	if rangeID2 == rangeID {
		t.Errorf("got same range id after split")
	}
	// Issue an increment for later check.
	incArgs := incrementArgs(key, 11)
	if _, err := client.SendWrappedWith(rg1(store0), nil, roachpb.Header{
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
		// Reading on non-leader replica should use inconsistent read
		if reply, err := client.SendWrappedWith(rg1(mtc.stores[1]), nil, roachpb.Header{
			RangeID:         rangeID2,
			ReadConsistency: roachpb.INCONSISTENT,
		}, &getArgs); err != nil {
			return util.Errorf("failed to read data: %s", err)
		} else if e, v := int64(11), mustGetInt(reply.(*roachpb.GetResponse).Value); v != e {
			return util.Errorf("failed to read correct data: expected %d, got %d", e, v)
		}
		return nil
	})
}

// TestRangeDescriptorSnapshotRace calls Snapshot() repeatedly while
// transactions are performed on the range descriptor.
func TestRangeDescriptorSnapshotRace(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mtc := startMultiTestContext(t, 1)
	defer mtc.Stop()

	stopper := stop.NewStopper()
	defer stopper.Stop()
	// Call Snapshot() in a loop and ensure it never fails.
	stopper.RunWorker(func() {
		for {
			select {
			case <-stopper.ShouldStop():
				return
			default:
				rng := mtc.stores[0].LookupReplica(roachpb.RKeyMin, nil)
				if rng == nil {
					t.Fatal("failed to look up min range")
				}
				_, err := rng.GetSnapshot()
				if err != nil {
					t.Fatalf("failed to snapshot min range: %s", err)
				}

				rng = mtc.stores[0].LookupReplica(roachpb.RKey("Z"), nil)
				if rng == nil {
					t.Fatal("failed to look up max range")
				}
				_, err = rng.GetSnapshot()
				if err != nil {
					t.Fatalf("failed to snapshot max range: %s", err)
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
	if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &splitArgs); err != nil {
		t.Fatal(err)
	}

	rangeID := roachpb.RangeID(2)
	mtc.replicateRange(rangeID, 1, 2)

	mtc.unreplicateRange(rangeID, 2)
	mtc.unreplicateRange(rangeID, 1)

	// Wait for the removal to be processed.
	util.SucceedsSoon(t, func() error {
		_, err := mtc.stores[1].GetReplica(rangeID)
		if _, ok := err.(*roachpb.RangeNotFoundError); ok {
			return nil
		} else if err != nil {
			return err
		}
		return util.Errorf("range still exists")
	})

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
	if err := mtc.transports[2].Send(&storage.RaftMessageRequest{
		GroupID:     0,
		ToReplica:   replica1,
		FromReplica: replica2,
		Message: raftpb.Message{
			From: uint64(replica2.ReplicaID),
			To:   uint64(replica1.ReplicaID),
			Type: raftpb.MsgHeartbeat,
		}}); err != nil {
		t.Fatal(err)
	}
	// Execute another replica change to ensure that raft has processed
	// the heartbeat just sent.
	mtc.replicateRange(roachpb.RangeID(1), 1)

	// Expire leases to ensure any remaining intent resolutions can complete.
	// TODO(bdarnell): understand why some tests need this.
	mtc.expireLeaderLeases()
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

	var sgs []*gossiputil.StoreGossiper
	for _, g := range mtc.gossips {
		sgs = append(sgs, gossiputil.NewStoreGossiper(g))
	}

	// Replicate the range to all stores.
	replica := mtc.stores[0].LookupReplica(roachpb.RKeyMin, nil)
	mtc.replicateRange(replica.RangeID, 1, 2)

	// Initialize the gossip network.
	var storeIDs []roachpb.StoreID
	for _, s := range mtc.stores {
		storeIDs = append(storeIDs, s.StoreID())
	}

	// Gossip all stores and wait for callbacks to be run. This is
	// tricky since we have multiple gossip objects communicating
	// asynchronously. We use StoreGossiper to track callbacks but we
	// need to set up all the callback tracking before any stores are
	// gossiped.
	var readyWG sync.WaitGroup
	var doneWG sync.WaitGroup
	readyWG.Add(len(sgs))
	doneWG.Add(len(sgs))
	for _, sg := range sgs {
		go func(sg *gossiputil.StoreGossiper) {
			ready := false
			sg.GossipWithFunction(storeIDs, func() {
				if !ready {
					readyWG.Done()
					ready = true
				}
			})
			doneWG.Done()
		}(sg)
	}
	readyWG.Wait()
	for _, s := range mtc.stores {
		s.GossipStore()
	}
	doneWG.Wait()

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
			mtc.stores[0].GossipStore()
			mtc.stores[1].GossipStore()

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

	// Start multiTestContext with replica rebalancing enabled.
	mtc := &multiTestContext{
		storeContext: &storage.StoreContext{},
	}
	*mtc.storeContext = storage.TestStoreContext()
	mtc.storeContext.AllocatorOptions = storage.AllocatorOptions{
		AllowRebalance: true,
		Deterministic:  true,
	}

	// Four stores.
	mtc.Start(t, 4)
	defer mtc.Stop()

	// Replicate the first range to the first three stores.
	store0 := mtc.stores[0]
	replica := store0.LookupReplica(roachpb.RKeyMin, nil)
	desc := replica.Desc()
	mtc.replicateRange(desc.RangeID, 1, 2)

	// Initialize the gossip network with fake capacity data.
	storeDescs := make([]*roachpb.StoreDescriptor, 0, len(mtc.stores))
	for _, s := range mtc.stores {
		desc, err := s.Descriptor()
		if err != nil {
			t.Fatal(err)
		}
		desc.Capacity.Capacity = 1024 * 1024
		desc.Capacity.Available = 1024 * 1024
		// Make sure store[1] is chosen as removal target.
		if desc.StoreID == mtc.stores[1].StoreID() {
			desc.Capacity.Available = 0
		}
		storeDescs = append(storeDescs, desc)
	}
	var sgs []*gossiputil.StoreGossiper
	for _, g := range mtc.gossips {
		sg := gossiputil.NewStoreGossiper(g)
		sgs = append(sgs, sg)
		sg.GossipStores(storeDescs, t)
	}

	// This can't use SucceedsSoon as using the exponential backoff mechanic
	// won't work well with the forced replication scans.
	maxTimeout := time.After(5 * time.Second)
	succeeded := false
	for !succeeded {
		select {
		case <-maxTimeout:
			t.Fatal("Failed to rebalance replica within 5 seconds")
		case <-time.After(10 * time.Millisecond):
			// Look up the official range descriptor, make sure fourth store is on it.
			rangeDesc := getRangeMetadata(roachpb.RKeyMin, mtc, t)

			// Test if we have already succeeded.
			for _, repl := range rangeDesc.Replicas {
				if repl.StoreID == mtc.stores[3].StoreID() {
					succeeded = true
				}
			}

			if succeeded {
				break
			}

			mtc.expireLeaderLeases()
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
	if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &incArgs); err != nil {
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
	if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &incArgs); err != nil {
		t.Fatal(err)
	}

	// Wait for the replica to be GC'd on node 1.
	// Store 0 has two writes, 1 has erased everything, and 2 still has the first write.
	// A single pass of ForceReplicaGCScanAndProcess is not enough, since the replica
	// may be recreated by a stray raft message, so we run the GC scan inside the loop.
	// TODO(bdarnell): if the call to RemoveReplica in replicaGCQueue.process can be
	// moved under the lock, then the GC scan can be moved out of this loop.
	util.SucceedsSoon(t, func() error {
		mtc.expireLeaderLeases()
		mtc.manualClock.Increment(int64(
			storage.ReplicaGCQueueInactivityThreshold) + 1)
		mtc.stores[1].ForceReplicaGCScanAndProcess()

		actual := mtc.readIntFromEngines(roachpb.Key("a"))
		expected := []int64{16, 0, 5}
		if !reflect.DeepEqual(expected, actual) {
			return util.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	// Bring node 2 back up.
	mtc.restartStore(2)

	// Try to issue a command on node 2. It should not be able to commit
	// (so we add it asynchronously).
	startWG := sync.WaitGroup{}
	startWG.Add(1)
	finishWG := sync.WaitGroup{}
	finishWG.Add(1)
	go func() {
		rng, err := mtc.stores[2].GetReplica(raftID)
		if err != nil {
			t.Fatal(err)
		}
		incArgs := incrementArgs([]byte("a"), 23)
		startWG.Done()
		defer finishWG.Done()
		if _, err := client.SendWrappedWith(rng, nil, roachpb.Header{Timestamp: mtc.stores[2].Clock().Now()}, &incArgs); err == nil {
			t.Fatal("expected error during shutdown")
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
	mtc.waitForValues(roachpb.Key("a"), []int64{16, 0, 5})

	// Run garbage collection on node 2. The lack of an active leader
	// lease will cause GC to do a consistent range lookup, where it
	// will see that the range has been moved and delete the old
	// replica.
	mtc.expireLeaderLeases()
	mtc.manualClock.Increment(int64(
		storage.ReplicaGCQueueInactivityThreshold) + 1)
	mtc.stores[2].ForceReplicaGCScanAndProcess()
	mtc.waitForValues(roachpb.Key("a"), []int64{16, 0, 0})

	// Now that the group has been GC'd, the goroutine that was
	// attempting to write has finished (with an error).
	finishWG.Wait()
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
	if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &incArgs); err != nil {
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
	if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &incArgs); err != nil {
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

// TestLeaderRemoveSelf verifies that a leader can remove itself
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
	mtc.stores[0].DisableReplicaGCQueue(true)
	raftID := roachpb.RangeID(1)
	mtc.replicateRange(raftID, 1)
	// Remove the replica from first store.
	mtc.unreplicateRange(raftID, 0)
	getArgs := getArgs([]byte("a"))

	// Force the read command request a new lease.
	clock := mtc.clocks[0]
	header := roachpb.Header{}
	header.Timestamp = clock.Update(clock.Now().Add(
		storage.LeaderLeaseExpiration(clock), 0))

	// Expect get a RangeNotFoundError.
	_, pErr := client.SendWrappedWith(rg1(mtc.stores[0]), nil, header, &getArgs)
	if _, ok := pErr.GetDetail().(*roachpb.RangeNotFoundError); !ok {
		t.Fatalf("expect get RangeNotFoundError, actual get %v ", pErr)
	}
}

// TestRemoveRangeWithoutGC ensures that we do not panic when a
// replica has been removed but not yet GC'd (and therefore
// does not have an active raft group).
func TestRemoveRangeWithoutGC(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mtc := startMultiTestContext(t, 2)
	defer mtc.Stop()
	// Disable the GC queue and move the range from store 0 to 1.
	mtc.stores[0].DisableReplicaGCQueue(true)
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
			return util.Errorf("range has %d replicas", len(desc.Replicas))
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
	// Turn off the GC queue to ensure that the replica is deleted at
	// startup instead of by the scanner. This is not 100% guaranteed
	// since the scanner could have already run at this point, but it
	// should be enough to prevent us from accidentally relying on the
	// scanner.
	mtc.stores[0].DisableReplicaGCQueue(true)

	// The Replica object is not recreated.
	if _, err := mtc.stores[0].GetReplica(rangeID); err == nil {
		t.Fatalf("expected replica to be missing")
	}

	// And the data is no longer on disk.
	if ok, err := engine.MVCCGetProto(context.Background(), mtc.stores[0].Engine(), descKey,
		mtc.stores[0].Clock().Now(), true, nil, &desc); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("expected range descriptor to be absent")
	}
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
	if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &putArgs); err != nil {
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
	if _, err := client.SendWrappedWith(rg1(mtc.stores[0]), nil, roachpb.Header{
		Timestamp: mtc.stores[0].Clock().Now(),
	}, &checkArgs); err != nil {
		t.Fatal(err)
	}
}

func TestCheckInconsistent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numStores = 3
	mtc := startMultiTestContext(t, numStores)
	defer mtc.Stop()
	// Setup replication of range 1 on store 0 to stores 1 and 2.
	mtc.replicateRange(1, 1, 2)

	// Write something to the DB.
	pArgs := putArgs([]byte("a"), []byte("b"))
	if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &pArgs); err != nil {
		t.Fatal(err)
	}
	pArgs = putArgs([]byte("c"), []byte("d"))
	if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &pArgs); err != nil {
		t.Fatal(err)
	}

	// Write some arbitrary data only to store 1. Inconsistent key "e"!
	key := []byte("e")
	var val roachpb.Value
	val.SetInt(42)
	timestamp := mtc.stores[1].Clock().Timestamp()
	if err := engine.MVCCPut(context.Background(), mtc.stores[1].Engine(), nil, key, timestamp, val, nil); err != nil {
		t.Fatal(err)
	}

	// The consistency check will panic on store 1.
	notify := make(chan struct{}, 1)
	mtc.stores[1].TestingKnobs().BadChecksumPanic = func(diff []storage.ReplicaSnapshotDiff) {
		if len(diff) != 1 {
			t.Errorf("diff length = %d, diff = %v", len(diff), diff)
		}
		d := diff[0]
		if d.Leader != false || !bytes.Equal([]byte("e"), d.Key) || !timestamp.Equal(d.Timestamp) {
			t.Errorf("diff = %v", d)
		}
		notify <- struct{}{}
	}
	// Run consistency check.
	checkArgs := roachpb.CheckConsistencyRequest{
		Span: roachpb.Span{
			// span of keys that include "a" & "c".
			Key:    []byte("a"),
			EndKey: []byte("z"),
		},
	}
	if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &checkArgs); err != nil {
		t.Fatal(err)
	}
	select {
	case <-notify:
	case <-time.After(5 * time.Second):
		t.Fatal("didn't receive notification from VerifyChecksum() that should have panicked")
	}
}

func TestTransferRaftLeadership(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numStores = 3
	mtc := startMultiTestContext(t, numStores)
	defer mtc.Stop()
	// Setup replication of range 1 on store 0 to stores 1 and 2.
	mtc.replicateRange(1, 1, 2)

	rng, err := mtc.stores[0].GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}
	status := rng.RaftStatus()
	if status.Lead != 1 {
		t.Fatalf("raft leader should be 1, but got %v", status.Lead)
	}

	mtc.expireLeaderLeases()
	// Force the read command request a new lease.
	getArgs := getArgs([]byte("a"))
	_, pErr := client.SendWrapped(rg1(mtc.stores[1]), nil, &getArgs)
	if pErr != nil {
		t.Fatalf("expect get nil, actual get %v ", pErr)
	}
	// Wait for raft leadership transferring to be finished.
	util.SucceedsSoon(t, func() error {
		status = rng.RaftStatus()
		if status.Lead != 2 {
			return util.Errorf("expected raft leader be 2; got %d", status.Lead)
		}
		return nil
	})
}

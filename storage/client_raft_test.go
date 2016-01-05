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
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/testutils/gossiputil"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
)

const replicaReadTimeout = 1 * time.Second

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
	defer leaktest.AfterTest(t)
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

		increment := func(rangeID roachpb.RangeID, key roachpb.Key, value int64) (*roachpb.IncrementResponse, error) {
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
		rangeID2 = store.LookupReplica(roachpb.RKey(key2), nil).Desc().RangeID
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
	defer leaktest.AfterTest(t)
	defer func() { storage.TestingCommandFilter = nil }()
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	engineStopper := stop.NewStopper()
	defer engineStopper.Stop()
	eng := engine.NewInMem(roachpb.Attributes{}, 1<<20, engineStopper)

	numIncrements := 0

	storage.TestingCommandFilter = func(_ roachpb.StoreID, args roachpb.Request, _ roachpb.Header) error {
		if _, ok := args.(*roachpb.IncrementRequest); ok && args.Header().Key.Equal(roachpb.Key("a")) {
			numIncrements++
		}
		return nil
	}

	func() {
		stopper := stop.NewStopper()
		defer stopper.Stop()
		store := createTestStoreWithEngine(t, eng, clock, true, nil, stopper)

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
	defer leaktest.AfterTest(t)
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
	if ok, err := engine.MVCCGetProto(mtc.stores[0].Engine(), key, mtc.stores[0].Clock().Now(), true, nil, &desc); !ok || err != nil {
		t.Fatalf("fetching range descriptor yielded %t, %s", ok, err)
	}
	// Verify that in time, no intents remain on meta addressing
	// keys, and that range descriptor on the meta records is correct.
	util.SucceedsWithin(t, 1*time.Second, func() error {
		meta2 := keys.Addr(keys.RangeMetaKey(roachpb.RKeyMax))
		meta1 := keys.Addr(keys.RangeMetaKey(meta2))
		for _, key := range []roachpb.RKey{meta2, meta1} {
			metaDesc := roachpb.RangeDescriptor{}
			if ok, err := engine.MVCCGetProto(mtc.stores[0].Engine(), key.AsRawKey(), mtc.stores[0].Clock().Now(), true, nil, &metaDesc); !ok || err != nil {
				return util.Errorf("failed to resolve %s", key.AsRawKey())
			}
			if !reflect.DeepEqual(metaDesc, desc) {
				return util.Errorf("descs not equal: %+v != %+v", metaDesc, desc)
			}
		}
		return nil
	})

	// Verify that the same data is available on the replica.
	util.SucceedsWithin(t, replicaReadTimeout, func() error {
		getArgs := getArgs([]byte("a"))
		if reply, err := client.SendWrappedWith(rg1(mtc.stores[1]), nil, roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, &getArgs); err != nil {
			return util.Errorf("failed to read data")
		} else if v := mustGetInt(reply.(*roachpb.GetResponse).Value); v != 5 {
			return util.Errorf("failed to read correct data: %d", v)
		}
		return nil
	})
}

// TestRestoreReplicas ensures that consensus group membership is properly
// persisted to disk and restored when a node is stopped and restarted.
func TestRestoreReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)
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
		_, err := client.SendWrapped(rg1(mtc.stores[1]), nil, &incArgs)
		if _, ok := err.(*roachpb.NotLeaderError); !ok {
			t.Fatalf("expected not leader error; got %s", err)
		}
	}
	// Send again, this time to first store.
	if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &incArgs); err != nil {
		t.Fatal(err)
	}

	if err := util.IsTrueWithin(func() bool {
		getArgs := getArgs([]byte("a"))
		reply, err := client.SendWrappedWith(rg1(mtc.stores[1]), nil, roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, &getArgs)
		if err != nil {
			return false
		}
		return mustGetInt(reply.(*roachpb.GetResponse).Value) == 39
	}, replicaReadTimeout); err != nil {
		t.Fatal(err)
	}

	// Both replicas have a complete list in Desc.Replicas
	for i, store := range mtc.stores {
		rng, err := store.GetReplica(1)
		if err != nil {
			t.Fatal(err)
		}
		rng.RLock()
		if len(rng.Desc().Replicas) != 2 {
			t.Fatalf("store %d: expected 2 replicas, found %d", i, len(rng.Desc().Replicas))
		}
		if rng.Desc().Replicas[0].NodeID != mtc.stores[0].Ident.NodeID {
			t.Errorf("store %d: expected replica[0].NodeID == %d, was %d",
				i, mtc.stores[0].Ident.NodeID, rng.Desc().Replicas[0].NodeID)
		}
		rng.RUnlock()
	}
}

func TestFailedReplicaChange(t *testing.T) {
	defer leaktest.AfterTest(t)
	defer func() { storage.TestingCommandFilter = nil }()

	mtc := startMultiTestContext(t, 2)
	defer mtc.Stop()

	var runFilter atomic.Value
	runFilter.Store(true)

	storage.TestingCommandFilter = func(_ roachpb.StoreID, args roachpb.Request, _ roachpb.Header) error {
		if runFilter.Load().(bool) {
			if et, ok := args.(*roachpb.EndTransactionRequest); ok && et.Commit {
				return util.Errorf("boom")
			}
			return nil
		}
		return nil
	}

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
	if err := util.IsTrueWithin(func() bool {
		for _, store := range mtc.stores {
			rang, err := store.GetReplica(1)
			if err != nil {
				return false
			}
			if len(rang.Desc().Replicas) == 1 {
				return false
			}
		}
		return true
	}, 1*time.Second); err != nil {
		t.Fatal(err)
	}
}

// We can truncate the old log entries and a new replica will be brought up from a snapshot.
func TestReplicateAfterTruncation(t *testing.T) {
	defer leaktest.AfterTest(t)
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
	index, err := rng.LastIndex()
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
	mvcc := rng.GetMVCCStats()

	// Now add the second replica.
	if err := rng.ChangeReplicas(roachpb.ADD_REPLICA,
		roachpb.ReplicaDescriptor{
			NodeID:  mtc.stores[1].Ident.NodeID,
			StoreID: mtc.stores[1].Ident.StoreID,
		}, rng.Desc()); err != nil {
		t.Fatal(err)
	}

	// Once it catches up, the effects of both commands can be seen.
	if err := util.IsTrueWithin(func() bool {
		getArgs := getArgs([]byte("a"))
		reply, err := client.SendWrappedWith(rg1(mtc.stores[1]), nil, roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, &getArgs)
		if err != nil {
			return false
		}
		getResp := reply.(*roachpb.GetResponse)
		if log.V(1) {
			log.Infof("read value %d", mustGetInt(getResp.Value))
		}
		return mustGetInt(getResp.Value) == 16
	}, replicaReadTimeout); err != nil {
		t.Fatal(err)
	}

	rng2, err := mtc.stores[1].GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}
	if mvcc2 := rng2.GetMVCCStats(); !reflect.DeepEqual(mvcc, mvcc2) {
		log.Errorf("expected stats on new range to equal old; %+v != %+v", mvcc2, mvcc)
	}

	// Send a third command to verify that the log states are synced up so the
	// new node can accept new commands.
	incArgs = incrementArgs([]byte("a"), 23)
	if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &incArgs); err != nil {
		t.Fatal(err)
	}

	if err := util.IsTrueWithin(func() bool {
		getArgs := getArgs([]byte("a"))
		reply, err := client.SendWrappedWith(rg1(mtc.stores[1]), nil, roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, &getArgs)
		if err != nil {
			return false
		}
		getResp := reply.(*roachpb.GetResponse)
		log.Infof("read value %d", mustGetInt(getResp.Value))
		return mustGetInt(getResp.Value) == 39
	}, replicaReadTimeout); err != nil {
		t.Fatal(err)
	}
}

// TestStoreRangeUpReplicate verifies that the replication queue will notice
// under-replicated ranges and replicate them.
func TestStoreRangeUpReplicate(t *testing.T) {
	defer leaktest.AfterTest(t)
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
	mtc.stores[0].ForceReplicationScan(t)

	// The range should become available on every node.
	if err := util.IsTrueWithin(func() bool {
		for _, s := range mtc.stores {
			r := s.LookupReplica(roachpb.RKey("a"), roachpb.RKey("b"))
			if r == nil {
				return false
			}
		}
		return true
	}, replicationTimeout); err != nil {
		t.Fatal(err)
	}
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
	if br, err := mtc.db.RunWithResponse(b); err != nil {
		t.Fatalf("error getting range metadata: %s", err)
	} else {
		reply = br.Responses[0].GetInner().(*roachpb.RangeLookupResponse)
	}
	if a, e := len(reply.Ranges), 1; a != e {
		t.Fatalf("expected %d range descriptor, got %d", e, a)
	}
	return reply.Ranges[0]
}

// TestStoreRangeDownReplicate verifies that the replication queue will notice
// over-replicated ranges and remove replicas from them.
func TestStoreRangeDownReplicate(t *testing.T) {
	defer leaktest.AfterTest(t)
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
		mtc.replicateRange(replica.Desc().RangeID, 0, 1, 2)
		desc := replica.Desc()
		splitArgs := adminSplitArgs(splitKey, splitKey)
		if _, err := replica.AdminSplit(splitArgs, desc); err != nil {
			t.Fatal(err)
		}
	}
	// Replicate the new range to all five stores.
	replica := store0.LookupReplica(keys.Addr(rightKey), nil)
	desc := replica.Desc()
	mtc.replicateRange(desc.RangeID, 0, 3, 4)

	// Initialize the gossip network.
	var wg sync.WaitGroup
	wg.Add(len(mtc.stores))
	key := gossip.MakePrefixPattern(gossip.KeyStorePrefix)
	mtc.stores[0].Gossip().RegisterCallback(key, func(_ string, _ roachpb.Value) { wg.Done() })
	for _, s := range mtc.stores {
		s.GossipStore()
	}
	wg.Wait()

	// storeIDset is used to compare the replica sets from different views (i.e.
	// local range descriptors)
	type storeIDset map[roachpb.StoreID]struct{}
	makeStoreIDset := func(replicas []roachpb.ReplicaDescriptor) storeIDset {
		idSet := make(storeIDset)
		for _, r := range replicas {
			idSet[r.StoreID] = struct{}{}
		}
		return idSet
	}

	// Function to see if the replication level of the new range has reached the
	// expected equilibrium. If equilibrium has not been reached, this function
	// returns the list of stores that *should* have a replica for the range.
	checkReplication := func() (bool, storeIDset) {
		// Query each store for a replica of the range, generating a real map of
		// the replicas.
		foundIDset := make(storeIDset)
		foundLocalRangeDescs := make([]*roachpb.RangeDescriptor, 0, len(mtc.stores))
		for _, s := range mtc.stores {
			r := s.LookupReplica(keys.Addr(splitKey), nil)
			if r != nil {
				foundLocalRangeDescs = append(foundLocalRangeDescs, r.Desc())
				foundIDset[s.StoreID()] = struct{}{}
			}
		}

		// Fail immediately if there are less than three replicas.
		replicaCount := len(foundIDset)
		if replicaCount < 3 {
			t.Fatalf("Removed too many replicas; expected at least three replicas, found %d", replicaCount)
		}

		// Look up the official range descriptor, make sure it agrees with the
		// found replicas.
		realRangeDesc := getRangeMetadata(keys.Addr(rightKey), mtc, t)
		realIDset := makeStoreIDset(realRangeDesc.Replicas)
		if !reflect.DeepEqual(realIDset, foundIDset) {
			return false, realIDset
		}

		// Ensure the local range descriptors everywhere agree with reality.
		for _, desc := range foundLocalRangeDescs {
			localIDset := makeStoreIDset(desc.Replicas)
			if !reflect.DeepEqual(localIDset, foundIDset) {
				return false, realIDset
			}
		}

		// If we have only three replicas, exit the loop.
		if replicaCount == 3 {
			return true, nil
		}
		return false, foundIDset
	}

	maxTimeout := time.After(10 * time.Second)
	succeeded := false
	for !succeeded {
		select {
		case <-maxTimeout:
			t.Fatalf("Failed to achieve proper replication within 10 seconds")
		case <-time.After(10 * time.Millisecond):
			mtc.expireLeaderLeases()
			// If our replication level matches the target, we have succeeded.
			var idSet storeIDset
			succeeded, idSet = checkReplication()
			if succeeded {
				break
			}

			// Kick off a manual ReplicaGC Scan on any store which is not part of the
			// current replica set. Kick off a Replication scan on *one* store which
			// is part of the replica set.
			kickedOffReplicationQueue := false
			for _, store := range mtc.stores {
				if _, ok := idSet[store.StoreID()]; !ok {
					store.ForceReplicaGCScanAndProcess(t)
				} else if !kickedOffReplicationQueue {
					store.ForceReplicationScan(t)
					kickedOffReplicationQueue = true
				}
			}
		}
	}

	// Expire leader leases one more time, so that any remaining resolutions can
	// get a leader lease.
	mtc.expireLeaderLeases()
}

// TestChangeReplicasDuplicateError tests that a replica change aborts if
// another change has been made to the RangeDescriptor since it was initiated.
func TestChangeReplicasDescriptorInvariant(t *testing.T) {
	defer leaktest.AfterTest(t)
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
	if err := util.IsTrueWithin(func() bool {
		r := mtc.stores[1].LookupReplica(roachpb.RKey("a"), roachpb.RKey("b"))
		if r == nil {
			return false
		}
		return true
	}, time.Second); err != nil {
		t.Fatal(err)
	}

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
		log.Infof("expected replica %s to point to store %s, but got %s", origDesc.NextReplicaID, a, e)
	}

	// Add to third store with fresh descriptor.
	if err := addReplica(2, repl.Desc()); err != nil {
		t.Fatal(err)
	}
	if err := util.IsTrueWithin(func() bool {
		r := mtc.stores[2].LookupReplica(roachpb.RKey("a"), roachpb.RKey("b"))
		if r == nil {
			return false
		}
		return true
	}, time.Second); err != nil {
		t.Fatal(err)
	}
}

// TestProgressWithDownNode verifies that a surviving quorum can make progress
// with a downed node.
func TestProgressWithDownNode(t *testing.T) {
	defer leaktest.AfterTest(t)
	mtc := startMultiTestContext(t, 3)
	defer mtc.Stop()

	rangeID := roachpb.RangeID(1)
	mtc.replicateRange(rangeID, 0, 1, 2)

	incArgs := incrementArgs([]byte("a"), 5)
	if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &incArgs); err != nil {
		t.Fatal(err)
	}

	// Verify that the first increment propagates to all the engines.
	verify := func(expected []int64) {
		util.SucceedsWithin(t, time.Second, func() error {
			values := []int64{}
			for _, eng := range mtc.engines {
				val, _, err := engine.MVCCGet(eng, roachpb.Key("a"), mtc.clock.Now(), true, nil)
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
	defer leaktest.AfterTest(t)

	// Run the test twice, once adding the replacement before removing
	// the downed node, and once removing the downed node first.
	for _, addFirst := range []bool{true, false} {
		mtc := startMultiTestContext(t, 4)
		defer mtc.Stop()

		// Replicate the initial range to three of the four nodes.
		rangeID := roachpb.RangeID(1)
		mtc.replicateRange(rangeID, 0, 3, 1)

		incArgs := incrementArgs([]byte("a"), 5)
		if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &incArgs); err != nil {
			t.Fatal(err)
		}

		verify := func(expected []int64) {
			util.SucceedsWithin(t, 3*time.Second, func() error {
				values := []int64{}
				for _, eng := range mtc.engines {
					val, _, err := engine.MVCCGet(eng, roachpb.Key("a"), mtc.clock.Now(), true, nil)
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
			mtc.replicateRange(rangeID, 0, 2)
			mtc.unreplicateRange(rangeID, 0, 1)
		} else {
			mtc.unreplicateRange(rangeID, 0, 1)
			mtc.replicateRange(rangeID, 0, 2)
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
		mtc.manualClock.Increment(int64(storage.ReplicaGCQueueInactivityThreshold+storage.DefaultLeaderLeaseDuration) + 1)
		mtc.stores[1].ForceReplicaGCScanAndProcess(t)

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
}

// TestRaftHeartbeats verifies that coalesced heartbeats are correctly
// suppressing elections in an idle cluster.
func TestRaftHeartbeats(t *testing.T) {
	defer leaktest.AfterTest(t)

	mtc := startMultiTestContext(t, 3)
	defer mtc.Stop()
	mtc.replicateRange(1, 0, 1, 2)

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
	defer leaktest.AfterTest(t)
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

	rangeID2 := store0.LookupReplica(roachpb.RKey(key), nil).Desc().RangeID
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
	mtc.replicateRange(rangeID2, 0, 1)

	if mtc.stores[1].LookupReplica(roachpb.RKey(key), nil).GetMaxBytes() == 0 {
		t.Error("Range MaxBytes is not set after snapshot applied")
	}
	// Once it catches up, the effects of increment commands can be seen.
	if err := util.IsTrueWithin(func() bool {
		getArgs := getArgs(key)
		// Reading on non-leader replica should use inconsistent read
		reply, err := client.SendWrappedWith(rg1(mtc.stores[1]), nil, roachpb.Header{
			RangeID:         rangeID2,
			ReadConsistency: roachpb.INCONSISTENT,
		}, &getArgs)
		if err != nil {
			return false
		}
		getResp := reply.(*roachpb.GetResponse)
		if log.V(1) {
			log.Infof("read value %d", mustGetInt(getResp.Value))
		}
		return mustGetInt(getResp.Value) == 11
	}, replicaReadTimeout); err != nil {
		t.Fatal(err)
	}
}

// TestRangeDescriptorSnapshotRace calls Snapshot() repeatedly while
// transactions are performed on the range descriptor.
func TestRangeDescriptorSnapshotRace(t *testing.T) {
	defer leaktest.AfterTest(t)

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
				_, err := rng.Snapshot()
				if err != nil {
					t.Fatalf("failed to snapshot min range: %s", err)
				}

				rng = mtc.stores[0].LookupReplica(roachpb.RKey("Z"), nil)
				if rng == nil {
					t.Fatal("failed to look up max range")
				}
				_, err = rng.Snapshot()
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
		if _, err := rng.AdminSplit(args, desc); err != nil {
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
		if _, err := rng.AdminSplit(args, desc); err != nil {
			t.Fatal(err)
		}
	}
}

// TestRaftAfterRemoveRange verifies that the raft state removes
// a remote node correctly after the Replica was removed from the Store.
func TestRaftAfterRemoveRange(t *testing.T) {
	defer leaktest.AfterTest(t)
	mtc := startMultiTestContext(t, 3)
	defer mtc.Stop()

	// Make the split.
	splitArgs := adminSplitArgs(roachpb.KeyMin, []byte("b"))
	if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &splitArgs); err != nil {
		t.Fatal(err)
	}

	rangeID := roachpb.RangeID(2)
	mtc.replicateRange(rangeID, 0, 1, 2)

	mtc.unreplicateRange(rangeID, 0, 2)
	mtc.unreplicateRange(rangeID, 0, 1)

	// Wait for the removal to be processed.
	util.SucceedsWithin(t, time.Second, func() error {
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
	if err := mtc.transport.Send(&storage.RaftMessageRequest{
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
	mtc.replicateRange(roachpb.RangeID(1), 0, 1)
}

// TestRaftRemoveRace adds and removes a replica repeatedly in an
// attempt to reproduce a race
// (https://github.com/cockroachdb/cockroach/issues/1911). Note that
// 10 repetitions is not enough to reliably reproduce the problem, but
// it's better than any other tests we have for this (increasing the
// number of repetitions adds an unacceptable amount of test runtime).
func TestRaftRemoveRace(t *testing.T) {
	defer leaktest.AfterTest(t)
	mtc := startMultiTestContext(t, 3)
	defer mtc.Stop()

	rangeID := roachpb.RangeID(1)
	mtc.replicateRange(rangeID, 0, 1, 2)

	for i := 0; i < 10; i++ {
		mtc.unreplicateRange(rangeID, 0, 2)
		mtc.replicateRange(rangeID, 0, 2)
	}
}

// TestStoreRangeRemoveDead verifies that if a store becomes dead, the
// ReplicateQueue will notice and remove any replicas on it.
func TestStoreRangeRemoveDead(t *testing.T) {
	defer leaktest.AfterTest(t)
	mtc := &multiTestContext{}
	mtc.timeUntilStoreDead = storage.TestTimeUntilStoreDead
	mtc.Start(t, 3)
	defer mtc.Stop()

	sg := gossiputil.NewStoreGossiper(mtc.gossip)

	// Replicate the range to all stores.
	replica := mtc.stores[0].LookupReplica(roachpb.RKeyMin, nil)
	mtc.replicateRange(replica.Desc().RangeID, 0, 1, 2)

	// Initialize the gossip network.
	var storeIDs []roachpb.StoreID
	for _, s := range mtc.stores {
		storeIDs = append(storeIDs, s.StoreID())
	}
	sg.GossipWithFunction(storeIDs, func() {
		for _, s := range mtc.stores {
			s.GossipStore()
		}
	})

	aliveStoreIDs := []roachpb.StoreID{
		mtc.stores[0].StoreID(),
		mtc.stores[1].StoreID(),
	}

	rangeDesc := getRangeMetadata(roachpb.RKeyMin, mtc, t)
	if e, a := 3, len(rangeDesc.Replicas); e != a {
		t.Fatalf("expected %d replicas, only found %d, rangeDesc: %+v", e, a, rangeDesc)
	}

	// This can't use SucceedsWithin as using the backoff mechanic won't work
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
			sg.GossipWithFunction(aliveStoreIDs, func() {
				mtc.stores[0].GossipStore()
				mtc.stores[1].GossipStore()
			})
			// Force the repair queues on all alive stores to run.
			mtc.stores[0].ForceReplicationScan(t)
			mtc.stores[1].ForceReplicationScan(t)
		}
	}
	ticker.Stop()
}

// TestStoreRangeRebalance verifies that the replication queue will take
// rebalancing opportunities and add a new replica on another store.
func TestStoreRangeRebalance(t *testing.T) {
	defer leaktest.AfterTest(t)
	t.Skip("TODO(mrtracy): https://github.com/cockroachdb/cockroach/issues/3092")

	// Start multiTestContext with replica rebalancing enabled.
	mtc := &multiTestContext{
		storeContext: &storage.StoreContext{},
	}
	*mtc.storeContext = storage.TestStoreContext
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
	mtc.replicateRange(desc.RangeID, 0, 1, 2)

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
	sg := gossiputil.NewStoreGossiper(mtc.gossip)
	sg.GossipStores(storeDescs, t)

	// This can't use SucceedsWithin as using the exponential backoff mechanic
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
			mtc.stores[1].ForceReplicationScan(t)
		}
	}
}

// TestReplicateRogueRemovedNode ensures that a rogue removed node
// (i.e. a node that has been removed from the range but doesn't know
// it yet because it was down or partitioned away when it happened)
// cannot cause other removed nodes to recreate their ranges.
func TestReplicateRogueRemovedNode(t *testing.T) {
	defer leaktest.AfterTest(t)

	mtc := startMultiTestContext(t, 3)
	defer mtc.Stop()

	// First put the range on all three nodes.
	raftID := roachpb.RangeID(1)
	mtc.replicateRange(raftID, 0, 1, 2)

	// Put some data in the range so we'll have something to test for.
	incArgs := incrementArgs([]byte("a"), 5)
	if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &incArgs); err != nil {
		t.Fatal(err)
	}

	// Wait for all nodes to catch up.
	mtc.waitForValues(roachpb.Key("a"), 3*time.Second, []int64{5, 5, 5})

	// Stop node 2; while it is down remove the range from nodes 2 and 1.
	mtc.stopStore(2)
	mtc.unreplicateRange(raftID, 0, 2)
	mtc.unreplicateRange(raftID, 0, 1)

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
	util.SucceedsWithin(t, time.Second, func() error {
		mtc.manualClock.Increment(int64(storage.DefaultLeaderLeaseDuration+
			storage.ReplicaGCQueueInactivityThreshold) + 1)
		mtc.stores[1].ForceReplicaGCScanAndProcess(t)

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
		if _, err := client.SendWrapped(rng, nil, &incArgs); err == nil {
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
	mtc.waitForValues(roachpb.Key("a"), 3*time.Second, []int64{16, 0, 5})

	// Run garbage collection on node 2. The lack of an active leader
	// lease will cause GC to do a consistent range lookup, where it
	// will see that the range has been moved and delete the old
	// replica.
	mtc.stores[2].ForceReplicaGCScanAndProcess(t)
	mtc.waitForValues(roachpb.Key("a"), 3*time.Second, []int64{16, 0, 0})

	// Now that the group has been GC'd, the goroutine that was
	// attempting to write has finished (with an error).
	finishWG.Wait()
}

func TestReplicateReAddAfterDown(t *testing.T) {
	defer leaktest.AfterTest(t)

	mtc := startMultiTestContext(t, 3)
	defer mtc.Stop()

	// First put the range on all three nodes.
	raftID := roachpb.RangeID(1)
	mtc.replicateRange(raftID, 0, 1, 2)

	// Put some data in the range so we'll have something to test for.
	incArgs := incrementArgs([]byte("a"), 5)
	if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &incArgs); err != nil {
		t.Fatal(err)
	}

	// Wait for all nodes to catch up.
	mtc.waitForValues(roachpb.Key("a"), 3*time.Second, []int64{5, 5, 5})

	// Stop node 2; while it is down remove the range from it. Since the node is
	// down it won't see the removal and clean up its replica.
	mtc.stopStore(2)
	mtc.unreplicateRange(raftID, 0, 2)

	// Perform another write.
	incArgs = incrementArgs([]byte("a"), 11)
	if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &incArgs); err != nil {
		t.Fatal(err)
	}
	mtc.waitForValues(roachpb.Key("a"), 3*time.Second, []int64{16, 16, 5})

	// Bring it back up and re-add the range. There is a race when the
	// store applies its removal and re-addition back to back: the
	// replica may or may not have (asynchronously) garbage collected
	// its data in between. Whether the existing data is reused or the
	// replica gets recreated, the replica ID is changed by this
	// process. An ill-timed GC has been known to cause bugs including
	// https://github.com/cockroachdb/cockroach/issues/2873.
	mtc.restartStore(2)
	mtc.replicateRange(raftID, 0, 2)

	// The range should be synced back up.
	mtc.waitForValues(roachpb.Key("a"), 3*time.Second, []int64{16, 16, 16})
}

// TestLeaderRemoveSelf verifies that a leader can remove itself
// without panicking and future access to the range returns a
// RangeNotFoundError (not errRaftGroupDeleted, and even before
// the ReplicaGCQueue has run).
func TestLeaderRemoveSelf(t *testing.T) {
	defer leaktest.AfterTest(t)

	mtc := startMultiTestContext(t, 2)
	defer mtc.Stop()
	// Disable the replica GC queue. This verifies that the replica is
	// considered removed even before the gc queue has run, and also
	// helps avoid a deadlock at shutdown.
	mtc.stores[0].DisableReplicaGCQueue(true)
	raftID := roachpb.RangeID(1)
	mtc.replicateRange(raftID, 0, 1)
	// Remove the replica from first store.
	mtc.unreplicateRange(raftID, 0, 0)
	getArgs := getArgs([]byte("a"))

	// Force the read command request a new lease.
	clock := mtc.clocks[0]
	header := roachpb.Header{}
	header.Timestamp = clock.Update(clock.Now().Add(int64(storage.DefaultLeaderLeaseDuration), 0))

	// Expect get a RangeNotFoundError.
	_, err := client.SendWrappedWith(rg1(mtc.stores[0]), nil, header, &getArgs)
	if _, ok := err.(*roachpb.RangeNotFoundError); !ok {
		t.Fatalf("expect get RangeNotFoundError, actual get %v ", err)
	}
}

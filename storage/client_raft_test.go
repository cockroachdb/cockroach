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
// Author: Ben Darnell

package storage_test

import (
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
)

// TestStoreRecoverFromEngine verifies that the store recovers all ranges and their contents
// after being stopped and recreated.
func TestStoreRecoverFromEngine(t *testing.T) {
	defer leaktest.AfterTest(t)
	raftID := int64(1)
	splitKey := proto.Key("m")
	key1 := proto.Key("a")
	key2 := proto.Key("z")

	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	eng := engine.NewInMem(proto.Attributes{}, 1<<20)
	var raftID2 int64

	get := func(store *storage.Store, raftID int64, key proto.Key) int64 {
		args, resp := getArgs(key, raftID, store.StoreID())
		err := store.ExecuteCmd(proto.Get, args, resp)
		if err != nil {
			t.Fatal(err)
		}
		return resp.Value.GetInteger()
	}
	validate := func(store *storage.Store) {
		if val := get(store, raftID, key1); val != 13 {
			t.Errorf("key %q: expected 13 but got %v", key1, val)
		}
		if val := get(store, raftID2, key2); val != 28 {
			t.Errorf("key %q: expected 28 but got %v", key2, val)
		}
	}

	// First, populate the store with data across two ranges. Each range contains commands
	// that both predate and postdate the split.
	func() {
		store := createTestStoreWithEngine(t, eng, clock, true)
		defer store.Stop()

		increment := func(raftID int64, key proto.Key, value int64) (*proto.IncrementResponse, error) {
			args, resp := incrementArgs(key, value, raftID, store.StoreID())
			err := store.ExecuteCmd(proto.Increment, args, resp)
			return resp, err
		}

		if _, err := increment(raftID, key1, 2); err != nil {
			t.Fatal(err)
		}
		if _, err := increment(raftID, key2, 5); err != nil {
			t.Fatal(err)
		}
		splitArgs, splitResp := adminSplitArgs(engine.KeyMin, splitKey, raftID, store.StoreID())
		if err := store.ExecuteCmd(proto.AdminSplit, splitArgs, splitResp); err != nil {
			t.Fatal(err)
		}
		raftID2 = store.LookupRange(key2, nil).Desc().RaftID
		if raftID2 == raftID {
			t.Errorf("got same raft id after split")
		}
		if _, err := increment(raftID, key1, 11); err != nil {
			t.Fatal(err)
		}
		if _, err := increment(raftID2, key2, 23); err != nil {
			t.Fatal(err)
		}
		validate(store)
	}()

	// Now create a new store with the same engine and make sure the expected data is present.
	// We must use the same clock because a newly-created manual clock will be behind the one
	// we wrote with and so will see stale MVCC data.
	store := createTestStoreWithEngine(t, eng, clock, false)
	defer store.Stop()

	validate(store)
}

// TestReplicateRange verifies basic replication functionality by creating two stores
// and a range, replicating the range to the second store, and reading its data there.
func TestReplicateRange(t *testing.T) {
	defer leaktest.AfterTest(t)
	mtc := multiTestContext{}
	mtc.Start(t, 2)
	defer mtc.Stop()

	// Issue a command on the first node before replicating.
	incArgs, incResp := incrementArgs([]byte("a"), 5, 1, mtc.stores[0].StoreID())
	if err := mtc.stores[0].ExecuteCmd(proto.Increment, incArgs, incResp); err != nil {
		t.Fatal(err)
	}

	rng, err := mtc.stores[0].GetRange(1)
	if err != nil {
		t.Fatal(err)
	}

	if err := rng.ChangeReplicas(proto.ADD_REPLICA,
		proto.Replica{
			NodeID:  mtc.stores[1].Ident.NodeID,
			StoreID: mtc.stores[1].Ident.StoreID,
			Attrs:   proto.Attributes{},
		}); err != nil {
		t.Fatal(err)
	}

	// Verify that the same data is available on the replica.
	// TODO(bdarnell): relies on the fact that we allow reads from followers.
	// When we enforce reads from leader/quorum leases, we'll need to introduce a
	// non-transactional local read for tests like this.
	// Also applies to other tests in this file.
	if err := util.IsTrueWithin(func() bool {
		getArgs, getResp := getArgs([]byte("a"), 1, mtc.stores[1].StoreID())
		if err := mtc.stores[1].ExecuteCmd(proto.Get, getArgs, getResp); err != nil {
			return false
		}
		return getResp.Value.GetInteger() == 5
	}, 1*time.Second); err != nil {
		t.Fatal(err)
	}
}

// TestRestoreReplicas ensures that consensus group membership is properly
// persisted to disk and restored when a node is stopped and restarted.
func TestRestoreReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)
	mtc := multiTestContext{}
	mtc.Start(t, 2)
	defer mtc.Stop()

	rng, err := mtc.stores[0].GetRange(1)
	if err != nil {
		t.Fatal(err)
	}

	if err := rng.ChangeReplicas(proto.ADD_REPLICA,
		proto.Replica{
			NodeID:  mtc.stores[1].Ident.NodeID,
			StoreID: mtc.stores[1].Ident.StoreID,
			Attrs:   proto.Attributes{},
		}); err != nil {
		t.Fatal(err)
	}

	// Let the range propagate to the second replica.
	// TODO(bdarnell): we need an observable indication that this has completed.
	// TODO(bdarnell): initial creation and replication needs to be atomic;
	// cutting off the process too soon currently results in a corrupted range.
	// We need 500ms to be safe in -race tests.
	time.Sleep(500 * time.Millisecond)

	mtc.StopStore(0)
	mtc.StopStore(1)
	mtc.RestartStore(0, t)
	mtc.RestartStore(1, t)

	// Send a command on each store. The follower will forward to the leader and both
	// commands will eventually commit.
	incArgs, incResp := incrementArgs([]byte("a"), 5, 1, mtc.stores[0].StoreID())
	if err := mtc.stores[0].ExecuteCmd(proto.Increment, incArgs, incResp); err != nil {
		t.Fatal(err)
	}
	incArgs, incResp = incrementArgs([]byte("a"), 11, 1, mtc.stores[1].StoreID())
	if err := mtc.stores[1].ExecuteCmd(proto.Increment, incArgs, incResp); err != nil {
		t.Fatal(err)
	}

	if err := util.IsTrueWithin(func() bool {
		getArgs, getResp := getArgs([]byte("a"), 1, mtc.stores[1].StoreID())
		if err := mtc.stores[1].ExecuteCmd(proto.Get, getArgs, getResp); err != nil {
			return false
		}
		return getResp.Value.GetInteger() == 16
	}, 1*time.Second); err != nil {
		t.Fatal(err)
	}

	// Both replicas have a complete list in Desc.Replicas
	for i, store := range mtc.stores {
		rng, err := store.GetRange(1)
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
	mtc := multiTestContext{}
	mtc.Start(t, 2)
	defer mtc.Stop()

	rng, err := mtc.stores[0].GetRange(1)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		storage.TestingCommandFilter = nil
	}()
	storage.TestingCommandFilter = func(method string, args proto.Request, reply proto.Response) bool {
		if method == proto.EndTransaction && args.(*proto.EndTransactionRequest).Commit == true {
			reply.Header().SetGoError(util.Errorf("boom"))
			return true
		}
		return false
	}

	err = rng.ChangeReplicas(proto.ADD_REPLICA,
		proto.Replica{
			NodeID:  mtc.stores[1].Ident.NodeID,
			StoreID: mtc.stores[1].Ident.StoreID,
			Attrs:   proto.Attributes{},
		})
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
	storage.TestingCommandFilter = nil

	err = rng.ChangeReplicas(proto.ADD_REPLICA,
		proto.Replica{
			NodeID:  mtc.stores[1].Ident.NodeID,
			StoreID: mtc.stores[1].Ident.StoreID,
			Attrs:   proto.Attributes{},
		})
	if err != nil {
		t.Fatal(err)
	}

	// Wait for the range to sync to both replicas (mainly so leaktest doesn't
	// complain about goroutines involved in the process).
	if err := util.IsTrueWithin(func() bool {
		for _, store := range mtc.stores {
			rng, err := store.GetRange(1)
			if err != nil {
				return false
			}
			if len(rng.Desc().Replicas) == 1 {
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
	mtc := multiTestContext{}
	mtc.Start(t, 2)
	defer mtc.Stop()

	rng, err := mtc.stores[0].GetRange(1)
	if err != nil {
		t.Fatal(err)
	}

	// Issue a command on the first node before replicating.
	incArgs, incResp := incrementArgs([]byte("a"), 5, 1, mtc.stores[0].StoreID())
	if err := mtc.stores[0].ExecuteCmd(proto.Increment, incArgs, incResp); err != nil {
		t.Fatal(err)
	}

	// Get that command's log index.
	index, err := rng.LastIndex()
	if err != nil {
		t.Fatal(err)
	}

	// Truncate the log at index+1 (log entries < N are removed, so this includes
	// the increment).
	truncArgs, truncResp := internalTruncateLogArgs(index+1, 1, mtc.stores[0].StoreID())
	if err := mtc.stores[0].ExecuteCmd(proto.InternalTruncateLog, truncArgs, truncResp); err != nil {
		t.Fatal(err)
	}

	// Issue a second command post-truncation.
	incArgs, incResp = incrementArgs([]byte("a"), 11, 1, mtc.stores[0].StoreID())
	if err := mtc.stores[0].ExecuteCmd(proto.Increment, incArgs, incResp); err != nil {
		t.Fatal(err)
	}

	// Now add the second replica.
	if err := rng.ChangeReplicas(proto.ADD_REPLICA,
		proto.Replica{
			NodeID:  mtc.stores[1].Ident.NodeID,
			StoreID: mtc.stores[1].Ident.StoreID,
			Attrs:   proto.Attributes{},
		}); err != nil {
		t.Fatal(err)
	}

	// Once it catches up, the effects of both commands can be seen.
	if err := util.IsTrueWithin(func() bool {
		getArgs, getResp := getArgs([]byte("a"), 1, mtc.stores[1].StoreID())
		if err := mtc.stores[1].ExecuteCmd(proto.Get, getArgs, getResp); err != nil {
			return false
		}
		log.Infof("read value %d", getResp.Value.GetInteger())
		return getResp.Value.GetInteger() == 16
	}, 1*time.Second); err != nil {
		t.Fatal(err)
	}

	// Send a third command to verify that the log states are synced up so the
	// new node can accept new commands.
	incArgs, incResp = incrementArgs([]byte("a"), 23, 1, mtc.stores[0].StoreID())
	if err := mtc.stores[0].ExecuteCmd(proto.Increment, incArgs, incResp); err != nil {
		t.Fatal(err)
	}

	if err := util.IsTrueWithin(func() bool {
		getArgs, getResp := getArgs([]byte("a"), 1, mtc.stores[1].StoreID())
		if err := mtc.stores[1].ExecuteCmd(proto.Get, getArgs, getResp); err != nil {
			return false
		}
		log.Infof("read value %d", getResp.Value.GetInteger())
		return getResp.Value.GetInteger() == 39
	}, 1*time.Second); err != nil {
		t.Fatal(err)
	}
}

// TestStoreRangeReplicate verifies that the replication queue will notice
// under-replicated ranges and replicate them.
func TestStoreRangeReplicate(t *testing.T) {
	defer leaktest.AfterTest(t)
	mtc := multiTestContext{}
	mtc.Start(t, 3)
	defer mtc.Stop()

	// Initialize the gossip network.
	for _, s := range mtc.stores {
		s.GossipCapacity(&storage.NodeDescriptor{NodeID: s.Ident.NodeID})
	}
	mtc.stores[0].WaitForNodes(3)

	// Once we know our peers, trigger a scan.
	mtc.stores[0].ForceReplicationScan()

	// The range should become available on every node.
	if err := util.IsTrueWithin(func() bool {
		for _, s := range mtc.stores {
			r := s.LookupRange(proto.Key("a"), proto.Key("b"))
			if r == nil {
				return false
			}
		}
		return true
	}, 1*time.Second); err != nil {
		t.Fatal(err)
	}
}

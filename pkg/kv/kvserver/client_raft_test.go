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
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"google.golang.org/grpc"
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
	storeCfg := kvserver.TestStoreConfig(nil /* clock */)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	storeCfg.TestingKnobs.DisableMergeQueue = true

	const rangeID = roachpb.RangeID(1)
	splitKey := roachpb.Key("m")
	key1 := roachpb.Key("a")
	key2 := roachpb.Key("z")

	engineStopper := stop.NewStopper()
	defer engineStopper.Stop(context.Background())
	eng := storage.NewDefaultInMem()
	engineStopper.AddCloser(eng)
	var rangeID2 roachpb.RangeID

	get := func(store *kvserver.Store, rangeID roachpb.RangeID, key roachpb.Key) int64 {
		args := getArgs(key)
		resp, err := kv.SendWrappedWith(context.Background(), store.TestSender(), roachpb.Header{
			RangeID: rangeID,
		}, args)
		if err != nil {
			t.Fatal(err)
		}
		return mustGetInt(resp.(*roachpb.GetResponse).Value)
	}
	validate := func(store *kvserver.Store) {
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
		defer stopper.Stop(context.Background())
		store := createTestStoreWithOpts(t,
			testStoreOpts{
				eng: eng,
				cfg: &storeCfg,
				// This test was written before the test stores were able to start with
				// more than one range and is not prepared to handle many ranges.
				dontCreateSystemRanges: true,
			},
			stopper)

		increment := func(rangeID roachpb.RangeID, key roachpb.Key, value int64) (*roachpb.IncrementResponse, *roachpb.Error) {
			args := incrementArgs(key, value)
			resp, err := kv.SendWrappedWith(context.Background(), store.TestSender(), roachpb.Header{
				RangeID: rangeID,
			}, args)
			incResp, _ := resp.(*roachpb.IncrementResponse)
			return incResp, err
		}

		if _, err := increment(rangeID, key1, 2); err != nil {
			t.Fatal(err)
		}
		if _, err := increment(rangeID, key2, 5); err != nil {
			t.Fatal(err)
		}
		splitArgs := adminSplitArgs(splitKey)
		if _, err := kv.SendWrapped(context.Background(), store.TestSender(), splitArgs); err != nil {
			t.Fatal(err)
		}
		rangeID2 = store.LookupReplica(roachpb.RKey(key2)).RangeID
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
	store := createTestStoreWithOpts(t,
		testStoreOpts{
			dontBootstrap: true,
			eng:           eng,
			cfg:           &storeCfg,
		},
		engineStopper)

	// Raft processing is initialized lazily; issue a no-op write request on each key to
	// ensure that is has been started.
	incArgs := incrementArgs(key1, 0)
	if _, err := kv.SendWrapped(context.Background(), store.TestSender(), incArgs); err != nil {
		t.Fatal(err)
	}
	incArgs = incrementArgs(key2, 0)
	if _, err := kv.SendWrappedWith(context.Background(), store.TestSender(), roachpb.Header{
		RangeID: rangeID2,
	}, incArgs); err != nil {
		t.Fatal(err)
	}

	validate(store)
}

// TestStoreRecoverWithErrors verifies that even commands that fail are marked as
// applied so they are not retried after recovery.
func TestStoreRecoverWithErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	storeCfg := kvserver.TestStoreConfig(nil)
	// Splits can cause our chosen keys to end up on ranges other than range 1,
	// and trying to handle that complicates the test without providing any
	// added benefit.
	storeCfg.TestingKnobs.DisableSplitQueue = true
	eng := storage.NewDefaultInMem()
	defer eng.Close()

	numIncrements := 0

	func() {
		stopper := stop.NewStopper()
		defer stopper.Stop(context.Background())
		keyA := roachpb.Key("a")
		storeCfg := storeCfg // copy
		storeCfg.TestingKnobs.EvalKnobs.TestingEvalFilter =
			func(filterArgs kvserverbase.FilterArgs) *roachpb.Error {
				_, ok := filterArgs.Req.(*roachpb.IncrementRequest)
				if ok && filterArgs.Req.Header().Key.Equal(keyA) {
					numIncrements++
				}
				return nil
			}
		store := createTestStoreWithOpts(
			t,
			testStoreOpts{eng: eng, cfg: &storeCfg},
			stopper)

		// Write a bytes value so the increment will fail.
		putArgs := putArgs(keyA, []byte("asdf"))
		if _, err := kv.SendWrapped(context.Background(), store.TestSender(), putArgs); err != nil {
			t.Fatal(err)
		}

		// Try and fail to increment the key. It is important for this test that the
		// failure be the last thing in the raft log when the store is stopped.
		incArgs := incrementArgs(keyA, 42)
		if _, err := kv.SendWrapped(context.Background(), store.TestSender(), incArgs); err == nil {
			t.Fatal("did not get expected error")
		}
	}()

	if numIncrements != 1 {
		t.Fatalf("expected 1 increments; was %d", numIncrements)
	}

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	// Recover from the engine.
	store := createTestStoreWithOpts(t,
		testStoreOpts{
			dontBootstrap: true,
			eng:           eng,
			cfg:           &storeCfg,
		},
		stopper)

	// Issue a no-op write to lazily initialize raft on the range.
	keyB := roachpb.Key("b")
	incArgs := incrementArgs(keyB, 0)
	if _, err := kv.SendWrapped(context.Background(), store.TestSender(), incArgs); err != nil {
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
	mtc := &multiTestContext{
		// This test was written before the multiTestContext started creating many
		// system ranges at startup, and hasn't been update to take that into
		// account.
		startWithSingleRange: true,
	}
	defer mtc.Stop()
	mtc.Start(t, 2)

	// Issue a command on the first node before replicating.
	incArgs := incrementArgs([]byte("a"), 5)
	if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), incArgs); err != nil {
		t.Fatal(err)
	}

	repl, err := mtc.stores[0].GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	chgs := roachpb.MakeReplicationChanges(roachpb.ADD_REPLICA, roachpb.ReplicationTarget{
		NodeID:  mtc.stores[1].Ident.NodeID,
		StoreID: mtc.stores[1].Ident.StoreID,
	})
	if _, err := repl.ChangeReplicas(context.Background(), repl.Desc(), kvserver.SnapshotRequest_REBALANCE, kvserverpb.ReasonRangeUnderReplicated, "", chgs); err != nil {
		t.Fatal(err)
	}
	// Verify no intent remains on range descriptor key.
	key := keys.RangeDescriptorKey(repl.Desc().StartKey)
	desc := roachpb.RangeDescriptor{}
	if ok, err := storage.MVCCGetProto(context.Background(), mtc.stores[0].Engine(), key,
		mtc.stores[0].Clock().Now(), &desc, storage.MVCCGetOptions{}); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatalf("range descriptor key %s was not found", key)
	}
	// Verify that in time, no intents remain on meta addressing
	// keys, and that range descriptor on the meta records is correct.
	testutils.SucceedsSoon(t, func() error {
		meta2 := keys.RangeMetaKey(roachpb.RKeyMax)
		meta1 := keys.RangeMetaKey(meta2)
		for _, key := range []roachpb.RKey{meta2, meta1} {
			metaDesc := roachpb.RangeDescriptor{}
			if ok, err := storage.MVCCGetProto(context.Background(), mtc.stores[0].Engine(), key.AsRawKey(),
				mtc.stores[0].Clock().Now(), &metaDesc, storage.MVCCGetOptions{}); err != nil {
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
	testutils.SucceedsSoon(t, func() error {
		getArgs := getArgs([]byte("a"))
		if reply, err := kv.SendWrappedWith(context.Background(), mtc.stores[1].TestSender(), roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, getArgs); err != nil {
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

	t.Skip("https://github.com/cockroachdb/cockroach/issues/40351")

	sc := kvserver.TestStoreConfig(nil)
	// Disable periodic gossip activities. The periodic gossiping of the first
	// range can cause spurious lease transfers which cause this test to fail.
	sc.TestingKnobs.DisablePeriodicGossips = true
	// Allow a replica to use the lease it had before a restart; we don't want
	// this test to deal with needing to acquire new leases after the restart.
	sc.TestingKnobs.DontPreventUseOfOldLeaseOnStart = true
	mtc := &multiTestContext{
		storeConfig: &sc,
		// This test was written before the multiTestContext started creating many
		// system ranges at startup, and hasn't been update to take that into
		// account.
		startWithSingleRange: true,
	}
	defer mtc.Stop()
	mtc.Start(t, 2)

	firstRng, err := mtc.stores[0].GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	// Perform an increment before replication to ensure that commands are not
	// repeated on restarts.
	incArgs := incrementArgs([]byte("a"), 23)
	if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), incArgs); err != nil {
		t.Fatal(err)
	}

	chgs := roachpb.MakeReplicationChanges(roachpb.ADD_REPLICA, roachpb.ReplicationTarget{
		NodeID:  mtc.stores[1].Ident.NodeID,
		StoreID: mtc.stores[1].Ident.StoreID,
	})
	if _, err := firstRng.ChangeReplicas(context.Background(), firstRng.Desc(), kvserver.SnapshotRequest_REBALANCE, kvserverpb.ReasonRangeUnderReplicated, "", chgs); err != nil {
		t.Fatal(err)
	}

	mtc.restart()

	// Send a command on each store. The original store (the lease holder still)
	// will succeed.
	incArgs = incrementArgs([]byte("a"), 5)
	if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), incArgs); err != nil {
		t.Fatal(err)
	}
	// The follower will return a not lease holder error, indicating the command
	// should be forwarded to the lease holder.
	incArgs = incrementArgs([]byte("a"), 11)
	{
		_, pErr := kv.SendWrapped(context.Background(), mtc.stores[1].TestSender(), incArgs)
		if _, ok := pErr.GetDetail().(*roachpb.NotLeaseHolderError); !ok {
			t.Fatalf("expected not lease holder error; got %s", pErr)
		}
	}
	// Send again, this time to first store.
	if _, pErr := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), incArgs); pErr != nil {
		t.Fatal(pErr)
	}

	testutils.SucceedsSoon(t, func() error {
		getArgs := getArgs([]byte("a"))
		if reply, err := kv.SendWrappedWith(context.Background(), mtc.stores[1].TestSender(), roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, getArgs); err != nil {
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
		if len(desc.InternalReplicas) != 2 {
			t.Fatalf("store %d: expected 2 replicas, found %d", i, len(desc.InternalReplicas))
		}
		if desc.InternalReplicas[0].NodeID != mtc.stores[0].Ident.NodeID {
			t.Errorf("store %d: expected replica[0].NodeID == %d, was %d",
				i, mtc.stores[0].Ident.NodeID, desc.InternalReplicas[0].NodeID)
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

	sc := kvserver.TestStoreConfig(nil)
	sc.Clock = nil // manual clock
	sc.TestingKnobs.EvalKnobs.TestingEvalFilter = func(filterArgs kvserverbase.FilterArgs) *roachpb.Error {
		if runFilter.Load().(bool) {
			if et, ok := filterArgs.Req.(*roachpb.EndTxnRequest); ok && et.Commit {
				return roachpb.NewErrorWithTxn(errors.Errorf("boom"), filterArgs.Hdr.Txn)
			}
		}
		return nil
	}
	mtc := &multiTestContext{storeConfig: &sc}
	defer mtc.Stop()
	mtc.Start(t, 2)

	repl, err := mtc.stores[0].GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	chgs := roachpb.MakeReplicationChanges(roachpb.ADD_REPLICA, roachpb.ReplicationTarget{
		NodeID:  mtc.stores[1].Ident.NodeID,
		StoreID: mtc.stores[1].Ident.StoreID,
	})
	if _, err := repl.ChangeReplicas(context.Background(), repl.Desc(), kvserver.SnapshotRequest_REBALANCE, kvserverpb.ReasonRangeUnderReplicated, "", chgs); !testutils.IsError(err, "boom") {
		t.Fatalf("did not get expected error: %+v", err)
	}

	// After the aborted transaction, r.Desc was not updated.
	// TODO(bdarnell): expose and inspect raft's internal state.
	if replicas := repl.Desc().InternalReplicas; len(replicas) != 1 {
		t.Fatalf("expected 1 replica, found %v", replicas)
	}

	// The pending config change flag was cleared, so a subsequent attempt
	// can succeed.
	runFilter.Store(false)

	// The first failed replica change has laid down intents. Make sure those
	// are pushable by making the transaction abandoned.
	mtc.manualClock.Increment(10 * base.DefaultTxnHeartbeatInterval.Nanoseconds())

	if _, err := repl.ChangeReplicas(context.Background(), repl.Desc(), kvserver.SnapshotRequest_REBALANCE, kvserverpb.ReasonRangeUnderReplicated, "", chgs); err != nil {
		t.Fatal(err)
	}

	// Wait for the range to sync to both replicas (mainly so leaktest doesn't
	// complain about goroutines involved in the process).
	testutils.SucceedsSoon(t, func() error {
		for _, store := range mtc.stores {
			rang, err := store.GetReplica(1)
			if err != nil {
				return err
			}
			if replicas := rang.Desc().InternalReplicas; len(replicas) <= 1 {
				return errors.Errorf("expected > 1 replicas; got %v", replicas)
			}
		}
		return nil
	})
}

// We can truncate the old log entries and a new replica will be brought up from a snapshot.
func TestReplicateAfterTruncation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := &multiTestContext{
		// This test was written before the multiTestContext started creating many
		// system ranges at startup, and hasn't been update to take that into
		// account.
		startWithSingleRange: true,
	}
	defer mtc.Stop()
	mtc.Start(t, 2)

	repl, err := mtc.stores[0].GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	// Issue a command on the first node before replicating.
	incArgs := incrementArgs([]byte("a"), 5)
	if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), incArgs); err != nil {
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
	if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), truncArgs); err != nil {
		t.Fatal(err)
	}

	// Issue a second command post-truncation.
	incArgs = incrementArgs([]byte("a"), 11)
	if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), incArgs); err != nil {
		t.Fatal(err)
	}

	// Now add the second replica.
	chgs := roachpb.MakeReplicationChanges(roachpb.ADD_REPLICA, roachpb.ReplicationTarget{
		NodeID:  mtc.stores[1].Ident.NodeID,
		StoreID: mtc.stores[1].Ident.StoreID,
	})
	if _, err := repl.ChangeReplicas(context.Background(), repl.Desc(), kvserver.SnapshotRequest_REBALANCE, kvserverpb.ReasonRangeUnderReplicated, "", chgs); err != nil {
		t.Fatal(err)
	}

	// Once it catches up, the effects of both commands can be seen.
	testutils.SucceedsSoon(t, func() error {
		getArgs := getArgs([]byte("a"))
		if reply, err := kv.SendWrappedWith(context.Background(), mtc.stores[1].TestSender(), roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, getArgs); err != nil {
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

	testutils.SucceedsSoon(t, func() error {
		if mvcc, mvcc2 := repl.GetMVCCStats(), repl2.GetMVCCStats(); mvcc2 != mvcc {
			return errors.Errorf("expected stats on new range:\n%+v\not equal old:\n%+v", mvcc2, mvcc)
		}
		return nil
	})

	// Send a third command to verify that the log states are synced up so the
	// new node can accept new commands.
	incArgs = incrementArgs([]byte("a"), 23)
	if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), incArgs); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		getArgs := getArgs([]byte("a"))
		if reply, err := kv.SendWrappedWith(context.Background(), mtc.stores[1].TestSender(), roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, getArgs); err != nil {
			return errors.Errorf("failed to read data: %s", err)
		} else if e, v := int64(39), mustGetInt(reply.(*roachpb.GetResponse).Value); v != e {
			return errors.Errorf("failed to read correct data: expected %d, got %d", e, v)
		}
		return nil
	})
}

func TestRaftLogSizeAfterTruncation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := &multiTestContext{
		// This test was written before the multiTestContext started creating many
		// system ranges at startup, and hasn't been update to take that into
		// account.
		startWithSingleRange: true,
	}
	defer mtc.Stop()
	mtc.Start(t, 1)

	const rangeID = 1

	repl, err := mtc.stores[0].GetReplica(rangeID)
	if err != nil {
		t.Fatal(err)
	}

	key := []byte("a")
	incArgs := incrementArgs(key, 5)
	if _, err := kv.SendWrapped(
		context.Background(), mtc.stores[0].TestSender(), incArgs); err != nil {
		t.Fatal(err)
	}

	index, err := repl.GetLastIndex()
	if err != nil {
		t.Fatal(err)
	}

	// Verifies the recomputed log size against what we track in `r.mu.raftLogSize`.
	assertCorrectRaftLogSize := func() error {
		// Recompute under raft lock so that the log doesn't change while we
		// compute its size.
		repl.RaftLock()
		realSize, err := kvserver.ComputeRaftLogSize(
			context.Background(), repl.RangeID, repl.Engine(), repl.SideloadedRaftMuLocked(),
		)
		size, _ := repl.GetRaftLogSize()
		repl.RaftUnlock()

		if err != nil {
			t.Fatal(err)
		}

		// If the size isn't trusted, it won't have to match (and in fact
		// likely won't). In this test, this is because the upreplication
		// elides old Raft log entries in the snapshot it uses.
		if size != realSize {
			return fmt.Errorf("%s: raft log claims size %d, but is in fact %d", repl, size, realSize)
		}
		return nil
	}

	assert.NoError(t, assertCorrectRaftLogSize())

	truncArgs := truncateLogArgs(index+1, 1)
	if _, err := kv.SendWrapped(
		context.Background(), mtc.stores[0].TestSender(), truncArgs); err != nil {
		t.Fatal(err)
	}

	// Note that if there were multiple nodes, the Raft log sizes would not
	// be correct for the followers as they would have received a shorter
	// Raft log than the leader.
	assert.NoError(t, assertCorrectRaftLogSize())
}

// TestSnapshotAfterTruncation tests that Raft will properly send a
// non-preemptive snapshot when a node is brought up and the log has been
// truncated.
func TestSnapshotAfterTruncation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, changeTerm := range []bool{false, true} {
		name := "sameTerm"
		if changeTerm {
			name = "differentTerm"
		}
		t.Run(name, func(t *testing.T) {
			mtc := &multiTestContext{
				// This test was written before the multiTestContext started creating many
				// system ranges at startup, and hasn't been update to take that into
				// account.
				startWithSingleRange: true,
			}
			defer mtc.Stop()
			mtc.Start(t, 3)
			const stoppedStore = 1
			repl0, err := mtc.stores[0].GetReplica(1)
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
			if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), incArgs); err != nil {
				t.Fatal(err)
			}

			mtc.replicateRange(1, 1, 2)
			mtc.waitForValues(key, []int64{incA, incA, incA})

			// Now kill one store, increment the key on the other stores and truncate
			// their logs to make sure that when store 1 comes back up it will require a
			// non-preemptive snapshot from Raft.
			mtc.stopStore(stoppedStore)

			incArgs = incrementArgs(key, incB)
			if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), incArgs); err != nil {
				t.Fatal(err)
			}

			mtc.waitForValues(key, []int64{incAB, incA, incAB})

			index, err := repl0.GetLastIndex()
			if err != nil {
				t.Fatal(err)
			}

			// Truncate the log at index+1 (log entries < N are removed, so this
			// includes the increment).
			truncArgs := truncateLogArgs(index+1, 1)
			if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), truncArgs); err != nil {
				t.Fatal(err)
			}

			if changeTerm {
				for i := range mtc.stores {
					if i != stoppedStore {
						// Stop and restart all the live stores, which guarantees that
						// we won't be in the same term we started with.
						mtc.stopStore(i)
						mtc.restartStore(i)
						// Disable the snapshot queue on the live stores so that
						// stoppedStore won't get a snapshot as soon as it starts
						// back up.
						mtc.stores[i].SetRaftSnapshotQueueActive(false)
					}
				}

				// Restart the stopped store and wait for raft
				// election/heartbeat traffic to settle down. Specifically, we
				// need stoppedStore to know about the new term number before
				// the snapshot is sent to reproduce #13506. If the snapshot
				// happened before it learned the term, it would accept the
				// snapshot no matter what term it contained.
				//
				// We do not wait for the store to successfully heartbeat
				// because it is not expected to succeed in cases where the
				// other two stores have already completed their leader
				// election. In this case, a successful heartbeat won't be
				// possible until we re-enable snapshots.
				mtc.restartStoreWithoutHeartbeat(stoppedStore)
				testutils.SucceedsSoon(t, func() error {
					hasLeader := false
					term := uint64(0)
					for i := range mtc.stores {
						repl, err := mtc.stores[i].GetReplica(1)
						if err != nil {
							return err
						}
						status := repl.RaftStatus()
						if status == nil {
							return errors.New("raft status not initialized")
						}
						if status.RaftState == raft.StateLeader {
							hasLeader = true
						}
						if term == 0 {
							term = status.Term
						} else if status.Term != term {
							return errors.Errorf("terms do not agree: %d vs %d", status.Term, term)
						}
					}
					if !hasLeader {
						return errors.New("no leader")
					}
					return nil
				})

				// Turn the queues back on and wait for the snapshot to be sent and processed.
				for i, store := range mtc.stores {
					if i != stoppedStore {
						store.SetRaftSnapshotQueueActive(true)
						if err := store.ForceRaftSnapshotQueueProcess(); err != nil {
							t.Fatal(err)
						}
					}
				}
			} else { // !changeTerm
				mtc.restartStore(stoppedStore)
			}
			mtc.waitForValues(key, []int64{incAB, incAB, incAB})

			testutils.SucceedsSoon(t, func() error {
				// Verify that the cached index and term (Replica.mu.last{Index,Term}))
				// on all of the replicas is the same. #18327 fixed an issue where the
				// cached term was left unchanged after applying a snapshot leading to a
				// persistently unavailable range.
				repl0, err = mtc.stores[0].GetReplica(1)
				if err != nil {
					t.Fatal(err)
				}
				expectedLastIndex, _ := repl0.GetLastIndex()
				expectedLastTerm := repl0.GetCachedLastTerm()

				for i := 1; i < len(mtc.stores); i++ {
					repl1, err := mtc.stores[i].GetReplica(1)
					if err != nil {
						return err
					}
					if lastIndex, _ := repl1.GetLastIndex(); expectedLastIndex != lastIndex {
						return fmt.Errorf("%d: expected last index %d, but found %d", i, expectedLastIndex, lastIndex)
					}
					if lastTerm := repl1.GetCachedLastTerm(); expectedLastTerm != lastTerm {
						return fmt.Errorf("%d: expected last term %d, but found %d", i, expectedLastTerm, lastTerm)
					}
				}
				return nil
			})
		})
	}
}

// TestSnapshotAfterTruncationWithUncommittedTail is similar in spirit to
// TestSnapshotAfterTruncation/differentTerm. However, it differs in that we
// take care to ensure that the partitioned Replica has a long uncommitted tail
// of Raft entries that is not entirely overwritten by the snapshot it receives
// after the partition heals. If the recipient of the snapshot did not purge its
// Raft entry cache when receiving the snapshot, it could get stuck repeatedly
// rejecting attempts to catch it up. This serves as a regression test for the
// bug seen in #37056.
func TestSnapshotAfterTruncationWithUncommittedTail(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	mtc := &multiTestContext{
		// This test was written before the multiTestContext started creating many
		// system ranges at startup, and hasn't been update to take that into
		// account.
		startWithSingleRange: true,
	}
	defer mtc.Stop()
	mtc.Start(t, 3)

	key := roachpb.Key("a")
	incA := int64(5)
	incB := int64(7)
	incC := int64(9)
	incAB := incA + incB
	incABC := incAB + incC

	// Set up a key to replicate across the cluster. We're going to modify this
	// key and truncate the raft logs from that command after partitioning one
	// of the nodes to check that it gets the new value after it reconnects.
	// We're then going to continue modifying this key to make sure that the
	// temporarily partitioned node can continue to receive updates.
	incArgs := incrementArgs(key, incA)
	if _, pErr := kv.SendWrapped(ctx, mtc.stores[0].TestSender(), incArgs); pErr != nil {
		t.Fatal(pErr)
	}

	mtc.replicateRange(1, 1, 2)
	mtc.waitForValues(key, []int64{incA, incA, incA})

	// We partition the original leader from the other two replicas. This allows
	// us to build up a large uncommitted Raft log on the partitioned node.
	const partStore = 0
	partRepl, err := mtc.stores[partStore].GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}
	partReplDesc, err := partRepl.GetReplicaDescriptor()
	if err != nil {
		t.Fatal(err)
	}
	partReplSender := mtc.stores[partStore].TestSender()

	// Partition the original leader from its followers. We do this by installing
	// unreliableRaftHandler listeners on all three Stores. The handler on the
	// partitioned store filters out all messages while the handler on the other
	// two stores only filters out messages from the partitioned store. The
	// configuration looks like:
	//
	//           [0]
	//          x  x
	//         /    \
	//        x      x
	//      [1]<---->[2]
	//
	for _, s := range []int{0, 1, 2} {
		h := &unreliableRaftHandler{rangeID: 1, RaftMessageHandler: mtc.stores[s]}
		if s != partStore {
			// Only filter messages from the partitioned store on the other
			// two stores.
			h.dropReq = func(req *kvserver.RaftMessageRequest) bool {
				return req.FromReplica.StoreID == partRepl.StoreID()
			}
			h.dropHB = func(hb *kvserver.RaftHeartbeat) bool {
				return hb.FromReplicaID == partReplDesc.ReplicaID
			}
		}
		mtc.transport.Listen(mtc.stores[s].Ident.StoreID, h)
	}

	// Perform a series of writes on the partitioned replica. The writes will
	// not succeed before their context is canceled, but they will be appended
	// to the partitioned replica's Raft log because it is currently the Raft
	// leader.
	g := ctxgroup.WithContext(ctx)
	for i := 0; i < 32; i++ {
		otherKey := roachpb.Key(fmt.Sprintf("other-%d", i))
		g.GoCtx(func(ctx context.Context) error {
			cCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
			defer cancel()
			incArgsOther := incrementArgs(otherKey, 1)
			if _, pErr := kv.SendWrapped(cCtx, partReplSender, incArgsOther); pErr == nil {
				return errors.New("unexpected success")
			} else if !testutils.IsPError(pErr, "context deadline exceeded") {
				return pErr.GoError()
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}

	// Transfer the lease to one of the followers and perform a write. The
	// partition ensures that this will require a Raft leadership change.
	const newLeaderStore = partStore + 1
	newLeaderRepl, err := mtc.stores[newLeaderStore].GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}
	newLeaderReplSender := mtc.stores[newLeaderStore].TestSender()

	incArgs = incrementArgs(key, incB)
	testutils.SucceedsSoon(t, func() error {
		mtc.advanceClock(ctx)
		_, pErr := kv.SendWrapped(ctx, newLeaderReplSender, incArgs)
		if _, ok := pErr.GetDetail().(*roachpb.NotLeaseHolderError); ok {
			return pErr.GoError()
		} else if pErr != nil {
			t.Fatal(pErr)
		}
		return nil
	})
	mtc.waitForValues(key, []int64{incA, incAB, incAB})

	index, err := newLeaderRepl.GetLastIndex()
	if err != nil {
		t.Fatal(err)
	}

	// Truncate the log at index+1 (log entries < N are removed, so this
	// includes the increment).
	truncArgs := truncateLogArgs(index+1, 1)
	testutils.SucceedsSoon(t, func() error {
		mtc.advanceClock(ctx)
		_, pErr := kv.SendWrapped(ctx, newLeaderReplSender, truncArgs)
		if _, ok := pErr.GetDetail().(*roachpb.NotLeaseHolderError); ok {
			return pErr.GoError()
		} else if pErr != nil {
			t.Fatal(pErr)
		}
		return nil
	})

	snapsMetric := mtc.stores[partStore].Metrics().RangeSnapshotsNormalApplied
	snapsBefore := snapsMetric.Count()

	// Remove the partition. Snapshot should follow.
	for _, s := range []int{0, 1, 2} {
		mtc.transport.Listen(mtc.stores[s].Ident.StoreID, &unreliableRaftHandler{
			rangeID:            1,
			RaftMessageHandler: mtc.stores[s],
			unreliableRaftHandlerFuncs: unreliableRaftHandlerFuncs{
				dropReq: func(req *kvserver.RaftMessageRequest) bool {
					// Make sure that even going forward no MsgApp for what we just truncated can
					// make it through. The Raft transport is asynchronous so this is necessary
					// to make the test pass reliably.
					// NB: the Index on the message is the log index that _precedes_ any of the
					// entries in the MsgApp, so filter where msg.Index < index, not <= index.
					return req.Message.Type == raftpb.MsgApp && req.Message.Index < index
				},
				dropHB:   func(*kvserver.RaftHeartbeat) bool { return false },
				dropResp: func(*kvserver.RaftMessageResponse) bool { return false },
			},
		})
	}

	// The partitioned replica should catch up after a snapshot.
	testutils.SucceedsSoon(t, func() error {
		snapsAfter := snapsMetric.Count()
		if !(snapsAfter > snapsBefore) {
			return errors.New("expected at least 1 snapshot to catch the partitioned replica up")
		}
		return nil
	})
	mtc.waitForValues(key, []int64{incAB, incAB, incAB})

	// Perform another write. The partitioned replica should be able to receive
	// replicated updates.
	incArgs = incrementArgs(key, incC)
	if _, pErr := kv.SendWrapped(ctx, mtc.distSenders[0], incArgs); pErr != nil {
		t.Fatal(pErr)
	}
	mtc.waitForValues(key, []int64{incABC, incABC, incABC})
}

type fakeSnapshotStream struct {
	nextReq *kvserver.SnapshotRequest
	nextErr error
}

// Recv implements the SnapshotResponseStream interface.
func (c fakeSnapshotStream) Recv() (*kvserver.SnapshotRequest, error) {
	return c.nextReq, c.nextErr
}

// Send implements the SnapshotResponseStream interface.
func (c fakeSnapshotStream) Send(request *kvserver.SnapshotResponse) error {
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
	mtc := &multiTestContext{}
	defer mtc.Stop()
	mtc.Start(t, 3)

	rep, err := mtc.stores[0].GetReplica(1)
	require.NoError(t, err)
	repDesc, err := rep.GetReplicaDescriptor()
	require.NoError(t, err)
	desc := protoutil.Clone(rep.Desc()).(*roachpb.RangeDescriptor)
	desc.AddReplica(2, 2, roachpb.LEARNER)
	rep2Desc, found := desc.GetReplicaDescriptor(2)
	require.True(t, found)
	header := kvserver.SnapshotRequest_Header{
		CanDecline: true,
		RangeSize:  100,
		State:      kvserverpb.ReplicaState{Desc: desc},
		RaftMessageRequest: kvserver.RaftMessageRequest{
			RangeID:     rep.RangeID,
			FromReplica: repDesc,
			ToReplica:   rep2Desc,
		},
	}
	header.RaftMessageRequest.Message.Snapshot.Data = uuid.UUID{}.GetBytes()
	// Cause this stream to return an error as soon as we ask it for something.
	// This injects an error into HandleSnapshotStream when we try to send the
	// "snapshot accepted" message.
	expectedErr := errors.Errorf("")
	stream := fakeSnapshotStream{nil, expectedErr}
	if err := mtc.stores[1].HandleSnapshot(&header, stream); !errors.Is(err, expectedErr) {
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
	// This test relies on concurrently waiting for a value to change in the
	// underlying engine(s). Since the teeing engine does not respond well to
	// value mismatches, whether transient or permanent, skip this test if the
	// teeing engine is being used. See
	// https://github.com/cockroachdb/cockroach/issues/42656 for more context.
	if storage.DefaultStorageEngine == enginepb.EngineTypeTeePebbleRocksDB {
		t.Skip("disabled on teeing engine")
	}

	mtc := &multiTestContext{
		// This test was written before the multiTestContext started creating many
		// system ranges at startup, and hasn't been update to take that into
		// account.
		startWithSingleRange: true,
	}
	defer mtc.Stop()
	mtc.Start(t, 5)
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
	if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), incArgs); err != nil {
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
	if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), incArgs); err != nil {
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
	if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), truncArgs); err != nil {
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

	sc := kvserver.TestStoreConfig(nil)
	sc.TestingKnobs.DisableMergeQueue = true
	sc.TestingKnobs.DisableReplicateQueue = true
	// Disable the replica GC queue so that it doesn't accidentally pick up the
	// removed replica and GC it. We'll explicitly enable it later in the test.
	sc.TestingKnobs.DisableReplicaGCQueue = true
	// Disable eager replica removal so we can manually remove the replica.
	sc.TestingKnobs.DisableEagerReplicaRemoval = true
	sc.Clock = nil // manual clock
	mtc := &multiTestContext{
		storeConfig: &sc,
		// This test was written before the multiTestContext started creating many
		// system ranges at startup, and hasn't been update to take that into
		// account.
		startWithSingleRange: true,
	}
	defer mtc.Stop()
	mtc.Start(t, 3)
	rep1, err := mtc.stores[0].GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	const rangeID = roachpb.RangeID(1)
	mtc.replicateRange(rangeID, 1, 2)

	// Kill store 2.
	mtc.stopStore(2)

	// Remove store 2 from the range to simulate removal of a dead node.
	mtc.unreplicateRange(rangeID, 2)

	// Split the range.
	splitKey := roachpb.Key("m")
	splitArgs := adminSplitArgs(splitKey)
	if _, err := rep1.AdminSplit(context.Background(), *splitArgs, "test"); err != nil {
		t.Fatal(err)
	}

	mtc.advanceClock(context.Background())

	// Restart store 2.
	mtc.restartStore(2)

	replicateRHS := func() error {
		// Try to up-replicate the RHS of the split to store 2. We can't use
		// replicateRange because this should fail on the first attempt and then
		// eventually succeed.
		startKey := roachpb.RKey(splitKey)

		var desc roachpb.RangeDescriptor
		if err := mtc.dbs[0].GetProto(context.Background(), keys.RangeDescriptorKey(startKey), &desc); err != nil {
			t.Fatal(err)
		}

		rep2, err := mtc.findMemberStoreLocked(desc).GetReplica(desc.RangeID)
		if err != nil {
			t.Fatal(err)
		}

		chgs := roachpb.MakeReplicationChanges(roachpb.ADD_REPLICA, roachpb.ReplicationTarget{
			NodeID:  mtc.stores[2].Ident.NodeID,
			StoreID: mtc.stores[2].Ident.StoreID,
		})
		_, err = rep2.ChangeReplicas(context.Background(), &desc, kvserver.SnapshotRequest_REBALANCE, kvserverpb.ReasonRangeUnderReplicated, "", chgs)
		return err
	}

	if err := replicateRHS(); !testutils.IsError(err, kvserver.IntersectingSnapshotMsg) {
		t.Fatalf("unexpected error %v", err)
	}

	// Enable the replica GC queue so that the next attempt to replicate the RHS
	// to store 2 will cause the obsolete replica to be GC'd allowing a
	// subsequent replication to succeed.
	mtc.stores[2].SetReplicaGCQueueActive(true)
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
	//
	// We don't test with only reasonNewLeader because that mechanism is less
	// robust than refreshing due to snapshot or ticks. In particular, it is
	// possible for node 3 to propose the RequestLease command and have that
	// command executed by the other nodes but to never see the execution locally
	// because it is caught up by applying a snapshot.
	testCases := map[string]kvserver.StoreTestingKnobs{
		"reasonSnapshotApplied": {
			DisableRefreshReasonNewLeader: true,
			DisableRefreshReasonTicks:     true,
		},
		"reasonTicks": {
			DisableRefreshReasonNewLeader:       true,
			DisableRefreshReasonSnapshotApplied: true,
		},
	}
	for name, c := range testCases {
		t.Run(name, func(t *testing.T) {
			sc := kvserver.TestStoreConfig(nil)
			sc.TestingKnobs = c
			// Disable periodic gossip tasks which can move the range 1 lease
			// unexpectedly.
			sc.TestingKnobs.DisablePeriodicGossips = true
			sc.Clock = nil // manual clock
			mtc := &multiTestContext{
				storeConfig: &sc,
				// This test was written before the multiTestContext started creating
				// many system ranges at startup, and hasn't been update to take that
				// into account.
				startWithSingleRange: true,
			}
			defer mtc.Stop()
			mtc.Start(t, 3)

			const rangeID = roachpb.RangeID(1)
			mtc.replicateRange(rangeID, 1, 2)

			// Put some data in the range so we'll have something to test for.
			incArgs := incrementArgs([]byte("a"), 5)
			if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), incArgs); err != nil {
				t.Fatal(err)
			}

			// Wait for all nodes to catch up.
			mtc.waitForValues(roachpb.Key("a"), []int64{5, 5, 5})

			// Stop node 2; while it is down write some more data.
			mtc.stopStore(2)

			if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), incArgs); err != nil {
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
			if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), truncArgs); err != nil {
				t.Fatal(err)
			}

			// Stop and restart node 0 in order to make sure that any in-flight Raft
			// messages have been sent.
			mtc.stopStore(0)
			mtc.restartStore(0)

			////////////////////////////////////////////////////////////////////
			// We want store 2 to take the lease later, so we'll drain the other
			// stores and expire the lease.
			////////////////////////////////////////////////////////////////////

			// Disable node liveness heartbeats which can reacquire leases when we're
			// trying to expire them. We pause liveness heartbeats here after node 0
			// was restarted (which creates a new NodeLiveness).
			pauseNodeLivenessHeartbeats(mtc, true)

			// Start draining stores 0 and 1 to prevent them from grabbing any new
			// leases.
			mtc.advanceClock(context.Background())
			var wg sync.WaitGroup
			for i := 0; i < 2; i++ {
				wg.Add(1)
				go func(i int) {
					mtc.stores[i].SetDraining(true, nil /* reporter */)
					wg.Done()
				}(i)
			}

			// Wait for the stores 0 and 1 to have entered draining mode, and then
			// advance the clock. Advancing the clock will leave the liveness records
			// of draining nodes in an expired state, so the SetDraining() call above
			// will be able to terminate.
			draining := false
			for !draining {
				draining = true
				for i := 0; i < 2; i++ {
					draining = draining && mtc.stores[i].IsDraining()
				}
				// Allow this loop to be preempted. Failure to do so can cause a
				// deadlock because a non-preemptible loop will prevent GC from
				// starting which in turn will cause all other goroutines to be stuck
				// as soon as they are called on to assist the GC (this shows up as
				// goroutines stuck in "GC assist wait"). With all of the other
				// goroutines stuck, nothing will be able to set mtc.stores[i].draining
				// to true.
				//
				// See #18554.
				runtime.Gosched()
			}
			mtc.advanceClock(context.Background())

			wg.Wait()

			// Restart node 2 and wait for the snapshot to be applied. Note that
			// waitForValues reads directly from the engine and thus isn't executing
			// a Raft command.
			mtc.restartStore(2)
			mtc.waitForValues(roachpb.Key("a"), []int64{10, 10, 10})

			// Send an increment to the restarted node. If we don't refresh pending
			// commands appropriately, the range lease command will not get
			// re-proposed when we discover the new leader.
			if _, err := kv.SendWrapped(context.Background(), mtc.stores[2].TestSender(), incArgs); err != nil {
				t.Fatal(err)
			}

			mtc.waitForValues(roachpb.Key("a"), []int64{15, 15, 15})
		})
	}
}

// Test that when a Raft group is not able to establish a quorum, its Raft log
// does not grow without bound. It tests two different scenarios where this used
// to be possible (see #27772):
// 1. The leader proposes a command and cannot establish a quorum. The leader
//    continually re-proposes the command.
// 2. The follower proposes a command and forwards it to the leader, who cannot
//    establish a quorum. The follower continually re-proposes and forwards the
//    command to the leader.
func TestLogGrowthWhenRefreshingPendingCommands(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sc := kvserver.TestStoreConfig(nil)
	// Drop the raft tick interval so the Raft group is ticked more.
	sc.RaftTickInterval = 10 * time.Millisecond
	// Don't timeout raft leader. We don't want leadership moving.
	sc.RaftElectionTimeoutTicks = 1000000
	// Reduce the max uncommitted entry size.
	sc.RaftMaxUncommittedEntriesSize = 64 << 10 // 64 KB
	// RaftProposalQuota cannot exceed RaftMaxUncommittedEntriesSize.
	sc.RaftProposalQuota = int64(sc.RaftMaxUncommittedEntriesSize)
	// RaftMaxInflightMsgs * RaftMaxSizePerMsg cannot exceed RaftProposalQuota.
	sc.RaftMaxInflightMsgs = 16
	sc.RaftMaxSizePerMsg = 1 << 10 // 1 KB
	// Disable leader transfers during leaseholder changes so that we
	// can easily create leader-not-leaseholder scenarios.
	sc.TestingKnobs.DisableLeaderFollowsLeaseholder = true
	// Refresh pending commands on every Raft group tick instead of
	// every RaftElectionTimeoutTicks.
	sc.TestingKnobs.RefreshReasonTicksPeriod = 1
	// Disable periodic gossip tasks which can move the range 1 lease
	// unexpectedly.
	sc.TestingKnobs.DisablePeriodicGossips = true
	mtc := &multiTestContext{
		storeConfig: &sc,
		// This test was written before the multiTestContext started creating many
		// system ranges at startup, and hasn't been update to take that into
		// account.
		startWithSingleRange: true,
	}
	defer mtc.Stop()
	mtc.Start(t, 5)

	const rangeID = roachpb.RangeID(1)
	mtc.replicateRange(rangeID, 1, 2, 3, 4)

	// Raft leadership is kept on node 0.
	leaderRepl, err := mtc.Store(0).GetReplica(rangeID)
	if err != nil {
		t.Fatal(err)
	}

	// Put some data in the range so we'll have something to test for.
	incArgs := incrementArgs([]byte("a"), 5)
	if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), incArgs); err != nil {
		t.Fatal(err)
	}

	// Wait for all nodes to catch up.
	mtc.waitForValues(roachpb.Key("a"), []int64{5, 5, 5, 5, 5})

	// Test proposing on leader and proposing on follower. Neither should result
	// in unbounded raft log growth.
	testutils.RunTrueAndFalse(t, "proposeOnFollower", func(t *testing.T, proposeOnFollower bool) {
		// Restart any nodes that are down.
		for _, s := range []int{2, 3, 4} {
			if mtc.Store(s) == nil {
				mtc.restartStore(s)
			}
		}

		// Determine which node to propose on. Transfer lease to that node.
		var propIdx, otherIdx int
		if !proposeOnFollower {
			propIdx, otherIdx = 0, 1
		} else {
			propIdx, otherIdx = 1, 0
		}
		propNode := mtc.stores[propIdx].TestSender()
		mtc.transferLease(context.Background(), rangeID, otherIdx, propIdx)
		testutils.SucceedsSoon(t, func() error {
			// Lease transfers may not be immediately observed by the new
			// leaseholder. Wait until the new leaseholder is aware.
			repl, err := mtc.Store(propIdx).GetReplica(rangeID)
			if err != nil {
				t.Fatal(err)
			}
			repDesc, err := repl.GetReplicaDescriptor()
			if err != nil {
				t.Fatal(err)
			}
			if lease, _ := repl.GetLease(); !lease.Replica.Equal(repDesc) {
				return errors.Errorf("lease not transferred yet; found %v", lease)
			}
			return nil
		})

		// Stop enough nodes to prevent a quorum.
		for _, s := range []int{2, 3, 4} {
			mtc.stopStore(s)
		}

		// Determine the current raft log size.
		initLogSize, _ := leaderRepl.GetRaftLogSize()

		// While a majority nodes are down, write some data.
		putRes := make(chan *roachpb.Error)
		go func() {
			putArgs := putArgs([]byte("b"), make([]byte, sc.RaftMaxUncommittedEntriesSize/8))
			_, err := kv.SendWrapped(context.Background(), propNode, putArgs)
			putRes <- err
		}()

		// Wait for a bit and watch for Raft log growth.
		wait := time.After(500 * time.Millisecond)
		ticker := time.Tick(50 * time.Millisecond)
	Loop:
		for {
			select {
			case <-wait:
				break Loop
			case <-ticker:
				// Verify that the leader is node 0.
				status := leaderRepl.RaftStatus()
				if status == nil || status.RaftState != raft.StateLeader {
					t.Fatalf("raft leader should be node 0, but got status %+v", status)
				}

				// Check the raft log size. We allow GetRaftLogSize to grow up
				// to twice RaftMaxUncommittedEntriesSize because its total
				// includes a little more state (the roachpb.Value checksum,
				// etc.). The important thing here is that the log doesn't grow
				// forever.
				logSizeLimit := int64(2 * sc.RaftMaxUncommittedEntriesSize)
				curlogSize, _ := leaderRepl.GetRaftLogSize()
				logSize := curlogSize - initLogSize
				logSizeStr := humanizeutil.IBytes(logSize)
				// Note that logSize could be negative if something got truncated.
				if logSize > logSizeLimit {
					t.Fatalf("raft log size grew to %s", logSizeStr)
				}
				t.Logf("raft log size grew to %s", logSizeStr)
			case err := <-putRes:
				t.Fatalf("write finished with quorum unavailable; err=%v", err)
			}
		}

		// Start enough nodes to establish a quorum.
		mtc.restartStore(2)

		// The write should now succeed.
		if err := <-putRes; err != nil {
			t.Fatal(err)
		}
	})
}

// TestStoreRangeUpReplicate verifies that the replication queue will notice
// under-replicated ranges and replicate them.
func TestStoreRangeUpReplicate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer kvserver.SetMockAddSSTable()()
	sc := kvserver.TestStoreConfig(nil)
	// Prevent the split queue from creating additional ranges while we're
	// waiting for replication.
	sc.TestingKnobs.DisableSplitQueue = true
	mtc := &multiTestContext{
		storeConfig: &sc,
	}
	defer mtc.Stop()
	mtc.Start(t, 3)
	mtc.initGossipNetwork()

	// Once we know our peers, trigger a scan.
	if err := mtc.stores[0].ForceReplicationScanAndProcess(); err != nil {
		t.Fatal(err)
	}

	// Wait until all ranges are upreplicated to all nodes.
	var replicaCount int64
	testutils.SucceedsSoon(t, func() error {
		var replicaCounts [3]int64
		for i, s := range mtc.stores {
			var err error
			mtc.stores[i].VisitReplicas(func(r *kvserver.Replica) bool {
				replicaCounts[i]++
				// Synchronize with the replica's raft processing goroutine.
				r.RaftLock()
				defer r.RaftUnlock()
				if len(r.Desc().InternalReplicas) != 3 {
					// This fails even after the snapshot has arrived and only
					// goes through once the replica has applied the conf change.
					err = errors.Errorf("not fully initialized")
					return false
				}
				return true
			})
			if err != nil {
				return err
			}
			if replicaCounts[i] != replicaCounts[0] {
				return errors.Errorf("not fully upreplicated")
			}
			if n := s.ReservationCount(); n != 0 {
				return errors.Errorf("expected 0 reservations, but found %d", n)
			}
		}
		replicaCount = replicaCounts[0]
		return nil
	})

	var generated int64
	var learnerApplied, raftApplied int64
	for _, s := range mtc.stores {
		m := s.Metrics()
		generated += m.RangeSnapshotsGenerated.Count()
		learnerApplied += m.RangeSnapshotsLearnerApplied.Count()
		raftApplied += m.RangeSnapshotsNormalApplied.Count()
	}
	if generated == 0 {
		t.Fatalf("expected at least 1 snapshot, but found 0")
	}
	// We upreplicate each range (once each for n2 and n3), so there should be
	// exactly 2 * replica learner snaps, one per upreplication.
	require.Equal(t, 2*replicaCount, learnerApplied)
	// Ideally there would be zero raft snaps, but etcd/raft is picky about
	// getting a snapshot at exactly the index it asked for.
	if raftApplied > learnerApplied {
		t.Fatalf("expected more learner snaps %d than raft snaps %d", learnerApplied, raftApplied)
	}
}

// TestUnreplicateFirstRange verifies that multiTestContext still functions in
// the case where the first range (which contains range metadata) is
// unreplicated from the first store. This situation can arise occasionally in
// tests, as can a similar situation where the first store is no longer the lease holder of
// the first range; this verifies that those tests will not be affected.
func TestUnreplicateFirstRange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mtc := &multiTestContext{}
	defer mtc.Stop()
	mtc.Start(t, 3)

	const rangeID = roachpb.RangeID(1)
	// Replicate the range to store 1.
	mtc.replicateRange(rangeID, 1)
	// Move the lease away from store 0 before removing its replica.
	mtc.transferLease(context.Background(), rangeID, 0, 1)
	// Unreplicate the from from store 0.
	mtc.unreplicateRange(rangeID, 0)
	// Replicate the range to store 2. The first range is no longer available on
	// store 1, and this command will fail if that situation is not properly
	// supported.
	mtc.replicateRange(rangeID, 2)
}

// TestChangeReplicasDescriptorInvariant tests that a replica change aborts if
// another change has been made to the RangeDescriptor since it was initiated.
func TestChangeReplicasDescriptorInvariant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := &multiTestContext{
		// This test was written before the multiTestContext started creating many
		// system ranges at startup, and hasn't been update to take that into
		// account.
		startWithSingleRange: true,
	}
	defer mtc.Stop()
	mtc.Start(t, 3)

	repl, err := mtc.stores[0].GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	addReplica := func(storeNum int, desc *roachpb.RangeDescriptor) error {
		chgs := roachpb.MakeReplicationChanges(roachpb.ADD_REPLICA, roachpb.ReplicationTarget{
			NodeID:  mtc.stores[storeNum].Ident.NodeID,
			StoreID: mtc.stores[storeNum].Ident.StoreID,
		})
		_, err := repl.ChangeReplicas(context.Background(), desc, kvserver.SnapshotRequest_REBALANCE, kvserverpb.ReasonRangeUnderReplicated, "", chgs)
		return err
	}

	// Retain the descriptor for the range at this point.
	origDesc := repl.Desc()

	// Add replica to the second store, which should succeed.
	if err := addReplica(1, origDesc); err != nil {
		t.Fatal(err)
	}
	testutils.SucceedsSoon(t, func() error {
		r := mtc.stores[1].LookupReplica(roachpb.RKey("a"))
		if r == nil {
			return errors.Errorf(`expected replica for key "a"`)
		}
		return nil
	})

	before := mtc.stores[2].Metrics().RangeSnapshotsLearnerApplied.Count()
	// Attempt to add replica to the third store with the original descriptor.
	// This should fail because the descriptor is stale.
	expectedErr := `change replicas of r1 failed: descriptor changed: \[expected\]`
	if err := addReplica(2, origDesc); !testutils.IsError(err, expectedErr) {
		t.Fatalf("got unexpected error: %+v", err)
	}

	after := mtc.stores[2].Metrics().RangeSnapshotsLearnerApplied.Count()
	// The failed ChangeReplicas call should NOT have applied a learner snapshot.
	if after != before {
		t.Fatalf(
			"ChangeReplicas call should not have applied a learner snapshot, before %d after %d",
			before, after)
	}

	before = mtc.stores[2].Metrics().RangeSnapshotsLearnerApplied.Count()
	// Add to third store with fresh descriptor.
	if err := addReplica(2, repl.Desc()); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		after := mtc.stores[2].Metrics().RangeSnapshotsLearnerApplied.Count()
		// The failed ChangeReplicas call should have applied a learner snapshot.
		if after != before+1 {
			return errors.Errorf(
				"ChangeReplicas call should have applied a learner snapshot, before %d after %d",
				before, after)
		}
		r := mtc.stores[2].LookupReplica(roachpb.RKey("a"))
		if r == nil {
			return errors.Errorf(`expected replica for key "a"`)
		}
		return nil
	})
}

// TestProgressWithDownNode verifies that a surviving quorum can make progress
// with a downed node.
func TestProgressWithDownNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// This test relies on concurrently waiting for a value to change in the
	// underlying engine(s). Since the teeing engine does not respond well to
	// value mismatches, whether transient or permanent, skip this test if the
	// teeing engine is being used. See
	// https://github.com/cockroachdb/cockroach/issues/42656 for more context.
	if storage.DefaultStorageEngine == enginepb.EngineTypeTeePebbleRocksDB {
		t.Skip("disabled on teeing engine")
	}
	mtc := &multiTestContext{
		// This test was written before the multiTestContext started creating many
		// system ranges at startup, and hasn't been update to take that into
		// account.
		startWithSingleRange: true,
	}
	defer mtc.Stop()
	mtc.Start(t, 3)

	const rangeID = roachpb.RangeID(1)
	mtc.replicateRange(rangeID, 1, 2)

	incArgs := incrementArgs([]byte("a"), 5)
	if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), incArgs); err != nil {
		t.Fatal(err)
	}

	// Verify that the first increment propagates to all the engines.
	verify := func(expected []int64) {
		testutils.SucceedsSoon(t, func() error {
			values := []int64{}
			for _, eng := range mtc.engines {
				val, _, err := storage.MVCCGet(context.Background(), eng, roachpb.Key("a"), mtc.clock().Now(),
					storage.MVCCGetOptions{})
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
	if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), incArgs); err != nil {
		t.Fatal(err)
	}

	// The new increment can be seen on both live replicas.
	verify([]int64{16, 5, 16})

	// Once the downed node is restarted, it will catch up.
	mtc.restartStore(1)
	verify([]int64{16, 16, 16})
}

// TestReplicateRestartAfterTruncationWithRemoveAndReAdd is motivated by issue
// #8111, which suggests the following test (which verifies the ability of a
// snapshot with a new replica ID to overwrite existing data):
//   - replicate a range to three stores
//   - stop a store
//   - remove the stopped store from the range
//   - truncate the logs
//   - re-add the store and restart it
//   - ensure that store can catch up with the rest of the group
func TestReplicateRestartAfterTruncationWithRemoveAndReAdd(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runReplicateRestartAfterTruncation(t, true /* removeBeforeTruncateAndReAdd */)
}

// TestReplicateRestartAfterTruncation is a variant of
// TestReplicateRestartAfterTruncationWithRemoveAndReAdd without the remove and
// re-add. Just stop, truncate, and restart. This verifies that a snapshot
// without a new replica ID works correctly.
func TestReplicateRestartAfterTruncation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runReplicateRestartAfterTruncation(t, false /* removeBeforeTruncateAndReAdd */)
}

func runReplicateRestartAfterTruncation(t *testing.T, removeBeforeTruncateAndReAdd bool) {
	sc := kvserver.TestStoreConfig(nil)
	// Don't timeout raft leaders or range leases (see the relation between
	// RaftElectionTimeoutTicks and RangeLeaseActiveDuration). This test expects
	// mtc.stores[0] to hold the range lease for range 1.
	sc.RaftElectionTimeoutTicks = 1000000
	sc.Clock = nil // manual clock
	mtc := &multiTestContext{
		storeConfig: &sc,
		// This test was written before the multiTestContext started creating many
		// system ranges at startup, and hasn't been update to take that into
		// account.
		startWithSingleRange: true,
	}
	defer mtc.Stop()
	mtc.Start(t, 3)

	key := roachpb.Key("a")

	// Replicate the initial range to all three nodes.
	const rangeID = roachpb.RangeID(1)
	mtc.replicateRange(rangeID, 1, 2)

	// Verify that the first increment propagates to all the engines.
	incArgs := incrementArgs(key, 2)
	if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), incArgs); err != nil {
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
		if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), truncArgs); err != nil {
			t.Fatal(err)
		}
	}

	// Ensure that store can catch up with the rest of the group.
	incArgs = incrementArgs(key, 3)
	if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), incArgs); err != nil {
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
		mtc.manualClock.Increment(int64(kvserver.ReplicaGCQueueInactivityThreshold + 1))
		testutils.SucceedsSoon(t, func() error {
			mtc.stores[1].MustForceReplicaGCScanAndProcess()
			_, err := mtc.stores[1].GetReplica(rangeID)
			if !errors.HasType(err, (*roachpb.RangeNotFoundError)(nil)) {
				return errors.Errorf("expected replica to be garbage collected, got %v %T", err, err)
			}
			return nil
		})

		mtc.replicateRange(rangeID, 1)
	}

	mtc.waitForValues(key, []int64{5, 5, 5})
}

func testReplicaAddRemove(t *testing.T, addFirst bool) {
	// This test relies on concurrently waiting for a value to change in the
	// underlying engine(s). Since the teeing engine does not respond well to
	// value mismatches, whether transient or permanent, skip this test if the
	// teeing engine is being used. See
	// https://github.com/cockroachdb/cockroach/issues/42656 for more context.
	if storage.DefaultStorageEngine == enginepb.EngineTypeTeePebbleRocksDB {
		t.Skip("disabled on teeing engine")
	}
	sc := kvserver.TestStoreConfig(nil)
	// We're gonna want to validate the state of the store before and after the
	// replica GC queue does its work, so we disable the replica gc queue here
	// and run it manually when we're ready.
	sc.TestingKnobs.DisableReplicaGCQueue = true
	sc.TestingKnobs.DisableEagerReplicaRemoval = true
	sc.Clock = nil // manual clock
	mtc := &multiTestContext{
		storeConfig: &sc,
		// This test was written before the multiTestContext started creating many
		// system ranges at startup, and hasn't been update to take that into
		// account.
		startWithSingleRange: true,
	}
	defer mtc.Stop()
	mtc.Start(t, 4)

	key := roachpb.Key("a")
	verifyFn := func(expected []int64) func() error {
		return func() error {
			values := make([]int64, len(mtc.engines))
			for i, eng := range mtc.engines {
				val, _, err := storage.MVCCGet(context.Background(), eng, key, mtc.clock().Now(),
					storage.MVCCGetOptions{})
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
		if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), incArgs); err != nil {
			t.Fatal(err)
		}
	}

	// The first increment is visible on all three replicas.
	testutils.SucceedsSoon(t, verifyFn([]int64{
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
	testutils.SucceedsSoon(t, verifyFn([]int64{
		inc1,
		inc1,
		inc1,
		inc1,
	}))

	// Ensure that the rest of the group can make progress.
	inc2 := int64(11)
	{
		incArgs := incrementArgs(key, inc2)
		if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), incArgs); err != nil {
			t.Fatal(err)
		}
	}
	testutils.SucceedsSoon(t, verifyFn([]int64{
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
		if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), incArgs); err != nil {
			t.Fatal(err)
		}
	}
	testutils.SucceedsSoon(t, verifyFn([]int64{
		inc1 + inc2 + inc3,
		inc1,
		inc1 + inc2 + inc3,
		inc1 + inc2 + inc3,
	}))

	// Wait out the range lease and the unleased duration to make the replica GC'able.
	mtc.advanceClock(context.Background())
	mtc.manualClock.Increment(int64(kvserver.ReplicaGCQueueInactivityThreshold + 1))
	mtc.stores[1].SetReplicaGCQueueActive(true)
	mtc.stores[1].MustForceReplicaGCScanAndProcess()

	// The removed store no longer has any of the data from the range.
	testutils.SucceedsSoon(t, verifyFn([]int64{
		inc1 + inc2 + inc3,
		0,
		inc1 + inc2 + inc3,
		inc1 + inc2 + inc3,
	}))

	desc := mtc.stores[0].LookupReplica(roachpb.RKeyMin).Desc()
	replicaIDsByStore := map[roachpb.StoreID]roachpb.ReplicaID{}
	for _, rep := range desc.InternalReplicas {
		replicaIDsByStore[rep.StoreID] = rep.ReplicaID
	}
	expected := map[roachpb.StoreID]roachpb.ReplicaID{1: 1, 4: 2, 3: 4}
	if !reflect.DeepEqual(expected, replicaIDsByStore) {
		t.Fatalf("expected replica IDs to be %v but got %v", expected, replicaIDsByStore)
	}
}

func TestReplicateAddAndRemove(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testReplicaAddRemove(t, true /* addFirst */)
}

func TestReplicateRemoveAndAdd(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testReplicaAddRemove(t, false /* addFirst */)
}

// TestQuotaPool verifies that writes get throttled in the case where we have
// two fast moving replicas with sufficiently fast growing raft logs and a
// slower replica catching up. By throttling write throughput we avoid having
// to constantly catch up the slower node via snapshots. See #8659.
func TestQuotaPool(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const quota = 10000
	const numReplicas = 3
	const rangeID = 1
	ctx := context.Background()
	sc := kvserver.TestStoreConfig(nil)
	// Suppress timeout-based elections to avoid leadership changes in ways
	// this test doesn't expect.
	sc.RaftElectionTimeoutTicks = 100000
	mtc := &multiTestContext{
		storeConfig: &sc,
		// This test was written before the multiTestContext started creating many
		// system ranges at startup, and hasn't been update to take that into
		// account.
		startWithSingleRange: true,
	}
	mtc.Start(t, numReplicas)
	defer mtc.Stop()

	mtc.replicateRange(rangeID, 1, 2)

	assertEqualLastIndex := func() error {
		var expectedIndex uint64

		for i, s := range mtc.stores {
			repl, err := s.GetReplica(rangeID)
			if err != nil {
				t.Fatal(err)
			}

			index, err := repl.GetLastIndex()
			if err != nil {
				t.Fatal(err)
			}
			if i == 0 {
				expectedIndex = index
			} else if expectedIndex != index {
				return fmt.Errorf("%s: expected lastIndex %d, but found %d", repl, expectedIndex, index)
			}
		}
		return nil
	}
	testutils.SucceedsSoon(t, assertEqualLastIndex)

	// NB: See TestRaftBlockedReplica/#9914 for why we use a separate	goroutine.
	raftLockReplica := func(repl *kvserver.Replica) {
		ch := make(chan struct{})
		go func() { repl.RaftLock(); close(ch) }()
		<-ch
	}

	leaderRepl := mtc.getRaftLeader(rangeID)
	// Grab the raftMu to re-initialize the QuotaPool to ensure that we don't
	// race with ongoing applications.
	raftLockReplica(leaderRepl)
	if err := leaderRepl.InitQuotaPool(quota); err != nil {
		t.Fatalf("failed to initialize quota pool: %v", err)
	}
	leaderRepl.RaftUnlock()
	followerRepl := func() *kvserver.Replica {
		for _, store := range mtc.stores {
			repl, err := store.GetReplica(rangeID)
			if err != nil {
				t.Fatal(err)
			}
			if repl == leaderRepl {
				continue
			}
			return repl
		}
		return nil
	}()
	if followerRepl == nil {
		t.Fatal("could not get a handle on a follower replica")
	}

	// We block the third replica effectively causing acquisition of quota
	// without subsequent release.
	raftLockReplica(followerRepl)
	ch := make(chan *roachpb.Error, 1)

	func() {
		defer followerRepl.RaftUnlock()

		// In order to verify write throttling we insert a value 3/4th the size of
		// total quota available in the system. This should effectively go through
		// and block the subsequent insert of the same size. We check to see whether
		// or not after this write has gone through by verifying that the total
		// quota available has decreased as expected.
		//
		// Following this we unblock the 'slow' replica allowing it to catch up to
		// the first write. This in turn releases quota back to the pool and the
		// second write, previously blocked by virtue of there not being enough
		// quota, is now free to proceed. We expect the final quota in the system
		// to be the same as what we started with.
		key := roachpb.Key("k")
		value := bytes.Repeat([]byte("v"), (3*quota)/4)
		var ba roachpb.BatchRequest
		ba.Add(putArgs(key, value))
		if err := ba.SetActiveTimestamp(mtc.clock().Now); err != nil {
			t.Fatal(err)
		}
		if _, pErr := leaderRepl.Send(ctx, ba); pErr != nil {
			t.Fatal(pErr)
		}

		if curQuota := leaderRepl.QuotaAvailable(); curQuota > quota/4 {
			t.Fatalf("didn't observe the expected quota acquisition, available: %d", curQuota)
		}

		testutils.SucceedsSoon(t, func() error {
			if qLen := leaderRepl.QuotaReleaseQueueLen(); qLen < 1 {
				return errors.Errorf("expected at least 1 queued quota release, found: %d", qLen)
			}
			return nil
		})

		go func() {
			var ba roachpb.BatchRequest
			ba.Add(putArgs(key, value))
			if err := ba.SetActiveTimestamp(mtc.clock().Now); err != nil {
				ch <- roachpb.NewError(err)
				return
			}
			_, pErr := leaderRepl.Send(ctx, ba)
			ch <- pErr
		}()
	}()

	testutils.SucceedsSoon(t, func() error {
		if curQuota := leaderRepl.QuotaAvailable(); curQuota != quota {
			return errors.Errorf("expected available quota %d, got %d", quota, curQuota)
		}
		if qLen := leaderRepl.QuotaReleaseQueueLen(); qLen != 0 {
			return errors.Errorf("expected no queued quota releases, found: %d", qLen)
		}
		return nil
	})

	if pErr := <-ch; pErr != nil {
		t.Fatal(pErr)
	}
}

// TestWedgedReplicaDetection verifies that a leader replica is able to
// correctly detect a wedged follower replica and no longer consider it
// as active for the purpose of proposal throttling.
func TestWedgedReplicaDetection(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numReplicas = 3
	const rangeID = 1

	sc := kvserver.TestStoreConfig(nil)
	// Suppress timeout-based elections to avoid leadership changes in ways
	// this test doesn't expect.
	sc.RaftElectionTimeoutTicks = 100000
	mtc := &multiTestContext{
		storeConfig: &sc,
		// This test was written before the multiTestContext started creating many
		// system ranges at startup, and hasn't been update to take that into
		// account.
		startWithSingleRange: true,
	}
	mtc.Start(t, numReplicas)
	defer mtc.Stop()
	mtc.replicateRange(rangeID, 1, 2)

	leaderRepl := mtc.getRaftLeader(rangeID)
	followerRepl := func() *kvserver.Replica {
		for _, store := range mtc.stores {
			repl, err := store.GetReplica(rangeID)
			if err != nil {
				t.Fatal(err)
			}
			if repl == leaderRepl {
				continue
			}
			return repl
		}
		return nil
	}()
	if followerRepl == nil {
		t.Fatal("could not get a handle on a follower replica")
	}

	// Lock the follower replica to prevent it from making progress from now
	// on. NB: See TestRaftBlockedReplica/#9914 for why we use a separate
	// goroutine.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		followerRepl.RaftLock()
		wg.Done()
	}()
	wg.Wait()
	defer followerRepl.RaftUnlock()

	// TODO(andrei): The test becomes flaky with a lower threshold because the
	// follower is considered inactive just below. Figure out how to switch the
	// test to a manual clock. The activity tracking for followers uses the
	// physical clock.
	inactivityThreshold := time.Second

	// Send a request to the leader replica. followerRepl is locked so it will
	// not respond.
	ctx := context.Background()
	key := roachpb.Key("k")
	value := []byte("value")
	var ba roachpb.BatchRequest
	ba.Add(putArgs(key, value))
	if err := ba.SetActiveTimestamp(mtc.clock().Now); err != nil {
		t.Fatal(err)
	}
	if _, pErr := leaderRepl.Send(ctx, ba); pErr != nil {
		t.Fatal(pErr)
	}

	// The follower should still be active.
	followerID := followerRepl.ReplicaID()
	if !leaderRepl.IsFollowerActiveSince(ctx, followerID, inactivityThreshold) {
		t.Fatalf("expected follower to still be considered active")
	}

	// It is possible that there are in-flight heartbeat responses from
	// followerRepl from before it was locked. The receipt of one of these
	// would bump the last active timestamp on the leader. Because of this,
	// we check whether the follower is eventually considered inactive.
	testutils.SucceedsSoon(t, func() error {
		// Send another request to the leader replica. followerRepl is locked
		// so it will not respond.
		if _, pErr := leaderRepl.Send(ctx, ba); pErr != nil {
			t.Fatal(pErr)
		}

		// The follower should no longer be considered active.
		if leaderRepl.IsFollowerActiveSince(ctx, followerID, inactivityThreshold) {
			return errors.New("expected follower to be considered inactive")
		}
		return nil
	})
}

// TestRaftHeartbeats verifies that coalesced heartbeats are correctly
// suppressing elections in an idle cluster.
func TestRaftHeartbeats(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mtc := &multiTestContext{}
	defer mtc.Stop()
	mtc.Start(t, 3)

	const rangeID = roachpb.RangeID(1)
	mtc.replicateRange(rangeID, 1, 2)

	// Capture the initial term and state.
	leaderIdx := -1
	for i, store := range mtc.stores {
		if store.RaftStatus(rangeID).SoftState.RaftState == raft.StateLeader {
			leaderIdx = i
			break
		}
	}
	initialTerm := mtc.stores[leaderIdx].RaftStatus(rangeID).Term

	// Wait for several ticks to elapse.
	ticksToWait := 2 * mtc.makeStoreConfig(leaderIdx).RaftElectionTimeoutTicks
	ticks := mtc.stores[leaderIdx].Metrics().RaftTicks.Count
	for targetTicks := ticks() + int64(ticksToWait); ticks() < targetTicks; {
		time.Sleep(time.Millisecond)
	}

	status := mtc.stores[leaderIdx].RaftStatus(rangeID)
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

	mtc := &multiTestContext{
		// This test was written before the multiTestContext started creating many
		// system ranges at startup, and hasn't been update to take that into
		// account.
		startWithSingleRange: true,
	}
	defer mtc.Stop()
	mtc.Start(t, 3)

	const rangeID = roachpb.RangeID(1)
	mtc.replicateRange(rangeID, 1, 2)

	leaderIdx := -1
	// Loop until a leader is elected.
	for {
		for i, store := range mtc.stores {
			if store.RaftStatus(rangeID).SoftState.RaftState == raft.StateLeader {
				leaderIdx = i
				break
			}
		}
		if leaderIdx == -1 {
			runtime.Gosched()
		} else {
			break
		}
	}
	initialTerm := mtc.stores[leaderIdx].RaftStatus(rangeID).Term
	// Choose a follower index that is guaranteed to not be the leader.
	followerIdx := (leaderIdx + 1) % len(mtc.stores)

	// Shut down a raft transport via the circuit breaker, and wait for two
	// election timeouts to trigger an election if reportUnreachable broke
	// heartbeat transmission to the other store.
	cb := mtc.transport.GetCircuitBreaker(mtc.stores[followerIdx].Ident.NodeID,
		rpc.DefaultClass)
	cb.Break()

	// Send a command to ensure Raft is aware of lost follower so that it won't
	// quiesce (which would prevent heartbeats).
	if _, err := kv.SendWrappedWith(
		context.Background(), mtc.stores[0].TestSender(), roachpb.Header{RangeID: rangeID},
		incrementArgs(roachpb.Key("a"), 1)); err != nil {
		t.Fatal(err)
	}

	ticksToWait := 2 * mtc.makeStoreConfig(leaderIdx).RaftElectionTimeoutTicks
	ticks := mtc.stores[leaderIdx].Metrics().RaftTicks.Count
	for targetTicks := ticks() + int64(ticksToWait); ticks() < targetTicks; {
		time.Sleep(time.Millisecond)
	}

	// Ensure that the leadership has not changed, to confirm that heartbeats
	// are sent to the store with a functioning transport.
	status := mtc.stores[leaderIdx].RaftStatus(rangeID)
	if status.SoftState.RaftState != raft.StateLeader {
		t.Errorf("expected node %d to be leader after sleeping but was %s", leaderIdx, status.SoftState.RaftState)
	}
	if status.Term != initialTerm {
		t.Errorf("while sleeping, term changed from %d to %d", initialTerm, status.Term)
	}
}

// TestReportUnreachableRemoveRace adds and removes the raft leader replica
// repeatedly while one of its peers is unreachable in an attempt to expose
// races (primarily in asynchronous coalesced heartbeats).
func TestReportUnreachableRemoveRace(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mtc := &multiTestContext{}
	defer mtc.Stop()
	mtc.Start(t, 3)

	const rangeID = roachpb.RangeID(1)
	mtc.replicateRange(rangeID, 1, 2)

outer:
	for i := 0; i < 5; i++ {
		for leaderIdx, store := range mtc.stores {
			repl, err := store.GetReplica(rangeID)
			if err != nil {
				t.Fatal(err)
			}
			if repl.RaftStatus().SoftState.RaftState == raft.StateLeader {
				for replicaIdx, toStore := range mtc.stores {
					if toStore == store {
						continue
					}
					repDesc, err := repl.GetReplicaDescriptor()
					if err != nil {
						t.Fatal(err)
					}
					if lease, _ := repl.GetLease(); lease.Replica.Equal(repDesc) {
						mtc.transferLease(context.Background(), rangeID, leaderIdx, replicaIdx)
					}
					mtc.unreplicateRange(rangeID, leaderIdx)
					cb := mtc.transport.GetCircuitBreaker(toStore.Ident.NodeID, rpc.DefaultClass)
					cb.Break()
					time.Sleep(mtc.storeConfig.CoalescedHeartbeatsInterval)
					cb.Reset()
					mtc.replicateRange(rangeID, leaderIdx)
					continue outer
				}
				t.Fatal("could not find raft replica")
			}
		}
		i-- // try again
	}
}

// TestReplicateAfterSplit verifies that a new replica whose start key
// is not KeyMin replicating to a fresh store can apply snapshots correctly.
func TestReplicateAfterSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	storeCfg := kvserver.TestStoreConfig(nil /* clock */)
	storeCfg.TestingKnobs.DisableMergeQueue = true
	mtc := &multiTestContext{
		storeConfig: &storeCfg,
	}
	defer mtc.Stop()
	mtc.Start(t, 2)

	const rangeID = roachpb.RangeID(1)
	splitKey := roachpb.Key("m")
	key := roachpb.Key("z")

	store0 := mtc.stores[0]
	// Make the split
	splitArgs := adminSplitArgs(splitKey)
	if _, err := kv.SendWrapped(context.Background(), store0.TestSender(), splitArgs); err != nil {
		t.Fatal(err)
	}

	rangeID2 := store0.LookupReplica(roachpb.RKey(key)).RangeID
	if rangeID2 == rangeID {
		t.Fatal("got same range id after split")
	}
	// Issue an increment for later check.
	incArgs := incrementArgs(key, 11)
	if _, err := kv.SendWrappedWith(context.Background(), store0.TestSender(), roachpb.Header{
		RangeID: rangeID2,
	}, incArgs); err != nil {
		t.Fatal(err)
	}
	// Now add the second replica.
	mtc.replicateRange(rangeID2, 1)

	if mtc.stores[1].LookupReplica(roachpb.RKey(key)).GetMaxBytes() == 0 {
		t.Error("Range MaxBytes is not set after snapshot applied")
	}
	// Once it catches up, the effects of increment commands can be seen.
	testutils.SucceedsSoon(t, func() error {
		getArgs := getArgs(key)
		// Reading on non-lease holder replica should use inconsistent read
		if reply, err := kv.SendWrappedWith(context.Background(), mtc.stores[1].TestSender(), roachpb.Header{
			RangeID:         rangeID2,
			ReadConsistency: roachpb.INCONSISTENT,
		}, getArgs); err != nil {
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
			storeCfg := kvserver.TestStoreConfig(nil /* clock */)
			storeCfg.TestingKnobs.DisableMergeQueue = true
			mtc := &multiTestContext{
				storeConfig: &storeCfg,
			}
			defer mtc.Stop()
			mtc.Start(t, 2)

			// Replicate range to enable raft campaigning.
			mtc.replicateRange(rangeID, 1)
			store0 := mtc.stores[0]

			// Make the split.
			splitArgs := adminSplitArgs(splitKey)
			if _, err := kv.SendWrapped(context.Background(), store0.TestSender(), splitArgs); err != nil {
				t.Fatal(err)
			}

			replica2 := store0.LookupReplica(roachpb.RKey(key2))

			rg2 := func(s *kvserver.Store) kv.Sender {
				return kv.Wrap(s, func(ba roachpb.BatchRequest) roachpb.BatchRequest {
					if ba.RangeID == 0 {
						ba.RangeID = replica2.RangeID
					}
					return ba
				})
			}

			// Raft processing is initialized lazily; issue a no-op write request to
			// ensure that the Raft group has been started.
			incArgs := incrementArgs(key2, 0)
			if _, err := kv.SendWrapped(context.Background(), rg2(store0), incArgs); err != nil {
				t.Fatal(err)
			}

			if td.remove {
				// Simulate second replica being transferred by removing it.
				if err := store0.RemoveReplica(context.Background(), replica2, replica2.Desc().NextReplicaID, kvserver.RemoveOptions{
					DestroyData: true,
				}); err != nil {
					t.Fatal(err)
				}
			}

			var latestTerm uint64
			if td.expectAdvance {
				testutils.SucceedsSoon(t, func() error {
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
	storeCfg := kvserver.TestStoreConfig(nil /* clock */)
	storeCfg.TestingKnobs.DisableMergeQueue = true
	storeCfg.Clock = nil // manual clock
	mtc := &multiTestContext{
		storeConfig: &storeCfg,
	}
	defer mtc.Stop()
	mtc.Start(t, 3)

	// Make the split.
	splitArgs := adminSplitArgs(roachpb.Key("b"))
	if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), splitArgs); err != nil {
		t.Fatal(err)
	}

	const rangeID = roachpb.RangeID(2)
	mtc.replicateRange(rangeID, 1, 2)

	mtc.unreplicateRange(rangeID, 2)
	mtc.unreplicateRange(rangeID, 1)

	// Wait for the removal to be processed.
	testutils.SucceedsSoon(t, func() error {
		for _, s := range mtc.stores[1:] {
			_, err := s.GetReplica(rangeID)
			if !errors.HasType(err, (*roachpb.RangeNotFoundError)(nil)) {
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
	mtc.transport.SendAsync(&kvserver.RaftMessageRequest{
		ToReplica:   replica1,
		FromReplica: replica2,
		Heartbeats: []kvserver.RaftHeartbeat{
			{
				RangeID:       rangeID,
				FromReplicaID: replica2.ReplicaID,
				ToReplicaID:   replica1.ReplicaID,
			},
		},
	}, rpc.DefaultClass)
	// Execute another replica change to ensure that raft has processed
	// the heartbeat just sent.
	mtc.replicateRange(roachpb.RangeID(1), 1)

	// Expire leases to ensure any remaining intent resolutions can complete.
	// TODO(bdarnell): understand why some tests need this.
	mtc.advanceClock(context.Background())
}

// TestRaftRemoveRace adds and removes a replica repeatedly in an attempt to
// reproduce a race (see #1911 and #9037).
func TestRaftRemoveRace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := &multiTestContext{}
	defer mtc.Stop()
	const rangeID = roachpb.RangeID(1)

	if !util.RaceEnabled {
		mtc.Start(t, 10)
		// Up-replicate to a bunch of nodes which stresses a condition where a
		// replica created via a preemptive snapshot receives a message for a
		// previous incarnation of the replica (i.e. has a smaller replica ID) that
		// existed on the same store.
		mtc.replicateRange(rangeID, 1, 2, 3, 4, 5, 6, 7, 8, 9)
	} else {
		// In race builds, running 10 nodes needs more than 1 full CPU
		// (due to background gossip and heartbeat overhead), so it can't
		// keep up when run under stress with one process per CPU. Run a
		// reduced version of this test in race builds. This isn't as
		// likely to reproduce the preemptive-snapshot race described in
		// the previous comment, but will still have a chance to do so, or
		// to find other races.
		mtc.Start(t, 3)
		mtc.replicateRange(rangeID, 1, 2)
	}

	for i := 0; i < 10; i++ {
		mtc.unreplicateRange(rangeID, 2)
		mtc.replicateRange(rangeID, 2)

		// Verify the tombstone key does not exist. See #12130.
		tombstoneKey := keys.RangeTombstoneKey(rangeID)
		var tombstone roachpb.RangeTombstone
		if ok, err := storage.MVCCGetProto(
			context.Background(), mtc.stores[2].Engine(), tombstoneKey,
			hlc.Timestamp{}, &tombstone, storage.MVCCGetOptions{},
		); err != nil {
			t.Fatal(err)
		} else if ok {
			t.Fatal("tombstone should not exist")
		}
	}
}

// TestRemovePlaceholderRace adds and removes a replica repeatedly (similar to
// TestRaftRemoveRace) in an attempt to stress the locking around replica
// placeholders.
func TestRemovePlaceholderRace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := &multiTestContext{}
	defer mtc.Stop()
	mtc.Start(t, 3)

	const rangeID = roachpb.RangeID(1)
	mtc.replicateRange(rangeID, 1, 2)

	repl, err := mtc.stores[0].GetReplica(rangeID)
	if err != nil {
		t.Fatal(err)
	}
	ctx := repl.AnnotateCtx(context.Background())

	for i := 0; i < 100; i++ {
		for _, action := range []roachpb.ReplicaChangeType{roachpb.REMOVE_REPLICA, roachpb.ADD_REPLICA} {
			for {
				chgs := roachpb.MakeReplicationChanges(action, roachpb.ReplicationTarget{
					NodeID:  mtc.stores[1].Ident.NodeID,
					StoreID: mtc.stores[1].Ident.StoreID,
				})
				if _, err := repl.ChangeReplicas(ctx, repl.Desc(), kvserver.SnapshotRequest_REBALANCE, kvserverpb.ReasonUnknown, "", chgs); err != nil {
					if kvserver.IsSnapshotError(err) {
						continue
					} else {
						t.Fatal(err)
					}
				}
				break
			}
		}
	}
}

type noConfChangeTestHandler struct {
	rangeID roachpb.RangeID
	kvserver.RaftMessageHandler
}

func (ncc *noConfChangeTestHandler) HandleRaftRequest(
	ctx context.Context,
	req *kvserver.RaftMessageRequest,
	respStream kvserver.RaftMessageResponseStream,
) *roachpb.Error {
	for i, e := range req.Message.Entries {
		if e.Type == raftpb.EntryConfChange {
			var cc raftpb.ConfChange
			if err := protoutil.Unmarshal(e.Data, &cc); err != nil {
				panic(err)
			}
			var ccCtx kvserver.ConfChangeContext
			if err := protoutil.Unmarshal(cc.Context, &ccCtx); err != nil {
				panic(err)
			}
			var command kvserverpb.RaftCommand
			if err := protoutil.Unmarshal(ccCtx.Payload, &command); err != nil {
				panic(err)
			}
			if req.RangeID == ncc.rangeID {
				if command.ReplicatedEvalResult.ChangeReplicas != nil {
					// We found a configuration change headed for our victim range;
					// sink it.
					req.Message.Entries = req.Message.Entries[:i]
				}
			}
		}
	}
	return ncc.RaftMessageHandler.HandleRaftRequest(ctx, req, respStream)
}

func (ncc *noConfChangeTestHandler) HandleRaftResponse(
	ctx context.Context, resp *kvserver.RaftMessageResponse,
) error {
	switch val := resp.Union.GetValue().(type) {
	case *roachpb.Error:
		switch val.GetDetail().(type) {
		case *roachpb.ReplicaTooOldError:
			// We're going to manually GC the replica, so ignore these errors.
			return nil
		}
	}
	return ncc.RaftMessageHandler.HandleRaftResponse(ctx, resp)
}

func TestReplicaGCRace(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mtc := &multiTestContext{}
	defer mtc.Stop()
	mtc.Start(t, 3)

	const rangeID = roachpb.RangeID(1)
	mtc.replicateRange(rangeID, 1)

	leaderStore := mtc.stores[0]
	fromStore := mtc.stores[1]
	toStore := mtc.stores[2]

	// Prevent the victim replica from processing configuration changes.
	mtc.transport.Stop(toStore.Ident.StoreID)
	mtc.transport.Listen(toStore.Ident.StoreID, &noConfChangeTestHandler{
		rangeID:            rangeID,
		RaftMessageHandler: toStore,
	})

	repl, err := leaderStore.GetReplica(rangeID)
	if err != nil {
		t.Fatal(err)
	}
	ctx := repl.AnnotateCtx(context.Background())

	// Add the victim replica. Note that it will receive a snapshot and raft log
	// replays, but will not process the configuration change containing the new
	// range descriptor, preventing it from learning of the new NextReplicaID.
	chgs := roachpb.MakeReplicationChanges(roachpb.ADD_REPLICA, roachpb.ReplicationTarget{
		NodeID:  toStore.Ident.NodeID,
		StoreID: toStore.Ident.StoreID,
	})
	if _, err := repl.ChangeReplicas(ctx, repl.Desc(), kvserver.SnapshotRequest_REBALANCE, kvserverpb.ReasonRangeUnderReplicated, "", chgs); err != nil {
		t.Fatal(err)
	}

	// Craft a heartbeat addressed to the victim replica. Note that this
	// heartbeat will be sent after the replica has been GC'ed.
	rangeDesc := repl.Desc()
	fromReplicaDesc, ok := rangeDesc.GetReplicaDescriptor(fromStore.Ident.StoreID)
	if !ok {
		t.Fatalf("expected %s to have a replica on %s", rangeDesc, fromStore)
	}
	toReplicaDesc, ok := rangeDesc.GetReplicaDescriptor(toStore.Ident.StoreID)
	if !ok {
		t.Fatalf("expected %s to have a replica on %s", rangeDesc, toStore)
	}

	hbReq := kvserver.RaftMessageRequest{
		FromReplica: fromReplicaDesc,
		ToReplica:   toReplicaDesc,
		Heartbeats: []kvserver.RaftHeartbeat{
			{
				RangeID:       rangeID,
				FromReplicaID: fromReplicaDesc.ReplicaID,
				ToReplicaID:   toReplicaDesc.ReplicaID,
			},
		},
	}

	// Wait for the victim's raft log to be non-empty, then configure the heartbeat
	// with the raft state.
	testutils.SucceedsSoon(t, func() error {
		status := repl.RaftStatus()
		progressByID := status.Progress
		progress, ok := progressByID[uint64(toReplicaDesc.ReplicaID)]
		if !ok {
			return errors.Errorf("%+v does not yet contain %s", progressByID, toReplicaDesc)
		}
		if progress.Match == 0 {
			return errors.Errorf("%+v has not yet advanced", progress)
		}
		for i := range hbReq.Heartbeats {
			hbReq.Heartbeats[i].Term = status.Term
			hbReq.Heartbeats[i].Commit = progress.Match
		}
		return nil
	})

	// Remove the victim replica and manually GC it.
	chgs[0].ChangeType = roachpb.REMOVE_REPLICA
	if _, err := repl.ChangeReplicas(ctx, repl.Desc(), kvserver.SnapshotRequest_REBALANCE, kvserverpb.ReasonRangeOverReplicated, "", chgs); err != nil {
		t.Fatal(err)
	}

	{
		removedReplica, err := toStore.GetReplica(rangeID)
		if err != nil {
			t.Fatal(err)
		}
		if err := toStore.ManualReplicaGC(removedReplica); err != nil {
			t.Fatal(err)
		}
	}

	// Create a new transport for store 0. Error responses are passed
	// back along the same grpc stream as the request so it's ok that
	// there are two (this one and the one actually used by the store).
	fromTransport := kvserver.NewRaftTransport(log.AmbientContext{Tracer: mtc.storeConfig.Settings.Tracer},
		cluster.MakeTestingClusterSettings(),
		nodedialer.New(mtc.rpcContext, gossip.AddressResolver(fromStore.Gossip())),
		nil, /* grpcServer */
		mtc.transportStopper,
	)
	errChan := errorChannelTestHandler(make(chan *roachpb.Error, 1))
	fromTransport.Listen(fromStore.StoreID(), errChan)

	// Send the heartbeat. Boom. See #11591.
	// We have to send this multiple times to protect against
	// dropped messages (see #18355).
	sendHeartbeat := func() (sent bool) {
		r := hbReq
		return fromTransport.SendAsync(&r, rpc.DefaultClass)
	}
	if sent := sendHeartbeat(); !sent {
		t.Fatal("failed to send heartbeat")
	}
	heartbeatsSent := 1

	// The receiver of this message should return an error. If we don't get a
	// quick response, assume that the message got dropped and try sending it
	// again.
	select {
	case pErr := <-errChan:
		switch pErr.GetDetail().(type) {
		case *roachpb.RaftGroupDeletedError:
		default:
			t.Fatalf("unexpected error type %T: %s", pErr.GetDetail(), pErr)
		}
	case <-time.After(time.Second):
		if heartbeatsSent >= 5 {
			t.Fatal("did not get expected error")
		}
		heartbeatsSent++
		if sent := sendHeartbeat(); !sent {
			t.Fatal("failed to send heartbeat")
		}
	}
}

func requireOnlyAtomicChanges(
	t *testing.T, db *sqlutils.SQLRunner, rangeID roachpb.RangeID, repFactor int, start time.Time,
) {
	// From all events pertaining to the given rangeID and post-dating the start time,
	// filter out those infos which indicate a (full and incoming) voter count in
	// excess of the replication factor. Any rows returned have the full info JSON
	// strings in them.
	const q = `
SELECT
	"uniqueID",
	count(t) AS repfactor,
	string_agg(info, e'\\n') AS infos
FROM
	[
		SELECT
			"uniqueID",
			replicas->'node_id' AS n,
			COALESCE(replicas->'type', '0') AS t,
			info
		FROM
			system.rangelog,
			ROWS FROM (
				jsonb_array_elements(
					info::JSONB->'UpdatedDesc'->'internal_replicas'
				)
			)
				AS replicas
		WHERE
			info::JSONB->'UpdatedDesc'->'range_id' = $1::JSONB AND timestamp >= $2
		ORDER BY
			"timestamp" ASC
	]
WHERE
	t IN ('0', '2')
GROUP BY
	"uniqueID"
HAVING
	count(t) > $3;
`
	matrix := db.QueryStr(t, q, rangeID, start, repFactor)
	if len(matrix) > 0 {
		t.Fatalf("more than %d voting replicas: %s", repFactor, sqlutils.MatrixToStr(matrix))
	}
}

func TestDecommission(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if util.RaceEnabled {
		// Five nodes is too much to reliably run under testrace with our aggressive
		// liveness timings.
		t.Skip("skipping under testrace: #39807 and #37811")
	}

	// This test relies on concurrently waiting for a value to change in the
	// underlying engine(s). Since the teeing engine does not respond well to
	// value mismatches, whether transient or permanent, skip this test if the
	// teeing engine is being used. See
	// https://github.com/cockroachdb/cockroach/issues/42656 for more context.
	if storage.DefaultStorageEngine == enginepb.EngineTypeTeePebbleRocksDB {
		t.Skip("disabled on teeing engine")
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 5, base.TestClusterArgs{
		ReplicationMode: base.ReplicationAuto,
	})
	defer tc.Stopper().Stop(ctx)

	k := tc.ScratchRange(t)
	cc, err := tc.Server(0).RPCContext().GRPCDialNode(tc.Server(0).RPCAddr(), 1, rpc.DefaultClass).Connect(ctx)
	require.NoError(t, err)
	admin := serverpb.NewAdminClient(cc)
	// Decommission the first node, which holds most of the leases.
	_, err = admin.Decommission(
		ctx, &serverpb.DecommissionRequest{
			NodeIDs:         []roachpb.NodeID{1},
			Decommissioning: true,
		},
	)
	require.NoError(t, err)

	requireNoReplicas := func(storeID roachpb.StoreID, repFactor int) {
		testutils.SucceedsSoon(t, func() error {
			desc := tc.LookupRangeOrFatal(t, k)
			for _, rDesc := range desc.Replicas().Voters() {
				store, err := tc.Servers[int(rDesc.NodeID-1)].Stores().GetStore(rDesc.StoreID)
				require.NoError(t, err)
				if err := store.ForceReplicationScanAndProcess(); err != nil {
					return err
				}
			}
			if sl := desc.Replicas().Filter(func(rDesc roachpb.ReplicaDescriptor) bool {
				return rDesc.StoreID == storeID
			}); len(sl) > 0 {
				return errors.Errorf("still a replica on s%d: %s", storeID, &desc)
			}
			if len(desc.Replicas().Voters()) != repFactor {
				return errors.Errorf("expected %d replicas: %s", repFactor, &desc)
			}
			return nil
		})
	}

	const triplicated = 3

	requireNoReplicas(1, triplicated)

	runner := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	ts := timeutil.Now()

	_, err = admin.Decommission(
		ctx, &serverpb.DecommissionRequest{
			NodeIDs:         []roachpb.NodeID{2},
			Decommissioning: true,
		},
	)
	require.NoError(t, err)

	// Both s1 and s2 are out, so neither ought to have replicas.
	requireNoReplicas(1, triplicated)
	requireNoReplicas(2, triplicated)

	// Going from three replicas to three replicas should have used atomic swaps
	// only. We didn't verify this before the first decommissioning op because
	// lots of ranges were over-replicated due to ranges recently having split
	// off from the five-fold replicated system ranges.
	requireOnlyAtomicChanges(t, runner, tc.LookupRangeOrFatal(t, k).RangeID, triplicated, ts)

	sqlutils.SetZoneConfig(t, runner, "RANGE default", "num_replicas: 1")

	const single = 1

	// The range should drop down to one replica on a non-decommissioning store.
	requireNoReplicas(1, single)
	requireNoReplicas(2, single)

	// Decommission two more nodes. Only n5 is left; getting the replicas there
	// can't use atomic replica swaps because the leaseholder can't be removed.
	_, err = admin.Decommission(
		ctx, &serverpb.DecommissionRequest{
			NodeIDs:         []roachpb.NodeID{3, 4},
			Decommissioning: true,
		},
	)
	require.NoError(t, err)

	requireNoReplicas(1, single)
	requireNoReplicas(2, single)
	requireNoReplicas(3, single)
	requireNoReplicas(4, single)
}

// TestReplicateRogueRemovedNode ensures that a rogue removed node
// (i.e. a node that has been removed from the range but doesn't know
// it yet because it was down or partitioned away when it happened)
// cannot cause other removed nodes to recreate their ranges.
func TestReplicateRogueRemovedNode(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sc := kvserver.TestStoreConfig(nil)
	// Newly-started stores (including the "rogue" one) should not GC
	// their replicas. We'll turn this back on when needed.
	sc.TestingKnobs.DisableReplicaGCQueue = true
	sc.Clock = nil // manual clock
	mtc := &multiTestContext{
		storeConfig: &sc,
		// This test was written before the multiTestContext started creating many
		// system ranges at startup, and hasn't been update to take that into
		// account.
		startWithSingleRange: true,
	}
	defer mtc.Stop()
	mtc.Start(t, 3)

	// We're going to set up the cluster with partitioning so that we can
	// partition node 0 from the others. The partition is not initially active.
	partRange, err := setupPartitionedRange(mtc, 1, 0, 0, false /* activated */, unreliableRaftHandlerFuncs{})
	require.NoError(t, err)
	// First put the range on all three nodes.
	raftID := roachpb.RangeID(1)
	mtc.replicateRange(raftID, 1, 2)

	// Put some data in the range so we'll have something to test for.
	incArgs := incrementArgs([]byte("a"), 5)
	if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), incArgs); err != nil {
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
	if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), incArgs); err != nil {
		t.Fatal(err)
	}

	// Wait for the replica to be GC'd on node 1.
	// Store 0 has two writes, 1 has erased everything, and 2 still has the first write.
	// A single pass of ForceReplicaGCScanAndProcess is not enough, since the replica
	// may be recreated by a stray raft message, so we run the GC scan inside the loop.
	// TODO(bdarnell): if the call to RemoveReplica in replicaGCQueue.process can be
	// moved under the lock, then the GC scan can be moved out of this loop.
	mtc.stores[1].SetReplicaGCQueueActive(true)
	testutils.SucceedsSoon(t, func() error {
		mtc.advanceClock(context.Background())
		mtc.manualClock.Increment(int64(
			kvserver.ReplicaGCQueueInactivityThreshold) + 1)
		mtc.stores[1].MustForceReplicaGCScanAndProcess()

		actual := mtc.readIntFromEngines(roachpb.Key("a"))
		expected := []int64{16, 0, 5}
		if !reflect.DeepEqual(expected, actual) {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})
	// Partition nodes 1 and 2 from node 0. Otherwise they'd get a
	// ReplicaTooOldError from node 0 and proceed to remove themselves.
	partRange.activate()
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
		_, pErr := kv.SendWrappedWith(
			context.Background(),
			mtc.stores[2],
			roachpb.Header{
				Replica:   replicaDesc,
				Timestamp: mtc.stores[2].Clock().Now(),
			}, incArgs,
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
	testutils.SucceedsSoon(t, func() error {
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
	mtc.stores[2].SetReplicaGCQueueActive(true)
	mtc.advanceClock(context.Background())
	mtc.manualClock.Increment(int64(
		kvserver.ReplicaGCQueueInactivityThreshold) + 1)
	mtc.stores[2].MustForceReplicaGCScanAndProcess()
	mtc.waitForValues(roachpb.Key("a"), []int64{16, 0, 0})

	// Now that the group has been GC'd, the goroutine that was
	// attempting to write has finished (with an error).
	finishWG.Wait()
}

type errorChannelTestHandler chan *roachpb.Error

func (errorChannelTestHandler) HandleRaftRequest(
	_ context.Context, _ *kvserver.RaftMessageRequest, _ kvserver.RaftMessageResponseStream,
) *roachpb.Error {
	panic("unimplemented")
}

func (d errorChannelTestHandler) HandleRaftResponse(
	ctx context.Context, resp *kvserver.RaftMessageResponse,
) error {
	switch val := resp.Union.GetValue().(type) {
	case *roachpb.Error:
		d <- val
	default:
		log.Fatalf(ctx, "unexpected response type %T", val)
	}
	return nil
}

func (errorChannelTestHandler) HandleSnapshot(
	_ *kvserver.SnapshotRequest_Header, _ kvserver.SnapshotResponseStream,
) error {
	panic("unimplemented")
}

// This test simulates a scenario where one replica has been removed from the
// range's Raft group but it is unaware of the fact. We check that this replica
// coming back from the dead cannot cause elections.
func TestReplicateRemovedNodeDisruptiveElection(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mtc := &multiTestContext{
		// This test was written before the multiTestContext started creating many
		// system ranges at startup, and hasn't been update to take that into
		// account.
		startWithSingleRange: true,
	}
	defer mtc.Stop()
	mtc.Start(t, 4)

	// Move the first range from the first node to the other three.
	const rangeID = roachpb.RangeID(1)
	mtc.replicateRange(rangeID, 1, 2, 3)
	mtc.transferLease(context.Background(), rangeID, 0, 1)
	mtc.unreplicateRange(rangeID, 0)

	// Ensure that we have a stable lease and raft leader so we can tell if the
	// removed node causes a disruption. This is a three-step process.

	// 1. Write on the second node, to ensure that a lease has been
	// established after the first node's removal.
	key := roachpb.Key("a")
	value := int64(5)
	incArgs := incrementArgs(key, value)
	if _, err := kv.SendWrapped(context.Background(), mtc.distSenders[1], incArgs); err != nil {
		t.Fatal(err)
	}

	// 2. Wait for all nodes to process the increment (and therefore the
	// new lease).
	mtc.waitForValues(key, []int64{0, value, value, value})

	// 3. Wait for the lease holder to obtain raft leadership too.
	testutils.SucceedsSoon(t, func() error {
		req := &roachpb.LeaseInfoRequest{
			RequestHeader: roachpb.RequestHeader{
				Key: roachpb.KeyMin,
			},
		}
		reply, pErr := kv.SendWrapped(context.Background(), mtc.distSenders[1], req)
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

	// Create a new transport for store 0 so that we can intercept the responses.
	// Error responses are passed back along the same grpc stream as the request
	// so it's ok that there are two (this one and the one actually used by the
	// store).
	transport0 := kvserver.NewRaftTransport(log.AmbientContext{Tracer: mtc.storeConfig.Settings.Tracer},
		cluster.MakeTestingClusterSettings(),
		nodedialer.New(mtc.rpcContext, gossip.AddressResolver(mtc.gossips[0])),
		nil, /* grpcServer */
		mtc.transportStopper,
	)
	errChan := errorChannelTestHandler(make(chan *roachpb.Error, 1))
	transport0.Listen(mtc.stores[0].StoreID(), errChan)

	// Simulate the removed node asking to trigger an election. Try and try again
	// until we're reasonably sure the message was sent.
	for !transport0.SendAsync(&kvserver.RaftMessageRequest{
		RangeID:     rangeID,
		ToReplica:   replica1,
		FromReplica: replica0,
		Message: raftpb.Message{
			From: uint64(replica0.ReplicaID),
			To:   uint64(replica1.ReplicaID),
			Type: raftpb.MsgVote,
			Term: term + 1,
		},
	}, rpc.DefaultClass) {
	}

	// The receiver of this message (i.e. replica1) should return an error telling
	// the sender that it's no longer part of the group.
	select {
	case pErr := <-errChan:
		switch pErr.GetDetail().(type) {
		case *roachpb.ReplicaTooOldError:
		default:
			t.Fatalf("unexpected error type %T: %s", pErr.GetDetail(), pErr)
		}
	case <-time.After(45 * time.Second):
		t.Fatal("did not get expected ReplicaTooOldError error")
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

	sc := kvserver.TestStoreConfig(nil)
	sc.TestingKnobs.DisableScanner = true
	mtc := &multiTestContext{
		storeConfig: &sc,
		// This test was written before the multiTestContext started creating many
		// system ranges at startup, and hasn't been update to take that into
		// account.
		startWithSingleRange: true,
	}
	defer mtc.Stop()
	mtc.Start(t, 4)

	// Replicate the first range onto all of the nodes.
	const rangeID = 1
	mtc.replicateRange(rangeID, 1, 2, 3)

	// Put some data in the range so we'll have something to test for.
	incArgs := incrementArgs([]byte("a"), 5)
	if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), incArgs); err != nil {
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
	if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), incArgs); err != nil {
		t.Fatal(err)
	}
	mtc.waitForValues(roachpb.Key("a"), []int64{16, 16, 16, 5})

	// Wait for a bunch of raft ticks in order to flush any heartbeats through
	// the system. In particular, a coalesced heartbeat containing a quiesce
	// message could have been sent before the node was removed from range but
	// arrive after the node restarted.
	ticks := mtc.stores[0].Metrics().RaftTicks.Count
	for targetTicks := ticks() + 5; ticks() < targetTicks; {
		time.Sleep(time.Millisecond)
	}

	// Restart node 3. The removed replica will start talking to the other
	// replicas and determine it needs to be GC'd.
	mtc.restartStore(3)

	// Because we lazily initialize Raft groups, we have to force the Raft group
	// to get created in order to get the replica talking to the other replicas.
	mtc.stores[3].EnqueueRaftUpdateCheck(rangeID)

	testutils.SucceedsSoon(t, func() error {
		replica, err := mtc.stores[3].GetReplica(rangeID)
		if err != nil {
			if errors.HasType(err, (*roachpb.RangeNotFoundError)(nil)) {
				return nil
			}
			return err
		} else if replica != nil {
			// Make sure the replica is unquiesced so that it will tick and
			// contact the leader to discover it's no longer part of the range.
			replica.UnquiesceAndWakeLeader()
		}
		return errors.Errorf("found %s, waiting for it to be GC'd", replica)
	})
}

func TestReplicaLazyLoad(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sc := kvserver.TestStoreConfig(nil)
	sc.RaftTickInterval = 10 * time.Millisecond // safe because there is only a single node
	sc.TestingKnobs.DisableScanner = true
	sc.TestingKnobs.DisablePeriodicGossips = true
	sc.TestingKnobs.DisableMergeQueue = true
	mtc := &multiTestContext{
		storeConfig: &sc,
		// This test was written before the multiTestContext started creating many
		// system ranges at startup, and hasn't been update to take that into
		// account.
		startWithSingleRange: true,
	}
	defer mtc.Stop()
	mtc.Start(t, 1)

	// Split so we can rely on RHS range being quiescent after a restart.
	// We use UserTableDataMin to avoid having the range activated to
	// gossip system table data.
	splitKey := keys.UserTableDataMin
	splitArgs := adminSplitArgs(splitKey)
	if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), splitArgs); err != nil {
		t.Fatal(err)
	}

	mtc.stopStore(0)
	mtc.restartStore(0)

	// Wait for a bunch of raft ticks.
	ticks := mtc.stores[0].Metrics().RaftTicks.Count
	for targetTicks := ticks() + 3; ticks() < targetTicks; {
		time.Sleep(time.Millisecond)
	}

	splitKeyAddr, err := keys.Addr(splitKey)
	if err != nil {
		t.Fatal(err)
	}

	replica := mtc.stores[0].LookupReplica(splitKeyAddr)
	if replica == nil {
		t.Fatalf("lookup replica at key %q returned nil", splitKey)
	}
	if replica.RaftStatus() != nil {
		t.Fatalf("expected replica Raft group to be uninitialized")
	}
}

func TestReplicateReAddAfterDown(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mtc := &multiTestContext{
		// This test was written before the multiTestContext started creating many
		// system ranges at startup, and hasn't been update to take that into
		// account.
		startWithSingleRange: true,
	}
	defer mtc.Stop()
	mtc.Start(t, 3)

	downedStoreIdx := 2

	// First put the range on all three nodes.
	raftID := roachpb.RangeID(1)
	mtc.replicateRange(raftID, 1, 2)

	// Put some data in the range so we'll have something to test for.
	incArgs := incrementArgs([]byte("a"), 5)
	if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), incArgs); err != nil {
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
	if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), incArgs); err != nil {
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

// TestLeaseHolderRemoveSelf verifies that a lease holder cannot remove itself
// without encountering an error.
func TestLeaseHolderRemoveSelf(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mtc := &multiTestContext{}
	defer mtc.Stop()
	mtc.Start(t, 2)

	leaseHolder := mtc.stores[0]

	raftID := roachpb.RangeID(1)
	mtc.replicateRange(raftID, 1)

	// Attempt to remove the replica from first store.
	expectedErr := "invalid ChangeReplicasTrigger"
	if err := mtc.unreplicateRangeNonFatal(raftID, 0); !testutils.IsError(err, expectedErr) {
		t.Fatalf("expected %q error trying to remove leaseholder replica; got %v", expectedErr, err)
	}

	// Expect that we can still successfully do a get on the range.
	getArgs := getArgs([]byte("a"))
	_, pErr := kv.SendWrappedWith(context.Background(), leaseHolder.TestSender(), roachpb.Header{}, getArgs)
	if pErr != nil {
		t.Fatal(pErr)
	}
}

// TestRemovedReplicaError verifies that a replica that has been removed from a
// range returns a RangeNotFoundError if it receives a request for that range
// (not RaftGroupDeletedError, and even before the ReplicaGCQueue has run).
func TestRemovedReplicaError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mtc := &multiTestContext{
		// This test was written before the multiTestContext started creating many
		// system ranges at startup, and hasn't been update to take that into
		// account.
		startWithSingleRange: true,
	}
	defer mtc.Stop()
	mtc.Start(t, 2)

	// Disable the replica GC queues. This verifies that the replica is
	// considered removed even before the gc queue has run, and also
	// helps avoid a deadlock at shutdown.
	mtc.stores[0].SetReplicaGCQueueActive(false)

	raftID := roachpb.RangeID(1)
	mtc.replicateRange(raftID, 1)
	mtc.transferLease(context.Background(), raftID, 0, 1)
	mtc.unreplicateRange(raftID, 0)

	mtc.manualClock.Increment(mtc.storeConfig.LeaseExpiration())

	// Expect to get a RangeNotFoundError. We have to allow for ambiguous result
	// errors to avoid the occasional test flake. Since we use demotions to remove
	// voters, the actual removal sees a learner, and so the learner is not in
	// the commit quorum for the removal itself. That is to say, we will only
	// start seeing the RangeNotFoundError after a little bit of time has passed.
	getArgs := getArgs([]byte("a"))
	testutils.SucceedsSoon(t, func() error {
		_, pErr := kv.SendWrappedWith(context.Background(), mtc.stores[0].TestSender(), roachpb.Header{}, getArgs)
		switch pErr.GetDetail().(type) {
		case *roachpb.AmbiguousResultError:
			return pErr.GoError()
		case *roachpb.NotLeaseHolderError:
			return pErr.GoError()
		case *roachpb.RangeNotFoundError:
			return nil
		default:
		}
		t.Fatal(pErr)
		return errors.New("unreachable")
	})
}

func TestTransferRaftLeadership(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numStores = 3
	sc := kvserver.TestStoreConfig(nil)
	sc.TestingKnobs.DisableMergeQueue = true
	// Suppress timeout-based elections (which also includes a previous
	// leader stepping down due to a quorum check). Running tests on a
	// heavily loaded CPU is enough to reach the raft election timeout
	// and cause leadership to change hands in ways this test doesn't
	// expect.
	sc.RaftElectionTimeoutTicks = 100000
	// This test can rapidly advance the clock via mtc.advanceClock(),
	// which could lead the replication queue to consider a store dead
	// and remove a replica in the middle of the test. Disable the
	// replication queue; we'll control replication manually.
	sc.TestingKnobs.DisableReplicateQueue = true
	sc.Clock = nil // manual clock
	mtc := &multiTestContext{
		storeConfig: &sc,
		// This test was written before the multiTestContext started creating many
		// system ranges at startup, and hasn't been update to take that into
		// account.
		startWithSingleRange: true,
	}
	defer mtc.Stop()
	mtc.Start(t, numStores)
	store0 := mtc.Store(0)
	store1 := mtc.Store(1)

	key := roachpb.Key("a")

	{
		// Split off a range to avoid interacting with the initial splits.
		splitArgs := adminSplitArgs(key)
		if _, err := kv.SendWrapped(context.Background(), mtc.distSenders[0], splitArgs); err != nil {
			t.Fatal(err)
		}
	}

	repl0 := store0.LookupReplica(keys.MustAddr(key))
	if repl0 == nil {
		t.Fatalf("no replica found for key '%s'", key)
	}
	rd0, err := repl0.GetReplicaDescriptor()
	if err != nil {
		t.Fatal(err)
	}
	mtc.replicateRange(repl0.RangeID, 1, 2)

	repl1 := store1.LookupReplica(keys.MustAddr(key))
	if repl1 == nil {
		t.Fatalf("no replica found for key '%s'", key)
	}
	rd1, err := repl1.GetReplicaDescriptor()
	if err != nil {
		t.Fatal(err)
	}

	getArgs := getArgs([]byte("a"))
	if _, pErr := kv.SendWrappedWith(
		context.Background(), store0, roachpb.Header{RangeID: repl0.RangeID}, getArgs,
	); pErr != nil {
		t.Fatalf("expect get nil, actual get %v ", pErr)
	}

	status := repl0.RaftStatus()
	if status == nil || status.Lead != uint64(rd0.ReplicaID) {
		t.Fatalf("raft leader should be %d, but got status %+v", rd0.ReplicaID, status)
	}

	// Force a read on Store 2 to request a new lease. Other moving parts in
	// the system could have requested another lease as well, so we
	// expire-request in a loop until we get our foot in the door.
	origCount0 := store0.Metrics().RangeRaftLeaderTransfers.Count()
	for {
		mtc.advanceClock(context.Background())
		if _, pErr := kv.SendWrappedWith(
			context.Background(), store1, roachpb.Header{RangeID: repl0.RangeID}, getArgs,
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
	// Verify lease is transferred.
	testutils.SucceedsSoon(t, func() error {
		if a, e := repl0.RaftStatus().Lead, uint64(rd1.ReplicaID); a != e {
			return errors.Errorf("expected raft leader be %d; got %d", e, a)
		}
		if a, e := store0.Metrics().RangeRaftLeaderTransfers.Count()-origCount0, int64(1); a < e {
			return errors.Errorf("expected raft leader transfer count >= %d; got %d", e, a)
		}
		return nil
	})
}

// Test that a single blocked replica does not block other replicas.
func TestRaftBlockedReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sc := kvserver.TestStoreConfig(nil)
	sc.TestingKnobs.DisableMergeQueue = true
	sc.TestingKnobs.DisableScanner = true
	mtc := &multiTestContext{
		storeConfig: &sc,
		// This test was written before the multiTestContext started creating many
		// system ranges at startup, and hasn't been update to take that into
		// account.
		startWithSingleRange: true,
	}
	defer mtc.Stop()
	mtc.Start(t, 3)

	// Create 2 ranges by splitting range 1.
	splitArgs := adminSplitArgs(roachpb.Key("b"))
	if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), splitArgs); err != nil {
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
	if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), incArgs); err != nil {
		t.Fatal(err)
	}
	mtc.waitForValues(roachpb.Key("a"), []int64{5, 5, 5})
}

// Test that ranges quiesce and if a follower unquiesces the leader is woken
// up.
func TestRangeQuiescence(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sc := kvserver.TestStoreConfig(nil)
	sc.TestingKnobs.DisableScanner = true
	sc.TestingKnobs.DisablePeriodicGossips = true
	mtc := &multiTestContext{
		storeConfig: &sc,
		// This test was written before the multiTestContext started creating many
		// system ranges at startup, and hasn't been update to take that into
		// account.
		startWithSingleRange: true,
	}
	defer mtc.Stop()
	mtc.Start(t, 3)

	pauseNodeLivenessHeartbeats(mtc, true)

	// Replica range 1 to all 3 nodes.
	const rangeID = roachpb.RangeID(1)
	mtc.replicateRange(rangeID, 1, 2)

	waitForQuiescence := func(rangeID roachpb.RangeID) {
		testutils.SucceedsSoon(t, func() error {
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
	waitForQuiescence(rangeID)

	// Find the leader replica.
	var rep *kvserver.Replica
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
	mtc.stores[followerIdx].EnqueueRaftUpdateCheck(rangeID)

	// Wait for a bunch of ticks to occur which will allow the follower time to
	// campaign.
	ticks := mtc.stores[followerIdx].Metrics().RaftTicks.Count
	for targetTicks := ticks() + int64(2*sc.RaftElectionTimeoutTicks); ticks() < targetTicks; {
		time.Sleep(time.Millisecond)
	}

	// Wait for the range to quiesce again.
	waitForQuiescence(rangeID)

	// The leadership should not have changed.
	if state := rep.RaftStatus().SoftState.RaftState; state != raft.StateLeader {
		t.Fatalf("%s should be the leader: %s", rep, state)
	}
}

// TestInitRaftGroupOnRequest verifies that an uninitialized Raft group
// is initialized if a request is received, even if the current range
// lease points to a different replica.
func TestInitRaftGroupOnRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	storeCfg := kvserver.TestStoreConfig(nil /* clock */)
	storeCfg.TestingKnobs.DisableMergeQueue = true
	// Don't timeout range leases (see the relation between
	// RaftElectionTimeoutTicks and RangeLeaseActiveDuration). This test expects
	// the replica that holds the lease before the cluster is restarted to
	// continue holding it after the restart, regardless of how long the restart
	// takes.
	storeCfg.RaftElectionTimeoutTicks = 1000000
	// Disable async intent resolution. This can lead to flakiness in the test
	// because it allows for the intents written by the split transaction to be
	// resolved at any time, including after the nodes are restarted. The intent
	// resolution on the RHS's local range descriptor can both wake up the RHS
	// range's Raft group and result in the wrong replica acquiring the lease.
	storeCfg.TestingKnobs.IntentResolverKnobs.DisableAsyncIntentResolution = true
	mtc := &multiTestContext{
		storeConfig: &storeCfg,
		// TODO(andrei): This test was written before multiTestContexts started with
		// multiple ranges, and for some unknown reason is flaky if we're not
		// forcing it to start with a single range, although it doesnt look like it
		// should be.
		startWithSingleRange: true,
	}
	defer mtc.Stop()
	mtc.Start(t, 2)

	// Split so we can rely on RHS range being quiescent after a restart.
	// We use UserTableDataMin to avoid having the range activated to
	// gossip system table data.
	splitKey := keys.UserTableDataMin
	splitArgs := adminSplitArgs(splitKey)
	if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), splitArgs); err != nil {
		t.Fatal(err)
	}

	repl := mtc.stores[0].LookupReplica(roachpb.RKey(splitKey))
	if repl == nil {
		t.Fatal("replica should not be nil for RHS range")
	}
	mtc.replicateRange(repl.RangeID, 1)

	// Find the leaseholder and then restart the test context.
	lease, _ := repl.GetLease()
	mtc.restart()

	// Get replica from the store which isn't the leaseholder.
	// NOTE: StoreID is 1-indexed and storeIdx is 0-indexed, so despite what
	// this might look like, this is grabbing the replica without the lease.
	storeIdx := int(lease.Replica.StoreID) % len(mtc.stores)
	if repl = mtc.stores[storeIdx].LookupReplica(roachpb.RKey(splitKey)); repl == nil {
		t.Fatal("replica should not be nil for RHS range")
	}

	// TODO(spencer): Raft messages seem to turn up
	// occasionally on restart, which initialize the replica, so
	// this is not a test failure. Not sure how to work around this
	// problem.
	// Verify the raft group isn't initialized yet.
	if repl.IsRaftGroupInitialized() {
		log.Errorf(context.Background(), "expected raft group to be uninitialized")
	}

	// Send an increment and verify that initializes the Raft group.
	incArgs := incrementArgs(splitKey, 1)
	_, pErr := kv.SendWrappedWith(
		context.Background(), mtc.stores[storeIdx], roachpb.Header{RangeID: repl.RangeID}, incArgs,
	)
	if _, ok := pErr.GetDetail().(*roachpb.NotLeaseHolderError); !ok {
		t.Fatalf("expected NotLeaseHolderError; got %s", pErr)
	}
	if !repl.IsRaftGroupInitialized() {
		t.Fatal("expected raft group to be initialized")
	}
}

// TestFailedConfChange verifies correct behavior after a configuration change
// experiences an error when applying EndTxn. Specifically, it verifies that
// https://github.com/cockroachdb/cockroach/issues/13506 has been fixed.
func TestFailedConfChange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Trigger errors at apply time so they happen on both leaders and
	// followers.
	var filterActive int32
	sc := kvserver.TestStoreConfig(nil)
	sc.TestingKnobs.TestingApplyFilter = func(filterArgs kvserverbase.ApplyFilterArgs) (int, *roachpb.Error) {
		if atomic.LoadInt32(&filterActive) == 1 && filterArgs.ChangeReplicas != nil {
			return 0, roachpb.NewErrorf("boom")
		}
		return 0, nil
	}
	mtc := &multiTestContext{
		storeConfig: &sc,
	}
	defer mtc.Stop()
	mtc.Start(t, 3)
	ctx := context.Background()

	// Replicate the range (successfully) to the second node.
	const rangeID = roachpb.RangeID(1)
	mtc.replicateRange(rangeID, 1)

	// Try and fail to replicate it to the third node.
	atomic.StoreInt32(&filterActive, 1)
	if err := mtc.replicateRangeNonFatal(rangeID, 2); !testutils.IsError(err, "boom") {
		t.Fatal(err)
	}

	// Raft state is only exposed on the leader, so we must transfer
	// leadership and check the stores one at a time.
	checkLeaderStore := func(i int) error {
		store := mtc.stores[i]
		repl, err := store.GetReplica(rangeID)
		if err != nil {
			t.Fatal(err)
		}
		if l := len(repl.Desc().InternalReplicas); l != 2 {
			return errors.Errorf("store %d: expected 2 replicas in descriptor, found %d in %s",
				i, l, repl.Desc())
		}
		status := repl.RaftStatus()
		if status.RaftState != raft.StateLeader {
			return errors.Errorf("store %d: expected StateLeader, was %s", i, status.RaftState)
		}
		// In issue #13506, the Progress map would be updated as if the
		// change had succeeded.
		if l := len(status.Progress); l != 2 {
			return errors.Errorf("store %d: expected 2 replicas in raft, found %d in %s", i, l, status)
		}
		return nil
	}

	if err := checkLeaderStore(0); err != nil {
		t.Fatal(err)
	}

	// Transfer leadership to the second node and wait for it to become leader.
	mtc.transferLease(ctx, rangeID, 0, 1)
	testutils.SucceedsSoon(t, func() error {
		repl, err := mtc.stores[1].GetReplica(rangeID)
		if err != nil {
			return err
		}
		status := repl.RaftStatus()
		if status.RaftState != raft.StateLeader {
			return errors.Errorf("store %d: expected StateLeader, was %s", 1, status.RaftState)
		}
		return nil
	})

	if err := checkLeaderStore(1); err != nil {
		t.Fatal(err)
	}
}

// TestStoreRangeRemovalCompactionSuggestion verifies that if a replica
// is removed from a store, a compaction suggestion is made to the
// compactor queue.
func TestStoreRangeRemovalCompactionSuggestion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := kvserver.TestStoreConfig(nil)
	mtc := &multiTestContext{storeConfig: &sc}

	ctx := context.Background()

	for i := 0; i < 3; i++ {
		// Use RocksDB engines because Pebble does not use the compactor queue.
		eng := storage.NewInMem(ctx, enginepb.EngineTypeRocksDB, roachpb.Attributes{}, 1<<20)
		defer eng.Close()
		mtc.engines = append(mtc.engines, eng)
	}

	defer mtc.Stop()
	mtc.Start(t, 3)

	const rangeID = roachpb.RangeID(1)
	mtc.replicateRange(rangeID, 1, 2)

	repl, err := mtc.stores[0].GetReplica(rangeID)
	if err != nil {
		t.Fatal(err)
	}
	ctx = repl.AnnotateCtx(ctx)

	deleteStore := mtc.stores[2]
	chgs := roachpb.MakeReplicationChanges(roachpb.REMOVE_REPLICA, roachpb.ReplicationTarget{
		NodeID:  deleteStore.Ident.NodeID,
		StoreID: deleteStore.Ident.StoreID,
	})
	if _, err := repl.ChangeReplicas(ctx, repl.Desc(), kvserver.SnapshotRequest_REBALANCE, kvserverpb.ReasonRebalance, "", chgs); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		// Function to check compaction metrics indicating a suggestion
		// was queued or a compaction was processed or skipped.
		haveCompaction := func(s *kvserver.Store, exp bool) error {
			queued := s.Compactor().Metrics.BytesQueued.Value()
			comps := s.Compactor().Metrics.BytesCompacted.Count()
			skipped := s.Compactor().Metrics.BytesSkipped.Count()
			if exp != (queued > 0 || comps > 0 || skipped > 0) {
				return errors.Errorf("%s: expected non-zero compaction metrics? %t; got queued=%d, compactions=%d, skipped=%d",
					s, exp, queued, comps, skipped)
			}
			return nil
		}
		// Verify that no compaction metrics are showing non-zero bytes in the
		// other stores.
		for _, s := range mtc.stores {
			if err := haveCompaction(s, s == deleteStore); err != nil {
				return err
			}
		}
		return nil
	})
}

func TestStoreRangeWaitForApplication(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var filterRangeIDAtomic int64

	ctx := context.Background()
	sc := kvserver.TestStoreConfig(nil)
	sc.TestingKnobs.DisableReplicateQueue = true
	sc.TestingKnobs.DisableReplicaGCQueue = true
	sc.TestingKnobs.TestingRequestFilter = func(_ context.Context, ba roachpb.BatchRequest) (retErr *roachpb.Error) {
		if rangeID := roachpb.RangeID(atomic.LoadInt64(&filterRangeIDAtomic)); rangeID != ba.RangeID {
			return nil
		}
		pErr := roachpb.NewErrorf("blocking %s in this test", ba.Summary())
		if len(ba.Requests) != 1 {
			return pErr
		}
		_, ok := ba.Requests[0].GetInner().(*roachpb.PutRequest)
		if !ok {
			return pErr
		}
		return nil
	}
	mtc := &multiTestContext{storeConfig: &sc}
	mtc.Start(t, 3)
	defer mtc.Stop()
	store0, store2 := mtc.Store(0), mtc.Store(2)
	distSender := mtc.distSenders[0]

	// Split off a non-system range so we don't have to account for node liveness
	// traffic.
	splitArgs := adminSplitArgs(roachpb.Key("a"))
	if _, pErr := kv.SendWrapped(ctx, distSender, splitArgs); pErr != nil {
		t.Fatal(pErr)
	}
	rangeID := store0.LookupReplica(roachpb.RKey("a")).RangeID
	mtc.replicateRange(rangeID, 1, 2)

	repl0, err := store0.GetReplica(rangeID)
	if err != nil {
		t.Fatal(err)
	}

	atomic.StoreInt64(&filterRangeIDAtomic, int64(rangeID))

	leaseIndex0 := repl0.LastAssignedLeaseIndex()

	type target struct {
		client kvserver.PerReplicaClient
		header kvserver.StoreRequestHeader
	}

	var targets []target
	for _, s := range mtc.stores {
		conn, err := mtc.nodeDialer.Dial(ctx, s.Ident.NodeID, rpc.DefaultClass)
		if err != nil {
			t.Fatal(err)
		}
		targets = append(targets, target{
			client: kvserver.NewPerReplicaClient(conn),
			header: kvserver.StoreRequestHeader{NodeID: s.Ident.NodeID, StoreID: s.Ident.StoreID},
		})
	}

	// Wait for a command that is already applied. The request should return
	// immediately.
	for i, target := range targets {
		_, err := target.client.WaitForApplication(ctx, &kvserver.WaitForApplicationRequest{
			StoreRequestHeader: target.header,
			RangeID:            rangeID,
			LeaseIndex:         leaseIndex0,
		})
		if err != nil {
			t.Fatalf("%d: %+v", i, err)
		}
	}

	const count = 5

	// Wait for a command that is `count` indexes later.
	var errChs []chan error
	for _, target := range targets {
		errCh := make(chan error)
		errChs = append(errChs, errCh)
		target := target
		go func() {
			_, err := target.client.WaitForApplication(ctx, &kvserver.WaitForApplicationRequest{
				StoreRequestHeader: target.header,
				RangeID:            rangeID,
				LeaseIndex:         leaseIndex0 + count,
			})
			errCh <- err
		}()
	}

	// The request should not return when less than `count` commands have
	// been issued.
	putArgs := putArgs(roachpb.Key("foo"), []byte("bar"))
	for i := 0; i < count-1; i++ {
		if _, pErr := kv.SendWrapped(ctx, distSender, putArgs); pErr != nil {
			t.Fatal(pErr)
		}
		// Wait a little bit to increase the likelihood that we observe an invalid
		// ordering. This is not intended to be foolproof.
		time.Sleep(10 * time.Millisecond)
		for j, errCh := range errChs {
			select {
			case err := <-errCh:
				t.Fatalf("%d: WaitForApplication returned early (request: %d, err: %v)", j, i, err)
			default:
			}
		}
	}

	// Once the `count`th command has been issued, the request should return.
	if _, pErr := kv.SendWrapped(ctx, distSender, putArgs); pErr != nil {
		t.Fatal(pErr)
	}
	for i, errCh := range errChs {
		if err := <-errCh; err != nil {
			t.Fatalf("%d: %+v", i, err)
		}
	}

	atomic.StoreInt64(&filterRangeIDAtomic, 0)

	// GC the replica while a request is in progress. The request should return
	// an error.
	go func() {
		_, err := targets[2].client.WaitForApplication(ctx, &kvserver.WaitForApplicationRequest{
			StoreRequestHeader: targets[2].header,
			RangeID:            rangeID,
			LeaseIndex:         math.MaxUint64,
		})
		errChs[2] <- err
	}()
	repl2, err := store2.GetReplica(rangeID)
	if err != nil {
		t.Fatal(err)
	}
	mtc.unreplicateRange(repl2.RangeID, 2)
	if err := store2.ManualReplicaGC(repl2); err != nil {
		t.Fatal(err)
	}
	if _, err := repl2.IsDestroyed(); err == nil {
		t.Fatalf("replica was not destroyed after gc on store2")
	}
	err = <-errChs[2]
	if exp := fmt.Sprintf("r%d was not found", rangeID); !testutils.IsError(err, exp) {
		t.Fatalf("expected %q error, but got %v", exp, err)
	}

	// Allow the client context to time out while a request is in progress. The
	// request should return an error.
	{
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		_, err := targets[0].client.WaitForApplication(ctx, &kvserver.WaitForApplicationRequest{
			StoreRequestHeader: targets[0].header,
			RangeID:            rangeID,
			LeaseIndex:         math.MaxUint64,
		})
		if exp := "context deadline exceeded"; !testutils.IsError(err, exp) {
			t.Fatalf("expected %q error, but got %v", exp, err)
		}
	}
}

func TestStoreWaitForReplicaInit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	sc := kvserver.TestStoreConfig(nil)
	mtc := &multiTestContext{
		storeConfig: &sc,
		// This test was written before the multiTestContext started creating many
		// system ranges at startup, and hasn't been update to take that into
		// account.
		startWithSingleRange: true,
	}
	mtc.Start(t, 1)
	defer mtc.Stop()
	store := mtc.Store(0)

	conn, err := mtc.nodeDialer.Dial(ctx, store.Ident.NodeID, rpc.DefaultClass)
	if err != nil {
		t.Fatal(err)
	}
	client := kvserver.NewPerReplicaClient(conn)
	storeHeader := kvserver.StoreRequestHeader{NodeID: store.Ident.NodeID, StoreID: store.Ident.StoreID}

	// Test that WaitForReplicaInit returns successfully if the replica exists.
	_, err = client.WaitForReplicaInit(ctx, &kvserver.WaitForReplicaInitRequest{
		StoreRequestHeader: storeHeader,
		RangeID:            roachpb.RangeID(1),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Test that WaitForReplicaInit times out if the replica does not exist.
	{
		timeoutCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		_, err = client.WaitForReplicaInit(timeoutCtx, &kvserver.WaitForReplicaInitRequest{
			StoreRequestHeader: storeHeader,
			RangeID:            roachpb.RangeID(2),
		})
		if exp := "context deadline exceeded"; !testutils.IsError(err, exp) {
			t.Fatalf("expected %q error, but got %v", exp, err)
		}
	}

	// Test that WaitForReplicaInit times out if the replica exists but is not
	// initialized.
	{
		// Constructing an permanently-uninitialized replica is somewhat difficult.
		// Sending a fake Raft heartbeat for a range ID that the store hasn't seen
		// before does the trick.
		var repl42 *kvserver.Replica
		testutils.SucceedsSoon(t, func() (err error) {
			// Try several times, as the message may be dropped (see #18355).
			mtc.transport.SendAsync(&kvserver.RaftMessageRequest{
				ToReplica: roachpb.ReplicaDescriptor{
					NodeID:  store.Ident.NodeID,
					StoreID: store.Ident.StoreID,
				},
				Heartbeats: []kvserver.RaftHeartbeat{{RangeID: 42, ToReplicaID: 1}},
			}, rpc.DefaultClass)
			repl42, err = store.GetReplica(42)
			return err
		})
		if repl42.IsInitialized() {
			t.Fatalf("test bug: repl42 is initialized")
		}

		timeoutCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		_, err = client.WaitForReplicaInit(timeoutCtx, &kvserver.WaitForReplicaInitRequest{
			StoreRequestHeader: storeHeader,
			RangeID:            roachpb.RangeID(42),
		})
		if exp := "context deadline exceeded"; !testutils.IsError(err, exp) {
			t.Fatalf("expected %q error, but got %v", exp, err)
		}
	}
}

// TestTracingDoesNotRaceWithCancelation ensures that the tracing underneath
// raft does not race with tracing operations which might occur concurrently
// due to a request cancelation. When this bug existed this test only
// uncovered it when run under stress.
func TestTracingDoesNotRaceWithCancelation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sc := kvserver.TestStoreConfig(nil)
	sc.TestingKnobs.TraceAllRaftEvents = true
	sc.TestingKnobs.DisableSplitQueue = true
	sc.TestingKnobs.DisableMergeQueue = true
	mtc := &multiTestContext{
		storeConfig: &sc,
	}
	mtc.Start(t, 3)
	defer mtc.Stop()

	db := mtc.Store(0).DB()
	ctx := context.Background()
	// Make the transport flaky for the range in question to encourage proposals
	// to be sent more times and ultimately traced more.
	ri, err := getRangeInfo(ctx, db, roachpb.Key("foo"))
	require.Nil(t, err)

	for i := 0; i < 3; i++ {
		mtc.transport.Listen(mtc.stores[i].Ident.StoreID, &unreliableRaftHandler{
			rangeID:            ri.Desc.RangeID,
			RaftMessageHandler: mtc.stores[i],
			unreliableRaftHandlerFuncs: unreliableRaftHandlerFuncs{
				dropReq: func(req *kvserver.RaftMessageRequest) bool {
					return rand.Intn(2) == 0
				},
			},
		})
	}
	val := []byte("asdf")
	var wg sync.WaitGroup
	put := func(i int) func() {
		wg.Add(1)
		return func() {
			defer wg.Done()
			totalDelay := 1 * time.Millisecond
			delay := time.Duration(rand.Intn(int(totalDelay)))
			startDelay := totalDelay - delay
			time.Sleep(startDelay)
			ctx, cancel := context.WithTimeout(context.Background(), delay)
			defer cancel()
			_ = db.Put(ctx, roachpb.Key(fmt.Sprintf("foo%d", i)), val)
		}
	}
	const N = 256
	for i := 0; i < N; i++ {
		go put(i)()
	}
	wg.Wait()
}

type disablingClientStream struct {
	grpc.ClientStream
	disabled *atomic.Value
}

func (cs *disablingClientStream) SendMsg(m interface{}) error {
	if cs.disabled.Load().(bool) {
		return nil
	}
	return cs.ClientStream.SendMsg(m)
}

// TestDefaultConnectionDisruptionDoesNotInterfereWithSystemTraffic tests that
// disconnection on connections of the rpc.DefaultClass do not interfere with
// traffic on the SystemClass connection.
func TestDefaultConnectionDisruptionDoesNotInterfereWithSystemTraffic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// This test relies on concurrently waiting for a value to change in the
	// underlying engine(s). Since the teeing engine does not respond well to
	// value mismatches, whether transient or permanent, skip this test if the
	// teeing engine is being used. See
	// https://github.com/cockroachdb/cockroach/issues/42656 for more context.
	if storage.DefaultStorageEngine == enginepb.EngineTypeTeePebbleRocksDB {
		t.Skip("disabled on teeing engine")
	}

	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)
	// disabled controls whether to disrupt DefaultClass streams.
	var disabled atomic.Value
	disabled.Store(false)
	knobs := rpc.ContextTestingKnobs{
		StreamClientInterceptor: func(target string, class rpc.ConnectionClass) grpc.StreamClientInterceptor {
			if class == rpc.SystemClass {
				return nil
			}
			return func(
				ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn,
				method string, streamer grpc.Streamer, opts ...grpc.CallOption,
			) (grpc.ClientStream, error) {
				cs, err := streamer(ctx, desc, cc, method, opts...)
				if err != nil {
					return nil, err
				}
				return &disablingClientStream{
					disabled:     &disabled,
					ClientStream: cs,
				}, nil
			}
		},
	}
	// Prevent the split queue from creating additional ranges while we're
	// waiting for replication.
	sc := kvserver.TestStoreConfig(nil)
	mtc := &multiTestContext{
		storeConfig:     &sc,
		rpcTestingKnobs: knobs,
	}

	const numReplicas = 3
	mtc.Start(t, numReplicas)
	defer mtc.Stop()
	for _, s := range mtc.stores {
		s.SetReplicateQueueActive(true)
	}
	mtc.replicateRange(1, 1, 2)
	// Make a key that's in the user data space.
	keyA := append(keys.SystemSQLCodec.TablePrefix(100), 'a')
	replica1 := mtc.stores[0].LookupReplica(roachpb.RKey(keyA))
	mtc.replicateRange(replica1.RangeID, 1, 2)
	// Create a test function so that we can run the test both immediately after
	// up-replicating and after a restart.
	runTest := func(t *testing.T) {
		// Look up the replica again because we may have restarted the store.
		replica1 = mtc.stores[0].LookupReplica(roachpb.RKey(keyA))
		// Put some data in the range so we'll have something to test for.
		db := mtc.Store(0).DB()
		require.NoError(t, db.Put(ctx, keyA, 1))

		// Wait for all nodes to catch up.
		mtc.waitForValues(keyA, []int64{1, 1, 1})
		disabled.Store(true)
		repl1, err := mtc.stores[0].GetReplica(1)
		require.Nil(t, err)
		// Transfer the lease on range 1. Make sure there's no pending transfer.
		var lease roachpb.Lease
		testutils.SucceedsSoon(t, func() error {
			var next roachpb.Lease
			lease, next = repl1.GetLease()
			if next != (roachpb.Lease{}) {
				return fmt.Errorf("lease transfer in process, next = %v", next)
			}
			return nil
		})

		var target int
		for i := roachpb.StoreID(1); i <= numReplicas; i++ {
			if lease.Replica.StoreID != i {
				target = int(i - 1)
				break
			}
		}
		// Use SucceedsSoon to deal with rare stress cases where the lease
		// transfer may fail.
		testutils.SucceedsSoon(t, func() error {
			return mtc.transferLeaseNonFatal(ctx, 1, target, int(lease.Replica.StoreID-1))
		})
		// Set a relatively short timeout so that this test doesn't take too long.
		// We should always hit it.
		withTimeout, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
		defer cancel()
		err = db.Put(withTimeout, keyA, 2)
		require.True(t, testutils.IsError(err, "deadline exceeded"), err)
		// Transfer the lease back to demonstrate that the system range is still live.
		testutils.SucceedsSoon(t, func() error {
			return mtc.transferLeaseNonFatal(ctx, 1, target, int(lease.Replica.StoreID-1))
		})

		// Heal the partition, the previous proposal may now succeed but it may have
		// have been canceled.
		disabled.Store(false)
		// Overwrite with a new value and ensure that it propagates.
		require.NoError(t, db.Put(ctx, keyA, 3))
		mtc.waitForValuesT(t, keyA, []int64{3, 3, 3})
	}
	t.Run("initial_run", runTest)
	mtc.restart()
	t.Run("after_restart", runTest)
}

// TestAckWriteBeforeApplication tests that the success of transactional writes
// is acknowledged after those writes have been committed to a Range's Raft log
// but before those writes have been applied to its replicated state machine.
func TestAckWriteBeforeApplication(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, tc := range []struct {
		repls            int
		expAckBeforeAppl bool
	}{
		// In a single-replica Range, each handleRaftReady iteration will append
		// new entries to the Raft log and immediately apply them. This prevents
		// "early acknowledgement" from being possible or useful. See the comment
		// on apply.Task.AckCommittedEntriesBeforeApplication.
		{1, false},
		// In a three-replica Range, each handleRaftReady iteration will append
		// a set of entries to the Raft log and then apply the previous set of
		// entries. This makes "early acknowledgement" a major optimization, as
		// it pulls the entire latency required to append the next set of entries
		// to the Raft log out of the client-perceived latency of the previous
		// set of entries.
		{3, true},
	} {
		t.Run(fmt.Sprintf("numRepls=%d", tc.repls), func(t *testing.T) {
			var filterActive int32
			var magicTS hlc.Timestamp
			blockPreApplication, blockPostApplication := make(chan struct{}), make(chan struct{})
			applyFilterFn := func(ch chan struct{}) kvserverbase.ReplicaApplyFilter {
				return func(filterArgs kvserverbase.ApplyFilterArgs) (int, *roachpb.Error) {
					if atomic.LoadInt32(&filterActive) == 1 && filterArgs.Timestamp == magicTS {
						<-ch
					}
					return 0, nil
				}
			}

			tsc := kvserver.TestStoreConfig(nil)
			tsc.TestingKnobs.TestingApplyFilter = applyFilterFn(blockPreApplication)
			tsc.TestingKnobs.TestingPostApplyFilter = applyFilterFn(blockPostApplication)

			mtc := &multiTestContext{storeConfig: &tsc}
			defer mtc.Stop()
			mtc.Start(t, tc.repls)

			// Replicate the Range, if necessary.
			key := roachpb.Key("a")
			rangeID := mtc.stores[0].LookupReplica(roachpb.RKey(key)).RangeID
			for i := 1; i < tc.repls; i++ {
				mtc.replicateRange(rangeID, i)
			}

			// Begin peforming a write on the Range.
			magicTS = mtc.stores[0].Clock().Now()
			atomic.StoreInt32(&filterActive, 1)
			ch := make(chan *roachpb.Error, 1)
			go func() {
				ctx := context.Background()
				put := putArgs(key, []byte("val"))
				_, pErr := kv.SendWrappedWith(ctx, mtc.stores[0].TestSender(), roachpb.Header{
					Timestamp: magicTS,
				}, put)
				ch <- pErr
			}()

			expResult := func() {
				t.Helper()
				if pErr := <-ch; pErr != nil {
					t.Fatalf("unexpected proposal result error: %v", pErr)
				}
			}
			dontExpResult := func() {
				t.Helper()
				select {
				case <-time.After(10 * time.Millisecond):
					// Expected.
				case pErr := <-ch:
					t.Fatalf("unexpected proposal acknowledged before TestingApplyFilter: %v", pErr)
				}
			}

			// The result should be blocked on the pre-apply filter.
			dontExpResult()

			// Release the pre-apply filter.
			close(blockPreApplication)
			// Depending on the cluster configuration, The result may not be blocked
			// on the post-apply filter because it may be able to acknowledges the
			// client before applying.
			if tc.expAckBeforeAppl {
				expResult()
			} else {
				dontExpResult()
			}

			// Stop blocking Raft application to allow everything to shut down cleanly.
			// This also confirms that the proposal does eventually apply.
			close(blockPostApplication)
			// If we didn't expect an acknowledgement before, we do now.
			if !tc.expAckBeforeAppl {
				expResult()
			}
		})
	}
}

// TestProcessSplitAfterRightHandSideHasBeenRemoved tests cases where we have
// a follower replica which has received information about the RHS of a split
// before it has processed that split. The replica can't both have an
// initialized RHS and process the split but it can have (1) an uninitialized
// RHS with a higher replica ID than in the split and (2) a RHS with an unknown
// replica ID and a tombstone with a higher replica ID than in the split.
// It may learn about a newer replica ID than the split without ever hearing
// about the split replica. If it does not crash (3) it will know that the
// split replica is too old and will not initialize it. If the node does
// crash (4) it will forget it had learned about the higher replica ID and
// will initialize the RHS as the split replica.
//
// Starting in 19.2 if a replica discovers from a raft message that it is an
// old replica then it knows that it has been removed and re-added to the range.
// In this case the Replica eagerly destroys itself and its data.
//
// Given this behavior there are 4 troubling cases with regards to splits.
//
//   *  In all cases we begin with s1 processing a presplit snapshot for
//      r20. After the split the store should have r21/3.
//
// In the first two cases the following occurs:
//
//   *  s1 receives a message for r21/3 prior to acquiring the split lock
//      in r21. This will create an uninitialized r21/3 which may write
//      HardState.
//
//   *  Before the r20 processes the split r21 is removed and re-added to
//      s1 as r21/4. s1 receives a raft message destined for r21/4 and proceeds
//      to destroy its uninitialized r21/3, laying down a tombstone at 4 in the
//      process.
//
//  (1) s1 processes the split and finds the RHS to be an uninitialized replica
//      with a higher replica ID.
//
//  (2) s1 crashes before processing the split, forgetting the replica ID of the
//      RHS but retaining its tombstone.
//
// In both cases we know that the RHS could not have committed anything because
// it cannot have gotten a snapshot but we want to be sure to not synthesize a
// HardState for the RHS that contains a non-zero commit index if we know that
// the RHS will need another snapshot later.
//
// In the third and fourth cases:
//
//   *  s1 never receives a message for r21/3.
//
//   *  Before the r20 processes the split r21 is removed and re-added to
//      s1 as r21/4. s1 receives a raft message destined for r21/4 and has never
//      heard about r21/3.
//
//  (3) s1 processes the split and finds the RHS to be an uninitialized replica
//      with a higher replica ID (but without a tombstone). This case is very
//      similar to (1)
//
//  (4) s1 crashes still before processing the split, forgetting that it had
//      known about r21/4. When it reboots r21/4 is totally partitioned and
//      r20 becomes unpartitioned.
//
//   *  r20 processes the split successfully and initialized r21/3.
//
// In the 4th case we find that until we unpartition r21/4 (the RHS) and let it
// learn about its removal with a ReplicaTooOldError that it will be initialized
// with a CommitIndex at 10 as r21/3, the split's value. After r21/4 becomes
// unpartitioned it will learn it is removed by either catching up on its
// its log or receiving a ReplicaTooOldError which will lead to a tombstone.
//
func TestProcessSplitAfterRightHandSideHasBeenRemoved(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// This test relies on concurrently waiting for a value to change in the
	// underlying engine(s). Since the teeing engine does not respond well to
	// value mismatches, whether transient or permanent, skip this test if the
	// teeing engine is being used. See
	// https://github.com/cockroachdb/cockroach/issues/42656 for more context.
	if storage.DefaultStorageEngine == enginepb.EngineTypeTeePebbleRocksDB {
		t.Skip("disabled on teeing engine")
	}
	sc := kvserver.TestStoreConfig(nil)
	// Newly-started stores (including the "rogue" one) should not GC
	// their replicas. We'll turn this back on when needed.
	sc.TestingKnobs.DisableReplicaGCQueue = true
	sc.RaftDelaySplitToSuppressSnapshotTicks = 0
	// Make the tick interval short so we don't need to wait too long for the
	// partitioned leader to time out. Also make the
	// RangeLeaseRaftElectionTimeout multiplier high so that system ranges
	// like node liveness can actually get leases.
	sc.RaftTickInterval = 10 * time.Millisecond
	sc.RangeLeaseRaftElectionTimeoutMultiplier = 1000
	noopProposalFilter := kvserverbase.ReplicaProposalFilter(func(args kvserverbase.ProposalFilterArgs) *roachpb.Error {
		return nil
	})
	var proposalFilter atomic.Value
	proposalFilter.Store(noopProposalFilter)
	sc.TestingKnobs.TestingProposalFilter = func(args kvserverbase.ProposalFilterArgs) *roachpb.Error {
		return proposalFilter.Load().(kvserverbase.ReplicaProposalFilter)(args)
	}

	ctx := context.Background()
	increment := func(t *testing.T, db *kv.DB, key roachpb.Key, by int64) {
		b := &kv.Batch{}
		b.AddRawRequest(incrementArgs(key, by))
		require.NoError(t, db.Run(ctx, b))
	}
	changeReplicas := func(
		t *testing.T, db *kv.DB, typ roachpb.ReplicaChangeType, key roachpb.Key, idx int,
	) error {
		ri, err := getRangeInfo(ctx, db, key)
		require.NoError(t, err)
		_, err = db.AdminChangeReplicas(ctx, ri.Desc.StartKey.AsRawKey(), ri.Desc,
			roachpb.MakeReplicationChanges(typ, makeReplicationTargets(idx+1)...))
		return err
	}
	split := func(t *testing.T, db *kv.DB, key roachpb.Key) {
		b := &kv.Batch{}
		b.AddRawRequest(adminSplitArgs(key))
		require.NoError(t, db.Run(ctx, b))
	}
	ensureNoTombstone := func(t *testing.T, store *kvserver.Store, rangeID roachpb.RangeID) {
		var tombstone roachpb.RangeTombstone
		tombstoneKey := keys.RangeTombstoneKey(rangeID)
		ok, err := storage.MVCCGetProto(
			ctx, store.Engine(), tombstoneKey, hlc.Timestamp{}, &tombstone, storage.MVCCGetOptions{},
		)
		require.NoError(t, err)
		require.False(t, ok)
	}
	getHardState := func(
		t *testing.T, store *kvserver.Store, rangeID roachpb.RangeID,
	) raftpb.HardState {
		hs, err := stateloader.Make(rangeID).LoadHardState(ctx, store.Engine())
		require.NoError(t, err)
		return hs
	}
	partitionReplicaOnSplit := func(t *testing.T, mtc *multiTestContext, key roachpb.Key, basePartition *mtcPartitionedRange, partRange **mtcPartitionedRange) {
		// Set up a hook to partition the RHS range at its initial range ID
		// before proposing the split trigger.
		var setupOnce sync.Once
		f := kvserverbase.ReplicaProposalFilter(func(args kvserverbase.ProposalFilterArgs) *roachpb.Error {
			req, ok := args.Req.GetArg(roachpb.EndTxn)
			if !ok {
				return nil
			}
			endTxn := req.(*roachpb.EndTxnRequest)
			if endTxn.InternalCommitTrigger == nil || endTxn.InternalCommitTrigger.SplitTrigger == nil {
				return nil
			}
			split := endTxn.InternalCommitTrigger.SplitTrigger

			if !split.RightDesc.StartKey.Equal(key) {
				return nil
			}
			setupOnce.Do(func() {
				replDesc, ok := split.RightDesc.GetReplicaDescriptor(1)
				require.True(t, ok)
				var err error
				*partRange, err = basePartition.extend(mtc, split.RightDesc.RangeID, replDesc.ReplicaID,
					0 /* partitionedNode */, true /* activated */, unreliableRaftHandlerFuncs{})
				require.NoError(t, err)
				proposalFilter.Store(noopProposalFilter)
			})
			return nil
		})
		proposalFilter.Store(f)
	}

	// The basic setup for all of these tests are that we have a LHS range on 3
	// nodes and we've partitioned store 0 for the LHS range. The tests will now
	// perform a split, remove the RHS, add it back and validate assumptions.
	//
	// Different outcomes will occur depending on whether and how the RHS is
	// partitioned and whether the server is killed. In all cases we want the
	// split to succeed and the RHS to eventually also be on all 3 nodes.
	setup := func(t *testing.T) (
		mtc *multiTestContext,
		db *kv.DB,
		keyA, keyB roachpb.Key,
		lhsID roachpb.RangeID,
		lhsPartition *mtcPartitionedRange,
	) {
		mtc = &multiTestContext{
			storeConfig: &sc,
		}
		mtc.Start(t, 3)

		db = mtc.Store(1).DB()

		// Split off a non-system range so we don't have to account for node liveness
		// traffic.
		scratchTableKey := keys.SystemSQLCodec.TablePrefix(math.MaxUint32)
		// Put some data in the range so we'll have something to test for.
		keyA = append(append(roachpb.Key{}, scratchTableKey...), 'a')
		keyB = append(append(roachpb.Key{}, scratchTableKey...), 'b')

		split(t, db, scratchTableKey)
		ri, err := getRangeInfo(ctx, db, scratchTableKey)
		require.Nil(t, err)
		lhsID = ri.Desc.RangeID
		// First put the range on all three nodes.
		mtc.replicateRange(lhsID, 1, 2)

		// Set up a partition for the LHS range only. Initially it is not active.
		lhsPartition, err = setupPartitionedRange(mtc, lhsID,
			0 /* replicaID */, 0 /* partitionedNode */, false /* activated */, unreliableRaftHandlerFuncs{})
		require.NoError(t, err)
		// Wait for all nodes to catch up.
		increment(t, db, keyA, 5)
		mtc.waitForValues(keyA, []int64{5, 5, 5})

		// Transfer the lease off of node 0.
		mtc.transferLease(ctx, lhsID, 0, 2)

		// Make sure everybody knows about that transfer.
		increment(t, db, keyA, 1)
		mtc.waitForValues(keyA, []int64{6, 6, 6})
		lhsPartition.activate()

		increment(t, db, keyA, 1)
		mtc.waitForValues(keyA, []int64{6, 7, 7})
		return mtc, db, keyA, keyB, lhsID, lhsPartition
	}

	// In this case we only have the LHS partitioned. The RHS will learn about its
	// identity as the replica in the split and after being re-added will learn
	// about the new replica ID and will lay down a tombstone. At this point we'll
	// partition the RHS and ensure that the split does not clobber the RHS's hard
	// state.
	t.Run("(1) no RHS partition", func(t *testing.T) {
		mtc, db, keyA, keyB, _, lhsPartition := setup(t)
		defer mtc.Stop()

		split(t, db, keyB)

		// Write a value which we can observe to know when the split has been
		// applied by the LHS.
		increment(t, db, keyA, 1)
		mtc.waitForValues(keyA, []int64{6, 8, 8})

		increment(t, db, keyB, 6)
		// Wait for all non-partitioned nodes to catch up.
		mtc.waitForValues(keyB, []int64{0, 6, 6})

		rhsInfo, err := getRangeInfo(ctx, db, keyB)
		require.NoError(t, err)
		rhsID := rhsInfo.Desc.RangeID
		_, store0Exists := rhsInfo.Desc.GetReplicaDescriptor(1)
		require.True(t, store0Exists)

		// Remove and re-add the RHS to create a new uninitialized replica at
		// a higher replica ID. This will lead to a tombstone being written.
		require.NoError(t, changeReplicas(t, db, roachpb.REMOVE_REPLICA, keyB, 0))
		// Unsuccessful because the RHS will not accept the learner snapshot
		// and will be rolled back. Nevertheless it will have learned that it
		// has been removed at the old replica ID.
		err = changeReplicas(t, db, roachpb.ADD_REPLICA, keyB, 0)
		require.True(t,
			testutils.IsError(err, "snapshot failed.*cannot apply snapshot: snapshot intersects"), err)

		// Without a partitioned RHS we'll end up always writing a tombstone here because
		// the RHS will be created at the initial replica ID because it will get
		// raft message when the other nodes split and then after the above call
		// it will find out about its new replica ID and write a tombstone for the
		// old one.
		waitForTombstone(t, mtc.Store(0).Engine(), rhsID)
		lhsPartition.deactivate()
		mtc.waitForValues(keyA, []int64{8, 8, 8})
		hs := getHardState(t, mtc.Store(0), rhsID)
		require.Equal(t, uint64(0), hs.Commit)
		testutils.SucceedsSoon(t, func() error {
			return changeReplicas(t, db, roachpb.ADD_REPLICA, keyB, 0)
		})
		mtc.waitForValues(keyB, []int64{6, 6, 6})
	})

	// This case is like the previous case except the store crashes after
	// laying down a tombstone.
	t.Run("(2) no RHS partition, with restart", func(t *testing.T) {
		mtc, db, keyA, keyB, _, lhsPartition := setup(t)
		defer mtc.Stop()

		split(t, db, keyB)

		// Write a value which we can observe to know when the split has been
		// applied by the LHS.
		increment(t, db, keyA, 1)
		mtc.waitForValues(keyA, []int64{6, 8, 8})

		increment(t, db, keyB, 6)
		// Wait for all non-partitioned nodes to catch up.
		mtc.waitForValues(keyB, []int64{0, 6, 6})

		rhsInfo, err := getRangeInfo(ctx, db, keyB)
		require.NoError(t, err)
		rhsID := rhsInfo.Desc.RangeID
		_, store0Exists := rhsInfo.Desc.GetReplicaDescriptor(1)
		require.True(t, store0Exists)

		// Remove and re-add the RHS to create a new uninitialized replica at
		// a higher replica ID. This will lead to a tombstone being written.
		require.NoError(t, changeReplicas(t, db, roachpb.REMOVE_REPLICA, keyB, 0))
		// Unsuccessfuly because the RHS will not accept the learner snapshot
		// and will be rolled back. Nevertheless it will have learned that it
		// has been removed at the old replica ID.
		err = changeReplicas(t, db, roachpb.ADD_REPLICA, keyB, 0)
		require.True(t,
			testutils.IsError(err, "snapshot failed.*cannot apply snapshot: snapshot intersects"), err)

		// Without a partitioned RHS we'll end up always writing a tombstone here because
		// the RHS will be created at the initial replica ID because it will get
		// raft message when the other nodes split and then after the above call
		// it will find out about its new replica ID and write a tombstone for the
		// old one.
		waitForTombstone(t, mtc.Store(0).Engine(), rhsID)

		// We do all of this incrementing to ensure that nobody will ever
		// succeed in sending a message the new RHS replica after we restart
		// the store. Previously there were races which could happen if we
		// stopped the store immediately. Sleeps worked but this feels somehow
		// more principled.
		curB := int64(6)
		for curB < 100 {
			curB++
			increment(t, db, keyB, 1)
			mtc.waitForValues(keyB, []int64{0, curB, curB})
		}

		// Restart store 0 so that it forgets about the newer replicaID.
		mtc.stopStore(0)
		mtc.restartStore(0)

		lhsPartition.deactivate()
		mtc.waitForValues(keyA, []int64{8, 8, 8})
		hs := getHardState(t, mtc.Store(0), rhsID)
		require.Equal(t, uint64(0), hs.Commit)
		testutils.SucceedsSoon(t, func() error {
			return changeReplicas(t, db, roachpb.ADD_REPLICA, keyB, 0)
		})
		mtc.waitForValues(keyB, []int64{curB, curB, curB})
	})

	// In this case the RHS will be partitioned from hearing anything about
	// the initial replica ID of the RHS after the split. It will learn about
	// the higher replica ID and have that higher replica ID in memory when
	// the split is processed. We partition the RHS's new replica ID before
	// processing the split to ensure that the RHS doesn't get initialized.
	t.Run("(3) initial replica RHS partition, no restart", func(t *testing.T) {
		mtc, db, keyA, keyB, _, lhsPartition := setup(t)
		defer mtc.Stop()
		var rhsPartition *mtcPartitionedRange
		partitionReplicaOnSplit(t, mtc, keyB, lhsPartition, &rhsPartition)
		split(t, db, keyB)

		// Write a value which we can observe to know when the split has been
		// applied by the LHS.
		increment(t, db, keyA, 1)
		mtc.waitForValues(keyA, []int64{6, 8, 8})

		increment(t, db, keyB, 6)
		// Wait for all non-partitioned nodes to catch up.
		mtc.waitForValues(keyB, []int64{0, 6, 6})

		rhsInfo, err := getRangeInfo(ctx, db, keyB)
		require.NoError(t, err)
		rhsID := rhsInfo.Desc.RangeID
		_, store0Exists := rhsInfo.Desc.GetReplicaDescriptor(1)
		require.True(t, store0Exists)

		// Remove and re-add the RHS to create a new uninitialized replica at
		// a higher replica ID. This will lead to a tombstone being written.
		require.NoError(t, changeReplicas(t, db, roachpb.REMOVE_REPLICA, keyB, 0))
		// Unsuccessful because the RHS will not accept the learner snapshot
		// and will be rolled back. Nevertheless it will have learned that it
		// has been removed at the old replica ID.
		err = changeReplicas(t, db, roachpb.ADD_REPLICA, keyB, 0)
		require.True(t,
			testutils.IsError(err, "snapshot failed.*cannot apply snapshot: snapshot intersects"), err)
		// Ensure that the replica exists with the higher replica ID.
		repl, err := mtc.Store(0).GetReplica(rhsInfo.Desc.RangeID)
		require.NoError(t, err)
		require.Equal(t, repl.ReplicaID(), rhsInfo.Desc.NextReplicaID)
		rhsPartition.addReplica(rhsInfo.Desc.NextReplicaID)

		// Ensure that there's no tombstone.
		// The RHS on store 0 never should have heard about its original ID.
		ensureNoTombstone(t, mtc.Store(0), rhsID)
		lhsPartition.deactivate()
		mtc.waitForValues(keyA, []int64{8, 8, 8})
		hs := getHardState(t, mtc.Store(0), rhsID)
		require.Equal(t, uint64(0), hs.Commit)
		// Now succeed in adding the RHS. Use SucceedsSoon because in rare cases
		// the learner snapshot can fail due to a race with a raft snapshot from
		// a raft leader on a different node.
		testutils.SucceedsSoon(t, func() error {
			return changeReplicas(t, db, roachpb.ADD_REPLICA, keyB, 0)
		})
		mtc.waitForValues(keyB, []int64{6, 6, 6})
	})

	// This case is set up like the previous one except after the RHS learns about
	// its higher replica ID the store crahes and forgets. The RHS replica gets
	// initialized by the split.
	t.Run("(4) initial replica RHS partition, with restart", func(t *testing.T) {
		mtc, db, keyA, keyB, _, lhsPartition := setup(t)
		defer mtc.Stop()
		var rhsPartition *mtcPartitionedRange

		partitionReplicaOnSplit(t, mtc, keyB, lhsPartition, &rhsPartition)
		split(t, db, keyB)

		// Write a value which we can observe to know when the split has been
		// applied by the LHS.
		increment(t, db, keyA, 1)
		mtc.waitForValues(keyA, []int64{6, 8, 8})

		increment(t, db, keyB, 6)
		// Wait for all non-partitioned nodes to catch up.
		mtc.waitForValues(keyB, []int64{0, 6, 6})

		rhsInfo, err := getRangeInfo(ctx, db, keyB)
		require.NoError(t, err)
		rhsID := rhsInfo.Desc.RangeID
		_, store0Exists := rhsInfo.Desc.GetReplicaDescriptor(1)
		require.True(t, store0Exists)

		// Remove and re-add the RHS to create a new uninitialized replica at
		// a higher replica ID. This will lead to a tombstone being written.
		require.NoError(t, changeReplicas(t, db, roachpb.REMOVE_REPLICA, keyB, 0))
		// Unsuccessfuly because the RHS will not accept the learner snapshot
		// and will be rolled back. Nevertheless it will have learned that it
		// has been removed at the old replica ID.
		err = changeReplicas(t, db, roachpb.ADD_REPLICA, keyB, 0)
		require.True(t,
			testutils.IsError(err, "snapshot failed.*cannot apply snapshot: snapshot intersects"), err)
		// Ensure that there's no tombstone.
		// The RHS on store 0 never should have heard about its original ID.
		ensureNoTombstone(t, mtc.Store(0), rhsID)

		// Now, before we deactivate the LHS partition, partition the newer replica
		// on the RHS too.
		rhsPartition.addReplica(rhsInfo.Desc.NextReplicaID)

		// We do all of this incrementing to ensure that nobody will ever
		// succeed in sending a message the new RHS replica after we restart
		// the store. Previously there were races which could happen if we
		// stopped the store immediately. Sleeps worked but this feels somehow
		// more principled.
		curB := int64(6)
		for curB < 100 {
			curB++
			increment(t, db, keyB, 1)
			mtc.waitForValues(keyB, []int64{0, curB, curB})
		}

		mtc.stopStore(0)
		mtc.restartStore(0)

		lhsPartition.deactivate()
		mtc.waitForValues(keyA, []int64{8, 8, 8})
		// In this case the store has forgotten that it knew the RHS of the split
		// could not exist. We ensure that it has been initialized to the initial
		// commit value, which is 10.
		testutils.SucceedsSoon(t, func() error {
			hs := getHardState(t, mtc.Store(0), rhsID)
			if hs.Commit != uint64(10) {
				return errors.Errorf("hard state not yet initialized: got %v, expected %v",
					hs.Commit, uint64(10))
			}
			return nil
		})
		rhsPartition.deactivate()
		testutils.SucceedsSoon(t, func() error {
			return changeReplicas(t, db, roachpb.ADD_REPLICA, keyB, 0)
		})
		mtc.waitForValues(keyB, []int64{curB, curB, curB})
	})
}

// TestReplicaRemovalClosesProposalQuota is a somewhat contrived test to ensure
// that when a replica is removed that it closes its proposal quota if it has
// one. This used to not be the case though it wasn't really very consequential.
// Firstly, it's rare that a removed replica has a proposal quota to begin with.
// Replicas which believe they are they leaseholder can only be removed if they
// have lost the lease and are behind. This requires a network partition.
// Regardless, there was never actually a problem because once the replica has
// been removed, all commands will eventually fail and remove themselves from
// the quota pool. This potentially adds latency as every pending request will
// need to acquire and release their quota. This is almost always very fast as
// it is rarely the case that there are more outstanding requests than there is
// quota. Nevertheless, we have this test to ensure that the pool does get
// closed if only to avoid asking the question and to ensure that that case is
// tested.
func TestReplicaRemovalClosesProposalQuota(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	// These variables track the request count to make sure that all of the
	// requests have made it to the Replica.
	var (
		rangeID         int64
		putRequestCount int64
	)
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{Store: &kvserver.StoreTestingKnobs{
				DisableReplicaGCQueue: true,
				TestingRequestFilter: kvserverbase.ReplicaRequestFilter(func(_ context.Context, r roachpb.BatchRequest) *roachpb.Error {
					if r.RangeID == roachpb.RangeID(atomic.LoadInt64(&rangeID)) {
						if _, isPut := r.GetArg(roachpb.Put); isPut {
							atomic.AddInt64(&putRequestCount, 1)
						}
					}
					return nil
				}),
			}},
			RaftConfig: base.RaftConfig{
				// Set the proposal quota to a tiny amount so that each write will
				// exceed it.
				RaftProposalQuota: 512,
				// RaftMaxInflightMsgs * RaftMaxSizePerMsg cannot exceed RaftProposalQuota.
				RaftMaxInflightMsgs: 2,
				RaftMaxSizePerMsg:   256,
			},
		},
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	key := tc.ScratchRange(t)
	require.NoError(t, tc.WaitForSplitAndInitialization(key))
	desc, err := tc.LookupRange(key)
	require.NoError(t, err)
	atomic.StoreInt64(&rangeID, int64(desc.RangeID))
	tc.AddReplicasOrFatal(t, key, tc.Target(1), tc.Target(2))
	// Partition node 1 from receiving any requests or responses.
	// This will prevent it from successfully replicating anything.
	require.NoError(t, tc.WaitForSplitAndInitialization(key))
	require.NoError(t, tc.TransferRangeLease(desc, tc.Target(0)))
	store, repl := getFirstStoreReplica(t, tc.Server(0), key)
	funcs := unreliableRaftHandlerFuncs{}
	tc.Servers[0].RaftTransport().Listen(store.StoreID(), &unreliableRaftHandler{
		rangeID:                    desc.RangeID,
		RaftMessageHandler:         store,
		unreliableRaftHandlerFuncs: funcs,
	})
	// NB: We need to be sure that our Replica is the leaseholder for this
	// test to make sense. It usually is.
	lease, pendingLease := repl.GetLease()
	if pendingLease != (roachpb.Lease{}) || lease.OwnedBy(store.StoreID()) {
		t.Skip("the replica is not the leaseholder, this happens rarely under stressrace")
	}
	var wg sync.WaitGroup
	const N = 100
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			k := append(key[0:len(key):len(key)], strconv.Itoa(i)...)
			_, pErr := kv.SendWrappedWith(context.Background(), store.TestSender(), roachpb.Header{
				RangeID: desc.RangeID,
			}, putArgs(k, bytes.Repeat([]byte{'a'}, 1000)))
			require.Regexp(t,
				`result is ambiguous \(removing replica\)|`+
					`r`+strconv.Itoa(int(desc.RangeID))+" was not found on s1", pErr.GoError())
		}(i)
	}
	testutils.SucceedsSoon(t, func() error {
		if seen := atomic.LoadInt64(&putRequestCount); seen < N {
			return fmt.Errorf("saw %d, waiting for %d", seen, N)
		}
		return nil
	})
	desc = *repl.Desc()
	fromReplDesc, found := desc.GetReplicaDescriptor(3)
	require.True(t, found)
	replDesc, found := desc.GetReplicaDescriptor(store.StoreID())
	require.True(t, found)
	newReplDesc := replDesc
	newReplDesc.ReplicaID = desc.NextReplicaID
	require.Nil(t, store.HandleRaftRequest(ctx, &kvserver.RaftMessageRequest{
		RangeID:       desc.RangeID,
		RangeStartKey: desc.StartKey,
		FromReplica:   fromReplDesc,
		ToReplica:     newReplDesc,
		Message:       raftpb.Message{Type: raftpb.MsgVote, Term: 2},
	}, noopRaftMessageResponseSteam{}))
	ts := waitForTombstone(t, store.Engine(), desc.RangeID)
	require.Equal(t, ts.NextReplicaID, desc.NextReplicaID)
	wg.Wait()
	_, err = repl.GetProposalQuota().Acquire(ctx, 1)
	require.Regexp(t, "closed.*destroyed", err)
}

type noopRaftMessageResponseSteam struct{}

func (n noopRaftMessageResponseSteam) Context() context.Context {
	return context.Background()
}

func (n noopRaftMessageResponseSteam) Send(*kvserver.RaftMessageResponse) error {
	return nil
}

var _ kvserver.RaftMessageResponseStream = noopRaftMessageResponseSteam{}

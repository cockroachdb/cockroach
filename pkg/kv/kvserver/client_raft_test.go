// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowdispatch"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/node_rac2"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	raft "github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/listenerutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/circuit"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
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
	defer log.Scope(t).Close(t)

	lisReg := listenerutil.NewListenerRegistry()
	defer lisReg.Close()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			ReplicationMode:     base.ReplicationManual,
			ReusableListenerReg: lisReg,
			ServerArgs: base.TestServerArgs{
				StoreSpecs: []base.StoreSpec{
					{
						InMemory:    true,
						StickyVFSID: "1",
					},
				},
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						StickyVFSRegistry: fs.NewStickyRegistry(),
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)

	splitKey := roachpb.Key("m")
	key1 := roachpb.Key("a")
	key2 := roachpb.Key("z")

	get := func(store *kvserver.Store, key roachpb.Key) int64 {
		args := getArgs(key)
		resp, err := kv.SendWrappedWith(ctx, store.TestSender(), kvpb.Header{}, args)
		if err != nil {
			t.Fatal(err)
		}
		return mustGetInt(resp.(*kvpb.GetResponse).Value)
	}
	validate := func(store *kvserver.Store) {
		if val := get(store, key1); val != 13 {
			t.Errorf("key %q: expected 13 but got %v", key1, val)
		}
		if val := get(store, key2); val != 28 {
			t.Errorf("key %q: expected 28 but got %v", key2, val)
		}
	}

	// First, populate the store with data across two ranges. Each range contains commands
	// that both predate and postdate the split.
	func() {
		store := tc.GetFirstStoreFromServer(t, 0)
		increment := func(key roachpb.Key, value int64) (*kvpb.IncrementResponse, *kvpb.Error) {
			args := incrementArgs(key, value)
			resp, err := kv.SendWrappedWith(ctx, store.TestSender(), kvpb.Header{}, args)
			incResp, _ := resp.(*kvpb.IncrementResponse)
			return incResp, err
		}

		if _, err := increment(key1, 2); err != nil {
			t.Fatal(err)
		}
		if _, err := increment(key2, 5); err != nil {
			t.Fatal(err)
		}
		splitArgs := adminSplitArgs(splitKey)
		if _, err := kv.SendWrapped(ctx, store.TestSender(), splitArgs); err != nil {
			t.Fatal(err)
		}
		lhsRepl := store.LookupReplica(roachpb.RKey(key1))
		rhsRepl := store.LookupReplica(roachpb.RKey(key2))

		if lhsRepl.RangeID == rhsRepl.RangeID {
			t.Fatal("got same range id after split")
		}
		if _, err := increment(key1, 11); err != nil {
			t.Fatal(err)
		}
		if _, err := increment(key2, 23); err != nil {
			t.Fatal(err)
		}
		validate(store)
	}()

	// Now create a new store with the same engine and make sure the expected data
	// is present.
	tc.StopServer(0)
	require.NoError(t, tc.RestartServer(0))
	store := tc.GetFirstStoreFromServer(t, 0)

	// Raft processing is initialized lazily; issue a no-op write request on each key to
	// ensure that is has been started.
	incArgs := incrementArgs(key1, 0)
	if _, err := kv.SendWrapped(ctx, store.TestSender(), incArgs); err != nil {
		t.Fatal(err)
	}
	incArgs = incrementArgs(key2, 0)
	if _, err := kv.SendWrappedWith(ctx, store.TestSender(), kvpb.Header{}, incArgs); err != nil {
		t.Fatal(err)
	}

	validate(store)
}

// TestStoreRecoverWithErrors verifies that even commands that fail are marked as
// applied so they are not retried after recovery.
func TestStoreRecoverWithErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stickyVFSRegistry := fs.NewStickyRegistry()
	lisReg := listenerutil.NewListenerRegistry()
	defer lisReg.Close()

	numIncrements := 0
	keyA := roachpb.Key("a")

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			ReplicationMode:     base.ReplicationManual,
			ReusableListenerReg: lisReg,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						StickyVFSRegistry: stickyVFSRegistry,
					},
					Store: &kvserver.StoreTestingKnobs{
						EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
							TestingEvalFilter: func(filterArgs kvserverbase.FilterArgs) *kvpb.Error {
								_, ok := filterArgs.Req.(*kvpb.IncrementRequest)
								if ok && filterArgs.Req.Header().Key.Equal(keyA) {
									numIncrements++
								}
								return nil
							},
						},
					},
				},
				StoreSpecs: []base.StoreSpec{
					{
						InMemory:    true,
						StickyVFSID: "1",
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)
	store := tc.GetFirstStoreFromServer(t, 0)

	// Write a bytes value so the increment will fail.
	putArgs := putArgs(keyA, []byte("asdf"))
	if _, err := kv.SendWrapped(ctx, store.TestSender(), putArgs); err != nil {
		t.Fatal(err)
	}

	// Try and fail to increment the key. It is important for this test that the
	// failure be the last thing in the raft log when the store is stopped.
	incArgs := incrementArgs(keyA, 42)
	if _, err := kv.SendWrapped(ctx, store.TestSender(), incArgs); err == nil {
		t.Fatal("did not get expected error")
	}

	if numIncrements != 1 {
		t.Fatalf("expected 1 increments; was %d", numIncrements)
	}

	// Recover from the engine.
	tc.StopServer(0)
	require.NoError(t, tc.RestartServer(0))
	recoveredStore := tc.GetFirstStoreFromServer(t, 0)

	// Issue a no-op write to lazily initialize raft on the range.
	keyB := roachpb.Key("b")
	incArgs = incrementArgs(keyB, 0)
	if _, err := kv.SendWrapped(ctx, recoveredStore.TestSender(), incArgs); err != nil {
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
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 2,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)
	store := tc.GetFirstStoreFromServer(t, 0)

	keyA := []byte("a")
	_, rhsDesc := tc.SplitRangeOrFatal(t, keyA)

	// Issue a command on the first node before replicating.
	incArgs := incrementArgs(keyA, 5)
	if _, err := kv.SendWrapped(ctx, store.TestSender(), incArgs); err != nil {
		t.Fatal(err)
	}

	tc.AddVotersOrFatal(t, keyA, tc.Target(1))
	// Verify no intent remains on range descriptor key.
	key := keys.RangeDescriptorKey(rhsDesc.StartKey)
	desc := roachpb.RangeDescriptor{}
	if ok, err := storage.MVCCGetProto(ctx, store.TODOEngine(), key,
		store.Clock().Now(), &desc, storage.MVCCGetOptions{}); err != nil {
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
			if ok, err := storage.MVCCGetProto(ctx, store.TODOEngine(), key.AsRawKey(),
				store.Clock().Now(), &metaDesc, storage.MVCCGetOptions{}); err != nil {
				return err
			} else if !ok {
				return errors.Errorf("failed to resolve %s", key.AsRawKey())
			}
		}
		return nil
	})

	// Verify that the same data is available on the replica.
	testutils.SucceedsSoon(t, func() error {
		getArgs := getArgs(keyA)
		if reply, err := kv.SendWrappedWith(ctx, tc.GetFirstStoreFromServer(t, 1).TestSender(), kvpb.Header{
			ReadConsistency: kvpb.INCONSISTENT,
		}, getArgs); err != nil {
			return errors.Errorf("failed to read data: %s", err)
		} else if e, v := int64(5), mustGetInt(reply.(*kvpb.GetResponse).Value); v != e {
			return errors.Errorf("failed to read correct data: expected %d, got %d", e, v)
		}
		return nil
	})
}

// TestRestoreReplicas ensures that consensus group membership is properly
// persisted to disk and restored when a node is stopped and restarted.
func TestRestoreReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stickyVFSRegistry := fs.NewStickyRegistry()
	lisReg := listenerutil.NewListenerRegistry()
	defer lisReg.Close()

	const numServers int = 2
	stickyServerArgs := make(map[int]base.TestServerArgs)
	for i := 0; i < numServers; i++ {
		stickyServerArgs[i] = base.TestServerArgs{
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:    true,
					StickyVFSID: strconv.FormatInt(int64(i), 10),
				},
			},
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					StickyVFSRegistry: stickyVFSRegistry,
				},
			},
		}
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 2,
		base.TestClusterArgs{
			ReplicationMode:     base.ReplicationManual,
			ReusableListenerReg: lisReg,
			ServerArgsPerNode:   stickyServerArgs,
		})
	defer tc.Stopper().Stop(ctx)
	store := tc.GetFirstStoreFromServer(t, 0)

	key := []byte("a")
	tc.SplitRangeOrFatal(t, key)

	// Perform an increment before replication to ensure that commands are not
	// repeated on restarts.
	incArgs := incrementArgs(key, 23)
	_, pErr := kv.SendWrapped(ctx, store.TestSender(), incArgs)
	require.NoError(t, pErr.GoError())

	tc.AddVotersOrFatal(t, key, tc.Target(1))

	require.NoError(t, tc.Restart())

	// Find the leaseholder and follower. The restart may cause the Raft
	// leadership to bounce around a bit, since we don't fully enable Raft
	// prevote, so we loop for a bit until we find the leaseholder.
	incArgs = incrementArgs(key, 5)
	var followerStore *kvserver.Store
	testutils.SucceedsSoon(t, func() error {
		var pErr *kvpb.Error
		for i := 0; i < tc.NumServers(); i++ {
			_, pErr = kv.SendWrapped(ctx, tc.GetFirstStoreFromServer(t, i).TestSender(), incArgs)
			if pErr == nil {
				followerStore = tc.GetFirstStoreFromServer(t, 1-i)
				break
			}
			require.IsType(t, &kvpb.NotLeaseHolderError{}, pErr.GetDetail())
		}
		return pErr.GoError()
	})

	// The follower should now return a NLHE.
	_, pErr = kv.SendWrapped(ctx, followerStore.TestSender(), incArgs)
	require.Error(t, pErr.GoError())
	require.IsType(t, &kvpb.NotLeaseHolderError{}, pErr.GetDetail())

	testutils.SucceedsSoon(t, func() error {
		getArgs := getArgs(key)
		if reply, err := kv.SendWrappedWith(ctx, followerStore.TestSender(), kvpb.Header{
			ReadConsistency: kvpb.INCONSISTENT,
		}, getArgs); err != nil {
			return errors.Errorf("failed to read data: %s", err)
		} else if e, v := int64(28), mustGetInt(reply.(*kvpb.GetResponse).Value); v != e {
			return errors.Errorf("failed to read correct data: expected %d, got %d", e, v)
		}
		return nil
	})

	validate := func(s *kvserver.Store) {
		repl := s.LookupReplica(key)
		desc := repl.Desc()
		if len(desc.InternalReplicas) != 2 {
			t.Fatalf("store %d: expected 2 replicas, found %d", s.Ident.StoreID, len(desc.InternalReplicas))
		}
		if desc.InternalReplicas[0].NodeID != store.Ident.NodeID {
			t.Errorf("store %d: expected replica[0].NodeID == %d, was %d",
				store.Ident.StoreID, store.Ident.NodeID, desc.InternalReplicas[0].NodeID)
		}
	}

	// Both replicas have a complete list in Desc.Replicas
	validate(tc.GetFirstStoreFromServer(t, 0))
	validate(tc.GetFirstStoreFromServer(t, 1))
}

// TODO(bdarnell): more aggressive testing here; especially with
// proposer-evaluated KV, what this test does is much less as it doesn't
// exercise the path in which the replica change fails at *apply* time (we only
// test the failfast path), in which case the replica change isn't even
// proposed.
func TestFailedReplicaChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var runFilter atomic.Value
	runFilter.Store(true)

	testingEvalFilter := func(filterArgs kvserverbase.FilterArgs) *kvpb.Error {
		if runFilter.Load().(bool) {
			if et, ok := filterArgs.Req.(*kvpb.EndTxnRequest); ok && et.Commit &&
				et.InternalCommitTrigger != nil && et.InternalCommitTrigger.ChangeReplicasTrigger != nil {
				return kvpb.NewErrorWithTxn(errors.Errorf("boom"), filterArgs.Hdr.Txn)
			}
		}
		return nil
	}
	ctx := context.Background()
	manualClock := hlc.NewHybridManualClock()
	tc := testcluster.StartTestCluster(t, 2,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						WallClock: manualClock,
					},
					Store: &kvserver.StoreTestingKnobs{
						EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
							TestingEvalFilter: testingEvalFilter,
						},
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)
	store := tc.GetFirstStoreFromServer(t, 0)

	key := tc.ScratchRange(t)
	repl := store.LookupReplica(roachpb.RKey(key))

	if _, err := tc.AddVoters(key, tc.Target(1)); !testutils.IsError(err, "boom") {
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
	manualClock.Increment(10 * base.DefaultTxnHeartbeatInterval.Nanoseconds())

	tc.AddVotersOrFatal(t, key, tc.Target(1))
	// Wait for the range to sync to both replicas (mainly so leaktest doesn't
	// complain about goroutines involved in the process).
	require.NoError(t, tc.WaitForVoters(key, tc.Targets(0, 1)...))
}

// We can truncate the old log entries and a new replica will be brought up from a snapshot.
func TestReplicateAfterTruncation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 2,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)
	store := tc.GetFirstStoreFromServer(t, 0)

	key := []byte("a")
	tc.SplitRangeOrFatal(t, key)

	// Issue a command on the first node before replicating.
	incArgs := incrementArgs(key, 5)
	if _, err := kv.SendWrapped(ctx, store.TestSender(), incArgs); err != nil {
		t.Fatal(err)
	}

	repl := store.LookupReplica(key)
	// Get that command's log index.
	index := repl.GetLastIndex()

	// Truncate the log at index+1 (log entries < N are removed, so this includes
	// the increment).
	truncArgs := truncateLogArgs(index+1, repl.GetRangeID())
	if _, err := kv.SendWrapped(ctx, store.TestSender(), truncArgs); err != nil {
		t.Fatal(err)
	}

	// Issue a second command post-truncation.
	incArgs = incrementArgs([]byte("a"), 11)
	if _, err := kv.SendWrapped(ctx, store.TestSender(), incArgs); err != nil {
		t.Fatal(err)
	}

	// Now add the second replica.
	tc.AddVotersOrFatal(t, key, tc.Target(1))

	// Once it catches up, the effects of both commands can be seen.
	testutils.SucceedsSoon(t, func() error {
		getArgs := getArgs([]byte("a"))
		if reply, err := kv.SendWrappedWith(ctx, tc.GetFirstStoreFromServer(t, 1).TestSender(), kvpb.Header{
			ReadConsistency: kvpb.INCONSISTENT,
		}, getArgs); err != nil {
			return errors.Errorf("failed to read data: %s", err)
		} else if e, v := int64(16), mustGetInt(reply.(*kvpb.GetResponse).Value); v != e {
			return errors.Errorf("failed to read correct data: expected %d, got %d", e, v)
		}
		return nil
	})

	repl2 := tc.GetFirstStoreFromServer(t, 1).LookupReplica(key)

	testutils.SucceedsSoon(t, func() error {
		if mvcc, mvcc2 := repl.GetMVCCStats(), repl2.GetMVCCStats(); mvcc2 != mvcc {
			return errors.Errorf("expected stats on new range:\n%+v\not equal old:\n%+v", mvcc2, mvcc)
		}
		return nil
	})

	// Send a third command to verify that the log states are synced up so the
	// new node can accept new commands.
	incArgs = incrementArgs([]byte("a"), 23)
	if _, err := kv.SendWrapped(ctx, store.TestSender(), incArgs); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		getArgs := getArgs([]byte("a"))
		if reply, err := kv.SendWrappedWith(ctx, tc.GetFirstStoreFromServer(t, 1).TestSender(), kvpb.Header{
			ReadConsistency: kvpb.INCONSISTENT,
		}, getArgs); err != nil {
			return errors.Errorf("failed to read data: %s", err)
		} else if e, v := int64(39), mustGetInt(reply.(*kvpb.GetResponse).Value); v != e {
			return errors.Errorf("failed to read correct data: expected %d, got %d", e, v)
		}
		return nil
	})
}

func TestRaftLogSizeAfterTruncation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)
	store := tc.GetFirstStoreFromServer(t, 0)

	key := []byte("a")
	tc.SplitRangeOrFatal(t, key)

	incArgs := incrementArgs(key, 5)
	if _, err := kv.SendWrapped(ctx, store.TestSender(), incArgs); err != nil {
		t.Fatal(err)
	}

	repl := store.LookupReplica(key)
	require.NotNil(t, repl)
	index := repl.GetLastIndex()

	// Verifies the recomputed log size against what we track in `r.mu.raftLogSize`.
	assertCorrectRaftLogSize := func() {
		// Lock raftMu so that the log doesn't change while we compute its size.
		repl.RaftLock()
		realSize, err := repl.LogStorageRaftMuLocked().ComputeSize(ctx)
		size, _ := repl.GetRaftLogSize()
		repl.RaftUnlock()
		require.NoError(t, err)
		// If the size isn't trusted, it won't have to match (and in fact
		// likely won't). In this test, this is because the upreplication
		// elides old Raft log entries in the snapshot it uses.
		require.Equal(t, size, realSize)
	}

	assertCorrectRaftLogSize()
	truncArgs := truncateLogArgs(index+1, repl.GetRangeID())
	if _, err := kv.SendWrapped(ctx, store.TestSender(), truncArgs); err != nil {
		t.Fatal(err)
	}
	// Note that if there were multiple nodes, the Raft log sizes would not
	// be correct for the followers as they would have received a shorter
	// Raft log than the leader.
	assertCorrectRaftLogSize()
}

// TestSnapshotAfterTruncation tests that Raft will properly send a snapshot
// when a node is brought up and the log has been truncated.
func TestSnapshotAfterTruncation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	for _, changeTerm := range []bool{false, true} {
		name := "sameTerm"
		if changeTerm {
			name = "differentTerm"
		}
		t.Run(name, func(t *testing.T) {
			lisReg := listenerutil.NewListenerRegistry()
			defer lisReg.Close()

			const numServers int = 3
			stickyServerArgs := make(map[int]base.TestServerArgs)
			for i := 0; i < numServers; i++ {
				stickyServerArgs[i] = base.TestServerArgs{
					StoreSpecs: []base.StoreSpec{
						{
							InMemory:    true,
							StickyVFSID: strconv.FormatInt(int64(i), 10),
						},
					},
					Knobs: base.TestingKnobs{
						Server: &server.TestingKnobs{
							StickyVFSRegistry: fs.NewStickyRegistry(),
						},
					},
				}
			}

			ctx := context.Background()
			tc := testcluster.StartTestCluster(t, numServers,
				base.TestClusterArgs{
					ReplicationMode:     base.ReplicationManual,
					ReusableListenerReg: lisReg,
					ServerArgsPerNode:   stickyServerArgs,
				})
			defer tc.Stopper().Stop(ctx)
			store := tc.GetFirstStoreFromServer(t, 0)

			var stoppedStore = 1
			var otherStore1 = 0
			var otherStore2 = 2

			key := []byte("a")
			tc.SplitRangeOrFatal(t, key)

			incA := int64(5)
			incB := int64(7)
			incAB := incA + incB

			// Set up a key to replicate across the cluster. We're going to modify this
			// key and truncate the raft logs from that command after killing one of the
			// nodes to check that it gets the new value after it comes up.
			incArgs := incrementArgs(key, incA)
			if _, err := kv.SendWrapped(ctx, store.TestSender(), incArgs); err != nil {
				t.Fatal(err)
			}

			tc.AddVotersOrFatal(t, key, tc.Targets(1, 2)...)
			tc.WaitForValues(t, key, []int64{incA, incA, incA})

			// Now kill one store, increment the key on the other stores and truncate
			// their logs to make sure that when store 1 comes back up it will require
			// a snapshot from Raft.
			tc.StopServer(stoppedStore)

			incArgs = incrementArgs(key, incB)
			if _, err := kv.SendWrapped(ctx, store.TestSender(), incArgs); err != nil {
				t.Fatal(err)
			}

			tc.WaitForValues(t, key, []int64{incAB, 0 /* stopped server */, incAB})

			repl0 := store.LookupReplica(key)
			index := repl0.GetLastIndex()

			// Truncate the log at index+1 (log entries < N are removed, so this
			// includes the increment).
			truncArgs := truncateLogArgs(index+1, repl0.GetRangeID())
			if _, err := kv.SendWrapped(ctx, store.TestSender(), truncArgs); err != nil {
				t.Fatal(err)
			}

			stopServer := func(i int) {
				// Stop and restart all the live stores, which guarantees that
				// we won't be in the same term we started with.
				tc.StopServer(i)
				require.NoError(t, tc.RestartServer(i))
				// Disable the snapshot queue on the live stores so that
				// stoppedStore won't get a snapshot as soon as it starts
				// back up.
				tc.GetFirstStoreFromServer(t, i).SetRaftSnapshotQueueActive(false)
			}

			if changeTerm {
				stopServer(otherStore2)
				stopServer(otherStore1)

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
				require.NoError(t, tc.RestartServer(stoppedStore))
				testutils.SucceedsSoon(t, func() error {
					hasLeader := false
					term := uint64(0)
					for i := 0; i < len(tc.Servers); i++ {
						repl := tc.GetFirstStoreFromServer(t, i).LookupReplica(key)
						require.NotNil(t, repl)
						status := repl.RaftStatus()
						if status == nil {
							return errors.New("raft status not initialized")
						}
						if status.RaftState == raftpb.StateLeader {
							hasLeader = true
						}
						if term == 0 {
							term = status.Term
						} else if status.Term != term {
							return errors.Errorf("terms do not agree: %d vs %d", status.Term, term)
						}
						if !hasLeader {
							// If we haven't been able to establish a  leader yet, send a get
							// request to force the issue.
							getReq := getArgs(key)
							_, err := kv.SendWrapped(ctx, tc.GetFirstStoreFromServer(t, i).TestSender(), getReq)
							// It should be fine if we get a NLHE for some reason.
							nlhe := &kvpb.NotLeaseHolderError{}
							if !errors.As(err.GetDetail(), &nlhe) {
								return err.GetDetail()
							}
						}
					}
					if !hasLeader {
						return errors.New("no leader")
					}
					return nil
				})

				// Turn the queues back on and wait for the snapshot to be sent and processed.
				for i := 0; i < len(tc.Servers)-1; i++ {
					tc.GetFirstStoreFromServer(t, i).SetRaftSnapshotQueueActive(true)
					if err := tc.GetFirstStoreFromServer(t, i).ForceRaftSnapshotQueueProcess(); err != nil {
						t.Fatal(err)
					}
				}
			} else { // !changeTerm
				require.NoError(t, tc.RestartServer(stoppedStore))
			}
			tc.WaitForValues(t, key, []int64{incAB, incAB, incAB})

			testutils.SucceedsSoon(t, func() error {
				// Verify that the cached index and term (Replica.mu.last{Index,Term}))
				// on all of the replicas is the same. #18327 fixed an issue where the
				// cached term was left unchanged after applying a snapshot leading to a
				// persistently unavailable range.
				repl0 = tc.GetFirstStoreFromServer(t, otherStore1).LookupReplica(key)
				require.NotNil(t, repl0)
				expectedLastIndex := repl0.GetLastIndex()
				expectedLastTerm := repl0.GetCachedLastTerm()

				verifyIndexAndTerm := func(i int) error {
					repl1 := tc.GetFirstStoreFromServer(t, i).LookupReplica(key)
					require.NotNil(t, repl1)
					if lastIndex := repl1.GetLastIndex(); expectedLastIndex != lastIndex {
						return fmt.Errorf("%d: expected last index %d, but found %d", i, expectedLastIndex, lastIndex)
					}
					if lastTerm := repl1.GetCachedLastTerm(); expectedLastTerm != lastTerm {
						return fmt.Errorf("%d: expected last term %d, but found %d", i, expectedLastTerm, lastTerm)
					}
					return nil
				}
				if err := verifyIndexAndTerm(otherStore2); err != nil {
					return err
				}
				if err := verifyIndexAndTerm(stoppedStore); err != nil {
					return err
				}
				return nil
			})
		})
	}
}

func waitForTruncationForTesting(t *testing.T, r *kvserver.Replica, newFirstIndex kvpb.RaftIndex) {
	testutils.SucceedsSoon(t, func() error {
		// Flush the engine to advance durability, which triggers truncation.
		require.NoError(t, r.Store().TODOEngine().Flush())
		// FirstIndex has changed.
		firstIndex := r.GetFirstIndex()
		if firstIndex != newFirstIndex {
			return errors.Errorf("expected firstIndex == %d, got %d", newFirstIndex, firstIndex)
		}
		return nil
	})
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
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	// Only run the test with Leader leases.
	st := cluster.MakeTestingClusterSettings()
	kvserver.OverrideDefaultLeaseType(ctx, &st.SV, roachpb.LeaseLeader)
	manualClock := hlc.NewHybridManualClock()
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Settings: st,
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						WallClock: manualClock,
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)
	store := tc.GetFirstStoreFromServer(t, 0)

	key := tc.ScratchRangeWithExpirationLease(t)
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
	if _, pErr := kv.SendWrapped(ctx, store.TestSender(), incArgs); pErr != nil {
		t.Fatal(pErr)
	}

	tc.AddVotersOrFatal(t, key, tc.Targets(1, 2)...)
	require.NoError(t, tc.WaitForVoters(key, tc.Targets(1, 2)...))
	tc.WaitForValues(t, key, []int64{incA, incA, incA})

	// We partition the original leader from the other two replicas. This allows
	// us to build up a large uncommitted Raft log on the partitioned node.
	const partStore = 0
	partRepl := tc.GetFirstStoreFromServer(t, partStore).LookupReplica(roachpb.RKey(key))
	partReplDesc, err := partRepl.GetReplicaDescriptor()
	if err != nil {
		t.Fatal(err)
	}
	partReplSender := tc.GetFirstStoreFromServer(t, partStore).TestSender()

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
	log.Infof(ctx, "test: partitioning n1 from n2, n3")
	desc, err := tc.LookupRange(key)
	require.NoError(t, err)
	dropRaftMessagesFrom(t, tc.Servers[1], desc, []roachpb.ReplicaID{1}, nil)
	dropRaftMessagesFrom(t, tc.Servers[2], desc, []roachpb.ReplicaID{1}, nil)
	dropRaftMessagesFrom(t, tc.Servers[0], desc, []roachpb.ReplicaID{2, 3}, nil)

	// Perform a series of writes on the partitioned replica. The writes will
	// not succeed before their context is canceled, but they will be appended
	// to the partitioned replica's Raft log because it is currently the Raft
	// leader.
	log.Infof(ctx, "test: sending writes to partitioned replica")
	g := ctxgroup.WithContext(ctx)
	otherKeys := make([]roachpb.Key, 32)
	otherKeys[0] = key.Next()
	for i := 1; i < 32; i++ {
		otherKeys[i] = otherKeys[i-1].Next()
	}
	for i := range otherKeys {
		// This makes the race detector happy.
		otherKey := otherKeys[i]
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

	// The partition ensures that the Raft leadership changes. The previous leader
	// (1) will step down and one of the followers (2 or 3) will become the new
	// leader. Either one can, and when one does, it'll be able to acquire the
	// lease. We'll first wait for a new leader to be established, have it acquire
	// the lease, and service a write request.
	log.Infof(ctx, "test: establishing new leader/leaseholder and sending writes")

	const otherStoreIdx = 1
	otherStore := tc.GetFirstStoreFromServer(t, otherStoreIdx)
	otherRepl, err := otherStore.GetReplica(desc.RangeID)
	require.NoError(t, err)

	incArgs = incrementArgs(key, incB)
	var newLeaderRepl *kvserver.Replica
	var newLeaderReplSender kv.Sender

	// Have StoreLiveness support for the partitioned store expire.
	manualClock.Increment(store.GetStoreConfig().LeaseExpiration())

	testutils.SucceedsSoon(t, func() error {
		if partRepl.RaftStatus().RaftState == raftpb.StateLeader {
			return errors.New("partitioned replica should have stepped down")
		}
		lead := otherRepl.RaftStatus().Lead
		if lead == raft.None {
			return errors.New("no leader yet")
		}
		if roachpb.ReplicaID(lead) == partReplDesc.ReplicaID {
			return errors.New("partitioned replica is still leader")
		}

		// Remember who the leader/leaseholder is and also send it a write request.
		newLeaderReplDesc, found := desc.GetReplicaDescriptorByID(roachpb.ReplicaID(lead))
		require.True(t, found)
		newLeaderRepl = tc.GetFirstStoreFromServer(t, int(newLeaderReplDesc.NodeID-1)).LookupReplica(roachpb.RKey(key))
		newLeaderReplSender = tc.GetFirstStoreFromServer(t, int(newLeaderReplDesc.NodeID-1)).TestSender()
		_, pErr := kv.SendWrapped(ctx, newLeaderReplSender, incArgs)
		require.Nil(t, pErr)
		return nil
	})

	log.Infof(ctx, "test: waiting for values...")
	tc.WaitForValues(t, key, []int64{incA, incAB, incAB})
	log.Infof(ctx, "test: waiting for values... done")

	index := newLeaderRepl.GetLastIndex()

	// Truncate the log at index+1 (log entries < N are removed, so this
	// includes the increment).
	log.Infof(ctx, "test: truncating log")
	truncArgs := truncateLogArgs(index+1, partRepl.RangeID)
	truncArgs.Key = partRepl.Desc().StartKey.AsRawKey()
	testutils.SucceedsSoon(t, func() error {
		manualClock.Increment(store.GetStoreConfig().LeaseExpiration())
		_, pErr := kv.SendWrappedWith(ctx, newLeaderReplSender, kvpb.Header{RangeID: partRepl.RangeID}, truncArgs)
		if _, ok := pErr.GetDetail().(*kvpb.NotLeaseHolderError); ok {
			return pErr.GoError()
		} else if pErr != nil {
			t.Fatal(pErr)
		}
		return nil
	})
	waitForTruncationForTesting(t, newLeaderRepl, index+1)

	snapsMetric := tc.GetFirstStoreFromServer(t, partStore).Metrics().RangeSnapshotsAppliedByVoters
	snapsBefore := snapsMetric.Count()

	// Remove the partition. Snapshot should follow.
	log.Infof(ctx, "test: removing the partition")
	for _, s := range []int{0, 1, 2} {
		tc.Servers[s].RaftTransport().(*kvserver.RaftTransport).ListenIncomingRaftMessages(tc.Target(s).StoreID, &unreliableRaftHandler{
			rangeID:                    partRepl.RangeID,
			IncomingRaftMessageHandler: tc.GetFirstStoreFromServer(t, s),
			unreliableRaftHandlerFuncs: unreliableRaftHandlerFuncs{
				dropReq: func(req *kvserverpb.RaftMessageRequest) bool {
					// Make sure that even going forward no MsgApp for what we just truncated can
					// make it through. The Raft transport is asynchronous so this is necessary
					// to make the test pass reliably.
					// NB: the Index on the message is the log index that _precedes_ any of the
					// entries in the MsgApp, so filter where msg.Index < index, not <= index.
					return req.Message.Type == raftpb.MsgApp && kvpb.RaftIndex(req.Message.Index) < index
				},
				dropHB:   func(*kvserverpb.RaftHeartbeat) bool { return false },
				dropResp: func(*kvserverpb.RaftMessageResponse) bool { return false },
			},
		})
		store := tc.GetFirstStoreFromServer(t, s)
		store.StoreLivenessTransport().ListenMessages(
			store.StoreID(), store.TestingStoreLivenessSupportManager(),
		)
	}

	// The partitioned replica should catch up after a snapshot.
	testutils.SucceedsSoon(t, func() error {
		snapsAfter := snapsMetric.Count()
		if !(snapsAfter > snapsBefore) {
			return errors.New("expected at least 1 snapshot to catch the partitioned replica up")
		}
		return nil
	})
	tc.WaitForValues(t, key, []int64{incAB, incAB, incAB})

	// Perform another write. The partitioned replica should be able to receive
	// replicated updates.
	incArgs = incrementArgs(key, incC)
	if _, pErr := kv.SendWrapped(ctx, tc.Servers[0].DistSenderI().(kv.Sender), incArgs); pErr != nil {
		t.Fatal(pErr)
	}
	tc.WaitForValues(t, key, []int64{incABC, incABC, incABC})
}

// TestRequestsOnLaggingReplica tests that requests sent to a replica that's
// behind in log application don't block. The test indirectly verifies that a
// replica that's not the leader does not attempt to acquire a lease and, thus,
// does not block until it figures out that it cannot, in fact, take the lease.
//
// This test relies on follower replicas refusing to forward lease acquisition
// requests to the leader, thereby refusing to acquire a lease. The point of
// this behavior is to prevent replicas that are behind from trying to acquire
// the lease and then blocking traffic for a long time until they find out
// whether they successfully took the lease or not.
func TestRequestsOnLaggingReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	testutils.RunTrueAndFalse(t, "symmetric", func(t *testing.T, symmetric bool) {
		st := cluster.MakeTestingClusterSettings()
		kvserver.OverrideDefaultLeaseType(ctx, &st.SV, roachpb.LeaseLeader)
		// TODO(arul): Once https://github.com/cockroachdb/cockroach/issues/118435 we
		// can remove this. Leader leases require us to reject lease requests on
		// replicas that are not the leader.
		kvserver.RejectLeaseOnLeaderUnknown.Override(ctx, &st.SV, true)

		clusterArgs := base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Settings: st,
				// Reduce the election timeout some to speed up the test.
				RaftConfig: base.RaftConfig{RaftElectionTimeoutTicks: 10},
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						// We eliminate clock offsets in order to eliminate the stasis
						// period of leases, in order to speed up the test.
						MaxOffset: time.Nanosecond,
					},
				},
			},
		}

		tc := testcluster.StartTestCluster(t, 3, clusterArgs)
		defer tc.Stopper().Stop(ctx)

		_, rngDesc, err := tc.Servers[0].ScratchRangeEx()
		require.NoError(t, err)
		key := rngDesc.StartKey.AsRawKey()
		// Add replicas on all the stores.
		rngDesc = tc.AddVotersOrFatal(t, rngDesc.StartKey.AsRawKey(), tc.Target(1), tc.Target(2))

		{
			// Write a value so that the respective key is present in all stores and
			// we can increment it again later.
			_, err := tc.Server(0).DB().Inc(ctx, key, 1)
			require.NoError(t, err)
			log.Infof(ctx, "test: waiting for initial values...")
			tc.WaitForValues(t, key, []int64{1, 1, 1})
			log.Infof(ctx, "test: waiting for initial values... done")
		}

		// Partition the original leader from its followers. We do this by
		// installing unreliableRaftHandler listeners on all three Stores. The
		// handler on the partitioned store filters out all messages while the
		// handler on the other two stores only filters out messages from the
		// partitioned store. The configuration looks like:
		//
		//     [symmetric=false]      [symmetric=true]
		//           [0]                     [0]
		//          ^  ^                    x   x
		//         /    \                  /     \
		//        x      x                x       x
		//      [1]<---->[2]            [1]<----->[2]
		//
		log.Infof(ctx, "test: partitioning node")
		const partitionNodeIdx = 0
		partitionStore := tc.GetFirstStoreFromServer(t, partitionNodeIdx)
		partitionedStoreSender := partitionStore.TestSender()
		const otherStoreIdx = 1
		otherStore := tc.GetFirstStoreFromServer(t, otherStoreIdx)
		otherRepl, err := otherStore.GetReplica(rngDesc.RangeID)
		require.NoError(t, err)

		dropRaftMessagesFrom(t, tc.Servers[1], rngDesc, []roachpb.ReplicaID{1}, nil)
		dropRaftMessagesFrom(t, tc.Servers[2], rngDesc, []roachpb.ReplicaID{1}, nil)
		if symmetric {
			dropRaftMessagesFrom(t, tc.Servers[0], rngDesc, []roachpb.ReplicaID{2, 3}, nil)
		}

		leaderReplicaID := waitForPartitionedLeaderStepDownAndNewLeaderToStepUp(
			t, tc, rngDesc, partitionNodeIdx, otherStoreIdx,
		)
		log.Infof(ctx, "test: the leader is replica ID %d", leaderReplicaID)
		if leaderReplicaID != 2 && leaderReplicaID != 3 {
			t.Fatalf("expected leader to be 1 or 2, was: %d", leaderReplicaID)
		}
		leaderNodeIdx := int(leaderReplicaID - 1)
		leaderNode := tc.Server(leaderNodeIdx)
		leaderStore, err := leaderNode.GetStores().(*kvserver.Stores).GetStore(leaderNode.GetFirstStoreID())
		require.NoError(t, err)

		// Now that the leadership has transferred, the lease should've expired too.
		log.Infof(ctx, "test: ensuring the lease expired")
		partitionedReplica, err := partitionStore.GetReplica(rngDesc.RangeID)
		require.NoError(t, err)
		status := partitionedReplica.CurrentLeaseStatus(ctx)
		// We're partitioned, so we won't know about a new lease elsewhere, even if
		// it exists.
		require.True(t,
			status.Lease.OwnedBy(partitionStore.StoreID()), "someone else got the lease: %s", status)
		require.True(t, status.IsExpired())
		log.Infof(ctx, "test: lease expired")

		{
			// Write something to generate some Raft log entries and then truncate the
			// log.
			log.Infof(ctx, "test: incrementing")
			incArgs := incrementArgs(key, 1)
			sender := leaderStore.TestSender()
			_, pErr := kv.SendWrapped(ctx, sender, incArgs)
			require.Nil(t, pErr)
		}

		tc.WaitForValues(t, key, []int64{1, 2, 2})
		index := otherRepl.GetLastIndex()

		// Truncate the log at index+1 (log entries < N are removed, so this includes
		// the increment). This means that the partitioned replica will need a
		// snapshot to catch up.
		log.Infof(ctx, "test: truncating log...")
		truncArgs := &kvpb.TruncateLogRequest{
			RequestHeader: kvpb.RequestHeader{
				Key: key,
			},
			Index:   index,
			RangeID: rngDesc.RangeID,
		}
		{
			_, pErr := kv.SendWrapped(ctx, leaderStore.TestSender(), truncArgs)
			require.NoError(t, pErr.GoError())
		}

		timeoutCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		// Before resolving the partition, send a request to the partitioned
		// replica. Ensure that it doesn't block, but instead returns a
		// NotLeaseholderError (with an empty leaseholder).
		//
		// NB: This relies on RejectLeaseOnLeaderUnknown being set to true.
		log.Infof(ctx, "test: sending request to partitioned replica")
		getRequest := getArgs(key)
		_, pErr := kv.SendWrapped(timeoutCtx, partitionedStoreSender, getRequest)
		require.Error(t, pErr.GoError(), "unexpected success")
		nlhe := &kvpb.NotLeaseHolderError{}
		require.ErrorAs(t, pErr.GetDetail(), &nlhe, pErr)

		if symmetric {
			// In symmetric=true, we expect that the partitioned replica to not know
			// about the leader. As a result, it returns a NotLeaseHolderError without
			// a speculative lease.
			require.True(t, nlhe.Lease.Empty(), "expected empty lease, got %v", nlhe.Lease)
		} else {
			require.False(t, nlhe.Lease.Empty())
			require.Equal(t, leaderReplicaID, nlhe.Lease.Replica.ReplicaID)
		}

		// Resolve the partition, but continue blocking snapshots destined for the
		// previously-partitioned replica. The point of blocking the snapshots is to
		// prevent the respective replica from catching up and becoming eligible to
		// become the leader/leaseholder. The point of resolving the partition is to
		// allow the replica in question to figure out who the new leader is.
		log.Infof(ctx, "test: removing partition")
		slowSnapHandler := &slowSnapRaftHandler{
			rangeID:                    rngDesc.RangeID,
			waitCh:                     make(chan struct{}),
			IncomingRaftMessageHandler: partitionStore,
		}
		defer slowSnapHandler.unblock()
		partitionStore.Transport().ListenIncomingRaftMessages(partitionStore.Ident.StoreID, slowSnapHandler)
		if symmetric {
			// Let StoreLiveness heartbeats go through. We override the RaftTransport
			// above, so we don't have to worry about Raft messages other than
			// snapshots being dropped.
			partitionStore.StoreLivenessTransport().ListenMessages(
				partitionStore.Ident.StoreID, partitionStore.TestingStoreLivenessSupportManager(),
			)
		}
		// Remove the unreliable transport from the other stores, so that messages
		// sent by the partitioned store can reach them.
		for _, i := range []int{0, 1, 2} {
			if i == partitionNodeIdx {
				// We've handled the partitioned store above.
				continue
			}
			store := tc.GetFirstStoreFromServer(t, i)
			store.Transport().ListenIncomingRaftMessages(store.StoreID(), store)
			store.StoreLivenessTransport().ListenMessages(
				store.StoreID(), store.TestingStoreLivenessSupportManager(),
			)
		}

		// Now we're going to send a request to the behind replica, and we expect it
		// to not block; we expect a redirection to the leader (once it's learned of
		// the leader).
		log.Infof(ctx, "test: sending request after partition has healed")
		for {
			getRequest := getArgs(key)
			_, pErr := kv.SendWrapped(timeoutCtx, partitionedStoreSender, getRequest)
			require.Error(t, pErr.GoError(), "unexpected success")
			nlhe := &kvpb.NotLeaseHolderError{}
			require.ErrorAs(t, pErr.GetDetail(), &nlhe, "%v", pErr)
			// When the old leader is partitioned, it will step down due to
			// CheckQuorum. Immediately after the partition is healed, the replica
			// will not know who the new leader is, so it will return a
			// NotLeaseholderError with an empty lease. This is expected and better
			// than blocking. Still, we have the test retry to make sure that the new
			// leader is eventually discovered and that after that point, the
			// NotLeaseholderErrors start including a lease which points to the new
			// leader.
			if nlhe.Lease.Empty() {
				t.Logf("got %s, retrying", nlhe)
				continue
			}
			require.Equal(t, leaderReplicaID, nlhe.Lease.Replica.ReplicaID)
			break
		}
	})
}

// TestRequestsOnFollowerWithNonLiveLeaseholder tests the availability of a
// range that has an expired epoch-based lease and a live Raft leader that is
// unable to heartbeat its liveness record. Such a range should recover once
// Raft leadership moves off the partitioned Raft leader to one of the followers
// that can reach node liveness.
//
// This test relies on follower replicas campaigning for Raft leadership in
// certain cases when refusing to forward lease acquisition requests to the
// leader. In these cases where they determine that the leader is non-live
// according to node liveness, they will attempt to steal Raft leadership and,
// if successful, will be able to perform future lease acquisition attempts.
func TestRequestsOnFollowerWithNonLiveLeaseholder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	var installPartition int32
	partitionFilter := func(_ context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
		if atomic.LoadInt32(&installPartition) == 0 {
			return nil
		}
		if ba.GatewayNodeID == 1 && ba.Replica.NodeID == 4 {
			return kvpb.NewError(context.Canceled)
		}
		return nil
	}

	st := cluster.MakeTestingClusterSettings()
	// This test is specifically designed for epoch based leases.
	kvserver.OverrideDefaultLeaseType(ctx, &st.SV, roachpb.LeaseEpoch)

	manualClock := hlc.NewHybridManualClock()
	clusterArgs := base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings: st,
			RaftConfig: base.RaftConfig{
				// Reduce the election timeout some to speed up the test.
				RaftElectionTimeoutTicks: 10,
				// This should work with PreVote+CheckQuorum too.
				RaftEnableCheckQuorum: true,
			},
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					WallClock: manualClock,
				},
				Store: &kvserver.StoreTestingKnobs{
					// We eliminate clock offsets in order to eliminate the stasis period
					// of leases, in order to speed up the test.
					MaxOffset:            time.Nanosecond,
					TestingRequestFilter: partitionFilter,
				},
			},
		},
	}

	tc := testcluster.StartTestCluster(t, 4, clusterArgs)
	defer tc.Stopper().Stop(ctx)

	{
		// Move the liveness range to node 4.
		desc := tc.LookupRangeOrFatal(t, keys.NodeLivenessPrefix)
		tc.RebalanceVoterOrFatal(ctx, t, desc.StartKey.AsRawKey(), tc.Target(0), tc.Target(3))
	}

	// Create a new range.
	_, rngDesc, err := tc.Servers[0].ScratchRangeEx()
	require.NoError(t, err)
	key := rngDesc.StartKey.AsRawKey()
	// Add replicas on all the stores.
	tc.AddVotersOrFatal(t, rngDesc.StartKey.AsRawKey(), tc.Target(1), tc.Target(2))

	// Store 0 holds the lease.
	store0 := tc.GetFirstStoreFromServer(t, 0)
	store0Repl, err := store0.GetReplica(rngDesc.RangeID)
	require.NoError(t, err)
	leaseStatus := store0Repl.CurrentLeaseStatus(ctx)
	require.True(t, leaseStatus.OwnedBy(store0.StoreID()))

	{
		// Write a value so that the respective key is present in all stores and we
		// can increment it again later.
		_, err := tc.Server(0).DB().Inc(ctx, key, 1)
		require.NoError(t, err)
		log.Infof(ctx, "test: waiting for initial values...")
		tc.WaitForValues(t, key, []int64{1, 1, 1, 0})
		log.Infof(ctx, "test: waiting for initial values... done")
	}

	// Begin dropping all node liveness heartbeats from the original raft leader
	// while allowing the leader to maintain Raft leadership and otherwise behave
	// normally. This mimics cases where the raft leader is partitioned away from
	// the liveness range but can otherwise reach its followers. In these cases,
	// it is still possible that the followers can reach the liveness range and
	// see that the leader becomes non-live. For example, the configuration could
	// look like:
	//
	//          [0]       raft leader
	//           ^
	//          / \
	//         /   \
	//        v     v
	//      [1]<--->[2]   raft followers
	//        ^     ^
	//         \   /
	//          \ /
	//           v
	//          [3]       liveness range
	//
	log.Infof(ctx, "test: partitioning node")
	atomic.StoreInt32(&installPartition, 1)

	// Wait until the lease expires.
	log.Infof(ctx, "test: waiting for lease expiration on r%d", store0Repl.RangeID)
	testutils.SucceedsSoon(t, func() error {
		dur, _ := store0.GetStoreConfig().NodeLivenessDurations()
		manualClock.Increment(dur.Nanoseconds())
		leaseStatus = store0Repl.CurrentLeaseStatus(ctx)
		// If we failed to pin the lease, it likely won't ever expire due to the particular
		// partition we've set up. Bail early instead of wasting 45s.
		require.True(t, leaseStatus.Lease.OwnedBy(store0.StoreID()), "failed to pin lease")

		// Lease is on s1, and it should be invalid now. The only reason there's a
		// retry loop is that there could be a race where we bump the clock while a
		// heartbeat is inflight (and which picks up the new expiration).
		if leaseStatus.IsValid() {
			return errors.Errorf("lease still valid: %+v", leaseStatus)
		}
		return nil
	})
	log.Infof(ctx, "test: lease expired")

	{
		tBegin := timeutil.Now()
		// Increment the initial value again, which requires range availability. To
		// get there, the request will need to trigger a lease request on a follower
		// replica, which will call a Raft election, acquire Raft leadership, then
		// acquire the range lease.
		log.Infof(ctx, "test: waiting for new lease...")
		_, err := tc.Server(0).DB().Inc(ctx, key, 1)
		require.NoError(t, err)
		tc.WaitForValues(t, key, []int64{2, 2, 2, 0})
		log.Infof(ctx, "test: waiting for new lease... done [%.2fs]", timeutil.Since(tBegin).Seconds())
	}

	// Store 0 no longer holds the lease.
	leaseStatus = store0Repl.CurrentLeaseStatus(ctx)
	require.False(t, leaseStatus.OwnedBy(store0.StoreID()))
}

type fakeSnapshotStream struct {
	nextReq *kvserverpb.SnapshotRequest
	nextErr error
}

// Recv implements the SnapshotResponseStream interface.
func (c fakeSnapshotStream) Recv() (*kvserverpb.SnapshotRequest, error) {
	return c.nextReq, c.nextErr
}

// Send implements the SnapshotResponseStream interface.
func (c fakeSnapshotStream) Send(request *kvserverpb.SnapshotResponse) error {
	return nil
}

type snapshotTestSignals struct {
	// Receiver-side wait channels.
	receiveErrCh        chan error
	batchReceiveReadyCh chan struct{}

	// Sender-side wait channels.
	svrContextDone        <-chan struct{}
	receiveStartedCh      chan struct{}
	batchReceiveStartedCh chan struct{}
	receiverDoneCh        chan struct{}
}

// TestReceiveSnapshotLogging tests that a snapshot receiver properly captures
// the collected tracing spans in the last response, or logs the span if the
// context is cancelled from the client side.
func TestReceiveSnapshotLogging(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const senderNodeIdx = 0
	const receiverNodeIdx = 1
	const dummyEventMsg = "test receive snapshot logging - dummy event"

	setupTest := func(t *testing.T) (context.Context, *testcluster.TestCluster, *roachpb.RangeDescriptor, *snapshotTestSignals) {
		ctx := context.Background()

		signals := &snapshotTestSignals{
			receiveErrCh:        make(chan error),
			batchReceiveReadyCh: make(chan struct{}),

			svrContextDone:        nil,
			receiveStartedCh:      make(chan struct{}),
			batchReceiveStartedCh: make(chan struct{}),
			receiverDoneCh:        make(chan struct{}, 1),
		}

		tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						DisableRaftSnapshotQueue: true,
					},
				},
			},
			ReplicationMode: base.ReplicationManual,
			ServerArgsPerNode: map[int]base.TestServerArgs{
				receiverNodeIdx: {
					Knobs: base.TestingKnobs{
						Store: &kvserver.StoreTestingKnobs{
							DisableRaftSnapshotQueue: true,
							ThrottleEmptySnapshots:   true,
							ReceiveSnapshot: func(ctx context.Context, _ *kvserverpb.SnapshotRequest_Header) error {
								t.Logf("incoming snapshot on n2")
								log.Event(ctx, dummyEventMsg)
								signals.svrContextDone = ctx.Done()
								close(signals.receiveStartedCh)
								return <-signals.receiveErrCh
							},
							BeforeRecvAcceptedSnapshot: func() {
								t.Logf("receiving on n2")
								signals.batchReceiveStartedCh <- struct{}{}
								<-signals.batchReceiveReadyCh
							},
							HandleSnapshotDone: func() {
								t.Logf("receiver on n2 completed")
								signals.receiverDoneCh <- struct{}{}
							},
						},
					},
				},
			},
		})

		_, scratchRange, err := tc.Servers[0].ScratchRangeEx()
		require.NoError(t, err)

		return ctx, tc, &scratchRange, signals
	}

	snapshotAndValidateLogs := func(t *testing.T, ctx context.Context, tc *testcluster.TestCluster, rngDesc *roachpb.RangeDescriptor, signals *snapshotTestSignals, expectTraceOnSender bool) error {
		t.Helper()

		repl := tc.GetFirstStoreFromServer(t, senderNodeIdx).LookupReplica(rngDesc.StartKey)
		chgs := kvpb.MakeReplicationChanges(roachpb.ADD_VOTER, tc.Target(receiverNodeIdx))

		testStartTs := timeutil.Now()
		_, pErr := repl.ChangeReplicas(ctx, rngDesc, kvserverpb.ReasonRangeUnderReplicated, "", chgs)

		// When ready, flush logs and check messages from store_raft.go since
		// call to repl.ChangeReplicas(..).
		<-signals.receiverDoneCh
		log.FlushFiles()
		entries, err := log.FetchEntriesFromFiles(testStartTs.UnixNano(),
			math.MaxInt64, 100, regexp.MustCompile(`store_raft\.go`), log.WithMarkedSensitiveData)
		require.NoError(t, err)

		errRegexp, err := regexp.Compile(`incoming snapshot stream failed with error`)
		require.NoError(t, err)
		foundEntry := false
		var entry logpb.Entry
		for _, entry = range entries {
			if errRegexp.MatchString(entry.Message) {
				foundEntry = true
				break
			}
		}
		expectTraceOnReceiver := !expectTraceOnSender
		require.Equal(t, expectTraceOnReceiver, foundEntry)
		if expectTraceOnReceiver {
			require.Contains(t, entry.Message, dummyEventMsg)
		}

		// Check that receiver traces were imported in sender's context on success.
		clientTraces := tracing.SpanFromContext(ctx).GetConfiguredRecording()
		_, receiverTraceFound := clientTraces.FindLogMessage(dummyEventMsg)
		require.Equal(t, expectTraceOnSender, receiverTraceFound)

		return pErr
	}

	t.Run("cancel on header", func(t *testing.T) {
		ctx, tc, scratchRange, signals := setupTest(t)
		defer tc.Stopper().Stop(ctx)

		ctx, sp := tracing.EnsureChildSpan(ctx, tc.GetFirstStoreFromServer(t, senderNodeIdx).GetStoreConfig().Tracer(),
			t.Name(), tracing.WithRecording(tracingpb.RecordingVerbose))
		defer sp.Finish()

		ctx, cancel := context.WithCancel(ctx)
		go func() {
			<-signals.receiveStartedCh
			cancel()
			<-signals.svrContextDone
			time.Sleep(10 * time.Millisecond)
			signals.receiveErrCh <- errors.Errorf("header is bad")
		}()
		err := snapshotAndValidateLogs(t, ctx, tc, scratchRange, signals, false /* expectTraceOnSender */)
		require.Error(t, err)
	})
	t.Run("cancel during receive", func(t *testing.T) {
		ctx, tc, scratchRange, signals := setupTest(t)
		defer tc.Stopper().Stop(ctx)

		ctx, sp := tracing.EnsureChildSpan(ctx, tc.GetFirstStoreFromServer(t, senderNodeIdx).GetStoreConfig().Tracer(),
			t.Name(), tracing.WithRecording(tracingpb.RecordingVerbose))
		defer sp.Finish()

		ctx, cancel := context.WithCancel(ctx)
		close(signals.receiveErrCh)
		go func() {
			<-signals.receiveStartedCh
			<-signals.batchReceiveStartedCh
			cancel()
			<-signals.svrContextDone
			time.Sleep(10 * time.Millisecond)
			close(signals.batchReceiveReadyCh)
		}()
		err := snapshotAndValidateLogs(t, ctx, tc, scratchRange, signals, false /* expectTraceOnSender */)
		require.Error(t, err)
	})
	t.Run("successful send", func(t *testing.T) {
		ctx, tc, scratchRange, signals := setupTest(t)
		defer tc.Stopper().Stop(ctx)

		ctx, sp := tracing.EnsureChildSpan(ctx, tc.GetFirstStoreFromServer(t, senderNodeIdx).GetStoreConfig().Tracer(),
			t.Name(), tracing.WithRecording(tracingpb.RecordingVerbose))
		defer sp.Finish()

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		close(signals.receiveErrCh)
		close(signals.batchReceiveReadyCh)
		go func() {
			<-signals.receiveStartedCh
			<-signals.batchReceiveStartedCh
		}()
		err := snapshotAndValidateLogs(t, ctx, tc, scratchRange, signals, true /* expectTraceOnSender */)
		require.NoError(t, err)
	})
}

// TestFailedSnapshotFillsReservation tests that failing to finish applying an
// incoming snapshot still cleans up the outstanding reservation that was made.
func TestFailedSnapshotFillsReservation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)
	store := tc.GetFirstStoreFromServer(t, 0)
	store1 := tc.GetFirstStoreFromServer(t, 1)

	key := tc.ScratchRange(t)

	rep := store.LookupReplica(roachpb.RKey(key))
	require.NotNil(t, rep)
	repDesc, err := rep.GetReplicaDescriptor()
	require.NoError(t, err)
	desc := protoutil.Clone(rep.Desc()).(*roachpb.RangeDescriptor)
	desc.AddReplica(2, 2, roachpb.LEARNER)
	rep2Desc, found := desc.GetReplicaDescriptor(2)
	require.True(t, found)
	header := kvserverpb.SnapshotRequest_Header{
		RangeSize: 100,
		State:     kvserverpb.ReplicaState{Desc: desc},
		RaftMessageRequest: kvserverpb.RaftMessageRequest{
			RangeID:     rep.RangeID,
			FromReplica: repDesc,
			ToReplica:   rep2Desc,
		},
	}
	header.RaftMessageRequest.Message.Snapshot = &raftpb.Snapshot{
		Data: uuid.UUID{}.GetBytes(),
	}
	// Cause this stream to return an error as soon as we ask it for something.
	// This injects an error into HandleSnapshotStream when we try to send the
	// "snapshot accepted" message.
	expectedErr := errors.Errorf("")
	stream := fakeSnapshotStream{nil, expectedErr}
	if err := store1.HandleSnapshot(ctx, &header, stream); !errors.Is(err, expectedErr) {
		t.Fatalf("expected error %s, but found %v", expectedErr, err)
	}
	if n := store1.ReservationCount(); n != 0 {
		t.Fatalf("expected 0 reservations, but found %d", n)
	}
}

// TestConcurrentRaftSnapshots tests that snapshots still work correctly when
// Raft requests multiple snapshots at the same time. This situation occurs when
// two replicas need snapshots at the same time.
func TestConcurrentRaftSnapshots(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numServers int = 5
	stickyServerArgs := make(map[int]base.TestServerArgs)
	for i := 0; i < numServers; i++ {
		stickyServerArgs[i] = base.TestServerArgs{
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:    true,
					StickyVFSID: strconv.FormatInt(int64(i), 10),
				},
			},
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					StickyVFSRegistry: fs.NewStickyRegistry(),
				},
			},
		}
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, numServers,
		base.TestClusterArgs{
			ReplicationMode:   base.ReplicationManual,
			ServerArgsPerNode: stickyServerArgs,
		})
	defer tc.Stopper().Stop(ctx)
	store := tc.GetFirstStoreFromServer(t, 0)

	key := []byte("a")
	tc.SplitRangeOrFatal(t, key)

	incA := int64(5)
	incB := int64(7)
	incAB := incA + incB

	repl := store.LookupReplica(key)
	require.NotNil(t, repl)

	// Set up a key to replicate across the cluster. We're going to modify this
	// key and truncate the raft logs from that command after killing one of the
	// nodes to check that it gets the new value after it comes up.
	incArgs := incrementArgs(key, incA)
	if _, err := kv.SendWrapped(ctx, store.TestSender(), incArgs); err != nil {
		t.Fatal(err)
	}

	tc.AddVotersOrFatal(t, key, tc.Targets(1, 2, 3, 4)...)
	tc.WaitForValues(t, key, []int64{incA, incA, incA, incA, incA})

	// Now kill stores 1 + 2, increment the key on the other stores and
	// truncate their logs to make sure that when store 1 + 2 comes back up
	// they will require a snapshot from Raft.
	tc.StopServer(1)
	tc.StopServer(2)

	incArgs = incrementArgs(key, incB)
	if _, err := kv.SendWrapped(ctx, store.TestSender(), incArgs); err != nil {
		t.Fatal(err)
	}

	tc.WaitForValues(t, key, []int64{incAB, 0 /* stopped */, 0 /* stopped */, incAB, incAB})

	index := repl.GetLastIndex()

	// Truncate the log at index+1 (log entries < N are removed, so this
	// includes the increment).
	truncArgs := truncateLogArgs(index+1, repl.GetRangeID())
	if _, err := kv.SendWrapped(ctx, store.TestSender(), truncArgs); err != nil {
		t.Fatal(err)
	}

	require.NoError(t, tc.RestartServer(1))
	require.NoError(t, tc.RestartServer(2))

	tc.WaitForValues(t, key, []int64{incAB, incAB, incAB, incAB, incAB})
}

// Test a scenario where a replica is removed from a down node, the associated
// range is split, the node restarts and we try to replicate the RHS of the
// split range back to the restarted node.
func TestReplicateAfterRemoveAndSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numServers int = 3
	stickyServerArgs := make(map[int]base.TestServerArgs)
	for i := 0; i < numServers; i++ {
		stickyServerArgs[i] = base.TestServerArgs{
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:    true,
					StickyVFSID: strconv.FormatInt(int64(i), 10),
				},
			},
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					StickyVFSRegistry: fs.NewStickyRegistry(),
				},
				Store: &kvserver.StoreTestingKnobs{
					// Disable the replica GC queue so that it doesn't accidentally pick up the
					// removed replica and GC it. We'll explicitly enable it later in the test.
					DisableReplicaGCQueue: true,
					// Disable eager replica removal so we can manually remove the replica.
					DisableEagerReplicaRemoval: true,
				},
			},
		}
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, numServers,
		base.TestClusterArgs{
			ReplicationMode:   base.ReplicationManual,
			ServerArgsPerNode: stickyServerArgs,
		})
	defer tc.Stopper().Stop(ctx)
	store := tc.GetFirstStoreFromServer(t, 0)

	// Split the range.
	splitKey := []byte("m")
	repl := store.LookupReplica(splitKey)
	require.NotNil(t, repl)
	tc.AddVotersOrFatal(t, repl.Desc().StartKey.AsRawKey(), tc.Targets(1, 2)...)

	// Kill store 2.
	tc.StopServer(2)

	tc.RemoveVotersOrFatal(t, repl.Desc().StartKey.AsRawKey(), tc.Target(2))

	tc.SplitRangeOrFatal(t, splitKey)

	// Restart store 2.
	tc.AddAndStartServer(t, stickyServerArgs[2])

	// Try to up-replicate the RHS of the split to store 2.
	// Don't use tc.AddVoter because we expect a retriable error and want it
	// returned to us.
	if _, err := tc.Servers[0].DB().AdminChangeReplicas(
		ctx, splitKey, tc.LookupRangeOrFatal(t, splitKey),
		kvpb.MakeReplicationChanges(roachpb.ADD_VOTER, tc.Target(3)),
	); !kvserver.IsRetriableReplicationChangeError(err) {
		t.Fatalf("unexpected error %v", err)
	}

	// Enable the replica GC queue so that the next attempt to replicate the RHS
	// to store 2 will cause the obsolete replica to be GC'd allowing a
	// subsequent replication to succeed.
	tc.GetFirstStoreFromServer(t, 3).SetReplicaGCQueueActive(true)
	tc.AddVotersOrFatal(t, splitKey, tc.Target(3))
}

// Test that when a Raft group is not able to establish a quorum, its Raft log
// does not grow without bound. It tests two different scenarios where this used
// to be possible (see #27772):
//  1. The leader proposes a command and cannot establish a quorum. The leader
//     continually re-proposes the command.
//  2. The follower proposes a command and forwards it to the leader, who cannot
//     establish a quorum. The follower continually re-proposes and forwards the
//     command to the leader.
func TestLogGrowthWhenRefreshingPendingCommands(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "lease-type", roachpb.TestingAllLeaseTypes(), func(t *testing.T, leaseType roachpb.LeaseType) {
		// Test proposing on leader and proposing on follower. Neither should result
		// in unbounded raft log growth.
		testutils.RunTrueAndFalse(t, "proposeOnFollower", func(t *testing.T, proposeOnFollower bool) {
			ctx := context.Background()
			settings := cluster.MakeTestingClusterSettings()
			kvserver.OverrideDefaultLeaseType(ctx, &settings.SV, leaseType)

			raftConfig := base.RaftConfig{
				// Drop the raft tick interval so the Raft group is ticked more.
				RaftTickInterval: 10 * time.Millisecond,
				// Reduce the max uncommitted entry size.
				RaftMaxUncommittedEntriesSize: 64 << 10, // 64 KB
				// RaftProposalQuota cannot exceed RaftMaxUncommittedEntriesSize.
				RaftProposalQuota: int64(64 << 10),
				// RaftMaxInflightMsgs * RaftMaxSizePerMsg cannot exceed RaftProposalQuota.
				RaftMaxInflightMsgs: 16,
				RaftMaxSizePerMsg:   1 << 10, // 1 KB
			}
			// Suppress timeout-based elections to avoid leadership changes in ways this
			// test doesn't expect. For leader leases, fortification itself provides us
			// this guarantee.
			if leaseType != roachpb.LeaseLeader {
				raftConfig.RaftElectionTimeoutTicks = 1000000
			}

			const numServers int = 5
			stickyServerArgs := make(map[int]base.TestServerArgs)
			for i := 0; i < numServers; i++ {
				stickyServerArgs[i] = base.TestServerArgs{
					Settings: settings,
					StoreSpecs: []base.StoreSpec{
						{
							InMemory:    true,
							StickyVFSID: strconv.FormatInt(int64(i), 10),
						},
					},
					RaftConfig: raftConfig,
					Knobs: base.TestingKnobs{
						Server: &server.TestingKnobs{
							StickyVFSRegistry: fs.NewStickyRegistry(),
						},
						Store: &kvserver.StoreTestingKnobs{
							// Disable leader transfers during leaseholder changes so that we
							// can easily create leader-not-leaseholder scenarios.
							DisableLeaderFollowsLeaseholder: true,
							// Refresh pending commands on every Raft group tick instead of
							// every RaftReproposalTimeoutTicks.
							RefreshReasonTicksPeriod: 1,
						},
					},
				}
			}

			tc := testcluster.StartTestCluster(t, numServers,
				base.TestClusterArgs{
					ReplicationMode:   base.ReplicationManual,
					ServerArgsPerNode: stickyServerArgs,
				})
			defer tc.Stopper().Stop(ctx)
			store := tc.GetFirstStoreFromServer(t, 0)

			key := []byte("a")
			tc.SplitRangeOrFatal(t, key)
			tc.AddVotersOrFatal(t, key, tc.Targets(1, 2, 3, 4)...)

			// Raft leadership is kept on node 0.
			leaderRepl := store.LookupReplica(key)
			require.NotNil(t, leaderRepl)

			// Put some data in the range so we'll have something to test for.
			incArgs := incrementArgs(key, 5)
			if _, err := kv.SendWrapped(ctx, store.TestSender(), incArgs); err != nil {
				t.Fatal(err)
			}

			// Wait for all nodes to catch up.
			tc.WaitForValues(t, key, []int64{5, 5, 5, 5, 5})

			// Determine which node to propose on. Transfer lease to that node.
			propIdx := 0
			if proposeOnFollower {
				propIdx = 1
			}
			propNode := tc.GetFirstStoreFromServer(t, propIdx).TestSender()
			tc.TransferRangeLeaseOrFatal(t, *leaderRepl.Desc(), tc.Target(propIdx))
			// The test disables leader follows leaseholder, so we will never be able
			// to upgrade to a leader lease.
			if !(leaseType == roachpb.LeaseLeader && proposeOnFollower) {
				tc.MaybeWaitForLeaseUpgrade(ctx, t, *leaderRepl.Desc())
			}
			testutils.SucceedsSoon(t, func() error {
				// Lease transfers may not be immediately observed by the new
				// leaseholder. Wait until the new leaseholder is aware.
				repl, err := tc.GetFirstStoreFromServer(t, propIdx).GetReplica(leaderRepl.RangeID)
				require.NoError(t, err)
				repDesc, err := repl.GetReplicaDescriptor()
				require.NoError(t, err)
				if lease, _ := repl.GetLease(); !lease.Replica.Equal(repDesc) {
					return errors.Errorf("lease not transferred yet; found %v", lease)
				}
				return nil
			})

			// Stop enough nodes to prevent a quorum.
			for i := 2; i < len(tc.Servers); i++ {
				tc.StopServer(i)
			}

			// Determine the current raft log size.
			initLogSize, _ := leaderRepl.GetRaftLogSize()

			// While a majority nodes are down, write some data.
			putRes := make(chan *kvpb.Error)
			go func() {
				putArgs := putArgs([]byte("b"), make([]byte, raftConfig.RaftMaxUncommittedEntriesSize/8))
				_, err := kv.SendWrapped(ctx, propNode, putArgs)
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
					if status == nil || status.RaftState != raftpb.StateLeader {
						t.Fatalf("raft leader should be node 0, but got status %+v", status)
					}

					// Check the raft log size. We allow GetRaftLogSize to grow up
					// to twice RaftMaxUncommittedEntriesSize because its total
					// includes a little more state (the roachpb.Value checksum,
					// etc.). The important thing here is that the log doesn't grow
					// forever.
					logSizeLimit := int64(2 * raftConfig.RaftMaxUncommittedEntriesSize)
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
			require.NoError(t, tc.RestartServer(2))

			// The write should now succeed.
			err := <-putRes
			require.NoError(t, err.GoError())
		})
	})
}

// TestStoreRangeUpReplicate verifies that the replication queue will notice
// under-replicated ranges and replicate them.
func TestStoreRangeUpReplicate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer kvserver.SetMockAddSSTable()()

	const numServers = 3
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, numServers,
		base.TestClusterArgs{
			// Set to auto since we want the replication queue on.
			ReplicationMode: base.ReplicationAuto,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{},
				},
			},
		})

	defer tc.Stopper().Stop(ctx)

	// Once we know our peers, trigger a scan.
	store := tc.GetFirstStoreFromServer(t, 0)
	if err := store.ForceReplicationScanAndProcess(); err != nil {
		t.Fatal(err)
	}

	// Wait until all under-replicated ranges are up-replicated to all nodes.
	var replicaCount int64
	testutils.SucceedsSoon(t, func() error {
		var replicaCounts [numServers]int64
		for i := range tc.Servers {
			var err error
			tc.GetFirstStoreFromServer(t, i).VisitReplicas(func(r *kvserver.Replica) bool {
				replicaCounts[i]++
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
			if n := tc.GetFirstStoreFromServer(t, i).ReservationCount(); n != 0 {
				return errors.Errorf("expected 0 reservations, but found %d", n)
			}
		}
		replicaCount = replicaCounts[0]
		return nil
	})

	var generated int64
	var learnerApplied, raftApplied int64
	for i := range tc.Servers {
		m := tc.GetFirstStoreFromServer(t, i).Metrics()
		generated += m.RangeSnapshotsGenerated.Count()
		learnerApplied += m.RangeSnapshotsAppliedForInitialUpreplication.Count()
		raftApplied += m.RangeSnapshotsAppliedByVoters.Count()
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

// TestChangeReplicasDescriptorInvariant tests that a replica change aborts if
// another change has been made to the RangeDescriptor since it was initiated.
func TestChangeReplicasDescriptorInvariant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)
	store := tc.GetFirstStoreFromServer(t, 0)
	store2 := tc.GetFirstStoreFromServer(t, 2)

	key := []byte("a")
	_, origDesc := tc.SplitRangeOrFatal(t, key)
	repl := store.LookupReplica(key)
	require.NotNil(t, repl)

	addReplica := func(storeNum int, desc *roachpb.RangeDescriptor) error {
		chgs := kvpb.MakeReplicationChanges(roachpb.ADD_VOTER, tc.Target(storeNum))
		_, err := repl.ChangeReplicas(ctx, desc, kvserverpb.ReasonRangeUnderReplicated, "", chgs)
		return err
	}

	// Add replica to the second store, which should succeed.
	if err := addReplica(1, &origDesc); err != nil {
		t.Fatal(err)
	}
	testutils.SucceedsSoon(t, func() error {
		r := tc.GetFirstStoreFromServer(t, 1).LookupReplica(key)
		if r == nil {
			return errors.Errorf(`expected replica for key "a"`)
		}
		return nil
	})

	before := store2.Metrics().RangeSnapshotsAppliedForInitialUpreplication.Count()
	// Attempt to add replica to the third store with the original descriptor.
	// This should fail because the descriptor is stale.
	expectedErr := `failed: descriptor changed: \[expected\]`
	if err := addReplica(2, &origDesc); !testutils.IsError(err, expectedErr) {
		t.Fatalf("got unexpected error: %+v", err)
	}

	after := store2.Metrics().RangeSnapshotsAppliedForInitialUpreplication.Count()
	// The failed ChangeReplicas call should NOT have applied a learner snapshot.
	if after != before {
		t.Fatalf(
			"ChangeReplicas call should not have applied a learner snapshot, before %d after %d",
			before, after)
	}

	before = store2.Metrics().RangeSnapshotsAppliedForInitialUpreplication.Count()
	// Add to third store with fresh descriptor.
	if err := addReplica(2, repl.Desc()); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		after := store2.Metrics().RangeSnapshotsAppliedForInitialUpreplication.Count()
		// The failed ChangeReplicas call should have applied a learner snapshot.
		if after != before+1 {
			return errors.Errorf(
				"ChangeReplicas call should have applied a learner snapshot, before %d after %d",
				before, after)
		}
		r := store2.LookupReplica(key)
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
	defer log.Scope(t).Close(t)

	const numServers int = 3
	stickyServerArgs := make(map[int]base.TestServerArgs)
	for i := 0; i < numServers; i++ {
		stickyServerArgs[i] = base.TestServerArgs{
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:    true,
					StickyVFSID: strconv.FormatInt(int64(i), 10),
				},
			},
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					StickyVFSRegistry: fs.NewStickyRegistry(),
				},
			},
		}
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, numServers,
		base.TestClusterArgs{
			ReplicationMode:   base.ReplicationManual,
			ServerArgsPerNode: stickyServerArgs,
		})
	defer tc.Stopper().Stop(ctx)
	store := tc.GetFirstStoreFromServer(t, 0)

	key := []byte("a")
	tc.SplitRangeOrFatal(t, key)
	tc.AddVotersOrFatal(t, key, tc.Targets(1, 2)...)

	incArgs := incrementArgs(key, 5)
	if _, err := kv.SendWrapped(ctx, store.TestSender(), incArgs); err != nil {
		t.Fatal(err)
	}

	tc.WaitForValues(t, key, []int64{5, 5, 5})

	// Stop one of the replicas and issue a new increment.
	tc.StopServer(1)
	incArgs = incrementArgs([]byte("a"), 11)
	if _, err := kv.SendWrapped(ctx, store.TestSender(), incArgs); err != nil {
		t.Fatal(err)
	}

	// The new increment can be seen on both live replicas.
	tc.WaitForValues(t, key, []int64{16, 0 /* stopped */, 16})

	// Once the downed node is restarted, it will catch up.
	require.NoError(t, tc.RestartServer(1))
	tc.WaitForValues(t, key, []int64{16, 16, 16})
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
	defer log.Scope(t).Close(t)
	runReplicateRestartAfterTruncation(t, true /* removeBeforeTruncateAndReAdd */)
}

// TestReplicateRestartAfterTruncation is a variant of
// TestReplicateRestartAfterTruncationWithRemoveAndReAdd without the remove and
// re-add. Just stop, truncate, and restart. This verifies that a snapshot
// without a new replica ID works correctly.
func TestReplicateRestartAfterTruncation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	runReplicateRestartAfterTruncation(t, false /* removeBeforeTruncateAndReAdd */)
}

func runReplicateRestartAfterTruncation(t *testing.T, removeBeforeTruncateAndReAdd bool) {
	ctx := context.Background()
	manualClock := hlc.NewHybridManualClock()

	const numServers int = 3
	stickyServerArgs := make(map[int]base.TestServerArgs)
	for i := 0; i < numServers; i++ {
		stickyServerArgs[i] = base.TestServerArgs{
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:    true,
					StickyVFSID: strconv.FormatInt(int64(i), 10),
				},
			},
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					StickyVFSRegistry: fs.NewStickyRegistry(),
					WallClock:         manualClock,
				},
			},
		}
	}
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode:   base.ReplicationManual,
			ServerArgsPerNode: stickyServerArgs,
		})
	defer tc.Stopper().Stop(ctx)
	store := tc.GetFirstStoreFromServer(t, 0)

	key := tc.ScratchRangeWithExpirationLease(t)

	// Replicate the initial range to all three nodes.
	desc := tc.AddVotersOrFatal(t, key, tc.Targets(1, 2)...)

	// Verify that the first increment propagates to all the engines.
	incArgs := incrementArgs(key, 2)
	if _, err := kv.SendWrapped(context.Background(), store.TestSender(), incArgs); err != nil {
		t.Fatal(err)
	}
	tc.WaitForValues(t, key, []int64{2, 2, 2})

	// Stop a store.
	tc.StopServer(1)
	if removeBeforeTruncateAndReAdd {
		// remove the stopped store from the range
		tc.RemoveVotersOrFatal(t, key, tc.Target(1))
	}

	// Truncate the logs.
	{
		// Get the last increment's log index.
		repl := store.LookupReplica(roachpb.RKey(key))
		index := repl.GetLastIndex()
		// Truncate the log at index+1 (log entries < N are removed, so this includes
		// the increment).
		truncArgs := truncateLogArgs(index+1, repl.RangeID)
		if _, err := kv.SendWrapped(context.Background(), store.TestSender(), truncArgs); err != nil {
			t.Fatal(err)
		}
	}

	// Ensure that store can catch up with the rest of the group.
	incArgs = incrementArgs(key, 3)
	if _, err := kv.SendWrapped(context.Background(), store.TestSender(), incArgs); err != nil {
		t.Fatal(err)
	}

	tc.WaitForValues(t, key, []int64{5, 0 /* stopped */, 5})

	// Re-add the store and restart it.
	// TODO(dt): ben originally suggested we also attempt this in the other order.
	// This currently hits an NPE in mtc.replicateRange though when it tries to
	// read the Ident.NodeID field in the specified store, and will become
	// impossible after streaming snapshots.
	require.NoError(t, tc.RestartServer(1))
	if removeBeforeTruncateAndReAdd {
		// Verify old replica is GC'd. Wait out the replica gc queue
		// inactivity threshold and force a gc scan.
		manualClock.Increment(int64(kvserver.ReplicaGCQueueCheckInterval + 1))
		testutils.SucceedsSoon(t, func() error {
			tc.GetFirstStoreFromServer(t, 1).MustForceReplicaGCScanAndProcess()
			_, err := tc.GetFirstStoreFromServer(t, 1).GetReplica(desc.RangeID)
			if !errors.HasType(err, (*kvpb.RangeNotFoundError)(nil)) {
				// NB: errors.Wrapf(nil, ...) returns nil.
				// nolint:errwrap
				return errors.Errorf("expected replica to be garbage collected, got %v %T", err, err)
			}
			return nil
		})

		tc.AddVotersOrFatal(t, key, tc.Target(1))
	}

	tc.WaitForValues(t, key, []int64{5, 5, 5})
}

func testReplicaAddRemove(t *testing.T, addFirst bool) {
	ctx := context.Background()
	manualClock := hlc.NewHybridManualClock()

	const numServers int = 4
	stickyServerArgs := make(map[int]base.TestServerArgs)
	for i := 0; i < numServers; i++ {
		stickyServerArgs[i] = base.TestServerArgs{
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:    true,
					StickyVFSID: strconv.FormatInt(int64(i), 10),
				},
			},
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					StickyVFSRegistry: fs.NewStickyRegistry(),
					WallClock:         manualClock,
				},
				Store: &kvserver.StoreTestingKnobs{
					// We're gonna want to validate the state of the store before and
					// after the replica GC queue does its work, so we disable the
					// replica gc queue here and run it manually when we're ready.
					DisableReplicaGCQueue:      true,
					DisableEagerReplicaRemoval: true,
				},
			},
		}
	}
	tc := testcluster.StartTestCluster(t, numServers,
		base.TestClusterArgs{
			ReplicationMode:   base.ReplicationManual,
			ServerArgsPerNode: stickyServerArgs,
		})
	defer tc.Stopper().Stop(ctx)
	store := tc.GetFirstStoreFromServer(t, 0)

	key := tc.ScratchRangeWithExpirationLease(t)

	// Replicate the initial range to three of the four nodes.
	tc.AddVotersOrFatal(t, key, tc.Targets(3, 1)...)

	inc1 := int64(5)
	{
		incArgs := incrementArgs(key, inc1)
		if _, err := kv.SendWrapped(context.Background(), store.TestSender(), incArgs); err != nil {
			t.Fatal(err)
		}
	}

	tc.WaitForValues(t, key, []int64{inc1, inc1, 0, inc1})

	// Stop a store and replace it.
	tc.StopServer(1)
	if addFirst {
		tc.AddVotersOrFatal(t, key, tc.Target(2))
		tc.RemoveVotersOrFatal(t, key, tc.Target(1))
	} else {
		tc.RemoveVotersOrFatal(t, key, tc.Target(1))
		tc.AddVotersOrFatal(t, key, tc.Target(2))
	}
	// The first increment is visible on the new replica.
	tc.WaitForValues(t, key, []int64{inc1, 0 /* stopped */, inc1, inc1})

	// Ensure that the rest of the group can make progress.
	inc2 := int64(11)
	{
		incArgs := incrementArgs(key, inc2)
		if _, err := kv.SendWrapped(context.Background(), store.TestSender(), incArgs); err != nil {
			t.Fatal(err)
		}
	}
	tc.WaitForValues(t, key, []int64{inc1 + inc2, 0 /* stopped */, inc1 + inc2, inc1 + inc2})

	// Bring the downed store back up (required for a clean shutdown).
	require.NoError(t, tc.RestartServer(1))

	// The downed store never sees the increment that was added while it was
	// down. Perform another increment now that it is back up to verify that it
	// doesn't see future activity.
	inc3 := int64(23)
	{
		incArgs := incrementArgs(key, inc3)
		if _, err := kv.SendWrapped(context.Background(), store.TestSender(), incArgs); err != nil {
			t.Fatal(err)
		}
	}
	tc.WaitForValues(t, key, []int64{inc1 + inc2 + inc3, inc1, inc1 + inc2 + inc3, inc1 + inc2 + inc3})

	// Wait out the range lease and the unleased duration to make the replica GC'able.
	manualClock.Increment(store.GetStoreConfig().LeaseExpiration())
	manualClock.Increment(int64(kvserver.ReplicaGCQueueCheckInterval + 1))
	tc.GetFirstStoreFromServer(t, 1).SetReplicaGCQueueActive(true)
	tc.GetFirstStoreFromServer(t, 1).MustForceReplicaGCScanAndProcess()

	// The removed store no longer has any of the data from the range.
	tc.WaitForValues(t, key, []int64{inc1 + inc2 + inc3, 0, inc1 + inc2 + inc3, inc1 + inc2 + inc3})

	desc := tc.LookupRangeOrFatal(t, key)
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
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "addFirst", testReplicaAddRemove)
}

// TestQuotaPool verifies that writes get throttled in the case where we have
// two fast moving replicas with sufficiently fast growing raft logs and a
// slower replica catching up. By throttling write throughput we avoid having
// to constantly catch up the slower node via snapshots. See #8659.
func TestQuotaPool(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const quota = 10000
	const numReplicas = 3

	ctx := context.Background()

	settings := cluster.MakeTestingClusterSettings()
	// Override the kvflowcontrol.Mode setting to apply_to_elastic, as when
	// apply_to_all is set (metamorphically), the quota pool will be disabled.
	// See getQuotaPoolEnabledRLocked.
	kvflowcontrol.Mode.Override(ctx, &settings.SV, kvflowcontrol.ApplyToElastic)
	// Disable metamorphism and always run with fortification enabled, as it helps
	// guard against unexpected leadership changes that the test doesn't expect.
	kvserver.RaftLeaderFortificationFractionEnabled.Override(ctx, &settings.SV, 1.0)
	// Using a manual clock here ensures that StoreLiveness support, once
	// established, never expires. By extension, leadership should stay sticky.
	manualClock := hlc.NewHybridManualClock()
	tc := testcluster.StartTestCluster(t, numReplicas,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Settings: settings,
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						WallClock: manualClock,
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)

	key := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, key, tc.Targets(1, 2)...)

	// NB: See TestRaftBlockedReplica/#9914 for why we use a separate	goroutine.
	raftLockReplica := func(repl *kvserver.Replica) {
		ch := make(chan struct{})
		go func() { repl.RaftLock(); close(ch) }()
		<-ch
	}

	leaderRepl := tc.GetRaftLeader(t, roachpb.RKey(key))
	// Grab the raftMu to re-initialize the QuotaPool to ensure that we don't
	// race with ongoing applications.
	raftLockReplica(leaderRepl)
	if err := leaderRepl.InitQuotaPool(quota); err != nil {
		t.Fatalf("failed to initialize quota pool: %v", err)
	}
	leaderRepl.RaftUnlock()
	// Wait until the follower will is not ignored by updateProposalQuotaRaftMuLocked.
	// Otherwise the quota gets prematurely released into the pool. We can use status.Applied
	// instead of proposalQuotaBaseIndex because it's strictly ahead or the same.
	testutils.SucceedsSoon(t, func() error {
		var err error
		status := leaderRepl.RaftStatus()
		minIndex := status.Applied
		for id, progress := range status.Progress {
			if progress.Match < minIndex {
				err = errors.Errorf("Replica %d is behind leader expected %d but was %d", id, minIndex, progress.Match)
			}
		}
		return err
	})

	followerRepl := func() *kvserver.Replica {
		for i := range tc.Servers {
			repl := tc.GetFirstStoreFromServer(t, i).LookupReplica(roachpb.RKey(key))
			require.NotNil(t, repl)
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
	ch := make(chan *kvpb.Error, 1)

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
		keyToWrite := key.Next()
		value := bytes.Repeat([]byte("v"), (3*quota)/4)
		ba := &kvpb.BatchRequest{}
		ba.Add(putArgs(keyToWrite, value))
		if err := ba.SetActiveTimestamp(tc.Servers[0].Clock()); err != nil {
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
			ba := &kvpb.BatchRequest{}
			ba.Add(putArgs(keyToWrite, value))
			if err := ba.SetActiveTimestamp(tc.Servers[0].Clock()); err != nil {
				ch <- kvpb.NewError(err)
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
	defer log.Scope(t).Close(t)

	const numReplicas = 3

	testutils.RunValues(t, "lease-type", roachpb.TestingAllLeaseTypes(), func(t *testing.T, leaseType roachpb.LeaseType) {
		ctx := context.Background()
		manual := hlc.NewHybridManualClock()

		settings := cluster.MakeTestingClusterSettings()
		kvserver.OverrideDefaultLeaseType(ctx, &settings.SV, leaseType)

		// Suppress timeout-based elections to avoid leadership changes in ways this
		// test doesn't expect. For leader leases, fortification itself provides us
		// this guarantee.
		var raftConfig base.RaftConfig
		if leaseType != roachpb.LeaseLeader {
			raftConfig = base.RaftConfig{
				RaftElectionTimeoutTicks: 100000,
			}
		}

		tc := testcluster.StartTestCluster(t, numReplicas,
			base.TestClusterArgs{
				ReplicationMode: base.ReplicationManual,
				ServerArgs: base.TestServerArgs{
					Settings:   settings,
					RaftConfig: raftConfig,
					Knobs: base.TestingKnobs{
						Server: &server.TestingKnobs{
							WallClock: manual,
						},
					},
				},
			})
		defer tc.Stopper().Stop(ctx)

		// Pause the manual clock so that we can carefully control the perceived
		// timing of the follower replica's activity.
		manual.Pause()
		t.Logf("paused clock at %s", manual.Now())

		key := []byte("a")
		tc.SplitRangeOrFatal(t, key)
		tc.AddVotersOrFatal(t, key, tc.Targets(1, 2)...)

		// Do a write; we'll use it to determine when the dust has settled.
		_, err := tc.Servers[0].DB().Inc(ctx, key, 1)
		require.Nil(t, err)
		tc.WaitForValues(t, key, []int64{1, 1, 1})

		// Get a handle on the leader and the follower replicas.
		leaderRepl := tc.GetRaftLeader(t, key)
		leaderClock := leaderRepl.Clock()
		followerRepl := func() *kvserver.Replica {
			for i := range tc.Servers {
				repl := tc.GetFirstStoreFromServer(t, i).LookupReplica(key)
				require.NotNil(t, repl)
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

		// Wait for the leader replica to have three entries in its lastUpdateTimes
		// map. It should already by this time because it was able to replicate a
		// log entry to its two followers, but we wait here to be sure and to avoid
		// flakiness. It is possible that the WaitForValues call above returned as
		// soon as one of the followers appended and applied a log entry, but before
		// its response was delivered to the leader.
		testutils.SucceedsSoon(t, func() error {
			lastUpdateTimes := leaderRepl.LastUpdateTimes()
			if len(lastUpdateTimes) == 3 {
				return nil
			}
			return errors.Errorf("expected leader replica to have 3 entries in lastUpdateTimes map, found %s", lastUpdateTimes)
		})

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

		// inactivityThreshold is the test's duration of inactivity after which the
		// follower replica is considered inactive. In practice, this is set to the
		// range lease duration.
		inactivityThreshold := time.Second

		// Increment the clock to be close to inactivityThreshold, but not past it.
		manual.Increment(inactivityThreshold.Nanoseconds() - 1)

		// Send a request to the leader replica. followerRepl is locked so it will
		// not respond.
		value := []byte("value")
		ba := &kvpb.BatchRequest{}
		ba.Add(putArgs(key, value))
		if err := ba.SetActiveTimestamp(leaderClock); err != nil {
			t.Fatal(err)
		}
		if _, pErr := leaderRepl.Send(ctx, ba); pErr != nil {
			t.Fatal(pErr)
		}

		// The follower should still be active.
		followerID := followerRepl.ReplicaID()
		leaderNow := leaderClock.PhysicalTime()
		if !leaderRepl.IsFollowerActiveSince(followerID, leaderNow, inactivityThreshold) {
			t.Fatalf("expected follower to still be considered active; "+
				"follower id: %d, last update times: %s, leader clock: %s",
				followerID, leaderRepl.LastUpdateTimes(), leaderNow)
		}

		// It is possible that there are in-flight heartbeat responses from
		// followerRepl from before it was locked. The receipt of one of these
		// would bump the last active timestamp on the leader. Because of this,
		// we check whether the follower is eventually considered inactive.
		testutils.SucceedsSoon(t, func() error {
			// Increment the clock to past inactivityThreshold.
			manual.Increment(inactivityThreshold.Nanoseconds() + 1)

			// Send another request to the leader replica. followerRepl is locked
			// so it will not respond.
			if _, pErr := leaderRepl.Send(ctx, ba); pErr != nil {
				t.Fatal(pErr)
			}

			// The follower should no longer be considered active.
			leaderNow = leaderClock.PhysicalTime()
			if leaderRepl.IsFollowerActiveSince(followerID, leaderNow, inactivityThreshold) {
				return errors.Errorf("expected follower to be considered inactive; "+
					"follower id: %d, last update times: %s, leader clock: %s",
					followerID, leaderRepl.LastUpdateTimes(), leaderNow)
			}
			return nil
		})
	})
}

// TestRaftHeartbeats verifies that coalesced heartbeats are correctly
// suppressing elections in an idle cluster.
func TestRaftHeartbeats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)

	key := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, key, tc.Targets(1, 2)...)

	// Capture the initial term and state.
	leaderRepl := tc.GetRaftLeader(t, roachpb.RKey(key))
	initialTerm := leaderRepl.RaftStatus().Term

	// Get the store for the leader
	store := tc.GetFirstStoreFromServer(t, int(leaderRepl.StoreID()-1))

	// Wait for several ticks to elapse.
	ticksToWait := 2 * store.GetStoreConfig().RaftElectionTimeoutTicks
	ticks := store.Metrics().RaftTicks.Count
	for targetTicks := ticks() + ticksToWait; ticks() < targetTicks; {
		time.Sleep(time.Millisecond)
	}

	status := leaderRepl.RaftStatus()
	if status.SoftState.RaftState != raftpb.StateLeader {
		t.Errorf("expected node %d to be leader after sleeping but was %s", leaderRepl.NodeID(), status.SoftState.RaftState)
	}
	if status.Term != initialTerm {
		t.Errorf("while sleeping, term changed from %d to %d", initialTerm, status.Term)
	}
}

// TestReportUnreachableHeartbeats tests that if a single transport fails,
// coalesced heartbeats are not stalled out entirely.
func TestReportUnreachableHeartbeats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)

	key := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, key, tc.Targets(1, 2)...)

	leaderRepl := tc.GetRaftLeader(t, roachpb.RKey(key))
	initialTerm := leaderRepl.RaftStatus().Term
	// Choose a follower index that is guaranteed to not be the leader.
	followerIdx := int(leaderRepl.StoreID()) % len(tc.Servers)
	// Get the store for the leader
	leaderStore := tc.GetFirstStoreFromServer(t, int(leaderRepl.StoreID()-1))

	// Shut down a raft transport via the circuit breaker, and wait for two
	// election timeouts to trigger an election if reportUnreachable broke
	// heartbeat transmission to the other store.
	b, ok := tc.Servers[followerIdx].RaftTransport().(*kvserver.RaftTransport).GetCircuitBreaker(
		tc.Target(followerIdx).NodeID, rpc.DefaultClass)
	require.True(t, ok)
	undo := circuit.TestingSetTripped(b, errors.New("boom"))
	defer undo()

	// Send a command to ensure Raft is aware of lost follower so that it won't
	// quiesce (which would prevent heartbeats).
	if _, err := kv.SendWrapped(
		ctx, tc.GetFirstStoreFromServer(t, 0).TestSender(),
		incrementArgs(key, 1)); err != nil {
		t.Fatal(err)
	}

	ticksToWait := 2 * leaderStore.GetStoreConfig().RaftElectionTimeoutTicks
	ticks := leaderStore.Metrics().RaftTicks.Count
	for targetTicks := ticks() + ticksToWait; ticks() < targetTicks; {
		time.Sleep(time.Millisecond)
	}

	// Ensure that the leadership has not changed, to confirm that heartbeats
	// are sent to the store with a functioning transport.
	status := leaderRepl.RaftStatus()
	if status.SoftState.RaftState != raftpb.StateLeader {
		t.Errorf("expected node %d to be leader after sleeping but was %s", leaderRepl.NodeID(), status.SoftState.RaftState)
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
	defer log.Scope(t).Close(t)

	rng, seed := randutil.NewTestRand()
	t.Logf("seed is %d", seed)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)
	key := tc.ScratchRangeWithExpirationLease(t)
	tc.AddVotersOrFatal(t, key, tc.Targets(1, 2)...)
	require.NoError(t, tc.WaitForVoters(key, tc.Targets(1, 2)...))

	for i := 0; i < 5; i++ {
		// Find the Raft leader.
		var leaderIdx int
		var leaderRepl *kvserver.Replica
		testutils.SucceedsSoon(t, func() error {
			for idx := range tc.Servers {
				repl := tc.GetFirstStoreFromServer(t, idx).LookupReplica(roachpb.RKey(key))
				require.NotNil(t, repl)
				if repl.RaftStatus().SoftState.RaftState == raftpb.StateLeader {
					leaderIdx = idx
					leaderRepl = repl
					return nil
				}
			}
			return errors.New("no Raft leader found")
		})

		// Raft leader found. Make sure it doesn't have the lease (transferring it
		// away if necessary, which entails picking a random other node and sending
		// the lease to it). We'll also partition this random node away from the rest
		// as this was (way back) how we triggered problems with coalesced heartbeats.
		partitionedMaybeLeaseholderIdx := (leaderIdx + 1 + rng.Intn(tc.NumServers()-1)) % tc.NumServers()
		t.Logf("leader is idx=%d, partitioning idx=%d", leaderIdx, partitionedMaybeLeaseholderIdx)
		leaderRepDesc, err := leaderRepl.GetReplicaDescriptor()
		require.NoError(t, err)
		if lease, _ := leaderRepl.GetLease(); lease.OwnedBy(leaderRepDesc.StoreID) {
			tc.TransferRangeLeaseOrFatal(t, *leaderRepl.Desc(), tc.Target(partitionedMaybeLeaseholderIdx))
		}

		// Remove the raft leader.
		t.Logf("removing leader")
		tc.RemoveVotersOrFatal(t, key, tc.Target(leaderIdx))

		// Pseudo-partition partitionedMaybeLeaseholderIdx away from everyone else. We do this by tripping
		// the circuit breaker on all other nodes.
		t.Logf("partitioning")
		var undos []func()
		for i := range tc.Servers {
			if i != partitionedMaybeLeaseholderIdx {
				b, ok := tc.Servers[i].RaftTransport().(*kvserver.RaftTransport).GetCircuitBreaker(tc.Target(partitionedMaybeLeaseholderIdx).NodeID, rpc.DefaultClass)
				require.True(t, ok)
				undos = append(undos, circuit.TestingSetTripped(b, errors.New("boom")))
			}
		}

		// Wait out the heartbeat interval and resolve the partition.
		heartbeatInterval := tc.GetFirstStoreFromServer(t, partitionedMaybeLeaseholderIdx).GetStoreConfig().CoalescedHeartbeatsInterval
		time.Sleep(heartbeatInterval)
		t.Logf("resolving partition")
		for _, undo := range undos {
			undo()
		}

		t.Logf("waiting for replicaGC of removed leader replica")
		// Make sure the old replica was actually removed by replicaGC, before we
		// try to re-add it. Otherwise the addition below might fail. One shot here
		// is often enough, but not always; in the worst case we need to wait out
		// something on the order of a election timeout plus
		// ReplicaGCQueueSuspectTimeout before replicaGC will be attempted (and will
		// then succeed on the first try).
		testutils.SucceedsSoon(t, func() error {
			s := tc.GetFirstStoreFromServer(t, leaderIdx)
			s.MustForceReplicaGCScanAndProcess()
			if oldRepl := tc.GetFirstStoreFromServer(t, leaderIdx).LookupReplica(roachpb.RKey(key)); oldRepl != nil {
				return errors.Errorf("Expected replica %s to be removed", oldRepl)
			}
			return nil
		})
		t.Logf("re-adding leader")
		tc.AddVotersOrFatal(t, key, tc.Target(leaderIdx))
		require.NoError(t, tc.WaitForVoters(key, tc.Target(leaderIdx)))
	}
}

// TestReplicateAfterSplit verifies that a new replica whose start key
// is not KeyMin replicating to a fresh store can apply snapshots correctly.
func TestReplicateAfterSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 2,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)

	splitKey := roachpb.Key("m")
	key := roachpb.Key("z")

	tc.SplitRangeOrFatal(t, splitKey)
	store0 := tc.GetFirstStoreFromServer(t, 0)

	// Issue an increment for later check.
	incArgs := incrementArgs(key, 11)
	if _, err := kv.SendWrappedWith(ctx, store0.TestSender(),
		kvpb.Header{}, incArgs); err != nil {
		t.Fatal(err)
	}
	// Now add the second replica.
	tc.AddVotersOrFatal(t, splitKey, tc.Target(1))

	if tc.GetFirstStoreFromServer(t, 1).LookupReplica(roachpb.RKey(key)).GetMaxBytes(ctx) == 0 {
		t.Error("Range MaxBytes is not set after snapshot applied")
	}
	// Once it catches up, the effects of increment commands can be seen.
	testutils.SucceedsSoon(t, func() error {
		getArgs := getArgs(key)
		// Reading on non-lease holder replica should use inconsistent read
		if reply, err := kv.SendWrappedWith(ctx, tc.GetFirstStoreFromServer(t, 1).TestSender(), kvpb.Header{
			ReadConsistency: kvpb.INCONSISTENT,
		}, getArgs); err != nil {
			return errors.Errorf("failed to read data: %s", err)
		} else if e, v := int64(11), mustGetInt(reply.(*kvpb.GetResponse).Value); v != e {
			return errors.Errorf("failed to read correct data: expected %d, got %d", e, v)
		}
		return nil
	})
}

// TestReplicaRemovalCampaign verifies that a new replica after a split can be
// transferred away/replaced without campaigning the old one.
func TestReplicaRemovalCampaign(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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

	splitKey := roachpb.Key("m")
	key2 := roachpb.Key("z")

	for i, td := range testData {
		func() {
			ctx := context.Background()
			tc := testcluster.StartTestCluster(t, 2,
				base.TestClusterArgs{
					ReplicationMode: base.ReplicationManual,
				})
			defer tc.Stopper().Stop(ctx)
			store0 := tc.GetFirstStoreFromServer(t, 0)

			// Make the split.
			tc.SplitRangeOrFatal(t, splitKey)
			// Replicate range to enable raft campaigning.
			tc.AddVotersOrFatal(t, splitKey, tc.Target(1))

			replica2 := store0.LookupReplica(roachpb.RKey(key2))

			rg2 := func(s *kvserver.Store) kv.Sender {
				return kv.Wrap(s, func(ba *kvpb.BatchRequest) *kvpb.BatchRequest {
					if ba.RangeID == 0 {
						ba.RangeID = replica2.RangeID
					}
					return ba
				})
			}

			// Raft processing is initialized lazily; issue a no-op write request to
			// ensure that the Raft group has been started.
			incArgs := incrementArgs(key2, 0)
			if _, err := kv.SendWrapped(ctx, rg2(store0), incArgs); err != nil {
				t.Fatal(err)
			}

			if td.remove {
				// Simulate second replica being transferred by removing it.
				if err := store0.RemoveReplica(ctx, replica2, replica2.Desc().NextReplicaID, kvserver.RemoveOptions{
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
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)

	key := []byte("b")
	_, desc := tc.SplitRangeOrFatal(t, key)
	tc.AddVotersOrFatal(t, key, tc.Targets(1, 2)...)

	tc.RemoveVotersOrFatal(t, key, tc.Target(2))
	tc.RemoveVotersOrFatal(t, key, tc.Target(1))

	// Wait for the removal to be processed.
	testutils.SucceedsSoon(t, func() error {
		for i := range tc.Servers[1:] {
			store := tc.GetFirstStoreFromServer(t, i)
			_, err := store.GetReplica(desc.RangeID)
			if !errors.HasType(err, (*kvpb.RangeNotFoundError)(nil)) {
				return errors.Wrapf(err, "range %d not yet removed from %s", desc.RangeID, store)
			}
		}
		return nil
	})

	target1 := tc.Target(1)
	target2 := tc.Target(2)

	// Test that a coalesced heartbeat is ingested correctly.
	replica1 := roachpb.ReplicaDescriptor{
		ReplicaID: roachpb.ReplicaID(target1.StoreID),
		NodeID:    target1.NodeID,
		StoreID:   target1.StoreID,
	}
	replica2 := roachpb.ReplicaDescriptor{
		ReplicaID: roachpb.ReplicaID(target2.StoreID),
		NodeID:    target2.NodeID,
		StoreID:   target2.StoreID,
	}

	tc.Servers[2].RaftTransport().(*kvserver.RaftTransport).SendAsync(&kvserverpb.RaftMessageRequest{
		ToReplica:   replica1,
		FromReplica: replica2,
		Heartbeats: []kvserverpb.RaftHeartbeat{
			{
				RangeID:       desc.RangeID,
				FromReplicaID: replica2.ReplicaID,
				ToReplicaID:   replica1.ReplicaID,
			},
		},
	}, rpc.DefaultClass)
	// Execute another replica change to ensure that raft has processed
	// the heartbeat just sent.
	tc.AddVotersOrFatal(t, key, tc.Target(1))
}

// TestRaftRemoveRace adds and removes a replica repeatedly in an attempt to
// reproduce a race (see #1911 and #9037).
func TestRaftRemoveRace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	numServers := 10
	if util.RaceEnabled {
		// In race builds, running 10 nodes needs more than 1 full CPU (due to
		// background gossip and heartbeat overhead), so it can't keep up when run
		// under stress with one process per CPU. Run a reduced version of this test
		// in race builds. This isn't as likely to reproduce the races but will
		// still have a chance to do so.
		numServers = 3
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, numServers,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)

	key := tc.ScratchRange(t)
	desc := tc.LookupRangeOrFatal(t, key)
	// Cyclically up-replicate to a bunch of nodes which stresses a condition
	// where replicas receive messages for a previous or later incarnation of the
	// replica.
	targets := make([]roachpb.ReplicationTarget, len(tc.Servers)-1)
	for i := 1; i < len(tc.Servers); i++ {
		targets[i-1] = tc.Target(i)
	}
	tc.AddVotersOrFatal(t, key, targets...)

	for i := 0; i < 10; i++ {
		tc.RemoveVotersOrFatal(t, key, tc.Target(2))
		tc.AddVotersOrFatal(t, key, tc.Target(2))

		// Verify the tombstone key does not exist. See #12130.
		tombstoneKey := keys.RangeTombstoneKey(desc.RangeID)
		var tombstone kvserverpb.RangeTombstone
		if ok, err := storage.MVCCGetProto(
			ctx, tc.GetFirstStoreFromServer(t, 2).TODOEngine(), tombstoneKey,
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
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)

	key := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, key, tc.Targets(1, 2)...)

	repl := tc.GetFirstStoreFromServer(t, 0).LookupReplica(roachpb.RKey(key))
	require.NotNil(t, repl)
	ctx = repl.AnnotateCtx(ctx)

	for i := 0; i < 100; i++ {
		for _, action := range []roachpb.ReplicaChangeType{roachpb.REMOVE_VOTER, roachpb.ADD_VOTER} {
			for {
				chgs := kvpb.MakeReplicationChanges(action, tc.Target(1))
				if _, err := repl.ChangeReplicas(ctx, repl.Desc(), kvserverpb.ReasonUnknown, "", chgs); err != nil {
					if kvserver.IsRetriableReplicationChangeError(err) ||
						kvserver.IsReplicationChangeInProgressError(err) {
						continue
					}
					t.Fatal(err)
				}
				break
			}
		}
	}
}

type noConfChangeTestHandler struct {
	rangeID roachpb.RangeID
	kvserver.IncomingRaftMessageHandler
}

func (ncc *noConfChangeTestHandler) HandleRaftRequest(
	ctx context.Context,
	req *kvserverpb.RaftMessageRequest,
	respStream kvserver.RaftMessageResponseStream,
) *kvpb.Error {
	for i, e := range req.Message.Entries {
		if e.Type == raftpb.EntryConfChange {
			var cc raftpb.ConfChange
			if err := protoutil.Unmarshal(e.Data, &cc); err != nil {
				panic(err)
			}
			var ccCtx kvserverpb.ConfChangeContext
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
	return ncc.IncomingRaftMessageHandler.HandleRaftRequest(ctx, req, respStream)
}

func (ncc *noConfChangeTestHandler) HandleRaftResponse(
	ctx context.Context, resp *kvserverpb.RaftMessageResponse,
) error {
	switch val := resp.Union.GetValue().(type) {
	case *kvpb.Error:
		switch val.GetDetail().(type) {
		case *kvpb.ReplicaTooOldError:
			// We're going to manually GC the replica, so ignore these errors.
			return nil
		}
	}
	return ncc.IncomingRaftMessageHandler.HandleRaftResponse(ctx, resp)
}

func TestReplicaGCRace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)

	key := tc.ScratchRange(t)
	desc := tc.LookupRangeOrFatal(t, key)
	tc.AddVotersOrFatal(t, key, tc.Target(1))

	leaderStore := tc.GetFirstStoreFromServer(t, 0)
	fromStore := tc.GetFirstStoreFromServer(t, 1)
	toStore := tc.GetFirstStoreFromServer(t, 2)

	// Prevent the victim replica from processing configuration changes.
	tc.Servers[2].RaftTransport().(*kvserver.RaftTransport).StopIncomingRaftMessages(toStore.Ident.StoreID)
	tc.Servers[2].RaftTransport().(*kvserver.RaftTransport).ListenIncomingRaftMessages(toStore.Ident.StoreID, &noConfChangeTestHandler{
		rangeID:                    desc.RangeID,
		IncomingRaftMessageHandler: toStore,
	})

	repl, err := leaderStore.GetReplica(desc.RangeID)
	if err != nil {
		t.Fatal(err)
	}
	ctx = repl.AnnotateCtx(ctx)

	// Add the victim replica. Note that it will receive a snapshot and raft log
	// replays, but will not process the configuration change containing the new
	// range descriptor, preventing it from learning of the new NextReplicaID.
	chgs := kvpb.MakeReplicationChanges(roachpb.ADD_VOTER, roachpb.ReplicationTarget{
		NodeID:  toStore.Ident.NodeID,
		StoreID: toStore.Ident.StoreID,
	})
	if _, err := repl.ChangeReplicas(ctx, repl.Desc(), kvserverpb.ReasonRangeUnderReplicated, "", chgs); err != nil {
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

	hbReq := kvserverpb.RaftMessageRequest{
		FromReplica: fromReplicaDesc,
		ToReplica:   toReplicaDesc,
		Heartbeats: []kvserverpb.RaftHeartbeat{
			{
				RangeID:       desc.RangeID,
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
		progress, ok := progressByID[raftpb.PeerID(toReplicaDesc.ReplicaID)]
		if !ok {
			return errors.Errorf("%+v does not yet contain %s", progressByID, toReplicaDesc)
		}
		if progress.Match == 0 {
			return errors.Errorf("%+v has not yet advanced", progress)
		}
		for i := range hbReq.Heartbeats {
			hbReq.Heartbeats[i].Term = kvpb.RaftTerm(status.Term)
			hbReq.Heartbeats[i].Commit = kvpb.RaftIndex(progress.Match)
		}
		return nil
	})

	// Remove the victim replica and manually GC it.
	chgs[0].ChangeType = roachpb.REMOVE_VOTER
	if _, err := repl.ChangeReplicas(ctx, repl.Desc(), kvserverpb.ReasonRangeOverReplicated, "", chgs); err != nil {
		t.Fatal(err)
	}

	{
		removedReplica, err := toStore.GetReplica(desc.RangeID)
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
	ambient := tc.Servers[0].AmbientCtx()
	ambient.AddLogTag("test-raft-transport", nil)
	fromTransport := kvserver.NewRaftTransport(
		ambient,
		cluster.MakeTestingClusterSettings(),
		tc.Servers[0].Stopper(),
		tc.Servers[0].Clock(),
		nodedialer.New(tc.Servers[0].RPCContext(), gossip.AddressResolver(fromStore.Gossip())),
		nil, /* grpcServer */
		kvflowdispatch.NewDummyDispatch(),
		kvserver.NoopStoresFlowControlIntegration{},
		kvserver.NoopRaftTransportDisconnectListener{},
		(*node_rac2.AdmittedPiggybacker)(nil),
		nil, /* PiggybackedAdmittedResponseScheduler */
		nil, /* knobs */
	)
	errChan := errorChannelTestHandler(make(chan *kvpb.Error, 1))
	fromTransport.ListenIncomingRaftMessages(fromStore.StoreID(), errChan)

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
		case *kvpb.RaftGroupDeletedError:
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

// TestReplicateRogueRemovedNode ensures that a rogue removed node
// (i.e. a node that has been removed from the range but doesn't know
// it yet because it was down or partitioned away when it happened)
// cannot cause other removed nodes to recreate their ranges.
func TestReplicateRogueRemovedNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	manualClock := hlc.NewHybridManualClock()

	const numServers int = 3
	stickyServerArgs := make(map[int]base.TestServerArgs)
	for i := 0; i < numServers; i++ {
		stickyServerArgs[i] = base.TestServerArgs{
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:    true,
					StickyVFSID: strconv.FormatInt(int64(i), 10),
				},
			},
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					StickyVFSRegistry: fs.NewStickyRegistry(),
					WallClock:         manualClock,
				},
				Store: &kvserver.StoreTestingKnobs{
					// Newly-started stores (including the "rogue" one) should not GC
					// their replicas. We'll turn this back on when needed.
					DisableReplicaGCQueue: true,
				},
			},
		}
	}
	tc := testcluster.StartTestCluster(t, numServers,
		base.TestClusterArgs{
			ReplicationMode:   base.ReplicationManual,
			ServerArgsPerNode: stickyServerArgs,
		})
	defer tc.Stopper().Stop(ctx)
	store := tc.GetFirstStoreFromServer(t, 0)

	key := tc.ScratchRangeWithExpirationLease(t)
	// First put the range on all three nodes.
	desc := tc.AddVotersOrFatal(t, key, tc.Targets(1, 2)...)
	// We're going to set up the cluster with partitioning so that we can
	// partition node 0 from the others. The partition is not initially active.
	partRange, err := setupPartitionedRange(tc, desc.RangeID, 0, 0, false /* activated */, unreliableRaftHandlerFuncs{})
	require.NoError(t, err)

	// Put some data in the range so we'll have something to test for.
	incArgs := incrementArgs(key, 5)
	if _, err := kv.SendWrapped(ctx, store.TestSender(), incArgs); err != nil {
		t.Fatal(err)
	}

	// Wait for all nodes to catch up.
	tc.WaitForValues(t, key, []int64{5, 5, 5})

	// Stop node 2; while it is down remove the range from nodes 2 and 1.
	tc.StopServer(2)
	tc.RemoveVotersOrFatal(t, key, tc.Targets(2, 1)...)

	// Make a write on node 0; this will not be replicated because 0 is the only node left.
	incArgs = incrementArgs(key, 11)
	if _, err := kv.SendWrapped(context.Background(), store.TestSender(), incArgs); err != nil {
		t.Fatal(err)
	}

	// Wait for the replica to be GC'd on node 1.
	// Store 0 has two writes, 1 has erased everything, and 2 still has the first write.
	// A single pass of ForceReplicaGCScanAndProcess is not enough, since the replica
	// may be recreated by a stray raft message, so we run the GC scan inside the loop.
	// TODO(bdarnell): if the call to RemoveReplica in replicaGCQueue.process can be
	// moved under the lock, then the GC scan can be moved out of this loop.
	tc.GetFirstStoreFromServer(t, 1).SetReplicaGCQueueActive(true)
	testutils.SucceedsSoon(t, func() error {
		manualClock.Increment(store.GetStoreConfig().LeaseExpiration())
		manualClock.Increment(int64(
			kvserver.ReplicaGCQueueCheckInterval) + 1)
		tc.GetFirstStoreFromServer(t, 1).MustForceReplicaGCScanAndProcess()

		actual := tc.ReadIntFromStores(key)
		expected := []int64{16, 0, 0 /* stopped */}
		if !reflect.DeepEqual(expected, actual) {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})
	// Partition nodes 1 and 2 from node 0. Otherwise they'd get a
	// ReplicaTooOldError from node 0 and proceed to remove themselves.
	partRange.activate()
	// Bring node 2 back up.
	require.NoError(t, tc.RestartServer(2))

	// Try to issue a command on node 2. It should not be able to commit
	// (so we add it asynchronously).
	var startWG sync.WaitGroup
	startWG.Add(1)
	var finishWG sync.WaitGroup
	finishWG.Add(1)

	rep := tc.GetFirstStoreFromServer(t, 2).LookupReplica(roachpb.RKey(key))
	replicaDesc, ok := rep.Desc().GetReplicaDescriptor(tc.Target(2).StoreID)
	require.True(t, ok)

	require.NoError(t, tc.Stopper().RunAsyncTask(ctx, "send-req", func(ctx context.Context) {
		incArgs := incrementArgs(key, 23)
		startWG.Done()
		defer finishWG.Done()
		_, pErr := kv.SendWrappedWith(
			context.Background(),
			tc.GetFirstStoreFromServer(t, 2).TestSender(),
			kvpb.Header{
				Replica:   replicaDesc,
				Timestamp: tc.Servers[2].Clock().Now(),
			}, incArgs,
		)
		detail := pErr.GetDetail()
		switch detail.(type) {
		case *kvpb.RangeNotFoundError:
			// The typical case.
		case *kvpb.NotLeaseHolderError:
			// The atypical case - the lease may have expired and the
			// lease acquisition may be refused.
		default:
			// NB: cannot fatal on a goroutine.
			t.Errorf("unexpected error: %v", pErr)
		}
	}))
	startWG.Wait()
	if t.Failed() {
		t.FailNow()
	}

	// Sleep a bit to let the command proposed on node 2 proceed if it's
	// going to. Prior to the introduction of replica tombstones, this
	// would lead to split-brain: Node 2 would wake up node 1 and they
	// would form a quorum, even though node 0 had removed them both.
	// Now the tombstone on node 1 prevents it from rejoining the rogue
	// copy of the group.
	time.Sleep(100 * time.Millisecond)
	testutils.SucceedsSoon(t, func() error {
		actual := tc.ReadIntFromStores(key)
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
	tc.GetFirstStoreFromServer(t, 2).SetReplicaGCQueueActive(true)
	manualClock.Increment(store.GetStoreConfig().LeaseExpiration())
	manualClock.Increment(int64(
		kvserver.ReplicaGCQueueCheckInterval) + 1)
	tc.GetFirstStoreFromServer(t, 2).MustForceReplicaGCScanAndProcess()
	tc.WaitForValues(t, key, []int64{16, 0, 0})

	// Now that the group has been GC'd, the goroutine that was
	// attempting to write has finished (with an error).
	finishWG.Wait()
}

type errorChannelTestHandler chan *kvpb.Error

func (errorChannelTestHandler) HandleRaftRequest(
	_ context.Context, _ *kvserverpb.RaftMessageRequest, _ kvserver.RaftMessageResponseStream,
) *kvpb.Error {
	panic("unimplemented")
}

func (d errorChannelTestHandler) HandleRaftResponse(
	ctx context.Context, resp *kvserverpb.RaftMessageResponse,
) error {
	switch val := resp.Union.GetValue().(type) {
	case *kvpb.Error:
		d <- val
	default:
		log.Fatalf(ctx, "unexpected response type %T", val)
	}
	return nil
}

func (errorChannelTestHandler) HandleSnapshot(
	_ context.Context, _ *kvserverpb.SnapshotRequest_Header, _ kvserver.SnapshotResponseStream,
) error {
	panic("unimplemented")
}

func (errorChannelTestHandler) HandleDelegatedSnapshot(
	_ context.Context, _ *kvserverpb.DelegateSendSnapshotRequest,
) *kvserverpb.DelegateSnapshotResponse {
	panic("unimplemented")
}

// This test simulates a scenario where one replica has been removed from the
// range's Raft group but it is unaware of the fact. We check that this replica
// coming back from the dead cannot cause elections.
func TestReplicateRemovedNodeDisruptiveElection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 4,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)

	// Move the first range from the first node to the other three.
	key := roachpb.Key("a")
	tc.SplitRangeOrFatal(t, key)
	tc.AddVotersOrFatal(t, key, tc.Targets(1, 2, 3)...)
	desc := tc.LookupRangeOrFatal(t, key)
	tc.TransferRangeLeaseOrFatal(t, desc, tc.Target(1))
	tc.RemoveVotersOrFatal(t, key, tc.Target(0))

	// Ensure that we have a stable lease and raft leader so we can tell if the
	// removed node causes a disruption. This is a three-step process.

	// 1. Write on the second node, to ensure that a lease has been
	// established after the first node's removal.
	value := int64(5)
	incArgs := incrementArgs(key, value)
	if _, err := kv.SendWrapped(ctx, tc.Servers[1].DistSenderI().(kv.Sender), incArgs); err != nil {
		t.Fatal(err)
	}

	// 2. Wait for all nodes to process the increment (and therefore the
	// new lease).
	tc.WaitForValues(t, key, []int64{0, value, value, value})

	// 3. Wait for the lease holder to obtain raft leadership too.
	testutils.SucceedsSoon(t, func() error {
		hint := tc.Target(1)
		leadReplica := tc.GetRaftLeader(t, roachpb.RKey(key))
		leaseReplica, err := tc.FindRangeLeaseHolder(*leadReplica.Desc(), &hint)
		if err != nil {
			return err
		}
		if leaseReplica.StoreID != leadReplica.StoreID() {
			return errors.Errorf("leaseReplica %s does not match leadReplica %s",
				leaseReplica, leadReplica)
		}

		return nil
	})

	// Save the current term, which is the latest among the live stores.
	findTerm := func() uint64 {
		var term uint64
		for i := 1; i < 4; i++ {
			s := tc.GetFirstStoreFromServer(t, i).RaftStatus(desc.RangeID)
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

	target0 := tc.Target(0)
	target1 := tc.Target(1)

	// replica0 is the one that  has been removed; replica1 is a current
	// member of the group.
	replica0 := roachpb.ReplicaDescriptor{
		ReplicaID: roachpb.ReplicaID(target0.StoreID),
		NodeID:    target0.NodeID,
		StoreID:   target0.StoreID,
	}
	replica1 := roachpb.ReplicaDescriptor{
		ReplicaID: roachpb.ReplicaID(target1.StoreID),
		NodeID:    target1.NodeID,
		StoreID:   target1.StoreID,
	}

	// Create a new transport for store 0 so that we can intercept the responses.
	// Error responses are passed back along the same grpc stream as the request
	// so it's ok that there are two (this one and the one actually used by the
	// store).
	transport0 := kvserver.NewRaftTransport(
		tc.Servers[0].AmbientCtx(),
		cluster.MakeTestingClusterSettings(),
		tc.Servers[0].Stopper(),
		tc.Servers[0].Clock(),
		nodedialer.New(tc.Servers[0].RPCContext(),
			gossip.AddressResolver(tc.GetFirstStoreFromServer(t, 0).Gossip())),
		nil, /* grpcServer */
		kvflowdispatch.NewDummyDispatch(),
		kvserver.NoopStoresFlowControlIntegration{},
		kvserver.NoopRaftTransportDisconnectListener{},
		(*node_rac2.AdmittedPiggybacker)(nil),
		nil, /* PiggybackedAdmittedResponseScheduler */
		nil, /* knobs */
	)
	errChan := errorChannelTestHandler(make(chan *kvpb.Error, 1))
	transport0.ListenIncomingRaftMessages(target0.StoreID, errChan)

	// Simulate the removed node asking to trigger an election. Try and try again
	// until we're reasonably sure the message was sent.
	for !transport0.SendAsync(&kvserverpb.RaftMessageRequest{
		RangeID:     desc.RangeID,
		ToReplica:   replica1,
		FromReplica: replica0,
		Message: raftpb.Message{
			From: raftpb.PeerID(replica0.ReplicaID),
			To:   raftpb.PeerID(replica1.ReplicaID),
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
		case *kvpb.ReplicaTooOldError:
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
	defer log.Scope(t).Close(t)

	const numServers int = 4
	stickyServerArgs := make(map[int]base.TestServerArgs)
	for i := 0; i < numServers; i++ {
		stickyServerArgs[i] = base.TestServerArgs{
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:    true,
					StickyVFSID: strconv.FormatInt(int64(i), 10),
				},
			},
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					StickyVFSRegistry: fs.NewStickyRegistry(),
				},
				Store: &kvserver.StoreTestingKnobs{
					DisableScanner: true,
				},
			},
		}
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, numServers,
		base.TestClusterArgs{
			ReplicationMode:   base.ReplicationManual,
			ServerArgsPerNode: stickyServerArgs,
		})
	defer tc.Stopper().Stop(ctx)
	store := tc.GetFirstStoreFromServer(t, 0)

	// Replicate the range onto all of the nodes.
	key := []byte("a")
	_, desc := tc.SplitRangeOrFatal(t, key)
	tc.AddVotersOrFatal(t, key, tc.Targets(1, 2, 3)...)

	// Put some data in the range so we'll have something to test for.
	incArgs := incrementArgs(key, 5)
	if _, err := kv.SendWrapped(ctx, store.TestSender(), incArgs); err != nil {
		t.Fatal(err)
	}
	// Wait for all nodes to catch up.
	tc.WaitForValues(t, key, []int64{5, 5, 5, 5})

	// Verify store 3 has the replica.
	if _, err := tc.GetFirstStoreFromServer(t, 3).GetReplica(desc.RangeID); err != nil {
		t.Fatal(err)
	}

	// Stop node 3; while it is down remove the range from it. Since the node is
	// down it won't see the removal and won't clean up its replica.
	tc.StopServer(3)
	tc.RemoveVotersOrFatal(t, key, tc.Target(3))

	// Perform another write.
	incArgs = incrementArgs(key, 11)
	if _, err := kv.SendWrapped(ctx, store.TestSender(), incArgs); err != nil {
		t.Fatal(err)
	}
	tc.WaitForValues(t, key, []int64{16, 16, 16, 0 /* stopped */})

	// Wait for a bunch of raft ticks in order to flush any heartbeats through
	// the system. In particular, a coalesced heartbeat containing a quiesce
	// message could have been sent before the node was removed from range but
	// arrive after the node restarted.
	ticks := store.Metrics().RaftTicks.Count
	for targetTicks := ticks() + 5; ticks() < targetTicks; {
		time.Sleep(time.Millisecond)
	}

	// Restart node 3. The removed replica will start talking to the other
	// replicas and determine it needs to be GC'd.
	require.NoError(t, tc.RestartServer(3))

	// Because we lazily initialize Raft groups, we have to force the Raft group
	// to get created in order to get the replica talking to the other replicas.
	tc.GetFirstStoreFromServer(t, 3).EnqueueRaftUpdateCheck(desc.RangeID)

	testutils.SucceedsSoon(t, func() error {
		replica, err := tc.GetFirstStoreFromServer(t, 3).GetReplica(desc.RangeID)
		if err != nil {
			if errors.HasType(err, (*kvpb.RangeNotFoundError)(nil)) {
				return nil
			}
			return err
		} else if replica != nil {
			// Make sure the replica is unquiesced so that it will tick and
			// contact the leader to discover it's no longer part of the range.
			replica.MaybeUnquiesce()
		}
		return errors.Errorf("found %s, waiting for it to be GC'd", replica)
	})
}

func TestReplicateReAddAfterDown(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numServers int = 3
	stickyServerArgs := make(map[int]base.TestServerArgs)
	for i := 0; i < numServers; i++ {
		stickyServerArgs[i] = base.TestServerArgs{
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:    true,
					StickyVFSID: strconv.FormatInt(int64(i), 10),
				},
			},
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					StickyVFSRegistry: fs.NewStickyRegistry(),
				},
			},
		}
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, numServers,
		base.TestClusterArgs{
			ReplicationMode:   base.ReplicationManual,
			ServerArgsPerNode: stickyServerArgs,
		})
	defer tc.Stopper().Stop(ctx)
	store := tc.GetFirstStoreFromServer(t, 0)

	downedStoreIdx := 2

	// First put the range on all three nodes.
	key := []byte("a")
	tc.SplitRangeOrFatal(t, key)
	tc.AddVotersOrFatal(t, key, tc.Targets(1, 2)...)

	// Put some data in the range so we'll have something to test for.
	incArgs := incrementArgs(key, 5)
	if _, err := kv.SendWrapped(ctx, store.TestSender(), incArgs); err != nil {
		t.Fatal(err)
	}

	// Wait for all nodes to catch up.
	tc.WaitForValues(t, key, []int64{5, 5, 5})

	// Stop node 2; while it is down remove the range from it. Since the node is
	// down it won't see the removal and clean up its replica.
	tc.StopServer(downedStoreIdx)
	tc.RemoveVotersOrFatal(t, key, tc.Target(downedStoreIdx))

	// Perform another write.
	incArgs = incrementArgs(key, 11)
	if _, err := kv.SendWrapped(ctx, store.TestSender(), incArgs); err != nil {
		t.Fatal(err)
	}
	tc.WaitForValues(t, key, []int64{16, 16, 0 /* stopped */})

	// Bring it back up and re-add the range. There is a race when the
	// store applies its removal and re-addition back to back: the
	// replica may or may not have (asynchronously) garbage collected
	// its data in between. Whether the existing data is reused or the
	// replica gets recreated, the replica ID is changed by this
	// process. An ill-timed GC has been known to cause bugs including
	// https://github.com/cockroachdb/cockroach/issues/2873.
	require.NoError(t, tc.RestartServer(downedStoreIdx))
	tc.AddVotersOrFatal(t, key, tc.Target(downedStoreIdx))

	// The range should be synced back up.
	tc.WaitForValues(t, key, []int64{16, 16, 16})
}

// TestLeaseHolderRemoveSelf verifies that a lease holder cannot remove itself
// without encountering an error.
func TestLeaseHolderRemoveSelf(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 2,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)

	leaseHolder := tc.GetFirstStoreFromServer(t, 0)
	key := []byte("a")
	tc.SplitRangeOrFatal(t, key)
	tc.AddVotersOrFatal(t, key, tc.Target(1))

	// Attempt to remove the replica from first store.
	expectedErr := "invalid ChangeReplicasTrigger"
	if _, err := tc.RemoveVoters(key, tc.Target(0)); !testutils.IsError(err, expectedErr) {
		t.Fatalf("expected %q error trying to remove leaseholder replica; got %v", expectedErr, err)
	}

	// Expect that we can still successfully do a get on the range.
	getArgs := getArgs(key)
	_, pErr := kv.SendWrappedWith(ctx, leaseHolder.TestSender(), kvpb.Header{}, getArgs)
	if pErr != nil {
		t.Fatal(pErr)
	}
}

// TestVoterRemovalWithoutDemotion verifies that a voter replica cannot be
// removed directly without first being demoted to a learner.
func TestVoterRemovalWithoutDemotion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Inject a filter which skips the demotion of a voter replica when removing
	// it from the range. This will trigger the raft-level check which ensures
	// that a voter replica cannot be removed directly.
	type noDemotionKey struct{}
	var removalTarget roachpb.ReplicationTarget
	testingProposalFilter := func(args kvserverbase.ProposalFilterArgs) *kvpb.Error {
		if args.Ctx.Value(noDemotionKey{}) != nil {
			if state := args.Cmd.ReplicatedEvalResult.State; state != nil && state.Desc != nil {
				repl, ok := state.Desc.GetReplicaDescriptor(removalTarget.StoreID)
				if ok && repl.Type == roachpb.VOTER_DEMOTING_LEARNER {
					t.Logf("intercepting proposal, skipping voter demotion: %+v", args.Cmd)
					_, ok := state.Desc.RemoveReplica(repl.NodeID, repl.StoreID)
					require.True(t, ok)
				}
			}
		}
		return nil
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 2,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						TestingProposalFilter: testingProposalFilter,
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)
	removalTarget = tc.Target(1)

	key := []byte("a")
	tc.SplitRangeOrFatal(t, key)
	tc.AddVotersOrFatal(t, key, removalTarget)

	var beforeDesc roachpb.RangeDescriptor
	db := tc.Servers[0].SystemLayer().DB()
	require.NoError(t, db.GetProto(ctx, keys.RangeDescriptorKey(key), &beforeDesc))

	// First attempt to remove the voter without demotion. Should fail.
	expectedErr := "cannot remove voter .* directly; must first demote to learner"
	noDemotionCtx := context.WithValue(ctx, noDemotionKey{}, struct{}{})
	removeVoter := kvpb.MakeReplicationChanges(roachpb.REMOVE_VOTER, removalTarget)
	_, err := db.AdminChangeReplicas(noDemotionCtx, key, beforeDesc, removeVoter)
	require.Error(t, err)
	require.Regexp(t, expectedErr, err)

	// Now demote the voter to a learner and then remove it.
	desc, err := db.AdminChangeReplicas(ctx, key, beforeDesc, removeVoter)
	require.NoError(t, err)
	_, ok := desc.GetReplicaDescriptor(removalTarget.StoreID)
	require.False(t, ok)
}

// TestRemovedReplicaError verifies that a replica that has been removed from a
// range returns a RangeNotFoundError if it receives a request for that range
// (not RaftGroupDeletedError, and even before the ReplicaGCQueue has run).
func TestRemovedReplicaError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	manualClock := hlc.NewHybridManualClock()
	tc := testcluster.StartTestCluster(t, 2,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						WallClock: manualClock,
					},
					Store: &kvserver.StoreTestingKnobs{
						// Disable the replica GC queues. This verifies that the replica is
						// considered removed even before the gc queue has run, and also
						// helps avoid a deadlock at shutdown.
						DisableReplicaGCQueue: true,
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)
	store := tc.GetFirstStoreFromServer(t, 0)

	key := tc.ScratchRangeWithExpirationLease(t)
	desc := tc.AddVotersOrFatal(t, key, tc.Target(1))
	tc.TransferRangeLeaseOrFatal(t, desc, tc.Target(1))
	tc.RemoveVotersOrFatal(t, key, tc.Target(0))

	manualClock.Increment(store.GetStoreConfig().LeaseExpiration())

	// Expect to get a RangeNotFoundError. We have to allow for ambiguous result
	// errors to avoid the occasional test flake. Since we use demotions to remove
	// voters, the actual removal sees a learner, and so the learner is not in
	// the commit quorum for the removal itself. That is to say, we will only
	// start seeing the RangeNotFoundError after a little bit of time has passed.
	getArgs := getArgs(key)
	testutils.SucceedsSoon(t, func() error {
		_, pErr := kv.SendWrappedWith(ctx, store, kvpb.Header{}, getArgs)
		switch pErr.GetDetail().(type) {
		case *kvpb.AmbiguousResultError:
			return pErr.GoError()
		case *kvpb.NotLeaseHolderError:
			return pErr.GoError()
		case *kvpb.RangeNotFoundError:
			return nil
		default:
		}
		t.Fatal(pErr)
		return errors.New("unreachable")
	})
}

// Test that the Raft leadership is transferred to follow the lease.
func TestTransferRaftLeadership(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "lease-type", roachpb.TestingAllLeaseTypes(),
		func(t *testing.T, leaseType roachpb.LeaseType) {
			ctx := context.Background()
			st := cluster.MakeTestingClusterSettings()
			kvserver.OverrideDefaultLeaseType(ctx, &st.SV, leaseType)

			tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
				ReplicationMode: base.ReplicationManual,
				ServerArgs: base.TestServerArgs{
					Settings: st,
				},
			})
			defer tc.Stopper().Stop(ctx)
			store0 := tc.GetFirstStoreFromServer(t, 0)
			store1 := tc.GetFirstStoreFromServer(t, 1)

			key := tc.ScratchRangeWithExpirationLease(t)
			repl0 := store0.LookupReplica(keys.MustAddr(key))
			require.NotNil(t, repl0, "no replica found for key '%s'", key)
			rd0, err := repl0.GetReplicaDescriptor()
			require.NoError(t, err)

			tc.AddVotersOrFatal(t, key, tc.Target(1))

			// NB: if we don't wait until node 2 applies the config change and becomes
			// voter, it may refuse to campaign for leadership. The large Raft election
			// timeout set in this test will prevent the current leader from retrying the
			// transfer. See issue #99213.
			require.NoError(t, tc.WaitForVoters(key, tc.Target(1)))

			repl1 := store1.LookupReplica(keys.MustAddr(key))
			require.NotNil(t, repl1, "no replica found for key '%s'", key)
			rd1, err := repl1.GetReplicaDescriptor()
			require.NoError(t, err)
			require.Equal(t, roachpb.VOTER_FULL, rd1.Type)

			_, pErr := kv.SendWrappedWith(ctx, store0, kvpb.Header{RangeID: repl0.RangeID}, getArgs(key))
			require.NoError(t, pErr.GoError())

			status := repl0.RaftStatus()
			require.NotNil(t, status)
			require.Equal(t, raftpb.PeerID(rd0.ReplicaID), status.Lead)

			origCount0 := store0.Metrics().RangeRaftLeaderTransfers.Count()
			// Transfer the lease. We'll then check that the leadership follows
			// automatically.
			transferLeaseArgs := adminTransferLeaseArgs(key, store1.StoreID())
			_, pErr = kv.SendWrappedWith(ctx, store0, kvpb.Header{RangeID: repl0.RangeID}, transferLeaseArgs)
			require.NoError(t, pErr.GoError())

			// Verify leadership is transferred.
			testutils.SucceedsSoon(t, func() error {
				if status := repl0.RaftStatus(); status == nil {
					return errors.New("raft status is nil")
				} else if a, e := status.Lead, raftpb.PeerID(rd1.ReplicaID); a != e {
					return errors.Errorf("expected raft leader be %d; got %d", e, a)
				}
				return nil
			})
			// And metrics are updated.
			require.Greater(t, store0.Metrics().RangeRaftLeaderTransfers.Count(), origCount0)
		})
}

// Test that a single blocked replica does not block other replicas.
func TestRaftBlockedReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						DisableScanner: true,
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)
	store := tc.GetFirstStoreFromServer(t, 0)

	lhsDesc, rhsDesc := tc.SplitRangeOrFatal(t, roachpb.Key("b"))

	// Create 2 ranges by splitting range 1.
	// Replicate range 1 to all 3 nodes. This ensures the usage of the network.
	tc.AddVotersOrFatal(t, lhsDesc.StartKey.AsRawKey(), tc.Targets(1, 2)...)

	// Lock range 2 for raft processing.
	rep, err := store.GetReplica(rhsDesc.RangeID)
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
	ticks := store.Metrics().RaftTicks.Count
	for targetTicks := ticks() + 3; ticks() < targetTicks; {
		time.Sleep(time.Millisecond)
	}

	// Verify we can still perform operations on the non-blocked replica.
	incArgs := incrementArgs([]byte("a"), 5)
	if _, err := kv.SendWrapped(ctx, store.TestSender(), incArgs); err != nil {
		t.Fatal(err)
	}
	tc.WaitForValues(t, roachpb.Key("a"), []int64{5, 5, 5})
}

// TestFollowersFallAsleep tests that followers fall asleep, and if the leader
// crashes, the followers wake up and elects a new leader.
func TestFollowersFallAsleep(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	// Only epoch based leases can be quiesced.
	kvserver.OverrideDefaultLeaseType(ctx, &st.SV, roachpb.LeaseLeader)
	kvserver.RaftStoreLivenessQuiescenceEnabled.Override(ctx, &st.SV, true)

	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Settings: st,
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						DisableScanner: true,
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)

	key := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, key, tc.Targets(1, 2)...)

	// Find the leader replica.
	oldLeader := tc.GetRaftLeader(t, roachpb.RKey(key))
	oldLeaderServerIdx := int(oldLeader.NodeID()) - 1

	checkSleep := func(expected bool) {
		testutils.SucceedsSoon(t, func() error {
			for i := range tc.Servers {
				rep := tc.GetFirstStoreFromServer(t, i).LookupReplica(roachpb.RKey(key))
				require.NotNil(t, rep)
				isLeader := rep.ID() == oldLeader.ID()
				if isLeader {
					require.False(t, rep.IsAsleep())
				} else {
					if expected && !rep.IsAsleep() {
						return errors.Errorf("%s not asleep yet", rep)
					}
					if !expected && rep.IsAsleep() {
						return errors.Errorf("%s not awake yet", rep)
					}
				}
			}
			return nil
		})
	}

	// Wait for the followers to fall asleep.
	checkSleep(true /* expected */)

	// Stop the leader, and ensure a new leader is elected.
	tc.StopServer(oldLeaderServerIdx)
	checkSleep(false /* expected */)
	testutils.SucceedsSoon(t, func() error {
		newLeader := tc.GetRaftLeader(t, roachpb.RKey(key))
		if oldLeader.ID() == newLeader.ID() {
			return errors.Errorf("no new leader yet")
		}
		return nil
	})
}

// TestUninitializedReplicaQuiescence tests the uninitialized replica quiescence
// behavior with various lease types.
func TestUninitializedReplicaQuiescence(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "leaseType", roachpb.TestingAllLeaseTypes(), func(t *testing.T, leaseType roachpb.LeaseType) {
		ctx := context.Background()
		st := cluster.MakeTestingClusterSettings()
		kvserver.OverrideDefaultLeaseType(ctx, &st.SV, leaseType)
		kvserver.RaftStoreLivenessQuiescenceEnabled.Override(ctx, &st.SV, true)

		tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Settings: st,
			},
		})
		defer tc.Stopper().Stop(ctx)

		_, desc, err := tc.Servers[0].ScratchRangeEx()
		key := desc.StartKey.AsRawKey()
		require.NoError(t, err)
		require.NoError(t, tc.WaitForSplitAndInitialization(key))

		// Block incoming snapshots on s2 until channel is signaled.
		blockSnapshot := make(chan struct{})
		handlerFuncs := noopRaftHandlerFuncs()
		handlerFuncs.snapErr = func(header *kvserverpb.SnapshotRequest_Header) error {
			select {
			case <-blockSnapshot:
			case <-tc.Stopper().ShouldQuiesce():
			}
			return nil
		}
		s2, err := tc.Server(1).GetStores().(*kvserver.Stores).GetStore(tc.Server(1).GetFirstStoreID())
		require.NoError(t, err)
		tc.Servers[1].RaftTransport().(*kvserver.RaftTransport).ListenIncomingRaftMessages(s2.StoreID(), &unreliableRaftHandler{
			rangeID:                    desc.RangeID,
			IncomingRaftMessageHandler: s2,
			unreliableRaftHandlerFuncs: handlerFuncs,
		})

		// Try to up-replicate to s2. Should block on a learner snapshot after the new
		// replica on s2 has been created, but before it has been initialized. While
		// the replica is uninitialized, it should remain quiesced, even while it is
		// receiving Raft traffic from the leader.
		replicateErrChan := make(chan error)
		go func() {
			_, err := tc.AddVoters(key, tc.Target(1))
			select {
			case replicateErrChan <- err:
			case <-tc.Stopper().ShouldQuiesce():
			}
		}()
		testutils.SucceedsSoon(t, func() error {
			repl, err := s2.GetReplica(desc.RangeID)
			if err == nil {
				// IMPORTANT: the replica should always be quiescent while uninitialized.
				require.False(t, repl.IsInitialized())
				require.True(t, repl.IsQuiescent())
				// But it's always awake for the purposes of store liveness quiescence.
				require.False(t, repl.IsAsleep())
			}
			return err
		})

		// Let the snapshot through. The up-replication attempt should succeed. The
		// replica should now be initialized, and the replica should quiesce again
		// if it has an epoch based lease. If it holds a leader lease it should
		// also fall asleep. Otherwise, if it holds an expiration based lease, it
		// shouldn't quiesce or fall asleep.
		close(blockSnapshot)
		require.NoError(t, <-replicateErrChan)
		repl, err := s2.GetReplica(desc.RangeID)
		require.NoError(t, err)
		require.True(t, repl.IsInitialized())
		require.False(t, repl.IsQuiescent())
		switch leaseType {
		case roachpb.LeaseEpoch:
			testutils.SucceedsSoon(t, func() error {
				if !repl.IsQuiescent() {
					return errors.Errorf("%s not quiescent", repl)
				}
				return nil
			})
		case roachpb.LeaseLeader:
			require.Never(t, func() bool {
				return repl.IsQuiescent()
			},
				time.Second*3, 100*time.Millisecond, "replica shouldn't quiesce")
			testutils.SucceedsSoon(t, func() error {
				if !repl.IsAsleep() {
					return errors.Errorf("%s not asleep", repl)
				}
				return nil
			})
		case roachpb.LeaseExpiration:
			require.Never(t, func() bool {
				return repl.IsQuiescent() || repl.IsAsleep()
			},
				time.Second*3, 100*time.Millisecond, "replica shouldn't quiesce or fall asleep")
		default:
			panic("unexpected lease type")
		}
	})
}

// TestFailedConfChange verifies correct behavior after a configuration change
// experiences an error when applying EndTxn. Specifically, it verifies that
// https://github.com/cockroachdb/cockroach/issues/13506 has been fixed.
func TestFailedConfChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Trigger errors at apply time so they happen on both leaders and
	// followers.
	var filterActive int32
	testingApplyFilter := func(filterArgs kvserverbase.ApplyFilterArgs) (int, *kvpb.Error) {
		if atomic.LoadInt32(&filterActive) == 1 && filterArgs.ChangeReplicas != nil {
			return 0, kvpb.NewErrorf("boom")
		}
		return 0, nil
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						TestingApplyCalledTwiceFilter: testingApplyFilter,
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)

	key := tc.ScratchRange(t)
	desc := tc.AddVotersOrFatal(t, key, tc.Target(1))
	if err := tc.WaitForVoters(key, tc.Target(1)); err != nil {
		t.Fatal(err)
	}

	// Try and fail to replicate it to the third node.
	atomic.StoreInt32(&filterActive, 1)
	if _, err := tc.AddVoters(key, tc.Target(2)); !testutils.IsError(err, "boom") {
		t.Fatal(err)
	}

	// Raft state is only exposed on the leader, so we must transfer
	// leadership and check the stores one at a time.
	checkLeaderStore := func(i int) error {
		store := tc.GetFirstStoreFromServer(t, i)
		repl, err := store.GetReplica(desc.RangeID)
		if err != nil {
			t.Fatal(err)
		}
		if l := len(repl.Desc().InternalReplicas); l != 2 {
			return errors.Errorf("store %d: expected 2 replicas in descriptor, found %d in %s",
				i, l, repl.Desc())
		}
		status := repl.RaftStatus()
		if status.RaftState != raftpb.StateLeader {
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
	tc.TransferRangeLeaseOrFatal(t, desc, tc.Target(1))

	testutils.SucceedsSoon(t, func() error {
		repl, err := tc.GetFirstStoreFromServer(t, 1).GetReplica(desc.RangeID)
		if err != nil {
			return err
		}
		status := repl.RaftStatus()
		if status.RaftState != raftpb.StateLeader {
			return errors.Errorf("store %d: expected StateLeader, was %s", 1, status.RaftState)
		}
		return nil
	})

	if err := checkLeaderStore(1); err != nil {
		t.Fatal(err)
	}
}

func TestStoreRangeWaitForApplication(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "lease-type", roachpb.TestingAllLeaseTypes(),
		func(t *testing.T, leaseType roachpb.LeaseType) {
			var filterRangeIDAtomic int64

			ctx := context.Background()
			testingRequestFilter := func(_ context.Context, ba *kvpb.BatchRequest) (retErr *kvpb.Error) {
				if rangeID := roachpb.RangeID(atomic.LoadInt64(&filterRangeIDAtomic)); rangeID != ba.RangeID {
					return nil
				}
				pErr := kvpb.NewErrorf("blocking %s in this test", ba.Summary())
				if len(ba.Requests) != 1 {
					return pErr
				}
				_, ok := ba.Requests[0].GetInner().(*kvpb.PutRequest)
				if !ok {
					return pErr
				}
				return nil
			}

			settings := cluster.MakeTestingClusterSettings()
			kvserver.OverrideDefaultLeaseType(ctx, &settings.SV, leaseType)
			tc := testcluster.StartTestCluster(t, 3,
				base.TestClusterArgs{
					ReplicationMode: base.ReplicationManual,
					ServerArgs: base.TestServerArgs{
						Settings: settings,
						Knobs: base.TestingKnobs{
							Store: &kvserver.StoreTestingKnobs{
								TestingRequestFilter: testingRequestFilter,
								// This test relies on a stable LAI, so we need to disable GC
								// queue and async intent resolution to avoid the LAI being
								// incremented in the middle of the test.
								DisableReplicaGCQueue: true,
								IntentResolverKnobs: kvserverbase.IntentResolverTestingKnobs{
									DisableAsyncIntentResolution: true,
								},
							},
						},
					},
				})
			defer tc.Stopper().Stop(ctx)

			store0, store2 := tc.GetFirstStoreFromServer(t, 0), tc.GetFirstStoreFromServer(t, 2)
			distSender := tc.Servers[0].DistSenderI().(kv.Sender)

			key := []byte("a")
			tc.SplitRangeOrFatal(t, key)
			desc := tc.AddVotersOrFatal(t, key, tc.Targets(1, 2)...)

			repl0, err := store0.GetReplica(desc.RangeID)
			if err != nil {
				t.Fatal(err)
			}

			// We wait for lease upgrade to avoid the lease being upgraded after we
			// capture the LAI.
			tc.MaybeWaitForLeaseUpgrade(ctx, t, desc)
			leaseIndex0 := repl0.GetLeaseAppliedIndex()

			atomic.StoreInt64(&filterRangeIDAtomic, int64(desc.RangeID))
			type target struct {
				client kvserver.PerReplicaClient
				header kvserver.StoreRequestHeader
			}

			var targets []target
			for _, s := range tc.Servers {
				conn, err := s.NodeDialer().(*nodedialer.Dialer).Dial(ctx, s.NodeID(), rpc.DefaultClass)
				if err != nil {
					t.Fatal(err)
				}
				targets = append(targets, target{
					client: kvserver.NewPerReplicaClient(conn),
					header: kvserver.StoreRequestHeader{NodeID: s.NodeID(), StoreID: s.GetFirstStoreID()},
				})
			}

			// Wait for a command that is already applied. The request should return
			// immediately.
			for i, target := range targets {
				_, err := target.client.WaitForApplication(ctx, &kvserver.WaitForApplicationRequest{
					StoreRequestHeader: target.header,
					RangeID:            desc.RangeID,
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
						RangeID:            desc.RangeID,
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
					RangeID:            desc.RangeID,
					LeaseIndex:         math.MaxInt64,
				})
				errChs[2] <- err
			}()
			repl2, err := store2.GetReplica(desc.RangeID)
			if err != nil {
				t.Fatal(err)
			}
			tc.RemoveVotersOrFatal(t, key, tc.Target(2))
			if err := store2.ManualReplicaGC(repl2); err != nil {
				t.Fatal(err)
			}
			if _, err := repl2.IsDestroyed(); err == nil {
				t.Fatalf("replica was not destroyed after gc on store2")
			}
			err = <-errChs[2]
			if exp := fmt.Sprintf("r%d was not found", desc.RangeID); !testutils.IsError(err, exp) {
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
					RangeID:            desc.RangeID,
					LeaseIndex:         math.MaxInt64,
				})
				if exp := "context deadline exceeded"; !testutils.IsError(err, exp) {
					t.Fatalf("expected %q error, but got %v", exp, err)
				}
			}
		})
}

func TestStoreWaitForReplicaInit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)
	store := tc.GetFirstStoreFromServer(t, 0)

	conn, err := tc.Servers[0].NodeDialer().(*nodedialer.Dialer).Dial(ctx, store.Ident.NodeID, rpc.DefaultClass)
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
			RangeID:            roachpb.RangeID(999),
		})
		if exp := "context deadline exceeded"; !testutils.IsError(err, exp) {
			t.Fatalf("expected %q error, but got %v", exp, err)
		}
	}

	// Test that WaitForReplicaInit times out if the replica exists but is not
	// initialized.
	{
		// Constructing a permanently-uninitialized replica is somewhat difficult.
		// Sending a fake Raft heartbeat for a range ID that the store hasn't seen
		// before does the trick.
		const unusedRangeID = 1234
		var repl *kvserver.Replica
		testutils.SucceedsSoon(t, func() (err error) {
			// Try several times, as the message may be dropped (see #18355).
			tc.Servers[0].RaftTransport().(*kvserver.RaftTransport).SendAsync(&kvserverpb.RaftMessageRequest{
				ToReplica: roachpb.ReplicaDescriptor{
					NodeID:  store.Ident.NodeID,
					StoreID: store.Ident.StoreID,
				},
				Heartbeats: []kvserverpb.RaftHeartbeat{{RangeID: unusedRangeID, ToReplicaID: 1}},
			}, rpc.DefaultClass)
			repl, err = store.GetReplica(unusedRangeID)
			return err
		})
		if repl.IsInitialized() {
			t.Fatalf("test bug: range %d is initialized", unusedRangeID)
		}

		timeoutCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		_, err = client.WaitForReplicaInit(timeoutCtx, &kvserver.WaitForReplicaInitRequest{
			StoreRequestHeader: storeHeader,
			RangeID:            roachpb.RangeID(unusedRangeID),
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
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						TraceAllRaftEvents: true,
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)
	store := tc.GetFirstStoreFromServer(t, 0)

	db := store.DB()
	// Make the transport flaky for the range in question to encourage proposals
	// to be sent more times and ultimately traced more.
	ri, err := getRangeInfo(ctx, db, roachpb.Key("foo"))
	require.Nil(t, err)

	for i := 0; i < 3; i++ {
		tc.Servers[i].RaftTransport().(*kvserver.RaftTransport).ListenIncomingRaftMessages(tc.Target(i).StoreID, &unreliableRaftHandler{
			rangeID:                    ri.Desc.RangeID,
			IncomingRaftMessageHandler: tc.GetFirstStoreFromServer(t, i),
			unreliableRaftHandlerFuncs: unreliableRaftHandlerFuncs{
				dropReq: func(req *kvserverpb.RaftMessageRequest) bool {
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
			ctxWithTimeout, cancel := context.WithTimeout(ctx, delay)
			defer cancel()
			_ = db.Put(ctxWithTimeout, roachpb.Key(fmt.Sprintf("foo%d", i)), val)
		}
	}
	const N = 256
	for i := 0; i < N; i++ {
		go put(i)()
	}
	wg.Wait()
}

// disablingClientStream allows delaying rRPC messages based on a user provided
// function. It is OK to arbitrarily delay messages, but if they are dropped, it
// breaks application level expectations of in-order delivery. By returning nil
// immediately, and then sending when the stream is not disabled, we don't break
// the SendMsg contract. Note that this test is still a little too high level in
// the sense that it is blocking at the gRPC layer, and not the TCP layer, but
// that is much more complex to fully implement, and this should get equivalent
// results.
type disablingClientStream struct {
	grpc.ClientStream
	wasDisabled bool
	buffer      []interface{}
	disabled    func() bool
}

func (cs *disablingClientStream) SendMsg(m interface{}) error {
	// When the stream is disabled, buffer all the messages, but don't send.
	if cs.disabled() {
		cs.buffer = append(cs.buffer, m)
		cs.wasDisabled = true
		return nil
	}
	// Now that it transitioned from disabled to not disabled, flush all the
	// messages out in the same order as originally expected.
	if cs.wasDisabled {
		for _, buf := range cs.buffer {
			_ = cs.ClientStream.SendMsg(buf)
		}
		cs.buffer = nil
		cs.wasDisabled = false
	}

	return cs.ClientStream.SendMsg(m)
}

// TestDefaultConnectionDisruptionDoesNotInterfereWithSystemTraffic tests that
// disconnection on connections of the rpc.DefaultClass do not interfere with
// traffic on the SystemClass connection.
func TestDefaultConnectionDisruptionDoesNotInterfereWithSystemTraffic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stickyVFSRegistry := fs.NewStickyRegistry()
	lisReg := listenerutil.NewListenerRegistry()
	defer lisReg.Close()

	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)

	// disabled controls whether to disrupt DefaultClass streams.
	var disabled, disabledSystem atomic.Value
	disabled.Store(false)
	disabledSystem.Store(false)
	knobs := rpc.ContextTestingKnobs{
		StreamClientInterceptor: func(target string, class rpc.ConnectionClass) grpc.StreamClientInterceptor {
			disabledFunc := func() bool {
				if class == rpc.SystemClass {
					return disabledSystem.Load().(bool)
				}
				return disabled.Load().(bool)
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
					disabled:     disabledFunc,
					ClientStream: cs,
				}, nil
			}
		},
	}

	// This test relies on epoch leases being invalidated when a node restart,
	// which isn't true for expiration leases, so we disable expiration lease
	// metamorphism.
	st := cluster.MakeTestingClusterSettings()
	kvserver.ExpirationLeasesOnly.Override(ctx, &st.SV, false) // override metamorphism

	const numServers int = 3
	stickyServerArgs := make(map[int]base.TestServerArgs)
	for i := 0; i < numServers; i++ {
		stickyServerArgs[i] = base.TestServerArgs{
			Settings: st,
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:    true,
					StickyVFSID: strconv.FormatInt(int64(i), 10),
				},
			},
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					ContextTestingKnobs: knobs,
					StickyVFSRegistry:   stickyVFSRegistry,
				},
			},
		}
	}

	tc := testcluster.StartTestCluster(t, numServers,
		base.TestClusterArgs{
			ReplicationMode:     base.ReplicationManual,
			ReusableListenerReg: lisReg,
			ServerArgsPerNode:   stickyServerArgs,
		})
	defer tc.Stopper().Stop(ctx)
	// Make a key that's in the user data space.
	keyA := append(keys.SystemSQLCodec.TablePrefix(100), 'a')
	// Split so that we can assign voters to the range and assign all three.
	tc.SplitRangeOrFatal(t, keyA)
	tc.AddVotersOrFatal(t, keyA, tc.Targets(1, 2)...)

	// We need a key in the meta range that we can add voters to. This range can't be split.
	keyLiveness := append(keys.NodeLivenessPrefix, 'a')
	tc.AddVotersOrFatal(t, keys.NodeLivenessPrefix, tc.Targets(1, 2)...)
	// Create a test function so that we can run the test both immediately after
	// up-replicating and after a restart.
	runTest := func(t *testing.T) {
		store := tc.GetFirstStoreFromServer(t, 0)
		// Put some data in the range so we'll have something to test for.
		db := store.DB()
		require.NoError(t, db.Put(ctx, keyA, 1))

		// Wait for all nodes to catch up.
		tc.WaitForValues(t, keyA, []int64{1, 1, 1})
		disabled.Store(true)

		// Create a pair of utilities to perform operations under suitable timeouts.
		expSucceed := func(msg string, fn func(context.Context) error) {
			// Set a long timeout so that this test doesn't flake under stress.
			// We should never hit it.
			ctxTimeout, cancel := context.WithTimeout(ctx, testutils.DefaultSucceedsSoonDuration)
			defer cancel()
			require.NoError(t, fn(ctxTimeout), "Expected success "+msg)
		}
		expTimeout := func(msg string, fn func(context.Context) error) {
			// Set a relatively short timeout so that this test doesn't take too long.
			// We should always hit it.
			ctxTimeout, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer cancel()
			require.ErrorIs(t, fn(ctxTimeout), context.DeadlineExceeded, "Expected timeout "+msg)
		}

		// Write to the liveness range on the System class.
		expSucceed("writing to liveness range", func(ctx context.Context) error {
			return db.Put(ctx, keyLiveness, 2)
		})

		// Write to the standard range on the default class.
		expTimeout("writing to key range", func(ctx context.Context) error {
			return db.Put(ctx, keyA, 2)
		})

		// Write to the liveness range on the System class with system disabled to
		// ensure the test is actually working.
		disabledSystem.Store(true)
		expTimeout("writing to liveness range", func(ctx context.Context) error {
			return db.Put(ctx, keyLiveness, 2)
		})
		disabledSystem.Store(false)

		// Heal the partition, the previous proposal may now succeed but it also may
		// have been canceled.
		disabled.Store(false)
		// Overwrite with a new value and ensure that it propagates.
		expSucceed("after healed partition", func(ctx context.Context) error {
			return db.Put(ctx, keyA, 3)
		})
		tc.WaitForValues(t, keyA, []int64{3, 3, 3})
	}
	t.Run("initial_run", runTest)
	require.NoError(t, tc.Restart())
	t.Run("after_restart", runTest)
}

// TestAckWriteBeforeApplication tests that the success of transactional writes
// is acknowledged after those writes have been committed to a Range's Raft log
// but before those writes have been applied to its replicated state machine.
func TestAckWriteBeforeApplication(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	for _, testcase := range []struct {
		repls            int
		expAckBeforeAppl bool
	}{

		// In a three-replica Range, each handleRaftReady iteration will append
		// a set of entries to the Raft log and then apply the previous set of
		// entries. This makes "early acknowledgement" a major optimization, as
		// it pulls the entire latency required to append the next set of entries
		// to the Raft log out of the client-perceived latency of the previous
		// set of entries.
		{3, true},
		// In the past, single-replica groups behaved differently but as of #89632
		// they too rely on early-acks as a major performance improvement.
		{1, true},
	} {
		t.Run(fmt.Sprintf("numRepls=%d", testcase.repls), func(t *testing.T) {
			var filterActive int32
			var magicTS hlc.Timestamp
			blockPreApplication, blockPostApplication := make(chan struct{}), make(chan struct{})
			applyFilterFn := func(ch chan struct{}) kvserverbase.ReplicaApplyFilter {
				return func(filterArgs kvserverbase.ApplyFilterArgs) (int, *kvpb.Error) {
					if atomic.LoadInt32(&filterActive) == 1 && filterArgs.WriteTimestamp == magicTS {
						<-ch
					}
					return 0, nil
				}
			}

			ctx := context.Background()
			tc := testcluster.StartTestCluster(t, testcase.repls,
				base.TestClusterArgs{
					ReplicationMode: base.ReplicationManual,
					ServerArgs: base.TestServerArgs{
						Knobs: base.TestingKnobs{
							Store: &kvserver.StoreTestingKnobs{
								TestingApplyCalledTwiceFilter: applyFilterFn(blockPreApplication),
								TestingPostApplyFilter:        applyFilterFn(blockPostApplication),
							},
						},
					},
				})
			defer tc.Stopper().Stop(ctx)

			// Replicate the Range, if necessary.
			key := roachpb.Key("a")
			tc.SplitRangeOrFatal(t, key)
			for i := 1; i < testcase.repls; i++ {
				tc.AddVotersOrFatal(t, key, tc.Target(i))
			}

			// Begin peforming a write on the Range.
			magicTS = tc.Servers[0].Clock().Now()
			atomic.StoreInt32(&filterActive, 1)
			ch := make(chan *kvpb.Error, 1)
			go func() {
				put := putArgs(key, []byte("val"))
				_, pErr := kv.SendWrappedWith(ctx, tc.GetFirstStoreFromServer(t, 0).TestSender(), kvpb.Header{
					Timestamp: magicTS,
				}, put)
				ch <- pErr
			}()

			expResult := func() {
				t.Helper()
				if pErr := <-ch; pErr != nil {
					t.Errorf("unexpected proposal result error: %v", pErr)
				}
			}
			dontExpResult := func() {
				t.Helper()
				select {
				case <-time.After(10 * time.Millisecond):
					// Expected.
				case pErr := <-ch:
					t.Errorf("unexpected proposal acknowledged before TestingApplyCalledTwiceFilter: %v", pErr)
				}
			}

			// The result should be blocked on the pre-apply filter.
			dontExpResult()

			// Release the pre-apply filter.
			close(blockPreApplication)
			// Depending on the cluster configuration, The result may not be blocked
			// on the post-apply filter because it may be able to acknowledges the
			// client before applying.
			if testcase.expAckBeforeAppl {
				expResult()
			} else {
				dontExpResult()
			}

			// Stop blocking Raft application to allow everything to shut down cleanly.
			// This also confirms that the proposal does eventually apply.
			close(blockPostApplication)
			// If we didn't expect an acknowledgement before, we do now.
			if !testcase.expAckBeforeAppl {
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
//   - In all cases we begin with s1 processing a presplit snapshot for
//     r20. After the split the store should have r21/3.
//
// In the first two cases the following occurs:
//
//   - s1 receives a message for r21/3 prior to acquiring the split lock
//     in r21. This will create an uninitialized r21/3 which may write
//     HardState.
//
//   - Before the r20 processes the split r21 is removed and re-added to
//     s1 as r21/4. s1 receives a raft message destined for r21/4 and proceeds
//     to destroy its uninitialized r21/3, laying down a tombstone at 4 in the
//     process.
//
//     (1) s1 processes the split and finds the RHS to be an uninitialized replica
//     with a higher replica ID.
//
//     (2) s1 crashes before processing the split, forgetting the replica ID of the
//     RHS but retaining its tombstone.
//
// In both cases we know that the RHS could not have committed anything because
// it cannot have gotten a snapshot but we want to be sure to not synthesize a
// HardState for the RHS that contains a non-zero commit index if we know that
// the RHS will need another snapshot later.
//
// In the third and fourth cases:
//
//   - s1 never receives a message for r21/3.
//
//   - Before the r20 processes the split r21 is removed and re-added to
//     s1 as r21/4. s1 receives a raft message destined for r21/4 and has never
//     heard about r21/3.
//
//     (3) s1 processes the split and finds the RHS to be an uninitialized replica
//     with a higher replica ID (but without a tombstone). This case is very
//     similar to (1)
//
//     (4) s1 crashes still before processing the split, forgetting that it had
//     known about r21/4. When it reboots r21/4 is totally partitioned and
//     r20 becomes unpartitioned.
//
//   - r20 processes the split successfully and initialized r21/3.
//
// In the 4th case we find that until we unpartition r21/4 (the RHS) and let it
// learn about its removal with a ReplicaTooOldError that it will be initialized
// with a CommitIndex at 10 as r21/3, the split's value. After r21/4 becomes
// unpartitioned it will learn it is removed by either catching up on its
// its log or receiving a ReplicaTooOldError which will lead to a tombstone.
func TestProcessSplitAfterRightHandSideHasBeenRemoved(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	testutils.RunValues(t, "lease-type", roachpb.TestingAllLeaseTypes(), func(t *testing.T, leaseType roachpb.LeaseType) {
		noopProposalFilter := kvserverbase.ReplicaProposalFilter(func(args kvserverbase.ProposalFilterArgs) *kvpb.Error {
			return nil
		})
		var proposalFilter atomic.Value
		proposalFilter.Store(noopProposalFilter)
		testingProposalFilter := func(args kvserverbase.ProposalFilterArgs) *kvpb.Error {
			return proposalFilter.Load().(kvserverbase.ReplicaProposalFilter)(args)
		}

		increment := func(t *testing.T, db *kv.DB, key roachpb.Key, by int64) {
			b := &kv.Batch{}
			b.AddRawRequest(incrementArgs(key, by))
			require.NoError(t, db.Run(ctx, b))
		}
		ensureNoTombstone := func(t *testing.T, store *kvserver.Store, rangeID roachpb.RangeID) {
			t.Helper()
			var tombstone kvserverpb.RangeTombstone
			tombstoneKey := keys.RangeTombstoneKey(rangeID)
			ok, err := storage.MVCCGetProto(
				ctx, store.TODOEngine(), tombstoneKey, hlc.Timestamp{}, &tombstone, storage.MVCCGetOptions{},
			)
			require.NoError(t, err)
			require.False(t, ok)
		}
		getHardState := func(
			t *testing.T, store *kvserver.Store, rangeID roachpb.RangeID,
		) raftpb.HardState {
			hs, err := stateloader.Make(rangeID).LoadHardState(ctx, store.TODOEngine())
			require.NoError(t, err)
			return hs
		}
		partitionReplicaOnSplit := func(t *testing.T, tc *testcluster.TestCluster, key roachpb.Key, basePartition *testClusterPartitionedRange, partRange **testClusterPartitionedRange) {
			// Set up a hook to partition the RHS range at its initial range ID
			// before proposing the split trigger.
			var setupOnce sync.Once
			f := kvserverbase.ReplicaProposalFilter(func(args kvserverbase.ProposalFilterArgs) *kvpb.Error {
				req, ok := args.Req.GetArg(kvpb.EndTxn)
				if !ok {
					return nil
				}
				endTxn := req.(*kvpb.EndTxnRequest)
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
					*partRange, err = basePartition.extend(tc, split.RightDesc.RangeID, replDesc.ReplicaID,
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
			tc *testcluster.TestCluster,
			db *kv.DB,
			keyA, keyB roachpb.Key,
			lhsID roachpb.RangeID,
			lhsPartition *testClusterPartitionedRange,
		) {
			lisReg := listenerutil.NewListenerRegistry()
			const numServers int = 3
			stickyServerArgs := make(map[int]base.TestServerArgs)
			for i := 0; i < numServers; i++ {
				st := cluster.MakeTestingClusterSettings()
				kvserver.OverrideDefaultLeaseType(ctx, &st.SV, leaseType)

				stickyServerArgs[i] = base.TestServerArgs{
					Settings: st,
					StoreSpecs: []base.StoreSpec{
						{
							InMemory:    true,
							StickyVFSID: strconv.FormatInt(int64(i), 10),
						},
					},
					Knobs: base.TestingKnobs{
						Server: &server.TestingKnobs{
							StickyVFSRegistry: fs.NewStickyRegistry(),
						},
						Store: &kvserver.StoreTestingKnobs{
							// Newly-started stores (including the "rogue" one) should not GC
							// their replicas. We'll turn this back on when needed.
							DisableReplicaGCQueue: true,
							TestingProposalFilter: testingProposalFilter,
						},
					},
					RaftConfig: base.RaftConfig{
						// Make the tick interval short so we don't need to wait too long for the
						// partitioned leader to time out.
						RaftTickInterval: 10 * time.Millisecond,
						// Make the lease duration a little shorter to make the test finish
						// faster with leader leases.
						RangeLeaseDuration: 1000 * time.Millisecond,
					},
				}
			}

			tc = testcluster.StartTestCluster(t, numServers,
				base.TestClusterArgs{
					ReplicationMode:     base.ReplicationManual,
					ReusableListenerReg: lisReg,
					ServerArgsPerNode:   stickyServerArgs,
				})

			tc.Stopper().AddCloser(stop.CloserFn(lisReg.Close))
			db = tc.GetFirstStoreFromServer(t, 1).DB()

			// Split off a non-system range so we don't have to account for node liveness
			// traffic.
			scratchTableKey := tc.ScratchRangeWithExpirationLease(t)
			// Put some data in the range so we'll have something to test for.
			keyA = append(append(roachpb.Key{}, scratchTableKey...), 'a')
			keyB = append(append(roachpb.Key{}, scratchTableKey...), 'b')
			// First put the range on all three nodes.
			desc := tc.AddVotersOrFatal(t, scratchTableKey, tc.Targets(1, 2)...)

			// Set up a partition for the LHS range only. Initially it is not active.
			lhsPartition, err := setupPartitionedRange(tc, desc.RangeID,
				0 /* replicaID */, 0 /* partitionedNode */, false /* activated */, unreliableRaftHandlerFuncs{})
			require.NoError(t, err)
			// Wait for all nodes to catch up.
			increment(t, db, keyA, 5)
			tc.WaitForValues(t, keyA, []int64{5, 5, 5})

			// Transfer the lease off of node 0.
			tc.TransferRangeLeaseOrFatal(t, desc, tc.Target(2))

			// Make sure everybody knows about that transfer.
			increment(t, db, keyA, 1)
			tc.WaitForValues(t, keyA, []int64{6, 6, 6})
			lhsPartition.activate()

			increment(t, db, keyA, 1)
			tc.WaitForValues(t, keyA, []int64{6, 7, 7})
			return tc, db, keyA, keyB, lhsID, lhsPartition
		}

		// In this case we only have the LHS partitioned. The RHS will learn about its
		// identity as the replica in the split and after being re-added will learn
		// about the new replica ID and will lay down a tombstone. At this point we'll
		// partition the RHS and ensure that the split does not clobber the RHS's hard
		// state.
		t.Run("(1) no RHS partition", func(t *testing.T) {
			tc, db, keyA, keyB, _, lhsPartition := setup(t)

			defer tc.Stopper().Stop(ctx)
			tc.SplitRangeOrFatal(t, keyB)

			// Write a value which we can observe to know when the split has been
			// applied by the LHS.
			increment(t, db, keyA, 1)
			tc.WaitForValues(t, keyA, []int64{6, 8, 8})

			increment(t, db, keyB, 6)
			// Wait for all non-partitioned nodes to catch up.
			tc.WaitForValues(t, keyB, []int64{0, 6, 6})

			rhsInfo, err := getRangeInfo(ctx, db, keyB)
			require.NoError(t, err)
			rhsID := rhsInfo.Desc.RangeID
			_, store0Exists := rhsInfo.Desc.GetReplicaDescriptor(1)
			require.True(t, store0Exists)

			// Remove and re-add the RHS to create a new uninitialized replica at
			// a higher replica ID. This will lead to a tombstone being written.
			tc.RemoveVotersOrFatal(t, keyB, tc.Target(0))
			// Unsuccessful because the RHS will not accept the learner snapshot
			// and will be rolled back. Nevertheless it will have learned that it
			// has been removed at the old replica ID.
			_, err = tc.Servers[0].DB().AdminChangeReplicas(
				ctx, keyB, tc.LookupRangeOrFatal(t, keyB),
				kvpb.MakeReplicationChanges(roachpb.ADD_VOTER, tc.Target(0)),
			)
			require.True(t, kvserver.IsRetriableReplicationChangeError(err), err)

			// Without a partitioned RHS we'll end up always writing a tombstone here because
			// the RHS will be created at the initial replica ID because it will get
			// raft message when the other nodes split and then after the above call
			// it will find out about its new replica ID and write a tombstone for the
			// old one.
			waitForTombstone(t, tc.GetFirstStoreFromServer(t, 0).TODOEngine(), rhsID)
			lhsPartition.deactivate()
			tc.WaitForValues(t, keyA, []int64{8, 8, 8})
			hs := getHardState(t, tc.GetFirstStoreFromServer(t, 0), rhsID)
			require.Equal(t, uint64(0), hs.Commit)
			testutils.SucceedsSoon(t, func() error {
				_, err := tc.AddVoters(keyB, tc.Target(0))
				return err
			})
			tc.WaitForValues(t, keyB, []int64{6, 6, 6})
		})

		// This case is like the previous case except the store crashes after
		// laying down a tombstone.
		t.Run("(2) no RHS partition, with restart", func(t *testing.T) {
			tc, db, keyA, keyB, _, lhsPartition := setup(t)
			defer tc.Stopper().Stop(ctx)

			tc.SplitRangeOrFatal(t, keyB)

			// Write a value which we can observe to know when the split has been
			// applied by the LHS.
			increment(t, db, keyA, 1)
			tc.WaitForValues(t, keyA, []int64{6, 8, 8})

			increment(t, db, keyB, 6)
			// Wait for all non-partitioned nodes to catch up.
			tc.WaitForValues(t, keyB, []int64{0, 6, 6})

			rhsInfo, err := getRangeInfo(ctx, db, keyB)
			require.NoError(t, err)
			rhsID := rhsInfo.Desc.RangeID
			_, store0Exists := rhsInfo.Desc.GetReplicaDescriptor(1)
			require.True(t, store0Exists)

			// Remove and re-add the RHS to create a new uninitialized replica at
			// a higher replica ID. This will lead to a tombstone being written.
			tc.RemoveVotersOrFatal(t, keyB, tc.Target(0))
			// Unsuccessfuly because the RHS will not accept the learner snapshot
			// and will be rolled back. Nevertheless it will have learned that it
			// has been removed at the old replica ID.
			_, err = tc.Servers[0].DB().AdminChangeReplicas(
				ctx, keyB, tc.LookupRangeOrFatal(t, keyB),
				kvpb.MakeReplicationChanges(roachpb.ADD_VOTER, tc.Target(0)),
			)
			require.True(t, kvserver.IsRetriableReplicationChangeError(err), err)

			// Without a partitioned RHS we'll end up always writing a tombstone here because
			// the RHS will be created at the initial replica ID because it will get
			// raft message when the other nodes split and then after the above call
			// it will find out about its new replica ID and write a tombstone for the
			// old one.
			waitForTombstone(t, tc.GetFirstStoreFromServer(t, 0).TODOEngine(), rhsID)

			// We do all of this incrementing to ensure that nobody will ever
			// succeed in sending a message the new RHS replica after we restart
			// the store. Previously there were races which could happen if we
			// stopped the store immediately. Sleeps worked but this feels somehow
			// more principled.
			curB := int64(6)
			for curB < 100 {
				curB++
				increment(t, db, keyB, 1)
				tc.WaitForValues(t, keyB, []int64{0, curB, curB})
			}

			// Restart store 0 so that it forgets about the newer replicaID.
			tc.StopServer(0)
			lhsPartition.deactivate()
			require.NoError(t, tc.RestartServer(0))

			tc.WaitForValues(t, keyA, []int64{8, 8, 8})
			hs := getHardState(t, tc.GetFirstStoreFromServer(t, 0), rhsID)
			require.Equal(t, uint64(0), hs.Commit)
			testutils.SucceedsSoon(t, func() error {
				_, err := tc.AddVoters(keyB, tc.Target(0))
				return err
			})
			tc.WaitForValues(t, keyB, []int64{curB, curB, curB})
		})

		// In this case the RHS will be partitioned from hearing anything about
		// the initial replica ID of the RHS after the split. It will learn about
		// the higher replica ID and have that higher replica ID in memory when
		// the split is processed. We partition the RHS's new replica ID before
		// processing the split to ensure that the RHS doesn't get initialized.
		t.Run("(3) initial replica RHS partition, no restart", func(t *testing.T) {
			tc, db, keyA, keyB, _, lhsPartition := setup(t)
			defer tc.Stopper().Stop(ctx)
			var rhsPartition *testClusterPartitionedRange
			partitionReplicaOnSplit(t, tc, keyB, lhsPartition, &rhsPartition)
			tc.SplitRangeOrFatal(t, keyB)

			// Write a value which we can observe to know when the split has been
			// applied by the LHS.
			increment(t, db, keyA, 1)
			tc.WaitForValues(t, keyA, []int64{6, 8, 8})

			increment(t, db, keyB, 6)
			// Wait for all non-partitioned nodes to catch up.
			tc.WaitForValues(t, keyB, []int64{0, 6, 6})

			rhsInfo, err := getRangeInfo(ctx, db, keyB)
			require.NoError(t, err)
			rhsID := rhsInfo.Desc.RangeID
			_, store0Exists := rhsInfo.Desc.GetReplicaDescriptor(1)
			require.True(t, store0Exists)

			// Remove and re-add the RHS to create a new uninitialized replica at
			// a higher replica ID. This will lead to a tombstone being written.
			tc.RemoveVotersOrFatal(t, keyB, tc.Target(0))
			// Unsuccessful because the RHS will not accept the learner snapshot and
			// will be rolled back. Nevertheless it will have learned that it has been
			// removed at the old replica ID. We don't use tc.AddVoters because that
			// will retry until it runs out of time, since we're creating a
			// retriable-looking situation here that will persist.
			_, err = tc.Servers[0].DB().AdminChangeReplicas(
				ctx, keyB, tc.LookupRangeOrFatal(t, keyB),
				kvpb.MakeReplicationChanges(roachpb.ADD_VOTER, tc.Target(0)),
			)
			require.True(t, kvserver.IsRetriableReplicationChangeError(err), err)
			// Ensure that the replica exists with the higher replica ID.
			repl, err := tc.GetFirstStoreFromServer(t, 0).GetReplica(rhsInfo.Desc.RangeID)
			require.NoError(t, err)
			require.Equal(t, repl.ReplicaID(), rhsInfo.Desc.NextReplicaID)
			rhsPartition.addReplica(rhsInfo.Desc.NextReplicaID)
			// Ensure that there's no tombstone.
			// The RHS on store 0 never should have heard about its original ID.
			ensureNoTombstone(t, tc.GetFirstStoreFromServer(t, 0), rhsID)
			lhsPartition.deactivate()
			rhsPartition.deactivate()
			tc.WaitForValues(t, keyA, []int64{8, 8, 8})
			hs := getHardState(t, tc.GetFirstStoreFromServer(t, 0), rhsID)
			require.Equal(t, uint64(0), hs.Commit)
			// Now succeed in adding the RHS. Use SucceedsSoon because in rare cases
			// the learner snapshot can fail due to a race with a raft snapshot from
			// a raft leader on a different node.
			testutils.SucceedsSoon(t, func() error {
				_, err := tc.AddVoters(keyB, tc.Target(0))
				return err
			})
			tc.WaitForValues(t, keyB, []int64{6, 6, 6})
		})

		// This case is set up like the previous one except after the RHS learns about
		// its higher replica ID the store crashes and forgets. The RHS replica gets
		// initialized by the split.
		t.Run("(4) initial replica RHS partition, with restart", func(t *testing.T) {
			tc, db, keyA, keyB, _, lhsPartition := setup(t)
			defer tc.Stopper().Stop(ctx)
			var rhsPartition *testClusterPartitionedRange

			partitionReplicaOnSplit(t, tc, keyB, lhsPartition, &rhsPartition)
			tc.SplitRangeOrFatal(t, keyB)

			if leaseType == roachpb.LeaseLeader {
				// Since both LHS and RHS use the same store, let's remove the store
				// partition from `rhsPartition` and keep it only in `lhsPartition`.
				// This will help us control the store partition using one object.
				// TODO(ibrahim): Make the test pass when both LHS and RHS ranges are
				// recovered at the same time.
				store, err := tc.Servers[0].GetStores().(*kvserver.Stores).
					GetStore(tc.Servers[0].GetFirstStoreID())
				require.NoError(t, err)
				rhsPartition.removeStore(store.StoreID())
			}

			// Write a value which we can observe to know when the split has been
			// applied by the LHS.
			increment(t, db, keyA, 1)
			tc.WaitForValues(t, keyA, []int64{6, 8, 8})

			increment(t, db, keyB, 6)
			// Wait for all non-partitioned nodes to catch up.
			tc.WaitForValues(t, keyB, []int64{0, 6, 6})

			rhsInfo, err := getRangeInfo(ctx, db, keyB)
			require.NoError(t, err)
			rhsID := rhsInfo.Desc.RangeID
			_, store0Exists := rhsInfo.Desc.GetReplicaDescriptor(1)
			require.True(t, store0Exists)

			// Remove and re-add the RHS to create a new uninitialized replica at
			// a higher replica ID. This will lead to a tombstone being written.
			tc.RemoveVotersOrFatal(t, keyB, tc.Target(0))
			// Unsuccessfuly because the RHS will not accept the learner snapshot
			// and will be rolled back. Nevertheless it will have learned that it
			// has been removed at the old replica ID.
			//
			// Not using tc.AddVoters because we expect an error, but that error
			// would be retried internally.
			_, err = tc.Servers[0].DB().AdminChangeReplicas(
				ctx, keyB, tc.LookupRangeOrFatal(t, keyB),
				kvpb.MakeReplicationChanges(roachpb.ADD_VOTER, tc.Target(0)),
			)
			require.True(t, kvserver.IsRetriableReplicationChangeError(err), err)
			// Ensure that there's no tombstone.
			// The RHS on store 0 never should have heard about its original ID.
			ensureNoTombstone(t, tc.GetFirstStoreFromServer(t, 0), rhsID)

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
				tc.WaitForValues(t, keyB, []int64{0, curB, curB})
			}

			tc.StopServer(0)
			lhsPartition.deactivate()
			require.NoError(t, tc.RestartServer(0))

			tc.WaitForValues(t, keyA, []int64{8, 8, 8})
			// In this case the store has forgotten that it knew the RHS of the split
			// could not exist. We ensure that it has been initialized to the initial
			// commit value, which is 10.
			testutils.SucceedsSoon(t, func() error {
				hs := getHardState(t, tc.GetFirstStoreFromServer(t, 0), rhsID)
				if hs.Commit != uint64(10) {
					return errors.Errorf("hard state not yet initialized: got %v, expected %v",
						hs.Commit, uint64(10))
				}
				return nil
			})
			rhsPartition.deactivate()
			testutils.SucceedsSoon(t, func() error {
				_, err := tc.AddVoters(keyB, tc.Target(0))
				return err
			})
			tc.WaitForValues(t, keyB, []int64{curB, curB, curB})
		})
	})
}

type noopRaftMessageResponseStream struct{}

func (n noopRaftMessageResponseStream) Send(*kvserverpb.RaftMessageResponse) error {
	return nil
}

var _ kvserver.RaftMessageResponseStream = noopRaftMessageResponseStream{}

// TestElectionAfterRestart is an end-to-end test for shouldCampaignOnWake (see
// TestReplicaShouldCampaignOnWake for the corresponding unit test). It sets up
// a cluster, makes 100 ranges, restarts the cluster, and verifies that the
// cluster serves a full table scan over these ranges without incurring any raft
// elections that are triggered by a timeout. It also tests that in a
// single-node cluster, the node establishes store liveness support before the
// initial campaign, and will thus avoid a timeout-based campaign.
//
// This test uses a single-node cluster. With expiration and epoch leases, the
// test can be adapted to run on multinode clusters, though it has been very
// difficult to deflake it in the past, as there can be rare but hard to avoid
// election stalemates if requests arrive at multiple nodes at once. With leader
// leases, in a multinode setting, we are guaranteed to see timeout-based
// elections for all ranges that failed to establish StoreLiveness support
// before the initial campaign.
func TestElectionAfterRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// See the top comment for why we use a single-node cluster.
	const numNodes = 1

	// Hard-code the election timeouts here for a 6s timeout. We want to make sure
	// that election timeouts never happen spuriously in this test as a result of
	// running it with very little headroom (such as stress). We avoid an infinite
	// timeout because that will make for a poor experience (hanging requests)
	// when something does flake.
	const electionTimeoutTicks = 30
	const raftTickInterval = 200 * time.Millisecond

	r := fs.NewStickyRegistry()
	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			RaftConfig: base.RaftConfig{
				RaftElectionTimeoutTicks: electionTimeoutTicks,
				RaftTickInterval:         raftTickInterval,
			},
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					StickyVFSRegistry: r,
				},
				Store: &kvserver.StoreTestingKnobs{
					OnRaftTimeoutCampaign: func(_ roachpb.RangeID) {
						t.Errorf("saw timeout-based election")
					},
				},
			},
		},
	}

	const numRanges = 100
	func() {
		tc := testcluster.NewTestCluster(t, numNodes, clusterArgs)
		tc.Start(t)
		defer t.Log("stopped cluster")
		defer tc.Stopper().Stop(ctx)
		_, err := tc.Conns[0].Exec(`CREATE TABLE t(x, PRIMARY KEY(x)) AS TABLE generate_series(1, $1)`, numRanges-1)
		require.NoError(t, err)
		// Splitting in reverse order is faster (splitDelayHelper doesn't have to add any delays).
		_, err = tc.Conns[0].Exec(`ALTER TABLE t SPLIT AT TABLE generate_series($1, 1, -1)`, numRanges-1)
		require.NoError(t, err)
		require.NoError(t, tc.WaitForFullReplication())

		t.Logf("created %d ranges", numRanges)
	}()

	tc := testcluster.NewTestCluster(t, numNodes, clusterArgs)
	tc.Start(t)
	t.Log("started cluster")
	defer tc.Stopper().Stop(ctx)

	runner := sqlutils.MakeSQLRunner(tc.Conns[0])
	tBegin := timeutil.Now()
	require.Equal(t, fmt.Sprint(numRanges-1), runner.QueryStr(t, `SELECT count(1) FROM t`)[0][0])
	dur := timeutil.Since(tBegin)
	t.Logf("scanned full table in %.2fs (%s/range)", dur.Seconds(), dur/time.Duration(numRanges))
}

// TestRaftSnapshotsWithMVCCRangeKeys tests that snapshots carry MVCC range keys
// (i.e. MVCC range tombstones).
func TestRaftSnapshotsWithMVCCRangeKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)
	ts := tc.Server(0)
	sender := ts.DB().NonTransactionalSender()

	// Split off a range at "a".
	keyA := roachpb.Key("a")
	tc.SplitRangeOrFatal(t, keyA)

	// Write a couple of overlapping MVCC range tombstones across [a-d) and [b-e), and
	// record their timestamps.
	ts1 := ts.Clock().Now()
	_, pErr := kv.SendWrappedWith(ctx, sender, kvpb.Header{
		Timestamp: ts1,
	}, &kvpb.DeleteRangeRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    roachpb.Key("a"),
			EndKey: roachpb.Key("d"),
		},
		UseRangeTombstone: true,
	})
	require.NoError(t, pErr.GoError())

	ts2 := ts.Clock().Now()
	_, pErr = kv.SendWrappedWith(ctx, sender, kvpb.Header{
		Timestamp: ts2,
	}, &kvpb.DeleteRangeRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    roachpb.Key("b"),
			EndKey: roachpb.Key("e"),
		},
		UseRangeTombstone: true,
	})
	require.NoError(t, pErr.GoError())

	// Split off a range at "c", in the middle of the range keys.
	keyC := roachpb.Key("c")
	descA, descC := tc.SplitRangeOrFatal(t, keyC)

	// Upreplicate both ranges.
	tc.AddVotersOrFatal(t, keyA, tc.Targets(1, 2)...)
	tc.AddVotersOrFatal(t, keyC, tc.Targets(1, 2)...)
	require.NoError(t, tc.WaitForVoters(keyA, tc.Targets(1, 2)...))
	require.NoError(t, tc.WaitForVoters(keyC, tc.Targets(1, 2)...))

	// Read them back from all stores.
	for _, srv := range tc.Servers {
		store, err := srv.GetStores().(*kvserver.Stores).GetStore(srv.GetFirstStoreID())
		require.NoError(t, err)
		require.Equal(t, kvs{
			rangeKVWithTS("a", "b", ts1, storage.MVCCValue{}),
			rangeKVWithTS("b", "c", ts2, storage.MVCCValue{}),
			rangeKVWithTS("b", "c", ts1, storage.MVCCValue{}),
		}, storageutils.ScanRange(t, store.TODOEngine(), descA))
		require.Equal(t, kvs{
			rangeKVWithTS("c", "d", ts2, storage.MVCCValue{}),
			rangeKVWithTS("c", "d", ts1, storage.MVCCValue{}),
			rangeKVWithTS("d", "e", ts2, storage.MVCCValue{}),
		}, storageutils.ScanRange(t, store.TODOEngine(), descC))
	}

	// Quick check of MVCC stats.
	_, replA := getFirstStoreReplica(t, ts, keyA)
	ms := replA.GetMVCCStats()
	require.EqualValues(t, 2, ms.RangeKeyCount)
	require.EqualValues(t, 3, ms.RangeValCount)

	_, replL := getFirstStoreReplica(t, ts, keyC)
	ms = replL.GetMVCCStats()
	require.EqualValues(t, 2, ms.RangeKeyCount)
	require.EqualValues(t, 3, ms.RangeValCount)

	// Run a consistency check.
	checkConsistency := func(desc roachpb.RangeDescriptor) {
		resp, pErr := kv.SendWrapped(ctx, sender, checkConsistencyArgs(&desc))
		require.NoError(t, pErr.GoError())
		ccResp := resp.(*kvpb.CheckConsistencyResponse)
		require.Len(t, ccResp.Result, 1)
		result := ccResp.Result[0]
		require.Equal(t, desc.RangeID, result.RangeID)
		require.Equal(t, kvpb.CheckConsistencyResponse_RANGE_CONSISTENT, result.Status, "%+v", result)
	}

	checkConsistency(descA)
	checkConsistency(descC)
}

// TestRaftSnapshotsWithMVCCRangeKeysEverywhere tests that snapshots carry MVCC
// range keys in every key range (even though we normally only expect to see
// them in the user key range).
func TestRaftSnapshotsWithMVCCRangeKeysEverywhere(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)
	srv := tc.Server(0)
	store := tc.GetFirstStoreFromServer(t, 0)
	engine := store.TODOEngine()
	sender := srv.DB().NonTransactionalSender()

	// Split off ranges at "a" and "b".
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
	tc.SplitRangeOrFatal(t, keyA)
	descA, descB := tc.SplitRangeOrFatal(t, keyB)
	descs := []roachpb.RangeDescriptor{descA, descB}

	// Write MVCC range keys [a-z) in each replicated key range of each range.
	// Throw in a local timestamp, to make sure the value is replicated too.
	now := srv.Clock().Now()
	valueLocalTS := storage.MVCCValue{
		MVCCValueHeader: enginepb.MVCCValueHeader{
			LocalTimestamp: now.WallPrev().UnsafeToClockTimestamp(),
		},
	}
	valueLocalTSRaw, err := storage.EncodeMVCCValue(valueLocalTS)
	require.NoError(t, err)

	for _, desc := range descs {
		for _, span := range rditer.MakeReplicatedKeySpans(&desc) {
			prefix := append(span.Key.Clone(), ':')
			require.NoError(t, engine.PutMVCCRangeKey(storage.MVCCRangeKey{
				StartKey:  append(prefix.Clone(), 'a'),
				EndKey:    append(prefix.Clone(), 'z'),
				Timestamp: now,
			}, valueLocalTS))
		}
	}

	// Recompute stats, since the above writes didn't update stats. We just do
	// this so that we can run a consistency check, since the stats may or may not
	// pick up the range keys in different key ranges.
	for _, desc := range descs {
		_, pErr := kv.SendWrapped(ctx, sender, &kvpb.RecomputeStatsRequest{
			RequestHeader: kvpb.RequestHeader{
				Key: desc.StartKey.AsRawKey(),
			},
		})
		require.NoError(t, pErr.GoError())
	}

	// Upreplicate both ranges.
	for _, desc := range descs {
		tc.AddVotersOrFatal(t, desc.StartKey.AsRawKey(), tc.Targets(1, 2)...)
		require.NoError(t, tc.WaitForVoters(desc.StartKey.AsRawKey(), tc.Targets(1, 2)...))
	}

	// Look for the range keys on the other servers.
	for _, srvIdx := range []int{1, 2} {
		e := tc.GetFirstStoreFromServer(t, srvIdx).TODOEngine()
		for _, desc := range descs {
			for _, span := range rditer.MakeReplicatedKeySpans(&desc) {
				prefix := append(span.Key.Clone(), ':')

				iter, err := e.NewEngineIterator(context.Background(), storage.IterOptions{
					KeyTypes:   storage.IterKeyTypeRangesOnly,
					LowerBound: span.Key,
					UpperBound: span.EndKey,
				})
				if err != nil {
					t.Fatal(err)
				}
				defer iter.Close()

				ok, err := iter.SeekEngineKeyGE(storage.EngineKey{Key: span.Key})
				require.NoError(t, err)
				require.True(t, ok)
				bounds, err := iter.EngineRangeBounds()
				require.NoError(t, err)
				require.Equal(t, roachpb.Span{
					Key:    append(prefix.Clone(), 'a'),
					EndKey: append(prefix.Clone(), 'z'),
				}, bounds)
				require.Equal(t, []storage.EngineRangeKeyValue{{
					Version: storage.EncodeMVCCTimestampSuffix(now),
					Value:   valueLocalTSRaw,
				}}, iter.EngineRangeKeys())

				ok, err = iter.NextEngineKey()
				require.NoError(t, err)
				require.False(t, ok)
			}
		}
	}

	// Run consistency checks.
	for _, desc := range descs {
		resp, pErr := kv.SendWrapped(ctx, sender, checkConsistencyArgs(&desc))
		require.NoError(t, pErr.GoError())
		ccResp := resp.(*kvpb.CheckConsistencyResponse)
		require.Len(t, ccResp.Result, 1)
		result := ccResp.Result[0]
		require.Equal(t, desc.RangeID, result.RangeID)
		require.Equal(t, kvpb.CheckConsistencyResponse_RANGE_CONSISTENT, result.Status, "%+v", result)
	}
}

// TestRaftCampaignPreVoteCheckQuorum tests that campaignLocked() respects
// PreVote+CheckQuorum, by not granting prevotes if there is an active leader.
func TestRaftCampaignPreVoteCheckQuorum(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Timing-sensitive, so skip under deadlock detector and race.
	skip.UnderDeadlock(t)
	skip.UnderRace(t)

	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			RaftConfig: base.RaftConfig{
				RaftEnableCheckQuorum: true,
				RaftTickInterval:      100 * time.Millisecond, // speed up test
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	logStatus := func(s *raft.Status) {
		t.Helper()
		require.NotNil(t, s)
		t.Logf("n%d %s at term=%d commit=%d", s.ID, s.RaftState, s.Term, s.Commit)
	}

	sender := tc.GetFirstStoreFromServer(t, 0).TestSender()

	// Create a range, upreplicate it, and replicate a write.
	key := tc.ScratchRange(t)
	desc := tc.AddVotersOrFatal(t, key, tc.Targets(1, 2)...)
	_, pErr := kv.SendWrapped(ctx, sender, incrementArgs(key, 1))
	require.NoError(t, pErr.GoError())
	tc.WaitForValues(t, key, []int64{1, 1, 1})

	repl1, err := tc.GetFirstStoreFromServer(t, 0).GetReplica(desc.RangeID)
	require.NoError(t, err)
	repl2, err := tc.GetFirstStoreFromServer(t, 1).GetReplica(desc.RangeID)
	require.NoError(t, err)
	repl3, err := tc.GetFirstStoreFromServer(t, 2).GetReplica(desc.RangeID)
	require.NoError(t, err)
	repls := []*kvserver.Replica{repl1, repl2, repl3}

	// Make sure n1 is leader.
	initialStatus := repl1.RaftStatus()
	require.Equal(t, raftpb.StateLeader, initialStatus.RaftState)
	logStatus(initialStatus)
	t.Logf("n1 is leader")

	// Campaign n3. It shouldn't win prevotes, reverting to follower
	// in the current term.
	repl3.Campaign(ctx)
	t.Logf("n3 campaigning")

	require.Eventually(t, func() bool {
		status := repl3.RaftStatus()
		logStatus(status)
		return status.RaftState == raftpb.StateFollower
	}, 10*time.Second, 500*time.Millisecond)
	t.Logf("n3 reverted to follower")

	// n1 should still be the leader in the same term, with n2 and n3 followers.
	for _, repl := range repls {
		st := repl.RaftStatus()
		logStatus(st)
		if st.ID == 1 {
			require.Equal(t, raftpb.StateLeader, st.RaftState)
		} else {
			require.Equal(t, raftpb.StateFollower, st.RaftState)
		}
		require.Equal(t, initialStatus.Term, st.Term)
	}
}

// TestRaftForceCampaignPreVoteCheckQuorum tests that forceCampaignLocked()
// ignores PreVote+CheckQuorum, transitioning directly to candidate and bumping
// the term. It may not actually win or hold onto leadership, but bumping the
// term is proof enough that it called an election.
func TestRaftForceCampaignPreVoteCheckQuorum(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Timing-sensitive, so skip under deadlock detector and race.
	skip.UnderDeadlock(t)
	skip.UnderRace(t)

	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			RaftConfig: base.RaftConfig{
				RaftEnableCheckQuorum:      true,
				RaftTickInterval:           200 * time.Millisecond, // speed up test
				RaftHeartbeatIntervalTicks: 10,                     // allow n3 to win the election
				RaftElectionTimeoutTicks:   20,
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	logStatus := func(s *raft.Status) {
		t.Helper()
		require.NotNil(t, s)
		t.Logf("n%d %s at term=%d commit=%d", s.ID, s.RaftState, s.Term, s.Commit)
	}

	sender := tc.GetFirstStoreFromServer(t, 0).TestSender()

	// Create a range, upreplicate it, and replicate a write.
	key := tc.ScratchRange(t)
	desc := tc.AddVotersOrFatal(t, key, tc.Targets(1, 2)...)
	_, pErr := kv.SendWrapped(ctx, sender, incrementArgs(key, 1))
	require.NoError(t, pErr.GoError())
	tc.WaitForValues(t, key, []int64{1, 1, 1})

	repl1, err := tc.GetFirstStoreFromServer(t, 0).GetReplica(desc.RangeID)
	require.NoError(t, err)
	repl2, err := tc.GetFirstStoreFromServer(t, 1).GetReplica(desc.RangeID)
	require.NoError(t, err)
	repl3, err := tc.GetFirstStoreFromServer(t, 2).GetReplica(desc.RangeID)
	require.NoError(t, err)
	repls := []*kvserver.Replica{repl1, repl2, repl3}

	// Make sure n1 is leader.
	initialStatus := repl1.RaftStatus()
	require.Equal(t, raftpb.StateLeader, initialStatus.RaftState)
	logStatus(initialStatus)
	t.Logf("n1 is leader in term %d", initialStatus.Term)

	// Force-campaign n3. It may not win or hold onto leadership, but it's enough
	// to know that it bumped the term.
	repl3.ForceCampaign(ctx, initialStatus.BasicStatus)
	t.Logf("n3 campaigning")

	var leaderStatus *raft.Status
	require.Eventually(t, func() bool {
		for _, repl := range repls {
			st := repl.RaftStatus()
			logStatus(st)
			if st.Term <= initialStatus.Term {
				return false
			}
			if st.RaftState == raftpb.StateLeader {
				leaderStatus = st
			}
		}
		return leaderStatus != nil
	}, 10*time.Second, 500*time.Millisecond)
	t.Logf("n%d is leader, with bumped term %d", leaderStatus.ID, leaderStatus.Term)
}

// TestRaftPreVote tests that Raft PreVote works properly, including the recent
// leader check only enabled via CheckQuorum. Specifically, a replica that's
// partitioned away from the leader (or restarted) should not be able to call an
// election, even if it's still up-to-date on the log, because followers should
// not grant prevotes if they've heard from a leader in the past election
// timeout.
//
// We set up a partial partition as such:
//
//	               n1 (leader)
//	              /  x
//	             /    x
//	(follower) n2 ---- n3 (partitioned)
//
// This will cause n3 to send prevote requests to n2, which should not succeed,
// leaving the current leader n1 alone. We make sure there are no writes on the
// range, to keep n3's log up-to-date such that it's eligible for prevotes.
//
// We test several combinations:
//
// - a partial and full partition of n3.
// - leader leases and expiration-based leases.
func TestRaftPreVote(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Timing-sensitive, so skip it under deadlock detector, race detector, and
	// stress.
	skip.UnderDuress(t, "takes >1 m")

	testutils.RunTrueAndFalse(t, "partial", func(t *testing.T, partial bool) {
		testutils.RunTrueAndFalse(t, "expLease", func(t *testing.T, expLease bool) {
			if !partial && !expLease {
				// A full partition with leader leases won't transition n3 to a
				// pre-candidate, as it will be partitioned away from both n1 and n2,
				// meaning it won't have adequate StoreLiveness support to become a
				// pre-candidate.
				return
			}
			ctx := context.Background()

			// We don't want any writes to the range, to avoid the follower from
			// falling behind on the log and failing prevotes only because of that.
			// We install a proposal filter which rejects proposals to the range
			// during the partition (typically txn record cleanup via GC requests,
			// but also e.g. lease extensions).
			var partitioned, blocked atomic.Bool
			var rangeID roachpb.RangeID
			propFilter := func(args kvserverbase.ProposalFilterArgs) *kvpb.Error {
				if blocked.Load() && args.Req.RangeID == rangeID {
					t.Logf("r%d proposal rejected: %s", rangeID, args.Req)
					return kvpb.NewError(errors.New("rejected"))
				}
				return nil
			}

			// We also disable lease extensions and expiration-based lease
			// transfers, to avoid range writes.
			st := cluster.MakeTestingClusterSettings()
			kvserver.TransferExpirationLeasesFirstEnabled.Override(ctx, &st.SV, false)
			if expLease {
				kvserver.OverrideDefaultLeaseType(ctx, &st.SV, roachpb.LeaseExpiration)
			} else {
				kvserver.OverrideDefaultLeaseType(ctx, &st.SV, roachpb.LeaseLeader)
			}

			tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
				ReplicationMode: base.ReplicationManual,
				ServerArgs: base.TestServerArgs{
					Settings: st,
					RaftConfig: base.RaftConfig{
						RaftEnableCheckQuorum: true,
						RaftTickInterval:      200 * time.Millisecond, // speed up test
					},
					Knobs: base.TestingKnobs{
						Store: &kvserver.StoreTestingKnobs{
							TestingProposalFilter:        propFilter,
							DisableAutomaticLeaseRenewal: true,
						},
					},
				},
			})
			defer tc.Stopper().Stop(ctx)

			logStatus := func(s *raft.Status) {
				t.Helper()
				require.NotNil(t, s)
				t.Logf("n%d %s at term=%d commit=%d", s.ID, s.RaftState, s.Term, s.Commit)
			}

			// Create a range, upreplicate it, and replicate a write.
			sender := tc.GetFirstStoreFromServer(t, 0).TestSender()
			key := tc.ScratchRange(t)
			desc := tc.AddVotersOrFatal(t, key, tc.Targets(1, 2)...)
			rangeID = desc.RangeID

			_, pErr := kv.SendWrapped(ctx, sender, incrementArgs(key, 1))
			require.NoError(t, pErr.GoError())
			tc.WaitForValues(t, key, []int64{1, 1, 1})

			repl1, err := tc.GetFirstStoreFromServer(t, 0).GetReplica(rangeID)
			require.NoError(t, err)
			repl3, err := tc.GetFirstStoreFromServer(t, 2).GetReplica(rangeID)
			require.NoError(t, err)

			// Configure the partition, but don't activate it yet.
			if partial {
				// Partition n3 away from n1, in both directions.
				dropRaftMessagesFrom(t, tc.Servers[0], desc, []roachpb.ReplicaID{3}, &partitioned)
				dropRaftMessagesFrom(t, tc.Servers[2], desc, []roachpb.ReplicaID{1}, &partitioned)
			} else {
				// Partition n3 away from both of n1 and n2, in both directions.
				dropRaftMessagesFrom(t, tc.Servers[0], desc, []roachpb.ReplicaID{3}, &partitioned)
				dropRaftMessagesFrom(t, tc.Servers[1], desc, []roachpb.ReplicaID{3}, &partitioned)
				dropRaftMessagesFrom(t, tc.Servers[2], desc, []roachpb.ReplicaID{1, 2}, &partitioned)
			}

			// Make sure the lease is on n1 and that everyone has applied it.
			tc.TransferRangeLeaseOrFatal(t, desc, tc.Target(0))
			_, pErr = kv.SendWrapped(ctx, sender, incrementArgs(key, 1))
			require.NoError(t, pErr.GoError())
			tc.WaitForValues(t, key, []int64{2, 2, 2})
			t.Logf("n1 has lease")

			// Block new proposals to the range.
			blocked.Store(true)
			t.Logf("n1 proposals blocked")

			var lastIndex uint64
			var lastChanged time.Time
			require.Eventually(t, func() bool {
				status := repl1.RaftStatus()
				require.Equal(t, raftpb.StateLeader, status.RaftState)
				if i := status.Progress[1].Match; i > lastIndex {
					t.Logf("n1 last index changed: %d -> %d", lastIndex, i)
					lastIndex, lastChanged = i, time.Now()
					return false
				}
				for i, pr := range status.Progress {
					if pr.Match != lastIndex {
						t.Logf("n%d match %d not at n1 last index %d, waiting", i, pr.Match, lastIndex)
						return false
					}
				}
				if since := time.Since(lastChanged); since < time.Second {
					t.Logf("n1 last index %d changed %s ago, waiting",
						lastIndex, since.Truncate(time.Millisecond))
					return false
				}
				return true
			}, 10*time.Second, 200*time.Millisecond)
			t.Logf("n1 stabilized range")
			logStatus(repl1.RaftStatus())

			// Partition n3.
			partitioned.Store(true)
			t.Logf("n3 partitioned")

			// Fetch the leader's initial status.
			initialStatus := repl1.RaftStatus()
			require.Equal(t, raftpb.StateLeader, initialStatus.RaftState)
			logStatus(initialStatus)

			// Wait for the follower to become a candidate.
			require.Eventually(t, func() bool {
				status := repl3.RaftStatus()
				logStatus(status)
				return status.RaftState == raftpb.StatePreCandidate
			}, 45*time.Second, 500*time.Millisecond)
			t.Logf("n3 became pre-candidate")

			// The candidate shouldn't change state for some time, i.e.  it
			// shouldn't move into Candidate or Follower because it received
			// prevotes or other messages.
			require.Never(t, func() bool {
				status := repl3.RaftStatus()
				logStatus(status)
				return status.RaftState != raftpb.StatePreCandidate
			}, 3*time.Second, time.Second)
			t.Logf("n3 is still pre-candidate")

			// Make sure the leader and term are still the same, and that there were
			// no writes to the range. In the case of a quiesced range under a
			// partial partition we have to account for a possible unquiesce entry,
			// due to the prevote received by n2 which will wake the leader.
			leaderStatus := repl1.RaftStatus()
			logStatus(leaderStatus)
			require.Equal(t, raftpb.StateLeader, leaderStatus.RaftState)
			require.Equal(t, initialStatus.Term, leaderStatus.Term)
			require.LessOrEqual(t, leaderStatus.Commit, initialStatus.Commit+1)
			t.Logf("n1 is still leader")

			// Heal the partition and unblock proposals, then wait for the replica
			// to become a follower.
			blocked.Store(false)
			partitioned.Store(false)
			t.Logf("n3 partition healed")

			require.Eventually(t, func() bool {
				status := repl3.RaftStatus()
				logStatus(status)
				return status.RaftState == raftpb.StateFollower
			}, 10*time.Second, 500*time.Millisecond)
			t.Logf("n3 became follower")

			// Make sure the leader and term are still the same.
			leaderStatus = repl1.RaftStatus()
			logStatus(leaderStatus)
			require.Equal(t, raftpb.StateLeader, leaderStatus.RaftState)
			require.Equal(t, initialStatus.Term, leaderStatus.Term)
			t.Logf("n1 is still leader")

			// Replicate another write to make sure the Raft group still works.
			_, pErr = kv.SendWrapped(ctx, sender, incrementArgs(key, 1))
			require.NoError(t, pErr.GoError())
			tc.WaitForValues(t, key, []int64{3, 3, 3})
			t.Logf("n1 replicated write")

			// Make sure the leader and term are still the same.
			leaderStatus = repl1.RaftStatus()
			logStatus(leaderStatus)
			require.Equal(t, raftpb.StateLeader, leaderStatus.RaftState)
			require.Equal(t, initialStatus.Term, leaderStatus.Term)
			t.Logf("n1 is still leader")
		})
	})
}

// TestRaftCheckQuorum tests that Raft CheckQuorum works properly, i.e. that a
// leader will step down if it hasn't heard from a quorum of followers in the
// last election timeout interval.
//
//	               n1 (leader)
//	              x  x
//	             x    x
//	(follower) n2 ---- n3 (follower)
//
// We test this by partitioning the leader away from two followers, using either
// a symmetric or asymmetric partition. In the asymmetric case, the leader can
// send heartbeats to followers, but won't receive responses. Eventually, it
// should step down and the followers should elect a new leader.
//
// Only runs with leader leases.
func TestRaftCheckQuorum(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test is timing-sensitive, so skip it under deadlock detector and
	// race.
	skip.UnderDeadlock(t)
	skip.UnderRace(t)

	testutils.RunTrueAndFalse(t, "symmetric", func(t *testing.T, symmetric bool) {
		testutils.RunTrueAndFalse(t, "sleep", func(t *testing.T, sleep bool) {
			ctx := context.Background()

			// Turn on leader leases.
			st := cluster.MakeTestingClusterSettings()
			kvserver.RaftStoreLivenessQuiescenceEnabled.Override(ctx, &st.SV, sleep)
			kvserver.OverrideDefaultLeaseType(ctx, &st.SV, roachpb.LeaseLeader)

			tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
				ReplicationMode: base.ReplicationManual,
				ServerArgs: base.TestServerArgs{
					Settings: st,
					RaftConfig: base.RaftConfig{
						RaftEnableCheckQuorum: true,
						RaftTickInterval:      200 * time.Millisecond, // speed up test
					},
				},
			})
			defer tc.Stopper().Stop(ctx)

			logStatus := func(s *raft.Status) {
				t.Helper()
				require.NotNil(t, s)
				t.Logf("n%d %s at term=%d commit=%d", s.ID, s.RaftState, s.Term, s.Commit)
			}

			// Create a range, upreplicate it, and replicate a write.
			sender := tc.GetFirstStoreFromServer(t, 0).TestSender()
			key := tc.ScratchRange(t)
			desc := tc.AddVotersOrFatal(t, key, tc.Targets(1, 2)...)

			_, pErr := kv.SendWrapped(ctx, sender, incrementArgs(key, 1))
			require.NoError(t, pErr.GoError())
			tc.WaitForValues(t, key, []int64{1, 1, 1})

			repl1, err := tc.GetFirstStoreFromServer(t, 0).GetReplica(desc.RangeID)
			require.NoError(t, err)
			repl2, err := tc.GetFirstStoreFromServer(t, 1).GetReplica(desc.RangeID)
			require.NoError(t, err)
			repl3, err := tc.GetFirstStoreFromServer(t, 2).GetReplica(desc.RangeID)
			require.NoError(t, err)

			// Set up dropping of inbound messages on n1 from n2,n3, but don't
			// activate it yet.
			var partitioned atomic.Bool
			dropRaftMessagesFrom(t, tc.Servers[0], desc, []roachpb.ReplicaID{2, 3}, &partitioned)
			if symmetric {
				// Drop outbound messages from n1 to n2,n3 too.
				dropRaftMessagesFrom(t, tc.Servers[1], desc, []roachpb.ReplicaID{1}, &partitioned)
				dropRaftMessagesFrom(t, tc.Servers[2], desc, []roachpb.ReplicaID{1}, &partitioned)
			}

			// Make sure the lease is on n1 and that everyone has applied it.
			tc.TransferRangeLeaseOrFatal(t, desc, tc.Target(0))
			tc.WaitForLeaseUpgrade(ctx, t, desc)
			_, pErr = kv.SendWrapped(ctx, sender, incrementArgs(key, 1))
			require.NoError(t, pErr.GoError())
			tc.WaitForValues(t, key, []int64{2, 2, 2})
			t.Logf("n1 has lease")

			// Wait for the followers to fall asleep, if enabled.
			if sleep {
				require.Eventually(t, func() bool {
					return repl2.IsAsleep() && repl3.IsAsleep()
				}, 10*time.Second, 100*time.Millisecond)
				// The leader shouldn't fall asleep.
				require.False(t, repl1.IsAsleep())
				t.Logf("n2 and n3 fell asleep")
			} else {
				require.False(t, repl1.IsAsleep() || repl2.IsAsleep() || repl3.IsAsleep())
				t.Logf("n1, n2, and n3 not asleep")
			}

			// Partition n1.
			partitioned.Store(true)
			t.Logf("n1 partitioned")

			// Fetch the leader's initial status.
			initialStatus := repl1.RaftStatus()
			require.Equal(t, raftpb.StateLeader, initialStatus.RaftState)
			logStatus(initialStatus)

			require.False(t, repl1.IsAsleep())
			t.Logf("n1 not asleep")

			// Wait for the followers to wake up.
			if sleep {
				testutils.SucceedsSoon(t, func() error {
					if repl2.IsAsleep() || repl3.IsAsleep() {
						return errors.Errorf("at least one follower still asleep")
					}
					return nil
				})
				t.Logf("n2 and n3 woke up")
			} else {
				require.False(t, repl1.IsAsleep() || repl2.IsAsleep() || repl3.IsAsleep())
				t.Logf("n1, n2, and n3 not asleep")
			}

			// Wait for the leader to step down.
			require.Eventually(t, func() bool {
				status := repl1.RaftStatus()
				logStatus(status)
				return status.RaftState != raftpb.StateLeader
			}, 10*time.Second, 500*time.Millisecond)
			t.Logf("n1 stepped down as a leader")

			// n2 or n3 should elect a new leader. At this point, the store liveness
			// SupportWithdrawalGracePeriod may not have expired yet, so this step waits
			// a little longer.
			var leaderStatus *raft.Status
			require.Eventually(t, func() bool {
				for _, status := range []*raft.Status{repl2.RaftStatus(), repl3.RaftStatus()} {
					logStatus(status)
					if status.RaftState == raftpb.StateLeader {
						leaderStatus = status
						return true
					}
				}
				return false
			}, 20*time.Second, 500*time.Millisecond)
			t.Logf("n%d became leader", leaderStatus.ID)

			// n1 shouldn't become a leader.
			require.Never(t, func() bool {
				status := repl1.RaftStatus()
				logStatus(status)
				expState := status.RaftState != raftpb.StateLeader && status.Lead == raft.None
				return !expState // require.Never
			}, 3*time.Second, 500*time.Millisecond)
			t.Logf("n1 remains not leader")

			// The existing leader shouldn't have been affected by the possible n1's
			// prevotes.
			var finalStatus *raft.Status
			for _, status := range []*raft.Status{repl2.RaftStatus(), repl3.RaftStatus()} {
				logStatus(status)
				if status.RaftState == raftpb.StateLeader {
					finalStatus = status
					break
				}
			}
			require.NotNil(t, finalStatus)
			require.Equal(t, leaderStatus.ID, finalStatus.ID)
			require.Equal(t, leaderStatus.Term, finalStatus.Term)
		})
	})
}

// TestRaftLeaderRemovesItself tests that when a raft leader removes itself via
// a conf change the leaseholder campaigns for leadership, ignoring
// PreVote+CheckQuorum and transitioning directly to candidate.
//
// We set up three replicas:
//
// n1: Raft leader
// n2: follower
// n3: follower + leaseholder
//
// We disable leader following the leaseholder (which would otherwise transfer
// leadership if we happened to tick during the conf change), and then remove n1
// from the range. n3 should acquire leadership.
//
// We disable election timeouts, such that the only way n3 can become leader is
// by campaigning explicitly.
func TestRaftLeaderRemovesItself(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Timing-sensitive, so skip under deadlock detector and race.
	skip.UnderDeadlock(t)
	skip.UnderRace(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	testutils.RunValues(t, "leaseType", roachpb.TestingAllLeaseTypes(), func(t *testing.T, leaseType roachpb.LeaseType) {
		settings := cluster.MakeTestingClusterSettings()
		kvserver.OverrideDefaultLeaseType(ctx, &settings.SV, leaseType)

		raftCfg := base.RaftConfig{
			RaftEnableCheckQuorum: true,
			RaftTickInterval:      100 * time.Millisecond, // speed up test
		}

		if leaseType != roachpb.LeaseLeader {
			// Set a large election timeout. We don't want replicas to call
			// elections due to timeouts, instead, we want leadership to get
			// transferred.
			//
			// We only need to do this if we're not running with leader leases. With
			// leader leases, we won't have elections due to timeouts because of
			// fortification. Unlike other lease types, which rely on per-range
			// heartbeats which are affected by leader step-down, leader leases use
			// per-store StoreLiveness heartbeats which are not affected by leader
			// step-down.
			//
			// In fact, we can't use a high value of RaftElectionTimeoutTicks when
			// using leader leases as the first attempt to establish raft leadership
			// is guaranteed to fail because we won't have StoreLiveness support (so
			// we won't campaign). The test will timeout if we set a high value for
			// RaftElectionTimeoutTicks, as it'll take too long for us to come back
			// a second time around to call an election.
			raftCfg.RaftElectionTimeoutTicks = 300
		}

		tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				RaftConfig: raftCfg,
				Settings:   settings,
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						DisableLeaderFollowsLeaseholder: true, // the leader should stay put
					},
				},
			},
		})
		defer tc.Stopper().Stop(ctx)

		logStatus := func(s *raft.Status) {
			t.Helper()
			require.NotNil(t, s)
			t.Logf("n%d %s at term=%d commit=%d", s.ID, s.RaftState, s.Term, s.Commit)
		}

		send1 := tc.GetFirstStoreFromServer(t, 0).TestSender()
		send3 := tc.GetFirstStoreFromServer(t, 2).TestSender()

		// Create a range, upreplicate it, and replicate a write.
		key := tc.ScratchRange(t)
		desc := tc.AddVotersOrFatal(t, key, tc.Targets(1, 2)...)
		_, pErr := kv.SendWrapped(ctx, send1, incrementArgs(key, 1))
		require.NoError(t, pErr.GoError())
		tc.WaitForValues(t, key, []int64{1, 1, 1})

		repl1, err := tc.GetFirstStoreFromServer(t, 0).GetReplica(desc.RangeID)
		require.NoError(t, err)
		repl2, err := tc.GetFirstStoreFromServer(t, 1).GetReplica(desc.RangeID)
		require.NoError(t, err)
		repl3, err := tc.GetFirstStoreFromServer(t, 2).GetReplica(desc.RangeID)
		require.NoError(t, err)

		// Move the lease to n3, and make sure everyone has applied it.
		tc.TransferRangeLeaseOrFatal(t, desc, tc.Target(2))
		require.Eventually(t, func() bool {
			lease, _ := repl3.GetLease()
			return lease.Replica.ReplicaID == repl3.ReplicaID()
		}, 10*time.Second, 500*time.Millisecond)
		_, pErr = kv.SendWrapped(ctx, send3, incrementArgs(key, 1))
		require.NoError(t, pErr.GoError())
		tc.WaitForValues(t, key, []int64{2, 2, 2})
		t.Logf("n3 has lease")

		// Make sure n1 is still leader.
		st := repl1.RaftStatus()
		require.Equal(t, raftpb.StateLeader, st.RaftState)
		logStatus(st)

		// Remove n1 and wait for n3 to become leader.
		tc.RemoveVotersOrFatal(t, key, tc.Target(0))
		t.Logf("n1 removed from range")

		// Make sure we didn't time out on the above.
		require.NoError(t, ctx.Err())

		require.Eventually(t, func() bool {
			logStatus(repl2.RaftStatus())
			logStatus(repl3.RaftStatus())
			if repl3.RaftStatus().RaftState == raftpb.StateLeader {
				t.Logf("n3 is leader")
				return true
			}
			return false
		}, 10*time.Second, 500*time.Millisecond)

		require.NoError(t, ctx.Err())
	})
}

// TestStoreMetricsOnIncomingOutgoingMsg verifies that HandleRaftRequest() and
// HandleRaftRequestSent() correctly update metrics for incoming and outgoing
// raft messages.
func TestStoreMetricsOnIncomingOutgoingMsg(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Unix(0, 123)))
	cfg := kvserver.TestStoreConfig(clock)
	var stopper *stop.Stopper
	stopper, _, _, cfg.StorePool, _ = storepool.CreateTestStorePool(ctx, cfg.Settings,
		liveness.TestTimeUntilNodeDead, false, /* deterministic */
		func() int { return 1 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_DEAD)
	defer stopper.Stop(ctx)

	// Create a noop store and request.
	node := roachpb.NodeDescriptor{NodeID: roachpb.NodeID(1)}
	eng := storage.NewDefaultInMemForTesting()
	stopper.AddCloser(eng)
	cfg.Transport = kvserver.NewDummyRaftTransport(cfg.AmbientCtx, cfg.Settings, cfg.Clock)
	store := kvserver.NewStore(ctx, cfg, eng, &node)
	store.Ident = &roachpb.StoreIdent{
		ClusterID: uuid.Nil,
		StoreID:   1,
		NodeID:    1,
	}
	request := &kvserverpb.RaftMessageRequest{
		RangeID:     1,
		FromReplica: roachpb.ReplicaDescriptor{},
		ToReplica:   roachpb.ReplicaDescriptor{},
		Message: raftpb.Message{
			From: 1,
			To:   2,
			Type: raftpb.MsgTimeoutNow,
			Term: 1,
		},
	}

	metricsNames := []string{
		"raft.rcvd.bytes",
		"raft.rcvd.cross_region.bytes",
		"raft.rcvd.cross_zone.bytes",
		"raft.sent.bytes",
		"raft.sent.cross_region.bytes",
		"raft.sent.cross_zone.bytes"}
	stream := noopRaftMessageResponseStream{}
	expectedSize := int64(request.Size())

	t.Run("received raft message", func(t *testing.T) {
		before, metricsErr := store.Metrics().GetStoreMetrics(metricsNames)
		if metricsErr != nil {
			t.Error(metricsErr)
		}
		if err := store.HandleRaftRequest(context.Background(), request, stream); err != nil {
			t.Fatalf("HandleRaftRequest returned err %s", err)
		}
		after, metricsErr := store.Metrics().GetStoreMetrics(metricsNames)
		if metricsErr != nil {
			t.Error(metricsErr)
		}
		actual := getMapsDiff(before, after)
		expected := map[string]int64{
			"raft.rcvd.bytes":              expectedSize,
			"raft.rcvd.cross_region.bytes": 0,
			"raft.rcvd.cross_zone.bytes":   0,
			"raft.sent.bytes":              0,
			"raft.sent.cross_region.bytes": 0,
			"raft.sent.cross_zone.bytes":   0,
		}
		require.Equal(t, expected, actual)
	})

	t.Run("sent raft message", func(t *testing.T) {
		before, metricsErr := store.Metrics().GetStoreMetrics(metricsNames)
		if metricsErr != nil {
			t.Error(metricsErr)
		}
		store.HandleRaftRequestSent(context.Background(),
			request.FromReplica.NodeID, request.ToReplica.NodeID, int64(request.Size()))
		after, metricsErr := store.Metrics().GetStoreMetrics(metricsNames)
		if metricsErr != nil {
			t.Error(metricsErr)
		}
		actual := getMapsDiff(before, after)
		expected := map[string]int64{
			"raft.rcvd.bytes":              0,
			"raft.rcvd.cross_region.bytes": 0,
			"raft.rcvd.cross_zone.bytes":   0,
			"raft.sent.bytes":              expectedSize,
			"raft.sent.cross_region.bytes": 0,
			"raft.sent.cross_zone.bytes":   0,
		}
		require.Equal(t, expected, actual)
	})
}

// TestInvalidConfChangeRejection is a regression test for [1]. It proposes
// an (intentionally) invalid configuration change and makes sure that raft
// does not drop it.
//
// [1]: https://github.com/cockroachdb/cockroach/issues/105797
func TestInvalidConfChangeRejection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This is a regression test against a stuck command, so set a timeout to get
	// a shot at a graceful failure on regression.
	ctx, cancel := context.WithTimeout(context.Background(), testutils.DefaultSucceedsSoonDuration)
	defer cancel()

	// When our configuration change shows up below raft, we need to apply it as a
	// no-op, since the config change is intentionally invalid and assertions
	// would fail if we were to try to actually apply it.
	injErr := errors.New("injected error")
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{Knobs: base.TestingKnobs{Store: &kvserver.StoreTestingKnobs{
			TestingApplyCalledTwiceFilter: func(args kvserverbase.ApplyFilterArgs) (int, *kvpb.Error) {
				if args.Req != nil && args.Req.Txn != nil && args.Req.Txn.Name == "fake" {
					return 0, kvpb.NewError(injErr)
				}
				return 0, nil
			}}}},
	})
	defer tc.Stopper().Stop(ctx)

	k := tc.ScratchRange(t)

	repl := tc.GetFirstStoreFromServer(t, 0).LookupReplica(keys.MustAddr(k))

	// Try to leave a joint config even though we're not in one. This is something
	// that will lead raft to propose an empty entry instead of our conf change.
	//
	// See: https://github.com/cockroachdb/cockroach/issues/105797
	var ba kvpb.BatchRequest
	now := tc.Server(0).Clock().Now()
	txn := roachpb.MakeTransaction("fake", k, isolation.Serializable, roachpb.NormalUserPriority, now, 500*time.Millisecond.Nanoseconds(), 1, 0, false /* omitInRangefeeds */)
	ba.Txn = &txn
	ba.Timestamp = now
	ba.Add(&kvpb.EndTxnRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: k,
		},
		Commit: true,
		InternalCommitTrigger: &roachpb.InternalCommitTrigger{
			ChangeReplicasTrigger: &roachpb.ChangeReplicasTrigger{
				Desc: repl.Desc(),
			},
		},
	})

	_, pErr := repl.Send(ctx, &ba)
	// Verify that we see the configuration change below raft, where we rejected it
	// (since it would've otherwise blow up the Replica: after all, we intentionally
	// proposed an invalid configuration change.
	require.True(t, errors.Is(pErr.GoError(), injErr), "%+v", pErr.GoError())
}

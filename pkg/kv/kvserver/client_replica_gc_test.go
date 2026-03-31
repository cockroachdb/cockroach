// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/stretchr/testify/require"
)

// TestReplicaGCQueueDropReplica verifies that a removed replica is
// immediately cleaned up.
func TestReplicaGCQueueDropReplicaDirect(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const numStores = 3

	testKnobs := kvserver.StoreTestingKnobs{}
	var tc *testcluster.TestCluster

	// In this test, the Replica on the second Node is removed, and the test
	// verifies that that Node adds this Replica to its RangeGCQueue. However,
	// the queue does a consistent lookup which will usually be read from
	// Node 1. Hence, if Node 1 hasn't processed the removal when Node 2 has,
	// no GC will take place since the consistent RangeLookup hits the first
	// Node. We use the TestingEvalFilter to make sure that the second Node
	// waits for the first.
	testKnobs.EvalKnobs.TestingEvalFilter =
		func(filterArgs kvserverbase.FilterArgs) *kvpb.Error {
			et, ok := filterArgs.Req.(*kvpb.EndTxnRequest)
			if !ok || filterArgs.Sid != 2 {
				return nil
			}
			crt := et.InternalCommitTrigger.GetChangeReplicasTrigger()
			if crt == nil || len(crt.InternalRemovedReplicas) == 0 {
				return nil
			}
			testutils.SucceedsSoon(t, func() error {
				k := tc.ScratchRange(t)
				desc, err := tc.LookupRange(k)
				if err != nil {
					return err
				}
				if _, ok := desc.GetReplicaDescriptor(2); ok {
					return errors.New("expected second node gone from first node's known replicas")
				}
				return nil
			})
			return nil
		}

	tc = testcluster.StartTestCluster(t, numStores,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &testKnobs,
				},
			},
		},
	)
	defer tc.Stopper().Stop(context.Background())

	// Create our scratch range and up-replicate it.
	k := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, k, tc.Target(1), tc.Target(2))
	require.NoError(t, tc.WaitForVoters(k, tc.Target(1), tc.Target(2)))

	ts := tc.Servers[1]
	store, pErr := ts.GetStores().(*kvserver.Stores).GetStore(ts.GetFirstStoreID())
	if pErr != nil {
		t.Fatal(pErr)
	}

	{
		repl1 := store.LookupReplica(roachpb.RKey(k))
		require.NotNil(t, repl1)

		// Put some bogus sideloaded data on the replica which we're about to
		// remove. Then, at the end of the test, check that that sideloaded
		// storage is now empty (in other words, GC'ing the Replica took care of
		// cleanup).
		repl1.RaftLock()
		dir := repl1.SideloadedRaftMuLocked().Dir()
		repl1.RaftUnlock()

		require.NotEmpty(t, dir, "no sideloaded directory")
		eng := store.StateEngine()
		require.NoError(t, eng.Env().MkdirAll(dir, os.ModePerm))
		require.NoError(t, fs.WriteFile(eng.Env(), filepath.Join(dir, "i1000000.t100000"), []byte("foo"), fs.UnspecifiedWriteCategory))

		defer func() {
			if !t.Failed() {
				testutils.SucceedsSoon(t, func() error {
					// Verify that the whole directory for the replica is gone.
					repl1.RaftLock()
					dir := repl1.SideloadedRaftMuLocked().Dir()
					repl1.RaftUnlock()
					_, err := eng.Env().Stat(dir)
					if oserror.IsNotExist(err) {
						return nil
					}
					// nolint:errwrap
					return errors.Errorf("replica still has sideloaded files despite GC: %v", err)
				})
			}
		}()
	}

	desc := tc.RemoveVotersOrFatal(t, k, tc.Target(1))

	// Make sure the range is removed from the store.
	testutils.SucceedsSoon(t, func() error {
		if _, err := store.GetReplica(desc.RangeID); !testutils.IsError(err, "r[0-9]+ was not found") {
			// nolint:errwrap
			return errors.Errorf("expected range removal: %v", err)
		}
		return nil
	})
}

// TestReplicaGCQueueDropReplicaOnScan verifies that the range GC queue
// removes a range from a store that no longer should have a replica.
func TestReplicaGCQueueDropReplicaGCOnScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						DisableEagerReplicaRemoval: true,
					},
				},
			},
		},
	)
	defer tc.Stopper().Stop(context.Background())

	ts := tc.Servers[1]
	store, pErr := ts.GetStores().(*kvserver.Stores).GetStore(ts.GetFirstStoreID())
	if pErr != nil {
		t.Fatal(pErr)
	}
	// Disable the replica gc queue to prevent direct removal of replica.
	store.SetReplicaGCQueueActive(false)

	// Create our scratch range and up-replicate it.
	k := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, k, tc.Target(1), tc.Target(2))
	require.NoError(t, tc.WaitForVoters(k, tc.Target(1), tc.Target(2)))

	desc := tc.RemoveVotersOrFatal(t, k, tc.Target(1))

	// Wait long enough for the direct replica GC to have had a chance and been
	// discarded because the queue is disabled.
	time.Sleep(10 * time.Millisecond)
	if _, err := store.GetReplica(desc.RangeID); err != nil {
		t.Error("unexpected range removal")
	}

	// Enable the queue.
	store.SetReplicaGCQueueActive(true)

	// Make sure the range is removed from the store.
	testutils.SucceedsSoon(t, func() error {
		store.MustForceReplicaGCScanAndProcess()
		if _, err := store.GetReplica(desc.RangeID); !testutils.IsError(err, "r[0-9]+ was not found") {
			// nolint:errwrap
			return errors.Errorf("expected range removal: %v", err) // NB: errors.Wrapf(nil, ...) returns nil.
		}
		return nil
	})
}

// TestReplicaGCQueueHandlesStagingError verifies that replica removal handles
// errors from the staging phase (stageDestroyReplica) gracefully: the error
// propagates without crashing, the replica survives intact, and a retry
// succeeds once the error clears. Three removal paths are exercised:
//
//  1. Direct RemoveReplica call — observes the error return directly.
//  2. Replica GC queue — the queue-based removal after voter removal.
//  3. getOrCreateReplica — simulates a newer replica contacting the store.
//
// The eager self-removal path (ChangeReplicasTrigger) is not tested because it
// fatals on error — the node must succeed at removing itself after applying the
// trigger (and it uses DestroyData: false, so staging is not involved).
func TestReplicaGCQueueHandlesStagingError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numNodes = 3
	const targetNode = numNodes - 1 // s3 (whose replica we test on)

	errInjected := errors.New("injected staging error")
	var shouldFail atomic.Bool
	shouldFail.Store(true)
	var failCount atomic.Int32

	knobs := &kvserver.StoreTestingKnobs{
		// Force removal through the GC queue instead of the eager
		// self-removal path (which fatals on error).
		DisableEagerReplicaRemoval: true,
		TestingReplicaDestroyErr: func() error {
			if shouldFail.Load() {
				failCount.Add(1)
				return errInjected
			}
			return nil
		},
	}
	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgsPerNode: map[int]base.TestServerArgs{
			targetNode: {Knobs: base.TestingKnobs{Store: knobs}},
		},
	})
	defer tc.Stopper().Stop(context.Background())

	// Create range, replicate to all nodes.
	k := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, k, tc.Target(1), tc.Target(2))
	require.NoError(t, tc.WaitForVoters(k, tc.Target(1), tc.Target(2)))

	ts := tc.Servers[targetNode]
	store, err := ts.GetStores().(*kvserver.Stores).GetStore(ts.GetFirstStoreID())
	require.NoError(t, err)

	// Remove voter from targetNode, making the replica GC-able.
	// With DisableEagerReplicaRemoval, the replica stays around until
	// the GC queue picks it up.
	desc := tc.RemoveVotersOrFatal(t, k, tc.Target(targetNode))

	// Call RemoveReplica directly to observe the error.
	repl, err := store.GetReplica(desc.RangeID)
	require.NoError(t, err)
	err = store.RemoveReplica(context.Background(), repl, desc.NextReplicaID, "test")
	require.True(t, errors.Is(err, errInjected), "unexpected: %+v", err)
	require.NotZero(t, failCount.Swap(0), "error injection was not hit")

	// Force a GC scan — the queue should also hit the injected error.
	store.MustForceReplicaGCScanAndProcess()
	require.NotZero(t, failCount.Swap(0), "error injection was not hit")

	// Replica should still exist because removal failed.
	_, err = store.GetReplica(desc.RangeID)
	require.NoError(t, err, "replica should still exist after failed removal")

	// Exercise the getOrCreateReplica path: simulate an incoming raft message
	// from a newer incarnation of this replica. This triggers tryGetReplica to
	// attempt removal of the current replica (since the incoming ReplicaID is
	// higher), which hits the injected staging error.
	_, _, err = store.GetOrCreateReplica(
		context.Background(), desc.RangeID,
		repl.ReplicaID()+1, /* pretend a newer replica is contacting us */
	)
	require.True(t, errors.Is(err, errInjected), "unexpected: %+v", err)
	require.NotZero(t, failCount.Swap(0), "error injection was not hit")

	// The replica should still exist — the failed removal left no side effects.
	_, err = store.GetReplica(desc.RangeID)
	require.NoError(t, err, "replica should survive failed getOrCreateReplica removal")

	// Remove error injection, force a GC scan — should succeed.
	shouldFail.Store(false)

	testutils.SucceedsSoon(t, func() error {
		store.MustForceReplicaGCScanAndProcess()
		if store.GetReplicaIfExists(desc.RangeID) != nil {
			return errors.New("expected replica removal")
		}
		return nil
	})
}

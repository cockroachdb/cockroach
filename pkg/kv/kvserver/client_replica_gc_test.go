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
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
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
		func(filterArgs kvserverbase.FilterArgs) *roachpb.Error {
			et, ok := filterArgs.Req.(*roachpb.EndTxnRequest)
			if !ok || filterArgs.Sid != 2 {
				return nil
			}
			crt := et.InternalCommitTrigger.GetChangeReplicasTrigger()
			if crt == nil || crt.DeprecatedChangeType != roachpb.REMOVE_VOTER {
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
	store, pErr := ts.Stores().GetStore(ts.GetFirstStoreID())
	if pErr != nil {
		t.Fatal(pErr)
	}

	{
		repl1 := store.LookupReplica(roachpb.RKey(k))
		require.NotNil(t, repl1)

		eng := store.Engine()

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
		if err := eng.MkdirAll(dir); err != nil {
			t.Fatal(err)
		}
		if err := fs.WriteFile(eng, filepath.Join(dir, "i1000000.t100000"), []byte("foo")); err != nil {
			t.Fatal(err)
		}

		defer func() {
			if !t.Failed() {
				testutils.SucceedsSoon(t, func() error {
					// Verify that the whole directory for the replica is gone.
					repl1.RaftLock()
					dir := repl1.SideloadedRaftMuLocked().Dir()
					repl1.RaftUnlock()
					_, err := eng.Stat(dir)
					if oserror.IsNotExist(err) {
						return nil
					}
					return errors.Errorf("replica still has sideloaded files despite GC: %v", err)
				})
			}
		}()
	}

	desc := tc.RemoveVotersOrFatal(t, k, tc.Target(1))

	// Make sure the range is removed from the store.
	testutils.SucceedsSoon(t, func() error {
		if _, err := store.GetReplica(desc.RangeID); !testutils.IsError(err, "r[0-9]+ was not found") {
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
	store, pErr := ts.Stores().GetStore(ts.GetFirstStoreID())
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
			return errors.Errorf("expected range removal: %v", err) // NB: errors.Wrapf(nil, ...) returns nil.
		}
		return nil
	})
}

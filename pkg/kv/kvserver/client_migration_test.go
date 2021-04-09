// Copyright 2020 The Cockroach Authors.
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
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestStorePurgeOutdatedReplicas sets up a replica in the replica GC
// queue without actually processing it, runs a migration against all other
// replicas of the range (thus rendering the GC-able replica as "outdated", i.e.
// with a replica version less than the latest possible). It then checks to see
// that PurgeOutdatedReplicas does in fact remove the outdated replica.
func TestStorePurgeOutdatedReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// The two sub-tests correspond to whether or not all replicas in the system
	// come with a replica version installed. Replica versions were only
	// introduced in the 21.1 cycle; replicas instantiated pre-21.1 have this
	// unset. We'll want to test that PurgeOutdatedReplicas, when invoked, also
	// clears out replicas without a version attached. These replicas
	// are necessarily "outdated", else they'd be migrated into using replica
	// versions (see clusterversion.ReplicaVersions).
	for _, withInitialVersion := range []bool{true, false} {
		t.Run(fmt.Sprintf("with-initial-version=%t", withInitialVersion), func(t *testing.T) {
			const numStores = 3
			ctx := context.Background()
			migrationVersion := roachpb.Version{Major: 42}

			storeKnobs := &kvserver.StoreTestingKnobs{
				DisableEagerReplicaRemoval: true,
				DisableReplicaGCQueue:      true,
			}
			if !withInitialVersion {
				storeKnobs.InitialReplicaVersionOverride = &roachpb.Version{}
			}

			tc := testcluster.StartTestCluster(t, numStores,
				base.TestClusterArgs{
					ReplicationMode: base.ReplicationManual,
					ServerArgs: base.TestServerArgs{
						Knobs: base.TestingKnobs{
							Store: storeKnobs,
						},
					},
				},
			)
			defer tc.Stopper().Stop(context.Background())

			// Create our scratch range and replicate it to n2 and n3.
			n1, n2, n3 := 0, 1, 2
			k := tc.ScratchRange(t)
			tc.AddVotersOrFatal(t, k, tc.Target(n2), tc.Target(n3))
			require.NoError(t, tc.WaitForVoters(k, tc.Target(n2), tc.Target(n3)))

			for _, node := range []int{n2, n3} {
				ts := tc.Servers[node]
				store, pErr := ts.Stores().GetStore(ts.GetFirstStoreID())
				if pErr != nil {
					t.Fatal(pErr)
				}

				require.NotNil(t, store.LookupReplica(roachpb.RKey(k)))
			}

			// Mark the replica on n2 as eligible for GC.
			desc := tc.RemoveVotersOrFatal(t, k, tc.Target(n2))

			// We register an interceptor seeing as how we're attempting a (dummy) below
			// raft migration below.
			unregister := batcheval.TestingRegisterMigrationInterceptor(migrationVersion, func() {})
			defer unregister()

			// Migrate the remaining replicas on n1 and n3.
			if err := tc.Server(n1).DB().Migrate(ctx, desc.StartKey, desc.EndKey, migrationVersion); err != nil {
				t.Fatal(err)
			}

			ts := tc.Servers[n2]
			store, pErr := ts.Stores().GetStore(ts.GetFirstStoreID())
			if pErr != nil {
				t.Fatal(pErr)
			}

			// Check to see that the replica still exists on n2.
			require.NotNil(t, store.LookupReplica(roachpb.RKey(k)))

			if err := store.PurgeOutdatedReplicas(ctx, migrationVersion); err != nil {
				t.Fatal(err)
			}

			// Check to see that the replica was purged from n2.
			require.Nil(t, store.LookupReplica(roachpb.RKey(k)))
		})
	}
}

// TestMigrateWithInflightSnapshot checks to see that the Migrate command blocks
// in the face of an in-flight snapshot that hasn't yet instantiated the
// target replica. We expect the Migrate command to wait for its own application
// on all replicas, including learners.
func TestMigrateWithInflightSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var once sync.Once
	blockUntilSnapshotCh := make(chan struct{})
	blockSnapshotsCh := make(chan struct{})
	knobs, ltk := makeReplicationTestKnobs()
	ltk.storeKnobs.DisableRaftSnapshotQueue = true // we'll control it ourselves
	ltk.storeKnobs.ReceiveSnapshot = func(h *kvserver.SnapshotRequest_Header) error {
		// We'll want a signal for when the snapshot was received by the sender.
		once.Do(func() { close(blockUntilSnapshotCh) })

		// We'll also want to temporarily stall incoming snapshots.
		select {
		case <-blockSnapshotsCh:
		case <-time.After(10 * time.Second):
			return errors.New(`test timed out`)
		}
		return nil
	}
	ctx := context.Background()
	migrationVersion := roachpb.Version{Major: 42}
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ServerArgs:      base.TestServerArgs{Knobs: knobs},
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	k := tc.ScratchRange(t)
	g := ctxgroup.WithContext(ctx)
	n1, n2 := 0, 1
	g.GoCtx(func(ctx context.Context) error {
		_, err := tc.AddVoters(k, tc.Target(n2))
		return err
	})

	// Wait until the snapshot starts, which happens after the learner has been
	// added.
	<-blockUntilSnapshotCh
	desc := tc.LookupRangeOrFatal(t, k)
	require.Len(t, desc.Replicas().VoterDescriptors(), 1)
	require.Len(t, desc.Replicas().LearnerDescriptors(), 1)

	// Enqueue the replica in the raftsnapshot queue. We use SucceedsSoon
	// because it may take a bit for raft to figure out that we need to be
	// generating a snapshot.
	store := tc.GetFirstStoreFromServer(t, 0)
	repl, err := store.GetReplica(desc.RangeID)
	require.NoError(t, err)
	testutils.SucceedsSoon(t, func() error {
		trace, processErr, err := store.ManuallyEnqueue(ctx, "raftsnapshot", repl, true /* skipShouldQueue */)
		if err != nil {
			return err
		}
		if processErr != nil {
			return processErr
		}
		const msg = `skipping snapshot; replica is likely a LEARNER in the process of being added: (n2,s2):2LEARNER`
		formattedTrace := trace.String()
		if !strings.Contains(formattedTrace, msg) {
			return errors.Errorf(`expected "%s" in trace got:\n%s`, msg, formattedTrace)
		}
		return nil
	})

	unregister := batcheval.TestingRegisterMigrationInterceptor(migrationVersion, func() {})
	defer unregister()

	// Migrate the scratch range. We expect this to hang given the in-flight
	// snapshot is held up.
	func() {
		cCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()

		err := tc.Server(n1).DB().Migrate(cCtx, desc.StartKey, desc.EndKey, migrationVersion)
		require.True(t, testutils.IsError(err, context.DeadlineExceeded.Error()), err)
	}()

	// Unblock the snapshot and let the learner get promoted to a voter.
	close(blockSnapshotsCh)
	require.NoError(t, g.Wait())

	// We expect the migration attempt to go through now.
	if err := tc.Server(n1).DB().Migrate(ctx, desc.StartKey, desc.EndKey, migrationVersion); err != nil {
		t.Fatal(err)
	}

	for _, node := range []int{n1, n2} {
		ts := tc.Servers[node]
		store, pErr := ts.Stores().GetStore(ts.GetFirstStoreID())
		if pErr != nil {
			t.Fatal(pErr)
		}

		repl := store.LookupReplica(roachpb.RKey(k))
		require.NotNil(t, repl)
		require.Equal(t, repl.Version(), migrationVersion)
	}
}

// TestMigrateWaitsForApplication checks to see that migrate commands wait to be
// applied on all replicas before returning to the caller.
func TestMigrateWaitsForApplication(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	n1, n2, n3 := 0, 1, 2
	blockApplicationCh := make(chan struct{})

	// We're going to be migrating from startV to endV.
	startV := roachpb.Version{Major: 41}
	endV := roachpb.Version{Major: 42}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings: cluster.MakeTestingClusterSettingsWithVersions(endV, startV, false),
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					BinaryVersionOverride:          startV,
					DisableAutomaticVersionUpgrade: 1,
				},
				Store: &kvserver.StoreTestingKnobs{
					TestingApplyFilter: func(args kvserverbase.ApplyFilterArgs) (int, *roachpb.Error) {
						if args.StoreID == roachpb.StoreID(n3) && args.State != nil && args.State.Version != nil {
							<-blockApplicationCh
						}
						return 0, nil
					},
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	// Create our scratch range and replicate it to n2 and n3.
	k := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, k, tc.Target(n2), tc.Target(n3))
	require.NoError(t, tc.WaitForVoters(k, tc.Target(n2), tc.Target(n3)))

	for _, node := range []int{n1, n2, n3} {
		ts := tc.Servers[node]
		store, pErr := ts.Stores().GetStore(ts.GetFirstStoreID())
		if pErr != nil {
			t.Fatal(pErr)
		}

		repl := store.LookupReplica(roachpb.RKey(k))
		require.NotNil(t, repl)
		require.Equal(t, repl.Version(), startV)
	}

	desc := tc.LookupRangeOrFatal(t, k)
	unregister := batcheval.TestingRegisterMigrationInterceptor(endV, func() {})
	defer unregister()

	// Migrate the scratch range. We expect this to hang given we've gated the
	// command application on n3.
	func() {
		cCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()

		err := tc.Server(n1).DB().Migrate(cCtx, desc.StartKey, desc.EndKey, endV)
		require.True(t, testutils.IsError(err, context.DeadlineExceeded.Error()), err)
	}()

	close(blockApplicationCh)

	// We expect the migration attempt to go through now.
	if err := tc.Server(n1).DB().Migrate(ctx, desc.StartKey, desc.EndKey, endV); err != nil {
		t.Fatal(err)
	}

	for _, node := range []int{n1, n2, n3} {
		ts := tc.Servers[node]
		store, pErr := ts.Stores().GetStore(ts.GetFirstStoreID())
		if pErr != nil {
			t.Fatal(pErr)
		}

		repl := store.LookupReplica(roachpb.RKey(k))
		require.NotNil(t, repl)
		require.Equal(t, repl.Version(), endV)
	}
}

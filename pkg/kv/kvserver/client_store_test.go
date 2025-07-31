// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/listenerutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestStoreSetRangesMaxBytes creates a set of ranges via splitting and then
// sets the config zone to a custom max bytes value to verify the ranges' max
// bytes are updated appropriately.
func TestStoreSetRangesMaxBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const expMaxBytes, defaultMaxBytes = 420 << 20, 512 << 20
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					SpanConfig: &spanconfig.TestingKnobs{
						ConfigureScratchRange: true,
					},
				},
			},
		},
	)
	defer tc.Stopper().Stop(ctx)
	store := tc.GetFirstStoreFromServer(t, 0)
	tdb := sqlutils.MakeSQLRunner(tc.Conns[0])

	tdb.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'`) // speeds up the test

	testKey := tc.ScratchRange(t)
	testutils.SucceedsSoon(t, func() error {
		repl := store.LookupReplica(roachpb.RKey(testKey))
		if got := repl.GetMaxBytes(ctx); got != defaultMaxBytes {
			return errors.Errorf("range max bytes values did not start at %d; got %d", defaultMaxBytes, got)
		}
		return nil
	})

	tdb.Exec(t, `ALTER RANGE DEFAULT CONFIGURE ZONE USING range_max_bytes = $1`, expMaxBytes)

	testutils.SucceedsSoon(t, func() error {
		repl := store.LookupReplica(roachpb.RKey(testKey))
		if got := repl.GetMaxBytes(ctx); got != expMaxBytes {
			return errors.Errorf("range max bytes values did not change to %d; got %d", expMaxBytes, got)
		}
		return nil
	})
}

// TestStoreRaftReplicaID tests that initialized replicas have a
// RaftReplicaID.
func TestStoreRaftReplicaID(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	srv := tc.Server(0)
	store, err := srv.GetStores().(*kvserver.Stores).GetStore(srv.GetFirstStoreID())
	require.NoError(t, err)

	scratchKey := tc.ScratchRange(t)
	desc, err := tc.LookupRange(scratchKey)
	require.NoError(t, err)
	repl, err := store.GetReplica(desc.RangeID)
	require.NoError(t, err)
	replicaID, err := stateloader.Make(desc.RangeID).LoadRaftReplicaID(ctx, store.TODOEngine())
	require.NoError(t, err)
	require.Equal(t, repl.ReplicaID(), replicaID.ReplicaID)

	// RHS of a split also has ReplicaID.
	splitKey := append(scratchKey, '0', '0')
	_, rhsDesc, err := tc.SplitRange(splitKey)
	require.NoError(t, err)
	rhsRepl, err := store.GetReplica(rhsDesc.RangeID)
	require.NoError(t, err)
	rhsReplicaID, err :=
		stateloader.Make(rhsDesc.RangeID).LoadRaftReplicaID(ctx, store.TODOEngine())
	require.NoError(t, err)
	require.Equal(t, rhsRepl.ReplicaID(), rhsReplicaID.ReplicaID)
}

// TestStoreLoadReplicaQuiescent tests whether replicas are initially quiescent
// when loaded during store start, with eager Raft group initialization. Epoch
// lease ranges will initially be quiesced (unless acquired by a store replica
// queue), but expiration leases shouldn't.
func TestStoreLoadReplicaQuiescent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "lease-type", roachpb.TestingAllLeaseTypes(), func(t *testing.T, leaseType roachpb.LeaseType) {
		storeReg := fs.NewStickyRegistry()
		listenerReg := listenerutil.NewListenerRegistry()
		defer listenerReg.Close()

		ctx := context.Background()
		st := cluster.MakeTestingClusterSettings()
		kvserver.OverrideDefaultLeaseType(ctx, &st.SV, leaseType)

		manualClock := hlc.NewHybridManualClock()
		tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
			ReplicationMode:     base.ReplicationManual,
			ReusableListenerReg: listenerReg,
			ServerArgs: base.TestServerArgs{
				Settings: st,
				RaftConfig: base.RaftConfig{
					RaftTickInterval: 100 * time.Millisecond,
				},
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						StickyVFSRegistry: storeReg,
						WallClock:         manualClock,
					},
					Store: &kvserver.StoreTestingKnobs{
						DisableScanner: true,
						// The lease queue will unquiesce the range and acquire the
						// proscribed leases upon restart. The range should quiesce again
						// in the regular duration.
						DisableLeaseQueue: true,
					},
				},
				StoreSpecs: []base.StoreSpec{
					{
						InMemory:    true,
						StickyVFSID: "test",
					},
				},
			},
		})
		defer tc.Stopper().Stop(ctx)

		// Split off a scratch range.
		key := tc.ScratchRange(t)
		desc := tc.LookupRangeOrFatal(t, key)
		repl := tc.GetFirstStoreFromServer(t, 0).GetReplicaIfExists(desc.RangeID)
		require.NotNil(t, repl)
		lease, _ := repl.GetLease()

		if leaseType == roachpb.LeaseLeader {
			// The first lease that'll be acquired above is going to be an expiration
			// based lease. That's because at that point, there won't be a leader,
			// and the lease acquisition will trigger an election. Expire that lease
			// and send a request that'll force a re-acquisition. This time, we should
			// get a leader lease.
			manualClock.Increment(tc.Server(0).RaftConfig().RangeLeaseDuration.Nanoseconds())
			incArgs := incrementArgs(key, int64(5))

			testutils.SucceedsSoon(t, func() error {
				_, err := kv.SendWrapped(ctx, tc.GetFirstStoreFromServer(t, 0).TestSender(), incArgs)
				return err.GoError()
			})
			lease, _ = repl.GetLease()
			require.Equal(t, roachpb.LeaseLeader, lease.Type())
		}

		switch leaseType {
		case roachpb.LeaseExpiration:
			require.Equal(t, roachpb.LeaseExpiration, lease.Type())
		case roachpb.LeaseEpoch:
			require.Equal(t, roachpb.LeaseEpoch, lease.Type())
		case roachpb.LeaseLeader:
			require.Equal(t, roachpb.LeaseLeader, lease.Type())
		default:
			panic("unknown")
		}

		// Restart the server and check whether the range starts out quiesced.
		tc.StopServer(0)
		require.NoError(t, tc.RestartServer(0))

		var err error
		repl, _, err = tc.Server(0).GetStores().(*kvserver.Stores).GetReplicaForRangeID(ctx, desc.RangeID)
		require.NoError(t, err)
		require.NotNil(t, repl.RaftStatus())
		switch leaseType {
		case roachpb.LeaseExpiration, roachpb.LeaseLeader:
			require.False(t, repl.IsQuiescent())
		case roachpb.LeaseEpoch:
			require.True(t, repl.IsQuiescent())
		default:
			panic("unknown")
		}
	})
}

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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/listenerutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
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
// lease ranges should be quiesced, but expiration leases shouldn't.
func TestStoreLoadReplicaQuiescent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "kv.expiration_leases_only.enabled", func(t *testing.T, expOnly bool) {
		storeReg := server.NewStickyVFSRegistry()
		listenerReg := listenerutil.NewListenerRegistry()
		defer listenerReg.Close()

		ctx := context.Background()
		st := cluster.MakeTestingClusterSettings()
		kvserver.ExpirationLeasesOnly.Override(ctx, &st.SV, expOnly)

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
					},
					Store: &kvserver.StoreTestingKnobs{
						DisableScanner: true,
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
		if expOnly {
			require.Equal(t, roachpb.LeaseExpiration, lease.Type())
		} else {
			require.Equal(t, roachpb.LeaseEpoch, lease.Type())
		}

		// Restart the server and check whether the range starts out quiesced.
		tc.StopServer(0)
		require.NoError(t, tc.RestartServer(0))

		var err error
		repl, _, err = tc.Server(0).GetStores().(*kvserver.Stores).GetReplicaForRangeID(ctx, desc.RangeID)
		require.NoError(t, err)
		require.NotNil(t, repl.RaftStatus())
		require.Equal(t, !expOnly, repl.IsQuiescent())
	})
}

// Copyright 2019 The Cockroach Authors.
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
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/confchange"
	"go.etcd.io/etcd/raft/v3/tracker"
)

// TestAtomicReplicationChange is a simple smoke test for atomic membership
// changes.
func TestAtomicReplicationChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	args := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{},
			},
		},
		ReplicationMode: base.ReplicationManual,
	}
	tc := testcluster.StartTestCluster(t, 6, args)
	defer tc.Stopper().Stop(ctx)

	// Create a range and put it on n1, n2, n3. Intentionally do this one at a
	// time so we're not using atomic replication changes yet.
	k := tc.ScratchRange(t)
	desc, err := tc.AddVoters(k, tc.Target(1))
	require.NoError(t, err)
	desc, err = tc.AddVoters(k, tc.Target(2))
	require.NoError(t, err)

	runChange := func(expDesc roachpb.RangeDescriptor, chgs []roachpb.ReplicationChange) roachpb.RangeDescriptor {
		t.Helper()
		desc, err := tc.Servers[0].DB().AdminChangeReplicas(ctx, k, expDesc, chgs)
		require.NoError(t, err)

		return *desc
	}

	checkDesc := func(desc roachpb.RangeDescriptor, expStores ...roachpb.StoreID) {
		testutils.SucceedsSoon(t, func() error {
			var sawStores []roachpb.StoreID
			for _, s := range tc.Servers {
				r, _, _ := s.Stores().GetReplicaForRangeID(ctx, desc.RangeID)
				if r == nil {
					continue
				}
				if _, found := desc.GetReplicaDescriptor(r.StoreID()); !found {
					// There's a replica but it's not in the new descriptor, so
					// it should be replicaGC'ed soon.
					return errors.Errorf("%s should have been removed", r)
				}
				sawStores = append(sawStores, r.StoreID())
				// Check that in-mem descriptor of repl is up-to-date.
				if diff := pretty.Diff(&desc, r.Desc()); len(diff) > 0 {
					return errors.Errorf("diff(want, have):\n%s", strings.Join(diff, "\n"))
				}
				// Check that conf state is up to date. This can fail even though
				// the descriptor already matches since the descriptor is updated
				// a hair earlier.
				cfg, _, err := confchange.Restore(confchange.Changer{
					Tracker:   tracker.MakeProgressTracker(1),
					LastIndex: 1,
				}, desc.Replicas().ConfState())
				require.NoError(t, err)
				act := r.RaftStatus().Config.Voters
				if diff := pretty.Diff(cfg.Voters, act); len(diff) > 0 {
					return errors.Errorf("diff(exp,act):\n%s", strings.Join(diff, "\n"))
				}
			}
			assert.Equal(t, expStores, sawStores)
			return nil
		})
	}

	// Run a fairly general change.
	desc = runChange(desc, []roachpb.ReplicationChange{
		{ChangeType: roachpb.ADD_VOTER, Target: tc.Target(3)},
		{ChangeType: roachpb.ADD_VOTER, Target: tc.Target(5)},
		{ChangeType: roachpb.REMOVE_VOTER, Target: tc.Target(2)},
		{ChangeType: roachpb.ADD_VOTER, Target: tc.Target(4)},
	})

	// Replicas should now live on all stores except s3.
	checkDesc(desc, 1, 2, 4, 5, 6)

	// Transfer the lease to s5.
	require.NoError(t, tc.TransferRangeLease(desc, tc.Target(4)))

	// Rebalance back down all the way.
	desc = runChange(desc, []roachpb.ReplicationChange{
		{ChangeType: roachpb.REMOVE_VOTER, Target: tc.Target(0)},
		{ChangeType: roachpb.REMOVE_VOTER, Target: tc.Target(1)},
		{ChangeType: roachpb.REMOVE_VOTER, Target: tc.Target(3)},
		{ChangeType: roachpb.REMOVE_VOTER, Target: tc.Target(5)},
	})

	// Only a lone voter on s5 should be left over.
	checkDesc(desc, 5)
}

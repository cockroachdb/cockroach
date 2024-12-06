// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestDecommission(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Five nodes is too much to reliably run under race/deadlock with our
	// aggressive liveness timings.
	skip.UnderRaceWithIssue(t, 39807, "#39807 and #37811")
	skip.UnderDeadlockWithIssue(t, 39807, "#39807 and #37811")

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 5, base.TestClusterArgs{
		ReplicationMode: base.ReplicationAuto,
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SpanConfig: &spanconfig.TestingKnobs{
					ConfigureScratchRange: true,
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	k := tc.ScratchRange(t)
	admin := tc.GetAdminClient(t, 0)

	// Decommission the first node, which holds most of the leases.
	_, err := admin.Decommission(
		ctx, &serverpb.DecommissionRequest{
			NodeIDs:          []roachpb.NodeID{1},
			TargetMembership: livenesspb.MembershipStatus_DECOMMISSIONING,
		},
	)
	require.NoError(t, err)

	requireNoReplicas := func(storeID roachpb.StoreID, repFactor int) {
		attempt := 0
		testutils.SucceedsSoon(t, func() error {
			attempt++
			desc := tc.LookupRangeOrFatal(t, k)
			for _, rDesc := range desc.Replicas().VoterDescriptors() {
				store, err := tc.Servers[int(rDesc.NodeID-1)].GetStores().(*kvserver.Stores).GetStore(rDesc.StoreID)
				require.NoError(t, err)
				if err := store.ForceReplicationScanAndProcess(); err != nil {
					return err
				}
			}
			if sl := desc.Replicas().FilterToDescriptors(func(rDesc roachpb.ReplicaDescriptor) bool {
				return rDesc.StoreID == storeID
			}); len(sl) > 0 {
				return errors.Errorf("still a replica on s%d: %s on attempt %d", storeID, &desc, attempt)
			}
			if len(desc.Replicas().VoterDescriptors()) != repFactor {
				return errors.Errorf("expected %d replicas: %s on attempt %d", repFactor, &desc, attempt)
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
			NodeIDs:          []roachpb.NodeID{2},
			TargetMembership: livenesspb.MembershipStatus_DECOMMISSIONING,
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

	sqlutils.SetZoneConfig(t, runner, "RANGE default", "num_replicas = 1")

	const single = 1

	// The range should drop down to one replica on a non-decommissioning store.
	requireNoReplicas(1, single)
	requireNoReplicas(2, single)

	// Decommission two more nodes. Only n5 is left; getting the replicas there
	// can't use atomic replica swaps because the leaseholder can't be removed.
	_, err = admin.Decommission(
		ctx, &serverpb.DecommissionRequest{
			NodeIDs:          []roachpb.NodeID{3, 4},
			TargetMembership: livenesspb.MembershipStatus_DECOMMISSIONING,
		},
	)
	require.NoError(t, err)

	requireNoReplicas(1, single)
	requireNoReplicas(2, single)
	requireNoReplicas(3, single)
	requireNoReplicas(4, single)
}

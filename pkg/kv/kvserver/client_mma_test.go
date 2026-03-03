// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestMMAUpreplication verifies that MMA correctly upreplicates a scratch range
// from 1 to 3 voters when operating in MultiMetricRepairAndRebalance mode,
// where the replicate and lease queues are fully disabled.
func TestMMAUpreplication(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Pre-configure the rebalance interval so that the MMA rebalancer ticks
	// frequently from the start. This must be set before cluster start because
	// the MMA rebalancer creates its ticker in run() and only re-reads the
	// interval on subsequent ticks; it cannot react to interval changes between
	// ticks.
	//
	// TODO(tbg): the MMA rebalancer should register a settings change callback
	// to reset its ticker when the interval changes.
	st := cluster.MakeTestingClusterSettings()
	allocator.LoadBasedRebalanceInterval.Override(ctx, &st.SV, time.Second)

	// Use ReplicationManual to avoid automatic upreplication by the replicate
	// queue. We configure span configs for the scratch range so MMA knows the
	// desired replication factor.
	//
	// NB: we can't use ReplicationAuto here because WaitForFullReplication
	// calls ForceReplicationScanAndProcess, which bypasses the replicate
	// queue's shouldQueue check. This means the replicate queue would
	// upreplicate all ranges even when shouldQueue returns false (as it does
	// under LBRebalancingMultiMetricRepairAndRebalance).
	//
	// TODO(tbg): ForceReplicationScanAndProcess should respect the MMA setting
	// and route repair work through the MMA rebalancer instead of the replicate
	// queue when LBRebalancingMultiMetricRepairAndRebalance is active.
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings: st,
			Knobs: base.TestingKnobs{
				SpanConfig: &spanconfig.TestingKnobs{
					ConfigureScratchRange: true,
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	k := tc.ScratchRange(t)

	// The scratch range starts with a single replica. Enable MMA
	// repair-and-rebalance mode on each server so it upreplicates.
	//
	// NB: TestCluster clones the Settings object per server (even when using
	// ServerArgsPerNode), so we must override on each server's own
	// ClusterSettings() rather than the original `st`.
	//
	// TODO(tbg): consider whether TestCluster should avoid this implicit
	// cloning, or at least document it more prominently.
	for _, server := range tc.Servers {
		serverSt := server.ClusterSettings()
		kvserverbase.LoadBasedRebalancingMode.Override(
			ctx, &serverSt.SV, kvserverbase.LBRebalancingMultiMetricRepairAndRebalance,
		)
	}

	// Wait for the scratch range to upreplicate to 3 voters. The MMA rebalancer
	// runs on a 1s timer and can perform multiple repairs per tick (1->2->3),
	// so this should converge well within SucceedsSoon's timeout.
	testutils.SucceedsSoon(t, func() error {
		desc := tc.LookupRangeOrFatal(t, k)
		voters := desc.Replicas().VoterDescriptors()
		if len(voters) != 3 {
			return errors.Errorf("expected 3 voters, got %d: %s", len(voters), &desc)
		}
		return nil
	})

	desc := tc.LookupRangeOrFatal(t, k)
	require.Len(t, desc.Replicas().VoterDescriptors(), 3)
}

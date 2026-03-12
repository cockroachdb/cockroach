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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
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

	// Use ReplicationAuto so that system ranges are upreplicated via the
	// replicate queue before we enable MMA. WaitForFullReplication (called
	// by StartTestCluster) drives this via ForceReplicationScanAndProcess.
	// Once MMA is enabled, ForceReplicationScanAndProcess delegates to the
	// MMA rebalancer instead of the replicate queue.
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationAuto,
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

	// Wait for the scratch range to upreplicate to 3 voters. We explicitly
	// call ForceReplicationScanAndProcess on each store to deterministically
	// drive MMA repair (instead of waiting for the background timer).
	testutils.SucceedsSoon(t, func() error {
		for _, server := range tc.Servers {
			if err := server.GetStores().(*kvserver.Stores).VisitStores(func(s *kvserver.Store) error {
				return s.ForceReplicationScanAndProcess()
			}); err != nil {
				t.Fatal(err)
			}
		}
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

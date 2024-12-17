// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package kvserver_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestReplicaRaftOverload is an end-to-end test verifying that leaseholders
// will "pause" followers that are on overloaded stores, and will unpause when
// the overload ends.
//
// This primarily tests the gossip signal as well as the mechanics of pausing,
// and does not check that "pausing" really results in no traffic being sent
// to the followers.
func TestReplicaRaftOverload(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	var on atomic.Value // bool
	on.Store(false)
	var args base.TestClusterArgs
	args.ReplicationMode = base.ReplicationManual
	args.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		GossipTestingKnobs: kvserver.StoreGossipTestingKnobs{
			StoreGossipIntercept: func(descriptor *roachpb.StoreDescriptor) {
				if !on.Load().(bool) || descriptor.StoreID != 3 {
					return
				}
				descriptor.Capacity.IOThreshold = admissionpb.IOThreshold{
					L0NumSubLevels:          1000000,
					L0NumSubLevelsThreshold: 1,
					L0NumFiles:              1000000,
					L0NumFilesThreshold:     1,
				}
			},
		}}
	tc := testcluster.StartTestCluster(t, 3, args)
	defer tc.Stopper().Stop(ctx)

	{
		_, err := tc.ServerConn(0).Exec(`SET CLUSTER SETTING admission.kv.pause_replication_io_threshold = 1.0`)
		require.NoError(t, err)
		// Replica pausing is disabled in apply_to_all mode, so make sure the
		// cluster is running with a setting where replica pausing is enabled.
		_, err = tc.ServerConn(0).Exec(`SET CLUSTER SETTING kvadmission.flow_control.mode = "apply_to_elastic"`)
		require.NoError(t, err)
	}
	k := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, k, tc.Targets(1, 2)...)
	// Also split off another range that is on s3 so that we can verify that a
	// quiesced range doesn't do anything unexpected, like not release the paused
	// follower from its map (and thus messing with metrics when overload recovers).
	tc.SplitRangeOrFatal(t, k.Next())

	// Gossip faux IO overload from s3. s1 should pick up on that and pause followers.
	on.Store(true)
	require.NoError(t, tc.GetFirstStoreFromServer(t, 2 /* n3 */).GossipStore(ctx, false /* useCached */))
	testutils.SucceedsSoon(t, func() error {
		// Touch the one range that is on s3 since it's likely quiesced, and wouldn't unquiesce
		// if s3 becomes overloaded. Note that we do no such thing for the right sibling range,
		// so it may or may not contribute here (depending on when it quiesces).
		//
		// See: https://github.com/cockroachdb/cockroach/issues/84252
		require.NoError(t, tc.Servers[0].DB().Put(ctx, tc.ScratchRange(t), "foo"))
		s1 := tc.GetFirstStoreFromServer(t, 0)
		require.NoError(t, s1.ComputeMetrics(ctx))
		if n := s1.Metrics().RaftPausedFollowerCount.Value(); n == 0 {
			return errors.New("no paused followers")
		}
		if n := s1.Metrics().RaftPausedFollowerDroppedMsgs.Count(); n == 0 {
			return errors.New("no dropped messages to paused followers")
		}
		return nil
	})

	// Now remove the gossip intercept and check again. The follower should un-pause immediately.
	on.Store(false)
	require.NoError(t, tc.GetFirstStoreFromServer(t, 2 /* n3 */).GossipStore(ctx, false /* useCached */))
	testutils.SucceedsSoon(t, func() error {
		s1 := tc.GetFirstStoreFromServer(t, 0)
		require.NoError(t, s1.ComputeMetrics(ctx))
		if n := s1.Metrics().RaftPausedFollowerCount.Value(); n > 0 {
			return errors.Errorf("%d paused followers", n)
		}
		return nil
	})
}

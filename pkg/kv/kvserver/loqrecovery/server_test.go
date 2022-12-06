// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package loqrecovery_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestReplicaCollection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc := testcluster.NewTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			StoreSpecs: []base.StoreSpec{{InMemory: true}},
			Insecure:   true,
			Knobs: base.TestingKnobs{
				LOQRecovery: &loqrecovery.TestingKnobs{
					MetadataScanTimeout: 15 * time.Second,
				},
			},
		},
	})
	tc.Start(t)
	defer tc.Stopper().Stop(ctx)
	require.NoError(t, tc.WaitForFullReplication())
	tc.ToggleReplicateQueues(false)

	r := tc.ServerConn(0).QueryRow("select count(*) from crdb_internal.ranges_no_leases")
	var totalRanges int
	require.NoError(t, r.Scan(&totalRanges), "failed to query range count")
	adm, err := tc.GetAdminClient(ctx, t, 2)
	require.NoError(t, err, "failed to get admin client")

	// Collect and assert replica metadata. For expectMeta case we sometimes have
	// meta and sometimes doesn't depending on which node holds the lease.
	// We just ignore descriptor counts if we are not expecting meta.
	assertReplicas := func(liveNodes int, expectMeta bool) {
		var replicas loqrecoverypb.ClusterReplicaInfo
		var stats loqrecovery.CollectionStats

		replicas, stats, err = loqrecovery.CollectRemoteReplicaInfo(ctx, adm)
		require.NoError(t, err, "failed to retrieve replica info")

		// Check counters on retrieved replica info.
		stores := map[roachpb.StoreID]interface{}{}
		totalReplicas := 0
		for _, nr := range replicas.LocalInfo {
			for _, r := range nr.Replicas {
				stores[r.StoreID] = struct{}{}
			}
			totalReplicas += len(nr.Replicas)
		}
		require.Equal(t, liveNodes, len(stores), "collected replicas from stores")
		if expectMeta {
			require.Equal(t, totalRanges, len(replicas.Descriptors),
				"number of collected descriptors from metadata")
		}
		require.Equal(t, totalRanges*liveNodes, totalReplicas, "number of collected replicas")
		// Check stats counters as well.
		require.Equal(t, liveNodes, stats.Nodes, "node counter stats")
		require.Equal(t, liveNodes, stats.Stores, "store counter stats")
		if expectMeta {
			require.Equal(t, totalRanges, stats.Descriptors, "range descriptor counter stats")
		}
		require.NotEqual(t, replicas.ClusterID, uuid.UUID{}.String(), "cluster UUID must not be empty")
	}

	tc.StopServer(0)
	assertReplicas(2, true)
	tc.StopServer(1)
	assertReplicas(1, false)

	tc.Stopper().Stop(ctx)
}

// TestStreamRestart verifies that if connection is dropped mid way through
// replica stream, it would be handled correctly with a stream restart that
// allows caller to rewind back partial replica data and receive consistent
// stream of replcia infos.
func TestStreamRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	var failCount atomic.Int64
	tc := testcluster.NewTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			StoreSpecs: []base.StoreSpec{{InMemory: true}},
			Insecure:   true,
			Knobs: base.TestingKnobs{
				LOQRecovery: &loqrecovery.TestingKnobs{
					MetadataScanTimeout: 15 * time.Second,
					ForwardReplicaFilter: func(response *serverpb.RecoveryCollectLocalReplicaInfoResponse) error {
						if response.ReplicaInfo.NodeID == 2 && response.ReplicaInfo.Desc.RangeID == 14 && failCount.Add(1) < 3 {
							return errors.New("rpc stream stopped")
						}
						return nil
					},
				},
			},
		},
	})
	tc.Start(t)
	defer tc.Stopper().Stop(ctx)
	require.NoError(t, tc.WaitForFullReplication())
	tc.ToggleReplicateQueues(false)

	r := tc.ServerConn(0).QueryRow("select count(*) from crdb_internal.ranges_no_leases")
	var totalRanges int
	require.NoError(t, r.Scan(&totalRanges), "failed to query range count")
	adm, err := tc.GetAdminClient(ctx, t, 2)
	require.NoError(t, err, "failed to get admin client")

	assertReplicas := func(liveNodes int) {
		var replicas loqrecoverypb.ClusterReplicaInfo
		var stats loqrecovery.CollectionStats

		replicas, stats, err = loqrecovery.CollectRemoteReplicaInfo(ctx, adm)
		require.NoError(t, err, "failed to retrieve replica info")

		// Check counters on retrieved replica info.
		stores := map[roachpb.StoreID]interface{}{}
		for _, r := range replicas.LocalInfo[0].Replicas {
			stores[r.StoreID] = struct{}{}
		}
		require.Equal(t, liveNodes, len(stores), "collected replicas from stores")
		require.Equal(t, totalRanges, len(replicas.Descriptors),
			"number of collected descriptors from metadata")
		require.Equal(t, totalRanges*liveNodes, len(replicas.LocalInfo[0].Replicas),
			"number of collected replicas")
		// Check stats counters as well.
		require.Equal(t, liveNodes, stats.Nodes, "node counter stats")
		require.Equal(t, liveNodes, stats.Stores, "store counter stats")
		require.Equal(t, totalRanges, stats.Descriptors, "range descriptor counter stats")
	}

	assertReplicas(3)

	tc.Stopper().Stop(ctx)
}

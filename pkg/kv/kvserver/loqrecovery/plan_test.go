// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package loqrecovery

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestVersionIsPreserved(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	current := clusterversion.ByKey(clusterversion.BinaryVersionKey)
	current.Patch += 1

	replicaInfo := infoWithVersion(current)

	plan, _, err := PlanReplicas(ctx, replicaInfo, nil, nil, uuid.DefaultGenerator)
	require.NoError(t, err, "good version is rejected")
	require.Equal(t, current, plan.Version, "plan version was not preserved")
}

func TestV1PlanContainOnlyRelevantInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	replicaInfo := infoWithVersion(legacyInfoFormatVersion)
	// Create a plan that could contain decom node ids and other optional fields
	// and check that none of newer fields leak to resulting plan.
	replicaInfo.LocalInfo = []loqrecoverypb.NodeReplicaInfo{
		{
			Replicas: []loqrecoverypb.ReplicaInfo{
				{
					NodeID:  1,
					StoreID: 1,
					Desc: roachpb.RangeDescriptor{
						RangeID:  1,
						StartKey: roachpb.RKeyMin,
						EndKey:   roachpb.RKeyMax,
						InternalReplicas: []roachpb.ReplicaDescriptor{
							{
								NodeID:    1,
								StoreID:   1,
								ReplicaID: 1,
							},
							{
								NodeID:    2,
								StoreID:   2,
								ReplicaID: 2,
							},
							{
								NodeID:    3,
								StoreID:   3,
								ReplicaID: 3,
							},
						},
						NextReplicaID: 4,
						Generation:    4,
					},
					RaftAppliedIndex:         0,
					RaftCommittedIndex:       0,
					RaftLogDescriptorChanges: nil,
					LocalAssumesLeaseholder:  false,
				},
			},
		},
	}

	plan, _, err := PlanReplicas(ctx, replicaInfo, nil, nil, uuid.DefaultGenerator)
	require.NoError(t, err, "good version is rejected")
	require.Nil(t, plan.DecommissionedNodeIDs, "v1 plan should have no decom nodes ids")
	require.Nil(t, plan.StaleLeaseholderNodeIDs, "v1 plan should have no stale node ids")
	require.Equal(t, uuid.UUID{}, plan.PlanID, "v1 plan should have no plan id")
	require.Empty(t, plan.ClusterID, "v1 plan should have no cluster id")
	require.Equal(t, roachpb.Version{}, plan.Version, "v1 plan should have no version")
}

// infoWithVersion creates a skeleton info that passes all checks beside version.
func infoWithVersion(v roachpb.Version) loqrecoverypb.ClusterReplicaInfo {
	return loqrecoverypb.ClusterReplicaInfo{
		ClusterID: uuid.FastMakeV4().String(),
		Version:   v,
	}
}

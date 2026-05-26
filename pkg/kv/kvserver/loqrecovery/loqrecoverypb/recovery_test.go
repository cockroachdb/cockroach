// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package loqrecoverypb

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestClusterInfoMergeChecksOverlapping(t *testing.T) {
	info123 := ClusterReplicaInfo{
		LocalInfo: []NodeReplicaInfo{
			{
				Replicas: []ReplicaInfo{
					{StoreID: 1, NodeID: 1},
					{StoreID: 2, NodeID: 1},
				},
			},
			{
				Replicas: []ReplicaInfo{
					{StoreID: 3, NodeID: 2},
				},
			},
		},
	}
	info1 := ClusterReplicaInfo{
		LocalInfo: []NodeReplicaInfo{
			{
				Replicas: []ReplicaInfo{
					{StoreID: 1, NodeID: 1},
					{StoreID: 2, NodeID: 1},
				},
			},
		},
	}
	info12 := ClusterReplicaInfo{
		LocalInfo: []NodeReplicaInfo{
			{
				Replicas: []ReplicaInfo{
					{StoreID: 2, NodeID: 1},
				},
			},
		},
	}
	info3 := ClusterReplicaInfo{
		LocalInfo: []NodeReplicaInfo{
			{
				Replicas: []ReplicaInfo{
					{StoreID: 3, NodeID: 2},
				},
			},
		},
	}

	require.Error(t, info123.Merge(info1))
	require.Error(t, info123.Merge(info12))
	require.Error(t, info123.Merge(info3))

	_ = info1.Merge(info3)
	require.EqualValues(t, info1, info123)
}

func TestClusterInfoMergeChecksDescriptor(t *testing.T) {
	info1 := ClusterReplicaInfo{
		Descriptors: []roachpb.RangeDescriptor{{RangeID: 1}},
		LocalInfo: []NodeReplicaInfo{
			{
				Replicas: []ReplicaInfo{
					{StoreID: 1, NodeID: 1},
					{StoreID: 2, NodeID: 1},
				},
			},
		},
	}
	info3 := ClusterReplicaInfo{
		Descriptors: []roachpb.RangeDescriptor{{RangeID: 2}},
		LocalInfo: []NodeReplicaInfo{
			{
				Replicas: []ReplicaInfo{
					{StoreID: 3, NodeID: 2},
				},
			},
		},
	}
	require.Error(t, info1.Merge(info3))
}

func TestClusterInfoMergeSameClusterID(t *testing.T) {
	uuid1 := uuid.MakeV4()
	info1 := ClusterReplicaInfo{
		ClusterID:   uuid1.String(),
		Descriptors: []roachpb.RangeDescriptor{{RangeID: 1}},
		LocalInfo: []NodeReplicaInfo{
			{
				Replicas: []ReplicaInfo{
					{StoreID: 1, NodeID: 1},
					{StoreID: 2, NodeID: 1},
				},
			},
		},
	}
	info3 := ClusterReplicaInfo{
		ClusterID: uuid1.String(),
		LocalInfo: []NodeReplicaInfo{
			{
				Replicas: []ReplicaInfo{
					{StoreID: 3, NodeID: 2},
				},
			},
		},
	}
	require.NoError(t, info1.Merge(info3),
		"should be able to merge partial info with equal cluster ids")
}

func TestClusterInfoMergeRejectDifferentMetadata(t *testing.T) {
	uuid1 := uuid.MakeV4()
	uuid2 := uuid.MakeV4()
	info1 := ClusterReplicaInfo{
		ClusterID:   uuid1.String(),
		Descriptors: []roachpb.RangeDescriptor{{RangeID: 1}},
		Version:     roachpb.Version{Major: 22},
		LocalInfo: []NodeReplicaInfo{
			{
				Replicas: []ReplicaInfo{
					{StoreID: 1, NodeID: 1},
					{StoreID: 2, NodeID: 1},
				},
			},
		},
	}
	info2 := ClusterReplicaInfo{
		ClusterID: uuid2.String(),
		Version:   roachpb.Version{Major: 22},
		LocalInfo: []NodeReplicaInfo{
			{
				Replicas: []ReplicaInfo{
					{StoreID: 3, NodeID: 2},
				},
			},
		},
	}
	info3 := ClusterReplicaInfo{
		ClusterID: uuid1.String(),
		Version:   roachpb.Version{Major: 23},
		LocalInfo: []NodeReplicaInfo{
			{
				Replicas: []ReplicaInfo{
					{StoreID: 3, NodeID: 2},
				},
			},
		},
	}
	require.Error(t, info1.Merge(info3), "reject merging of info from different clusters")
	require.Error(t, info1.Merge(info2), "reject merging of info from different versions")
}

func TestClusterInfoInitializeByMerge(t *testing.T) {
	uuid1 := uuid.MakeV4().String()
	version1 := roachpb.Version{Major: 22, Minor: 2}
	info := ClusterReplicaInfo{
		ClusterID:   uuid1,
		Version:     version1,
		Descriptors: []roachpb.RangeDescriptor{{RangeID: 1}},
		LocalInfo: []NodeReplicaInfo{
			{
				Replicas: []ReplicaInfo{
					{StoreID: 1, NodeID: 1},
					{StoreID: 2, NodeID: 1},
				},
			},
		},
	}
	empty := ClusterReplicaInfo{}
	require.NoError(t, empty.Merge(info), "should be able to merge into empty struct")
	require.Equal(t, empty.ClusterID, uuid1, "merge should update empty info fields")
	require.Equal(t, empty.Version, version1, "merge should update empty info fields")
}

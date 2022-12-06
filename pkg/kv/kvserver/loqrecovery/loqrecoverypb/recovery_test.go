// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package loqrecoverypb

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
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

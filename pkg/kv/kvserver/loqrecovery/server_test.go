// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package loqrecovery

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestProbeRangeHealth(t *testing.T) {
	ctx := context.Background()
	for _, d := range []struct {
		name         string
		metaNodes    []roachpb.NodeID
		replicaNodes map[roachpb.NodeID][]roachpb.NodeID
		deadNodes    map[roachpb.NodeID]interface{}
		result       loqrecoverypb.RangeHealth
	}{
		{
			name:      "meta healthy",
			metaNodes: nodes(1, 2, 3),
			result:    loqrecoverypb.RangeHealth_HEALTHY,
		},
		{
			name:      "no live replicas",
			metaNodes: nodes(1, 2, 3),
			deadNodes: nodeSet(1, 2, 3),
			result:    loqrecoverypb.RangeHealth_LOSS_OF_QUORUM,
		},
		{
			name:      "no replicas can make progress",
			metaNodes: nodes(1, 2, 3, 4, 5),
			replicaNodes: map[roachpb.NodeID][]roachpb.NodeID{
				1: nodes(1, 2, 3, 4, 5),
				2: nodes(1, 2, 3, 4, 5),
			},
			deadNodes: nodeSet(3, 4, 5),
			result:    loqrecoverypb.RangeHealth_LOSS_OF_QUORUM,
		},
		{
			name:      "all live replicas can make progress",
			metaNodes: nodes(1, 2, 3, 4, 5),
			replicaNodes: map[roachpb.NodeID][]roachpb.NodeID{
				2: nodes(2),
			},
			deadNodes: nodeSet(1, 3, 4, 5),
			result:    loqrecoverypb.RangeHealth_HEALTHY,
		},
		{
			name:      "mix of replicas",
			metaNodes: nodes(1, 2, 3, 4, 5),
			replicaNodes: map[roachpb.NodeID][]roachpb.NodeID{
				1: nodes(1, 2, 3, 4, 5),
				2: nodes(2),
			},
			deadNodes: nodeSet(3, 4, 5),
			result:    loqrecoverypb.RangeHealth_WAITING_FOR_META,
		},
		{
			name:      "replica check error",
			metaNodes: nodes(1, 2, 3, 4, 5),
			replicaNodes: map[roachpb.NodeID][]roachpb.NodeID{
				1: nodes(1, 2, 3, 4, 5),
			},
			deadNodes: nodeSet(3, 4, 5),
			result:    loqrecoverypb.RangeHealth_LOSS_OF_QUORUM,
		},
	} {
		t.Run(d.name, func(t *testing.T) {
			isLive := func(rd roachpb.ReplicaDescriptor) bool {
				_, ok := d.deadNodes[rd.NodeID]
				return !ok
			}
			rangeInfo := func(ctx context.Context, rID roachpb.RangeID, nID roachpb.NodeID,
			) (serverpb.RangeInfo, error) {
				ids, ok := d.replicaNodes[nID]
				if ok {
					desc := buildDescForNodes(ids)
					return serverpb.RangeInfo{
						State: kvserverpb.RangeInfo{
							ReplicaState: kvserverpb.ReplicaState{
								Desc: &desc,
							},
						},
					}, nil
				}
				return serverpb.RangeInfo{}, errors.Newf("no replica for r%d on node n%d", rID, nID)
			}
			metaDesc := buildDescForNodes(d.metaNodes)
			require.Equal(t, d.result.String(), checkRangeHealth(ctx, metaDesc,
				isLive, rangeInfo).String(), "incorrect health computed")
		})
	}
}

func nodes(ns ...roachpb.NodeID) []roachpb.NodeID {
	return append([]roachpb.NodeID(nil), ns...)
}

func nodeSet(nodes ...roachpb.NodeID) map[roachpb.NodeID]interface{} {
	r := make(map[roachpb.NodeID]interface{})
	for _, id := range nodes {
		r[id] = struct{}{}
	}
	return r
}

func buildDescForNodes(nodes []roachpb.NodeID) roachpb.RangeDescriptor {
	desc := roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: keys.MustAddr(keys.MetaMin),
		EndKey:   keys.MustAddr(keys.Meta2KeyMax),
	}
	var rs roachpb.ReplicaSet
	for i, id := range nodes {
		rs.AddReplica(roachpb.ReplicaDescriptor{
			NodeID:    id,
			StoreID:   roachpb.StoreID(id),
			ReplicaID: roachpb.ReplicaID(i + 1),
			Type:      roachpb.VOTER_FULL,
		})
	}
	desc.SetReplicas(rs)
	return desc
}

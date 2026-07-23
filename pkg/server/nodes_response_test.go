// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestNodeStatusToResp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var nodeStatus = &statuspb.NodeStatus{
		StoreStatuses: []statuspb.StoreStatus{
			{Desc: roachpb.StoreDescriptor{
				Properties: roachpb.StoreProperties{
					Encrypted: true,
					FileStoreProperties: &roachpb.FileStoreProperties{
						Path:   "/secret",
						FsType: "ext4",
					},
				},
			}},
		},
		Desc: roachpb.NodeDescriptor{
			Address: util.UnresolvedAddr{
				NetworkField: "network",
				AddressField: "address",
			},
			Attrs: roachpb.Attributes{
				Attrs: []string{"attr"},
			},
			LocalityAddress: []roachpb.LocalityAddress{{Address: util.UnresolvedAddr{
				NetworkField: "network",
				AddressField: "address",
			}, LocalityTier: roachpb.Tier{Value: "v", Key: "k"}}},
			SQLAddress: util.UnresolvedAddr{
				NetworkField: "network",
				AddressField: "address",
			},
		},
		Args: []string{"args"},
		Env:  []string{"env"},
	}
	resp := nodeStatusToResp(nodeStatus, false)
	require.Empty(t, resp.Args)
	require.Empty(t, resp.Env)
	require.Empty(t, resp.Desc.Address)
	require.Empty(t, resp.Desc.Attrs.Attrs)
	require.Empty(t, resp.Desc.LocalityAddress)
	require.Empty(t, resp.Desc.SQLAddress)
	require.True(t, resp.StoreStatuses[0].Desc.Properties.Encrypted)
	require.NotEmpty(t, resp.StoreStatuses[0].Desc.Properties.FileStoreProperties.FsType)
	require.Empty(t, resp.StoreStatuses[0].Desc.Properties.FileStoreProperties.Path)

	// Now fetch all the node statuses as admin.
	resp = nodeStatusToResp(nodeStatus, true)
	require.NotEmpty(t, resp.Args)
	require.NotEmpty(t, resp.Env)
	require.NotEmpty(t, resp.Desc.Address)
	require.NotEmpty(t, resp.Desc.Attrs.Attrs)
	require.NotEmpty(t, resp.Desc.LocalityAddress)
	require.NotEmpty(t, resp.Desc.SQLAddress)
	require.True(t, resp.StoreStatuses[0].Desc.Properties.Encrypted)
	require.NotEmpty(t, resp.StoreStatuses[0].Desc.Properties.FileStoreProperties.FsType)
	require.NotEmpty(t, resp.StoreStatuses[0].Desc.Properties.FileStoreProperties.Path)
}

func TestRegionsResponseFromNodesResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	makeNodeResponseWithLocalities := func(tiers [][]roachpb.Tier) *serverpb.NodesResponse {
		ret := &serverpb.NodesResponse{}
		for _, l := range tiers {
			ret.Nodes = append(
				ret.Nodes,
				statuspb.NodeStatus{
					Desc: roachpb.NodeDescriptor{
						Locality: roachpb.Locality{Tiers: l},
					},
				},
			)
		}
		return ret
	}

	makeTiers := func(region, zone string) []roachpb.Tier {
		return []roachpb.Tier{
			{Key: "region", Value: region},
			{Key: "zone", Value: zone},
		}
	}

	testCases := []struct {
		desc     string
		resp     *serverpb.NodesResponse
		expected *serverpb.RegionsResponse
	}{
		{
			desc: "no nodes with regions",
			resp: makeNodeResponseWithLocalities([][]roachpb.Tier{
				{{Key: "a", Value: "a"}},
				{},
			}),
			expected: &serverpb.RegionsResponse{
				Regions: map[string]*serverpb.RegionsResponse_Region{},
			},
		},
		{
			desc: "nodes, some with AZs",
			resp: makeNodeResponseWithLocalities([][]roachpb.Tier{
				makeTiers("us-east1", "us-east1-a"),
				makeTiers("us-east1", "us-east1-a"),
				makeTiers("us-east1", "us-east1-a"),
				makeTiers("us-east1", "us-east1-b"),

				makeTiers("us-east2", "us-east2-a"),
				makeTiers("us-east2", "us-east2-a"),
				makeTiers("us-east2", "us-east2-a"),

				makeTiers("us-east3", "us-east3-a"),
				makeTiers("us-east3", "us-east3-b"),
				makeTiers("us-east3", "us-east3-b"),
				{{Key: "region", Value: "us-east3"}},

				{{Key: "region", Value: "us-east4"}},
			}),
			expected: &serverpb.RegionsResponse{
				Regions: map[string]*serverpb.RegionsResponse_Region{
					"us-east1": {
						Zones: []string{"us-east1-a", "us-east1-b"},
					},
					"us-east2": {
						Zones: []string{"us-east2-a"},
					},
					"us-east3": {
						Zones: []string{"us-east3-a", "us-east3-b"},
					},
					"us-east4": {
						Zones: []string{},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ret := regionsResponseFromNodesResponse(tc.resp)
			require.Equal(t, tc.expected, ret)
		})
	}
}

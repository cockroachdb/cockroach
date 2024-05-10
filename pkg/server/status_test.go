// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestDetailsRedacted checks if the `DetailsResponse` contains redacted fields
// when the `Redact` flag is set in the `DetailsRequest`
func TestDetailsRedacted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	server := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer server.Stopper().Stop(ctx)

	s := server.StatusServer().(*systemStatusServer)
	res, err := s.Details(ctx, &serverpb.DetailsRequest{
		NodeId: "local",
		Redact: true,
	})
	require.NoError(t, err)

	jsonResponse, _ := json.Marshal(res)
	hostname, _ := os.Hostname()

	require.Equal(t, redactedMarker, res.Address.AddressField)
	require.Equal(t, redactedMarker, res.SQLAddress.AddressField)
	require.NotContains(t, string(jsonResponse), hostname)
}

// TestDetailsUnredacted checks if the `DetailsResponse` contains un-redacted fields
// when the `Redact` flag is not set in the `DetailsRequest`
func TestDetailsUnredacted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	server := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer server.Stopper().Stop(ctx)

	s := server.StatusServer().(*systemStatusServer)
	res, err := s.Details(ctx, &serverpb.DetailsRequest{
		NodeId: "local",
	})
	require.NoError(t, err)

	require.NotEqual(t, redactedMarker, res.Address.AddressField)
	require.NotEqual(t, redactedMarker, res.SQLAddress.AddressField)
}

// TestNodesListRedacted checks if the `NodesListResponse` contains redacted fields
// when the `Redact` flag is set in the `NodesListRequest`
func TestNodesListRedacted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	server := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer server.Stopper().Stop(ctx)

	s := server.StatusServer().(*systemStatusServer)
	res, err := s.NodesList(ctx, &serverpb.NodesListRequest{
		Redact: true,
	})
	require.NoError(t, err)

	jsonResponse, _ := json.Marshal(res)
	hostname, _ := os.Hostname()

	require.NotContains(t, string(jsonResponse), hostname)

	for i := range res.Nodes {
		require.Equal(t, redactedMarker, res.Nodes[i].Address.AddressField)
		require.Equal(t, redactedMarker, res.Nodes[i].SQLAddress.AddressField)
	}
}

// TestNodesListUnredacted checks if the `NodesListResponse` contains un-redacted fields
// when the `Redact` flag is not set in the `NodesListRequest`
func TestNodesListUnredacted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	server := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer server.Stopper().Stop(ctx)

	s := server.StatusServer().(*systemStatusServer)
	res, err := s.NodesList(ctx, &serverpb.NodesListRequest{})
	require.NoError(t, err)

	for i := range res.Nodes {
		require.NotEqual(t, redactedMarker, res.Nodes[i].Address.AddressField)
		require.NotEqual(t, redactedMarker, res.Nodes[i].SQLAddress.AddressField)
	}
}

// TestNodesRedacted checks if the `NodesResponse` contains redacted fields
// when the `Redact` flag is set in the `NodesRequest`
func TestNodesRedacted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	server := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer server.Stopper().Stop(ctx)

	s := server.StatusServer().(*systemStatusServer)
	res, err := s.Nodes(ctx, &serverpb.NodesRequest{Redact: true})
	require.NoError(t, err)

	jsonResponse, _ := json.Marshal(res)
	hostname, _ := os.Hostname()

	require.NotContains(t, string(jsonResponse), hostname)

	for i := range res.Nodes {
		require.Equal(t, redactedMarker, res.Nodes[i].Desc.Address.AddressField)
		require.Equal(t, redactedMarker, res.Nodes[i].Desc.SQLAddress.AddressField)
		require.Equal(t, redactedMarker, res.Nodes[i].Desc.HTTPAddress.AddressField)

		for j := range res.Nodes[i].Desc.Locality.Tiers {
			require.Equal(t, redactedMarker, res.Nodes[0].Desc.Locality.Tiers[j].Value)
		}

		for j := range res.Nodes[i].StoreStatuses {
			require.Equal(t, redactedMarker, res.Nodes[i].StoreStatuses[j].Desc.Node.Address.AddressField)
			require.Equal(t, redactedMarker, res.Nodes[i].StoreStatuses[j].Desc.Node.SQLAddress.AddressField)
			require.Equal(t, redactedMarker, res.Nodes[i].StoreStatuses[j].Desc.Node.HTTPAddress.AddressField)

			for k := range res.Nodes[i].StoreStatuses[j].Desc.Node.Locality.Tiers {
				require.Equal(t, redactedMarker, res.Nodes[i].StoreStatuses[j].Desc.Node.Locality.Tiers[k].Value)
			}
		}
	}
}

// TestNodesUnredacted checks if the `NodesResponse` contains un-redacted fields
// when the `Redact` flag is not set in the `NodesRequest`
func TestNodesUnredacted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	server := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer server.Stopper().Stop(ctx)

	s := server.StatusServer().(*systemStatusServer)
	res, err := s.Nodes(ctx, &serverpb.NodesRequest{})
	require.NoError(t, err)

	for i := range res.Nodes {
		require.NotEqual(t, redactedMarker, res.Nodes[i].Desc.Address.AddressField)
		require.NotEqual(t, redactedMarker, res.Nodes[i].Desc.SQLAddress.AddressField)
		require.NotEqual(t, redactedMarker, res.Nodes[i].Desc.HTTPAddress.AddressField)

		for j := range res.Nodes[i].Desc.Locality.Tiers {
			require.NotEqual(t, redactedMarker, res.Nodes[0].Desc.Locality.Tiers[j].Value)
		}

		for j := range res.Nodes[i].StoreStatuses {
			require.NotEqual(t, redactedMarker, res.Nodes[i].StoreStatuses[j].Desc.Node.Address.AddressField)
			require.NotEqual(t, redactedMarker, res.Nodes[i].StoreStatuses[j].Desc.Node.SQLAddress.AddressField)
			require.NotEqual(t, redactedMarker, res.Nodes[i].StoreStatuses[j].Desc.Node.HTTPAddress.AddressField)

			for k := range res.Nodes[i].StoreStatuses[j].Desc.Node.Locality.Tiers {
				require.NotEqual(t, redactedMarker, res.Nodes[i].StoreStatuses[j].Desc.Node.Locality.Tiers[k].Value)
			}
		}
	}
}

func TestRedactNodesResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	server := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer server.Stopper().Stop(ctx)

	resp := serverpb.NodesResponse{
		Nodes: []statuspb.NodeStatus{
			{
				Desc: roachpb.NodeDescriptor{
					Address: util.UnresolvedAddr{
						AddressField: "127.0.0.1:5328",
					},
					SQLAddress: util.UnresolvedAddr{
						AddressField: "127.0.0.1:5328",
					},
					HTTPAddress: util.UnresolvedAddr{
						AddressField: "127.0.0.1:5328",
					},
					Locality: roachpb.Locality{},
				},
				StoreStatuses: []statuspb.StoreStatus{
					{
						Desc: roachpb.StoreDescriptor{
							Node: roachpb.NodeDescriptor{
								Address: util.UnresolvedAddr{
									AddressField: "127.0.0.1:5328",
								},
								SQLAddress: util.UnresolvedAddr{
									AddressField: "127.0.0.1:5328",
								},
								HTTPAddress: util.UnresolvedAddr{
									AddressField: "http://127.0.0.1/abcd",
								},
								Locality: roachpb.Locality{
									Tiers: []roachpb.Tier{
										{
											Key:   "dns",
											Value: "127.0.0.1:5328",
										},
										{
											Key:   "abc",
											Value: "127.0.0.1:5328",
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	s := server.StatusServer().(*systemStatusServer)
	res := s.redactNodesResponse(&resp)

	for i := range res.Nodes {
		require.Equal(t, redactedMarker, res.Nodes[i].Desc.Address.AddressField)
		require.Equal(t, redactedMarker, res.Nodes[i].Desc.SQLAddress.AddressField)
		require.Equal(t, redactedMarker, res.Nodes[i].Desc.HTTPAddress.AddressField)

		for j := range res.Nodes[i].Desc.Locality.Tiers {
			require.Equal(t, redactedMarker, res.Nodes[0].Desc.Locality.Tiers[j].Value)
		}

		for j := range res.Nodes[i].StoreStatuses {
			require.Equal(t, redactedMarker, res.Nodes[i].StoreStatuses[j].Desc.Node.Address.AddressField)
			require.Equal(t, redactedMarker, res.Nodes[i].StoreStatuses[j].Desc.Node.SQLAddress.AddressField)
			require.Equal(t, redactedMarker, res.Nodes[i].StoreStatuses[j].Desc.Node.HTTPAddress.AddressField)

			for k := range res.Nodes[i].StoreStatuses[j].Desc.Node.Locality.Tiers {
				require.Equal(t, redactedMarker, res.Nodes[i].StoreStatuses[j].Desc.Node.Locality.Tiers[k].Value)
			}
		}
	}
}

// TestNodeStatusRedacted checks if the `NodeResponse` contains redacted fields
// when the `Redact` flag is set in the `NodeRequest`
func TestNodeStatusRedacted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	server := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer server.Stopper().Stop(ctx)

	s := server.StatusServer().(*systemStatusServer)
	res, err := s.Node(ctx, &serverpb.NodeRequest{Redact: true})
	require.NoError(t, err)

	jsonResponse, _ := json.Marshal(res)
	hostname, _ := os.Hostname()

	require.NotContains(t, string(jsonResponse), hostname)
	require.Equal(t, redactedMarker, res.Desc.Address.AddressField)
	require.Equal(t, redactedMarker, res.Desc.SQLAddress.AddressField)
}

// TestNodeStatusUnredacted checks if the `NodeResponse` contains un-redacted fields
// when the `Redact` flag is not set in the `NodeRequest`
func TestNodeStatusUnredacted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	server := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer server.Stopper().Stop(ctx)

	s := server.StatusServer().(*systemStatusServer)
	res, err := s.Node(ctx, &serverpb.NodeRequest{})
	require.NoError(t, err)

	require.NotEqual(t, redactedMarker, res.Desc.Address.AddressField)
	require.NotEqual(t, redactedMarker, res.Desc.SQLAddress.AddressField)
}

// TestRangesRedacted checks if the `RangesResponse` contains redacted fields
// when the `Redact` flag is set in the `RangesRequest`
func TestRangesRedacted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	server := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer server.Stopper().Stop(ctx)

	s := server.StatusServer().(*systemStatusServer)
	res, err := s.Ranges(ctx, &serverpb.RangesRequest{Redact: true})
	require.NoError(t, err)

	jsonResponse, _ := json.Marshal(res)
	hostname, _ := os.Hostname()

	require.NotContains(t, string(jsonResponse), hostname)

	for i := range res.Ranges {
		for j := range res.Ranges[i].Locality.Tiers {
			if res.Ranges[i].Locality.Tiers[j].Key == "dns" {
				require.Equal(t, redactedMarker, res.Ranges[i].Locality.Tiers[j].Value)
			}
		}
	}
}

// TestRangesUnredacted checks if the `RangesResponse` contains un-redacted fields
// when the `Redact` flag is not set in the `RangesRequest`
func TestRangesUnredacted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	server := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer server.Stopper().Stop(ctx)

	s := server.StatusServer().(*systemStatusServer)
	res, err := s.Ranges(ctx, &serverpb.RangesRequest{})
	require.NoError(t, err)

	for i := range res.Ranges {
		for j := range res.Ranges[i].Locality.Tiers {
			if res.Ranges[i].Locality.Tiers[j].Key == "dns" {
				require.NotEqual(t, redactedMarker, res.Ranges[i].Locality.Tiers[j].Value)
			}
		}
	}
}

// TestGossipRedacted checks if the `GossipResponse` contains redacted fields
// when the `Redact` flag is set in the `GossipRequest`
func TestGossipRedacted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	server := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer server.Stopper().Stop(ctx)

	s := server.StatusServer().(*systemStatusServer)
	res, err := s.Gossip(ctx, &serverpb.GossipRequest{
		Redact: true,
	})
	require.NoError(t, err)

	jsonResponse, _ := json.Marshal(res)
	hostname, _ := os.Hostname()

	require.NotContains(t, string(jsonResponse), hostname)

	for i := range res.Server.ConnStatus {
		require.Equal(t, redactedMarker, res.Server.ConnStatus[i].Address)
	}
}

// TestGossipUnredacted checks if the `GossipResponse` contains un-redacted fields
// when the `Redact` flag is not set in the `GossipRequest`
func TestGossipUnredacted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	server := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer server.Stopper().Stop(ctx)

	s := server.StatusServer().(*systemStatusServer)
	res, err := s.Gossip(ctx, &serverpb.GossipRequest{})
	require.NoError(t, err)

	for i := range res.Server.ConnStatus {
		require.NotEqual(t, redactedMarker, res.Server.ConnStatus[i].Address)
	}
}

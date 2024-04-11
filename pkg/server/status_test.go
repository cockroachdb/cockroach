package server

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
	"testing"
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
	res, _ := s.Details(ctx, &serverpb.DetailsRequest{
		NodeId: "local",
		Redact: true,
	})

	require.Equal(t, "<redacted>", res.Address.AddressField)
	require.Equal(t, "<redacted>", res.SQLAddress.AddressField)
}

// TestDetailsUnRedacted checks if the `DetailsResponse` contains un-redacted fields
// when the `Redact` flag is not set in the `DetailsRequest`
func TestDetailsUnRedacted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	server := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer server.Stopper().Stop(ctx)

	s := server.StatusServer().(*systemStatusServer)
	res, _ := s.Details(ctx, &serverpb.DetailsRequest{
		NodeId: "local",
	})

	require.NotEqual(t, "<redacted>", res.Address.AddressField)
	require.NotEqual(t, "<redacted>", res.SQLAddress.AddressField)
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
	res, _ := s.NodesList(ctx, &serverpb.NodesListRequest{
		Redact: true,
	})

	for i, _ := range res.Nodes {
		require.Equal(t, "<redacted>", res.Nodes[i].Address.AddressField)
		require.Equal(t, "<redacted>", res.Nodes[i].SQLAddress.AddressField)
	}
}

// TestNodesListUnRedacted checks if the `NodesListResponse` contains un-redacted fields
// when the `Redact` flag is not set in the `NodesListRequest`
func TestNodesListUnRedacted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	server := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer server.Stopper().Stop(ctx)

	s := server.StatusServer().(*systemStatusServer)
	res, _ := s.NodesList(ctx, &serverpb.NodesListRequest{})

	for i, _ := range res.Nodes {
		require.NotEqual(t, "<redacted>", res.Nodes[i].Address.AddressField)
		require.NotEqual(t, "<redacted>", res.Nodes[i].SQLAddress.AddressField)
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
	res, _ := s.Nodes(ctx, &serverpb.NodesRequest{Redact: true})

	for i := range res.Nodes {
		require.Equal(t, "<redacted>", res.Nodes[i].Desc.Address.AddressField)
		require.Equal(t, "<redacted>", res.Nodes[i].Desc.SQLAddress.AddressField)
		require.Equal(t, "<redacted>", res.Nodes[i].Desc.HTTPAddress.AddressField)

		for j := range res.Nodes[i].Desc.Locality.Tiers {
			if res.Nodes[i].Desc.Locality.Tiers[j].Key == "dns" {
				require.Equal(t, "<redacted>", res.Nodes[0].Desc.Locality.Tiers[j].Value)
			}
		}

		for j := range res.Nodes[i].StoreStatuses {
			require.Equal(t, "<redacted>", res.Nodes[i].StoreStatuses[j].Desc.Node.Address.AddressField)
			require.Equal(t, "<redacted>", res.Nodes[i].StoreStatuses[j].Desc.Node.SQLAddress.AddressField)
			require.Equal(t, "<redacted>", res.Nodes[i].StoreStatuses[j].Desc.Node.HTTPAddress.AddressField)

			for k := range res.Nodes[i].StoreStatuses[j].Desc.Node.Locality.Tiers {
				if res.Nodes[i].StoreStatuses[j].Desc.Node.Locality.Tiers[k].Key == "dns" {
					require.Equal(t, "<redacted>", res.Nodes[i].StoreStatuses[j].Desc.Node.Locality.Tiers[k].Value)
				}
			}
		}
	}
}

// TestNodesUnRedacted checks if the `NodesResponse` contains un-redacted fields
// when the `Redact` flag is not set in the `NodesRequest`
func TestNodesUnRedacted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	server := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer server.Stopper().Stop(ctx)

	s := server.StatusServer().(*systemStatusServer)
	res, _ := s.Nodes(ctx, &serverpb.NodesRequest{})

	for i := range res.Nodes {
		require.NotEqual(t, "<redacted>", res.Nodes[i].Desc.Address.AddressField)
		require.NotEqual(t, "<redacted>", res.Nodes[i].Desc.SQLAddress.AddressField)
		require.NotEqual(t, "<redacted>", res.Nodes[i].Desc.HTTPAddress.AddressField)

		for j := range res.Nodes[i].Desc.Locality.Tiers {
			if res.Nodes[i].Desc.Locality.Tiers[j].Key == "dns" {
				require.NotEqual(t, "<redacted>", res.Nodes[0].Desc.Locality.Tiers[j].Value)
			}
		}

		for j := range res.Nodes[i].StoreStatuses {
			require.NotEqual(t, "<redacted>", res.Nodes[i].StoreStatuses[j].Desc.Node.Address.AddressField)
			require.NotEqual(t, "<redacted>", res.Nodes[i].StoreStatuses[j].Desc.Node.SQLAddress.AddressField)
			require.NotEqual(t, "<redacted>", res.Nodes[i].StoreStatuses[j].Desc.Node.HTTPAddress.AddressField)

			for k := range res.Nodes[i].StoreStatuses[j].Desc.Node.Locality.Tiers {
				if res.Nodes[i].StoreStatuses[j].Desc.Node.Locality.Tiers[k].Key == "dns" {
					require.NotEqual(t, "<redacted>", res.Nodes[i].StoreStatuses[j].Desc.Node.Locality.Tiers[k].Value)
				}
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
	res, _ := s.Node(ctx, &serverpb.NodeRequest{Redact: true})

	require.Equal(t, "<redacted>", res.Desc.Address.AddressField)
	require.Equal(t, "<redacted>", res.Desc.SQLAddress.AddressField)
}

// TestNodeStatusUnRedacted checks if the `NodeResponse` contains un-redacted fields
// when the `Redact` flag is not set in the `NodeRequest`
func TestNodeStatusUnRedacted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	server := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer server.Stopper().Stop(ctx)

	s := server.StatusServer().(*systemStatusServer)
	res, _ := s.Node(ctx, &serverpb.NodeRequest{})

	require.NotEqual(t, "<redacted>", res.Desc.Address.AddressField)
	require.NotEqual(t, "<redacted>", res.Desc.SQLAddress.AddressField)
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
	res, _ := s.Ranges(ctx, &serverpb.RangesRequest{Redact: true})

	for i := range res.Ranges {
		for j := range res.Ranges[i].Locality.Tiers {
			if res.Ranges[i].Locality.Tiers[j].Key == "dns" {
				require.Equal(t, "<redacted>", res.Ranges[i].Locality.Tiers[j].Value)
			}
		}
	}
}

// TestRangesUnRedacted checks if the `RangesResponse` contains un-redacted fields
// when the `Redact` flag is not set in the `RangesRequest`
func TestRangesUnRedacted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	server := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer server.Stopper().Stop(ctx)

	s := server.StatusServer().(*systemStatusServer)
	res, _ := s.Ranges(ctx, &serverpb.RangesRequest{})

	for i := range res.Ranges {
		for j := range res.Ranges[i].Locality.Tiers {
			if res.Ranges[i].Locality.Tiers[j].Key == "dns" {
				require.NotEqual(t, "<redacted>", res.Ranges[i].Locality.Tiers[j].Value)
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
	res, _ := s.Gossip(ctx, &serverpb.GossipRequest{
		Redact: true,
	})

	for i := range res.Server.ConnStatus {
		require.Equal(t, "<redacted>", res.Server.ConnStatus[i].Address)
	}
}

// TestGossipUnRedacted checks if the `GossipResponse` contains un-redacted fields
// when the `Redact` flag is not set in the `GossipRequest`
func TestGossipUnRedacted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	server := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer server.Stopper().Stop(ctx)

	s := server.StatusServer().(*systemStatusServer)
	res, _ := s.Gossip(ctx, &serverpb.GossipRequest{})

	for i := range res.Server.ConnStatus {
		require.NotEqual(t, "<redacted>", res.Server.ConnStatus[i].Address)
	}
}

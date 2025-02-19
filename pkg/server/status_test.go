// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/insights"
	tablemetadatacacheutil "github.com/cockroachdb/cockroach/pkg/sql/tablemetadatacache/util"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
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

	// Override cluster setting for this test
	DebugZipRedactAddressesEnabled.Override(ctx, &server.ClusterSettings().SV, true)

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

	// Override cluster setting for this test
	DebugZipRedactAddressesEnabled.Override(ctx, &server.ClusterSettings().SV, true)

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

	// Override cluster setting for this test
	DebugZipRedactAddressesEnabled.Override(ctx, &server.ClusterSettings().SV, true)

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

	// Override cluster setting for this test
	DebugZipRedactAddressesEnabled.Override(ctx, &server.ClusterSettings().SV, true)

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

	// Override cluster setting for this test
	DebugZipRedactAddressesEnabled.Override(ctx, &server.ClusterSettings().SV, true)

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

	// Override cluster setting for this test
	DebugZipRedactAddressesEnabled.Override(ctx, &server.ClusterSettings().SV, true)

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

// TestListExecutionInsightsWhileEvictingInsights is a regression test
// for #130290. It verifies that the status server does not panic when
// listing execution insights while the system is evicting insights.
func TestListExecutionInsightsWhileEvictingInsights(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	server := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer server.Stopper().Stop(ctx)

	s := server.StatusServer().(*systemStatusServer)
	insights.ExecutionInsightsCapacity.Override(ctx, &server.ClusterSettings().SV, 5)
	insights.LatencyThreshold.Override(ctx, &server.ClusterSettings().SV, 1*time.Millisecond)
	var wg sync.WaitGroup
	wg.Add(3)

	conn := sqlutils.MakeSQLRunner(server.ApplicationLayer().SQLConn(t))
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			conn.Exec(t, "SELECT * FROM system.users")
		}
	}()

	conn2 := sqlutils.MakeSQLRunner(server.ApplicationLayer().SQLConn(t))
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			conn2.Exec(t, "SELECT * FROM system.users")
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			require.NotPanics(t, func() {
				_, err := s.ListExecutionInsights(ctx, &serverpb.ListExecutionInsightsRequest{})
				require.NoError(t, err)
			})
		}
	}()

	wg.Wait()
}

// TestStatusUpdateTableMetadataCache tests that signalling the update
// table metadata cache job via the status server triggers the update
// table metadata job to run.
func TestStatusUpdateTableMetadataCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var zeroDuration time.Duration
	jobCompleteCh := make(chan struct{})
	ctx := context.Background()
	tc := serverutils.StartCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: &jobs.TestingKnobs{
					IntervalOverrides: jobs.TestingIntervalOverrides{
						Adopt: &zeroDuration,
					}},
				TableMetadata: &tablemetadatacacheutil.TestingKnobs{
					TableMetadataUpdater: &tablemetadatacacheutil.NoopUpdater{},
					OnJobComplete: func() {
						jobCompleteCh <- struct{}{}
					},
				},
			},
		},
	})
	defer tc.Stopper().Stop(context.Background())

	conn := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	t.Run("gated on admin privilege", func(t *testing.T) {
		authCtx := authserver.ForwardHTTPAuthInfoToRPCCalls(authserver.ContextWithHTTPAuthInfo(ctx, username.TestUser, 1), nil)
		_, err := tc.Server(0).GetStatusClient(t).UpdateTableMetadataCache(authCtx,
			&serverpb.UpdateTableMetadataCacheRequest{Local: false})
		require.Truef(t, testutils.IsError(err, updateTableMetadataCachePermissionErrMsg), "received error: %v", err)
	})

	t.Run("triggers update table metadata cache job", func(t *testing.T) {
		// Get the node id that claimed the update job. We'll issue the
		// RPC to a node that doesn't own the job to test that the RPC can
		// propagate the request to the correct node.
		var nodeID int
		testutils.SucceedsSoon(t, func() error {
			row := conn.Query(t, `
SELECT claim_instance_id FROM system.jobs 
WHERE id = $1 AND claim_instance_id IS NOT NULL`, jobs.UpdateTableMetadataCacheJobID)
			if !row.Next() {
				return errors.New("no node has claimed the job")
			}
			require.NoError(t, row.Scan(&nodeID))

			rpcGatewayNode := (nodeID + 1) % 3
			_, err := tc.Server(rpcGatewayNode).GetStatusClient(t).UpdateTableMetadataCache(ctx,
				&serverpb.UpdateTableMetadataCacheRequest{Local: false})
			if err != nil {
				return err
			}
			// The job shouldn't be busy.
			return nil
		})

		// Wait for the job to complete.
		t.Log("waiting for job to complete")
		<-jobCompleteCh
		t.Log("job completed")

		row := conn.Query(t,
			`SELECT running_status FROM crdb_internal.jobs WHERE job_id = $1 AND running_status IS NOT NULL`,
			jobs.UpdateTableMetadataCacheJobID)
		if !row.Next() {
			t.Fatal("last_run_time not updated")
		}
		var status string
		require.NoError(t, row.Scan(&status))
		require.Containsf(t, status, "Job completed at", "status not updated: %s", status)
	})
}

// TestNodesUiMetrics tests that the metrics fields of NodesUI
// rpcs only returns the subset of metrics needed in the UI
func TestNodesUiMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ts := serverutils.StartServerOnly(t, base.TestServerArgs{})

	ctx := context.Background()
	defer ts.Stopper().Stop(ctx)

	s := ts.StatusServer().(*systemStatusServer)
	resp, err := s.NodesUI(ctx, &serverpb.NodesRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Nodes, 1)
	for _, node := range resp.Nodes {
		for _, m := range uiNodeMetrics {
			require.Contains(t, node.Metrics, m)
		}
		require.Greater(t, len(node.StoreStatuses), 0)
		for _, storeStatus := range node.StoreStatuses {
			for _, m := range uiStoreMetrics {
				require.Contains(t, storeStatus.Metrics, m)
			}
		}
	}
}

// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package statusccl

import (
	"context"
	"io"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestTenantGRPCServices tests that the gRPC servers that are externally
// facing have been started up on the tenant server. This includes gRPC that is
// used for pod-to-pod communication as well as the HTTP services powered by
// gRPC Gateway that are used to serve endpoints to power observability UIs.
func TestTenantGRPCServices(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderStress(t, "test can time out under stress")

	ctx := context.Background()

	testCluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
		},
	})
	defer testCluster.Stopper().Stop(ctx)

	server := testCluster.Server(0)

	tenantID := serverutils.TestTenantID()
	testingKnobs := base.TestingKnobs{
		SQLStatsKnobs: sqlstats.CreateTestingKnobs(),
	}
	tenant, connTenant := serverutils.StartTenant(t, server, base.TestTenantArgs{
		TenantID:     tenantID,
		TestingKnobs: testingKnobs,
	})
	defer connTenant.Close()

	t.Logf("subtests starting")

	t.Run("gRPC is running", func(t *testing.T) {
		client := tenant.GetStatusClient(t)
		resp, err := client.Statements(ctx, &serverpb.StatementsRequest{NodeID: "local"})
		require.NoError(t, err)
		require.NotEmpty(t, resp.Statements)
	})

	httpClient, err := tenant.GetAdminHTTPClient()
	require.NoError(t, err)
	defer httpClient.CloseIdleConnections()

	t.Run("gRPC Gateway is running", func(t *testing.T) {
		resp, err := httpClient.Get(tenant.AdminURL().WithPath("/_status/statements").String())
		require.NoError(t, err)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Contains(t, string(body), "transactions")
	})

	sqlRunner := sqlutils.MakeSQLRunner(connTenant)
	sqlRunner.Exec(t, "CREATE TABLE test (id int)")
	sqlRunner.Exec(t, "INSERT INTO test VALUES (1)")

	tenant2, connTenant2 := serverutils.StartTenant(t, server, base.TestTenantArgs{
		TenantID:     tenantID,
		TestingKnobs: testingKnobs,
	})
	defer connTenant2.Close()

	t.Run("statements endpoint fans out request to multiple pods", func(t *testing.T) {
		resp, err := httpClient.Get(tenant2.AdminURL().WithPath("/_status/statements").String())
		require.NoError(t, err)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Contains(t, string(body), "CREATE TABLE test")
		require.Contains(t, string(body), "INSERT INTO test VALUES")
	})

	tenant3, connTenant3 := serverutils.StartTenant(t, server, base.TestTenantArgs{
		TenantID:     roachpb.MustMakeTenantID(11),
		TestingKnobs: testingKnobs,
	})
	defer connTenant3.Close()

	t.Run("fanout of statements endpoint is segregated by tenant", func(t *testing.T) {
		httpClient3, err := tenant3.GetAdminHTTPClient()
		require.NoError(t, err)
		defer httpClient3.CloseIdleConnections()

		resp, err := httpClient3.Get(tenant3.AdminURL().WithPath("/_status/statements").String())
		require.NoError(t, err)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NotContains(t, string(body), "CREATE TABLE test")
		require.NotContains(t, string(body), "INSERT INTO test VALUES")
	})

	t.Run("fanout of statements endpoint between tenants", func(t *testing.T) {
		grpcAddr := tenant.RPCAddr()
		rpcCtx := tenant2.RPCContext()

		nodeID := roachpb.NodeID(tenant.SQLInstanceID())
		conn, err := rpcCtx.GRPCDialNode(grpcAddr, nodeID, roachpb.Locality{}, rpc.DefaultClass).Connect(ctx)
		require.NoError(t, err)

		client := serverpb.NewStatusClient(conn)

		resp, err := client.Statements(ctx, &serverpb.StatementsRequest{NodeID: "local"})
		require.NoError(t, err)
		require.NotEmpty(t, resp.Statements)
	})

	t.Run("tenant request to KV Node Statements fails", func(t *testing.T) {
		grpcAddr := server.RPCAddr()
		rpcCtx := tenant.RPCContext()

		conn, err := rpcCtx.GRPCDialNode(grpcAddr, server.NodeID(), roachpb.Locality{}, rpc.DefaultClass).Connect(ctx)
		require.NoError(t, err)

		client := serverpb.NewStatusClient(conn)

		_, err = client.Statements(ctx, &serverpb.StatementsRequest{NodeID: "local"})
		require.Errorf(t, err, "statements endpoint should not be accessed on KV node by tenant")
	})

	t.Run("sessions endpoint is available", func(t *testing.T) {
		resp, err := httpClient.Get(tenant.AdminURL().WithPath("/_status/sessions").String())
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, 200, resp.StatusCode)
	})
}

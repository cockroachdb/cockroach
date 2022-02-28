// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package statusccl

import (
	"context"
	"io/ioutil"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
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

	ctx := context.Background()

	serverParams, _ := tests.CreateTestServerParams()
	testCluster := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: serverParams,
	})
	defer testCluster.Stopper().Stop(ctx)

	server := testCluster.Server(0)

	tenantID := serverutils.TestTenantID()
	testingKnobs := base.TestingKnobs{
		SQLStatsKnobs: &sqlstats.TestingKnobs{
			AOSTClause: "AS OF SYSTEM TIME '-1us'",
		},
	}
	tenant, connTenant := serverutils.StartTenant(t, server, base.TestTenantArgs{
		TenantID:     tenantID,
		TestingKnobs: testingKnobs,
	})
	defer connTenant.Close()

	t.Logf("subtests starting")

	t.Run("gRPC is running", func(t *testing.T) {
		grpcAddr := tenant.SQLAddr()
		rpcCtx := tenant.RPCContext()

		nodeID := roachpb.NodeID(tenant.SQLInstanceID())
		conn, err := rpcCtx.GRPCDialNode(grpcAddr, nodeID, rpc.DefaultClass).Connect(ctx)
		require.NoError(t, err)

		client := serverpb.NewStatusClient(conn)

		resp, err := client.Statements(ctx, &serverpb.StatementsRequest{NodeID: "local"})
		require.NoError(t, err)
		require.NotEmpty(t, resp.Statements)
	})

	httpClient, err := tenant.GetAdminAuthenticatedHTTPClient()
	require.NoError(t, err)
	defer httpClient.CloseIdleConnections()

	t.Run("gRPC Gateway is running", func(t *testing.T) {
		resp, err := httpClient.Get(tenant.AdminURL() + "/_status/statements")
		require.NoError(t, err)
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Contains(t, string(body), "transactions")
	})

	sqlRunner := sqlutils.MakeSQLRunner(connTenant)
	sqlRunner.Exec(t, "CREATE TABLE test (id int)")
	sqlRunner.Exec(t, "INSERT INTO test VALUES (1)")

	tenant2, connTenant2 := serverutils.StartTenant(t, server, base.TestTenantArgs{
		TenantID:     tenantID,
		Existing:     true,
		TestingKnobs: testingKnobs,
	})
	defer connTenant2.Close()

	t.Run("statements endpoint fans out request to multiple pods", func(t *testing.T) {
		resp, err := httpClient.Get(tenant2.AdminURL() + "/_status/statements")
		require.NoError(t, err)
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Contains(t, string(body), "CREATE TABLE test")
		require.Contains(t, string(body), "INSERT INTO test VALUES")
	})

	tenant3, connTenant3 := serverutils.StartTenant(t, server, base.TestTenantArgs{
		TenantID:     roachpb.MakeTenantID(11),
		TestingKnobs: testingKnobs,
	})
	defer connTenant3.Close()

	t.Run("fanout of statements endpoint is segregated by tenant", func(t *testing.T) {
		httpClient3, err := tenant3.GetAdminAuthenticatedHTTPClient()
		require.NoError(t, err)
		defer httpClient3.CloseIdleConnections()

		resp, err := httpClient3.Get(tenant3.AdminURL() + "/_status/statements")
		require.NoError(t, err)
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NotContains(t, string(body), "CREATE TABLE test")
		require.NotContains(t, string(body), "INSERT INTO test VALUES")
	})

	t.Run("fanout of statements endpoint between tenants", func(t *testing.T) {
		grpcAddr := tenant.SQLAddr()
		rpcCtx := tenant2.RPCContext()

		nodeID := roachpb.NodeID(tenant.SQLInstanceID())
		conn, err := rpcCtx.GRPCDialNode(grpcAddr, nodeID, rpc.DefaultClass).Connect(ctx)
		require.NoError(t, err)

		client := serverpb.NewStatusClient(conn)

		resp, err := client.Statements(ctx, &serverpb.StatementsRequest{NodeID: "local"})
		require.NoError(t, err)
		require.NotEmpty(t, resp.Statements)
	})

	t.Run("tenant request to KV Node Statements fails", func(t *testing.T) {
		grpcAddr := server.RPCAddr()
		rpcCtx := tenant.RPCContext()

		conn, err := rpcCtx.GRPCDialNode(grpcAddr, server.NodeID(), rpc.DefaultClass).Connect(ctx)
		require.NoError(t, err)

		client := serverpb.NewStatusClient(conn)

		_, err = client.Statements(ctx, &serverpb.StatementsRequest{NodeID: "local"})
		require.Errorf(t, err, "statements endpoint should not be accessed on KV node by tenant")
	})

	t.Run("sessions endpoint is available", func(t *testing.T) {
		resp, err := httpClient.Get(tenant.AdminURL() + "/_status/sessions")
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, 200, resp.StatusCode)
	})
}

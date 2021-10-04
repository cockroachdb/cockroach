// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package serverccl

import (
	"context"
	gosql "database/sql"
	"net/http"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

type testTenant struct {
	tenant         serverutils.TestTenantInterface
	tenantConn     *gosql.DB
	tenantDB       *sqlutils.SQLRunner
	tenantStatus   serverpb.SQLStatusServer
	tenantSQLStats *persistedsqlstats.PersistedSQLStats
}

func newTestTenant(
	t *testing.T,
	server serverutils.TestServerInterface,
	existing bool,
	tenantID roachpb.TenantID,
	knobs base.TestingKnobs,
) *testTenant {
	t.Helper()

	tenantParams := tests.CreateTestTenantParams(tenantID)
	tenantParams.Existing = existing
	tenantParams.TestingKnobs = knobs

	log.TestingClearServerIdentifiers()
	tenant, tenantConn := serverutils.StartTenant(t, server, tenantParams)
	sqlDB := sqlutils.MakeSQLRunner(tenantConn)
	status := tenant.StatusServer().(serverpb.SQLStatusServer)
	sqlStats := tenant.PGServer().(*pgwire.Server).SQLServer.
		GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats)

	return &testTenant{
		tenant:         tenant,
		tenantConn:     tenantConn,
		tenantDB:       sqlDB,
		tenantStatus:   status,
		tenantSQLStats: sqlStats,
	}
}

func (h *testTenant) cleanup(t *testing.T) {
	require.NoError(t, h.tenantConn.Close())
}

type tenantTestHelper struct {
	hostCluster serverutils.TestClusterInterface

	// Creating two separate tenant clusters. This allows unit tests to test
	// the isolation between different tenants are properly enforced.
	tenantTestCluster    tenantCluster
	tenantControlCluster tenantCluster
}

func newTestTenantHelper(
	t *testing.T, tenantClusterSize int, knobs base.TestingKnobs,
) *tenantTestHelper {
	t.Helper()

	params, _ := tests.CreateTestServerParams()
	testCluster := serverutils.StartNewTestCluster(t, 3 /* numNodes */, base.TestClusterArgs{
		ServerArgs: params,
	})
	server := testCluster.Server(0)

	return &tenantTestHelper{
		hostCluster: testCluster,
		tenantTestCluster: newTenantCluster(
			t,
			server,
			tenantClusterSize,
			security.EmbeddedTenantIDs()[0],
			knobs,
		),
		tenantControlCluster: newTenantCluster(
			t,
			server,
			tenantClusterSize,
			security.EmbeddedTenantIDs()[1],
			knobs,
		),
	}
}

func (h *tenantTestHelper) testCluster() tenantCluster {
	return h.tenantTestCluster
}

func (h *tenantTestHelper) controlCluster() tenantCluster {
	return h.tenantControlCluster
}

func (h *tenantTestHelper) cleanup(ctx context.Context, t *testing.T) {
	t.Helper()
	h.hostCluster.Stopper().Stop(ctx)
	h.tenantTestCluster.cleanup(t)
	h.tenantControlCluster.cleanup(t)
}

type tenantCluster []*testTenant

func newTenantCluster(
	t *testing.T,
	server serverutils.TestServerInterface,
	tenantClusterSize int,
	tenantID uint64,
	knobs base.TestingKnobs,
) tenantCluster {
	t.Helper()

	cluster := make([]*testTenant, tenantClusterSize)
	existing := false
	for i := 0; i < tenantClusterSize; i++ {
		cluster[i] =
			newTestTenant(t, server, existing, roachpb.MakeTenantID(tenantID), knobs)
		existing = true
	}

	return cluster
}

func (c tenantCluster) tenantConn(idx int) *sqlutils.SQLRunner {
	return c[idx].tenantDB
}

func (c tenantCluster) tenantHTTPJSONClient(idx int) (*httpJSONClient, error) {
	client, err := c[idx].tenant.RPCContext().GetHTTPClient()
	if err != nil {
		return nil, err
	}
	return &httpJSONClient{client: client, baseURL: "https://" + c[idx].tenant.HTTPAddr()}, nil
}

func (c tenantCluster) tenantSQLStats(idx int) *persistedsqlstats.PersistedSQLStats {
	return c[idx].tenantSQLStats
}

func (c tenantCluster) tenantStatusSrv(idx int) serverpb.SQLStatusServer {
	return c[idx].tenantStatus
}

func (c tenantCluster) cleanup(t *testing.T) {
	for _, tenant := range c {
		tenant.cleanup(t)
	}
}

type httpJSONClient struct {
	client  http.Client
	baseURL string
}

func (c *httpJSONClient) GetJSON(path string, response protoutil.Message) error {
	return httputil.GetJSON(c.client, c.baseURL+path, response)
}

func (c *httpJSONClient) PostJSON(
	path string, request protoutil.Message, response protoutil.Message,
) error {
	return httputil.PostJSON(c.client, c.baseURL+path, request, response)
}

func (c *httpJSONClient) Close() {
	c.client.CloseIdleConnections()
}

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
	gosql "database/sql"
	"math/rand"
	"net/http"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/contention"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

// serverIdx is the index of the node within a test cluster. A special value
// `randomServer` can be used to let the test helper to randomly choose to
// a server from the test cluster.
type serverIdx int

const randomServer serverIdx = -1

type testTenant struct {
	tenant                   serverutils.TestTenantInterface
	tenantConn               *gosql.DB
	tenantDB                 *sqlutils.SQLRunner
	tenantStatus             serverpb.SQLStatusServer
	tenantSQLStats           *persistedsqlstats.PersistedSQLStats
	tenantContentionRegistry *contention.Registry
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

	tenant, tenantConn := serverutils.StartTenant(t, server, tenantParams)
	sqlDB := sqlutils.MakeSQLRunner(tenantConn)
	status := tenant.StatusServer().(serverpb.SQLStatusServer)
	sqlStats := tenant.PGServer().(*pgwire.Server).SQLServer.
		GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats)
	contentionRegistry := tenant.ExecutorConfig().(sql.ExecutorConfig).ContentionRegistry

	return &testTenant{
		tenant:                   tenant,
		tenantConn:               tenantConn,
		tenantDB:                 sqlDB,
		tenantStatus:             status,
		tenantSQLStats:           sqlStats,
		tenantContentionRegistry: contentionRegistry,
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
	testCluster := serverutils.StartNewTestCluster(t, 1 /* numNodes */, base.TestClusterArgs{
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
		// Spin up a small tenant cluster under a different tenant ID to test
		// tenant isolation.
		tenantControlCluster: newTenantCluster(
			t,
			server,
			1, /* tenantClusterSize */
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

func (c tenantCluster) tenantConn(idx serverIdx) *sqlutils.SQLRunner {
	return c.tenant(idx).tenantDB
}

func (c tenantCluster) tenantHTTPClient(t *testing.T, idx serverIdx) *httpClient {
	client, err := c.tenant(idx).tenant.GetAdminAuthenticatedHTTPClient()
	require.NoError(t, err)
	return &httpClient{t: t, client: client, baseURL: c[idx].tenant.AdminURL()}
}

func (c tenantCluster) tenantSQLStats(idx serverIdx) *persistedsqlstats.PersistedSQLStats {
	return c.tenant(idx).tenantSQLStats
}

func (c tenantCluster) tenantStatusSrv(idx serverIdx) serverpb.SQLStatusServer {
	return c.tenant(idx).tenantStatus
}

func (c tenantCluster) tenantContentionRegistry(idx serverIdx) *contention.Registry {
	return c.tenant(idx).tenantContentionRegistry
}

func (c tenantCluster) cleanup(t *testing.T) {
	for _, tenant := range c {
		tenant.cleanup(t)
	}
}

// tenant selects a tenant node from the tenant cluster. If randomServer
// is passed in, then a random node is selected.
func (c tenantCluster) tenant(idx serverIdx) *testTenant {
	if idx == randomServer {
		return c[rand.Intn(len(c))]
	}

	return c[idx]
}

type httpClient struct {
	t       *testing.T
	client  http.Client
	baseURL string
}

func (c *httpClient) GetJSON(path string, response protoutil.Message) {
	err := httputil.GetJSON(c.client, c.baseURL+path, response)
	require.NoError(c.t, err)
}

func (c *httpClient) PostJSON(path string, request protoutil.Message, response protoutil.Message) {
	err := httputil.PostJSON(c.client, c.baseURL+path, request, response)
	require.NoError(c.t, err)
}

func (c *httpClient) Close() {
	c.client.CloseIdleConnections()
}

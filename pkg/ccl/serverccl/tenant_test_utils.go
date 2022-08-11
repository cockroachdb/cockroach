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

const RandomServer serverIdx = -1

type testTenant struct {
	Tenant                   serverutils.TestTenantInterface
	TenantConn               *gosql.DB
	tenantDB                 *sqlutils.SQLRunner
	tenantStatus             serverpb.SQLStatusServer
	tenantSQLStats           *persistedsqlstats.PersistedSQLStats
	tenantContentionRegistry *contention.Registry
}

func newTestTenant(
	t *testing.T,
	server serverutils.TestServerInterface,
	tenantID roachpb.TenantID,
	knobs base.TestingKnobs,
) *testTenant {
	t.Helper()

	tenantParams := tests.CreateTestTenantParams(tenantID)
	tenantParams.TestingKnobs = knobs

	tenant, tenantConn := serverutils.StartTenant(t, server, tenantParams)
	sqlDB := sqlutils.MakeSQLRunner(tenantConn)
	status := tenant.StatusServer().(serverpb.SQLStatusServer)
	sqlStats := tenant.PGServer().(*pgwire.Server).SQLServer.
		GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats)
	contentionRegistry := tenant.ExecutorConfig().(sql.ExecutorConfig).ContentionRegistry

	return &testTenant{
		Tenant:                   tenant,
		TenantConn:               tenantConn,
		tenantDB:                 sqlDB,
		tenantStatus:             status,
		tenantSQLStats:           sqlStats,
		tenantContentionRegistry: contentionRegistry,
	}
}

func (h *testTenant) cleanup(t *testing.T) {
	require.NoError(t, h.TenantConn.Close())
}

type TenantTestHelper struct {
	HostCluster serverutils.TestClusterInterface

	// Creating two separate tenant clusters. This allows unit tests to test
	// the isolation between different tenants are properly enforced.
	tenantTestCluster    TenantCluster
	tenantControlCluster TenantCluster
}

func NewTestTenantHelper(
	t *testing.T, tenantClusterSize int, knobs base.TestingKnobs,
) *TenantTestHelper {
	t.Helper()

	params, _ := tests.CreateTestServerParams()
	params.Knobs = knobs
	// We're running tenant tests, no need for a default tenant.
	params.DisableDefaultTestTenant = true
	testCluster := serverutils.StartNewTestCluster(t, 1 /* numNodes */, base.TestClusterArgs{
		ServerArgs: params,
	})
	server := testCluster.Server(0)

	return &TenantTestHelper{
		HostCluster: testCluster,
		tenantTestCluster: NewTenantCluster(
			t,
			server,
			tenantClusterSize,
			security.EmbeddedTenantIDs()[0],
			knobs,
		),
		// Spin up a small tenant cluster under a different tenant ID to test
		// tenant isolation.
		tenantControlCluster: NewTenantCluster(
			t,
			server,
			1, /* tenantClusterSize */
			security.EmbeddedTenantIDs()[1],
			knobs,
		),
	}
}

func (h *TenantTestHelper) TestCluster() TenantCluster {
	return h.tenantTestCluster
}

func (h *TenantTestHelper) ControlCluster() TenantCluster {
	return h.tenantControlCluster
}

func (h *TenantTestHelper) Cleanup(ctx context.Context, t *testing.T) {
	t.Helper()
	h.HostCluster.Stopper().Stop(ctx)
	h.tenantTestCluster.Cleanup(t)
	h.tenantControlCluster.Cleanup(t)
}

type TenantCluster []*testTenant

func NewTenantCluster(
	t *testing.T,
	server serverutils.TestServerInterface,
	tenantClusterSize int,
	tenantID uint64,
	knobs base.TestingKnobs,
) TenantCluster {
	t.Helper()

	cluster := make([]*testTenant, tenantClusterSize)
	for i := 0; i < tenantClusterSize; i++ {
		cluster[i] =
			newTestTenant(t, server, roachpb.MakeTenantID(tenantID), knobs)
	}

	return cluster
}

func (c TenantCluster) TenantConn(idx serverIdx) *sqlutils.SQLRunner {
	return c.Tenant(idx).tenantDB
}

func (c TenantCluster) TenantHTTPClient(t *testing.T, idx serverIdx, isAdmin bool) *httpClient {
	var client http.Client
	var err error
	if isAdmin {
		client, err = c.Tenant(idx).Tenant.GetAdminHTTPClient()
	} else {
		client, err = c.Tenant(idx).Tenant.GetAuthenticatedHTTPClient(false)
	}
	require.NoError(t, err)
	return &httpClient{t: t, client: client, baseURL: c[idx].Tenant.AdminURL()}
}

func (c TenantCluster) TenantAdminHTTPClient(t *testing.T, idx serverIdx) *httpClient {
	return c.TenantHTTPClient(t, idx, true /* isAdmin */)
}

func (c TenantCluster) TenantSQLStats(idx serverIdx) *persistedsqlstats.PersistedSQLStats {
	return c.Tenant(idx).tenantSQLStats
}

func (c TenantCluster) TenantStatusSrv(idx serverIdx) serverpb.SQLStatusServer {
	return c.Tenant(idx).tenantStatus
}

func (c TenantCluster) TenantContentionRegistry(idx serverIdx) *contention.Registry {
	return c.Tenant(idx).tenantContentionRegistry
}

func (c TenantCluster) Cleanup(t *testing.T) {
	for _, tenant := range c {
		tenant.cleanup(t)
	}
}

// tenant selects a tenant node from the tenant cluster. If randomServer
// is passed in, then a random node is selected.
func (c TenantCluster) Tenant(idx serverIdx) *testTenant {
	if idx == RandomServer {
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

func (c *httpClient) GetJSONChecked(path string, response protoutil.Message) error {
	return httputil.GetJSON(c.client, c.baseURL+path, response)
}

func (c *httpClient) PostJSON(path string, request protoutil.Message, response protoutil.Message) {
	err := c.PostJSONChecked(path, request, response)
	require.NoError(c.t, err)
}

func (c *httpClient) PostJSONChecked(
	path string, request protoutil.Message, response protoutil.Message,
) error {
	return httputil.PostJSON(c.client, c.baseURL+path, request, response)
}

func (c *httpClient) Close() {
	c.client.CloseIdleConnections()
}

// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package serverccl

import (
	"context"
	gosql "database/sql"
	"math/rand"
	"net/http"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/contention"
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

// RandomServer is a magic value, that when passed to the Tenant() method of
// TenantClusterHelper picks a random tenant.
const RandomServer serverIdx = -1

type testTenant struct {
	tenant                   serverutils.ApplicationLayerInterface
	tenantConn               *gosql.DB
	tenantDB                 *sqlutils.SQLRunner
	tenantStatus             serverpb.SQLStatusServer
	tenantSQLServer          *sql.Server
	tenantContentionRegistry *contention.Registry
}

func (h *testTenant) GetRPCContext() *rpc.Context {
	return h.tenant.RPCContext()
}

func (h *testTenant) GetTenantConn() *sqlutils.SQLRunner {
	return h.tenantDB
}

func (h *testTenant) TenantSQLServer() *sql.Server {
	return h.tenantSQLServer
}

func (h *testTenant) TenantStatusSrv() serverpb.SQLStatusServer {
	return h.tenantStatus
}

func (h *testTenant) TenantContentionRegistry() *contention.Registry {
	return h.tenantContentionRegistry
}

func (h *testTenant) GetTenant() serverutils.ApplicationLayerInterface {
	return h.tenant
}

func (h *testTenant) GetTenantDB() *gosql.DB {
	return h.tenantConn
}

// TestTenant exposes an interface for testing an individual tenant
type TestTenant interface {
	GetTenant() serverutils.ApplicationLayerInterface
	GetTenantDB() *gosql.DB
	GetTenantConn() *sqlutils.SQLRunner
	TenantSQLServer() *sql.Server
	TenantStatusSrv() serverpb.SQLStatusServer
	TenantContentionRegistry() *contention.Registry
	GetRPCContext() *rpc.Context
	Cleanup(t *testing.T)
}

var _ TestTenant = &testTenant{}

func newTestTenant(
	t *testing.T, server serverutils.TestServerInterface, args base.TestTenantArgs,
) TestTenant {
	t.Helper()

	tenant, tenantConn := serverutils.StartTenant(t, server, args)
	sqlDB := sqlutils.MakeSQLRunner(tenantConn)
	status := tenant.StatusServer().(serverpb.SQLStatusServer)
	sqlServer := tenant.SQLServer().(*sql.Server)
	contentionRegistry := tenant.ExecutorConfig().(sql.ExecutorConfig).ContentionRegistry

	return &testTenant{
		tenant:                   tenant,
		tenantConn:               tenantConn,
		tenantDB:                 sqlDB,
		tenantStatus:             status,
		tenantSQLServer:          sqlServer,
		tenantContentionRegistry: contentionRegistry,
	}
}

func (h *testTenant) Cleanup(t *testing.T) {
	require.NoError(t, h.tenantConn.Close())
}

type tenantTestHelper struct {
	hostCluster serverutils.TestClusterInterface

	// Creating two separate tenant clusters. This allows unit tests to test
	// the isolation between different tenants are properly enforced.
	tenantTestCluster    TenantClusterHelper
	tenantControlCluster TenantClusterHelper
}

// TenantTestHelper is an interface that provides a helpful structure for tests
// involving a tenant where we have a test target tenant and a separate control
// tenant operating on the same host.
type TenantTestHelper interface {
	TestCluster() TenantClusterHelper
	ControlCluster() TenantClusterHelper
	HostCluster() serverutils.TestClusterInterface
	Cleanup(ctx context.Context, t *testing.T)
}

var _ TenantTestHelper = &tenantTestHelper{}

// NewTestTenantHelper constructs a TenantTestHelper instance.
func NewTestTenantHelper(
	t *testing.T, tenantClusterSize int, knobs base.TestingKnobs,
) TenantTestHelper {
	t.Helper()

	testCluster := serverutils.StartCluster(t, 1 /* numNodes */, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs:             knobs,
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
		},
	})
	server := testCluster.Server(0)

	return &tenantTestHelper{
		hostCluster: testCluster,
		tenantTestCluster: newTenantClusterHelper(
			t,
			server,
			tenantClusterSize,
			securitytest.EmbeddedTenantIDs()[0],
			knobs,
		),
		// Spin up a small tenant cluster under a different tenant ID to test
		// tenant isolation.
		tenantControlCluster: newTenantClusterHelper(
			t,
			server,
			1, /* tenantClusterSize */
			securitytest.EmbeddedTenantIDs()[1],
			knobs,
		),
	}
}

func (h *tenantTestHelper) HostCluster() serverutils.TestClusterInterface {
	return h.hostCluster
}

func (h *tenantTestHelper) TestCluster() TenantClusterHelper {
	return h.tenantTestCluster
}

func (h *tenantTestHelper) ControlCluster() TenantClusterHelper {
	return h.tenantControlCluster
}

func (h *tenantTestHelper) Cleanup(ctx context.Context, t *testing.T) {
	t.Helper()
	h.hostCluster.Stopper().Stop(ctx)
	h.tenantTestCluster.Cleanup(t)
	h.tenantControlCluster.Cleanup(t)
}

type tenantCluster []TestTenant

// TenantClusterHelper is an interface that provides access to a set of tenants
// on a host cluster under test.
type TenantClusterHelper interface {
	Tenant(idx serverIdx) TestTenant
	TenantConn(idx serverIdx) *sqlutils.SQLRunner
	TenantDB(idx serverIdx) *gosql.DB
	TenantHTTPClient(t *testing.T, idx serverIdx, isAdmin bool) *httpClient
	TenantAdminHTTPClient(t *testing.T, idx serverIdx) *httpClient
	TenantSQLServer(idx serverIdx) *sql.Server
	TenantStatusSrv(idx serverIdx) serverpb.SQLStatusServer
	TenantContentionRegistry(idx serverIdx) *contention.Registry
	Cleanup(t *testing.T)
}

var _ TenantClusterHelper = tenantCluster{}

func newTenantClusterHelper(
	t *testing.T,
	server serverutils.TestServerInterface,
	tenantClusterSize int,
	tenantID uint64,
	knobs base.TestingKnobs,
) TenantClusterHelper {
	t.Helper()

	var cluster tenantCluster = make([]TestTenant, tenantClusterSize)
	for i := 0; i < tenantClusterSize; i++ {
		cluster[i] = newTestTenant(t, server, base.TestTenantArgs{
			TenantID:     roachpb.MustMakeTenantID(tenantID),
			TestingKnobs: knobs,
		})
	}

	return cluster
}

func (c tenantCluster) TenantDB(idx serverIdx) *gosql.DB {
	return c.Tenant(idx).GetTenantDB()
}

func (c tenantCluster) TenantConn(idx serverIdx) *sqlutils.SQLRunner {
	return c.Tenant(idx).GetTenantConn()
}

func (c tenantCluster) TenantHTTPClient(t *testing.T, idx serverIdx, isAdmin bool) *httpClient {
	var client http.Client
	var err error
	if isAdmin {
		client, err = c.Tenant(idx).GetTenant().GetAdminHTTPClient()
	} else {
		client, err = c.Tenant(idx).GetTenant().GetAuthenticatedHTTPClient(false, serverutils.SingleTenantSession)
	}
	require.NoError(t, err)
	return &httpClient{t: t, client: client, baseURL: c[idx].GetTenant().AdminURL().String()}
}

func (c tenantCluster) TenantAdminHTTPClient(t *testing.T, idx serverIdx) *httpClient {
	return c.TenantHTTPClient(t, idx, true /* isAdmin */)
}

func (c tenantCluster) TenantSQLServer(idx serverIdx) *sql.Server {
	return c.Tenant(idx).TenantSQLServer()
}

func (c tenantCluster) TenantStatusSrv(idx serverIdx) serverpb.SQLStatusServer {
	return c.Tenant(idx).TenantStatusSrv()
}

func (c tenantCluster) TenantContentionRegistry(idx serverIdx) *contention.Registry {
	return c.Tenant(idx).TenantContentionRegistry()
}

func (c tenantCluster) Cleanup(t *testing.T) {
	for _, tenant := range c {
		tenant.Cleanup(t)
	}
}

// Tenant selects a tenant node from the tenant cluster. If randomServer
// is passed in, then a random node is selected.
func (c tenantCluster) Tenant(idx serverIdx) TestTenant {
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

func (c *httpClient) GetClient() http.Client {
	return c.client
}

func (c *httpClient) GetBaseURL() string {
	return c.baseURL
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

func (c *httpClient) PostJSONRawChecked(path string, request []byte) (*http.Response, error) {
	return httputil.PostJSONRaw(c.client, c.baseURL+path, request)
}

func (c *httpClient) Close() {
	c.client.CloseIdleConnections()
}

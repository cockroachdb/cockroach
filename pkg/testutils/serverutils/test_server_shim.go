// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//
// This file provides generic interfaces that allow tests to set up test servers
// without importing the server package (avoiding circular dependencies).
// To be used, the binary needs to call
// InitTestServerFactory(server.TestServerFactory), generally from a TestMain()
// in an "foo_test" package (which can import server and is linked together with
// the other tests in package "foo").

package serverutils

import (
	"context"
	gosql "database/sql"
	"net/http"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// TestServerInterface defines test server functionality that tests need; it is
// implemented by server.TestServer.
type TestServerInterface interface {
	Stopper() *stop.Stopper

	Start(context.Context) error

	// Node returns the server.Node as an interface{}.
	Node() interface{}

	// NodeID returns the ID of this node within its cluster.
	NodeID() roachpb.NodeID

	// ClusterID returns the cluster ID as understood by this node in the
	// cluster.
	ClusterID() uuid.UUID

	// ServingRPCAddr returns the server's advertised address.
	ServingRPCAddr() string

	// ServingSQLAddr returns the server's advertised SQL address.
	ServingSQLAddr() string

	// HTTPAddr returns the server's http address.
	HTTPAddr() string

	// RPCAddr returns the server's RPC address.
	// Note: use ServingRPCAddr() instead unless specific reason not to.
	RPCAddr() string

	// SQLAddr returns the server's SQL address.
	// Note: use ServingSQLAddr() instead unless specific reason not to.
	SQLAddr() string

	// DB returns a *client.DB instance for talking to this KV server.
	DB() *kv.DB

	// RPCContext returns the rpc context used by the test server.
	RPCContext() *rpc.Context

	// LeaseManager() returns the *sql.LeaseManager as an interface{}.
	LeaseManager() interface{}

	// InternalExecutor returns a *sql.InternalExecutor as an interface{} (which
	// also implements sqlutil.InternalExecutor if the test cannot depend on sql).
	InternalExecutor() interface{}

	// ExecutorConfig returns a copy of the server's ExecutorConfig.
	// The real return type is sql.ExecutorConfig.
	ExecutorConfig() interface{}

	// Tracer returns a *tracing.Tracer as an interface{}.
	Tracer() interface{}

	// GossipI returns the gossip used by the TestServer.
	// The real return type is *gossip.Gossip.
	GossipI() interface{}

	// RangeFeedFactory returns the range feed factory used by the TestServer.
	// The real return type is *rangefeed.Factory.
	RangeFeedFactory() interface{}

	// Clock returns the clock used by the TestServer.
	Clock() *hlc.Clock

	// DistSenderI returns the DistSender used by the TestServer.
	// The real return type is *kv.DistSender.
	DistSenderI() interface{}

	// MigrationServer returns the internal *migrationServer as in interface{}
	MigrationServer() interface{}

	// SQLServer returns the *sql.Server as an interface{}.
	SQLServer() interface{}

	// DistSQLServer returns the *distsql.ServerImpl as an interface{}.
	DistSQLServer() interface{}

	// SQLLivenessStorage returns the sqlliveness.Provider as an interface{}.
	SQLLivenessProvider() interface{}

	// JobRegistry returns the *jobs.Registry as an interface{}.
	JobRegistry() interface{}

	// SQLMigrationsManager returns the *sqlmigrations.Manager as an interface{}.
	SQLMigrationsManager() interface{}

	// NodeLiveness exposes the NodeLiveness instance used by the TestServer as an
	// interface{}.
	NodeLiveness() interface{}

	// HeartbeatNodeLiveness heartbeats the server's NodeLiveness record.
	HeartbeatNodeLiveness() error

	// NodeDialer exposes the NodeDialer instance used by the TestServer as an
	// interface{}.
	NodeDialer() interface{}

	// SetDistSQLSpanResolver changes the SpanResolver used for DistSQL inside the
	// server's executor. The argument must be a physicalplan.SpanResolver
	// instance.
	//
	// This method exists because we cannot pass the fake span resolver with the
	// server or cluster params: the fake span resolver needs the node IDs and
	// addresses of the servers in a cluster, which are not available before we
	// start the servers.
	//
	// It is the caller's responsibility to make sure no queries are being run
	// with DistSQL at the same time.
	SetDistSQLSpanResolver(spanResolver interface{})

	// AdminURL returns the URL for the admin UI.
	AdminURL() string
	// GetHTTPClient returns an http client configured with the client TLS
	// config required by the TestServer's configuration.
	GetHTTPClient() (http.Client, error)
	// GetAdminAuthenticatedHTTPClient returns an http client which has been
	// authenticated to access Admin API methods (via a cookie).
	// The user has admin privileges.
	GetAdminAuthenticatedHTTPClient() (http.Client, error)
	// GetAuthenticatedHTTPClient returns an http client which has been
	// authenticated to access Admin API methods (via a cookie).
	GetAuthenticatedHTTPClient(isAdmin bool) (http.Client, error)

	// MustGetSQLCounter returns the value of a counter metric from the server's
	// SQL Executor. Runs in O(# of metrics) time, which is fine for test code.
	MustGetSQLCounter(name string) int64
	// MustGetSQLNetworkCounter returns the value of a counter metric from the
	// server's SQL server. Runs in O(# of metrics) time, which is fine for test
	// code.
	MustGetSQLNetworkCounter(name string) int64
	// WriteSummaries records summaries of time-series data, which is required for
	// any tests that query server stats.
	WriteSummaries() error

	// GetFirstStoreID is a utility function returning the StoreID of the first
	// store on this node.
	GetFirstStoreID() roachpb.StoreID

	// GetStores returns the collection of stores from this TestServer's node.
	// The return value is of type *kvserver.Stores.
	GetStores() interface{}

	// ClusterSettings returns the ClusterSettings shared by all components of
	// this server.
	ClusterSettings() *cluster.Settings

	// Decommission idempotently sets the decommissioning flag for specified nodes.
	Decommission(ctx context.Context, targetStatus livenesspb.MembershipStatus, nodeIDs []roachpb.NodeID) error

	// SplitRange splits the range containing splitKey.
	SplitRange(splitKey roachpb.Key) (left roachpb.RangeDescriptor, right roachpb.RangeDescriptor, err error)

	// MergeRanges merges the range containing leftKey with the following adjacent
	// range.
	MergeRanges(leftKey roachpb.Key) (merged roachpb.RangeDescriptor, err error)

	// ExpectedInitialRangeCount returns the expected number of ranges that should
	// be on the server after initial (asynchronous) splits have been completed,
	// assuming no additional information is added outside of the normal bootstrap
	// process.
	ExpectedInitialRangeCount() (int, error)

	// ForceTableGC sends a GCRequest for the ranges corresponding to a table.
	//
	// An error will be returned if the same table name exists in multiple schemas
	// inside the specified database.
	ForceTableGC(ctx context.Context, database, table string, timestamp hlc.Timestamp) error

	// UpdateChecker returns the server's *diagnostics.UpdateChecker as an
	// interface{}. The UpdateChecker periodically phones home to check for new
	// updates that are available.
	UpdateChecker() interface{}

	// DiagnosticsReporter returns the server's *diagnostics.Reporter as an
	// interface{}. The DiagnosticsReporter periodically phones home to report
	// diagnostics and usage.
	DiagnosticsReporter() interface{}

	// StartTenant spawns off tenant process connecting to this TestServer.
	StartTenant(ctx context.Context, params base.TestTenantArgs) (TestTenantInterface, error)

	// ScratchRange splits off a range suitable to be used as KV scratch space.
	// (it doesn't overlap system spans or SQL tables).
	//
	// Calling this multiple times is undefined (but see
	// TestCluster.ScratchRange() which is idempotent).
	ScratchRange() (roachpb.Key, error)

	// Engines returns the TestServer's engines.
	Engines() []storage.Engine

	// MetricsRecorder periodically records node-level and store-level metrics.
	MetricsRecorder() *status.MetricsRecorder
}

// TestServerFactory encompasses the actual implementation of the shim
// service.
type TestServerFactory interface {
	// New instantiates a test server.
	New(params base.TestServerArgs) (interface{}, error)
}

var srvFactoryImpl TestServerFactory

// InitTestServerFactory should be called once to provide the implementation
// of the service. It will be called from a xx_test package that can import the
// server package.
func InitTestServerFactory(impl TestServerFactory) {
	srvFactoryImpl = impl
}

// StartServer creates and starts a test server, and sets up a gosql DB
// connection to it. The server should be stopped by calling
// server.Stopper().Stop().
func StartServer(
	t testing.TB, params base.TestServerArgs,
) (TestServerInterface, *gosql.DB, *kv.DB) {
	server, err := NewServer(params)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("%+v", err)
	}
	goDB := OpenDBConn(
		t, server.ServingSQLAddr(), params.UseDatabase, params.Insecure, server.Stopper())
	return server, goDB, server.DB()
}

// NewServer creates a test server.
func NewServer(params base.TestServerArgs) (TestServerInterface, error) {
	if srvFactoryImpl == nil {
		return nil, errors.AssertionFailedf("TestServerFactory not initialized. One needs to be injected " +
			"from the package's TestMain()")
	}

	srv, err := srvFactoryImpl.New(params)
	if err != nil {
		return nil, err
	}
	return srv.(TestServerInterface), nil
}

// OpenDBConnE is like OpenDBConn, but returns an error.
func OpenDBConnE(
	sqlAddr string, useDatabase string, insecure bool, stopper *stop.Stopper,
) (*gosql.DB, error) {
	pgURL, cleanupGoDB, err := sqlutils.PGUrlE(
		sqlAddr, "StartServer" /* prefix */, url.User(security.RootUser))
	if err != nil {
		return nil, err
	}

	pgURL.Path = useDatabase
	if insecure {
		pgURL.RawQuery = "sslmode=disable"
	}
	goDB, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		return nil, err
	}

	stopper.AddCloser(
		stop.CloserFn(func() {
			_ = goDB.Close()
			cleanupGoDB()
		}))
	return goDB, nil
}

// OpenDBConn sets up a gosql DB connection to the given server.
func OpenDBConn(
	t testing.TB, sqlAddr string, useDatabase string, insecure bool, stopper *stop.Stopper,
) *gosql.DB {
	conn, err := OpenDBConnE(sqlAddr, useDatabase, insecure, stopper)
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

// StartServerRaw creates and starts a TestServer.
// Generally StartServer() should be used. However this function can be used
// directly when opening a connection to the server is not desired.
func StartServerRaw(args base.TestServerArgs) (TestServerInterface, error) {
	server, err := NewServer(args)
	if err != nil {
		return nil, err
	}
	if err := server.Start(context.Background()); err != nil {
		return nil, err
	}
	return server, nil
}

// StartTenant starts a tenant SQL server connecting to the supplied test
// server. It uses the server's stopper to shut down automatically. However,
// the returned DB is for the caller to close.
func StartTenant(
	t testing.TB, ts TestServerInterface, params base.TestTenantArgs,
) (TestTenantInterface, *gosql.DB) {
	tenant, err := ts.StartTenant(context.Background(), params)
	if err != nil {
		t.Fatal(err)
	}

	stopper := params.Stopper
	if stopper == nil {
		stopper = ts.Stopper()
	}

	goDB := OpenDBConn(
		t, tenant.SQLAddr(), params.UseDatabase, false /* insecure */, stopper)
	return tenant, goDB
}

// TestTenantID returns a roachpb.TenantID that can be used when
// starting a test Tenant. The returned tenant IDs match those built
// into the test certificates.
func TestTenantID() roachpb.TenantID {
	return roachpb.MakeTenantID(security.EmbeddedTenantIDs()[0])
}

// GetJSONProto uses the supplied client to GET the URL specified by the parameters
// and unmarshals the result into response.
func GetJSONProto(ts TestServerInterface, path string, response protoutil.Message) error {
	return GetJSONProtoWithAdminOption(ts, path, response, true)
}

// GetJSONProtoWithAdminOption is like GetJSONProto but the caller can customize
// whether the request is performed with admin privilege
func GetJSONProtoWithAdminOption(
	ts TestServerInterface, path string, response protoutil.Message, isAdmin bool,
) error {
	httpClient, err := ts.GetAuthenticatedHTTPClient(isAdmin)
	if err != nil {
		return err
	}
	return httputil.GetJSON(httpClient, ts.AdminURL()+path, response)
}

// PostJSONProto uses the supplied client to POST the URL specified by the parameters
// and unmarshals the result into response.
func PostJSONProto(ts TestServerInterface, path string, request, response protoutil.Message) error {
	return PostJSONProtoWithAdminOption(ts, path, request, response, true)
}

// PostJSONProtoWithAdminOption is like PostJSONProto but the caller
// can customize whether the request is performed with admin
// privilege.
func PostJSONProtoWithAdminOption(
	ts TestServerInterface, path string, request, response protoutil.Message, isAdmin bool,
) error {
	httpClient, err := ts.GetAuthenticatedHTTPClient(isAdmin)
	if err != nil {
		return err
	}
	return httputil.PostJSON(httpClient, ts.AdminURL()+path, request, response)
}

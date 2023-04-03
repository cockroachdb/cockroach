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
	"flag"
	"math/rand"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// TenantModeFlagName is the exported name of the tenantMode flag, for use
// in other packages.
const TenantModeFlagName = "tenantMode"

var tenantModeFlag = flag.String(
	TenantModeFlagName, tenantModeDefault,
	"tenantMode in which to run tests. Options are forceTenant, forceNoTenant, and default "+
		"which alternates between tenant and no-tenant mode probabilistically. Note that the two force "+
		"modes are ignored if the test is already forced to run in one of the two modes.")

const (
	tenantModeForceTenant   = "forceTenant"
	tenantModeForceNoTenant = "forceNoTenant"
	tenantModeDefault       = "default"
)

// ShouldStartDefaultTestTenant determines whether a default test tenant
// should be started for test servers or clusters, to serve SQL traffic by
// default. It defaults to 50% probability, but can be overridden by the
// tenantMode test flag or the COCKROACH_TEST_TENANT_MODE environment variable.
// If both the environment variable and the test flag are set, the environment
// variable wins out.
func ShouldStartDefaultTestTenant(t testing.TB) bool {
	var defaultProbabilityOfStartingTestTenant = 0.5
	if skip.UnderBench() {
		// Until #83461 is resolved, we want to make sure that we don't use the
		// multi-tenant setup so that the comparison against old single-tenant
		// SHAs in the benchmarks is fair.
		defaultProbabilityOfStartingTestTenant = 0
	}
	var probabilityOfStartingDefaultTestTenant float64

	tenantModeTestString, envSet := envutil.EnvString("COCKROACH_TEST_TENANT_MODE", 0)
	if !envSet {
		tenantModeTestString = *tenantModeFlag
	}

	switch tenantModeTestString {
	case tenantModeForceTenant:
		probabilityOfStartingDefaultTestTenant = 1.0
	case tenantModeForceNoTenant:
		probabilityOfStartingDefaultTestTenant = 0.0
	case tenantModeDefault:
		probabilityOfStartingDefaultTestTenant = defaultProbabilityOfStartingTestTenant
	default:
		t.Fatal("invalid setting of tenantMode flag")
	}

	return rand.Float64() <= probabilityOfStartingDefaultTestTenant
}

// TestServerInterface defines test server functionality that tests need; it is
// implemented by server.TestServer.
type TestServerInterface interface {
	Start(context.Context) error

	// TestTenantInterface embeds SQL-only APIs that tests need to interact with
	// the host tenant.
	//
	// TODO(irfansharif): Audit the remaining symbols in TestServerInterface to
	// see if they're better suited to TestTenantInterface.
	TestTenantInterface

	// Node returns the server.Node as an interface{}.
	Node() interface{}

	// NodeID returns the ID of this node within its cluster.
	NodeID() roachpb.NodeID

	// StorageClusterID returns the storage cluster ID as understood by
	// this node in the cluster.
	StorageClusterID() uuid.UUID

	// ServingRPCAddr returns the server's advertised address.
	ServingRPCAddr() string

	// ServingSQLAddr returns the server's advertised SQL address.
	ServingSQLAddr() string

	// RPCAddr returns the server's RPC address.
	// Note: use ServingRPCAddr() instead unless specific reason not to.
	RPCAddr() string

	// LeaseManager() returns the *sql.LeaseManager as an interface{}.
	LeaseManager() interface{}

	// InternalExecutor returns a *sql.InternalExecutor as an interface{} (which
	// also implements insql.InternalExecutor if the test cannot depend on sql).
	InternalExecutor() interface{}

	// InternalExecutorInternalExecutorFactory returns a
	// insql.InternalDB as an interface{}.
	InternalDB() interface{}

	// TracerI returns a *tracing.Tracer as an interface{}.
	TracerI() interface{}

	// GossipI returns the gossip used by the TestServer.
	// The real return type is *gossip.Gossip.
	GossipI() interface{}

	// DistSenderI returns the DistSender used by the TestServer.
	// The real return type is *kv.DistSender.
	DistSenderI() interface{}

	// MigrationServer returns the internal *migrationServer as in interface{}
	MigrationServer() interface{}

	// SQLServer returns the *sql.Server as an interface{}.
	SQLServer() interface{}

	// SQLLivenessProvider returns the sqlliveness.Provider as an interface{}.
	SQLLivenessProvider() interface{}

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

	// Decommission idempotently sets the decommissioning flag for specified nodes.
	Decommission(ctx context.Context, targetStatus livenesspb.MembershipStatus, nodeIDs []roachpb.NodeID) error

	// DecommissioningNodeMap returns a map of nodeIDs that are known to the
	// server to be decommissioning.
	DecommissioningNodeMap() map[roachpb.NodeID]interface{}

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

	// StartSharedProcessTenant starts a "shared-process" tenant - i.e. a tenant
	// running alongside a KV server.
	//
	// args.TenantName must be specified. If a tenant with that name already
	// exists, its ID is checked against args.TenantID (if set), and, if it
	// matches, new tenant metadata is not created in the system.tenants table.
	//
	// See also StartTenant(), which starts a tenant mimicking out-of-process tenant
	// servers.
	StartSharedProcessTenant(
		ctx context.Context, args base.TestSharedProcessTenantArgs,
	) (TestTenantInterface, *gosql.DB, error)

	// StartTenant starts a tenant server connecting to this TestServer. The
	// tenant server simulates an out-of-process server. See also
	// StartSharedProcessTenant() for a tenant simulating a shared-memory server.
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

	// CollectionFactory returns a *descs.CollectionFactory.
	CollectionFactory() interface{}

	// SystemTableIDResolver returns a catalog.SystemTableIDResolver.
	SystemTableIDResolver() interface{}

	// SpanConfigKVSubscriber returns the embedded spanconfig.KVSubscriber for
	// the server.
	SpanConfigKVSubscriber() interface{}

	// TestTenants returns the test tenants associated with the server
	TestTenants() []TestTenantInterface

	// StartedDefaultTestTenant returns true if the server has started the default
	// test tenant.
	StartedDefaultTestTenant() bool

	// TenantOrServer returns the default test tenant, if it was started or this
	// server if not.
	TenantOrServer() TestTenantInterface

	// BinaryVersionOverride returns the value of an override if set using
	// TestingKnobs.
	BinaryVersionOverride() roachpb.Version
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
	if !params.DisableDefaultTestTenant {
		// Determine if we should probabilistically start a test tenant
		// for this server.
		startDefaultSQLServer := ShouldStartDefaultTestTenant(t)
		if !startDefaultSQLServer {
			// If we're told not to start a test tenant, set the
			// disable flag explicitly.
			params.DisableDefaultTestTenant = true
		}
	}

	s, err := NewServer(params)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("%+v", err)
	}
	goDB := OpenDBConn(
		t, s.ServingSQLAddr(), params.UseDatabase, params.Insecure, s.Stopper())

	// Now that we have started the server on the bootstrap version, let us run
	// the migrations up to the overridden BinaryVersion.
	if v := s.BinaryVersionOverride(); v != (roachpb.Version{}) {
		if _, err := goDB.Exec(`SET CLUSTER SETTING version = $1`, v.String()); err != nil {
			t.Fatal(err)
		}
	}

	return s, goDB, s.DB()
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
		sqlAddr, "StartServer" /* prefix */, url.User(username.RootUser))
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
//
// Note: log.Scope() should always be used in tests that start a tenant
// (otherwise, having more than one test in a package which uses StartTenant
// without log.Scope() will cause a a "clusterID already set" panic).
func StartTenant(
	t testing.TB, ts TestServerInterface, params base.TestTenantArgs,
) (TestTenantInterface, *gosql.DB) {
	tenant, err := ts.StartTenant(context.Background(), params)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	stopper := params.Stopper
	if stopper == nil {
		stopper = ts.Stopper()
	}

	goDB := OpenDBConn(
		t, tenant.SQLAddr(), params.UseDatabase, false /* insecure */, stopper)
	return tenant, goDB
}

func StartSharedProcessTenant(
	t testing.TB, ts TestServerInterface, params base.TestSharedProcessTenantArgs,
) (TestTenantInterface, *gosql.DB) {
	tenant, goDB, err := ts.StartSharedProcessTenant(context.Background(), params)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	return tenant, goDB
}

// TestTenantID returns a roachpb.TenantID that can be used when
// starting a test Tenant. The returned tenant IDs match those built
// into the test certificates.
func TestTenantID() roachpb.TenantID {
	return roachpb.MustMakeTenantID(security.EmbeddedTenantIDs()[0])
}

// TestTenantID2 returns another roachpb.TenantID that can be used when
// starting a test Tenant. The returned tenant IDs match those built
// into the test certificates.
func TestTenantID2() roachpb.TenantID {
	return roachpb.MustMakeTenantID(security.EmbeddedTenantIDs()[1])
}

// TestTenantID3 returns another roachpb.TenantID that can be used when
// starting a test Tenant. The returned tenant IDs match those built
// into the test certificates.
func TestTenantID3() roachpb.TenantID {
	return roachpb.MustMakeTenantID(security.EmbeddedTenantIDs()[2])
}

// GetJSONProto uses the supplied client to GET the URL specified by the parameters
// and unmarshals the result into response.
func GetJSONProto(ts TestTenantInterface, path string, response protoutil.Message) error {
	return GetJSONProtoWithAdminOption(ts, path, response, true)
}

// GetJSONProtoWithAdminOption is like GetJSONProto but the caller can customize
// whether the request is performed with admin privilege
func GetJSONProtoWithAdminOption(
	ts TestTenantInterface, path string, response protoutil.Message, isAdmin bool,
) error {
	httpClient, err := ts.GetAuthenticatedHTTPClient(isAdmin, SingleTenantSession)
	if err != nil {
		return err
	}
	return httputil.GetJSON(httpClient, ts.AdminURL()+path, response)
}

// PostJSONProto uses the supplied client to POST the URL specified by the parameters
// and unmarshals the result into response.
func PostJSONProto(ts TestTenantInterface, path string, request, response protoutil.Message) error {
	return PostJSONProtoWithAdminOption(ts, path, request, response, true)
}

// PostJSONProtoWithAdminOption is like PostJSONProto but the caller
// can customize whether the request is performed with admin
// privilege.
func PostJSONProtoWithAdminOption(
	ts TestTenantInterface, path string, request, response protoutil.Message, isAdmin bool,
) error {
	httpClient, err := ts.GetAuthenticatedHTTPClient(isAdmin, SingleTenantSession)
	if err != nil {
		return err
	}
	return httputil.PostJSON(httpClient, ts.AdminURL()+path, request, response)
}

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
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvprober"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// DefaultTestTenantMessage is a message that is printed when a test is run
// with the default test tenant. This is useful for debugging test failures.
const DefaultTestTenantMessage = `
Test server was configured to route SQL queries to a secondary tenant (virtual cluster).
If you are only seeing a test failure when this message appears, there may be a problem
specific to cluster virtualization or multi-tenancy.

To investigate, consider using "COCKROACH_TEST_TENANT=true" to force-enable just
the secondary tenant in all runs (or, alternatively, "false" to force-disable), or use
"COCKROACH_INTERNAL_DISABLE_METAMORPHIC_TESTING=false" to disable all random test variables altogether.`

var PreventStartTenantError = errors.New("attempting to manually start a server for a secondary tenant while " +
	"DefaultTestTenant is set to TestTenantProbabilisticOnly")

// ShouldStartDefaultTestTenant determines whether a default test tenant
// should be started for test servers or clusters, to serve SQL traffic by
// default.
// This can be overridden either via the build tag `metamorphic_disable`
// or just for test tenants via COCKROACH_TEST_TENANT.
func ShouldStartDefaultTestTenant(t testing.TB, serverArgs base.TestServerArgs) bool {
	// Explicit cases for enabling or disabling the default test tenant.
	if serverArgs.DefaultTestTenant.TestTenantAlwaysEnabled() {
		return true
	}
	if serverArgs.DefaultTestTenant.TestTenantAlwaysDisabled() {
		if issueNum, label := serverArgs.DefaultTestTenant.IssueRef(); issueNum != 0 {
			t.Logf("cluster virtualization disabled due to issue: #%d (expected label: %s)", issueNum, label)
		}
		return false
	}

	if skip.UnderBench() {
		// Until #83461 is resolved, we want to make sure that we don't use the
		// multi-tenant setup so that the comparison against old single-tenant
		// SHAs in the benchmarks is fair.
		return false
	}

	// Obey the env override if present.
	if str, present := envutil.EnvString("COCKROACH_TEST_TENANT", 0); present {
		v, err := strconv.ParseBool(str)
		if err != nil {
			panic(err)
		}
		return v
	}

	// Note: we ask the metamorphic framework for a "disable" value, instead
	// of an "enable" value, because it probabilistically returns its default value
	// more often than not and that is what we want.
	enabled := !util.ConstantWithMetamorphicTestBoolWithoutLogging("disable-test-tenant", false)
	if enabled {
		t.Log(DefaultTestTenantMessage)
	}
	return enabled
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

	// InternalExecutorInternalExecutorFactory returns a
	// insql.InternalDB as an interface{}.
	InternalDB() interface{}

	// GossipI returns the gossip used by the TestServer.
	// The real return type is *gossip.Gossip.
	GossipI() interface{}

	// DistSenderI returns the DistSender used by the TestServer.
	// The real return type is *kv.DistSender.
	DistSenderI() interface{}

	// MigrationServer returns the internal *migrationServer as in interface{}
	MigrationServer() interface{}

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

	// DisableStartTenant prevents manual starting of tenants. If an attempt at
	// starting a tenant is made, the server will return the specified error.
	DisableStartTenant(reason error)

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

	// KVFlowController returns the embedded kvflowcontrol.Controller for the
	// server.
	KVFlowController() interface{}

	// KVFlowHandles returns the embedded kvflowcontrol.Handles for the server.
	KVFlowHandles() interface{}

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

	// KvProber returns a *kvprober.Prober, which is useful when asserting the
	//correctness of the prober from integration tests.
	KvProber() *kvprober.Prober

	// TenantCapabilitiesReader retrieves a reference to the
	// capabilities reader.
	TenantCapabilitiesReader() tenantcapabilities.Reader
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
	allowAdditionalTenants := params.DefaultTestTenant.AllowAdditionalTenants()
	// Determine if we should probabilistically start a test tenant
	// for this server.
	startDefaultSQLServer := ShouldStartDefaultTestTenant(t, params)
	if !startDefaultSQLServer {
		// If we're told not to start a test tenant, set the
		// disable flag explicitly.
		//
		// TODO(#76378): review the definition of params.DefaultTestTenant
		// so we do not need this weird sentinel value.
		params.DefaultTestTenant = base.InternalNonDefaultDecision
	}

	s, err := NewServer(params)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("%+v", err)
	}

	if s.StartedDefaultTestTenant() {
		t.Log(DefaultTestTenantMessage)
	}

	if !allowAdditionalTenants {
		s.DisableStartTenant(PreventStartTenantError)
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
// Generally StartServer() should be used. However, this function can be used
// directly when opening a connection to the server is not desired.
func StartServerRaw(t testing.TB, args base.TestServerArgs) (TestServerInterface, error) {
	server, err := NewServer(args)
	if err != nil {
		return nil, err
	}
	if err := server.Start(context.Background()); err != nil {
		return nil, err
	}
	if server.StartedDefaultTestTenant() {
		t.Log(DefaultTestTenantMessage)
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
	u := ts.AdminURL().String()
	fullURL := u + path
	log.Infof(context.Background(), "test retrieving protobuf over HTTP: %s", fullURL)
	return httputil.GetJSON(httpClient, fullURL, response)
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
	fullURL := ts.AdminURL().WithPath(path).String()
	log.Infof(context.Background(), "test retrieving protobuf over HTTP: %s", fullURL)
	return httputil.PostJSON(httpClient, fullURL, request, response)
}

// WaitForTenantCapabilities waits until the given set of capabilities have been cached.
func WaitForTenantCapabilities(
	t testing.TB,
	s TestServerInterface,
	tenID roachpb.TenantID,
	targetCaps map[tenantcapabilities.ID]string,
	errPrefix string,
) {
	if errPrefix != "" && !strings.HasSuffix(errPrefix, ": ") {
		errPrefix += ": "
	}
	testutils.SucceedsSoon(t, func() error {
		if tenID.IsSystem() {
			return nil
		}
		if len(targetCaps) == 0 {
			return nil
		}

		missingCapabilityError := func(capID tenantcapabilities.ID) error {
			return errors.Newf("%stenant %s cap %q not at expected value", errPrefix, tenID, capID)
		}
		capabilities, found := s.TenantCapabilitiesReader().GetCapabilities(tenID)
		if !found {
			return errors.Newf("%scapabilities not ready for tenant %v", errPrefix, tenID)
		}

		for capID, expectedValue := range targetCaps {
			curVal := tenantcapabilities.MustGetValueByID(capabilities, capID).String()
			if curVal != expectedValue {
				return missingCapabilityError(capID)
			}
		}

		return nil
	})
}

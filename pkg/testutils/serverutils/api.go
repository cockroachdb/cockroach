// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//
// This file provides generic interfaces that allow tests to set up test tenants
// without importing the server package (avoiding circular dependencies). This

package serverutils

import (
	"context"
	gosql "database/sql"
	"net/http"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvprober"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/decommissioning"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"google.golang.org/grpc"
)

// TestServerInterfaceRaw is the interface of server.testServer.
type TestServerInterfaceRaw interface {
	TestServerController
	StorageLayerInterface
	TenantControlInterface

	// in testServer, the base implementation of
	// ApplicationLayerInterface always points to the system tenant.
	ApplicationLayerInterface
}

type TestServerInterface interface {
	TestServerController

	// StorageLayerInterface is implemented by TestServerInterface
	// for backward-compatibility with existing test code.
	// New code should spell out their intent clearly by calling
	// the .StorageLayer() method.
	StorageLayerInterface

	// ApplicationLayerInterface is implemented by TestServerInterface
	// for backward-compatibility with existing test code.
	// It is equivalent to the result of calling .ApplicationLayer().
	//
	// New tests should spell out their intent clearly by calling the
	// .ApplicationLayer() (preferred) or .SystemLayer() methods
	// directly.
	ApplicationLayerInterface

	// TenantControlInterface is implemented by TestServerInterface
	// for backward-compatibility with existing test code.
	// New code should spell out their intent clearly by calling
	// the .TenantController() method.
	TenantControlInterface

	// ApplicationLayer returns the interface to the application layer used
	// in testing. If the server starts with tenancy enabled under the default
	// tenant option, this refers to the SQL layer of the virtual cluster.
	// Otherwise, in single-tenant mode, it refers to the system layer.
	ApplicationLayer() ApplicationLayerInterface

	// SystemLayer returns the interface to the application layer
	// of the system tenant.
	SystemLayer() ApplicationLayerInterface

	// StorageLayer returns the interface to the storage layer.
	StorageLayer() StorageLayerInterface

	// TenantController returns the interface to the tenant controller.
	TenantController() TenantControlInterface
}

// TestServerController defines the control interface for a test server.
type TestServerController interface {
	// Start runs the server. This is pre-called by StartServer().
	// It is provided for tests that use the TestServerFactory directly
	// (mostly 'cockroach demo').
	//
	// For convenience, the caller can assume that Stop() has been called
	// already if Start() fails with an error.
	//
	// Start is an alias for PreStart() followed by Activate(). This
	// distinction exists mainly for the benefit of the TestCluster
	// boostrap logic.
	Start(context.Context) error

	// PreStart is the bootstrap/init phase of Start().
	PreStart(context.Context) error

	// Activate is the service activation phase of Start().
	Activate(context.Context) error

	// Stopper returns the stopper used by the server.
	// TODO(knz): replace uses by Stop().
	Stopper() *stop.Stopper

	// Stop stops the server. This must be called at the end of a test
	// to avoid leaking resources.
	Stop(context.Context)

	// SetReadyFn can be configured to notify a test when the server is
	// ready. This is only effective when called before Start().
	SetReadyFn(fn func(bool))

	// RunInitialSQL is used by 'cockroach demo' to initialize
	// an admin user.
	// TODO(knz): Migrate this logic to a demo-specific init task
	// or config profile.
	RunInitialSQL(ctx context.Context, startSingleNode bool, adminUser, adminPassword string) error
}

// DeploymentMode defines the mode of the underlying test server or tenant,
// which can be single-tenant (system-only), shared-process, or
// external-process.
type DeploymentMode uint8

const (
	SingleTenant DeploymentMode = iota
	SharedProcess
	ExternalProcess
)

func (d DeploymentMode) IsExternal() bool {
	return d == ExternalProcess
}

// ApplicationLayerInterface defines accessors to the application
// layer of a test server. Tests written against this interface are
// effectively agnostic to whether they use a virtual cluster or not.
type ApplicationLayerInterface interface {
	// Readiness returns true when the server is ready, that is,
	// when it is accepting connections and it is not draining.
	Readiness(ctx context.Context) error

	// SQLInstanceID is the ephemeral ID assigned to a running instance of the
	// SQLServer. Each tenant can have zero or more running SQLServer instances.
	SQLInstanceID() base.SQLInstanceID

	// AdvRPCAddr returns the server's advertised address.
	AdvRPCAddr() string

	// AdvSQLAddr returns the server's advertised SQL address.
	AdvSQLAddr() string

	// SQLAddr returns the server's SQL address. Note that for "shared-process
	// tenants" (i.e. tenants created with TestServer.StartSharedProcessTenant),
	// simply connecting to this address connects to the system tenant, not to
	// this tenant. In order to connect to this tenant,
	// "cluster:<tenantName>/<databaseName>" needs to be added to the connection
	// string as the database name.
	SQLAddr() string

	// HTTPAddr returns the server's http address.
	HTTPAddr() string

	// RPCAddr returns the server's RPC address.
	RPCAddr() string

	// NodeDialer exposes the instance-to-instance dialer interface{}.
	// The concrete type is *nodedialer.Dialer.
	NodeDialer() interface{}

	// SQLConn returns a handle to the server's SQL interface, opened with the
	// 'root' user if no user option is passed. If no database name option is
	// passed, DefaultDatabaseName is used.
	// The connection is closed automatically when the server is stopped.
	// Beware that each call returns a separate, new connection object.
	SQLConn(t TestFataler, opts ...SQLConnOption) *gosql.DB

	// SQLConnE is like SQLConn, but it allows the test to check the error.
	SQLConnE(opts ...SQLConnOption) (*gosql.DB, error)

	// PGUrl returns a postgres connection URL for the server's SQL interface
	// similar to the URL that SQLConn uses to open a SQL connection. The SQLConn
	// method should be preferred and this method only should be used when the
	// test needs to open a connection with special options, or in a specific way.
	PGUrl(t TestFataler, opts ...SQLConnOption) (url.URL, func())

	// PGUrlE is like PGUrl, but it allows the test to check the error.
	PGUrlE(opts ...SQLConnOption) (url.URL, func(), error)

	// DB returns a handle to the cluster's KV interface.
	DB() *kv.DB

	// PGServer returns the server's *pgwire.Server as an interface{}.
	PGServer() interface{}

	// PGPreServer returns the server's *pgwire.PreServeConnHandler as an interface{}.
	PGPreServer() interface{}

	// DiagnosticsReporter returns the server's *diagnostics.Reporter as an
	// interface{}. The DiagnosticsReporter periodically phones home to report
	// diagnostics and usage.
	DiagnosticsReporter() interface{}

	// StatusServer returns the server's *server.SQLStatusServer as an
	// interface{}.
	StatusServer() interface{}

	// TenantStatusServer returns the server's *server.TenantStatusServer as an
	// interface{}.
	TenantStatusServer() interface{}

	// HTTPAuthServer returns the authserver.Server as an interface{}.
	HTTPAuthServer() interface{}

	// HTTPServer returns the server.httpServer as an interface{}.
	HTTPServer() interface{}

	// SQLLoopbackListener returns the *netutil.LoopbackListener as an interface{}.
	SQLLoopbackListener() interface{}

	// SQLServer returns the *sql.Server as an interface{}.
	SQLServer() interface{}

	// DistSQLServer returns the *distsql.ServerImpl as an interface{}.
	DistSQLServer() interface{}

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

	// DistSenderI returns the *kvcoord.DistSender as an interface{}.
	DistSenderI() interface{}

	// InternalDB returns an isql.DB as an interface{}.
	InternalDB() interface{}

	// InternalExecutor returns a *sql.InternalExecutor as an interface{} (which
	// also implements isql.Executor if the test cannot depend on sql).
	InternalExecutor() interface{}

	// LeaseManager returns the *sql.LeaseManager as an interface{}.
	LeaseManager() interface{}

	// JobRegistry returns the *jobs.Registry as an interface{}.
	JobRegistry() interface{}

	// RPCContext returns the *rpc.Context used by the server.
	RPCContext() *rpc.Context

	// NewClientRPCContext creates a new rpc.Context suitable to open
	// client RPC connections to the server.
	NewClientRPCContext(ctx context.Context, userName username.SQLUsername) *rpc.Context

	// RPCClientConn opens a RPC client connection to the server.
	RPCClientConn(t TestFataler, userName username.SQLUsername) *grpc.ClientConn

	// RPCClientConnE is like RPCClientConn but it allows the test to check the
	// error.
	RPCClientConnE(userName username.SQLUsername) (*grpc.ClientConn, error)

	// GetAdminClient creates a serverpb.AdminClient connection to the server.
	// Shorthand for serverpb.AdminClient(.RPCClientConn(t, "root"))
	GetAdminClient(t TestFataler) serverpb.AdminClient

	// GetStatusClient creates a serverpb.StatusClient connection to the server.
	// Shorthand for serverpb.StatusClient(.RPCClientConn(t, "root"))
	GetStatusClient(t TestFataler) serverpb.StatusClient

	// AnnotateCtx annotates a context.
	AnnotateCtx(context.Context) context.Context

	// ExecutorConfig returns a copy of the server's ExecutorConfig.
	// The real return type is sql.ExecutorConfig.
	ExecutorConfig() interface{}

	// RangeFeedFactory returns the range feed factory used by the tenant.
	// The real return type is *rangefeed.Factory.
	RangeFeedFactory() interface{}

	// ClusterSettings returns the ClusterSettings shared by all components of
	// this tenant.
	ClusterSettings() *cluster.Settings

	// SettingsWatcher returns the *settingswatcher.SettingsWatcher used by the
	// tenant server.
	SettingsWatcher() interface{}

	// AppStopper returns the stopper used by the tenant.
	AppStopper() *stop.Stopper

	// Clock returns the clock used by the tenant.
	Clock() *hlc.Clock

	// SpanConfigKVAccessor returns the underlying spanconfig.KVAccessor as an
	// interface{}.
	SpanConfigKVAccessor() interface{}

	// SpanConfigReporter returns the underlying spanconfig.Reporter as an
	// interface{}.
	SpanConfigReporter() interface{}

	// SpanConfigReconciler returns the underlying spanconfig.Reconciler as an
	// interface{}.
	SpanConfigReconciler() interface{}

	// SpanConfigSQLTranslatorFactory returns the underlying
	// spanconfig.SQLTranslatorFactory as an interface{}.
	SpanConfigSQLTranslatorFactory() interface{}

	// SpanConfigSQLWatcher returns the underlying spanconfig.SQLWatcher as an
	// interface{}.
	SpanConfigSQLWatcher() interface{}

	// TestingKnobs returns the TestingKnobs in use by the test server.
	TestingKnobs() *base.TestingKnobs

	// ExternalIODir returns ExternalIODir form the server config.
	ExternalIODir() string

	// SQLServerInternal returns the *server.SQLServer as an interface{}
	// Note: most tests should use SQLServer() and InternalExecutor() instead.
	SQLServerInternal() interface{}

	// AmbientCtx retrieves the AmbientContext for this server,
	// so that a test can instantiate additional one-off components
	// using the same context details as the server. This should not
	// be used in non-test code.
	AmbientCtx() log.AmbientContext

	// AdminURL returns the URL for the admin UI.
	AdminURL() *TestURL

	// GetUnauthenticatedHTTPClient returns an http client configured with the client TLS
	// config required by the TestServer's configuration.
	// Discourages implementer from using unauthenticated http connections
	// with verbose method name.
	GetUnauthenticatedHTTPClient() (http.Client, error)

	// GetAdminHTTPClient returns an http client which has been
	// authenticated to access Admin API methods (via a cookie).
	// The user has admin privileges.
	GetAdminHTTPClient() (http.Client, error)

	// GetAuthenticatedHTTPClient returns an http client which has been
	// authenticated to access Admin API methods (via a cookie).
	GetAuthenticatedHTTPClient(isAdmin bool, sessionType SessionType) (http.Client, error)

	// GetAuthenticatedHTTPClientAndCookie returns an http client which
	// has been authenticated to access Admin API methods and
	// the corresponding session cookie.
	GetAuthenticatedHTTPClientAndCookie(
		authUser username.SQLUsername, isAdmin bool, session SessionType,
	) (http.Client, *serverpb.SessionCookie, error)

	// GetAuthSession returns a byte array containing a valid auth
	// session.
	GetAuthSession(isAdmin bool) (*serverpb.SessionCookie, error)

	// CreateAuthUser is exported for use in tests.
	CreateAuthUser(userName username.SQLUsername, isAdmin bool) error

	// DrainClients shuts down client connections.
	DrainClients(ctx context.Context) error

	// SystemConfigProvider provides access to the system config.
	SystemConfigProvider() config.SystemConfigProvider

	// MustGetSQLCounter returns the value of a counter metric from the server's
	// SQL Executor. Runs in O(# of metrics) time, which is fine for test code.
	MustGetSQLCounter(name string) int64
	// MustGetSQLNetworkCounter returns the value of a counter metric from the
	// server's SQL server. Runs in O(# of metrics) time, which is fine for test
	// code.
	MustGetSQLNetworkCounter(name string) int64

	// Codec returns this server's codec (or keys.SystemSQLCodec if this is the
	// system tenant).
	Codec() keys.SQLCodec

	// RangeDescIteratorFactory returns the underlying rangedesc.IteratorFactory
	// as an interface{}.
	RangeDescIteratorFactory() interface{}

	// Tracer returns a reference to the server's Tracer.
	Tracer() *tracing.Tracer

	// TracerI is the same as Tracer but returns an interface{}.
	TracerI() interface{}

	// MigrationServer returns the server's migration server, which is used in
	// upgrade testing.
	MigrationServer() interface{}

	// CollectionFactory returns a *descs.CollectionFactory.
	CollectionFactory() interface{}

	// SystemTableIDResolver returns a catalog.SystemTableIDResolver.
	SystemTableIDResolver() interface{}

	// QueryDatabaseID provides access to the database name-to-ID conversion function
	// for use in API tests.
	QueryDatabaseID(
		ctx context.Context, userName username.SQLUsername, dbName string,
	) (descpb.ID, error)

	// QueryTableID provides access to the table name-to-ID conversion function
	// for use in API tests.
	QueryTableID(
		ctx context.Context, userName username.SQLUsername, dbName, tbName string,
	) (descpb.ID, error)

	// StatsForSpans provides access to the span stats inspection function
	// for use in API tests.
	StatsForSpan(
		ctx context.Context, span roachpb.Span,
	) (*serverpb.TableStatsResponse, error)

	// ForceTableGC forces a KV GC round on the key range for the given table.
	ForceTableGC(
		ctx context.Context, database, table string, timestamp hlc.Timestamp,
	) error

	// DefaultZoneConfig is a convenience function that accesses
	// .SystemConfigProvider().GetSystemConfig().DefaultZoneConfig.
	DefaultZoneConfig() zonepb.ZoneConfig

	// SetReady changes the SQL readiness.
	SetReady(bool)

	// SetAcceptSQLWithoutTLS changes the corresponding configuration parameter.
	SetAcceptSQLWithoutTLS(bool)

	// PrivilegeChecker returns the privilege checker in use by the HTTP
	// server. The concrete return value is of type
	// privchecker.SQLPrivilegeChecker (interface).
	PrivilegeChecker() interface{}

	// NodeDescStoreI returns the node descriptor lookup interface.
	// The concrete return type is compatible with interface kvclient.NodeDescStore.
	NodeDescStoreI() interface{}

	// Locality returns the locality used by the server.
	Locality() roachpb.Locality

	// DistSQLPlanningNodeID returns the NodeID to use by the DistSQL span resolver.
	DistSQLPlanningNodeID() roachpb.NodeID

	// DeploymentMode returns the deployment mode of the underlying server or
	// tenant, which can be single-tenant (system-only), shared-process, or
	// external-process.
	DeploymentMode() DeploymentMode
}

// TenantControlInterface defines the API of a test server that can
// start the SQL and HTTP service for secondary tenants (virtual
// clusters).
type TenantControlInterface interface {
	// StartSharedProcessTenant starts the service for a virtual cluster
	// using the special configuration we define for shared-process deployments.
	//
	// args.TenantName must be specified. If a tenant with that name already
	// exists, its ID is checked against args.TenantID (if set), and, if it
	// matches, new tenant metadata is not created in the system.tenants table.
	//
	// See also StartTenant(), which starts a tenant mimicking out-of-process tenant
	// servers.
	//
	// TODO(knz): fold with StartTenant below.
	StartSharedProcessTenant(
		ctx context.Context, args base.TestSharedProcessTenantArgs,
	) (ApplicationLayerInterface, *gosql.DB, error)

	// StartTenant starts the service for a virtual cluster using the special
	// configuration we define for separate-process deployments. This incidentally
	// is also the configuration we use in CC Serverless.
	//
	// TODO(knz): Rename this to StartApplicationService. Take the
	// deployment mode as parameter instead of using a separate method.
	StartTenant(ctx context.Context, params base.TestTenantArgs) (ApplicationLayerInterface, error)

	// DisableStartTenant prevents calls to StartTenant(). If an attempt
	// is made, the server will return the specified error.
	DisableStartTenant(reason error)

	// WaitForTenantReadiness waits until the tenant record is known
	// to the in-RAM caches. Trying to start a tenant server before
	// this is called can run into a "missing record" error even
	// if the tenant record exists in KV.
	WaitForTenantReadiness(ctx context.Context, tenantID roachpb.TenantID) error

	// GrantTenantCapabilities grants a capability to a tenant and waits until the
	// in-memory cache reflects the change.
	//
	// Note: There is no need to call WaitForTenantCapabilities separately.
	GrantTenantCapabilities(
		context.Context,
		roachpb.TenantID,
		map[tenantcapabilities.ID]string,
	) error

	// WaitForTenantCapabilities waits until the in-RAM cache of
	// tenant capabilities has been populated for the given tenant ID
	// with the expected target capability values.
	WaitForTenantCapabilities(
		ctx context.Context,
		tenID roachpb.TenantID,
		targetCaps map[tenantcapabilities.ID]string,
		errPrefix string,
	) error

	// TestTenant returns the test tenant associated with the server.
	//
	// TODO(knz): rename to TestApplicationService.
	TestTenant() ApplicationLayerInterface

	// StartedDefaultTestTenant returns true if the server has started
	// the service for the default test tenant.
	StartedDefaultTestTenant() bool

	// ServerController returns the *server.serverController as an interface{}
	ServerController() interface{}
}

// StorageLayerInterface defines accessors to the storage layer of a
// test server. See ApplicationLayerInterface for the relevant
// application-level APIs.
type StorageLayerInterface interface {
	// Node returns the server.Node as an interface{}.
	Node() interface{}

	// NodeID returns the ID of this node within its cluster.
	NodeID() roachpb.NodeID

	// StorageClusterID returns the storage cluster ID as understood by
	// this node in the cluster.
	StorageClusterID() uuid.UUID

	// GossipI returns the gossip used by the TestServer.
	// The real return type is *gossip.Gossip.
	GossipI() interface{}

	// SQLLivenessProvider returns the sqlliveness.Provider as an interface{}.
	SQLLivenessProvider() interface{}

	// NodeLiveness exposes the NodeLiveness instance used by the TestServer as an
	// interface{}.
	NodeLiveness() interface{}

	// HeartbeatNodeLiveness heartbeats the server's NodeLiveness record.
	HeartbeatNodeLiveness() error

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

	// SplitRangeWithExpiration splits the range containing splitKey with a sticky
	// bit expiring at expirationTime.
	// The right range created by the split starts at the split key and extends to the
	// original range's end key.
	// Returns the new descriptors of the left and right ranges.
	//
	// splitKey must correspond to a SQL table key (it must end with a family ID /
	// col ID).
	SplitRangeWithExpiration(
		splitKey roachpb.Key, expirationTime hlc.Timestamp,
	) (roachpb.RangeDescriptor, roachpb.RangeDescriptor, error)

	// MergeRanges merges the range containing leftKey with the following adjacent
	// range.
	MergeRanges(leftKey roachpb.Key) (merged roachpb.RangeDescriptor, err error)

	// LookupRange looks up the range descriptor which contains key.
	LookupRange(key roachpb.Key) (roachpb.RangeDescriptor, error)

	// ExpectedInitialRangeCount returns the expected number of ranges that should
	// be on the server after initial (asynchronous) splits have been completed,
	// assuming no additional information is added outside of the normal bootstrap
	// process.
	ExpectedInitialRangeCount() (int, error)

	// UpdateChecker returns the server's *diagnostics.UpdateChecker as an
	// interface{}. The UpdateChecker periodically phones home to check for new
	// updates that are available.
	UpdateChecker() interface{}

	// ScratchRange splits off a range suitable to be used as KV scratch space.
	// (it doesn't overlap system spans or SQL tables).
	//
	// Calling this multiple times is undefined (but see
	// TestCluster.ScratchRange() which is idempotent).
	ScratchRange() (roachpb.Key, error)

	// ScratchRangeEx splits off a range suitable to be used as KV scratch space.
	// (it doesn't overlap system spans or SQL tables).
	ScratchRangeEx() (roachpb.RangeDescriptor, roachpb.RangeDescriptor, error)

	// ScratchRangeWithExpirationLease is like ScratchRange but the
	// range has an expiration lease.
	ScratchRangeWithExpirationLease() (roachpb.Key, error)

	// ScratchRangeWithExpirationLeaseEx is like ScratchRangeEx but the
	// range has an expiration lease.
	ScratchRangeWithExpirationLeaseEx() (
		roachpb.RangeDescriptor,
		roachpb.RangeDescriptor,
		error)

	// Engines returns the TestServer's engines.
	Engines() []storage.Engine

	// MetricsRecorder periodically records node-level and store-level metrics.
	MetricsRecorder() *status.MetricsRecorder

	// SpanConfigKVSubscriber returns the embedded spanconfig.KVSubscriber for
	// the server.
	SpanConfigKVSubscriber() interface{}

	// KVFlowController returns the embedded kvflowcontrol.Controller for the
	// server.
	KVFlowController() interface{}

	// KVFlowHandles returns the embedded kvflowcontrol.Handles for the server.
	KVFlowHandles() interface{}

	// KvProber returns a *kvprober.Prober, which is useful when asserting the
	// correctness of the prober from integration tests.
	KvProber() *kvprober.Prober

	// RaftTransport returns access to the raft transport.
	// The return value is of type *kvserver.RaftTransport.
	RaftTransport() interface{}

	// StoreLivenessTransport provides access to the store liveness transport.
	// The return value is of type *storeliveness.Transport.
	StoreLivenessTransport() interface{}

	// GetRangeLease returns information on the lease for the range
	// containing key, and a timestamp taken from the node. The lease is
	// returned regardless of its status.
	//
	// queryPolicy specifies if its OK to forward the request to a
	// different node.
	GetRangeLease(
		ctx context.Context, key roachpb.Key, queryPolicy roachpb.LeaseInfoOpt,
	) (_ roachpb.LeaseInfo, now hlc.ClockTimestamp, _ error)

	// TenantCapabilitiesReader retrieves a reference to the
	// capabilities reader.
	TenantCapabilitiesReader() tenantcapabilities.Reader

	// TsDB returns the ts.DB instance used by the TestServer.
	TsDB() interface{}

	// DefaultSystemZoneConfig returns the internal system zone config
	// for the server.
	// Note: most tests should instead use the .DefaultZoneConfig() method
	// on ApplicationLayerInterface.
	DefaultSystemZoneConfig() zonepb.ZoneConfig

	// DecommissionPreCheck is used to evaluate if nodes are ready for decommission.
	DecommissionPreCheck(
		ctx context.Context,
		nodeIDs []roachpb.NodeID,
		strictReadiness bool,
		collectTraces bool,
		maxErrors int,
	) (decommissioning.PreCheckResult, error)

	// RaftConfig retrieves a copy of the raft configuration.
	RaftConfig() base.RaftConfig
}

// TestServerFactory encompasses the actual implementation of the shim
// service.
type TestServerFactory interface {
	// New instantiates a test server.
	New(params base.TestServerArgs) (interface{}, error)
}

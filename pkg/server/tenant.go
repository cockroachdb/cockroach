// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/inspectz"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvtenant"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptprovider"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptreconcile"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/multitenantcpu"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiesauthorizer"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/apiutil"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/privchecker"
	"github.com/cockroachdb/cockroach/pkg/server/serverctl"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/server/structlogging"
	"github.com/cockroachdb/cockroach/pkg/server/systemconfigwatcher"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfiglimiter"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/optionalnodeliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionprotectedts"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logmetrics"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/schedulerlatency"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
	sentry "github.com/getsentry/sentry-go"
)

// SQLServerWrapper is a utility struct that encapsulates
// a SQLServer and its helpers that make it a networked service.
type SQLServerWrapper struct {
	// NB: This struct definition mirrors that of Server.
	// The fields are kept in a similar order to make their comparison easier
	// during reviews.
	//
	// TODO(knz): Find a way to merge these two togethers so there is just
	// one implementation.

	cfg        *BaseConfig
	clock      *hlc.Clock
	rpcContext *rpc.Context
	// The gRPC server on which the different RPC handlers will be registered.
	grpc         *grpcServer
	kvNodeDialer *nodedialer.Dialer
	db           *kv.DB

	// Metric registries.
	// See the explanatory comments in server.go and status/recorder.g o
	// for details.
	registry    *metric.Registry
	sysRegistry *metric.Registry

	recorder *status.MetricsRecorder
	runtime  *status.RuntimeStatSampler

	http            *httpServer
	adminAuthzCheck privchecker.CheckerForRPCHandlers
	tenantAdmin     *adminServer
	tenantStatus    *statusServer
	drainServer     *drainServer
	authentication  authserver.Server
	stopper         *stop.Stopper

	debug *debug.Server

	// pgL is the SQL listener.
	pgL net.Listener
	// loopbackPgL is the SQL listener for internal pgwire connections.
	loopbackPgL *netutil.LoopbackListener

	// pgPreServer handles SQL connections prior to routing them to a
	// specific tenant.
	pgPreServer *pgwire.PreServeConnHandler

	sqlServer *SQLServer
	sqlCfg    *SQLConfig

	// Created in NewServer but initialized (made usable) in `(*Server).PreStart`.
	externalStorageBuilder *externalStorageBuilder

	// Used for multi-tenant cost control (on the tenant side).
	costController multitenant.TenantSideCostController

	// promRuleExporter is used by the tenant to expose the prometheus rules.
	promRuleExporter *metric.PrometheusRuleExporter

	tenantTimeSeries *ts.TenantServer
}

// Drain idempotently activates the draining mode.
// Note: new code should not be taught to use this method
// directly. Use the Drain() RPC instead with a suitably crafted
// DrainRequest.
//
// On failure, the system may be in a partially drained
// state; the client should either continue calling Drain() or shut
// down the server.
//
// The reporter function, if non-nil, is called for each
// packet of load shed away from the server during the drain.
//
// TODO(knz): This method is currently exported for use by the
// shutdown code in cli/start.go; however, this is a mis-design. The
// start code should use the Drain() RPC like quit does.
func (s *SQLServerWrapper) Drain(
	ctx context.Context, verbose bool,
) (remaining uint64, info redact.RedactableString, err error) {
	return s.drainServer.runDrain(ctx, verbose)
}

// tenantServerDeps holds dependencies for the SQL server that we want
// to vary based on whether we are in a shared process or separate
// process tenant.
type tenantServerDeps struct {
	instanceIDContainer *base.SQLIDContainer
	nodeIDGetter        func() roachpb.NodeID

	// The following should eventually be connected to tenant
	// capabilities.
	costControllerFactory costControllerFactory
	spanLimiterFactory    spanLimiterFactory
}

type spanLimiterFactory func(isql.Executor, *cluster.Settings, *spanconfig.TestingKnobs) spanconfig.Limiter
type costControllerFactory func(
	*cluster.Settings,
	roachpb.TenantID,
	kvtenant.TokenBucketProvider,
	kvclient.NodeDescStore,
	roachpb.Locality,
) (multitenant.TenantSideCostController, error)

// NewSeparateProcessTenantServer creates a tenant-specific, SQL-only
// server against a KV backend, with defaults appropriate for a
// SQLServer that is not located in the same process as a KVServer.
//
// The caller is responsible for listening to the server's ShutdownRequested()
// channel and stopping cfg.stopper when signaled.
func NewSeparateProcessTenantServer(
	ctx context.Context,
	stopper *stop.Stopper,
	baseCfg BaseConfig,
	sqlCfg SQLConfig,
	tenantNameContainer *roachpb.TenantNameContainer,
) (*SQLServerWrapper, error) {
	deps := tenantServerDeps{
		instanceIDContainer: baseCfg.IDContainer.SwitchToSQLIDContainerForStandaloneSQLInstance(),
		// The kvcoord.DistSender uses the node ID to preferentially route
		// requests to a local replica (if one exists). In separate-process
		// mode, not knowing the node ID, and thus not being able to take
		// advantage of this optimization is okay, given tenants not running
		// in-process with KV instances have no such optimization to take
		// advantage of to begin with.
		nodeIDGetter:          nil,
		costControllerFactory: NewTenantSideCostController,
		spanLimiterFactory: func(ie isql.Executor, st *cluster.Settings, knobs *spanconfig.TestingKnobs) spanconfig.Limiter {
			return spanconfiglimiter.New(ie, st, knobs)
		},
	}

	return newTenantServer(ctx, stopper, baseCfg, sqlCfg, tenantNameContainer, deps, mtinfopb.ServiceModeExternal)
}

// newSharedProcessTenantServer creates a tenant-specific, SQL-only
// server against a KV backend, with defaults appropriate for a
// SQLServer that is not located in the same process as a KVServer.
//
// The caller is responsible for listening to the server's ShutdownRequested()
// channel and stopping cfg.stopper when signaled.
func newSharedProcessTenantServer(
	ctx context.Context,
	stopper *stop.Stopper,
	baseCfg BaseConfig,
	sqlCfg SQLConfig,
	tenantNameContainer *roachpb.TenantNameContainer,
) (*SQLServerWrapper, error) {
	if baseCfg.IDContainer.Get() == 0 {
		return nil, errors.AssertionFailedf("programming error: NewSharedProcessTenantServer called before NodeID was assigned.")
	}

	deps := tenantServerDeps{
		instanceIDContainer: base.NewSQLIDContainerForNode(baseCfg.IDContainer),
		// The kvcoord.DistSender uses the node ID to preferentially route
		// requests to a local replica (if one exists). In shared-process mode
		// we can easily provide that without accessing the gossip.
		nodeIDGetter: baseCfg.IDContainer.Get,
		// TODO(ssd): The cost controller should instead be able to
		// read from the capability system and return immediately if
		// the tenant is exempt. For now we are turning off the
		// tenant-side cost controller for shared-memory tenants until
		// we have the abilility to read capabilities tenant-side.
		//
		// https://github.com/cockroachdb/cockroach/issues/84586
		costControllerFactory: NewNoopTenantSideCostController,
		spanLimiterFactory: func(isql.Executor, *cluster.Settings, *spanconfig.TestingKnobs) spanconfig.Limiter {
			return spanconfiglimiter.NoopLimiter{}
		},
	}
	return newTenantServer(ctx, stopper, baseCfg, sqlCfg, tenantNameContainer, deps, mtinfopb.ServiceModeShared)
}

// newTenantServer constructs a SQLServerWrapper.
//
// The tenant's metrics registry is registered with parentRecorder, if not nil.
func newTenantServer(
	ctx context.Context,
	stopper *stop.Stopper,
	baseCfg BaseConfig,
	sqlCfg SQLConfig,
	tenantNameContainer *roachpb.TenantNameContainer,
	deps tenantServerDeps,
	serviceMode mtinfopb.TenantServiceMode,
) (*SQLServerWrapper, error) {
	// TODO(knz): Make the license application a per-server thing
	// instead of a global thing.
	err := ApplyTenantLicense()
	if err != nil {
		return nil, err
	}

	// Start the SQL listener early so any delay that happen from this point onward
	// (until the server is ready) won't cause client connections to be rejected.
	if baseCfg.SplitListenSQL && !baseCfg.DisableSQLListener {
		sqlAddrListener, err := ListenAndUpdateAddrs(
			ctx, &baseCfg.SQLAddr, &baseCfg.SQLAdvertiseAddr, "sql", baseCfg.AcceptProxyProtocolHeaders)
		if err != nil {
			return nil, err
		}
		baseCfg.SQLAddrListener = sqlAddrListener
	}

	// The setting of tenant id may have not been done until now. If this is the
	// case, DelayedSetTenantID will be set and should be used to populate
	// TenantID in the config. We call it here as we need a valid TenantID below.
	if sqlCfg.DelayedSetTenantID != nil {
		cfgTenantID, cfgLocality, err := sqlCfg.DelayedSetTenantID(ctx)
		if err != nil {
			return nil, err
		}
		// We need to update sqlCfg and baseCfg here explicitly since copies
		// were passed into newTenantServer instead of the original serverCfg
		// object.
		sqlCfg.TenantID = cfgTenantID
		baseCfg.Locality = cfgLocality
	}
	log.Ops.Infof(ctx, "server starting for tenant %q", redact.Safe(sqlCfg.TenantID))
	// Inform the server identity provider that we're operating
	// for a tenant server.
	//
	// TODO(#77336): we would like to set the tenant name here too.
	// Unfortunately, this is not possible for now because the name is
	// only known after the SQL server has initialized the connector (in
	// preStart), which cannot be called yet.
	// Instead, the tenant name is currently added to the idProvider
	// inside preStart().
	// The better approach would be to have the CLI flag use a name,
	// then rely on some mechanism to retrieve the ID from the name to
	// initialize the rest of the server.
	baseCfg.idProvider.SetTenantID(sqlCfg.TenantID)
	args, err := makeTenantSQLServerArgs(ctx, stopper, baseCfg, sqlCfg, tenantNameContainer, deps, serviceMode)
	if err != nil {
		return nil, err
	}
	err = args.ValidateAddrs(ctx)
	if err != nil {
		return nil, err
	}

	// The following initialization mirrors that of NewServer().
	// Please keep them in sync.
	// Instantiate the API privilege checker.
	//
	// TODO(tbg): give adminServer only what it needs (and avoid circular deps).
	adminAuthzCheck := privchecker.NewChecker(args.circularInternalExecutor, args.Settings)

	// Instantiate the HTTP server.
	// These callbacks help us avoid a dependency on gossip in httpServer.
	parseNodeIDFn := func(s string) (roachpb.NodeID, bool, error) {
		return roachpb.NodeID(0), false, errors.New("tenants cannot proxy to KV Nodes")
	}
	getNodeIDHTTPAddressFn := func(id roachpb.NodeID) (*util.UnresolvedAddr, roachpb.Locality, error) {
		return nil, roachpb.Locality{}, errors.New("tenants cannot proxy to KV Nodes")
	}
	sHTTP := newHTTPServer(baseCfg, args.rpcContext, parseNodeIDFn, getNodeIDHTTPAddressFn)

	// This is where we would be instantiating the SQL session registry
	// in NewServer().
	// This is currently performed in makeTenantSQLServerArgs().

	// Instantiate the cache of closed SQL sessions.
	closedSessionCache := sql.NewClosedSessionCache(
		baseCfg.Settings, args.monitorAndMetrics.rootSQLMemoryMonitor, time.Now)
	args.closedSessionCache = closedSessionCache

	// Instantiate the serverIterator to provide fanout to SQL instances. The
	// serverIterator needs access to sqlServer which is assigned below once we
	// have an instance.
	serverIterator := &tenantFanoutClient{
		sqlServer: nil,
		rpcCtx:    args.rpcContext,
		stopper:   args.stopper,
	}

	// Instantiate the status API server. The statusServer needs access to the
	// sqlServer, but we also need the same object to set up the sqlServer. So
	// construct the status server with a nil sqlServer, and then assign it once
	// an SQL server gets created. We are going to assume that the status server
	// won't require the SQL server object until later.
	var serverKnobs TestingKnobs
	if s, ok := baseCfg.TestingKnobs.Server.(*TestingKnobs); ok {
		serverKnobs = *s
	}
	sStatus := newStatusServer(
		baseCfg.AmbientCtx,
		baseCfg.Settings,
		baseCfg.Config,
		adminAuthzCheck,
		args.db,
		args.recorder,
		args.rpcContext,
		stopper,
		args.sessionRegistry,
		closedSessionCache,
		args.remoteFlowRunner,
		args.circularInternalExecutor,
		serverIterator,
		args.clock,
		&serverKnobs,
	)
	args.sqlStatusServer = sStatus

	// This is the location in NewServer() where we would be configuring
	// the path to the special file that blocks background jobs.
	// This should probably done here.
	// See: https://github.com/cockroachdb/cockroach/issues/90524

	// This is the location in NewServer() where we would be creating
	// the eventsExporter. This is currently performed in
	// makeTenantSQLServerArgs().

	var pgPreServer *pgwire.PreServeConnHandler
	if !baseCfg.DisableSQLListener {
		// Initialize the pgwire pre-server, which initializes connections,
		// sets up TLS and reads client status parameters.
		pgPreServer = pgwire.NewPreServeConnHandler(
			baseCfg.AmbientCtx,
			baseCfg.Config,
			args.Settings,
			args.rpcContext.GetServerTLSConfig,
			baseCfg.HistogramWindowInterval(),
			args.monitorAndMetrics.rootSQLMemoryMonitor,
			false, /* acceptTenantName */
		)
		for _, m := range pgPreServer.Metrics() {
			args.registry.AddMetricStruct(m)
		}
	}

	// NB: On a shared process tenant, we start cidr once per tenant.
	// Potentially we could share this across tenants, but this breaks the
	// tenant separation model. For a small number of tenants this is OK, but if
	// we have a large number of tenants in shared process mode this could be a
	// problem from a memory and network perspective.
	if err = baseCfg.CidrLookup.Start(ctx, stopper); err != nil {
		return nil, err
	}

	// Instantiate the SQL server proper.
	sqlServer, err := newSQLServer(ctx, args)
	if err != nil {
		return nil, err
	}

	// Instantiate the migration API server.
	tms := newTenantMigrationServer(sqlServer)
	serverpb.RegisterMigrationServer(args.grpc.Server, tms)
	sqlServer.migrationServer = tms // only for testing via testTenant

	// Tell the authz server how to connect to SQL.
	adminAuthzCheck.SetAuthzAccessorFactory(func(opName redact.SafeString) (sql.AuthorizationAccessor, func()) {
		// This is a hack to get around a Go package dependency cycle. See comment
		// in sql/jobs/registry.go on planHookMaker.
		txn := args.db.NewTxn(ctx, "check-system-privilege")
		p, cleanup := sql.NewInternalPlanner(
			opName,
			txn,
			username.NodeUserName(),
			&sql.MemoryMetrics{},
			sqlServer.execCfg,
			sql.NewInternalSessionData(ctx, sqlServer.execCfg.Settings, opName),
		)
		return p.(sql.AuthorizationAccessor), cleanup
	})

	// Create the authentication RPC server (login/logout).
	sAuth := authserver.NewServer(baseCfg.Config, sqlServer)

	// Create a drain server.
	drainServer := newDrainServer(baseCfg, args.stopper, args.stopTrigger, args.grpc, sqlServer)

	// Instantiate the admin API server.
	sAdmin := newAdminServer(
		sqlServer,
		args.Settings,
		adminAuthzCheck,
		sqlServer.internalExecutor,
		args.BaseConfig.AmbientCtx,
		args.recorder,
		args.db,
		args.rpcContext,
		serverIterator,
		args.clock,
		args.distSender,
		args.grpc,
		drainServer,
	)

	// Connect the various servers to RPC.
	for _, gw := range []grpcGatewayServer{sAdmin, sStatus, sAuth, args.tenantTimeSeriesServer} {
		gw.RegisterService(args.grpc.Server)
	}

	// Tell the status/admin servers how to access SQL structures.
	sStatus.setStmtDiagnosticsRequester(sqlServer.execCfg.StmtDiagnosticsRecorder)
	serverIterator.sqlServer = sqlServer
	sStatus.baseStatusServer.sqlServer = sqlServer
	sAdmin.sqlServer = sqlServer

	var processCapAuthz tenantcapabilities.Authorizer = &tenantcapabilitiesauthorizer.AllowEverythingAuthorizer{}
	if lsi := sqlCfg.LocalKVServerInfo; lsi != nil {
		processCapAuthz = lsi.SameProcessCapabilityAuthorizer
	}

	// Create the debug API server.
	debugServer := debug.NewServer(
		baseCfg.AmbientCtx,
		args.Settings,
		sqlServer.pgServer.HBADebugFn(),
		sqlServer.execCfg.SQLStatusServer,
		sqlCfg.TenantID,
		processCapAuthz,
	)

	return &SQLServerWrapper{
		cfg: args.BaseConfig,

		clock:      args.clock,
		rpcContext: args.rpcContext,

		grpc:         args.grpc,
		kvNodeDialer: args.kvNodeDialer,
		db:           args.db,
		registry:     args.registry,
		sysRegistry:  args.sysRegistry,
		recorder:     args.recorder,
		runtime:      args.runtime,

		http:            sHTTP,
		adminAuthzCheck: adminAuthzCheck,
		tenantAdmin:     sAdmin,
		tenantStatus:    sStatus,
		drainServer:     drainServer,
		authentication:  sAuth,
		stopper:         args.stopper,

		debug: debugServer,

		pgPreServer: pgPreServer,

		sqlServer: sqlServer,
		sqlCfg:    args.SQLConfig,

		externalStorageBuilder: args.externalStorageBuilder,
		costController:         args.costController,
		promRuleExporter:       args.promRuleExporter,
		tenantTimeSeries:       args.tenantTimeSeriesServer,
	}, nil
}

// PreStart starts the server on the specified port(s) and
// initializes subsystems.
//
// It does not activate the pgwire listener over the network / unix
// socket, which is done by the AcceptClients() method. The separation
// between the two exists so that SQL initialization can take place
// before the first client is accepted.
func (s *SQLServerWrapper) PreStart(ctx context.Context) error {
	// NB: This logic mirrors the relevants bits in (*Server).PreStart.
	// They should be kept in sync.
	// We also use the same order so they can be positioned side-by-side
	// for easier comparison during reviews.
	//
	// TODO(knz): Find a way to combine this common logic for both methods.

	// Start a context for the asynchronous network workers.
	workersCtx := s.AnnotateCtx(context.Background())

	if !s.sqlServer.cfg.Insecure {
		cm, err := s.rpcContext.GetCertificateManager()
		if err != nil {
			return err
		}
		// Ensure that SIGHUP will make this cert manager reload its certs
		// from disk.
		if err := cm.RegisterSignalHandler(workersCtx, s.stopper); err != nil {
			return err
		}
	}

	// If DisableHTTPListener is set, we are relying on the HTTP request
	// routing performed by the serverController.
	if !s.sqlServer.cfg.DisableHTTPListener {
		// Load the TLS configuration for the HTTP server.
		uiTLSConfig, err := s.rpcContext.GetUIServerTLSConfig()
		if err != nil {
			return err
		}

		// Start the admin UI server. This opens the HTTP listen socket,
		// optionally sets up TLS, and dispatches the server worker for the
		// web UI.
		if err := startHTTPService(ctx, workersCtx, s.sqlServer.cfg, uiTLSConfig, s.stopper, s.http.baseHandler); err != nil {
			return err
		}
	}

	// Start the RPC server. This opens the RPC/SQL listen socket,
	// and dispatches the server worker for the RPC.
	// The SQL listener is returned, to start the SQL server later
	// below when the server has initialized.
	enableSQLListener := !s.sqlServer.cfg.DisableSQLListener
	lf := ListenAndUpdateAddrs
	if s.sqlServer.cfg.RPCListenerFactory != nil {
		lf = s.sqlServer.cfg.RPCListenerFactory
	}

	pgL, loopbackPgL, rpcLoopbackDialFn, startRPCServer, err := startListenRPCAndSQL(ctx, workersCtx, *s.sqlServer.cfg, s.stopper, s.grpc, lf, enableSQLListener, s.cfg.AcceptProxyProtocolHeaders)
	if err != nil {
		return err
	}
	if enableSQLListener {
		s.pgL = pgL
	}
	s.loopbackPgL = loopbackPgL

	// Tell the RPC context how to connect in-memory.
	s.rpcContext.SetLoopbackDialer(rpcLoopbackDialFn)

	// NB: This is where (*Server).PreStart() reports the listener readiness
	// via testing knobs: PauseAfterGettingRPCAddress, SignalAfterGettingRPCAddress.
	// As of this writing, only `cockroach demo` uses those, to coordinate
	// the initialization of the demo cluster. We do not need this logic
	// in secondary tenants.

	// Initialize grpc-gateway mux and context in order to get the /health
	// endpoint working even before the node has fully initialized.
	gwMux, gwCtx, conn, err := configureGRPCGateway(
		ctx,
		workersCtx,
		s.sqlServer.cfg.AmbientCtx,
		s.rpcContext,
		s.stopper,
		s.grpc,
		s.sqlServer.cfg.AdvertiseAddr,
	)
	if err != nil {
		return err
	}

	// Connect the various RPC handlers to the gRPC gateway.
	for _, gw := range []grpcGatewayServer{s.tenantAdmin, s.tenantStatus, s.authentication, s.tenantTimeSeries} {
		if err := gw.RegisterGateway(gwCtx, gwMux, conn); err != nil {
			return err
		}
	}

	// Handle /health early. This is necessary for orchestration.  Note
	// that /health is not authenticated, on purpose. This is both
	// because it needs to be available before the cluster is up and can
	// serve authentication requests, and also because it must work for
	// monitoring tools which operate without authentication.
	s.http.handleHealth(gwMux)

	// Write listener info files early in the startup sequence. `listenerInfo` has a comment.
	listenerFiles := listenerInfo{
		listenRPC:    s.sqlServer.cfg.Addr,
		advertiseRPC: s.sqlServer.cfg.AdvertiseAddr,
		listenSQL:    s.sqlServer.cfg.SQLAddr,
		advertiseSQL: s.sqlServer.cfg.SQLAdvertiseAddr,
		listenHTTP:   s.sqlServer.cfg.HTTPAdvertiseAddr,
	}.Iter()

	encryptedStore := false
	for _, storeSpec := range s.sqlServer.cfg.Stores.Specs {
		if storeSpec.InMemory {
			continue
		}
		if storeSpec.IsEncrypted() {
			encryptedStore = true
		}

		for name, val := range listenerFiles {
			file := filepath.Join(storeSpec.Path, name)
			if err := os.WriteFile(file, []byte(val), 0644); err != nil {
				return errors.Wrapf(err, "failed to write %s", file)
			}
		}
		// TODO(knz): Do we really want to write the listener files
		// in _every_ store directory? Not just the first one?
	}

	// Set up calling s.cfg.ReadyFn at the right time. Essentially, this call
	// determines when `./cockroach [...] --background` returns.
	var onSuccessfulReturnFn func()
	{
		readyFn := func(bool) {}
		if s.sqlServer.cfg.ReadyFn != nil {
			readyFn = s.sqlServer.cfg.ReadyFn
		}
		onSuccessfulReturnFn = func() { readyFn(false /* waitForInit */) }
	}

	// This opens the main listener.
	startRPCServer(workersCtx)

	// Ensure components in the DistSQLPlanner that rely on the node ID are
	// initialized before store startup continues.
	s.sqlServer.execCfg.DistSQLPlanner.ConstructAndSetSpanResolver(ctx, 0 /* NodeID */, s.sqlServer.execCfg.Locality)

	// Start measuring the Go scheduler latency.
	if err := schedulerlatency.StartSampler(
		workersCtx, s.sqlServer.cfg.Settings, s.stopper, s.sysRegistry, base.DefaultMetricsSampleInterval,
		nil, /* listener */
	); err != nil {
		return err
	}

	// TODO(knz): This is the point where we could call
	// checkHLCUpperBoundExistsAndEnsureMonotonicity(). Why is this not
	// needed?

	// Record a walltime that is lower than the lowest hlc timestamp this current
	// instance of the node can use. We do not use startTime because it is lower
	// than the timestamp used to create the bootstrap schema.
	//
	// TODO(tbg): clarify the contract here and move closer to usage if possible.
	orphanedLeasesTimeThresholdNanos := s.clock.Now().WallTime

	// Signal server readiness to the caller.
	onSuccessfulReturnFn()

	// Configure the Sentry reporter to add some additional context to reports.
	//
	// NB: In (*Server).PreStart(), we can also configure the cluster ID
	// and node ID in Sentry reports as early as this point.
	// However, for a secondary tenant we must wait on sqlServer.preStart()
	// to add this information. See below.
	sentry.ConfigureScope(func(scope *sentry.Scope) {
		scope.SetTags(map[string]string{
			"encrypted_store": strconv.FormatBool(encryptedStore),
		})
	})

	// Init a log metrics registry.
	logRegistry := logmetrics.NewRegistry()
	if logRegistry == nil {
		panic(errors.AssertionFailedf("nil log metrics registry at server startup"))
	}

	// We can now connect the metric registries to the recorder.
	s.recorder.AddNode(
		metric.NewRegistry(), // node registry -- unused here
		s.registry,
		logRegistry, s.sysRegistry,
		roachpb.NodeDescriptor{
			NodeID: s.rpcContext.NodeID.Get(),
		},
		timeutil.Now().UnixNano(),
		s.sqlServer.cfg.AdvertiseAddr,
		s.sqlServer.cfg.HTTPAdvertiseAddr,
		s.sqlServer.cfg.SQLAdvertiseAddr,
	)
	// If there's a higher-level recorder, we link our metrics registry to it.
	if s.sqlCfg.NodeMetricsRecorder != nil {
		s.sqlCfg.NodeMetricsRecorder.AddTenantRegistry(s.sqlCfg.TenantID, s.registry)
		s.stopper.AddCloser(stop.CloserFn(func() {
			s.sqlCfg.NodeMetricsRecorder.RemoveTenantRegistry(s.sqlCfg.TenantID)
		}))
	} else {
		// Export statistics to graphite, if enabled by configuration. We only do
		// this if there isn't a higher-level recorder; if there is, that one takes
		// responsibility for exporting to Graphite.
		var graphiteOnce sync.Once
		graphiteEndpoint.SetOnChange(&s.ClusterSettings().SV, func(context.Context) {
			if graphiteEndpoint.Get(&s.ClusterSettings().SV) != "" {
				graphiteOnce.Do(func() {
					startGraphiteStatsExporter(workersCtx, s.stopper, s.recorder, s.ClusterSettings())
				})
			}
		})
	}

	if !s.sqlServer.cfg.DisableRuntimeStatsMonitor {
		// Begin recording runtime statistics.
		if err := startSampleEnvironment(workersCtx,
			s.sqlServer.cfg,
			0, /* pebbleCacheSize */
			s.stopper,
			s.runtime,
			s.tenantStatus.sessionRegistry,
			s.sqlServer.execCfg.RootMemoryMonitor,
		); err != nil {
			return err
		}
	}

	// After setting modeOperational, we can block until all stores are fully
	// initialized.
	s.grpc.setMode(modeOperational)

	// Report server listen addresses to logs.
	log.Ops.Infof(ctx, "starting %s server at %s (use: %s)",
		redact.Safe(s.sqlServer.cfg.HTTPRequestScheme()),
		log.SafeManaged(s.sqlServer.cfg.HTTPAddr),
		log.SafeManaged(s.sqlServer.cfg.HTTPAdvertiseAddr))
	rpcConnType := redact.SafeString("grpc/postgres")
	if s.sqlServer.cfg.SplitListenSQL {
		rpcConnType = "grpc"
		log.Ops.Infof(ctx, "starting postgres server at %s (use: %s)",
			log.SafeManaged(s.sqlServer.cfg.SQLAddr),
			log.SafeManaged(s.sqlServer.cfg.SQLAdvertiseAddr))
	}
	log.Ops.Infof(ctx, "starting %s server at %s", log.SafeManaged(rpcConnType), log.SafeManaged(s.sqlServer.cfg.Addr))
	log.Ops.Infof(ctx, "advertising SQL server node at %s", log.SafeManaged(s.sqlServer.cfg.AdvertiseAddr))

	log.Event(ctx, "accepting connections")

	// Start the SQL subsystem.
	if err := s.sqlServer.preStart(
		workersCtx,
		s.stopper,
		s.sqlServer.cfg.TestingKnobs,
		orphanedLeasesTimeThresholdNanos,
	); err != nil {
		return err
	}

	// Connect the HTTP endpoints. This also wraps the privileged HTTP
	// endpoints served by gwMux by the HTTP cookie authentication
	// check.
	// NB: This must occur after sqlServer.preStart() which initializes
	// the cluster version from storage as the http auth server relies on
	// the cluster version being initialized.
	if err := s.http.setupRoutes(ctx,
		s.authentication,  /* authnServer */
		s.adminAuthzCheck, /* adminAuthzCheck */
		s.recorder,        /* metricSource */
		s.runtime,         /* runtimeStatsSampler */
		gwMux,             /* handleRequestsUnauthenticated */
		s.debug,           /* handleDebugUnauthenticated */
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			apiutil.WriteJSONResponse(r.Context(), w, http.StatusNotImplemented, nil)
		}),
		newAPIV2Server(workersCtx, &apiV2ServerOpts{
			admin:            s.tenantAdmin,
			status:           s.tenantStatus,
			promRuleExporter: s.promRuleExporter,
			sqlServer:        s.sqlServer,
			db:               s.db,
		}), /* apiServer */
		serverpb.FeatureFlags{
			CanViewKvMetricDashboards: s.rpcContext.TenantID.Equal(roachpb.SystemTenantID) ||
				s.sqlServer.serviceMode == mtinfopb.ServiceModeShared,
			DisableKvLevelAdvancedDebug: true,
		},
	); err != nil {
		return err
	}

	// Start garbage collecting system events.
	if err := startSystemLogsGC(workersCtx, s.sqlServer); err != nil {
		return err
	}

	// Initialize the external storage builders configuration params now that the
	// engines have been created. The object can be used to create ExternalStorage
	// objects hereafter.
	ieMon := sql.MakeInternalExecutorMemMonitor(sql.MemoryMetrics{}, s.ClusterSettings())
	ieMon.StartNoReserved(ctx, s.PGServer().SQLServer.GetBytesMonitor())
	s.stopper.AddCloser(stop.CloserFn(func() { ieMon.Stop(ctx) }))
	s.externalStorageBuilder.init(
		s.cfg.EarlyBootExternalStorageAccessor,
		s.cfg.ExternalIODirConfig,
		s.sqlServer.cfg.Settings,
		s.sqlServer.sqlIDContainer,
		s.kvNodeDialer,
		s.sqlServer.cfg.TestingKnobs,
		false, /* allowLocalFastpath */
		s.sqlServer.execCfg.InternalDB.
			CloneWithMemoryMonitor(sql.MemoryMetrics{}, ieMon),
		s.costController,
		s.registry,
		s.cfg.ExternalIODir,
	)

	// Start the job scheduler now that the SQL Server and
	// external storage is initialized.
	if err := s.initJobScheduler(ctx); err != nil {
		return err
	}

	// If enabled, start reporting diagnostics.
	if s.sqlServer.cfg.StartDiagnosticsReporting && !cluster.TelemetryOptOut {
		s.startDiagnostics(workersCtx)
	}
	// Enable the Obs Server.
	// There is more logic here than in (*Server).PreStart() because
	// we care about the SQL instance ID too.
	clusterID := s.rpcContext.LogicalClusterID.Get()
	instanceID := s.sqlServer.SQLInstanceID()
	if clusterID.Equal(uuid.Nil) {
		log.Fatalf(ctx, "expected LogicalClusterID to be initialized after preStart")
	}
	if instanceID == 0 {
		log.Fatalf(ctx, "expected SQLInstanceID to be initialized after preStart")
	}

	// Add more context to the Sentry reporter.
	sentry.ConfigureScope(func(scope *sentry.Scope) {
		scope.SetTags(map[string]string{
			"cluster":   clusterID.String(),
			"instance":  instanceID.String(),
			"server_id": fmt.Sprintf("%s-%s", clusterID.Short(), instanceID.String()),
			// TODO(jaylim-crl): Consider using tenant names here in the future
			// as well. See discussions in https://github.com/cockroachdb/cockroach/pull/128602.
			"tenant_id": s.rpcContext.TenantID.String(),
		})
	})

	// externalUsageFn measures the CPU time, for use by tenant
	// resource usage accounting in costController.Start below.
	externalUsageFn := func(ctx context.Context) multitenant.ExternalUsage {
		return multitenant.ExternalUsage{
			CPUSecs:           multitenantcpu.GetCPUSeconds(ctx),
			PGWireEgressBytes: s.sqlServer.pgServer.BytesOut(),
		}
	}

	nextLiveInstanceIDFn := makeNextLiveInstanceIDFn(s.sqlServer.sqlInstanceReader, instanceID)

	// Start the cost controller for this secondary tenant.
	if err := s.costController.Start(
		workersCtx, s.stopper, instanceID, s.sqlServer.sqlLivenessSessionID,
		externalUsageFn, nextLiveInstanceIDFn,
	); err != nil {
		return err
	}

	return nil
}

func (s *SQLServerWrapper) serveConn(
	ctx context.Context, conn net.Conn, status pgwire.PreServeStatus,
) error {
	pgServer := s.PGServer()
	switch status.State {
	case pgwire.PreServeCancel:
		// Cancel requests are unauthenticated so run the cancel async to prevent
		// the client from deriving any info about the cancel based on how long it
		// takes.
		return s.stopper.RunAsyncTask(ctx, "cancel", func(ctx context.Context) {
			pgServer.HandleCancel(ctx, status.CancelKey)
		})
	case pgwire.PreServeReady:
		return pgServer.ServeConn(ctx, conn, status)
	default:
		return errors.AssertionFailedf("programming error: missing case %v", status.State)
	}
}

// initJobScheduler starts the job scheduler. This must be called
// after sqlServer.preStart and after our external storage providers
// have been initialized.
//
// TODO(ssd): We need to clean up the ordering/ownership here. The SQL
// server owns the job scheduler because the job scheduler needs an
// internal executor. But, the topLevelServer owns initialization of
// the external storage providers.
//
// TODO(ssd): Remove duplication with *topLevelServer.
func (s *SQLServerWrapper) initJobScheduler(ctx context.Context) error {
	if s.cfg.DisableSQLServer {
		return nil
	}
	// The job scheduler may immediately start jobs that require
	// external storage providers to be available. We expect the
	// server start up ordering to ensure this. Hitting this error
	// is a programming error somewhere in server startup.
	if err := s.externalStorageBuilder.assertInitComplete(); err != nil {
		return err
	}
	s.sqlServer.startJobScheduler(ctx, s.cfg.TestingKnobs)
	return nil
}

// AcceptClients starts listening for incoming SQL clients over the network.
// This mirrors the implementation of (*Server).AcceptClients.
// TODO(knz): Find a way to implement this method only once for both.
func (s *SQLServerWrapper) AcceptClients(ctx context.Context) error {
	if s.sqlServer.cfg.DisableSQLServer {
		return serverutils.PreventDisableSQLForTenantError()
	}

	if !s.sqlServer.cfg.DisableSQLListener {
		if err := startServeSQL(
			s.AnnotateCtx(context.Background()),
			s.stopper,
			s.pgPreServer,
			s.serveConn,
			s.pgL,
			s.ClusterSettings(),
			&s.sqlServer.cfg.SocketFile,
		); err != nil {
			return err
		}
	}

	if err := structlogging.StartHotRangesLoggingScheduler(
		ctx,
		s.stopper,
		s.sqlServer.tenantConnect,
		*s.sqlServer.internalExecutor,
		s.ClusterSettings(),
	); err != nil {
		return err
	}

	s.sqlServer.isReady.Store(true)

	log.Event(ctx, "server ready")
	return nil
}

// AcceptInternalClients starts listening for incoming SQL connections on the
// internal loopback interface.
func (s *SQLServerWrapper) AcceptInternalClients(ctx context.Context) error {
	if s.sqlServer.cfg.DisableSQLServer {
		return serverutils.PreventDisableSQLForTenantError()
	}

	connManager := netutil.MakeTCPServer(ctx, s.stopper)

	return s.stopper.RunAsyncTaskEx(ctx,
		stop.TaskOpts{TaskName: "sql-internal-listener", SpanOpt: stop.SterileRootSpan},
		func(ctx context.Context) {
			err := connManager.ServeWith(ctx, s.loopbackPgL, func(ctx context.Context, conn net.Conn) {
				connCtx := s.pgPreServer.AnnotateCtxForIncomingConn(ctx, conn)
				connCtx = logtags.AddTag(connCtx, "internal-conn", nil)

				conn, status, err := s.pgPreServer.PreServe(connCtx, conn, pgwire.SocketInternalLoopback)
				if err != nil {
					log.Ops.Errorf(connCtx, "serving SQL client conn: %v", err)
					return
				}
				defer status.ReleaseMemory(ctx)

				if err := s.serveConn(connCtx, conn, status); err != nil {
					log.Ops.Errorf(connCtx, "serving internal SQL client conn: %s", err)
				}
			})
			netutil.FatalIfUnexpected(err)
		})
}

// Start calls PreStart() and AcceptClient() in sequence.
// This is suitable for use e.g. in tests.
// This mirrors the implementation of (*Server).Start.
// TODO(knz): Find a way to implement this method only once for both.
func (s *SQLServerWrapper) Start(ctx context.Context) error {
	if err := s.PreStart(ctx); err != nil {
		return err
	}
	return s.AcceptClients(ctx)
}

// AnnotateCtx is a convenience wrapper; see AmbientContext.
func (s *SQLServerWrapper) AnnotateCtx(ctx context.Context) context.Context {
	return s.sqlServer.cfg.AmbientCtx.AnnotateCtx(ctx)
}

// ClusterSettings returns the cluster settings.
func (s *SQLServerWrapper) ClusterSettings() *cluster.Settings {
	return s.sqlServer.cfg.Settings
}

// PGServer exports the pgwire server. Used by tests.
func (s *SQLServerWrapper) PGServer() *pgwire.Server {
	return s.sqlServer.pgServer
}

// LogicalClusterID retrieves the logical cluster ID of this tenant server.
// Used in cli/mt_start_sql.go.
func (s *SQLServerWrapper) LogicalClusterID() uuid.UUID {
	return s.sqlServer.LogicalClusterID()
}

// startDiagnostics begins the diagnostic loop of this tenant server.
// Used in cli/mt_start_sql.go.
func (s *SQLServerWrapper) startDiagnostics(ctx context.Context) {
	s.sqlServer.StartDiagnostics(ctx)
}

// InitialStart implements cli.serverStartupInterface. For SQL-only servers,
// no start is an initial cluster start.
func (s *SQLServerWrapper) InitialStart() bool {
	return false
}

// ShutdownRequested returns a channel that is signaled when a subsystem wants
// the server to be shut down.
func (s *SQLServerWrapper) ShutdownRequested() <-chan serverctl.ShutdownRequest {
	return s.sqlServer.ShutdownRequested()
}

func makeTenantSQLServerArgs(
	startupCtx context.Context,
	stopper *stop.Stopper,
	baseCfg BaseConfig,
	sqlCfg SQLConfig,
	tenantNameContainer *roachpb.TenantNameContainer,
	deps tenantServerDeps,
	serviceMode mtinfopb.TenantServiceMode,
) (sqlServerArgs, error) {
	st := baseCfg.Settings

	// Ensure that the settings accessor bark if an attempt is made
	// to use a SystemOnly setting.
	st.SV.SpecializeForVirtualCluster()

	// We want all log messages issued on behalf of this SQL instance to report
	// the instance ID (once known) as a tag.
	startupCtx = baseCfg.AmbientCtx.AnnotateCtx(startupCtx)

	clock, err := newClockFromConfig(startupCtx, baseCfg)
	if err != nil {
		return sqlServerArgs{}, err
	}

	registry := metric.NewRegistry()
	ruleRegistry := metric.NewRuleRegistry()
	promRuleExporter := metric.NewPrometheusRuleExporter(ruleRegistry)

	var rpcTestingKnobs rpc.ContextTestingKnobs
	var testingKnobShutdownTenantConnectorEarlyIfNoRecordPresent bool
	if p, ok := baseCfg.TestingKnobs.Server.(*TestingKnobs); ok {
		rpcTestingKnobs = p.ContextTestingKnobs
		testingKnobShutdownTenantConnectorEarlyIfNoRecordPresent = p.ShutdownTenantConnectorEarlyIfNoRecordPresent
	}

	rpcCtxOpts := rpc.ServerContextOptionsFromBaseConfig(baseCfg.Config)
	rpcCtxOpts.TenantID = sqlCfg.TenantID
	rpcCtxOpts.UseNodeAuth = sqlCfg.LocalKVServerInfo != nil
	rpcCtxOpts.NodeID = baseCfg.IDContainer
	rpcCtxOpts.StorageClusterID = baseCfg.ClusterIDContainer
	rpcCtxOpts.Clock = clock.WallClock()
	rpcCtxOpts.ToleratedOffset = clock.ToleratedOffset()
	rpcCtxOpts.Stopper = stopper
	rpcCtxOpts.Settings = st
	rpcCtxOpts.Knobs = rpcTestingKnobs
	// This tenant's SQL server only serves SQL connections and SQL-to-SQL
	// RPCs; so it should refuse to serve SQL-to-KV RPCs completely.
	rpcCtxOpts.TenantRPCAuthorizer = tenantcapabilitiesauthorizer.NewAllowNothingAuthorizer()
	rpcCtxOpts.Locality = baseCfg.Locality

	rpcContext := rpc.NewContext(startupCtx, rpcCtxOpts)

	if !baseCfg.Insecure {
		// This check mirrors that done in NewServer().
		// Needed for receiving RPC connections until
		// this issue is fixed:
		// https://github.com/cockroachdb/cockroach/issues/92524
		if _, err := rpcContext.GetServerTLSConfig(); err != nil {
			return sqlServerArgs{}, err
		}
		// Needed for outgoing connections.
		if _, err := rpcContext.GetClientTLSConfig(); err != nil {
			return sqlServerArgs{}, err
		}
		cm, err := rpcContext.GetCertificateManager()
		if err != nil {
			return sqlServerArgs{}, err
		}
		// Expose cert expirations in metrics.
		registry.AddMetricStruct(cm.Metrics())
	}

	registry.AddMetricStruct(rpcContext.Metrics())
	registry.AddMetricStruct(rpcContext.RemoteClocks.Metrics())

	// If there is a local KV server, hook this SQLServer to it so that the
	// SQLServer can perform some RPCs directly, without going through gRPC.
	if lsi := sqlCfg.LocalKVServerInfo; lsi != nil {
		rpcContext.SetLocalInternalServer(
			lsi.InternalServer,
			lsi.ServerInterceptors,
			rpcContext.ClientInterceptors())
	}

	var dsKnobs kvcoord.ClientTestingKnobs
	if dsKnobsP, ok := baseCfg.TestingKnobs.KVClient.(*kvcoord.ClientTestingKnobs); ok {
		dsKnobs = *dsKnobsP
	}
	rpcRetryOptions := base.DefaultRetryOptions()

	tcCfg := kvtenant.ConnectorConfig{
		TenantID:          sqlCfg.TenantID,
		AmbientCtx:        baseCfg.AmbientCtx,
		RPCContext:        rpcContext,
		RPCRetryOptions:   rpcRetryOptions,
		DefaultZoneConfig: &baseCfg.DefaultZoneConfig,

		ShutdownTenantConnectorEarlyIfNoRecordPresent: testingKnobShutdownTenantConnectorEarlyIfNoRecordPresent,
	}
	kvAddressConfig := kvtenant.KVAddressConfig{RemoteAddresses: sqlCfg.TenantKVAddrs, LoopbackAddress: sqlCfg.TenantLoopbackAddr}
	tenantConnect, err := kvtenant.Factory.NewConnector(tcCfg, kvAddressConfig)
	if err != nil {
		return sqlServerArgs{}, err
	}
	// TODO(baptist): This call does not take into account locality addresses.
	resolver := kvtenant.AddressResolver(tenantConnect)
	kvNodeDialer := nodedialer.New(rpcContext, resolver)

	provider := kvtenant.TokenBucketProvider(tenantConnect)
	if tenantKnobs, ok := baseCfg.TestingKnobs.TenantTestingKnobs.(*sql.TenantTestingKnobs); ok &&
		tenantKnobs.OverrideTokenBucketProvider != nil {
		provider = tenantKnobs.OverrideTokenBucketProvider(provider)
	}
	costController, err := deps.costControllerFactory(
		st, sqlCfg.TenantID, provider, tenantConnect, baseCfg.Locality)
	if err != nil {
		return sqlServerArgs{}, err
	}
	registry.AddMetricStruct(costController.Metrics())

	dsCfg := kvcoord.DistSenderConfig{
		AmbientCtx:        baseCfg.AmbientCtx,
		Settings:          st,
		Clock:             clock,
		NodeDescs:         tenantConnect,
		NodeIDGetter:      deps.nodeIDGetter,
		RPCRetryOptions:   &rpcRetryOptions,
		Stopper:           stopper,
		LatencyFunc:       rpcContext.RemoteClocks.Latency,
		TransportFactory:  kvcoord.GRPCTransportFactory(kvNodeDialer),
		RangeDescriptorDB: tenantConnect,
		Locality:          baseCfg.Locality,
		KVInterceptor:     costController,
		TestingKnobs:      dsKnobs,
	}
	ds := kvcoord.NewDistSender(dsCfg)
	registry.AddMetricStruct(ds.Metrics())

	var clientKnobs kvcoord.ClientTestingKnobs
	if p, ok := baseCfg.TestingKnobs.KVClient.(*kvcoord.ClientTestingKnobs); ok {
		clientKnobs = *p
	}

	txnMetrics := kvcoord.MakeTxnMetrics(baseCfg.HistogramWindowInterval())
	registry.AddMetricStruct(txnMetrics)
	tcsFactory := kvcoord.NewTxnCoordSenderFactory(
		kvcoord.TxnCoordSenderFactoryConfig{
			AmbientCtx:        baseCfg.AmbientCtx,
			Settings:          st,
			Clock:             clock,
			Stopper:           stopper,
			HeartbeatInterval: base.DefaultTxnHeartbeatInterval,
			Linearizable:      sqlCfg.Linearizable,
			Metrics:           txnMetrics,
			TestingKnobs:      clientKnobs,
		},
		ds,
	)

	dbCtx := kv.DefaultDBContext(st, stopper)
	dbCtx.NodeID = deps.instanceIDContainer
	db := kv.NewDBWithContext(baseCfg.AmbientCtx, tcsFactory, clock, dbCtx)

	rangeFeedKnobs, _ := baseCfg.TestingKnobs.RangeFeed.(*rangefeed.TestingKnobs)
	rangeFeedFactory, err := rangefeed.NewFactory(stopper, db, st, rangeFeedKnobs)
	if err != nil {
		return sqlServerArgs{}, err
	}

	sTS := ts.MakeTenantServer(baseCfg.AmbientCtx, tenantConnect, rpcContext.TenantID, registry)

	systemConfigWatcher := systemconfigwatcher.New(
		keys.MakeSQLCodec(sqlCfg.TenantID), clock, rangeFeedFactory, &baseCfg.DefaultZoneConfig,
	)

	// Define structures which have circular dependencies. The underlying structures
	// will be filled in during the construction of the sql server.
	circularInternalExecutor := &sql.InternalExecutor{}
	internalExecutorFactory := sql.NewShimInternalDB(db)
	circularJobRegistry := &jobs.Registry{}

	// Initialize the protectedts subsystem in multi-tenant clusters.
	protectedtsKnobs, _ := baseCfg.TestingKnobs.ProtectedTS.(*protectedts.TestingKnobs)
	protectedTSProvider, err := ptprovider.New(ptprovider.Config{
		DB:       internalExecutorFactory,
		Settings: st,
		Knobs:    protectedtsKnobs,
		ReconcileStatusFuncs: ptreconcile.StatusFuncs{
			jobsprotectedts.GetMetaType(jobsprotectedts.Jobs): jobsprotectedts.MakeStateFunc(
				circularJobRegistry, jobsprotectedts.Jobs,
			),
			jobsprotectedts.GetMetaType(jobsprotectedts.Schedules): jobsprotectedts.MakeStateFunc(
				circularJobRegistry, jobsprotectedts.Schedules,
			),
			sessionprotectedts.SessionMetaType: sessionprotectedts.MakeStatusFunc(),
		},
	})
	if err != nil {
		return sqlServerArgs{}, err
	}
	registry.AddMetricStruct(protectedTSProvider.Metrics())

	recorder := status.NewMetricsRecorder(
		sqlCfg.TenantID, tenantNameContainer, nil /* nodeLiveness */, nil, /* remoteClocks */
		clock.WallClock(), st)

	var runtime *status.RuntimeStatSampler
	if baseCfg.RuntimeStatSampler != nil {
		runtime = baseCfg.RuntimeStatSampler
	} else {
		runtime = status.NewRuntimeStatSampler(startupCtx, clock.WallClock())
	}
	sysRegistry := metric.NewRegistry()
	sysRegistry.AddMetricStruct(runtime)

	// NB: The init method will be called in (*SQLServerWrapper).PreStart().
	esb := &externalStorageBuilder{}
	externalStorage := esb.makeExternalStorage
	externalStorageFromURI := esb.makeExternalStorageFromURI

	grpcServer, err := newGRPCServer(startupCtx, rpcContext)
	if err != nil {
		return sqlServerArgs{}, err
	}

	sessionRegistry := sql.NewSessionRegistry()

	monitorAndMetrics := newRootSQLMemoryMonitor(monitorAndMetricsOptions{
		memoryPoolSize:          sqlCfg.MemoryPoolSize,
		histogramWindowInterval: baseCfg.HistogramWindowInterval(),
		settings:                baseCfg.Settings,
	})
	remoteFlowRunnerAcc := monitorAndMetrics.rootSQLMemoryMonitor.MakeBoundAccount()
	remoteFlowRunner := flowinfra.NewRemoteFlowRunner(baseCfg.AmbientCtx, stopper, &remoteFlowRunnerAcc)

	// TODO(irfansharif): hook up NewGrantCoordinatorSQL.
	var noopElasticCPUGrantCoord *admission.ElasticCPUGrantCoordinator = nil
	return sqlServerArgs{
		sqlServerOptionalKVArgs: sqlServerOptionalKVArgs{
			nodesStatusServer: serverpb.MakeOptionalNodesStatusServer(nil),
			nodeLiveness:      optionalnodeliveness.MakeContainer(nil),
			gossip:            gossip.MakeOptionalGossip(nil),
			grpcServer:        grpcServer.Server,
			isMeta1Leaseholder: func(_ context.Context, _ hlc.ClockTimestamp) (bool, error) {
				return false, errors.New("isMeta1Leaseholder is not available to secondary tenants")
			},
			externalStorage:        externalStorage,
			externalStorageFromURI: externalStorageFromURI,
			// Set instance ID to 0 and node ID to nil to indicate
			// that the instance ID will be bound later during preStart.
			nodeIDContainer:      deps.instanceIDContainer,
			spanConfigKVAccessor: tenantConnect,
			kvStoresIterator:     kvserverbase.UnsupportedStoresIterator{},
			inspectzServer:       inspectz.Unsupported{},
		},
		sqlServerOptionalTenantArgs: sqlServerOptionalTenantArgs{
			spanLimiterFactory: deps.spanLimiterFactory,
			tenantConnect:      tenantConnect,
			serviceMode:        serviceMode,
			promRuleExporter:   promRuleExporter,
		},
		SQLConfig:                &sqlCfg,
		BaseConfig:               &baseCfg,
		stopper:                  stopper,
		stopTrigger:              newStopTrigger(),
		clock:                    clock,
		runtime:                  runtime,
		rpcContext:               rpcContext,
		nodeDescs:                tenantConnect,
		systemConfigWatcher:      systemConfigWatcher,
		spanConfigAccessor:       tenantConnect,
		kvNodeDialer:             kvNodeDialer,
		distSender:               ds,
		db:                       db,
		registry:                 registry,
		sysRegistry:              sysRegistry,
		recorder:                 recorder,
		sessionRegistry:          sessionRegistry,
		remoteFlowRunner:         remoteFlowRunner,
		circularInternalExecutor: circularInternalExecutor,
		internalDB:               internalExecutorFactory,
		circularJobRegistry:      circularJobRegistry,
		protectedtsProvider:      protectedTSProvider,
		rangeFeedFactory:         rangeFeedFactory,
		tenantStatusServer:       tenantConnect,
		costController:           costController,
		monitorAndMetrics:        monitorAndMetrics,
		grpc:                     grpcServer,
		externalStorageBuilder:   esb,
		admissionPacerFactory:    noopElasticCPUGrantCoord,
		rangeDescIteratorFactory: tenantConnect,
		tenantTimeSeriesServer:   sTS,
		tenantCapabilitiesReader: sql.EmptySystemTenantOnly[tenantcapabilities.Reader](),
	}, nil
}

func makeNextLiveInstanceIDFn(
	sqlInstanceProvider sqlinstance.AddressResolver, instanceID base.SQLInstanceID,
) multitenant.NextLiveInstanceIDFn {
	return func(ctx context.Context) base.SQLInstanceID {
		instances, err := sqlInstanceProvider.GetAllInstances(ctx)
		if err != nil {
			log.Infof(ctx, "GetAllInstances failed: %v", err)
			return 0
		}
		if len(instances) == 0 {
			return 0
		}
		// Find the next ID in circular order.
		var minID, nextID base.SQLInstanceID
		for i := range instances {
			id := instances[i].InstanceID
			if minID == 0 || minID > id {
				minID = id
			}
			if id > instanceID && (nextID == 0 || nextID > id) {
				nextID = id
			}
		}
		if nextID == 0 {
			return minID
		}
		return nextID
	}
}

// NewTenantSideCostController is a hook for CCL code which implements the
// controller.
var NewTenantSideCostController costControllerFactory = NewNoopTenantSideCostController

// NewNoopTenantSideCostController returns a noop cost
// controller. Used by shared-process tenants.
func NewNoopTenantSideCostController(
	*cluster.Settings,
	roachpb.TenantID,
	kvtenant.TokenBucketProvider,
	kvclient.NodeDescStore,
	roachpb.Locality,
) (multitenant.TenantSideCostController, error) {
	// Return a no-op implementation.
	return noopTenantSideCostController{}, nil
}

// ApplyTenantLicense is a hook for CCL code which enables enterprise features
// for the tenant process if the COCKROACH_TENANT_LICENSE environment variable
// is set.
var ApplyTenantLicense = func() error { return nil /* no-op */ }

// noopTenantSideCostController is a no-op implementation of
// TenantSideCostController.
type noopTenantSideCostController struct{}

var _ multitenant.TenantSideCostController = noopTenantSideCostController{}

func (noopTenantSideCostController) Start(
	ctx context.Context,
	stopper *stop.Stopper,
	instanceID base.SQLInstanceID,
	sessionID sqlliveness.SessionID,
	externalUsageFn multitenant.ExternalUsageFn,
	nextLiveInstanceIDFn multitenant.NextLiveInstanceIDFn,
) error {
	return nil
}

func (noopTenantSideCostController) OnRequestWait(ctx context.Context) error {
	return nil
}

func (noopTenantSideCostController) OnResponseWait(
	ctx context.Context,
	request *kvpb.BatchRequest,
	response *kvpb.BatchResponse,
	targetRange *roachpb.RangeDescriptor,
	targetReplica *roachpb.ReplicaDescriptor,
) error {
	return nil
}

func (noopTenantSideCostController) OnExternalIOWait(
	ctx context.Context, usage multitenant.ExternalIOUsage,
) error {
	return nil
}

func (noopTenantSideCostController) OnExternalIO(
	ctx context.Context, usage multitenant.ExternalIOUsage,
) {
}

func (noopTenantSideCostController) GetCPUMovingAvg() float64 {
	return 0
}

func (noopTenantSideCostController) GetRequestUnitModel() *tenantcostmodel.RequestUnitModel {
	return nil
}

func (noopTenantSideCostController) GetEstimatedCPUModel() *tenantcostmodel.EstimatedCPUModel {
	return nil
}

func (noopTenantSideCostController) Metrics() metric.Struct {
	return emptyMetricStruct{}
}

// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvtenant"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptprovider"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptreconcile"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/multitenant/multitenantcpu"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiesauthorizer"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/obs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/server/systemconfigwatcher"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfiglimiter"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/optionalnodeliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

	clock      *hlc.Clock
	rpcContext *rpc.Context
	// The gRPC server on which the different RPC handlers will be registered.
	grpc       *grpcServer
	nodeDialer *nodedialer.Dialer
	db         *kv.DB
	registry   *metric.Registry
	recorder   *status.MetricsRecorder
	runtime    *status.RuntimeStatSampler

	http            *httpServer
	adminAuthzCheck *adminPrivilegeChecker
	tenantAdmin     *adminServer
	tenantStatus    *statusServer
	drainServer     *drainServer
	authentication  *authenticationServer
	// eventsExporter exports data to the Observability Service.
	eventsExporter obs.EventsExporterInterface
	stopper        *stop.Stopper

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

	// The following should eventually be connected to tenant
	// capabilities.
	costControllerFactory costControllerFactory
	spanLimiterFactory    spanLimiterFactory
}

type spanLimiterFactory func(isql.Executor, *cluster.Settings, *spanconfig.TestingKnobs) spanconfig.Limiter
type costControllerFactory func(*cluster.Settings, roachpb.TenantID, kvtenant.TokenBucketProvider) (multitenant.TenantSideCostController, error)

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
		instanceIDContainer:   baseCfg.IDContainer.SwitchToSQLIDContainerForStandaloneSQLInstance(),
		costControllerFactory: NewTenantSideCostController,
		spanLimiterFactory: func(ie isql.Executor, st *cluster.Settings, knobs *spanconfig.TestingKnobs) spanconfig.Limiter {
			return spanconfiglimiter.New(ie, st, knobs)
		},
	}

	return newTenantServer(ctx, stopper, baseCfg, sqlCfg, tenantNameContainer, deps)
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
	return newTenantServer(ctx, stopper, baseCfg, sqlCfg, tenantNameContainer, deps)
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
) (*SQLServerWrapper, error) {
	// TODO(knz): Make the license application a per-server thing
	// instead of a global thing.
	err := ApplyTenantLicense()
	if err != nil {
		return nil, err
	}

	// Inform the server identity provider that we're operating
	// for a tenant server.
	baseCfg.idProvider.SetTenant(sqlCfg.TenantID)
	args, err := makeTenantSQLServerArgs(ctx, stopper, baseCfg, sqlCfg, tenantNameContainer, deps)
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
	adminAuthzCheck := &adminPrivilegeChecker{
		ie:          args.circularInternalExecutor,
		st:          args.Settings,
		makePlanner: nil,
	}

	// Instantiate the HTTP server.
	// These callbacks help us avoid a dependency on gossip in httpServer.
	parseNodeIDFn := func(s string) (roachpb.NodeID, bool, error) {
		return roachpb.NodeID(0), false, errors.New("tenants cannot proxy to KV Nodes")
	}
	getNodeIDHTTPAddressFn := func(id roachpb.NodeID) (*util.UnresolvedAddr, error) {
		return nil, errors.New("tenants cannot proxy to KV Nodes")
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

	// Instantiate the SQL server proper.
	sqlServer, err := newSQLServer(ctx, args)
	if err != nil {
		return nil, err
	}

	// Instantiate the migration API server.
	tms := newTenantMigrationServer(sqlServer)
	serverpb.RegisterMigrationServer(args.grpc.Server, tms)
	sqlServer.migrationServer = tms // only for testing via TestTenant

	// Tell the authz server how to connect to SQL.
	adminAuthzCheck.makePlanner = func(opName string) (interface{}, func()) {
		// This is a hack to get around a Go package dependency cycle. See comment
		// in sql/jobs/registry.go on planHookMaker.
		txn := args.db.NewTxn(ctx, "check-system-privilege")
		return sql.NewInternalPlanner(
			opName,
			txn,
			username.RootUserName(),
			&sql.MemoryMetrics{},
			sqlServer.execCfg,
			sessiondatapb.SessionData{},
		)
	}

	// Create the authentication RPC server (login/logout).
	sAuth := newAuthenticationServer(baseCfg.Config, sqlServer)

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
	//
	// TODO(knz): If/when we want to support statement diagnostic requests
	// in secondary tenants, this is where we would call setStmtDiagnosticsRequester(),
	// like in NewServer().
	serverIterator.sqlServer = sqlServer
	sStatus.baseStatusServer.sqlServer = sqlServer
	sAdmin.sqlServer = sqlServer

	// Create the debug API server.
	debugServer := debug.NewServer(
		baseCfg.AmbientCtx,
		args.Settings,
		sqlServer.pgServer.HBADebugFn(),
		sqlServer.execCfg.SQLStatusServer,
		nil, /* serverTickleFn */
	)

	return &SQLServerWrapper{
		clock:      args.clock,
		rpcContext: args.rpcContext,

		grpc:       args.grpc,
		nodeDialer: args.nodeDialer,
		db:         args.db,
		registry:   args.registry,
		recorder:   args.recorder,
		runtime:    args.runtime,

		http:            sHTTP,
		adminAuthzCheck: adminAuthzCheck,
		tenantAdmin:     sAdmin,
		tenantStatus:    sStatus,
		drainServer:     drainServer,
		authentication:  sAuth,
		eventsExporter:  args.eventsExporter,
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
	pgL, loopbackPgL, rpcLoopbackDialFn, startRPCServer, err := startListenRPCAndSQL(ctx, workersCtx, *s.sqlServer.cfg, s.stopper, s.grpc, enableSQLListener)
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
		workersCtx, s.sqlServer.cfg.Settings, s.stopper, s.registry, base.DefaultMetricsSampleInterval,
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
			"engine_type":     s.sqlServer.cfg.StorageEngine.String(),
			"encrypted_store": strconv.FormatBool(encryptedStore),
		})
	})

	// We can now add the node registry.
	s.recorder.AddNode(
		s.registry,
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
			s.ClusterSettings(),
			s.stopper,
			s.sqlServer.cfg.GoroutineDumpDirName,
			s.sqlServer.cfg.HeapProfileDirName,
			s.sqlServer.cfg.CPUProfileDirName,
			s.runtime,
			s.tenantStatus.sessionRegistry,
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

	// Start garbage collecting system events.
	if err := startSystemLogsGC(workersCtx, s.sqlServer); err != nil {
		return err
	}

	// Connect the HTTP endpoints. This also wraps the privileged HTTP
	// endpoints served by gwMux by the HTTP cookie authentication
	// check.
	if err := s.http.setupRoutes(ctx,
		s.authentication,  /* authnServer */
		s.adminAuthzCheck, /* adminAuthzCheck */
		s.recorder,        /* metricSource */
		s.runtime,         /* runtimeStatsSampler */
		gwMux,             /* handleRequestsUnauthenticated */
		s.debug,           /* handleDebugUnauthenticated */
		newAPIV2Server(workersCtx, &apiV2ServerOpts{
			admin:            s.tenantAdmin,
			status:           s.tenantStatus,
			promRuleExporter: s.promRuleExporter,
			sqlServer:        s.sqlServer,
			db:               s.db,
		}), /* apiServer */
		serverpb.FeatureFlags{
			CanViewKvMetricDashboards:   s.rpcContext.TenantID.Equal(roachpb.SystemTenantID),
			DisableKvLevelAdvancedDebug: true,
		},
	); err != nil {
		return err
	}

	// Start the SQL subsystem.
	if err := s.sqlServer.preStart(
		workersCtx,
		s.stopper,
		s.sqlServer.cfg.TestingKnobs,
		orphanedLeasesTimeThresholdNanos,
	); err != nil {
		return err
	}

	// Initialize the external storage builders configuration params now that the
	// engines have been created. The object can be used to create ExternalStorage
	// objects hereafter.
	ieMon := sql.MakeInternalExecutorMemMonitor(sql.MemoryMetrics{}, s.ClusterSettings())
	ieMon.StartNoReserved(ctx, s.PGServer().SQLServer.GetBytesMonitor())
	s.stopper.AddCloser(stop.CloserFn(func() { ieMon.Stop(ctx) }))
	s.externalStorageBuilder.init(
		ctx,
		s.sqlCfg.ExternalIODirConfig,
		s.sqlServer.cfg.Settings,
		s.sqlServer.sqlIDContainer,
		s.nodeDialer,
		s.sqlServer.cfg.TestingKnobs,
		false, /* allowLocalFastpath */
		s.sqlServer.execCfg.InternalDB.
			CloneWithMemoryMonitor(sql.MemoryMetrics{}, ieMon),
		s.costController,
		s.registry,
	)

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
	s.eventsExporter.SetNodeInfo(obs.NodeInfo{
		ClusterID:     clusterID,
		NodeID:        int32(instanceID),
		BinaryVersion: build.BinaryVersion(),
	})
	if err := s.eventsExporter.Start(ctx, s.stopper); err != nil {
		return errors.Wrap(err, "failed to start the event exporter")
	}

	// Add more context to the Sentry reporter.
	sentry.ConfigureScope(func(scope *sentry.Scope) {
		scope.SetTags(map[string]string{
			"cluster":   clusterID.String(),
			"instance":  instanceID.String(),
			"server_id": fmt.Sprintf("%s-%s", clusterID.Short(), instanceID.String()),
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
		pgServer.HandleCancel(ctx, status.CancelKey)
		return nil
	case pgwire.PreServeReady:
		return pgServer.ServeConn(ctx, conn, status)
	default:
		return errors.AssertionFailedf("programming error: missing case %v", status.State)
	}
}

// AcceptClients starts listening for incoming SQL clients over the network.
// This mirrors the implementation of (*Server).AcceptClients.
// TODO(knz): Find a way to implement this method only once for both.
func (s *SQLServerWrapper) AcceptClients(ctx context.Context) error {
	if !s.sqlServer.cfg.DisableSQLListener {
		if err := startServeSQL(
			s.AnnotateCtx(context.Background()),
			s.stopper,
			s.pgPreServer,
			s.serveConn,
			s.pgL,
			&s.sqlServer.cfg.SocketFile,
		); err != nil {
			return err
		}
	}

	s.sqlServer.isReady.Set(true)

	log.Event(ctx, "server ready")
	return nil
}

// AcceptInternalClients starts listening for incoming SQL connections on the
// internal loopback interface.
func (s *SQLServerWrapper) AcceptInternalClients(ctx context.Context) error {
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
func (s *SQLServerWrapper) ShutdownRequested() <-chan ShutdownRequest {
	return s.sqlServer.ShutdownRequested()
}

func makeTenantSQLServerArgs(
	startupCtx context.Context,
	stopper *stop.Stopper,
	baseCfg BaseConfig,
	sqlCfg SQLConfig,
	tenantNameContainer *roachpb.TenantNameContainer,
	deps tenantServerDeps,
) (sqlServerArgs, error) {
	st := baseCfg.Settings

	// We want all log messages issued on behalf of this SQL instance to report
	// the instance ID (once known) as a tag.
	startupCtx = baseCfg.AmbientCtx.AnnotateCtx(startupCtx)

	maxOffset := time.Duration(baseCfg.MaxOffset)
	toleratedOffset := baseCfg.ToleratedOffset()
	clock := hlc.NewClockWithSystemTimeSource(maxOffset, toleratedOffset)

	registry := metric.NewRegistry()
	ruleRegistry := metric.NewRuleRegistry()
	promRuleExporter := metric.NewPrometheusRuleExporter(ruleRegistry)

	var rpcTestingKnobs rpc.ContextTestingKnobs
	if p, ok := baseCfg.TestingKnobs.Server.(*TestingKnobs); ok {
		rpcTestingKnobs = p.ContextTestingKnobs
	}

	authorizer := tenantcapabilitiesauthorizer.NewNoopAuthorizer()
	rpcContext := rpc.NewContext(startupCtx, rpc.ContextOptions{
		TenantID:            sqlCfg.TenantID,
		UseNodeAuth:         sqlCfg.LocalKVServerInfo != nil,
		NodeID:              baseCfg.IDContainer,
		StorageClusterID:    baseCfg.ClusterIDContainer,
		Config:              baseCfg.Config,
		Clock:               clock.WallClock(),
		ToleratedOffset:     clock.ToleratedOffset(),
		Stopper:             stopper,
		Settings:            st,
		Knobs:               rpcTestingKnobs,
		TenantRPCAuthorizer: authorizer,
	})

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
	}
	tenantConnect, err := kvtenant.Factory.NewConnector(tcCfg, sqlCfg.TenantKVAddrs)
	if err != nil {
		return sqlServerArgs{}, err
	}
	resolver := kvtenant.AddressResolver(tenantConnect)
	nodeDialer := nodedialer.New(rpcContext, resolver)

	provider := kvtenant.TokenBucketProvider(tenantConnect)
	if tenantKnobs, ok := baseCfg.TestingKnobs.TenantTestingKnobs.(*sql.TenantTestingKnobs); ok &&
		tenantKnobs.OverrideTokenBucketProvider != nil {
		provider = tenantKnobs.OverrideTokenBucketProvider(provider)
	}
	costController, err := deps.costControllerFactory(st, sqlCfg.TenantID, provider)
	if err != nil {
		return sqlServerArgs{}, err
	}

	dsCfg := kvcoord.DistSenderConfig{
		AmbientCtx:        baseCfg.AmbientCtx,
		Settings:          st,
		Clock:             clock,
		NodeDescs:         tenantConnect,
		RPCRetryOptions:   &rpcRetryOptions,
		RPCContext:        rpcContext,
		NodeDialer:        nodeDialer,
		RangeDescriptorDB: tenantConnect,
		Locality:          baseCfg.Locality,
		KVInterceptor:     costController,
		TestingKnobs:      dsKnobs,
	}
	ds := kvcoord.NewDistSender(dsCfg)

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

	dbCtx := kv.DefaultDBContext(stopper)
	dbCtx.NodeID = deps.instanceIDContainer
	db := kv.NewDBWithContext(baseCfg.AmbientCtx, tcsFactory, clock, dbCtx)

	rangeFeedKnobs, _ := baseCfg.TestingKnobs.RangeFeed.(*rangefeed.TestingKnobs)
	rangeFeedFactory, err := rangefeed.NewFactory(stopper, db, st, rangeFeedKnobs)
	if err != nil {
		return sqlServerArgs{}, err
	}

	sTS := ts.MakeTenantServer(baseCfg.AmbientCtx, tenantConnect, rpcContext.TenantID)

	systemConfigWatcher := systemconfigwatcher.NewWithAdditionalProvider(
		keys.MakeSQLCodec(sqlCfg.TenantID), clock, rangeFeedFactory, &baseCfg.DefaultZoneConfig,
		tenantConnect,
	)

	// Define structures which have circular dependencies. The underlying structures
	// will be filled in during the construction of the sql server.
	circularInternalExecutor := &sql.InternalExecutor{}
	internalExecutorFactory := sql.NewShimInternalDB(db)
	circularJobRegistry := &jobs.Registry{}

	// Initialize the protectedts subsystem in multi-tenant clusters.
	var protectedTSProvider protectedts.Provider
	protectedtsKnobs, _ := baseCfg.TestingKnobs.ProtectedTS.(*protectedts.TestingKnobs)
	pp, err := ptprovider.New(ptprovider.Config{
		DB:       internalExecutorFactory,
		Settings: st,
		Knobs:    protectedtsKnobs,
		ReconcileStatusFuncs: ptreconcile.StatusFuncs{
			jobsprotectedts.GetMetaType(jobsprotectedts.Jobs): jobsprotectedts.MakeStatusFunc(
				circularJobRegistry, jobsprotectedts.Jobs,
			),
			jobsprotectedts.GetMetaType(jobsprotectedts.Schedules): jobsprotectedts.MakeStatusFunc(
				circularJobRegistry, jobsprotectedts.Schedules,
			),
		},
	})
	if err != nil {
		return sqlServerArgs{}, err
	}
	registry.AddMetricStruct(pp.Metrics())
	protectedTSProvider = pp

	recorder := status.NewMetricsRecorder(
		sqlCfg.TenantID, tenantNameContainer, nil /* nodeLiveness */, nil, /* remoteClocks */
		clock.WallClock(), st)

	var runtime *status.RuntimeStatSampler
	if baseCfg.RuntimeStatSampler != nil {
		runtime = baseCfg.RuntimeStatSampler
	} else {
		runtime = status.NewRuntimeStatSampler(startupCtx, clock.WallClock())
	}
	registry.AddMetricStruct(runtime)

	// NB: The init method will be called in (*SQLServerWrapper).PreStart().
	esb := &externalStorageBuilder{}
	externalStorage := esb.makeExternalStorage
	externalStorageFromURI := esb.makeExternalStorageFromURI

	grpcServer, err := newGRPCServer(rpcContext)
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

	// Create the EventServer. It will be made operational later, after the
	// cluster ID is known, with a Start() call.
	var eventsExporter obs.EventsExporterInterface
	if baseCfg.ObsServiceAddr != "" {
		if baseCfg.ObsServiceAddr == base.ObsServiceEmbedFlagValue {
			// TODO(andrei): Add support for this option for tenants - at least for
			// shared-process tenants where the event exporting should be hooked up to
			// the ingester running in the host process.
			return sqlServerArgs{}, errors.New("--obsservice-addr=embed is not currently supported for tenants")
		}
		ee := obs.NewEventsExporter(
			baseCfg.ObsServiceAddr,
			timeutil.DefaultTimeSource{},
			baseCfg.Tracer,
			5*time.Second,                          // maxStaleness
			1<<20,                                  // triggerSizeBytes - 1MB
			10*1<<20,                               // maxBufferSizeBytes - 10MB
			monitorAndMetrics.rootSQLMemoryMonitor, // memMonitor - this is not "SQL" usage, but we don't have another memory pool
		)
		eventsExporter = ee
	} else {
		eventsExporter = &obs.NoopEventsExporter{}
	}

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
		},
		sqlServerOptionalTenantArgs: sqlServerOptionalTenantArgs{
			spanLimiterFactory: deps.spanLimiterFactory,
			tenantConnect:      tenantConnect,
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
		nodeDialer:               nodeDialer,
		distSender:               ds,
		db:                       db,
		registry:                 registry,
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
		eventsExporter:           eventsExporter,
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
	*cluster.Settings, roachpb.TenantID, kvtenant.TokenBucketProvider,
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
	ctx context.Context, req tenantcostmodel.RequestInfo, resp tenantcostmodel.ResponseInfo,
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

func (noopTenantSideCostController) GetCostConfig() *tenantcostmodel.Config {
	return nil
}

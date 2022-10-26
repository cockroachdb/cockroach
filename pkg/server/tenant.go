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
	"net/http"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
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
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/obs"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/server/systemconfigwatcher"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/optionalnodeliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// SQLServerWrapper is a utility struct that encapsulates
// a SQLServer and its helpers that make it a networked service.
type SQLServerWrapper struct {
	clock      *hlc.Clock
	rpcContext *rpc.Context
	// The gRPC server on which the different RPC handlers will be registered.
	grpc *grpcServer

	http        *httpServer
	sqlServer   *SQLServer
	authServer  *authenticationServer
	drainServer *drainServer
	// The Observability Server, used by the Observability Service to subscribe to
	// CRDB data.
	eventsServer *obs.EventsServer
	stopper      *stop.Stopper

	// Used for multi-tenant cost control (on the tenant side).
	costController multitenant.TenantSideCostController
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

// NewTenantServer creates a tenant-specific, SQL-only server against a KV backend.
func NewTenantServer(
	ctx context.Context, stopper *stop.Stopper, baseCfg BaseConfig, sqlCfg SQLConfig,
) (*SQLServerWrapper, error) {
	err := ApplyTenantLicense()
	if err != nil {
		return nil, err
	}

	// Inform the server identity provider that we're operating
	// for a tenant server.
	baseCfg.idProvider.SetTenant(sqlCfg.TenantID)

	args, err := makeTenantSQLServerArgs(ctx, stopper, baseCfg, sqlCfg)
	if err != nil {
		return nil, err
	}
	err = args.ValidateAddrs(ctx)
	if err != nil {
		return nil, err
	}
	closedSessionCache := sql.NewClosedSessionCache(
		baseCfg.Settings, args.monitorAndMetrics.rootSQLMemoryMonitor, time.Now)
	args.closedSessionCache = closedSessionCache

	// Add the server tags to the startup context.
	//
	// We use args.BaseConfig here instead of baseCfg directly because
	// makeTenantSQLArgs defines its own AmbientCtx instance and it's
	// defined by-value.
	ctx = args.BaseConfig.AmbientCtx.AnnotateCtx(ctx)

	// Add the server tags to a generic background context for use
	// by async goroutines.
	// We can only annotate the context after makeTenantSQLServerArgs
	// has defined the instance ID container in the AmbientCtx.
	background := args.BaseConfig.AmbientCtx.AnnotateCtx(context.Background())

	// The tenantStatusServer needs access to the sqlServer,
	// but we also need the same object to set up the sqlServer.
	// So construct the tenant status server with a nil sqlServer,
	// and then assign it once an SQL server gets created. We are
	// going to assume that the tenant status server won't require
	// the SQL server object.
	tenantStatusServer := newTenantStatusServer(
		baseCfg.AmbientCtx, nil,
		args.sessionRegistry, args.closedSessionCache, args.flowScheduler, baseCfg.Settings, nil,
		args.rpcContext, args.stopper,
	)

	args.sqlStatusServer = tenantStatusServer
	s, err := newSQLServer(ctx, args)
	if err != nil {
		return nil, err
	}
	adminAuthzCheck := &adminPrivilegeChecker{
		ie: s.execCfg.InternalExecutor,
		st: args.Settings,
		makePlanner: func(opName string) (interface{}, func()) {
			txn := args.db.NewTxn(ctx, "check-system-privilege")
			return sql.NewInternalPlanner(
				opName,
				txn,
				username.RootUserName(),
				&sql.MemoryMetrics{},
				s.execCfg,
				sessiondatapb.SessionData{},
			)
		},
	}
	tenantStatusServer.privilegeChecker = adminAuthzCheck
	tenantStatusServer.sqlServer = s

	drainServer := newDrainServer(baseCfg, args.stopper, args.grpc, s)

	tenantAdminServer := newTenantAdminServer(baseCfg.AmbientCtx, s, tenantStatusServer, drainServer)

	s.execCfg.DistSQLPlanner.ConstructAndSetSpanResolver(ctx, 0 /* NodeID */, s.execCfg.Locality)

	authServer := newAuthenticationServer(baseCfg.Config, s)

	// Register and start gRPC service on pod. This is separate from the
	// gRPC + Gateway services configured below.
	for _, gw := range []grpcGatewayServer{tenantAdminServer, tenantStatusServer, authServer} {
		gw.RegisterService(args.grpcServer)
	}

	// Begin configuration of GRPC Gateway
	gwMux, gwCtx, conn, err := configureGRPCGateway(
		ctx,
		background,
		args.AmbientCtx,
		args.rpcContext,
		s.stopper,
		args.grpc,
		baseCfg.AdvertiseAddr,
	)
	if err != nil {
		return nil, err
	}

	for _, gw := range []grpcGatewayServer{tenantAdminServer, tenantStatusServer, authServer} {
		if err := gw.RegisterGateway(gwCtx, gwMux, conn); err != nil {
			return nil, err
		}
	}

	debugServer := debug.NewServer(baseCfg.AmbientCtx, args.Settings, s.pgServer.HBADebugFn(), s.execCfg.SQLStatusServer)

	parseNodeIDFn := func(s string) (roachpb.NodeID, bool, error) {
		return roachpb.NodeID(0), false, errors.New("tenants cannot proxy to KV Nodes")
	}
	getNodeIDHTTPAddressFn := func(id roachpb.NodeID) (*util.UnresolvedAddr, error) {
		return nil, errors.New("tenants cannot proxy to KV Nodes")
	}
	httpServer := newHTTPServer(baseCfg, args.rpcContext, parseNodeIDFn, getNodeIDHTTPAddressFn)

	httpServer.handleHealth(gwMux)

	// Begin an async task to periodically purge old sessions in the system.web_sessions table.
	if err := startPurgeOldSessions(ctx, authServer); err != nil {
		return nil, err
	}

	// TODO(knz): Add support for the APIv2 tree here.
	if err := httpServer.setupRoutes(ctx,
		authServer,      /* authnServer */
		adminAuthzCheck, /* adminAuthzCheck */
		args.recorder,   /* metricSource */
		args.runtime,    /* runtimeStatSampler */
		gwMux,           /* handleRequestsUnauthenticated */
		debugServer,     /* handleDebugUnauthenticated */
		nil,             /* apiServer */
	); err != nil {
		return nil, err
	}

	args.recorder.AddNode(
		args.registry,
		roachpb.NodeDescriptor{},
		timeutil.Now().UnixNano(),
		baseCfg.AdvertiseAddr,     // advertised addr
		baseCfg.HTTPAdvertiseAddr, // http addr
		baseCfg.SQLAdvertiseAddr,  // sql addr
	)

	// TODO(tbg): the log dir is not configurable at this point
	// since it is integrated too tightly with the `./cockroach start` command.
	if err := startSampleEnvironment(ctx,
		args.Settings,
		args.stopper,
		args.GoroutineDumpDirName,
		args.HeapProfileDirName,
		args.runtime,
		args.sessionRegistry,
	); err != nil {
		return nil, err
	}

	sw := &SQLServerWrapper{
		clock:      args.clock,
		rpcContext: args.rpcContext,

		grpc: args.grpc,

		http:         httpServer,
		sqlServer:    s,
		authServer:   authServer,
		drainServer:  drainServer,
		eventsServer: args.eventsServer,
		stopper:      args.stopper,

		costController: args.costController,
	}
	return sw, nil
}

// PreStart starts the server on the specified port(s) and
// initializes subsystems.
//
// It does not activate the pgwire listener over the network / unix
// socket, which is done by the AcceptClients() method. The separation
// between the two exists so that SQL initialization can take place
// before the first client is accepted.
func (s *SQLServerWrapper) PreStart(ctx context.Context) error {
	// TODO(knz): Move morecode here.

	// Start a context for the asynchronous network workers.
	workersCtx := s.AnnotateCtx(context.Background())

	// Load the TLS configuration for the HTTP server.
	uiTLSConfig, err := s.rpcContext.GetUIServerTLSConfig()
	if err != nil {
		return err
	}

	// connManager tracks incoming connections accepted via listeners
	// and automatically closes them when the stopper indicates a
	// shutdown.
	// This handles both:
	// - HTTP connections for the admin UI with an optional TLS handshake over HTTP.
	// - SQL client connections with a TLS handshake over TCP.
	// (gRPC connections are handled separately via s.grpc and perform
	// their TLS handshake on their own)
	connManager := netutil.MakeServer(workersCtx, s.stopper, uiTLSConfig, http.HandlerFunc(s.http.baseHandler))

	// Start the admin UI server. This opens the HTTP listen socket,
	// optionally sets up TLS, and dispatches the server worker for the
	// web UI.
	if err := s.http.start(ctx, workersCtx, connManager, uiTLSConfig, s.stopper); err != nil {
		return err
	}

	// Start the RPC server. This opens the RPC/SQL listen socket,
	// and dispatches the server worker for the RPC.
	// The SQL listener is returned, to start the SQL server later
	// below when the server has initialized.
	pgL, startRPCServer, err := startListenRPCAndSQL(ctx, workersCtx, *s.sqlServer.cfg, s.stopper, s.grpc)
	if err != nil {
		return err
	}

	// This opens the main listener.
	startRPCServer(workersCtx)

	// Record a walltime that is lower than the lowest hlc timestamp this current
	// instance of the node can use. We do not use startTime because it is lower
	// than the timestamp used to create the bootstrap schema.
	//
	// TODO(tbg): clarify the contract here and move closer to usage if possible.
	orphanedLeasesTimeThresholdNanos := s.clock.Now().WallTime

	// After setting modeOperational, we can block until all stores are fully
	// initialized.
	s.grpc.setMode(modeOperational)

	if err := s.sqlServer.preStart(
		workersCtx,
		s.stopper,
		s.sqlServer.cfg.TestingKnobs,
		connManager,
		pgL,
		orphanedLeasesTimeThresholdNanos,
	); err != nil {
		return err
	}
	clusterID := s.rpcContext.LogicalClusterID.Get()
	instanceID := s.sqlServer.SQLInstanceID()
	if clusterID.Equal(uuid.Nil) {
		log.Fatalf(ctx, "expected LogicalClusterID to be initialized after preStart")
	}
	if instanceID == 0 {
		log.Fatalf(ctx, "expected SQLInstanceID to be initialized after preStart")
	}
	s.eventsServer.SetResourceInfo(clusterID, int32(instanceID), "unknown" /* version */)

	// externalUsageFn measures the CPU time, for use by tenant
	// resource usage accounting in costController.Start below.
	externalUsageFn := func(ctx context.Context) multitenant.ExternalUsage {
		userTimeMillis, sysTimeMillis, err := status.GetCPUTime(ctx)
		if err != nil {
			log.Ops.Errorf(ctx, "unable to get cpu usage: %v", err)
		}
		return multitenant.ExternalUsage{
			CPUSecs:           float64(userTimeMillis+sysTimeMillis) * 1e-3,
			PGWireEgressBytes: s.sqlServer.pgServer.BytesOut(),
		}
	}

	nextLiveInstanceIDFn := makeNextLiveInstanceIDFn(s.sqlServer.sqlInstanceProvider, s.sqlServer.SQLInstanceID())
	if err := s.costController.Start(
		ctx, s.stopper, s.sqlServer.SQLInstanceID(), s.sqlServer.sqlLivenessSessionID,
		externalUsageFn, nextLiveInstanceIDFn,
	); err != nil {
		return err
	}

	return nil
}

// AcceptClients starts listening for incoming SQL clients over the network.
// This mirrors the implementation of (*Server).AcceptClients.
// TODO(knz): Find a way to implement this method only once for both.
func (s *SQLServerWrapper) AcceptClients(ctx context.Context) error {
	workersCtx := s.AnnotateCtx(context.Background())

	if err := s.sqlServer.startServeSQL(
		workersCtx,
		s.stopper,
		s.sqlServer.connManager,
		s.sqlServer.pgL,
		&s.sqlServer.cfg.SocketFile,
	); err != nil {
		return err
	}

	log.Event(ctx, "server ready")
	return nil
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

// StartDiagnostics begins the diagnostic loop of this tenant server.
// Used in cli/mt_start_sql.go.
func (s *SQLServerWrapper) StartDiagnostics(ctx context.Context) {
	s.sqlServer.StartDiagnostics(ctx)
}

func makeTenantSQLServerArgs(
	startupCtx context.Context, stopper *stop.Stopper, baseCfg BaseConfig, sqlCfg SQLConfig,
) (sqlServerArgs, error) {
	st := baseCfg.Settings

	// We want all log messages issued on behalf of this SQL instance to report
	// the instance ID (once known) as a tag.
	instanceIDContainer := baseCfg.IDContainer.SwitchToSQLIDContainer()
	startupCtx = baseCfg.AmbientCtx.AnnotateCtx(startupCtx)

	clock := hlc.NewClockWithSystemTimeSource(time.Duration(baseCfg.MaxOffset))

	registry := metric.NewRegistry()

	var rpcTestingKnobs rpc.ContextTestingKnobs
	if p, ok := baseCfg.TestingKnobs.Server.(*TestingKnobs); ok {
		rpcTestingKnobs = p.ContextTestingKnobs
	}
	rpcContext := rpc.NewContext(startupCtx, rpc.ContextOptions{
		TenantID:         sqlCfg.TenantID,
		NodeID:           baseCfg.IDContainer,
		StorageClusterID: baseCfg.ClusterIDContainer,
		Config:           baseCfg.Config,
		Clock:            clock.WallClock(),
		MaxOffset:        clock.MaxOffset(),
		Stopper:          stopper,
		Settings:         st,
		Knobs:            rpcTestingKnobs,
	})

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
	costController, err := NewTenantSideCostController(st, sqlCfg.TenantID, provider)
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
			Linearizable:      false,
			Metrics:           txnMetrics,
			TestingKnobs:      clientKnobs,
		},
		ds,
	)
	db := kv.NewDB(baseCfg.AmbientCtx, tcsFactory, clock, stopper)
	rangeFeedKnobs, _ := baseCfg.TestingKnobs.RangeFeed.(*rangefeed.TestingKnobs)
	rangeFeedFactory, err := rangefeed.NewFactory(stopper, db, st, rangeFeedKnobs)
	if err != nil {
		return sqlServerArgs{}, err
	}

	systemConfigWatcher := systemconfigwatcher.NewWithAdditionalProvider(
		keys.MakeSQLCodec(sqlCfg.TenantID), clock, rangeFeedFactory, &baseCfg.DefaultZoneConfig,
		tenantConnect,
	)

	circularInternalExecutor := &sql.InternalExecutor{}
	internalExecutorFactory := &sql.InternalExecutorFactory{}
	circularJobRegistry := &jobs.Registry{}

	// Initialize the protectedts subsystem in multi-tenant clusters.
	var protectedTSProvider protectedts.Provider
	protectedtsKnobs, _ := baseCfg.TestingKnobs.ProtectedTS.(*protectedts.TestingKnobs)
	pp, err := ptprovider.New(ptprovider.Config{
		DB:               db,
		InternalExecutor: circularInternalExecutor,
		Settings:         st,
		Knobs:            protectedtsKnobs,
		ReconcileStatusFuncs: ptreconcile.StatusFuncs{
			jobsprotectedts.GetMetaType(jobsprotectedts.Jobs): jobsprotectedts.MakeStatusFunc(
				circularJobRegistry, circularInternalExecutor, jobsprotectedts.Jobs),
			jobsprotectedts.GetMetaType(jobsprotectedts.Schedules): jobsprotectedts.MakeStatusFunc(
				circularJobRegistry, circularInternalExecutor, jobsprotectedts.Schedules),
		},
	})
	if err != nil {
		return sqlServerArgs{}, err
	}
	registry.AddMetricStruct(pp.Metrics())
	protectedTSProvider = tenantProtectedTSProvider{Provider: pp, st: st}

	recorder := status.NewMetricsRecorder(clock, nil, rpcContext, nil, st)

	runtime := status.NewRuntimeStatSampler(startupCtx, clock)
	registry.AddMetricStruct(runtime)

	esb := &externalStorageBuilder{}
	externalStorage := esb.makeExternalStorage
	externalStorageFromURI := esb.makeExternalStorageFromURI

	esb.init(
		startupCtx,
		sqlCfg.ExternalIODirConfig,
		baseCfg.Settings,
		baseCfg.IDContainer,
		nodeDialer,
		baseCfg.TestingKnobs,
		circularInternalExecutor,
		internalExecutorFactory,
		db,
		costController,
	)

	grpcServer := newGRPCServer(rpcContext)

	sessionRegistry := sql.NewSessionRegistry()
	flowScheduler := flowinfra.NewFlowScheduler(baseCfg.AmbientCtx, stopper, st)

	monitorAndMetrics := newRootSQLMemoryMonitor(monitorAndMetricsOptions{
		memoryPoolSize:          sqlCfg.MemoryPoolSize,
		histogramWindowInterval: baseCfg.HistogramWindowInterval(),
		settings:                baseCfg.Settings,
	})

	// Create the EventServer. It will be made operational later, after the
	// cluster ID is known, with a SetResourceInfo() call.
	eventsServer := obs.NewEventServer(
		baseCfg.AmbientCtx,
		timeutil.DefaultTimeSource{},
		stopper,
		5*time.Second,                          // maxStaleness
		1<<20,                                  // triggerSizeBytes - 1MB
		10*1<<20,                               // maxBufferSizeBytes - 10MB
		monitorAndMetrics.rootSQLMemoryMonitor, // memMonitor - this is not "SQL" usage, but we don't have another memory pool,
	)
	obspb.RegisterObsServer(grpcServer.Server, eventsServer)

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
			nodeIDContainer:      instanceIDContainer,
			spanConfigKVAccessor: tenantConnect,
			kvStoresIterator:     kvserverbase.UnsupportedStoresIterator{},
		},
		sqlServerOptionalTenantArgs: sqlServerOptionalTenantArgs{
			tenantConnect: tenantConnect,
		},
		SQLConfig:                &sqlCfg,
		BaseConfig:               &baseCfg,
		stopper:                  stopper,
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
		flowScheduler:            flowScheduler,
		circularInternalExecutor: circularInternalExecutor,
		internalExecutorFactory:  internalExecutorFactory,
		circularJobRegistry:      circularJobRegistry,
		protectedtsProvider:      protectedTSProvider,
		rangeFeedFactory:         rangeFeedFactory,
		regionsServer:            tenantConnect,
		tenantStatusServer:       tenantConnect,
		costController:           costController,
		monitorAndMetrics:        monitorAndMetrics,
		grpc:                     grpcServer,
		eventsServer:             eventsServer,
	}, nil
}

func makeNextLiveInstanceIDFn(
	sqlInstanceProvider sqlinstance.Provider, instanceID base.SQLInstanceID,
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
var NewTenantSideCostController = func(
	st *cluster.Settings, tenantID roachpb.TenantID, provider kvtenant.TokenBucketProvider,
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

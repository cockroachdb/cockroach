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
	"crypto/tls"
	"net/http"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvtenant"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptprovider"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/contention"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/optionalnodeliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"google.golang.org/grpc/metadata"
)

// StartTenant starts a stand-alone SQL server against a KV backend.
func StartTenant(
	ctx context.Context,
	stopper *stop.Stopper,
	kvClusterName string, // NB: gone after https://github.com/cockroachdb/cockroach/issues/42519
	baseCfg BaseConfig,
	sqlCfg SQLConfig,
) (sqlServer *SQLServer, pgAddr string, httpAddr string, err error) {
	sqlServer, _, pgAddr, httpAddr, err = startTenantInternal(ctx, stopper, kvClusterName, baseCfg, sqlCfg)
	return
}

// startTenantInternal is used to build TestServers.
func startTenantInternal(
	ctx context.Context,
	stopper *stop.Stopper,
	kvClusterName string, // NB: gone after https://github.com/cockroachdb/cockroach/issues/42519
	baseCfg BaseConfig,
	sqlCfg SQLConfig,
) (
	sqlServer *SQLServer,
	authServer *authenticationServer,
	pgAddr string,
	httpAddr string,
	_ error,
) {
	err := ApplyTenantLicense()
	if err != nil {
		return nil, nil, "", "", err
	}

	args, err := makeTenantSQLServerArgs(stopper, kvClusterName, baseCfg, sqlCfg)
	if err != nil {
		return nil, nil, "", "", err
	}
	err = args.ValidateAddrs(ctx)
	if err != nil {
		return nil, nil, "", "", err
	}
	args.monitorAndMetrics = newRootSQLMemoryMonitor(monitorAndMetricsOptions{
		memoryPoolSize:          args.MemoryPoolSize,
		histogramWindowInterval: args.HistogramWindowInterval(),
		settings:                args.Settings,
	})

	connManager := netutil.MakeServer(
		args.stopper,
		// The SQL server only uses connManager.ServeWith. The both below
		// are unused.
		nil, // tlsConfig
		nil, // handler
	)

	// Initialize gRPC server for use on shared port with pg
	grpcMain := newGRPCServer(args.rpcContext)
	grpcMain.setMode(modeOperational)

	// TODO(davidh): Do we need to force this to be false?
	baseCfg.SplitListenSQL = false

	background := baseCfg.AmbientCtx.AnnotateCtx(context.Background())

	// StartListenRPCAndSQL will replace the SQLAddr fields if we choose
	// to share the SQL and gRPC port so here, since the tenant config
	// expects to have port set on the SQL param we transfer those to
	// the base Addr params in order for the RPC to be configured
	// correctly.
	baseCfg.Addr = baseCfg.SQLAddr
	baseCfg.AdvertiseAddr = baseCfg.SQLAdvertiseAddr
	pgL, startRPCServer, err := StartListenRPCAndSQL(ctx, background, baseCfg, stopper, grpcMain)
	if err != nil {
		return nil, nil, "", "", err
	}

	{
		waitQuiesce := func(ctx context.Context) {
			<-args.stopper.ShouldQuiesce()
			// NB: we can't do this as a Closer because (*Server).ServeWith is
			// running in a worker and usually sits on accept(pgL) which unblocks
			// only when pgL closes. In other words, pgL needs to close when
			// quiescing starts to allow that worker to shut down.
			_ = pgL.Close()
		}
		if err := args.stopper.RunAsyncTask(ctx, "wait-quiesce-pgl", waitQuiesce); err != nil {
			waitQuiesce(ctx)
			return nil, nil, "", "", err
		}
	}

	serverTLSConfig, err := args.rpcContext.GetUIServerTLSConfig()
	if err != nil {
		return nil, nil, "", "", err
	}
	httpL, err := ListenAndUpdateAddrs(ctx, &args.Config.HTTPAddr, &args.Config.HTTPAdvertiseAddr, "http")
	if err != nil {
		return nil, nil, "", "", err
	}
	if serverTLSConfig != nil {
		httpL = tls.NewListener(httpL, serverTLSConfig)
	}

	{
		waitQuiesce := func(ctx context.Context) {
			<-args.stopper.ShouldQuiesce()
			_ = httpL.Close()
		}
		if err := args.stopper.RunAsyncTask(ctx, "wait-quiesce-http", waitQuiesce); err != nil {
			waitQuiesce(ctx)
			return nil, nil, "", "", err
		}
	}
	pgLAddr := pgL.Addr().String()
	httpLAddr := httpL.Addr().String()
	args.advertiseAddr = baseCfg.AdvertiseAddr
	// The tenantStatusServer needs access to the sqlServer,
	// but we also need the same object to set up the sqlServer.
	// So construct the tenant status server with a nil sqlServer,
	// and then assign it once an SQL server gets created. We are
	// going to assume that the tenant status server won't require
	// the SQL server object.
	tenantStatusServer := newTenantStatusServer(
		baseCfg.AmbientCtx, &adminPrivilegeChecker{ie: args.circularInternalExecutor},
		args.sessionRegistry, args.contentionRegistry, args.flowScheduler, baseCfg.Settings, nil,
		args.rpcContext, args.stopper,
	)
	args.sqlStatusServer = tenantStatusServer
	s, err := newSQLServer(ctx, args)
	tenantStatusServer.sqlServer = s

	if err != nil {
		return nil, nil, "", "", err
	}

	// TODO(asubiotto): remove this. Right now it is needed to initialize the
	// SpanResolver.
	s.execCfg.DistSQLPlanner.SetNodeInfo(roachpb.NodeDescriptor{NodeID: 0})
	workersCtx := tenantStatusServer.AnnotateCtx(context.Background())

	authServer = newAuthenticationServer(baseCfg.Config, s)

	// Register and start gRPC service on pod. This is separate from the
	// gRPC + Gateway services configured below.
	for _, gw := range []grpcGatewayServer{tenantStatusServer, authServer} {
		gw.RegisterService(grpcMain.Server)
	}
	startRPCServer(background)

	// Begin configuration of GRPC Gateway
	gwMux, gwCtx, conn, err := ConfigureGRPCGateway(
		ctx,
		workersCtx,
		args.AmbientCtx,
		tenantStatusServer.rpcCtx,
		s.stopper,
		grpcMain,
		pgLAddr,
	)
	if err != nil {
		return nil, nil, "", "", err
	}

	for _, gw := range []grpcGatewayServer{tenantStatusServer, authServer} {
		if err := gw.RegisterGateway(gwCtx, gwMux, conn); err != nil {
			return nil, nil, "", "", err
		}
	}

	args.recorder.AddNode(
		args.registry,
		roachpb.NodeDescriptor{},
		timeutil.Now().UnixNano(),
		pgLAddr,   // advertised addr
		httpLAddr, // http addr
		pgLAddr,   // sql addr
	)

	debugServer := debug.NewServer(args.Settings, s.pgServer.HBADebugFn(), s.execCfg.SQLStatusServer)

	// Add HTTP authentication to the gRPC-gateway endpoints used by the UI,
	// if not disabled by configuration.
	var authenticatedHandler http.Handler = gwMux
	if args.RequireWebSession() {
		authenticatedHandler = newAuthenticationMux(authServer, authenticatedHandler)
	}

	// Add HTTP authentication and admin authorization to the debug endpoints.
	var handleDebugAuthenticated http.Handler = debugServer
	adminAuthzCheck := &adminPrivilegeChecker{ie: s.execCfg.InternalExecutor}
	if args.RequireWebSession() {
		// Mandate both authentication and admin authorization.
		handleDebugAuthenticated = makeAdminAuthzCheckHandler(adminAuthzCheck, handleDebugAuthenticated)
		handleDebugAuthenticated = newAuthenticationMux(authServer, handleDebugAuthenticated)
	}

	if err := args.stopper.RunAsyncTask(ctx, "serve-http", func(ctx context.Context) {
		mux := http.NewServeMux()
		mux.Handle(debug.Endpoint, handleDebugAuthenticated)
		mux.Handle("/_status/", authenticatedHandler)
		mux.HandleFunc("/health", func(w http.ResponseWriter, req *http.Request) {
			// Return Bad Request if called with arguments.
			if err := req.ParseForm(); err != nil || len(req.Form) != 0 {
				http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
				return
			}
		})
		f := varsHandler{metricSource: args.recorder, st: args.Settings}.handleVars
		mux.Handle(statusVars, http.HandlerFunc(f))
		ff := loadVarsHandler(ctx, args.runtime)
		mux.Handle(loadStatusVars, http.HandlerFunc(ff))

		tlsConnManager := netutil.MakeServer(
			args.stopper,
			serverTLSConfig, // tlsConfig
			mux,             // handler
		)

		netutil.FatalIfUnexpected(tlsConnManager.Serve(httpL))
	}); err != nil {
		return nil, nil, "", "", err
	}

	const (
		socketFile = "" // no unix socket
	)
	orphanedLeasesTimeThresholdNanos := args.clock.Now().WallTime

	// TODO(tbg): the log dir is not configurable at this point
	// since it is integrated too tightly with the `./cockroach start` command.
	if err := startSampleEnvironment(ctx, sampleEnvironmentCfg{
		st:                   args.Settings,
		stopper:              args.stopper,
		minSampleInterval:    base.DefaultMetricsSampleInterval,
		goroutineDumpDirName: args.GoroutineDumpDirName,
		heapProfileDirName:   args.HeapProfileDirName,
		runtime:              args.runtime,
		sessionRegistry:      args.sessionRegistry,
	}); err != nil {
		return nil, nil, "", "", err
	}

	if err := s.preStart(ctx,
		args.stopper,
		args.TestingKnobs,
		connManager,
		pgL,
		socketFile,
		orphanedLeasesTimeThresholdNanos,
	); err != nil {
		return nil, nil, "", "", err
	}

	// This is necessary so the grpc server doesn't error out on heartbeat
	// ping when we make pod-to-pod calls, we pass the InstanceID with the
	// request to ensure we're dialing the pod we think we are.
	//
	// The InstanceID subsystem is not available until `preStart`.
	args.rpcContext.NodeID.Set(ctx, roachpb.NodeID(s.SQLInstanceID()))

	// Register the server's identifiers so that log events are
	// decorated with the server's identity. This helps when gathering
	// log events from multiple servers into the same log collector.
	//
	// We do this only here, as the identifiers may not be known before this point.
	clusterID := args.rpcContext.ClusterID.Get().String()
	log.SetNodeIDs(clusterID, 0 /* nodeID is not known for a SQL-only server. */)
	log.SetTenantIDs(args.TenantID.String(), int32(s.SQLInstanceID()))

	externalUsageFn := func(ctx context.Context) multitenant.ExternalUsage {
		userTimeMillis, _, err := status.GetCPUTime(ctx)
		if err != nil {
			log.Ops.Errorf(ctx, "unable to get cpu usage: %v", err)
		}
		return multitenant.ExternalUsage{
			CPUSecs:           float64(userTimeMillis) * 1e-3,
			PGWireEgressBytes: s.pgServer.BytesOut(),
		}
	}

	nextLiveInstanceIDFn := makeNextLiveInstanceIDFn(
		ctx,
		args.stopper,
		s.sqlInstanceProvider,
		s.SQLInstanceID(),
	)

	if err := args.costController.Start(
		ctx, args.stopper, s.SQLInstanceID(), s.sqlLivenessSessionID,
		externalUsageFn, nextLiveInstanceIDFn,
	); err != nil {
		return nil, nil, "", "", err
	}

	if err := s.startServeSQL(ctx,
		args.stopper,
		s.connManager,
		s.pgL,
		socketFile); err != nil {
		return nil, nil, "", "", err
	}

	return s, authServer, pgLAddr, httpLAddr, nil
}

// This authz code for the debug endpoint was back-ported from v22.1.
func makeAdminAuthzCheckHandler(
	adminAuthzCheck *adminPrivilegeChecker, handler http.Handler,
) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// Retrieve the username embedded in the grpc metadata, if any.
		// This will be provided by the authenticationMux.
		md := forwardAuthenticationMetadata(req.Context(), req)
		authCtx := metadata.NewIncomingContext(req.Context(), md)
		// Check the privileges of the requester.
		_, err := adminAuthzCheck.requireAdminUser(authCtx)
		if errors.Is(err, errRequiresAdmin) {
			http.Error(w, "admin privilege required", http.StatusUnauthorized)
			return
		} else if err != nil {
			log.Ops.Infof(authCtx, "web session error: %s", err)
			http.Error(w, "error checking authentication", http.StatusInternalServerError)
			return
		}
		// Forward the request to the inner handler.
		handler.ServeHTTP(w, req)
	})
}

// Construct a handler responsible for serving the instant values of selected
// load metrics. These include user and system CPU time currently.
func loadVarsHandler(
	ctx context.Context, rsr *status.RuntimeStatSampler,
) func(http.ResponseWriter, *http.Request) {
	cpuUserNanos := metric.NewGauge(rsr.CPUUserNS.GetMetadata())
	cpuSysNanos := metric.NewGauge(rsr.CPUSysNS.GetMetadata())
	cpuNowNanos := metric.NewGauge(rsr.CPUNowNS.GetMetadata())
	registry := metric.NewRegistry()
	registry.AddMetric(cpuUserNanos)
	registry.AddMetric(cpuSysNanos)
	registry.AddMetric(cpuNowNanos)

	return func(w http.ResponseWriter, r *http.Request) {
		userTimeMillis, sysTimeMillis, err := status.GetCPUTime(ctx)
		if err != nil {
			// Just log but don't return an error to match the _status/vars metrics handler.
			log.Ops.Errorf(ctx, "unable to get cpu usage: %v", err)
		}

		// cpuTime.{User,Sys} are in milliseconds, convert to nanoseconds.
		utime := userTimeMillis * 1e6
		stime := sysTimeMillis * 1e6
		cpuUserNanos.Update(utime)
		cpuSysNanos.Update(stime)
		cpuNowNanos.Update(timeutil.Now().UnixNano())

		exporter := metric.MakePrometheusExporter()
		exporter.ScrapeRegistry(registry, true)
		if err := exporter.PrintAsText(w); err != nil {
			log.Errorf(r.Context(), "%v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}

func makeTenantSQLServerArgs(
	stopper *stop.Stopper, kvClusterName string, baseCfg BaseConfig, sqlCfg SQLConfig,
) (sqlServerArgs, error) {
	st := baseCfg.Settings
	baseCfg.AmbientCtx.AddLogTag("sql", nil)
	// TODO(tbg): this is needed so that the RPC heartbeats between the testcluster
	// and this tenant work.
	//
	// TODO(tbg): address this when we introduce the real tenant RPCs in:
	// https://github.com/cockroachdb/cockroach/issues/47898
	baseCfg.ClusterName = kvClusterName

	clock := hlc.NewClock(hlc.UnixNano, time.Duration(baseCfg.MaxOffset))

	registry := metric.NewRegistry()

	var rpcTestingKnobs rpc.ContextTestingKnobs
	if p, ok := baseCfg.TestingKnobs.Server.(*TestingKnobs); ok {
		rpcTestingKnobs = p.ContextTestingKnobs
	}
	rpcContext := rpc.NewContext(rpc.ContextOptions{
		TenantID:   sqlCfg.TenantID,
		AmbientCtx: baseCfg.AmbientCtx,
		Config:     baseCfg.Config,
		Clock:      clock,
		Stopper:    stopper,
		Settings:   st,
		Knobs:      rpcTestingKnobs,
	})

	var dsKnobs kvcoord.ClientTestingKnobs
	if dsKnobsP, ok := baseCfg.TestingKnobs.DistSQL.(*kvcoord.ClientTestingKnobs); ok {
		dsKnobs = *dsKnobsP
	}
	rpcRetryOptions := base.DefaultRetryOptions()

	tcCfg := kvtenant.ConnectorConfig{
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
	rangeFeedFactory, err := rangefeed.NewFactory(stopper, db, rangeFeedKnobs)
	if err != nil {
		return sqlServerArgs{}, err
	}

	circularInternalExecutor := &sql.InternalExecutor{}
	// Protected timestamps won't be available (at first) in multi-tenant
	// clusters.
	var protectedTSProvider protectedts.Provider
	{
		pp, err := ptprovider.New(ptprovider.Config{
			DB:               db,
			InternalExecutor: circularInternalExecutor,
			Settings:         st,
		})
		if err != nil {
			panic(err)
		}
		protectedTSProvider = dummyProtectedTSProvider{pp}
	}

	recorder := status.NewMetricsRecorder(clock, nil, rpcContext, nil, st)

	runtime := status.NewRuntimeStatSampler(context.Background(), clock)
	registry.AddMetricStruct(runtime)

	esb := &externalStorageBuilder{}
	externalStorage := func(ctx context.Context, dest roachpb.ExternalStorage) (cloud.
		ExternalStorage, error) {
		return esb.makeExternalStorage(ctx, dest)
	}
	externalStorageFromURI := func(ctx context.Context, uri string,
		user security.SQLUsername) (cloud.ExternalStorage, error) {
		return esb.makeExternalStorageFromURI(ctx, uri, user)
	}

	var blobClientFactory blobs.BlobClientFactory
	if p, ok := baseCfg.TestingKnobs.Server.(*TestingKnobs); ok && p.TenantBlobClientFactory != nil {
		blobClientFactory = p.TenantBlobClientFactory
	}
	esb.init(sqlCfg.ExternalIODirConfig, baseCfg.Settings, blobClientFactory, circularInternalExecutor, db)

	// We don't need this for anything except some services that want a gRPC
	// server to register against (but they'll never get RPCs at the time of
	// writing): the blob service and DistSQL.
	dummyRPCServer := rpc.NewServer(rpcContext)
	sessionRegistry := sql.NewSessionRegistry()
	contentionRegistry := contention.NewRegistry()
	flowScheduler := flowinfra.NewFlowScheduler(baseCfg.AmbientCtx, stopper, st)
	return sqlServerArgs{
		sqlServerOptionalKVArgs: sqlServerOptionalKVArgs{
			nodesStatusServer: serverpb.MakeOptionalNodesStatusServer(nil),
			nodeLiveness:      optionalnodeliveness.MakeContainer(nil),
			gossip:            gossip.MakeOptionalGossip(nil),
			grpcServer:        dummyRPCServer,
			isMeta1Leaseholder: func(_ context.Context, _ hlc.ClockTimestamp) (bool, error) {
				return false, errors.New("isMeta1Leaseholder is not available to secondary tenants")
			},
			externalStorage:        externalStorage,
			externalStorageFromURI: externalStorageFromURI,
			// Set instance ID to 0 and node ID to nil to indicate
			// that the instance ID will be bound later during preStart.
			nodeIDContainer:  base.NewSQLIDContainer(0, nil),
			kvStoresIterator: kvserverbase.UnsupportedStoresIterator{},
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
		systemConfigProvider:     tenantConnect,
		spanConfigAccessor:       tenantConnect,
		nodeDialer:               nodeDialer,
		distSender:               ds,
		db:                       db,
		registry:                 registry,
		recorder:                 recorder,
		sessionRegistry:          sessionRegistry,
		contentionRegistry:       contentionRegistry,
		flowScheduler:            flowScheduler,
		circularInternalExecutor: circularInternalExecutor,
		circularJobRegistry:      &jobs.Registry{},
		protectedtsProvider:      protectedTSProvider,
		rangeFeedFactory:         rangeFeedFactory,
		regionsServer:            tenantConnect,
		costController:           costController,
	}, nil
}

func makeNextLiveInstanceIDFn(
	serverCtx context.Context,
	stopper *stop.Stopper,
	sqlInstanceProvider sqlinstance.Provider,
	instanceID base.SQLInstanceID,
) multitenant.NextLiveInstanceIDFn {
	retrieveNextLiveInstanceID := func(ctx context.Context) base.SQLInstanceID {
		instances, err := sqlInstanceProvider.GetAllInstances(ctx)
		if err != nil {
			log.Infof(ctx, "GetAllInstances failed: %v", err)
			// We will try again.
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

	// We retrieve the value from the provider every minute.
	//
	// We report each retrieved value only once; for all other calls we return 0.
	// We prefer to not provide a value rather than providing a stale value which
	// might cause a bit of unnecessary work on the server side.
	//
	// TODO(radu): once the provider caches the information (see #69976), we can
	// use it directly each time.
	const interval = 1 * time.Minute
	var mu syncutil.Mutex
	var lastRefresh time.Time
	var lastValue base.SQLInstanceID
	var refreshInProgress bool

	serverCtx = logtags.AddTag(serverCtx, "get-next-live-instance-id", nil)

	return func(ctx context.Context) base.SQLInstanceID {
		mu.Lock()
		defer mu.Unlock()
		if lastValue != 0 {
			v := lastValue
			lastValue = 0
			return v
		}

		if now := timeutil.Now(); lastRefresh.Before(now.Add(-interval)) && !refreshInProgress {
			lastRefresh = now
			refreshInProgress = true

			// An error here indicates that the server is shutting down, so we can
			// ignore it.
			_ = stopper.RunAsyncTask(serverCtx, "get-next-live-instance-id", func(ctx context.Context) {
				newValue := retrieveNextLiveInstanceID(ctx)

				mu.Lock()
				defer mu.Unlock()
				lastValue = newValue
				refreshInProgress = false
			})
		}
		return 0
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

func (noopTenantSideCostController) OnRequestWait(
	ctx context.Context, info tenantcostmodel.RequestInfo,
) error {
	return nil
}

func (noopTenantSideCostController) OnResponse(
	ctx context.Context, req tenantcostmodel.RequestInfo, resp tenantcostmodel.ResponseInfo,
) {
}

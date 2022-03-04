// Copyright 2014 The Cockroach Authors.
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
	"io/ioutil"
	"net/http"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvprober"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/sidetransport"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptprovider"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptreconcile"
	serverrangefeed "github.com/cockroachdb/cockroach/pkg/kv/kvserver/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/reports"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/diagnostics"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/server/systemconfigwatcher"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/server/tenantsettingswatcher"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	_ "github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigjob" // register jobs declared outside of pkg/sql
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigkvaccessor"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigkvsubscriber"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigptsreader"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	_ "github.com/cockroachdb/cockroach/pkg/sql/gcjob"    // register jobs declared outside of pkg/sql
	_ "github.com/cockroachdb/cockroach/pkg/sql/importer" // register jobs/planHooks declared outside of pkg/sql
	"github.com/cockroachdb/cockroach/pkg/sql/optionalnodeliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	_ "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scjob" // register jobs declared outside of pkg/sql
	_ "github.com/cockroachdb/cockroach/pkg/sql/ttl/ttljob"          // register jobs declared outside of pkg/sql
	_ "github.com/cockroachdb/cockroach/pkg/sql/ttl/ttlschedule"     // register schedules declared outside of pkg/sql
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/goschedstats"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	sentry "github.com/getsentry/sentry-go"
	"google.golang.org/grpc/codes"
)

// Server is the cockroach server node.
type Server struct {
	// The following fields are populated in NewServer.

	nodeIDContainer *base.NodeIDContainer
	cfg             Config
	st              *cluster.Settings
	clock           *hlc.Clock
	rpcContext      *rpc.Context
	engines         Engines
	// The gRPC server on which the different RPC handlers will be registered.
	grpc             *grpcServer
	gossip           *gossip.Gossip
	nodeDialer       *nodedialer.Dialer
	nodeLiveness     *liveness.NodeLiveness
	storePool        *kvserver.StorePool
	tcsFactory       *kvcoord.TxnCoordSenderFactory
	distSender       *kvcoord.DistSender
	db               *kv.DB
	node             *Node
	registry         *metric.Registry
	recorder         *status.MetricsRecorder
	runtime          *status.RuntimeStatSampler
	ruleRegistry     *metric.RuleRegistry
	promRuleExporter *metric.PrometheusRuleExporter
	updates          *diagnostics.UpdateChecker
	ctSender         *sidetransport.Sender

	http            *httpServer
	adminAuthzCheck *adminPrivilegeChecker
	admin           *adminServer
	status          *statusServer
	drain           *drainServer
	authentication  *authenticationServer
	migrationServer *migrationServer
	tsDB            *ts.DB
	tsServer        *ts.Server
	raftTransport   *kvserver.RaftTransport
	stopper         *stop.Stopper

	debug    *debug.Server
	kvProber *kvprober.Prober

	replicationReporter *reports.Reporter
	protectedtsProvider protectedts.Provider

	spanConfigSubscriber spanconfig.KVSubscriber

	sqlServer *SQLServer

	// Created in NewServer but initialized (made usable) in `(*Server).Start`.
	externalStorageBuilder *externalStorageBuilder

	storeGrantCoords *admission.StoreGrantCoordinators
	// kvMemoryMonitor is a child of the rootSQLMemoryMonitor and is used to
	// account for and bound the memory used for request processing in the KV
	// layer.
	kvMemoryMonitor *mon.BytesMonitor

	// The following fields are populated at start time, i.e. in `(*Server).Start`.
	startTime time.Time
}

// NewServer creates a Server from a server.Config.
func NewServer(cfg Config, stopper *stop.Stopper) (*Server, error) {
	if err := cfg.ValidateAddrs(context.Background()); err != nil {
		return nil, err
	}

	st := cfg.Settings

	if cfg.AmbientCtx.Tracer == nil {
		panic(errors.New("no tracer set in AmbientCtx"))
	}

	var clock *hlc.Clock
	if cfg.ClockDevicePath != "" {
		clockSrc, err := hlc.MakeClockSource(context.Background(), cfg.ClockDevicePath)
		if err != nil {
			return nil, errors.Wrap(err, "instantiating clock source")
		}
		clock = hlc.NewClock(clockSrc.UnixNano, time.Duration(cfg.MaxOffset))
	} else if cfg.TestingKnobs.Server != nil &&
		cfg.TestingKnobs.Server.(*TestingKnobs).ClockSource != nil {
		clock = hlc.NewClock(cfg.TestingKnobs.Server.(*TestingKnobs).ClockSource,
			time.Duration(cfg.MaxOffset))
	} else {
		clock = hlc.NewClock(hlc.UnixNano, time.Duration(cfg.MaxOffset))
	}
	registry := metric.NewRegistry()
	ruleRegistry := metric.NewRuleRegistry()
	promRuleExporter := metric.NewPrometheusRuleExporter(ruleRegistry)
	stopper.SetTracer(cfg.AmbientCtx.Tracer)
	stopper.AddCloser(cfg.AmbientCtx.Tracer)

	// Add a dynamic log tag value for the node ID.
	//
	// We need to pass an ambient context to the various server components, but we
	// won't know the node ID until we Start(). At that point it's too late to
	// change the ambient contexts in the components (various background processes
	// will have already started using them).
	//
	// NodeIDContainer allows us to add the log tag to the context now and update
	// the value asynchronously. It's not significantly more expensive than a
	// regular tag since it's just doing an (atomic) load when a log/trace message
	// is constructed. The node ID is set by the Store if this host was
	// bootstrapped; otherwise a new one is allocated in Node.
	nodeIDContainer := cfg.IDContainer
	idContainer := base.NewSQLIDContainerForNode(nodeIDContainer)

	ctx := cfg.AmbientCtx.AnnotateCtx(context.Background())

	engines, err := cfg.CreateEngines(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create engines")
	}
	stopper.AddCloser(&engines)

	nodeTombStorage, checkPingFor := getPingCheckDecommissionFn(engines)

	rpcCtxOpts := rpc.ContextOptions{
		TenantID:  roachpb.SystemTenantID,
		NodeID:    cfg.IDContainer,
		ClusterID: cfg.ClusterIDContainer,
		Config:    cfg.Config,
		Clock:     clock,
		Stopper:   stopper,
		Settings:  cfg.Settings,
		OnOutgoingPing: func(ctx context.Context, req *rpc.PingRequest) error {
			// Outgoing ping will block requests with codes.FailedPrecondition to
			// notify caller that this replica is decommissioned but others could
			// still be tried as caller node is valid, but not the destination.
			return checkPingFor(ctx, req.TargetNodeID, codes.FailedPrecondition)
		},
		OnIncomingPing: func(ctx context.Context, req *rpc.PingRequest) error {
			// Decommission state is only tracked for the system tenant.
			if tenantID, isTenant := roachpb.TenantFromContext(ctx); isTenant &&
				!roachpb.IsSystemTenantID(tenantID.ToUint64()) {
				return nil
			}
			// Incoming ping will reject requests with codes.PermissionDenied to
			// signal remote node that it is not considered valid anymore and
			// operations should fail immediately.
			return checkPingFor(ctx, req.OriginNodeID, codes.PermissionDenied)
		},
	}
	if knobs := cfg.TestingKnobs.Server; knobs != nil {
		serverKnobs := knobs.(*TestingKnobs)
		rpcCtxOpts.Knobs = serverKnobs.ContextTestingKnobs
	}
	rpcContext := rpc.NewContext(ctx, rpcCtxOpts)

	rpcContext.HeartbeatCB = func() {
		if err := rpcContext.RemoteClocks.VerifyClockOffset(ctx); err != nil {
			log.Ops.Fatalf(ctx, "%v", err)
		}
	}
	registry.AddMetricStruct(rpcContext.Metrics())

	// Attempt to load TLS configs right away, failures are permanent.
	if !cfg.Insecure {
		// TODO(peter): Call methods on CertificateManager directly. Need to call
		// base.wrapError or similar on the resulting error.
		if _, err := rpcContext.GetServerTLSConfig(); err != nil {
			return nil, err
		}
		if _, err := rpcContext.GetUIServerTLSConfig(); err != nil {
			return nil, err
		}
		if _, err := rpcContext.GetClientTLSConfig(); err != nil {
			return nil, err
		}
		cm, err := rpcContext.GetCertificateManager()
		if err != nil {
			return nil, err
		}
		cm.RegisterSignalHandler(stopper)
		registry.AddMetricStruct(cm.Metrics())
	}

	// Check the compatibility between the configured addresses and that
	// provided in certificates. This also logs the certificate
	// addresses in all cases to aid troubleshooting.
	// This must be called after the certificate manager was initialized
	// and after ValidateAddrs().
	rpcContext.CheckCertificateAddrs(ctx)

	grpcServer := newGRPCServer(rpcContext)

	g := gossip.New(
		cfg.AmbientCtx,
		rpcContext.ClusterID,
		nodeIDContainer,
		rpcContext,
		grpcServer.Server,
		stopper,
		registry,
		cfg.Locality,
		&cfg.DefaultZoneConfig,
	)

	var dialerKnobs nodedialer.DialerTestingKnobs
	if dk := cfg.TestingKnobs.DialerKnobs; dk != nil {
		dialerKnobs = dk.(nodedialer.DialerTestingKnobs)
	}

	nodeDialer := nodedialer.NewWithOpt(rpcContext, gossip.AddressResolver(g),
		nodedialer.DialerOpt{TestingKnobs: dialerKnobs})

	runtimeSampler := status.NewRuntimeStatSampler(ctx, clock)
	registry.AddMetricStruct(runtimeSampler)

	registry.AddMetric(base.LicenseTTL)
	err = base.UpdateMetricOnLicenseChange(ctx, cfg.Settings, base.LicenseTTL, timeutil.DefaultTimeSource{}, stopper)
	if err != nil {
		log.Errorf(ctx, "unable to initialize periodic license metric update: %v", err)
	}

	// Create and add KV metric rules
	kvserver.CreateAndAddRules(ctx, ruleRegistry)

	// A custom RetryOptions is created which uses stopper.ShouldQuiesce() as
	// the Closer. This prevents infinite retry loops from occurring during
	// graceful server shutdown
	//
	// Such a loop occurs when the DistSender attempts a connection to the
	// local server during shutdown, and receives an internal server error (HTTP
	// Code 5xx). This is the correct error for a server to return when it is
	// shutting down, and is normally retryable in a cluster environment.
	// However, on a single-node setup (such as a test), retries will never
	// succeed because the only server has been shut down; thus, the
	// DistSender needs to know that it should not retry in this situation.
	var clientTestingKnobs kvcoord.ClientTestingKnobs
	if kvKnobs := cfg.TestingKnobs.KVClient; kvKnobs != nil {
		clientTestingKnobs = *kvKnobs.(*kvcoord.ClientTestingKnobs)
	}
	retryOpts := cfg.RetryOptions
	if retryOpts == (retry.Options{}) {
		retryOpts = base.DefaultRetryOptions()
	}
	retryOpts.Closer = stopper.ShouldQuiesce()
	distSenderCfg := kvcoord.DistSenderConfig{
		AmbientCtx:         cfg.AmbientCtx,
		Settings:           st,
		Clock:              clock,
		NodeDescs:          g,
		RPCContext:         rpcContext,
		RPCRetryOptions:    &retryOpts,
		NodeDialer:         nodeDialer,
		FirstRangeProvider: g,
		TestingKnobs:       clientTestingKnobs,
	}
	distSender := kvcoord.NewDistSender(distSenderCfg)
	registry.AddMetricStruct(distSender.Metrics())

	txnMetrics := kvcoord.MakeTxnMetrics(cfg.HistogramWindowInterval())
	registry.AddMetricStruct(txnMetrics)
	txnCoordSenderFactoryCfg := kvcoord.TxnCoordSenderFactoryConfig{
		AmbientCtx:   cfg.AmbientCtx,
		Settings:     st,
		Clock:        clock,
		Stopper:      stopper,
		Linearizable: cfg.Linearizable,
		Metrics:      txnMetrics,
		TestingKnobs: clientTestingKnobs,
	}
	tcsFactory := kvcoord.NewTxnCoordSenderFactory(txnCoordSenderFactoryCfg, distSender)

	admissionOptions := admission.DefaultOptions
	if opts, ok := cfg.TestingKnobs.AdmissionControl.(*admission.Options); ok {
		admissionOptions.Override(opts)
	}
	admissionOptions.Settings = st
	gcoords, metrics := admission.NewGrantCoordinators(cfg.AmbientCtx, admissionOptions)
	for i := range metrics {
		registry.AddMetricStruct(metrics[i])
	}
	cbID := goschedstats.RegisterRunnableCountCallback(gcoords.Regular.CPULoad)
	stopper.AddCloser(stop.CloserFn(func() {
		goschedstats.UnregisterRunnableCountCallback(cbID)
	}))
	stopper.AddCloser(gcoords)

	dbCtx := kv.DefaultDBContext(stopper)
	dbCtx.NodeID = idContainer
	dbCtx.Stopper = stopper
	db := kv.NewDBWithContext(cfg.AmbientCtx, tcsFactory, clock, dbCtx)
	db.SQLKVResponseAdmissionQ = gcoords.Regular.GetWorkQueue(admission.SQLKVResponseWork)

	nlActive, nlRenewal := cfg.NodeLivenessDurations()
	if knobs := cfg.TestingKnobs.NodeLiveness; knobs != nil {
		nlKnobs := knobs.(kvserver.NodeLivenessTestingKnobs)
		if duration := nlKnobs.LivenessDuration; duration != 0 {
			nlActive = duration
		}
		if duration := nlKnobs.RenewalDuration; duration != 0 {
			nlRenewal = duration
		}
	}

	rangeFeedKnobs, _ := cfg.TestingKnobs.RangeFeed.(*rangefeed.TestingKnobs)
	rangeFeedFactory, err := rangefeed.NewFactory(stopper, db, st, rangeFeedKnobs)
	if err != nil {
		return nil, err
	}

	nodeLiveness := liveness.NewNodeLiveness(liveness.NodeLivenessOptions{
		AmbientCtx:              cfg.AmbientCtx,
		Clock:                   clock,
		DB:                      db,
		Gossip:                  g,
		LivenessThreshold:       nlActive,
		RenewalDuration:         nlRenewal,
		Settings:                st,
		HistogramWindowInterval: cfg.HistogramWindowInterval(),
		OnNodeDecommissioned: func(liveness livenesspb.Liveness) {
			if knobs, ok := cfg.TestingKnobs.Server.(*TestingKnobs); ok && knobs.OnDecommissionedCallback != nil {
				knobs.OnDecommissionedCallback(liveness)
			}
			if err := nodeTombStorage.SetDecommissioned(
				ctx, liveness.NodeID, timeutil.Unix(0, liveness.Expiration.WallTime).UTC(),
			); err != nil {
				log.Fatalf(ctx, "unable to add tombstone for n%d: %s", liveness.NodeID, err)
			}
		},
	})
	registry.AddMetricStruct(nodeLiveness.Metrics())

	nodeLivenessFn := kvserver.MakeStorePoolNodeLivenessFunc(nodeLiveness)
	if nodeLivenessKnobs, ok := cfg.TestingKnobs.Store.(*kvserver.NodeLivenessTestingKnobs); ok &&
		nodeLivenessKnobs.StorePoolNodeLivenessFn != nil {
		nodeLivenessFn = nodeLivenessKnobs.StorePoolNodeLivenessFn
	}
	storePool := kvserver.NewStorePool(
		cfg.AmbientCtx,
		st,
		g,
		clock,
		nodeLiveness.GetNodeCount,
		nodeLivenessFn,
		/* deterministic */ false,
	)

	raftTransport := kvserver.NewRaftTransport(
		cfg.AmbientCtx, st, nodeDialer, grpcServer.Server, stopper,
	)

	ctSender := sidetransport.NewSender(stopper, st, clock, nodeDialer)
	stores := kvserver.NewStores(cfg.AmbientCtx, clock)
	ctReceiver := sidetransport.NewReceiver(nodeIDContainer, stopper, stores, nil /* testingKnobs */)

	// The InternalExecutor will be further initialized later, as we create more
	// of the server's components. There's a circular dependency - many things
	// need an InternalExecutor, but the InternalExecutor needs an xecutorConfig,
	// which in turn needs many things. That's why everybody that needs an
	// InternalExecutor uses this one instance.
	internalExecutor := &sql.InternalExecutor{}
	jobRegistry := &jobs.Registry{} // ditto

	// Create an ExternalStorageBuilder. This is only usable after Start() where
	// we initialize all the configuration params.
	externalStorageBuilder := &externalStorageBuilder{}
	externalStorage := externalStorageBuilder.makeExternalStorage
	externalStorageFromURI := externalStorageBuilder.makeExternalStorageFromURI

	protectedtsKnobs, _ := cfg.TestingKnobs.ProtectedTS.(*protectedts.TestingKnobs)
	protectedtsProvider, err := ptprovider.New(ptprovider.Config{
		DB:               db,
		InternalExecutor: internalExecutor,
		Settings:         st,
		Knobs:            protectedtsKnobs,
		ReconcileStatusFuncs: ptreconcile.StatusFuncs{
			jobsprotectedts.GetMetaType(jobsprotectedts.Jobs): jobsprotectedts.MakeStatusFunc(
				jobRegistry, internalExecutor, jobsprotectedts.Jobs),
			jobsprotectedts.GetMetaType(jobsprotectedts.Schedules): jobsprotectedts.MakeStatusFunc(jobRegistry,
				internalExecutor, jobsprotectedts.Schedules),
		},
	})
	if err != nil {
		return nil, err
	}
	registry.AddMetricStruct(protectedtsProvider.Metrics())

	// Break a circular dependency: we need the rootSQLMemoryMonitor to construct
	// the KV memory monitor for the StoreConfig.
	sqlMonitorAndMetrics := newRootSQLMemoryMonitor(monitorAndMetricsOptions{
		memoryPoolSize:          cfg.MemoryPoolSize,
		histogramWindowInterval: cfg.HistogramWindowInterval(),
		settings:                cfg.Settings,
	})
	kvMemoryMonitor := mon.NewMonitorInheritWithLimit(
		"kv-mem", 0 /* limit */, sqlMonitorAndMetrics.rootSQLMemoryMonitor)
	kvMemoryMonitor.Start(ctx, sqlMonitorAndMetrics.rootSQLMemoryMonitor, mon.BoundAccount{})
	rangeReedBudgetFactory := serverrangefeed.NewBudgetFactory(ctx, kvMemoryMonitor, cfg.MemoryPoolSize,
		cfg.HistogramWindowInterval())
	if rangeReedBudgetFactory != nil {
		registry.AddMetricStruct(rangeReedBudgetFactory.Metrics())
	}
	// Closer order is important with BytesMonitor.
	stopper.AddCloser(stop.CloserFn(func() {
		rangeReedBudgetFactory.Stop(ctx)
	}))
	stopper.AddCloser(stop.CloserFn(func() {
		kvMemoryMonitor.Stop(ctx)
	}))

	tsDB := ts.NewDB(db, cfg.Settings)
	registry.AddMetricStruct(tsDB.Metrics())
	nodeCountFn := func() int64 {
		return nodeLiveness.Metrics().LiveNodes.Value()
	}
	sTS := ts.MakeServer(
		cfg.AmbientCtx, tsDB, nodeCountFn, cfg.TimeSeriesServerConfig,
		sqlMonitorAndMetrics.rootSQLMemoryMonitor, stopper,
	)

	systemConfigWatcher := systemconfigwatcher.New(
		keys.SystemSQLCodec, clock, rangeFeedFactory, &cfg.DefaultZoneConfig,
	)

	var spanConfig struct {
		// kvAccessor powers the span configuration RPCs and the host tenant's
		// reconciliation job.
		kvAccessor spanconfig.KVAccessor
		// subscriber is used by stores to subscribe to span configuration updates.
		subscriber spanconfig.KVSubscriber
		// kvAccessorForTenantRecords is when creating/destroying secondary
		// tenant records.
		kvAccessorForTenantRecords spanconfig.KVAccessor
	}
	if !cfg.SpanConfigsDisabled {
		spanConfigKnobs, _ := cfg.TestingKnobs.SpanConfig.(*spanconfig.TestingKnobs)
		if spanConfigKnobs != nil && spanConfigKnobs.StoreKVSubscriberOverride != nil {
			spanConfig.subscriber = spanConfigKnobs.StoreKVSubscriberOverride
		} else {
			// We use the span configs infra to control whether rangefeeds are
			// enabled on a given range. At the moment this only applies to
			// system tables (on both host and secondary tenants). We need to
			// consider two things:
			// - The sql-side reconciliation process runs asynchronously. When
			//   the config for a given range is requested, we might not yet have
			//   it, thus falling back to the static config below.
			// - Various internal subsystems rely on rangefeeds to function.
			//
			// Consequently, we configure our static fallback config to actually
			// allow rangefeeds. As the sql-side reconciliation process kicks
			// off, it'll install the actual configs that we'll later consult.
			// For system table ranges we install configs that allow for
			// rangefeeds. Until then, we simply allow rangefeeds when a more
			// targeted config is not found.
			fallbackConf := cfg.DefaultZoneConfig.AsSpanConfig()
			fallbackConf.RangefeedEnabled = true
			// We do the same for opting out of strict GC enforcement; it
			// really only applies to user table ranges
			fallbackConf.GCPolicy.IgnoreStrictEnforcement = true

			spanConfig.subscriber = spanconfigkvsubscriber.New(
				clock,
				rangeFeedFactory,
				keys.SpanConfigurationsTableID,
				1<<20, /* 1 MB */
				fallbackConf,
				spanConfigKnobs,
			)
		}

		scKVAccessor := spanconfigkvaccessor.New(
			db, internalExecutor, cfg.Settings,
			systemschema.SpanConfigurationsTableName.FQString(),
		)
		spanConfig.kvAccessor, spanConfig.kvAccessorForTenantRecords = scKVAccessor, scKVAccessor
	} else {
		// If the spanconfigs infrastructure is disabled, there should be no
		// reconciliation jobs or RPCs issued against the infrastructure. Plug
		// in a disabled spanconfig.KVAccessor that would error out for
		// unexpected use.
		spanConfig.kvAccessor = spanconfigkvaccessor.DisabledKVAccessor

		// Use a no-op accessor where tenant records are created/destroyed.
		spanConfig.kvAccessorForTenantRecords = spanconfigkvaccessor.NoopKVAccessor

		spanConfig.subscriber = spanconfigkvsubscriber.NewNoopSubscriber(clock)
	}

	var protectedTSReader spanconfig.ProtectedTSReader
	if cfg.TestingKnobs.SpanConfig != nil &&
		cfg.TestingKnobs.SpanConfig.(*spanconfig.TestingKnobs).ProtectedTSReaderOverrideFn != nil {
		fn := cfg.TestingKnobs.SpanConfig.(*spanconfig.TestingKnobs).ProtectedTSReaderOverrideFn
		protectedTSReader = fn(clock)
	} else {
		protectedTSReader = spanconfigptsreader.NewAdapter(protectedtsProvider.(*ptprovider.Provider).Cache, spanConfig.subscriber)
	}

	storeCfg := kvserver.StoreConfig{
		DefaultSpanConfig:        cfg.DefaultZoneConfig.AsSpanConfig(),
		Settings:                 st,
		AmbientCtx:               cfg.AmbientCtx,
		RaftConfig:               cfg.RaftConfig,
		Clock:                    clock,
		DB:                       db,
		Gossip:                   g,
		NodeLiveness:             nodeLiveness,
		Transport:                raftTransport,
		NodeDialer:               nodeDialer,
		RPCContext:               rpcContext,
		ScanInterval:             cfg.ScanInterval,
		ScanMinIdleTime:          cfg.ScanMinIdleTime,
		ScanMaxIdleTime:          cfg.ScanMaxIdleTime,
		HistogramWindowInterval:  cfg.HistogramWindowInterval(),
		StorePool:                storePool,
		SQLExecutor:              internalExecutor,
		LogRangeEvents:           cfg.EventLogEnabled,
		RangeDescriptorCache:     distSender.RangeDescriptorCache(),
		TimeSeriesDataStore:      tsDB,
		ClosedTimestampSender:    ctSender,
		ClosedTimestampReceiver:  ctReceiver,
		ExternalStorage:          externalStorage,
		ExternalStorageFromURI:   externalStorageFromURI,
		ProtectedTimestampReader: protectedTSReader,
		KVMemoryMonitor:          kvMemoryMonitor,
		RangefeedBudgetFactory:   rangeReedBudgetFactory,
		SystemConfigProvider:     systemConfigWatcher,
		SpanConfigSubscriber:     spanConfig.subscriber,
		SpanConfigsDisabled:      cfg.SpanConfigsDisabled,
	}

	if storeTestingKnobs := cfg.TestingKnobs.Store; storeTestingKnobs != nil {
		storeCfg.TestingKnobs = *storeTestingKnobs.(*kvserver.StoreTestingKnobs)
	}

	recorder := status.NewMetricsRecorder(clock, nodeLiveness, rpcContext, g, st)
	registry.AddMetricStruct(rpcContext.RemoteClocks.Metrics())

	updates := &diagnostics.UpdateChecker{
		StartTime:     timeutil.Now(),
		AmbientCtx:    &cfg.AmbientCtx,
		Config:        cfg.BaseConfig.Config,
		Settings:      cfg.Settings,
		ClusterID:     cfg.ClusterIDContainer.Get,
		NodeID:        nodeIDContainer.Get,
		SQLInstanceID: idContainer.SQLInstanceID,
	}

	if cfg.TestingKnobs.Server != nil {
		updates.TestingKnobs = &cfg.TestingKnobs.Server.(*TestingKnobs).DiagnosticsTestingKnobs
	}

	tenantUsage := NewTenantUsageServer(st, db, internalExecutor)
	registry.AddMetricStruct(tenantUsage.Metrics())

	tenantSettingsWatcher := tenantsettingswatcher.New(
		clock, rangeFeedFactory, stopper, st,
	)

	node := NewNode(
		storeCfg, recorder, registry, stopper,
		txnMetrics, stores, nil /* execCfg */, cfg.ClusterIDContainer,
		gcoords.Regular.GetWorkQueue(admission.KVWork), gcoords.Stores,
		tenantUsage, tenantSettingsWatcher, spanConfig.kvAccessor,
	)
	roachpb.RegisterInternalServer(grpcServer.Server, node)
	kvserver.RegisterPerReplicaServer(grpcServer.Server, node.perReplicaServer)
	kvserver.RegisterPerStoreServer(grpcServer.Server, node.perReplicaServer)
	ctpb.RegisterSideTransportServer(grpcServer.Server, ctReceiver)

	replicationReporter := reports.NewReporter(
		db, node.stores, storePool, st, nodeLiveness, internalExecutor, systemConfigWatcher,
	)

	lateBoundServer := &Server{}
	// TODO(tbg): give adminServer only what it needs (and avoid circular deps).
	adminAuthzCheck := &adminPrivilegeChecker{ie: internalExecutor}
	sAdmin := newAdminServer(lateBoundServer, adminAuthzCheck, internalExecutor)

	// These callbacks help us avoid a dependency on gossip in httpServer.
	parseNodeIDFn := func(s string) (roachpb.NodeID, bool, error) {
		return parseNodeID(g, s)
	}
	getNodeIDHTTPAddressFn := func(id roachpb.NodeID) (*util.UnresolvedAddr, error) {
		return g.GetNodeIDHTTPAddress(id)
	}
	sHTTP := newHTTPServer(cfg.BaseConfig, rpcContext, parseNodeIDFn, getNodeIDHTTPAddressFn)

	sessionRegistry := sql.NewSessionRegistry()
	flowScheduler := flowinfra.NewFlowScheduler(cfg.AmbientCtx, stopper, st)

	sStatus := newStatusServer(
		cfg.AmbientCtx,
		st,
		cfg.Config,
		adminAuthzCheck,
		sAdmin,
		db,
		g,
		recorder,
		nodeLiveness,
		storePool,
		rpcContext,
		node.stores,
		stopper,
		sessionRegistry,
		flowScheduler,
		internalExecutor,
	)

	var jobAdoptionStopFile string
	for _, spec := range cfg.Stores.Specs {
		if !spec.InMemory && spec.Path != "" {
			jobAdoptionStopFile = filepath.Join(spec.Path, jobs.PreventAdoptionFile)
			break
		}
	}

	kvProber := kvprober.NewProber(kvprober.Opts{
		Tracer:                  cfg.AmbientCtx.Tracer,
		DB:                      db,
		Settings:                st,
		HistogramWindowInterval: cfg.HistogramWindowInterval(),
	})
	registry.AddMetricStruct(kvProber.Metrics())

	settingsWriter := newSettingsCacheWriter(engines[0], stopper)
	sqlServer, err := newSQLServer(ctx, sqlServerArgs{
		sqlServerOptionalKVArgs: sqlServerOptionalKVArgs{
			nodesStatusServer:        serverpb.MakeOptionalNodesStatusServer(sStatus),
			nodeLiveness:             optionalnodeliveness.MakeContainer(nodeLiveness),
			gossip:                   gossip.MakeOptionalGossip(g),
			grpcServer:               grpcServer.Server,
			nodeIDContainer:          idContainer,
			externalStorage:          externalStorage,
			externalStorageFromURI:   externalStorageFromURI,
			isMeta1Leaseholder:       node.stores.IsMeta1Leaseholder,
			sqlSQLResponseAdmissionQ: gcoords.Regular.GetWorkQueue(admission.SQLSQLResponseWork),
			spanConfigKVAccessor:     spanConfig.kvAccessorForTenantRecords,
		},
		SQLConfig:                &cfg.SQLConfig,
		BaseConfig:               &cfg.BaseConfig,
		stopper:                  stopper,
		clock:                    clock,
		runtime:                  runtimeSampler,
		rpcContext:               rpcContext,
		nodeDescs:                g,
		systemConfigWatcher:      systemConfigWatcher,
		spanConfigAccessor:       spanConfig.kvAccessor,
		nodeDialer:               nodeDialer,
		distSender:               distSender,
		db:                       db,
		registry:                 registry,
		recorder:                 recorder,
		sessionRegistry:          sessionRegistry,
		flowScheduler:            flowScheduler,
		circularInternalExecutor: internalExecutor,
		circularJobRegistry:      jobRegistry,
		jobAdoptionStopFile:      jobAdoptionStopFile,
		protectedtsProvider:      protectedtsProvider,
		rangeFeedFactory:         rangeFeedFactory,
		sqlStatusServer:          sStatus,
		regionsServer:            sStatus,
		tenantStatusServer:       sStatus,
		tenantUsageServer:        tenantUsage,
		monitorAndMetrics:        sqlMonitorAndMetrics,
		settingsStorage:          settingsWriter,
	})
	if err != nil {
		return nil, err
	}

	sAuth := newAuthenticationServer(cfg.Config, sqlServer)
	for i, gw := range []grpcGatewayServer{sAdmin, sStatus, sAuth, &sTS} {
		if reflect.ValueOf(gw).IsNil() {
			return nil, errors.Errorf("%d: nil", i)
		}
		gw.RegisterService(grpcServer.Server)
	}

	sStatus.setStmtDiagnosticsRequester(sqlServer.execCfg.StmtDiagnosticsRecorder)
	sStatus.baseStatusServer.sqlServer = sqlServer
	debugServer := debug.NewServer(cfg.BaseConfig.AmbientCtx, st, sqlServer.pgServer.HBADebugFn(), sStatus)
	node.InitLogger(sqlServer.execCfg)

	drain := newDrainServer(cfg.BaseConfig, stopper, grpcServer, sqlServer)
	drain.setNode(node, nodeLiveness)

	*lateBoundServer = Server{
		nodeIDContainer:        nodeIDContainer,
		cfg:                    cfg,
		st:                     st,
		clock:                  clock,
		rpcContext:             rpcContext,
		engines:                engines,
		grpc:                   grpcServer,
		gossip:                 g,
		nodeDialer:             nodeDialer,
		nodeLiveness:           nodeLiveness,
		storePool:              storePool,
		tcsFactory:             tcsFactory,
		distSender:             distSender,
		db:                     db,
		node:                   node,
		registry:               registry,
		recorder:               recorder,
		ruleRegistry:           ruleRegistry,
		promRuleExporter:       promRuleExporter,
		updates:                updates,
		ctSender:               ctSender,
		runtime:                runtimeSampler,
		http:                   sHTTP,
		adminAuthzCheck:        adminAuthzCheck,
		admin:                  sAdmin,
		status:                 sStatus,
		drain:                  drain,
		authentication:         sAuth,
		tsDB:                   tsDB,
		tsServer:               &sTS,
		raftTransport:          raftTransport,
		stopper:                stopper,
		debug:                  debugServer,
		kvProber:               kvProber,
		replicationReporter:    replicationReporter,
		protectedtsProvider:    protectedtsProvider,
		spanConfigSubscriber:   spanConfig.subscriber,
		sqlServer:              sqlServer,
		externalStorageBuilder: externalStorageBuilder,
		storeGrantCoords:       gcoords.Stores,
		kvMemoryMonitor:        kvMemoryMonitor,
	}

	// Begin an async task to periodically purge old sessions in the system.web_sessions table.
	if err = startPurgeOldSessions(ctx, sAuth); err != nil {
		return nil, err
	}

	return lateBoundServer, err
}

// ClusterSettings returns the cluster settings.
func (s *Server) ClusterSettings() *cluster.Settings {
	return s.st
}

// AnnotateCtx is a convenience wrapper; see AmbientContext.
func (s *Server) AnnotateCtx(ctx context.Context) context.Context {
	return s.cfg.AmbientCtx.AnnotateCtx(ctx)
}

// AnnotateCtxWithSpan is a convenience wrapper; see AmbientContext.
func (s *Server) AnnotateCtxWithSpan(
	ctx context.Context, opName string,
) (context.Context, *tracing.Span) {
	return s.cfg.AmbientCtx.AnnotateCtxWithSpan(ctx, opName)
}

// ClusterID returns the ID of the cluster this server is a part of.
func (s *Server) ClusterID() uuid.UUID {
	return s.rpcContext.ClusterID.Get()
}

// NodeID returns the ID of this node within its cluster.
func (s *Server) NodeID() roachpb.NodeID {
	return s.node.Descriptor.NodeID
}

// InitialStart returns whether this is the first time the node has started (as
// opposed to being restarted). Only intended to help print debugging info
// during server startup.
func (s *Server) InitialStart() bool {
	return s.node.initialStart
}

// listenerInfo is a helper used to write files containing various listener
// information to the store directories. In contrast to the "listening url
// file", these are written once the listeners are available, before the server
// is necessarily ready to serve.
type listenerInfo struct {
	listenRPC    string // the (RPC) listen address, rewritten after name resolution and port allocation
	advertiseRPC string // contains the original addr part of --listen/--advertise, with actual port number after port allocation if original was 0
	listenSQL    string // the SQL endpoint, rewritten after name resolution and port allocation
	advertiseSQL string // contains the original addr part of --sql-addr, with actual port number after port allocation if original was 0
	listenHTTP   string // the HTTP endpoint
}

// Iter returns a mapping of file names to desired contents.
func (li listenerInfo) Iter() map[string]string {
	return map[string]string{
		"cockroach.listen-addr":        li.listenRPC,
		"cockroach.advertise-addr":     li.advertiseRPC,
		"cockroach.sql-addr":           li.listenSQL,
		"cockroach.advertise-sql-addr": li.advertiseSQL,
		"cockroach.http-addr":          li.listenHTTP,
	}
}

// Start calls PreStart() and AcceptClient() in sequence.
// This is suitable for use e.g. in tests.
func (s *Server) Start(ctx context.Context) error {
	if err := s.PreStart(ctx); err != nil {
		return err
	}
	return s.AcceptClients(ctx)
}

// PreStart starts the server on the specified port, starts gossip and
// initializes the node using the engines from the server's context.
//
// It does not activate the pgwire listener over the network / unix
// socket, which is done by the AcceptClients() method. The separation
// between the two exists so that SQL initialization can take place
// before the first client is accepted.
//
// PreStart is complex since it sets up the listeners and the associated
// port muxing, but especially since it has to solve the
// "bootstrapping problem": nodes need to connect to Gossip fairly
// early, but what drives Gossip connectivity are the first range
// replicas in the kv store. This in turn suggests opening the Gossip
// server early. However, naively doing so also serves most other
// services prematurely, which exposes a large surface of potentially
// underinitialized services. This is avoided with some additional
// complexity that can be summarized as follows:
//
// - before blocking trying to connect to the Gossip network, we already open
//   the admin UI (so that its diagnostics are available)
// - we also allow our Gossip and our connection health Ping service
// - everything else returns Unavailable errors (which are retryable)
// - once the node has started, unlock all RPCs.
//
// The passed context can be used to trace the server startup. The context
// should represent the general startup operation.
func (s *Server) PreStart(ctx context.Context) error {
	ctx = s.AnnotateCtx(ctx)

	// Start the time sanity checker.
	s.startTime = timeutil.Now()
	if err := s.startMonitoringForwardClockJumps(ctx); err != nil {
		return err
	}

	// Connect the node as loopback handler for RPC requests to the
	// local node.
	s.rpcContext.SetLocalInternalServer(s.node)

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
	connManager := netutil.MakeServer(s.stopper, uiTLSConfig, http.HandlerFunc(s.http.baseHandler))

	// Start a context for the asynchronous network workers.
	workersCtx := s.AnnotateCtx(context.Background())

	// Start the admin UI server. This opens the HTTP listen socket,
	// optionally sets up TLS, and dispatches the server worker for the
	// web UI.
	if err := s.http.start(ctx, workersCtx, connManager, uiTLSConfig, s.stopper); err != nil {
		return err
	}

	// Initialize the external storage builders configuration params now that the
	// engines have been created. The object can be used to create ExternalStorage
	// objects hereafter.
	fileTableInternalExecutor := sql.MakeInternalExecutor(ctx, s.PGServer().SQLServer, sql.MemoryMetrics{}, s.st)
	s.externalStorageBuilder.init(
		s.cfg.ExternalIODirConfig,
		s.st,
		s.nodeIDContainer,
		s.nodeDialer,
		s.cfg.TestingKnobs,
		&fileTableInternalExecutor,
		s.db)

	// Filter out self from the gossip bootstrap addresses.
	filtered := s.cfg.FilterGossipBootstrapAddresses(ctx)

	// Set up the init server. We have to do this relatively early because we
	// can't call RegisterInitServer() after `grpc.Serve`, which is called in
	// startRPCServer (and for the loopback grpc-gw connection).
	var initServer *initServer
	{
		dialOpts, err := s.rpcContext.GRPCDialOptions()
		if err != nil {
			return err
		}

		initConfig := newInitServerConfig(ctx, s.cfg, dialOpts)
		inspectedDiskState, err := inspectEngines(
			ctx,
			s.engines,
			s.cfg.Settings.Version.BinaryVersion(),
			s.cfg.Settings.Version.BinaryMinSupportedVersion(),
		)
		if err != nil {
			return err
		}

		initServer = newInitServer(s.cfg.AmbientCtx, inspectedDiskState, initConfig)
	}

	initialDiskClusterVersion := initServer.DiskClusterVersion()
	{
		// The invariant we uphold here is that any version bump needs to be
		// persisted on all engines before it becomes "visible" to the version
		// setting. To this end, we:
		//
		// a) write back the disk-loaded cluster version to all engines,
		// b) initialize the version setting (using the disk-loaded version).
		//
		// Note that "all engines" means "all engines", not "all initialized
		// engines". We cannot initialize engines this early in the boot
		// sequence.
		//
		// The version setting loaded from disk is the maximum cluster version
		// seen on any engine. If new stores are being added to the server right
		// now, or if the process crashed earlier half-way through the callback,
		// that version won't be on all engines. For that reason, we backfill
		// once.
		if err := kvserver.WriteClusterVersionToEngines(
			ctx, s.engines, initialDiskClusterVersion,
		); err != nil {
			return err
		}

		// Note that at this point in the code we don't know if we'll bootstrap
		// or join an existing cluster, so we have to conservatively go with the
		// version from disk. If there are no initialized engines, this is the
		// binary min supported version.
		if err := clusterversion.Initialize(ctx, initialDiskClusterVersion.Version, &s.cfg.Settings.SV); err != nil {
			return err
		}

		// At this point, we've established the invariant: all engines hold the
		// version currently visible to the setting. Going forward whenever we
		// set an active cluster version (`SetActiveClusterVersion`), we'll
		// persist it to all the engines first (`WriteClusterVersionToEngines`).
		// This happens at two places:
		//
		// - Right below, if we learn that we're the bootstrapping node, given
		//   we'll be setting the active cluster version as the binary version.
		// - Within the BumpClusterVersion RPC, when we're informed by another
		//   node what our new active cluster version should be.
	}

	serverpb.RegisterInitServer(s.grpc.Server, initServer)

	// Register the Migration service, to power internal crdb migrations.
	migrationServer := &migrationServer{server: s}
	serverpb.RegisterMigrationServer(s.grpc.Server, migrationServer)
	s.migrationServer = migrationServer // only for testing via TestServer

	// Start the RPC server. This opens the RPC/SQL listen socket,
	// and dispatches the server worker for the RPC.
	// The SQL listener is returned, to start the SQL server later
	// below when the server has initialized.
	pgL, startRPCServer, err := startListenRPCAndSQL(ctx, workersCtx, s.cfg.BaseConfig, s.stopper, s.grpc)
	if err != nil {
		return err
	}

	if s.cfg.TestingKnobs.Server != nil {
		knobs := s.cfg.TestingKnobs.Server.(*TestingKnobs)
		if knobs.SignalAfterGettingRPCAddress != nil {
			log.Infof(ctx, "signaling caller that RPC address is ready")
			close(knobs.SignalAfterGettingRPCAddress)
		}
		if knobs.PauseAfterGettingRPCAddress != nil {
			log.Infof(ctx, "waiting for signal from caller to proceed with initialization")
			select {
			case <-knobs.PauseAfterGettingRPCAddress:
				// Normal case. Just continue below.

			case <-ctx.Done():
				// Test timeout or some other condition in the caller, by which
				// we are instructed to stop.
				return errors.CombineErrors(errors.New("server stopping prematurely from context shutdown"), ctx.Err())

			case <-s.stopper.ShouldQuiesce():
				// The server is instructed to stop before it even finished
				// starting up.
				return errors.New("server stopping prematurely")
			}
			log.Infof(ctx, "caller is letting us proceed with initialization")
		}
	}

	// Initialize grpc-gateway mux and context in order to get the /health
	// endpoint working even before the node has fully initialized.
	gwMux, gwCtx, conn, err := configureGRPCGateway(
		ctx,
		workersCtx,
		s.cfg.AmbientCtx,
		s.rpcContext,
		s.stopper,
		s.grpc,
		s.cfg.AdvertiseAddr,
	)
	if err != nil {
		return err
	}

	for _, gw := range []grpcGatewayServer{s.admin, s.status, s.authentication, s.tsServer} {
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
		listenRPC:    s.cfg.Addr,
		advertiseRPC: s.cfg.AdvertiseAddr,
		listenSQL:    s.cfg.SQLAddr,
		advertiseSQL: s.cfg.SQLAdvertiseAddr,
		listenHTTP:   s.cfg.HTTPAdvertiseAddr,
	}.Iter()

	encryptedStore := false
	for _, storeSpec := range s.cfg.Stores.Specs {
		if storeSpec.InMemory {
			continue
		}
		if storeSpec.IsEncrypted() {
			encryptedStore = true
		}

		for name, val := range listenerFiles {
			file := filepath.Join(storeSpec.Path, name)
			if err := ioutil.WriteFile(file, []byte(val), 0644); err != nil {
				return errors.Wrapf(err, "failed to write %s", file)
			}
		}
	}

	if s.cfg.DelayedBootstrapFn != nil {
		defer time.AfterFunc(30*time.Second, s.cfg.DelayedBootstrapFn).Stop()
	}

	// We self bootstrap for when we're configured to do so, which should only
	// happen during tests and for `cockroach start-single-node`.
	selfBootstrap := s.cfg.AutoInitializeCluster && initServer.NeedsBootstrap()
	if selfBootstrap {
		if _, err := initServer.Bootstrap(ctx, &serverpb.BootstrapRequest{}); err != nil {
			return err
		}
	}

	// Set up calling s.cfg.ReadyFn at the right time. Essentially, this call
	// determines when `./cockroach [...] --background` returns. For any
	// initialized nodes (i.e. already part of a cluster) this is when this
	// method returns (assuming there's no error). For nodes that need to join a
	// cluster, we return once the initServer is ready to accept requests.
	var onSuccessfulReturnFn, onInitServerReady func()
	{
		readyFn := func(bool) {}
		if s.cfg.ReadyFn != nil {
			readyFn = s.cfg.ReadyFn
		}
		if !initServer.NeedsBootstrap() || selfBootstrap {
			onSuccessfulReturnFn = func() { readyFn(false /* waitForInit */) }
			onInitServerReady = func() {}
		} else {
			onSuccessfulReturnFn = func() {}
			onInitServerReady = func() { readyFn(true /* waitForInit */) }
		}
	}

	// This opens the main listener. When the listener is open, we can call
	// onInitServerReady since any request initiated to the initServer at that
	// point will reach it once ServeAndWait starts handling the queue of
	// incoming connections.
	startRPCServer(workersCtx)
	onInitServerReady()
	state, initialStart, err := initServer.ServeAndWait(ctx, s.stopper, &s.cfg.Settings.SV)
	if err != nil {
		return errors.Wrap(err, "during init")
	}
	if err := state.validate(); err != nil {
		return errors.Wrap(err, "invalid init state")
	}

	// Apply any cached initial settings (and start the gossip listener) as early
	// as possible, to avoid spending time with stale settings.
	if err := initializeCachedSettings(
		ctx, keys.SystemSQLCodec, s.st.MakeUpdater(), state.initialSettingsKVs,
	); err != nil {
		return errors.Wrap(err, "during initializing settings updater")
	}

	// TODO(irfansharif): Let's make this unconditional. We could avoid
	// persisting + initializing the cluster version in response to being
	// bootstrapped (within `ServeAndWait` above) and simply do it here, in the
	// same way we're doing for when we join an existing cluster.
	if state.clusterVersion != initialDiskClusterVersion {
		// We just learned about a cluster version different from the one we
		// found on/synthesized from disk. This indicates that we're either the
		// bootstrapping node (and are using the binary version as the cluster
		// version), or we're joining an existing cluster that just informed us
		// to activate the given cluster version.
		//
		// Either way, we'll do so by first persisting the cluster version
		// itself, and then informing the version setting about it (an invariant
		// we must up hold whenever setting a new active version).
		if err := kvserver.WriteClusterVersionToEngines(
			ctx, s.engines, state.clusterVersion,
		); err != nil {
			return err
		}

		if err := s.ClusterSettings().Version.SetActiveVersion(ctx, state.clusterVersion); err != nil {
			return err
		}
	}

	s.rpcContext.ClusterID.Set(ctx, state.clusterID)
	s.rpcContext.NodeID.Set(ctx, state.nodeID)

	// TODO(irfansharif): Now that we have our node ID, we should run another
	// check here to make sure we've not been decommissioned away (if we're here
	// following a server restart). See the discussions in #48843 for how that
	// could be done, and what's motivating it.
	//
	// In summary: We'd consult our local store keys to see if they contain a
	// kill file informing us we've been decommissioned away (the
	// decommissioning process, that prefers to decommission live targets, will
	// inform the target node to persist such a file).
	//
	// Short of that, if we were decommissioned in absentia, we'd attempt to
	// reach out to already connected nodes in our join list to see if they have
	// any knowledge of our node ID being decommissioned. This is something the
	// decommissioning node will broadcast (best-effort) to cluster if the
	// target node is unavailable, and is only done with the operator guarantee
	// that this node is indeed never coming back. If we learn that we're not
	// decommissioned, we'll solicit the decommissioned list from the already
	// connected node to be able to respond to inbound decomm check requests.
	//
	// As for the problem of the ever growing list of decommissioned node IDs
	// being maintained on each node, given that we're populating+broadcasting
	// this list in best effort fashion (like said above, we're relying on the
	// operator to guarantee that the target node is never coming back), perhaps
	// it's also fine for us to age out the node ID list we maintain if it gets
	// too large. Though even maintaining a max of 64 MB of decommissioned node
	// IDs would likely outlive us all
	//
	//   536,870,912 bits/64 bits = 8,388,608 decommissioned node IDs.

	// TODO(tbg): split this method here. Everything above this comment is
	// the early stage of startup -- setting up listeners and determining the
	// initState -- and everything after it is actually starting the server,
	// using the listeners and init state.

	// Spawn a goroutine that will print a nice message when Gossip connects.
	// Note that we already know the clusterID, but we don't know that Gossip
	// has connected. The pertinent case is that of restarting an entire
	// cluster. Someone has to gossip the ClusterID before Gossip is connected,
	// but this gossip only happens once the first range has a leaseholder, i.e.
	// when a quorum of nodes has gone fully operational.
	_ = s.stopper.RunAsyncTask(ctx, "connect-gossip", func(ctx context.Context) {
		log.Ops.Infof(ctx, "connecting to gossip network to verify cluster ID %q", state.clusterID)
		select {
		case <-s.gossip.Connected:
			log.Ops.Infof(ctx, "node connected via gossip")
		case <-ctx.Done():
		case <-s.stopper.ShouldQuiesce():
		}
	})

	hlcUpperBoundExists, err := s.checkHLCUpperBoundExistsAndEnsureMonotonicity(ctx, initialStart)
	if err != nil {
		return err
	}

	// Record a walltime that is lower than the lowest hlc timestamp this current
	// instance of the node can use. We do not use startTime because it is lower
	// than the timestamp used to create the bootstrap schema.
	//
	// TODO(tbg): clarify the contract here and move closer to usage if possible.
	orphanedLeasesTimeThresholdNanos := s.clock.Now().WallTime

	onSuccessfulReturnFn()

	// NB: This needs to come after `startListenRPCAndSQL`, which determines
	// what the advertised addr is going to be if nothing is explicitly
	// provided.
	advAddrU := util.NewUnresolvedAddr("tcp", s.cfg.AdvertiseAddr)

	// We're going to need to start gossip before we spin up Node below.
	s.gossip.Start(advAddrU, filtered)
	log.Event(ctx, "started gossip")

	// Now that we have a monotonic HLC wrt previous incarnations of the process,
	// init all the replicas. At this point *some* store has been initialized or
	// we're joining an existing cluster for the first time.
	advSQLAddrU := util.NewUnresolvedAddr("tcp", s.cfg.SQLAdvertiseAddr)

	advHTTPAddrU := util.NewUnresolvedAddr("tcp", s.cfg.HTTPAdvertiseAddr)

	if err := s.node.start(
		ctx,
		advAddrU,
		advSQLAddrU,
		advHTTPAddrU,
		*state,
		initialStart,
		s.cfg.ClusterName,
		s.cfg.NodeAttributes,
		s.cfg.Locality,
		s.cfg.LocalityAddresses,
		s.sqlServer.execCfg.DistSQLPlanner.SetSQLInstanceInfo,
	); err != nil {
		return err
	}
	// Stores have been initialized, so Node can now provide Pebble metrics.
	s.storeGrantCoords.SetPebbleMetricsProvider(ctx, s.node)

	log.Event(ctx, "started node")
	if err := s.startPersistingHLCUpperBound(ctx, hlcUpperBoundExists); err != nil {
		return err
	}
	s.replicationReporter.Start(ctx, s.stopper)

	sentry.ConfigureScope(func(scope *sentry.Scope) {
		scope.SetTags(map[string]string{
			"cluster":         s.ClusterID().String(),
			"node":            s.NodeID().String(),
			"server_id":       fmt.Sprintf("%s-%s", s.ClusterID().Short(), s.NodeID()),
			"engine_type":     s.cfg.StorageEngine.String(),
			"encrypted_store": strconv.FormatBool(encryptedStore),
		})
	})

	// We can now add the node registry.
	s.recorder.AddNode(
		s.registry,
		s.node.Descriptor,
		s.node.startedAt,
		s.cfg.AdvertiseAddr,
		s.cfg.HTTPAdvertiseAddr,
		s.cfg.SQLAdvertiseAddr,
	)

	// Begin recording runtime statistics.
	if err := startSampleEnvironment(s.AnnotateCtx(ctx),
		s.ClusterSettings(),
		s.stopper,
		s.cfg.GoroutineDumpDirName,
		s.cfg.HeapProfileDirName,
		s.runtime,
		s.status.sessionRegistry,
	); err != nil {
		return err
	}

	var graphiteOnce sync.Once
	graphiteEndpoint.SetOnChange(&s.st.SV, func(context.Context) {
		if graphiteEndpoint.Get(&s.st.SV) != "" {
			graphiteOnce.Do(func() {
				s.node.startGraphiteStatsExporter(s.st)
			})
		}
	})

	// Start the protected timestamp subsystem. Note that this needs to happen
	// before the modeOperational switch below, as the protected timestamps
	// subsystem will crash if accessed before being Started (and serving general
	// traffic may access it).
	//
	// See https://github.com/cockroachdb/cockroach/issues/73897.
	if err := s.protectedtsProvider.Start(ctx, s.stopper); err != nil {
		return err
	}

	// After setting modeOperational, we can block until all stores are fully
	// initialized.
	s.grpc.setMode(modeOperational)

	// We'll block here until all stores are fully initialized. We do this here
	// for two reasons:
	// - some of the components below depend on all stores being fully
	//   initialized (like the debug server registration for e.g.)
	// - we'll need to do it after having opened up the RPC floodgates (due to
	//   the hazard described in Node.start, around initializing additional
	//   stores)
	s.node.waitForAdditionalStoreInit()

	// Once all stores are initialized, check if offline storage recovery
	// was done prior to start and record any actions appropriately.
	logPendingLossOfQuorumRecoveryEvents(ctx, s.node.stores)

	log.Ops.Infof(ctx, "starting %s server at %s (use: %s)",
		redact.Safe(s.cfg.HTTPRequestScheme()), s.cfg.HTTPAddr, s.cfg.HTTPAdvertiseAddr)
	rpcConnType := redact.SafeString("grpc/postgres")
	if s.cfg.SplitListenSQL {
		rpcConnType = "grpc"
		log.Ops.Infof(ctx, "starting postgres server at %s (use: %s)", s.cfg.SQLAddr, s.cfg.SQLAdvertiseAddr)
	}
	log.Ops.Infof(ctx, "starting %s server at %s", rpcConnType, s.cfg.Addr)
	log.Ops.Infof(ctx, "advertising CockroachDB node at %s", s.cfg.AdvertiseAddr)

	log.Event(ctx, "accepting connections")

	// Begin the node liveness heartbeat. Add a callback which records the local
	// store "last up" timestamp for every store whenever the liveness record is
	// updated.
	s.nodeLiveness.Start(ctx, liveness.NodeLivenessStartOptions{
		Stopper: s.stopper,
		Engines: s.engines,
		OnSelfLive: func(ctx context.Context) {
			now := s.clock.Now()
			if err := s.node.stores.VisitStores(func(s *kvserver.Store) error {
				return s.WriteLastUpTimestamp(ctx, now)
			}); err != nil {
				log.Ops.Warningf(ctx, "writing last up timestamp: %v", err)
			}
		},
	})

	// Begin recording status summaries.
	if err := s.node.startWriteNodeStatus(base.DefaultMetricsSampleInterval); err != nil {
		return err
	}

	if !s.cfg.SpanConfigsDisabled && s.spanConfigSubscriber != nil {
		if subscriber, ok := s.spanConfigSubscriber.(*spanconfigkvsubscriber.KVSubscriber); ok {
			if err := subscriber.Start(ctx, s.stopper); err != nil {
				return err
			}
		}
	}
	// Start garbage collecting system events.
	//
	// NB: As written, this falls awkwardly between SQL and KV. KV is used only
	// to make sure this runs only on one node. SQL is used to actually GC. We
	// count it as a KV operation since it grooms cluster-wide data, not
	// something associated to SQL tenants.
	s.startSystemLogsGC(ctx)

	// Connect the HTTP endpoints. This also wraps the privileged HTTP
	// endpoints served by gwMux by the HTTP cookie authentication
	// check.
	if err := s.http.setupRoutes(ctx,
		s.authentication,       /* authnServer */
		s.adminAuthzCheck,      /* adminAuthzCheck */
		s.recorder,             /* metricSource */
		s.runtime,              /* runtimeStatsSampler */
		gwMux,                  /* handleRequestsUnauthenticated */
		s.debug,                /* handleDebugUnauthenticated */
		newAPIV2Server(ctx, s), /* apiServer */
	); err != nil {
		return err
	}

	// Record node start in telemetry. Get the right counter for this storage
	// engine type as well as type of start (initial boot vs restart).
	nodeStartCounter := "storage.engine."
	switch s.cfg.StorageEngine {
	case enginepb.EngineTypeDefault:
		fallthrough
	case enginepb.EngineTypePebble:
		nodeStartCounter += "pebble."
	}
	if s.InitialStart() {
		nodeStartCounter += "initial-boot"
	} else {
		nodeStartCounter += "restart"
	}
	telemetry.Count(nodeStartCounter)

	// Record that this node joined the cluster in the event log. Since this
	// executes a SQL query, this must be done after the SQL layer is ready.
	s.node.recordJoinEvent(ctx)

	if err := s.sqlServer.preStart(
		workersCtx,
		s.stopper,
		s.cfg.TestingKnobs,
		connManager,
		pgL,
		s.cfg.SocketFile,
		orphanedLeasesTimeThresholdNanos,
	); err != nil {
		return err
	}

	if err := s.debug.RegisterEngines(s.cfg.Stores.Specs, s.engines); err != nil {
		return errors.Wrapf(err, "failed to register engines with debug server")
	}
	s.debug.RegisterClosedTimestampSideTransport(s.ctSender, s.node.storeCfg.ClosedTimestampReceiver)
	s.debug.RegisterTracez(s.cfg.Tracer)

	s.ctSender.Run(ctx, state.nodeID)

	// Attempt to upgrade cluster version now that the sql server has been
	// started. At this point we know that all startupmigrations have successfully
	// been run so it is safe to upgrade to the binary's current version.
	s.startAttemptUpgrade(ctx)

	if err := s.node.tenantSettingsWatcher.Start(ctx, s.sqlServer.execCfg.SystemTableIDResolver); err != nil {
		return errors.Wrap(err, "failed to initialize the tenant settings watcher")
	}

	if err := s.kvProber.Start(ctx, s.stopper); err != nil {
		return errors.Wrapf(err, "failed to start KV prober")
	}

	// As final stage of loss of quorum recovery, write events into corresponding
	// range logs. We do it as a separate stage to log events early just in case
	// startup fails, and write to range log once the server is running as we need
	// to run sql statements to update rangelog.
	publishPendingLossOfQuorumRecoveryEvents(ctx, s.node.stores, s.stopper)

	log.Event(ctx, "server initialized")

	// Begin recording time series data collected by the status monitor.
	// This will perform the first write synchronously, which is now
	// acceptable.
	s.tsDB.PollSource(
		s.cfg.AmbientCtx, s.recorder, base.DefaultMetricsSampleInterval, ts.Resolution10s, s.stopper,
	)

	return maybeImportTS(ctx, s)
}

// AcceptClients starts listening for incoming SQL clients over the network.
func (s *Server) AcceptClients(ctx context.Context) error {
	workersCtx := s.AnnotateCtx(context.Background())

	if err := s.sqlServer.startServeSQL(
		workersCtx,
		s.stopper,
		s.sqlServer.connManager,
		s.sqlServer.pgL,
		s.cfg.SocketFile,
	); err != nil {
		return err
	}

	log.Event(ctx, "server ready")
	return nil
}

// Stop shuts down this server instance. Note that this method exists
// solely for the benefit of the `\demo shutdown` command in
// `cockroach demo`. It is not called as part of the regular server
// shutdown sequence; for this, see cli/start.go and the Drain()
// RPC.
func (s *Server) Stop() {
	s.stopper.Stop(context.Background())
}

// TempDir returns the filepath of the temporary directory used for temp storage.
// It is empty for an in-memory temp storage.
func (s *Server) TempDir() string {
	return s.cfg.TempStorageConfig.Path
}

// PGServer exports the pgwire server. Used by tests.
func (s *Server) PGServer() *pgwire.Server {
	return s.sqlServer.pgServer
}

// StartDiagnostics starts periodic diagnostics reporting and update checking.
// NOTE: This is not called in PreStart so that it's disabled by default for
// testing.
func (s *Server) StartDiagnostics(ctx context.Context) {
	s.updates.PeriodicallyCheckForUpdates(ctx, s.stopper)
	s.sqlServer.StartDiagnostics(ctx)
}

func init() {
	tracing.RegisterTagRemapping("n", "node")
}

// RunLocalSQL calls fn on a SQL internal executor on this server.
// This is meant for use for SQL initialization during bootstrapping.
//
// The internal SQL interface should be used instead of a regular SQL
// network connection for SQL initializations when setting up a new
// server, because it is possible for the server to listen on a
// network interface that is not reachable from loopback. It is also
// possible for the TLS certificates to be invalid when used locally
// (e.g. if the hostname in the cert is an advertised address that's
// only reachable externally).
func (s *Server) RunLocalSQL(
	ctx context.Context, fn func(ctx context.Context, sqlExec *sql.InternalExecutor) error,
) error {
	return fn(ctx, s.sqlServer.internalExecutor)
}

// Insecure returns true iff the server has security disabled.
func (s *Server) Insecure() bool {
	return s.cfg.Insecure
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
func (s *Server) Drain(
	ctx context.Context, verbose bool,
) (remaining uint64, info redact.RedactableString, err error) {
	return s.drain.runDrain(ctx, verbose)
}

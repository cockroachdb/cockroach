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
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/blobs/blobspb"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/inspectz"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvispb"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvissubscriber"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/spanstatskvaccessor"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangestats"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvprober"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/sidetransport"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvadmission"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontroller"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowdispatch"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowhandle"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptprovider"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptreconcile"
	serverrangefeed "github.com/cockroachdb/cockroach/pkg/kv/kvserver/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rangelog"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/reports"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiesauthorizer"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitieswatcher"
	"github.com/cockroachdb/cockroach/pkg/obs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/security/clientsecopts"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/diagnostics"
	"github.com/cockroachdb/cockroach/pkg/server/privchecker"
	"github.com/cockroachdb/cockroach/pkg/server/serverctl"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverrules"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/server/structlogging"
	"github.com/cockroachdb/cockroach/pkg/server/systemconfigwatcher"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/server/tenantsettingswatcher"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	_ "github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigjob" // register jobs declared outside of pkg/sql
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigkvaccessor"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigkvsubscriber"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigptsreader"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigreporter"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigstore"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	_ "github.com/cockroachdb/cockroach/pkg/sql/catalog/schematelemetry" // register schedules declared outside of pkg/sql
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	_ "github.com/cockroachdb/cockroach/pkg/sql/gcjob"    // register jobs declared outside of pkg/sql
	_ "github.com/cockroachdb/cockroach/pkg/sql/importer" // register jobs/planHooks declared outside of pkg/sql
	"github.com/cockroachdb/cockroach/pkg/sql/optionalnodeliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	_ "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scjob" // register jobs declared outside of pkg/sql
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	_ "github.com/cockroachdb/cockroach/pkg/sql/ttl/ttljob"      // register jobs declared outside of pkg/sql
	_ "github.com/cockroachdb/cockroach/pkg/sql/ttl/ttlschedule" // register schedules declared outside of pkg/sql
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/goschedstats"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logmetrics"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/rangedesc"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/schedulerlatency"
	"github.com/cockroachdb/cockroach/pkg/util/startup"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/ptp"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
	sentry "github.com/getsentry/sentry-go"
	"google.golang.org/grpc/codes"
)

// topLevelServer is the cockroach server node.
type topLevelServer struct {
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
	storePool        *storepool.StorePool
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
	adminAuthzCheck privchecker.CheckerForRPCHandlers
	admin           *systemAdminServer
	status          *systemStatusServer
	drain           *drainServer
	decomNodeMap    *decommissioningNodeMap
	authentication  authserver.Server
	migrationServer *migrationServer
	tsDB            *ts.DB
	tsServer        *ts.Server

	// keyVisualizerServer implements `keyvispb.KeyVisualizerServer`
	keyVisualizerServer *KeyVisualizerServer

	// The Observability Server, used by the Observability Service to subscribe to
	// CRDB data.
	eventsExporter obs.EventsExporterInterface
	recoveryServer *loqrecovery.Server
	raftTransport  *kvserver.RaftTransport
	stopper        *stop.Stopper
	stopTrigger    *stopTrigger

	debug          *debug.Server
	kvProber       *kvprober.Prober
	inspectzServer *inspectz.Server

	replicationReporter *reports.Reporter
	protectedtsProvider protectedts.Provider

	spanConfigSubscriber spanconfig.KVSubscriber
	spanConfigReporter   spanconfig.Reporter

	tenantCapabilitiesWatcher *tenantcapabilitieswatcher.Watcher

	// pgL is the SQL listener for pgwire connections coming over the network.
	pgL net.Listener
	// loopbackPgL is the SQL listener for internal pgwire connections.
	loopbackPgL *netutil.LoopbackListener

	// pgPreServer handles SQL connections prior to routing them to a
	// specific tenant.
	pgPreServer *pgwire.PreServeConnHandler

	// TODO(knz): pull this down under the serverController.
	sqlServer *SQLServer

	// serverController is responsible for on-demand instantiation
	// of services.
	serverController *serverController

	// Created in NewServer but initialized (made usable) in `(*Server).PreStart`.
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
//
// The caller is responsible for listening on the server's ShutdownRequested()
// channel and calling stopper.Stop().
func NewServer(cfg Config, stopper *stop.Stopper) (serverctl.ServerStartupInterface, error) {
	ctx := cfg.AmbientCtx.AnnotateCtx(context.Background())

	if err := cfg.ValidateAddrs(ctx); err != nil {
		return nil, err
	}

	st := cfg.Settings

	if cfg.AmbientCtx.Tracer == nil {
		panic(errors.New("no tracer set in AmbientCtx"))
	}

	clock, err := newClockFromConfig(ctx, cfg.BaseConfig)
	if err != nil {
		return nil, err
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

	admissionOptions := admission.DefaultOptions
	if opts, ok := cfg.TestingKnobs.AdmissionControlOptions.(*admission.Options); ok {
		admissionOptions.Override(opts)
	}

	engines, err := cfg.CreateEngines(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create engines")
	}
	stopper.AddCloser(&engines)

	// Loss of quorum recovery store is created and pending plan is applied to
	// engines as soon as engines are created and before any data is read in a
	// way similar to offline engine content patching.
	planStore, err := newPlanStore(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create loss of quorum plan store")
	}
	if err := loqrecovery.MaybeApplyPendingRecoveryPlan(ctx, planStore, engines, timeutil.DefaultTimeSource{}); err != nil {
		return nil, errors.Wrap(err, "failed to apply loss of quorum recovery plan")
	}

	nodeTombStorage, decommissionCheck := getPingCheckDecommissionFn(engines)

	g := gossip.New(
		cfg.AmbientCtx,
		cfg.ClusterIDContainer,
		nodeIDContainer,
		stopper,
		registry,
		cfg.Locality,
		&cfg.DefaultZoneConfig,
	)

	tenantCapabilitiesTestingKnobs, _ := cfg.TestingKnobs.TenantCapabilitiesTestingKnobs.(*tenantcapabilities.TestingKnobs)
	authorizer := tenantcapabilitiesauthorizer.New(cfg.Settings, tenantCapabilitiesTestingKnobs)
	rpcCtxOpts := rpc.ServerContextOptionsFromBaseConfig(cfg.BaseConfig.Config)

	rpcCtxOpts.TenantID = roachpb.SystemTenantID
	rpcCtxOpts.UseNodeAuth = true
	rpcCtxOpts.NodeID = nodeIDContainer
	rpcCtxOpts.StorageClusterID = cfg.ClusterIDContainer
	rpcCtxOpts.Clock = clock.WallClock()
	rpcCtxOpts.ToleratedOffset = clock.ToleratedOffset()
	rpcCtxOpts.FatalOnOffsetViolation = true
	rpcCtxOpts.Stopper = stopper
	rpcCtxOpts.Settings = cfg.Settings
	rpcCtxOpts.OnOutgoingPing = func(ctx context.Context, req *rpc.PingRequest) error {
		// Outgoing ping will block requests with codes.FailedPrecondition to
		// notify caller that this replica is decommissioned but others could
		// still be tried as caller node is valid, but not the destination.
		return decommissionCheck(ctx, req.TargetNodeID, codes.FailedPrecondition)
	}
	rpcCtxOpts.TenantRPCAuthorizer = authorizer
	rpcCtxOpts.NeedsDialback = true

	if knobs := cfg.TestingKnobs.Server; knobs != nil {
		serverKnobs := knobs.(*TestingKnobs)
		rpcCtxOpts.Knobs = serverKnobs.ContextTestingKnobs
	}
	rpcContext := rpc.NewContext(ctx, rpcCtxOpts)

	rpcContext.OnIncomingPing = func(ctx context.Context, req *rpc.PingRequest, resp *rpc.PingResponse) error {
		// Decommission state is only tracked for the system tenant.
		if tenantID, isTenant := roachpb.ClientTenantFromContext(ctx); !isTenant ||
			roachpb.IsSystemTenantID(tenantID.ToUint64()) {
			// Incoming ping will reject requests with codes.PermissionDenied to
			// signal remote node that it is not considered valid anymore and
			// operations should fail immediately.
			if err := decommissionCheck(ctx, req.OriginNodeID, codes.PermissionDenied); err != nil {
				return err
			}
		}
		// VerifyDialback verifies if a reverse connection to the sending node can
		// be established.
		return rpc.VerifyDialback(ctx, rpcContext, req, resp, cfg.Locality, &rpcContext.Settings.SV)
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
		// Expose cert expirations in metrics.
		registry.AddMetricStruct(cm.Metrics())
	}

	// Check the compatibility between the configured addresses and that
	// provided in certificates. This also logs the certificate
	// addresses in all cases to aid troubleshooting.
	// This must be called after the certificate manager was initialized
	// and after ValidateAddrs().
	rpcContext.CheckCertificateAddrs(ctx)

	grpcServer, err := newGRPCServer(rpcContext)
	if err != nil {
		return nil, err
	}
	gossip.RegisterGossipServer(grpcServer.Server, g)

	var dialerKnobs nodedialer.DialerTestingKnobs
	if dk := cfg.TestingKnobs.DialerKnobs; dk != nil {
		dialerKnobs = dk.(nodedialer.DialerTestingKnobs)
	}

	nodeDialer := nodedialer.NewWithOpt(rpcContext, gossip.AddressResolver(g),
		nodedialer.DialerOpt{TestingKnobs: dialerKnobs})

	runtimeSampler := status.NewRuntimeStatSampler(ctx, clock.WallClock())
	registry.AddMetricStruct(runtimeSampler)
	// Save a reference to this sampler for use by additional servers
	// started via the server controller.
	cfg.RuntimeStatSampler = runtimeSampler

	registry.AddMetric(base.LicenseTTL)
	err = base.UpdateMetricOnLicenseChange(ctx, cfg.Settings, base.LicenseTTL, timeutil.DefaultTimeSource{}, stopper)
	if err != nil {
		log.Errorf(ctx, "unable to initialize periodic license metric update: %v", err)
	}

	// Create and add KV metric rules.
	kvserver.CreateAndAddRules(ctx, ruleRegistry)
	// Create and add server metric rules.
	serverrules.CreateAndAddRules(ctx, ruleRegistry)

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
		Locality:           cfg.Locality,
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

	dbCtx := kv.DefaultDBContext(stopper)
	dbCtx.NodeID = idContainer
	dbCtx.Stopper = stopper
	db := kv.NewDBWithContext(cfg.AmbientCtx, tcsFactory, clock, dbCtx)

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

	stores := kvserver.NewStores(cfg.AmbientCtx, clock)

	decomNodeMap := &decommissioningNodeMap{
		nodes: make(map[roachpb.NodeID]interface{}),
	}
	nodeLiveness := liveness.NewNodeLiveness(liveness.NodeLivenessOptions{
		AmbientCtx:              cfg.AmbientCtx,
		Stopper:                 stopper,
		Clock:                   clock,
		Storage:                 liveness.NewKVStorage(db),
		Gossip:                  g,
		LivenessThreshold:       nlActive,
		RenewalDuration:         nlRenewal,
		Settings:                st,
		HistogramWindowInterval: cfg.HistogramWindowInterval(),
		// When we learn that a node is decommissioning, we want to proactively
		// enqueue the ranges we have that also have a replica on the
		// decommissioning node.
		OnNodeDecommissioning: decomNodeMap.makeOnNodeDecommissioningCallback(stores),
		OnNodeDecommissioned: func(id roachpb.NodeID) {
			if knobs, ok := cfg.TestingKnobs.Server.(*TestingKnobs); ok && knobs.OnDecommissionedCallback != nil {
				knobs.OnDecommissionedCallback(id)
			}
			if err := nodeTombStorage.SetDecommissioned(
				ctx, id, clock.PhysicalTime().UTC(),
			); err != nil {
				log.Fatalf(ctx, "unable to add tombstone for n%d: %s", id, err)
			}

			decomNodeMap.onNodeDecommissioned(id)
		},
		Engines: engines,
		OnSelfHeartbeat: func(ctx context.Context) {
			now := clock.Now()
			if err := stores.VisitStores(func(s *kvserver.Store) error {
				return s.WriteLastUpTimestamp(ctx, now)
			}); err != nil {
				log.Ops.Warningf(ctx, "writing last up timestamp: %v", err)
			}
		},
		NodeDialer: nodeDialer,
	})

	registry.AddMetricStruct(nodeLiveness.Metrics())

	nodeLivenessFn := storepool.MakeStorePoolNodeLivenessFunc(nodeLiveness)
	if nodeLivenessKnobs, ok := cfg.TestingKnobs.NodeLiveness.(kvserver.NodeLivenessTestingKnobs); ok {
		if nodeLivenessKnobs.StorePoolNodeLivenessFn != nil {
			nodeLivenessFn = nodeLivenessKnobs.StorePoolNodeLivenessFn
		}

		if nodeLivenessKnobs.IsLiveCallback != nil {
			nodeLiveness.RegisterCallback(nodeLivenessKnobs.IsLiveCallback)
		}
	}
	nodeLiveCountFn := func() int {
		var count int
		for _, nv := range nodeLiveness.ScanNodeVitalityFromCache() {
			if !nv.IsDecommissioning() && !nv.IsDecommissioned() {
				count++
			}
		}
		return count
	}
	storePool := storepool.NewStorePool(
		cfg.AmbientCtx,
		st,
		g,
		clock,
		nodeLiveCountFn,
		nodeLivenessFn,
		/* deterministic */ false,
	)

	storesForFlowControl := kvserver.MakeStoresForFlowControl(stores)
	kvflowTokenDispatch := kvflowdispatch.New(registry, storesForFlowControl, nodeIDContainer)
	admittedEntryAdaptor := newAdmittedLogEntryAdaptor(kvflowTokenDispatch)
	admissionKnobs, ok := cfg.TestingKnobs.AdmissionControl.(*admission.TestingKnobs)
	if !ok {
		admissionKnobs = &admission.TestingKnobs{}
	}
	gcoords := admission.NewGrantCoordinators(
		cfg.AmbientCtx,
		st,
		admissionOptions,
		registry,
		admittedEntryAdaptor,
		admissionKnobs,
	)
	db.SQLKVResponseAdmissionQ = gcoords.Regular.GetWorkQueue(admission.SQLKVResponseWork)
	cbID := goschedstats.RegisterRunnableCountCallback(gcoords.Regular.CPULoad)
	stopper.AddCloser(stop.CloserFn(func() {
		goschedstats.UnregisterRunnableCountCallback(cbID)
	}))
	stopper.AddCloser(gcoords)

	var admissionControl struct {
		schedulerLatencyListener admission.SchedulerLatencyListener
		kvflowController         kvflowcontrol.Controller
		kvflowTokenDispatch      kvflowcontrol.Dispatch
		kvAdmissionController    kvadmission.Controller
		storesFlowControl        kvserver.StoresForFlowControl
		kvFlowHandleMetrics      *kvflowhandle.Metrics
	}
	admissionControl.schedulerLatencyListener = gcoords.Elastic.SchedulerLatencyListener
	admissionControl.kvflowController = kvflowcontroller.New(registry, st, clock)
	admissionControl.kvflowTokenDispatch = kvflowTokenDispatch
	admissionControl.storesFlowControl = storesForFlowControl
	admissionControl.kvAdmissionController = kvadmission.MakeController(
		nodeIDContainer,
		gcoords.Regular.GetWorkQueue(admission.KVWork),
		gcoords.Elastic,
		gcoords.Stores,
		admissionControl.kvflowController,
		admissionControl.storesFlowControl,
		cfg.Settings,
	)
	admissionControl.kvFlowHandleMetrics = kvflowhandle.NewMetrics(registry)
	kvflowcontrol.Mode.SetOnChange(&st.SV, func(ctx context.Context) {
		admissionControl.storesFlowControl.ResetStreams(ctx)
	})

	var raftTransportKnobs *kvserver.RaftTransportTestingKnobs
	if knobs := cfg.TestingKnobs.RaftTransport; knobs != nil {
		raftTransportKnobs = knobs.(*kvserver.RaftTransportTestingKnobs)
	}
	raftTransport := kvserver.NewRaftTransport(
		cfg.AmbientCtx,
		st,
		cfg.AmbientCtx.Tracer,
		nodeDialer,
		grpcServer.Server,
		stopper,
		admissionControl.kvflowTokenDispatch,
		admissionControl.storesFlowControl,
		admissionControl.storesFlowControl,
		raftTransportKnobs,
	)
	registry.AddMetricStruct(raftTransport.Metrics())

	ctSender := sidetransport.NewSender(stopper, st, clock, nodeDialer)
	ctReceiver := sidetransport.NewReceiver(nodeIDContainer, stopper, stores, nil /* testingKnobs */)

	// The Executor will be further initialized later, as we create more
	// of the server's components. There's a circular dependency - many things
	// need an Executor, but the Executor needs an executorConfig,
	// which in turn needs many things. That's why everybody that needs an
	// Executor uses this one instance.
	internalExecutor := &sql.InternalExecutor{}
	insqlDB := sql.NewShimInternalDB(db)
	jobRegistry := &jobs.Registry{} // ditto

	// Create an ExternalStorageBuilder. This is only usable after Start() where
	// we initialize all the configuration params.
	externalStorageBuilder := &externalStorageBuilder{}
	externalStorage := externalStorageBuilder.makeExternalStorage
	externalStorageFromURI := externalStorageBuilder.makeExternalStorageFromURI

	protectedtsKnobs, _ := cfg.TestingKnobs.ProtectedTS.(*protectedts.TestingKnobs)
	protectedtsProvider, err := ptprovider.New(ptprovider.Config{
		DB:       insqlDB,
		Settings: st,
		Knobs:    protectedtsKnobs,
		ReconcileStatusFuncs: ptreconcile.StatusFuncs{
			jobsprotectedts.GetMetaType(jobsprotectedts.Jobs): jobsprotectedts.MakeStatusFunc(
				jobRegistry, jobsprotectedts.Jobs,
			),
			jobsprotectedts.GetMetaType(jobsprotectedts.Schedules): jobsprotectedts.MakeStatusFunc(
				jobRegistry, jobsprotectedts.Schedules,
			),
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
	kvMemoryMonitor.StartNoReserved(ctx, sqlMonitorAndMetrics.rootSQLMemoryMonitor)
	rangeReedBudgetFactory := serverrangefeed.NewBudgetFactory(
		ctx,
		serverrangefeed.CreateBudgetFactoryConfig(
			kvMemoryMonitor,
			cfg.MemoryPoolSize,
			cfg.HistogramWindowInterval(),
			func(limit int64) int64 {
				if !serverrangefeed.RangefeedBudgetsEnabled.Get(&st.SV) {
					return 0
				}
				if raftCmdLimit := kvserverbase.MaxCommandSize.Get(&st.SV); raftCmdLimit > limit {
					return raftCmdLimit
				}
				return limit
			},
			&st.SV))
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

	tenantCapabilitiesWatcher := tenantcapabilitieswatcher.New(
		clock,
		cfg.Settings,
		rangeFeedFactory,
		keys.TenantsTableID,
		stopper,
		1<<20, /* 1 MB */
		tenantCapabilitiesTestingKnobs,
	)

	var spanConfig struct {
		// kvAccessor powers the span configuration RPCs and the host tenant's
		// reconciliation job.
		kvAccessor spanconfig.KVAccessor
		// reporter is used to report over span config conformance.
		reporter spanconfig.Reporter
		// subscriber is used by stores to subscribe to span configuration updates.
		subscriber spanconfig.KVSubscriber
		// kvAccessorForTenantRecords is when creating/destroying secondary
		// tenant records.
		kvAccessorForTenantRecords spanconfig.KVAccessor
	}
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
			cfg.Settings,
			spanconfigstore.NewBoundsReader(tenantCapabilitiesWatcher),
			spanConfigKnobs,
			registry,
		)
	}

	scKVAccessor := spanconfigkvaccessor.New(
		db, internalExecutor, cfg.Settings, clock,
		systemschema.SpanConfigurationsTableName.FQString(),
		spanConfigKnobs,
	)
	spanConfig.kvAccessor, spanConfig.kvAccessorForTenantRecords = scKVAccessor, scKVAccessor
	spanConfig.reporter = spanconfigreporter.New(
		nodeLiveness,
		storePool,
		spanConfig.subscriber,
		rangedesc.NewScanner(db),
		cfg.Settings,
		spanConfigKnobs,
	)

	var protectedTSReader spanconfig.ProtectedTSReader
	if cfg.TestingKnobs.SpanConfig != nil &&
		cfg.TestingKnobs.SpanConfig.(*spanconfig.TestingKnobs).ProtectedTSReaderOverrideFn != nil {
		fn := cfg.TestingKnobs.SpanConfig.(*spanconfig.TestingKnobs).ProtectedTSReaderOverrideFn
		protectedTSReader = fn(clock)
	} else {
		protectedTSReader = spanconfigptsreader.NewAdapter(protectedtsProvider.(*ptprovider.Provider).Cache, spanConfig.subscriber)
	}

	rangeLogWriter := rangelog.NewWriter(
		keys.SystemSQLCodec,
		func() int64 {
			return int64(builtins.GenerateUniqueInt(
				builtins.ProcessUniqueID(nodeIDContainer.Get()),
			))
		},
	)
	eagerLeaseAcquisitionLimiter := quotapool.NewIntPool("eager-lease-acquisitions",
		uint64(kvserver.EagerLeaseAcquisitionConcurrency.Get(&cfg.Settings.SV)))
	kvserver.EagerLeaseAcquisitionConcurrency.SetOnChange(&cfg.Settings.SV, func(ctx context.Context) {
		eagerLeaseAcquisitionLimiter.UpdateCapacity(
			uint64(kvserver.EagerLeaseAcquisitionConcurrency.Get(&cfg.Settings.SV)))
	})

	storeCfg := kvserver.StoreConfig{
		DefaultSpanConfig:            cfg.DefaultZoneConfig.AsSpanConfig(),
		Settings:                     st,
		AmbientCtx:                   cfg.AmbientCtx,
		RaftConfig:                   cfg.RaftConfig,
		Clock:                        clock,
		DB:                           db,
		Gossip:                       g,
		NodeLiveness:                 nodeLiveness,
		Transport:                    raftTransport,
		NodeDialer:                   nodeDialer,
		RPCContext:                   rpcContext,
		ScanInterval:                 cfg.ScanInterval,
		ScanMinIdleTime:              cfg.ScanMinIdleTime,
		ScanMaxIdleTime:              cfg.ScanMaxIdleTime,
		HistogramWindowInterval:      cfg.HistogramWindowInterval(),
		StorePool:                    storePool,
		LogRangeAndNodeEvents:        cfg.EventLogEnabled,
		RangeDescriptorCache:         distSender.RangeDescriptorCache(),
		TimeSeriesDataStore:          tsDB,
		ClosedTimestampSender:        ctSender,
		ClosedTimestampReceiver:      ctReceiver,
		ProtectedTimestampReader:     protectedTSReader,
		EagerLeaseAcquisitionLimiter: eagerLeaseAcquisitionLimiter,
		KVMemoryMonitor:              kvMemoryMonitor,
		RangefeedBudgetFactory:       rangeReedBudgetFactory,
		SystemConfigProvider:         systemConfigWatcher,
		SpanConfigSubscriber:         spanConfig.subscriber,
		SnapshotApplyLimit:           cfg.SnapshotApplyLimit,
		SnapshotSendLimit:            cfg.SnapshotSendLimit,
		RangeLogWriter:               rangeLogWriter,
		KVAdmissionController:        admissionControl.kvAdmissionController,
		KVFlowController:             admissionControl.kvflowController,
		KVFlowHandles:                admissionControl.storesFlowControl,
		KVFlowHandleMetrics:          admissionControl.kvFlowHandleMetrics,
		SchedulerLatencyListener:     admissionControl.schedulerLatencyListener,
	}
	if storeTestingKnobs := cfg.TestingKnobs.Store; storeTestingKnobs != nil {
		storeCfg.TestingKnobs = *storeTestingKnobs.(*kvserver.StoreTestingKnobs)
	}
	storeCfg.SetDefaults(len(engines))

	systemTenantNameContainer := roachpb.NewTenantNameContainer(catconstants.SystemTenantName)

	recorder := status.NewMetricsRecorder(
		rpcContext.TenantID,
		systemTenantNameContainer,
		nodeLiveness,
		rpcContext.RemoteClocks,
		clock.WallClock(),
		st,
	)
	registry.AddMetricStruct(rpcContext.RemoteClocks.Metrics())

	updates := &diagnostics.UpdateChecker{
		StartTime:        timeutil.Now(),
		AmbientCtx:       &cfg.AmbientCtx,
		Config:           cfg.BaseConfig.Config,
		Settings:         cfg.Settings,
		StorageClusterID: rpcContext.StorageClusterID.Get,
		LogicalClusterID: rpcContext.LogicalClusterID.Get,
		NodeID:           nodeIDContainer.Get,
		SQLInstanceID:    idContainer.SQLInstanceID,
	}

	if cfg.TestingKnobs.Server != nil {
		updates.TestingKnobs = &cfg.TestingKnobs.Server.(*TestingKnobs).DiagnosticsTestingKnobs
	}

	tenantUsage := NewTenantUsageServer(st, db, insqlDB)
	registry.AddMetricStruct(tenantUsage.Metrics())

	tenantSettingsWatcher := tenantsettingswatcher.New(
		clock, rangeFeedFactory, stopper, st,
	)

	node := NewNode(
		storeCfg,
		recorder,
		registry,
		stopper,
		txnMetrics,
		stores,
		cfg.ClusterIDContainer,
		gcoords.Regular.GetWorkQueue(admission.KVWork),
		gcoords.Elastic,
		gcoords.Stores,
		tenantUsage,
		tenantSettingsWatcher,
		tenantCapabilitiesWatcher,
		spanConfig.kvAccessor,
		spanConfig.reporter,
	)
	kvpb.RegisterInternalServer(grpcServer.Server, node)
	kvserver.RegisterPerReplicaServer(grpcServer.Server, node.perReplicaServer)
	kvserver.RegisterPerStoreServer(grpcServer.Server, node.perReplicaServer)
	ctpb.RegisterSideTransportServer(grpcServer.Server, ctReceiver)

	// Create blob service for inter-node file sharing.
	blobService, err := blobs.NewBlobService(cfg.Settings.ExternalIODir)
	if err != nil {
		return nil, errors.Wrap(err, "creating blob service")
	}
	blobspb.RegisterBlobServer(grpcServer.Server, blobService)

	{ // wire up admission control's scheduler latency listener
		slcbID := schedulerlatency.RegisterCallback(
			node.storeCfg.SchedulerLatencyListener.SchedulerLatency,
		)
		stopper.AddCloser(stop.CloserFn(func() {
			schedulerlatency.UnregisterCallback(slcbID)
		}))
	}

	replicationReporter := reports.NewReporter(
		db, node.stores, storePool, st, nodeLiveness, internalExecutor, systemConfigWatcher,
	)

	lateBoundServer := &topLevelServer{}

	// The following initialization is mirrored in NewTenantServer().
	// Please keep them in sync.

	// Instantiate the API privilege checker.
	//
	// TODO(tbg): give adminServer only what it needs (and avoid circular deps).
	adminAuthzCheck := privchecker.NewChecker(internalExecutor, st)

	// Instantiate the HTTP server.
	// These callbacks help us avoid a dependency on gossip in httpServer.
	parseNodeIDFn := func(s string) (roachpb.NodeID, bool, error) {
		return parseNodeID(g, s)
	}
	getNodeIDHTTPAddressFn := func(id roachpb.NodeID) (*util.UnresolvedAddr, error) {
		return g.GetNodeIDHTTPAddress(id)
	}
	sHTTP := newHTTPServer(cfg.BaseConfig, rpcContext, parseNodeIDFn, getNodeIDHTTPAddressFn)

	// Instantiate the SQL session registry.
	sessionRegistry := sql.NewSessionRegistry()

	// Instantiate the cache of closed SQL sessions.
	closedSessionCache := sql.NewClosedSessionCache(cfg.Settings, sqlMonitorAndMetrics.rootSQLMemoryMonitor, time.Now)

	// Instantiate the distSQL remote flow runner.
	remoteFlowRunnerAcc := sqlMonitorAndMetrics.rootSQLMemoryMonitor.MakeBoundAccount()
	remoteFlowRunner := flowinfra.NewRemoteFlowRunner(cfg.AmbientCtx, stopper, &remoteFlowRunnerAcc)

	serverIterator := &kvFanoutClient{
		gossip:       g,
		rpcCtx:       rpcContext,
		db:           db,
		nodeLiveness: nodeLiveness,
		clock:        clock,
		st:           st,
		ambientCtx:   cfg.AmbientCtx,
	}

	// Instantiate the status API server.
	sStatus := newSystemStatusServer(
		cfg.AmbientCtx,
		st,
		cfg.Config,
		adminAuthzCheck,
		db,
		g,
		recorder,
		nodeLiveness,
		storePool,
		rpcContext,
		node.stores,
		stopper,
		sessionRegistry,
		closedSessionCache,
		remoteFlowRunner,
		internalExecutor,
		serverIterator,
		spanConfig.reporter,
		clock,
		rangestats.NewFetcher(db),
		node,
	)

	keyVisualizerServer := &KeyVisualizerServer{
		ie:         internalExecutor,
		settings:   st,
		nodeDialer: nodeDialer,
		status:     sStatus,
		node:       node,
	}
	keyVisServerAccessor := spanstatskvaccessor.New(keyVisualizerServer)

	// Instantiate the KV prober.
	kvProber := kvprober.NewProber(kvprober.Opts{
		Tracer:                  cfg.AmbientCtx.Tracer,
		DB:                      db,
		Settings:                st,
		HistogramWindowInterval: cfg.HistogramWindowInterval(),
	})
	registry.AddMetricStruct(kvProber.Metrics())

	flushInterval := 5 * time.Second
	flushTriggerBytesSize := uint64(1 << 20) // 1MB
	if cfg.TestingKnobs.EventExporter != nil {
		knobs := cfg.TestingKnobs.EventExporter.(*obs.EventExporterTestingKnobs)
		if knobs.FlushInterval != time.Duration(0) {
			flushInterval = knobs.FlushInterval
		}
		if knobs.FlushTriggerByteSize != 0 {
			flushTriggerBytesSize = knobs.FlushTriggerByteSize
		}
	}

	// Create the EventExporter, which will export events to the Obs Service.
	// We'll start it later, once we know our node ID.
	var eventsExporter obs.EventsExporterInterface
	if cfg.ObsServiceAddr != "" {
		if cfg.ObsServiceAddr == base.ObsServiceEmbedFlagValue {
			ee := obs.NewEventsExporter(
				"", // targetAddr - we'll configure a custom dialer connecting to the local node later
				timeutil.DefaultTimeSource{},
				cfg.Tracer,
				flushInterval,
				flushTriggerBytesSize,
				10*1<<20, // maxBufferSizeBytes - 10MB
				sqlMonitorAndMetrics.rootSQLMemoryMonitor, // memMonitor - this is not "SQL" usage, but we don't have another memory pool
			)
			eventsExporter = ee
		} else {
			targetAddr, err := obs.ValidateOTLPTargetAddr(cfg.ObsServiceAddr)
			if err != nil {
				return nil, err
			}
			ee := obs.NewEventsExporter(
				targetAddr,
				timeutil.DefaultTimeSource{},
				cfg.Tracer,
				flushInterval,
				flushTriggerBytesSize,
				10*1<<20, // maxBufferSizeBytes - 10MB
				sqlMonitorAndMetrics.rootSQLMemoryMonitor, // memMonitor - this is not "SQL" usage, but we don't have another memory pool
			)
			log.Infof(ctx, "will export events over OTLP to: %s", cfg.ObsServiceAddr)
			eventsExporter = ee
		}
	} else {
		eventsExporter = &obs.NoopEventsExporter{}
	}

	// The settings cache writer is responsible for persisting the
	// cluster settings on KV nodes across restarts.
	settingsWriter := newSettingsCacheWriter(engines[0], stopper)
	stopTrigger := newStopTrigger()

	// Initialize the pgwire pre-server, which initializes connections,
	// sets up TLS and reads client status parameters.
	pgPreServer := pgwire.NewPreServeConnHandler(
		cfg.AmbientCtx,
		cfg.Config,
		cfg.Settings,
		rpcContext.GetServerTLSConfig,
		cfg.HistogramWindowInterval(),
		sqlMonitorAndMetrics.rootSQLMemoryMonitor,
		true, /* acceptTenantName */
	)
	for _, m := range pgPreServer.Metrics() {
		registry.AddMetricStruct(m)
	}

	inspectzServer := inspectz.NewServer(
		cfg.BaseConfig.AmbientCtx,
		node.storeCfg.KVFlowHandles,
		node.storeCfg.KVFlowController,
	)

	// Instantiate the SQL server proper.
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
			kvStoresIterator:         kvserver.MakeStoresIterator(node.stores),
			inspectzServer:           inspectzServer,
		},
		SQLConfig:                &cfg.SQLConfig,
		BaseConfig:               &cfg.BaseConfig,
		stopper:                  stopper,
		stopTrigger:              stopTrigger,
		clock:                    clock,
		runtime:                  runtimeSampler,
		rpcContext:               rpcContext,
		nodeDescs:                g,
		systemConfigWatcher:      systemConfigWatcher,
		spanConfigAccessor:       spanConfig.kvAccessor,
		keyVisServerAccessor:     keyVisServerAccessor,
		nodeDialer:               nodeDialer,
		distSender:               distSender,
		db:                       db,
		registry:                 registry,
		recorder:                 recorder,
		sessionRegistry:          sessionRegistry,
		closedSessionCache:       closedSessionCache,
		remoteFlowRunner:         remoteFlowRunner,
		circularInternalExecutor: internalExecutor,
		internalDB:               insqlDB,
		circularJobRegistry:      jobRegistry,
		protectedtsProvider:      protectedtsProvider,
		rangeFeedFactory:         rangeFeedFactory,
		sqlStatusServer:          sStatus,
		tenantStatusServer:       sStatus,
		tenantUsageServer:        tenantUsage,
		monitorAndMetrics:        sqlMonitorAndMetrics,
		settingsStorage:          settingsWriter,
		eventsExporter:           eventsExporter,
		admissionPacerFactory:    gcoords.Elastic,
		rangeDescIteratorFactory: rangedesc.NewIteratorFactory(db),
		tenantCapabilitiesReader: sql.MakeSystemTenantOnly[tenantcapabilities.Reader](tenantCapabilitiesWatcher),
	})
	if err != nil {
		return nil, err
	}

	// Tell the authz server how to connect to SQL.
	adminAuthzCheck.SetAuthzAccessorFactory(func(opName string) (sql.AuthorizationAccessor, func()) {
		// This is a hack to get around a Go package dependency cycle. See comment
		// in sql/jobs/registry.go on planHookMaker.
		txn := db.NewTxn(ctx, "check-system-privilege")
		p, cleanup := sql.NewInternalPlanner(
			opName,
			txn,
			username.RootUserName(),
			&sql.MemoryMetrics{},
			sqlServer.execCfg,
			sql.NewInternalSessionData(ctx, sqlServer.execCfg.Settings, opName),
		)
		return p.(sql.AuthorizationAccessor), cleanup
	})

	// Create the authentication RPC server (login/logout).
	sAuth := authserver.NewServer(cfg.Config, sqlServer)

	// Create a drain server.
	drain := newDrainServer(cfg.BaseConfig, stopper, stopTrigger, grpcServer, sqlServer)
	drain.setNode(node, nodeLiveness)

	// Instantiate the admin API server.
	sAdmin := newSystemAdminServer(
		sqlServer,
		cfg.Settings,
		adminAuthzCheck,
		internalExecutor,
		cfg.BaseConfig.AmbientCtx,
		recorder,
		db,
		nodeLiveness,
		rpcContext,
		serverIterator,
		clock,
		distSender,
		grpcServer,
		drain,
		lateBoundServer,
	)

	// Connect the various servers to RPC.
	for i, gw := range []grpcGatewayServer{sAdmin, sStatus, sAuth, &sTS} {
		if reflect.ValueOf(gw).IsNil() {
			return nil, errors.Errorf("%d: nil", i)
		}
		gw.RegisterService(grpcServer.Server)
	}

	// Tell the node event logger (join, restart) how to populate SQL entries
	// into system.eventlog.
	node.InitLogger(sqlServer.execCfg)

	// Tell the status server how to access SQL structures.
	sStatus.setStmtDiagnosticsRequester(sqlServer.execCfg.StmtDiagnosticsRecorder)
	sStatus.baseStatusServer.sqlServer = sqlServer

	// Create a server controller.
	sc := newServerController(ctx,
		cfg.BaseConfig.AmbientCtx,
		node, cfg.BaseConfig.IDContainer,
		stopper, st,
		lateBoundServer,
		&systemServerWrapper{server: lateBoundServer},
		systemTenantNameContainer,
		pgPreServer.SendRoutingError,
	)
	drain.serverCtl = sc

	// Create the debug API server.
	debugServer := debug.NewServer(
		cfg.BaseConfig.AmbientCtx,
		st,
		sqlServer.pgServer.HBADebugFn(),
		sqlServer.execCfg.SQLStatusServer,
		// TODO(knz): Remove this once
		// https://github.com/cockroachdb/cockroach/issues/84585 is
		// implemented.
		func(ctx context.Context, name roachpb.TenantName) error {
			d, err := sc.getServer(ctx, name)
			if err != nil {
				return err
			}
			return errors.Newf("server found with type %T", d)
		},
		roachpb.SystemTenantID,
		authorizer,
	)

	recoveryServer := loqrecovery.NewServer(
		nodeIDContainer,
		st,
		stores,
		planStore,
		g,
		cfg.Locality,
		rpcContext,
		cfg.TestingKnobs.LOQRecovery,
		func(ctx context.Context, id roachpb.NodeID) error {
			return nodeTombStorage.SetDecommissioned(ctx, id, timeutil.Now())
		},
	)

	*lateBoundServer = topLevelServer{
		nodeIDContainer:           nodeIDContainer,
		cfg:                       cfg,
		st:                        st,
		clock:                     clock,
		rpcContext:                rpcContext,
		engines:                   engines,
		grpc:                      grpcServer,
		gossip:                    g,
		nodeDialer:                nodeDialer,
		nodeLiveness:              nodeLiveness,
		storePool:                 storePool,
		tcsFactory:                tcsFactory,
		distSender:                distSender,
		db:                        db,
		node:                      node,
		registry:                  registry,
		recorder:                  recorder,
		ruleRegistry:              ruleRegistry,
		promRuleExporter:          promRuleExporter,
		updates:                   updates,
		ctSender:                  ctSender,
		runtime:                   runtimeSampler,
		http:                      sHTTP,
		adminAuthzCheck:           adminAuthzCheck,
		admin:                     sAdmin,
		status:                    sStatus,
		drain:                     drain,
		decomNodeMap:              decomNodeMap,
		authentication:            sAuth,
		tsDB:                      tsDB,
		tsServer:                  &sTS,
		eventsExporter:            eventsExporter,
		recoveryServer:            recoveryServer,
		raftTransport:             raftTransport,
		stopper:                   stopper,
		stopTrigger:               stopTrigger,
		debug:                     debugServer,
		kvProber:                  kvProber,
		replicationReporter:       replicationReporter,
		protectedtsProvider:       protectedtsProvider,
		spanConfigSubscriber:      spanConfig.subscriber,
		spanConfigReporter:        spanConfig.reporter,
		tenantCapabilitiesWatcher: tenantCapabilitiesWatcher,
		pgPreServer:               pgPreServer,
		sqlServer:                 sqlServer,
		serverController:          sc,
		externalStorageBuilder:    externalStorageBuilder,
		storeGrantCoords:          gcoords.Stores,
		kvMemoryMonitor:           kvMemoryMonitor,
		keyVisualizerServer:       keyVisualizerServer,
		inspectzServer:            inspectzServer,
	}

	return lateBoundServer, err
}

// newClockFromConfig creates a HLC clock from the server configuration.
func newClockFromConfig(ctx context.Context, cfg BaseConfig) (*hlc.Clock, error) {
	maxOffset := time.Duration(cfg.MaxOffset)
	toleratedOffset := cfg.ToleratedOffset()
	var clock *hlc.Clock
	if cfg.ClockDevicePath != "" {
		ptpClock, err := ptp.MakeClock(ctx, cfg.ClockDevicePath)
		if err != nil {
			return nil, errors.Wrap(err, "instantiating clock source")
		}
		clock = hlc.NewClock(ptpClock, maxOffset, toleratedOffset)
	} else if cfg.TestingKnobs.Server != nil &&
		cfg.TestingKnobs.Server.(*TestingKnobs).WallClock != nil {
		clock = hlc.NewClock(cfg.TestingKnobs.Server.(*TestingKnobs).WallClock,
			maxOffset, toleratedOffset)
	} else {
		clock = hlc.NewClockWithSystemTimeSource(maxOffset, toleratedOffset)
	}
	return clock, nil
}

// ClusterSettings returns the cluster settings.
func (s *topLevelServer) ClusterSettings() *cluster.Settings {
	return s.st
}

// AnnotateCtx is a convenience wrapper; see AmbientContext.
func (s *topLevelServer) AnnotateCtx(ctx context.Context) context.Context {
	return s.cfg.AmbientCtx.AnnotateCtx(ctx)
}

// AnnotateCtxWithSpan is a convenience wrapper; see AmbientContext.
func (s *topLevelServer) AnnotateCtxWithSpan(
	ctx context.Context, opName string,
) (context.Context, *tracing.Span) {
	return s.cfg.AmbientCtx.AnnotateCtxWithSpan(ctx, opName)
}

// StorageClusterID returns the ID of the storage cluster this server is a part of.
func (s *topLevelServer) StorageClusterID() uuid.UUID {
	return s.rpcContext.StorageClusterID.Get()
}

// NodeID returns the ID of this node within its cluster.
func (s *topLevelServer) NodeID() roachpb.NodeID {
	return s.node.Descriptor.NodeID
}

// InitialStart returns whether this is the first time the node has started (as
// opposed to being restarted). Only intended to help print debugging info
// during server startup.
func (s *topLevelServer) InitialStart() bool {
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
//   - before blocking trying to connect to the Gossip network, we already open
//     the admin UI (so that its diagnostics are available)
//   - we also allow our Gossip and our connection health Ping service
//   - everything else returns Unavailable errors (which are retryable)
//   - once the node has started, unlock all RPCs.
//
// The passed context can be used to trace the server startup. The context
// should represent the general startup operation.
func (s *topLevelServer) PreStart(ctx context.Context) error {
	ctx = s.AnnotateCtx(ctx)
	done := startup.Begin(ctx)
	defer done()

	// The following initialization is mirrored in
	// (*SQLServerWrapper).PreStart. Please keep them in sync.

	// Start a context for the asynchronous network workers.
	workersCtx := s.AnnotateCtx(context.Background())

	if !s.cfg.Insecure {
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

	// Start the time sanity checker.
	s.startTime = timeutil.Now()
	if err := s.startMonitoringForwardClockJumps(workersCtx); err != nil {
		return err
	}

	// Connect the node as loopback handler for RPC requests to the
	// local node.
	s.rpcContext.SetLocalInternalServer(
		s.node,
		s.grpc.serverInterceptorsInfo, s.rpcContext.ClientInterceptors())

	// Load the TLS configuration for the HTTP server.
	uiTLSConfig, err := s.rpcContext.GetUIServerTLSConfig()
	if err != nil {
		return err
	}

	// Start the admin UI server. This opens the HTTP listen socket,
	// optionally sets up TLS, and dispatches the server worker for the
	// web UI.
	if err := startHTTPService(ctx,
		workersCtx, &s.cfg.BaseConfig, uiTLSConfig, s.stopper, s.serverController.httpMux); err != nil {
		return err
	}

	// Filter out self from the gossip bootstrap addresses.
	filtered := s.cfg.FilterGossipBootstrapAddresses(ctx)

	// Set up the init server. We have to do this relatively early because we
	// can't call RegisterInitServer() after `grpc.Serve`, which is called in
	// startRPCServer (and for the loopback grpc-gw connection).
	var initServer *initServer
	{
		getDialOpts := s.rpcContext.GRPCDialOptions
		initConfig := newInitServerConfig(ctx, s.cfg, getDialOpts)
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
		if err := kvstorage.WriteClusterVersionToEngines(
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
		// set an active cluster version (`SetActiveVersion`), we'll
		// persist it to all the engines first (`WriteClusterVersionToEngines`).
		// This happens at two places:
		//
		// - Right below, if we learn that we're the bootstrapping node, given
		//   we'll be setting the active cluster version as the binary version.
		// - Within the BumpClusterVersion RPC, when we're informed by another
		//   node what our new active cluster version should be.
	}

	serverpb.RegisterInitServer(s.grpc.Server, initServer)

	// Register the Migration service, to power internal crdb upgrades.
	migrationServer := &migrationServer{server: s}
	serverpb.RegisterMigrationServer(s.grpc.Server, migrationServer)
	s.migrationServer = migrationServer // only for testing via testServer

	// Register the KeyVisualizer Server
	keyvispb.RegisterKeyVisualizerServer(s.grpc.Server, s.keyVisualizerServer)

	// Start the RPC server. This opens the RPC/SQL listen socket,
	// and dispatches the server worker for the RPC.
	// The SQL listener is returned, to start the SQL server later
	// below when the server has initialized.
	pgL, loopbackPgL, rpcLoopbackDialFn, startRPCServer, err := startListenRPCAndSQL(
		ctx, workersCtx, s.cfg.BaseConfig, s.stopper, s.grpc, true /* enableSQLListener */)
	if err != nil {
		return err
	}
	s.pgL = pgL
	s.loopbackPgL = loopbackPgL

	// Tell the RPC context how to connect in-memory.
	s.rpcContext.SetLoopbackDialer(rpcLoopbackDialFn)

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

	// Connect the various RPC handlers to the gRPC gateway.
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
			if err := os.WriteFile(file, []byte(val), 0644); err != nil {
				return errors.Wrapf(err, "failed to write %s", file)
			}
		}
		// TODO(knz): Do we really want to write the listener files
		// in _every_ store directory? Not just the first one?
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
	state, initialStart, err := initServer.ServeAndWait(workersCtx, s.stopper, &s.cfg.Settings.SV)
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
		if err := kvstorage.WriteClusterVersionToEngines(
			ctx, s.engines, state.clusterVersion,
		); err != nil {
			return err
		}

		if err := s.ClusterSettings().Version.SetActiveVersion(ctx, state.clusterVersion); err != nil {
			return err
		}
	}

	s.rpcContext.StorageClusterID.Set(ctx, state.clusterID)
	s.rpcContext.NodeID.Set(ctx, state.nodeID)

	// Ensure components in the DistSQLPlanner that rely on the node ID are
	// initialized before store startup continues.
	s.sqlServer.execCfg.DistSQLPlanner.SetGatewaySQLInstanceID(base.SQLInstanceID(state.nodeID))
	s.sqlServer.execCfg.DistSQLPlanner.ConstructAndSetSpanResolver(ctx, state.nodeID, s.cfg.Locality)

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

	initialStoreIDs, err := state.initialStoreIDs()
	if err != nil {
		return err
	}

	// Inform the raft transport of these initial store IDs.
	s.raftTransport.SetInitialStoreIDs(initialStoreIDs)

	// Spawn a goroutine that will print a nice message when Gossip connects.
	// Note that we already know the clusterID, but we don't know that Gossip
	// has connected. The pertinent case is that of restarting an entire
	// cluster. Someone has to gossip the ClusterID before Gossip is connected,
	// but this gossip only happens once the first range has a leaseholder, i.e.
	// when a quorum of nodes has gone fully operational.
	_ = s.stopper.RunAsyncTask(workersCtx, "connect-gossip", func(ctx context.Context) {
		log.Ops.Infof(ctx, "connecting to gossip network to verify cluster ID %q", state.clusterID)
		select {
		case <-s.gossip.Connected:
			log.Ops.Infof(ctx, "node connected via gossip")
		case <-ctx.Done():
		case <-s.stopper.ShouldQuiesce():
		}
	})

	// Start measuring the Go scheduler latency.
	if err := schedulerlatency.StartSampler(
		workersCtx, s.st, s.stopper, s.registry, base.DefaultMetricsSampleInterval,
	); err != nil {
		return err
	}

	// Check that the HLC clock is only moving forward.
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
	s.gossip.Start(advAddrU, filtered, s.rpcContext)
	log.Event(ctx, "started gossip")

	// Now that we have a monotonic HLC wrt previous incarnations of the process,
	// init all the replicas. At this point *some* store has been initialized or
	// we're joining an existing cluster for the first time.
	advSQLAddrU := util.NewUnresolvedAddr("tcp", s.cfg.SQLAdvertiseAddr)

	advHTTPAddrU := util.NewUnresolvedAddr("tcp", s.cfg.HTTPAdvertiseAddr)

	if err := s.node.start(
		ctx, workersCtx,
		advAddrU,
		advSQLAddrU,
		advHTTPAddrU,
		*state,
		initialStart,
		s.cfg.ClusterName,
		s.cfg.NodeAttributes,
		s.cfg.Locality,
		s.cfg.LocalityAddresses,
	); err != nil {
		return err
	}

	log.Event(ctx, "started node")
	if err := s.startPersistingHLCUpperBound(ctx, hlcUpperBoundExists); err != nil {
		return err
	}
	s.replicationReporter.Start(workersCtx, s.stopper)

	// Configure the Sentry reporter to add some additional context to reports.
	sentry.ConfigureScope(func(scope *sentry.Scope) {
		scope.SetTags(map[string]string{
			"cluster":         s.StorageClusterID().String(),
			"node":            s.NodeID().String(),
			"server_id":       fmt.Sprintf("%s-%s", s.StorageClusterID().Short(), s.NodeID()),
			"engine_type":     s.cfg.StorageEngine.String(),
			"encrypted_store": strconv.FormatBool(encryptedStore),
		})
	})

	// Init a log metrics registry.
	logRegistry := logmetrics.NewRegistry()
	if logRegistry == nil {
		panic(errors.AssertionFailedf("nil log metrics registry at server startup"))
	}

	// We can now add the node registry.
	s.recorder.AddNode(
		s.registry,
		logRegistry,
		s.node.Descriptor,
		s.node.startedAt,
		s.cfg.AdvertiseAddr,
		s.cfg.HTTPAdvertiseAddr,
		s.cfg.SQLAdvertiseAddr,
	)

	// Begin recording runtime statistics.
	if err := startSampleEnvironment(workersCtx,
		s.ClusterSettings(),
		s.stopper,
		s.cfg.GoroutineDumpDirName,
		s.cfg.HeapProfileDirName,
		s.cfg.CPUProfileDirName,
		s.runtime,
		s.status.sessionRegistry,
	); err != nil {
		return err
	}

	// Begin recording time series data collected by the status monitor.
	// The writes will be async; we'll wait for the first one to go through
	// later in this method, using the returned channel.
	firstTSDBPollDone := s.tsDB.PollSource(
		s.cfg.AmbientCtx, s.recorder, base.DefaultMetricsSampleInterval, ts.Resolution10s, s.stopper,
	)

	// Export statistics to graphite, if enabled by configuration.
	var graphiteOnce sync.Once
	graphiteEndpoint.SetOnChange(&s.st.SV, func(context.Context) {
		if graphiteEndpoint.Get(&s.st.SV) != "" {
			graphiteOnce.Do(func() {
				startGraphiteStatsExporter(workersCtx, s.stopper, s.recorder, s.st)
			})
		}
	})

	// Start the protected timestamp subsystem. Note that this needs to happen
	// before the modeOperational switch below, as the protected timestamps
	// subsystem will crash if accessed before being Started (and serving general
	// traffic may access it).
	//
	// See https://github.com/cockroachdb/cockroach/issues/73897.
	if err := s.protectedtsProvider.Start(workersCtx, s.stopper); err != nil {
		// TODO(knz,arul): This mechanism could probably be removed now.
		// The PTS Cache is a thing from the past when secondary tenants
		// couldn’t use protected timestamps. We started using span configs
		// (in both the system and secondary tenants) to store PTS
		// information in 22.1, at which point the PTS cache was only kept
		// around to migrate between the old and new subsystems.
		return err
	}

	// After setting modeOperational, we can block until all stores are fully
	// initialized.
	s.grpc.setMode(modeOperational)

	s.nodeLiveness.Start(workersCtx)

	// We'll block here until all stores are fully initialized. We do this here
	// for several reasons:
	// - some of the components below depend on all stores being fully
	//   initialized (like the debug server registration for e.g.)
	// - we'll need to do it after having opened up the RPC floodgates (due to
	//   the hazard described in Node.start, around initializing additional
	//   stores)
	// - we'll need to do it after starting node liveness, see:
	//   https://github.com/cockroachdb/cockroach/issues/106706#issuecomment-1640254715
	s.node.waitForAdditionalStoreInit()

	additionalStoreIDs, err := state.additionalStoreIDs()
	if err != nil {
		return err
	}

	// Inform the raft transport of these additional store IDs.
	s.raftTransport.SetAdditionalStoreIDs(additionalStoreIDs)

	// Connect the engines to the disk stats map constructor. This needs to
	// wait until after waitForAdditionalStoreInit returns since it realizes on
	// wholly initialized stores (it reads the StoreIdentKeys). It also needs
	// to come before the call into SetPebbleMetricsProvider, which internally
	// uses the disk stats map we're initializing.
	if err := s.node.registerEnginesForDiskStatsMap(s.cfg.Stores.Specs, s.engines); err != nil {
		return errors.Wrapf(err, "failed to register engines for the disk stats map")
	}

	// Stores have been initialized, so Node can now provide Pebble metrics.
	//
	// Note that all existing stores will be operational before Pebble-level
	// admission control is online. However, we won’t have started to heartbeat
	// our liveness record until after we call SetPebbleMetricsProvider, so the
	// existing stores shouldn’t be able to acquire leases yet. Although, below
	// Raft commands like log application and snapshot application may be able
	// to bypass admission control.
	s.storeGrantCoords.SetPebbleMetricsProvider(ctx, s.node, s.node)

	// Once all stores are initialized, check if offline storage recovery
	// was done prior to start and record any actions appropriately.
	logPendingLossOfQuorumRecoveryEvents(workersCtx, s.node.stores)

	// Report server listen addresses to logs.
	log.Ops.Infof(ctx, "starting %s server at %s (use: %s)",
		redact.Safe(s.cfg.HTTPRequestScheme()), log.SafeManaged(s.cfg.HTTPAddr), log.SafeManaged(s.cfg.HTTPAdvertiseAddr))
	rpcConnType := redact.SafeString("grpc/postgres")
	if s.cfg.SplitListenSQL {
		rpcConnType = "grpc"
		log.Ops.Infof(ctx, "starting postgres server at %s (use: %s)",
			log.SafeManaged(s.cfg.SQLAddr), log.SafeManaged(s.cfg.SQLAdvertiseAddr))
	}
	log.Ops.Infof(ctx, "starting %s server at %s", log.SafeManaged(rpcConnType), log.SafeManaged(s.cfg.Addr))
	log.Ops.Infof(ctx, "advertising CockroachDB node at %s", log.SafeManaged(s.cfg.AdvertiseAddr))

	log.Event(ctx, "accepting connections")

	// Begin recording status summaries.
	if err := s.node.startWriteNodeStatus(base.DefaultMetricsSampleInterval); err != nil {
		return err
	}

	if subscriber, ok := s.spanConfigSubscriber.(*spanconfigkvsubscriber.KVSubscriber); ok {
		if err := subscriber.Start(workersCtx, s.stopper); err != nil {
			return err
		}
	}
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
		s.inspectzServer,  /* handleInspectzUnauthenticated */
		newAPIV2Server(ctx, &apiV2ServerOpts{
			admin:            s.admin,
			status:           s.status,
			promRuleExporter: s.promRuleExporter,
			sqlServer:        s.sqlServer,
			db:               s.db,
		}), /* apiServer */
		serverpb.FeatureFlags{
			CanViewKvMetricDashboards:   s.rpcContext.TenantID.Equal(roachpb.SystemTenantID),
			DisableKvLevelAdvancedDebug: false,
		},
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

	// Start the SQL subsystem.
	if err := s.sqlServer.preStart(
		workersCtx,
		s.stopper,
		s.cfg.TestingKnobs,
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
		s.cfg.ExternalIODirConfig,
		s.st,
		s.sqlServer.sqlIDContainer,
		s.nodeDialer,
		s.cfg.TestingKnobs,
		true, /* allowLocalFastPath */
		s.sqlServer.execCfg.InternalDB.CloneWithMemoryMonitor(sql.MemoryMetrics{}, ieMon),
		nil, /* TenantExternalIORecorder */
		s.registry,
	)
	if err := s.cfg.ExternalStorageAccessor.Init(
		s.externalStorageBuilder.makeExternalStorage, s.externalStorageBuilder.makeExternalStorageFromURI,
	); err != nil {
		return err
	}

	// If enabled, start reporting diagnostics.
	if s.cfg.StartDiagnosticsReporting && !cluster.TelemetryOptOut {
		s.startDiagnostics(workersCtx)
	}

	s.eventsExporter.SetNodeInfo(obs.NodeInfo{
		ClusterID:     state.clusterID,
		NodeID:        int32(state.nodeID),
		BinaryVersion: build.BinaryVersion(),
	})
	if s.cfg.ObsServiceAddr != base.ObsServiceEmbedFlagValue {
		if err := s.eventsExporter.Start(ctx, s.stopper); err != nil {
			return errors.Wrapf(err, "failed to start events exporter")
		}
	}

	if storage.WorkloadCollectorEnabled {
		if err := s.debug.RegisterWorkloadCollector(s.node.stores); err != nil {
			return errors.Wrapf(err, "failed to register workload collector with debug server")
		}
	}

	// Register the engines debug endpoints.
	if err := s.debug.RegisterEngines(s.cfg.Stores.Specs, s.engines); err != nil {
		return errors.Wrapf(err, "failed to register engines with debug server")
	}

	// Register the ctc debug endpoints.
	s.debug.RegisterClosedTimestampSideTransport(s.ctSender, s.node.storeCfg.ClosedTimestampReceiver)

	// Start the closed timestamp loop.
	s.ctSender.Run(workersCtx, state.nodeID)

	// Start dispatching extant flow tokens.
	if err := s.raftTransport.Start(workersCtx); err != nil {
		return err
	}

	// Attempt to upgrade cluster version now that the sql server has been
	// started. At this point we know that all startupmigrations and permanent
	// upgrades have successfully been run so it is safe to upgrade to the
	// binary's current version.
	//
	// NB: We run this under the startup ctx (not workersCtx) so as to ensure
	// all the upgrade steps are traced, for use during troubleshooting.
	if err := s.startAttemptUpgrade(ctx); err != nil {
		return errors.Wrap(err, "cannot start upgrade task")
	}

	// Initialize the key visualizer boundary subscriber rangefeed,
	// and start the rangefeed to broadcast updates to the collector.
	if err := keyvissubscriber.Start(
		ctx,
		s.stopper,
		s.db,
		s.ClusterSettings(),
		s.sqlServer.execCfg.SystemTableIDResolver,
		s.clock.Now(),
		func(update *keyvispb.UpdateBoundariesRequest) {
			s.node.spanStatsCollector.SaveBoundaries(update.Boundaries, update.Time)
		}); err != nil {
		return err
	}

	if err := s.node.tenantSettingsWatcher.Start(workersCtx, s.sqlServer.execCfg.SystemTableIDResolver); err != nil {
		return errors.Wrap(err, "failed to initialize the tenant settings watcher")
	}
	if err := s.tenantCapabilitiesWatcher.Start(ctx); err != nil {
		return errors.Wrap(err, "initializing tenant capabilities")
	}
	// Now that we've got the tenant capabilities subsystem all started, we bind
	// the Reader to the TenantRPCAuthorizer, so that it has a handle into the
	// global tenant capabilities state.
	s.rpcContext.TenantRPCAuthorizer.BindReader(s.tenantCapabilitiesWatcher)

	if err := s.kvProber.Start(workersCtx, s.stopper); err != nil {
		return errors.Wrapf(err, "failed to start KV prober")
	}

	// Perform loss of quorum recovery cleanup if any actions were scheduled.
	// Cleanup actions rely on node being connected to the cluster and hopefully
	// in a healthy or healthier stats to update node liveness records.
	maybeRunLossOfQuorumRecoveryCleanup(
		ctx,
		s.node.execCfg.InternalDB.Executor(),
		s.node.stores,
		s,
		s.stopper)

	// Let the server controller start watching tenant service mode changes.
	if err := s.serverController.start(workersCtx,
		s.node.execCfg.InternalDB.Executor(),
	); err != nil {
		return errors.Wrap(err, "failed to start the server controller")
	}

	log.Event(ctx, "server initialized")

	// Wait for the first ts poll to have succeeded before acknowledging server
	// start. This helps with predictable tests.
	select {
	case <-s.stopper.ShouldQuiesce():
	case <-firstTSDBPollDone:
	}
	return maybeImportTS(ctx, s)
}

// AcceptClients starts listening for incoming SQL clients over the network.
// This mirrors the implementation of (*SQLServerWrapper).AcceptClients.
// TODO(knz): Find a way to implement this method only once for both.
func (s *topLevelServer) AcceptClients(ctx context.Context) error {
	workersCtx := s.AnnotateCtx(context.Background())

	if err := startServeSQL(
		workersCtx,
		s.stopper,
		s.pgPreServer,
		s.serverController.sqlMux,
		s.pgL,
		&s.cfg.SocketFile,
	); err != nil {
		return err
	}

	if err := structlogging.StartHotRangesLoggingScheduler(
		ctx,
		s.stopper,
		s.status,
		*s.sqlServer.internalExecutor,
		s.ClusterSettings(),
	); err != nil {
		return err
	}

	s.sqlServer.isReady.Set(true)

	log.Event(ctx, "server ready")
	return nil
}

// AcceptInternalClients starts listening for incoming SQL connections on the
// internal loopback interface.
func (s *topLevelServer) AcceptInternalClients(ctx context.Context) error {
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

				if err := s.serverController.sqlMux(connCtx, conn, status); err != nil {
					log.Ops.Errorf(connCtx, "serving internal SQL client conn: %s", err)
				}
			})
			netutil.FatalIfUnexpected(err)
		})
}

// ShutdownRequested returns a channel that is signaled when a subsystem wants
// the server to be shut down.
func (s *topLevelServer) ShutdownRequested() <-chan serverctl.ShutdownRequest {
	return s.stopTrigger.C()
}

// TempDir returns the filepath of the temporary directory used for temp storage.
// It is empty for an in-memory temp storage.
func (s *topLevelServer) TempDir() string {
	return s.cfg.TempStorageConfig.Path
}

// PGServer exports the pgwire server. Used by tests.
func (s *topLevelServer) PGServer() *pgwire.Server {
	return s.sqlServer.pgServer
}

// SpanConfigReporter returns the spanconfig.Reporter. Used by tests.
func (s *topLevelServer) SpanConfigReporter() spanconfig.Reporter {
	return s.spanConfigReporter
}

// LogicalClusterID implements cli.serverStartupInterface. This
// implementation exports the logical cluster ID of the system tenant.
func (s *topLevelServer) LogicalClusterID() uuid.UUID {
	return s.sqlServer.LogicalClusterID()
}

// startDiagnostics starts periodic diagnostics reporting and update checking.
func (s *topLevelServer) startDiagnostics(ctx context.Context) {
	s.updates.PeriodicallyCheckForUpdates(ctx, s.stopper)
	s.sqlServer.StartDiagnostics(ctx)
}

func init() {
	tracing.RegisterTagRemapping("n", "node")
}

// Insecure returns true iff the server has security disabled.
func (s *topLevelServer) Insecure() bool {
	return s.cfg.Insecure
}

// TenantCapabilitiesReader returns the Server's tenantcapabilities.Reader.
func (s *topLevelServer) TenantCapabilitiesReader() tenantcapabilities.Reader {
	return s.tenantCapabilitiesWatcher
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
func (s *topLevelServer) Drain(
	ctx context.Context, verbose bool,
) (remaining uint64, info redact.RedactableString, err error) {
	return s.drain.runDrain(ctx, verbose)
}

// MakeServerOptionsForURL creates the input for MakeURLForServer().
// Beware of not calling this too early; the server address
// is finalized late in the network initialization sequence.
func MakeServerOptionsForURL(
	baseCfg *base.Config,
) (clientsecopts.ClientSecurityOptions, clientsecopts.ServerParameters) {
	clientConnOptions := clientsecopts.ClientSecurityOptions{
		Insecure: baseCfg.Insecure,
		CertsDir: baseCfg.SSLCertsDir,
	}
	serverParams := clientsecopts.ServerParameters{
		ServerAddr:      baseCfg.SQLAdvertiseAddr,
		DefaultPort:     base.DefaultPort,
		DefaultDatabase: catalogkeys.DefaultDatabaseName,
	}
	return clientConnOptions, serverParams
}

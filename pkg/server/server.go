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
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cmux"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/cloud"
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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/reports"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/diagnostics"
	"github.com/cockroachdb/cockroach/pkg/server/goroutinedumper"
	"github.com/cockroachdb/cockroach/pkg/server/heapprofiler"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	_ "github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigjob" // register jobs declared outside of pkg/sql
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigkvaccessor"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigkvsubscriber"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/contention"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	_ "github.com/cockroachdb/cockroach/pkg/sql/gcjob" // register jobs declared outside of pkg/sql
	"github.com/cockroachdb/cockroach/pkg/sql/optionalnodeliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	_ "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scjob" // register jobs declared outside of pkg/sql
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/ui"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/goschedstats"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/cockroachdb/sentry-go"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"
	yaml "gopkg.in/yaml.v2"
)

var (
	// Allocation pool for gzipResponseWriters.
	gzipResponseWriterPool sync.Pool

	forwardClockJumpCheckEnabled = settings.RegisterBoolSetting(
		settings.TenantWritable,
		"server.clock.forward_jump_check_enabled",
		"if enabled, forward clock jumps > max_offset/2 will cause a panic",
		false,
	).WithPublic()

	persistHLCUpperBoundInterval = settings.RegisterDurationSetting(
		settings.TenantWritable,
		"server.clock.persist_upper_bound_interval",
		"the interval between persisting the wall time upper bound of the clock. The clock "+
			"does not generate a wall time greater than the persisted timestamp and will panic if "+
			"it sees a wall time greater than this value. When cockroach starts, it waits for the "+
			"wall time to catch-up till this persisted timestamp. This guarantees monotonic wall "+
			"time across server restarts. Not setting this or setting a value of 0 disables this "+
			"feature.",
		0,
	).WithPublic()
)

// Server is the cockroach server node.
type Server struct {
	// The following fields are populated in NewServer.

	nodeIDContainer *base.NodeIDContainer
	cfg             Config
	st              *cluster.Settings
	mux             http.ServeMux
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

	admin           *adminServer
	status          *statusServer
	authentication  *authenticationServer
	migrationServer *migrationServer
	oidc            OIDC
	tsDB            *ts.DB
	tsServer        *ts.Server
	raftTransport   *kvserver.RaftTransport
	stopper         *stop.Stopper

	debug    *debug.Server
	kvProber *kvprober.Prober

	replicationReporter   *reports.Reporter
	protectedtsProvider   protectedts.Provider
	protectedtsReconciler *ptreconcile.Reconciler

	spanConfigSubscriber *spanconfigkvsubscriber.KVSubscriber

	sqlServer    *SQLServer
	drainSleepFn func(time.Duration)

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

// externalStorageBuilder is a wrapper around the ExternalStorage factory
// methods. It allows us to separate the creation and initialization of the
// builder between NewServer() and Start() respectively.
// TODO(adityamaru): Consider moving this to pkg/cloud/impl at a future
// stage of the ongoing refactor.
type externalStorageBuilder struct {
	conf              base.ExternalIODirConfig
	settings          *cluster.Settings
	blobClientFactory blobs.BlobClientFactory
	initCalled        bool
	ie                *sql.InternalExecutor
	db                *kv.DB
}

func (e *externalStorageBuilder) init(
	conf base.ExternalIODirConfig,
	settings *cluster.Settings,
	blobClientFactory blobs.BlobClientFactory,
	ie *sql.InternalExecutor,
	db *kv.DB,
) {
	e.conf = conf
	e.settings = settings
	e.blobClientFactory = blobClientFactory
	e.initCalled = true
	e.ie = ie
	e.db = db
}

func (e *externalStorageBuilder) makeExternalStorage(
	ctx context.Context, dest roachpb.ExternalStorage,
) (cloud.ExternalStorage, error) {
	if !e.initCalled {
		return nil, errors.New("cannot create external storage before init")
	}
	return cloud.MakeExternalStorage(ctx, dest, e.conf, e.settings, e.blobClientFactory, e.ie,
		e.db)
}

func (e *externalStorageBuilder) makeExternalStorageFromURI(
	ctx context.Context, uri string, user security.SQLUsername,
) (cloud.ExternalStorage, error) {
	if !e.initCalled {
		return nil, errors.New("cannot create external storage before init")
	}
	return cloud.ExternalStorageFromURI(ctx, uri, e.conf, e.settings, e.blobClientFactory, user, e.ie, e.db)
}

// NewServer creates a Server from a server.Config.
func NewServer(cfg Config, stopper *stop.Stopper) (*Server, error) {
	if err := cfg.ValidateAddrs(context.Background()); err != nil {
		return nil, err
	}

	st := cfg.Settings

	if cfg.Tracer == nil {
		panic(errors.New("no tracer set in BaseConfig"))
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
	stopper.SetTracer(cfg.Tracer)
	stopper.AddCloser(cfg.Tracer)

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

	nodeTombStorage := &nodeTombstoneStorage{engs: engines}
	checkPingFor := func(ctx context.Context, nodeID roachpb.NodeID, errorCode codes.Code) error {
		ts, err := nodeTombStorage.IsDecommissioned(ctx, nodeID)
		if err != nil {
			// An error here means something very basic is not working. Better to terminate
			// than to limp along.
			log.Fatalf(ctx, "unable to read decommissioned status for n%d: %v", nodeID, err)
		}
		if !ts.IsZero() {
			// The node was decommissioned.
			return grpcstatus.Errorf(errorCode,
				"n%d was permanently removed from the cluster at %s; it is not allowed to rejoin the cluster",
				nodeID, ts,
			)
		}
		// The common case - target node is not decommissioned.
		return nil
	}

	rpcCtxOpts := rpc.ContextOptions{
		TenantID:  roachpb.SystemTenantID,
		NodeID:    cfg.IDContainer,
		ClusterID: cfg.ClusterIDContainer,
		Config:    cfg.Config,
		Clock:     clock,
		Stopper:   stopper,
		Settings:  cfg.Settings,
		OnOutgoingPing: func(req *rpc.PingRequest) error {
			// Outgoing ping will block requests with codes.FailedPrecondition to
			// notify caller that this replica is decommissioned but others could
			// still be tried as caller node is valid, but not the destination.
			return checkPingFor(ctx, req.TargetNodeID, codes.FailedPrecondition)
		},
		OnIncomingPing: func(req *rpc.PingRequest) error {
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

	gcoords, metrics := admission.NewGrantCoordinators(
		cfg.AmbientCtx,
		admission.Options{
			MinCPUSlots:                    1,
			MaxCPUSlots:                    100000, /* TODO(sumeer): add cluster setting */
			SQLKVResponseBurstTokens:       100000, /* TODO(sumeer): add cluster setting */
			SQLSQLResponseBurstTokens:      100000, /* arbitrary, and unused */
			SQLStatementLeafStartWorkSlots: 100,    /* arbitrary, and unused */
			SQLStatementRootStartWorkSlots: 100,    /* arbitrary, and unused */
			Settings:                       st,
		})
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
		Tracer:                  stopper.Tracer(),
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

	tsDB := ts.NewDB(db, cfg.Settings)
	registry.AddMetricStruct(tsDB.Metrics())
	nodeCountFn := func() int64 {
		return nodeLiveness.Metrics().LiveNodes.Value()
	}
	sTS := ts.MakeServer(cfg.AmbientCtx, tsDB, nodeCountFn, cfg.TimeSeriesServerConfig, stopper)

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
	externalStorage := func(ctx context.Context, dest roachpb.ExternalStorage) (cloud.
		ExternalStorage, error) {
		return externalStorageBuilder.makeExternalStorage(ctx, dest)
	}
	externalStorageFromURI := func(ctx context.Context, uri string,
		user security.SQLUsername) (cloud.ExternalStorage, error) {
		return externalStorageBuilder.makeExternalStorageFromURI(ctx, uri, user)
	}
	protectedtsProvider, err := ptprovider.New(ptprovider.Config{
		DB:               db,
		InternalExecutor: internalExecutor,
		Settings:         st,
	})
	if err != nil {
		return nil, err
	}

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
	stopper.AddCloser(stop.CloserFn(func() {
		kvMemoryMonitor.Stop(ctx)
	}))

	storeCfg := kvserver.StoreConfig{
		DefaultSpanConfig:       cfg.DefaultZoneConfig.AsSpanConfig(),
		Settings:                st,
		AmbientCtx:              cfg.AmbientCtx,
		RaftConfig:              cfg.RaftConfig,
		Clock:                   clock,
		DB:                      db,
		Gossip:                  g,
		NodeLiveness:            nodeLiveness,
		Transport:               raftTransport,
		NodeDialer:              nodeDialer,
		RPCContext:              rpcContext,
		ScanInterval:            cfg.ScanInterval,
		ScanMinIdleTime:         cfg.ScanMinIdleTime,
		ScanMaxIdleTime:         cfg.ScanMaxIdleTime,
		HistogramWindowInterval: cfg.HistogramWindowInterval(),
		StorePool:               storePool,
		SQLExecutor:             internalExecutor,
		LogRangeEvents:          cfg.EventLogEnabled,
		RangeDescriptorCache:    distSender.RangeDescriptorCache(),
		TimeSeriesDataStore:     tsDB,
		ClosedTimestampSender:   ctSender,
		ClosedTimestampReceiver: ctReceiver,
		ExternalStorage:         externalStorage,
		ExternalStorageFromURI:  externalStorageFromURI,
		ProtectedTimestampCache: protectedtsProvider,
		KVMemoryMonitor:         kvMemoryMonitor,
	}

	var spanConfig struct {
		// kvAccessor powers the span configuration RPCs and the host tenant's
		// reconciliation job.
		kvAccessor spanconfig.KVAccessor
		// subscriber is used by stores to subsribe to span configuration
		// updates.
		subscriber *spanconfigkvsubscriber.KVSubscriber
		// kvAccessorForTenantRecords is when creating/destroying secondary
		// tenant records.
		kvAccessorForTenantRecords spanconfig.KVAccessor
	}
	if cfg.SpanConfigsEnabled {
		storeCfg.SpanConfigsEnabled = true
		spanConfigKnobs, _ := cfg.TestingKnobs.SpanConfig.(*spanconfig.TestingKnobs)
		if spanConfigKnobs != nil && spanConfigKnobs.StoreKVSubscriberOverride != nil {
			storeCfg.SpanConfigSubscriber = spanConfigKnobs.StoreKVSubscriberOverride
		} else {
			spanConfig.subscriber = spanconfigkvsubscriber.New(
				stopper,
				db,
				clock,
				rangeFeedFactory,
				keys.SpanConfigurationsTableID,
				1<<20, /* 1 MB */
				storeCfg.DefaultSpanConfig,
				spanConfigKnobs,
			)
			storeCfg.SpanConfigSubscriber = spanConfig.subscriber
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
	}

	if storeTestingKnobs := cfg.TestingKnobs.Store; storeTestingKnobs != nil {
		storeCfg.TestingKnobs = *storeTestingKnobs.(*kvserver.StoreTestingKnobs)
	}

	recorder := status.NewMetricsRecorder(clock, nodeLiveness, rpcContext, g, st)
	registry.AddMetricStruct(rpcContext.RemoteClocks.Metrics())

	updates := &diagnostics.UpdateChecker{
		StartTime:     timeutil.Now(),
		AmbientCtx:    &cfg.AmbientCtx,
		Stopper:       stopper,
		Config:        cfg.BaseConfig.Config,
		Settings:      cfg.Settings,
		ClusterID:     cfg.ClusterIDContainer.Get,
		NodeID:        nodeIDContainer.Get,
		SQLInstanceID: idContainer.SQLInstanceID,
	}

	var drainSleepFn = time.Sleep
	if cfg.TestingKnobs.Server != nil {
		if cfg.TestingKnobs.Server.(*TestingKnobs).DrainSleepFn != nil {
			drainSleepFn = cfg.TestingKnobs.Server.(*TestingKnobs).DrainSleepFn
		}
		updates.TestingKnobs = &cfg.TestingKnobs.Server.(*TestingKnobs).DiagnosticsTestingKnobs
	}

	tenantUsage := NewTenantUsageServer(st, db, internalExecutor)
	registry.AddMetricStruct(tenantUsage.Metrics())

	node := NewNode(
		storeCfg, recorder, registry, stopper,
		txnMetrics, stores, nil /* execCfg */, cfg.ClusterIDContainer,
		gcoords.Regular.GetWorkQueue(admission.KVWork), gcoords.Stores,
		tenantUsage, spanConfig.kvAccessor,
	)
	roachpb.RegisterInternalServer(grpcServer.Server, node)
	kvserver.RegisterPerReplicaServer(grpcServer.Server, node.perReplicaServer)
	kvserver.RegisterPerStoreServer(grpcServer.Server, node.perReplicaServer)
	ctpb.RegisterSideTransportServer(grpcServer.Server, ctReceiver)
	replicationReporter := reports.NewReporter(
		db, node.stores, storePool, st, nodeLiveness, internalExecutor)

	protectedtsReconciler := ptreconcile.NewReconciler(ptreconcile.Config{
		Settings: st,
		Stores:   node.stores,
		DB:       db,
		Storage:  protectedtsProvider,
		Cache:    protectedtsProvider,
		StatusFuncs: ptreconcile.StatusFuncs{
			jobsprotectedts.GetMetaType(jobsprotectedts.Jobs): jobsprotectedts.MakeStatusFunc(
				jobRegistry, internalExecutor, jobsprotectedts.Jobs),
			jobsprotectedts.GetMetaType(jobsprotectedts.Schedules): jobsprotectedts.MakeStatusFunc(jobRegistry,
				internalExecutor, jobsprotectedts.Schedules),
		},
	})
	registry.AddMetricStruct(protectedtsReconciler.Metrics())

	lateBoundServer := &Server{}
	// TODO(tbg): give adminServer only what it needs (and avoid circular deps).
	sAdmin := newAdminServer(lateBoundServer, internalExecutor)
	sessionRegistry := sql.NewSessionRegistry()
	contentionRegistry := contention.NewRegistry()
	flowScheduler := flowinfra.NewFlowScheduler(cfg.AmbientCtx, stopper, st)

	sStatus := newStatusServer(
		cfg.AmbientCtx,
		st,
		cfg.Config,
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
		contentionRegistry,
		flowScheduler,
		internalExecutor,
	)
	// TODO(tbg): don't pass all of Server into this to avoid this hack.
	sAuth := newAuthenticationServer(lateBoundServer)
	for i, gw := range []grpcGatewayServer{sAdmin, sStatus, sAuth, &sTS} {
		if reflect.ValueOf(gw).IsNil() {
			return nil, errors.Errorf("%d: nil", i)
		}
		gw.RegisterService(grpcServer.Server)
	}

	var jobAdoptionStopFile string
	for _, spec := range cfg.Stores.Specs {
		if !spec.InMemory && spec.Path != "" {
			jobAdoptionStopFile = filepath.Join(spec.Path, jobs.PreventAdoptionFile)
			break
		}
	}

	kvProber := kvprober.NewProber(kvprober.Opts{
		Tracer:                  stopper.Tracer(),
		DB:                      db,
		Settings:                st,
		HistogramWindowInterval: cfg.HistogramWindowInterval(),
	})
	registry.AddMetricStruct(kvProber.Metrics())

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
		systemConfigProvider:     g,
		spanConfigAccessor:       spanConfig.kvAccessor,
		nodeDialer:               nodeDialer,
		distSender:               distSender,
		db:                       db,
		registry:                 registry,
		recorder:                 recorder,
		sessionRegistry:          sessionRegistry,
		contentionRegistry:       contentionRegistry,
		flowScheduler:            flowScheduler,
		circularInternalExecutor: internalExecutor,
		circularJobRegistry:      jobRegistry,
		jobAdoptionStopFile:      jobAdoptionStopFile,
		protectedtsProvider:      protectedtsProvider,
		rangeFeedFactory:         rangeFeedFactory,
		sqlStatusServer:          sStatus,
		regionsServer:            sStatus,
		tenantUsageServer:        tenantUsage,
		monitorAndMetrics:        sqlMonitorAndMetrics,
	})
	if err != nil {
		return nil, err
	}
	sStatus.setStmtDiagnosticsRequester(sqlServer.execCfg.StmtDiagnosticsRecorder)
	sStatus.baseStatusServer.sqlServer = sqlServer
	debugServer := debug.NewServer(st, sqlServer.pgServer.HBADebugFn(), sStatus)
	node.InitLogger(sqlServer.execCfg)

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
		admin:                  sAdmin,
		status:                 sStatus,
		authentication:         sAuth,
		tsDB:                   tsDB,
		tsServer:               &sTS,
		raftTransport:          raftTransport,
		stopper:                stopper,
		debug:                  debugServer,
		kvProber:               kvProber,
		replicationReporter:    replicationReporter,
		protectedtsProvider:    protectedtsProvider,
		protectedtsReconciler:  protectedtsReconciler,
		spanConfigSubscriber:   spanConfig.subscriber,
		sqlServer:              sqlServer,
		drainSleepFn:           drainSleepFn,
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

// grpcGatewayServer represents a grpc service with HTTP endpoints through GRPC
// gateway.
type grpcGatewayServer interface {
	RegisterService(g *grpc.Server)
	RegisterGateway(
		ctx context.Context,
		mux *gwruntime.ServeMux,
		conn *grpc.ClientConn,
	) error
}

// ListenError is returned from Start when we fail to start listening on either
// the main Cockroach port or the HTTP port, so that the CLI can instruct the
// user on what might have gone wrong.
type ListenError struct {
	cause error
	Addr  string
}

// Error implements error.
func (l *ListenError) Error() string { return l.cause.Error() }

// Unwrap is because ListenError is a wrapper.
func (l *ListenError) Unwrap() error { return l.cause }

// inspectEngines goes through engines and constructs an initState. The
// initState returned by this method will reflect a zero NodeID if none has
// been assigned yet (i.e. if none of the engines is initialized). See
// commentary on initState for the intended usage of inspectEngines.
func inspectEngines(
	ctx context.Context,
	engines []storage.Engine,
	binaryVersion, binaryMinSupportedVersion roachpb.Version,
) (*initState, error) {
	var clusterID uuid.UUID
	var nodeID roachpb.NodeID
	var initializedEngines, uninitializedEngines []storage.Engine
	var initialSettingsKVs []roachpb.KeyValue

	for _, eng := range engines {
		// Once cached settings are loaded from any engine we can stop.
		if len(initialSettingsKVs) == 0 {
			var err error
			initialSettingsKVs, err = loadCachedSettingsKVs(ctx, eng)
			if err != nil {
				return nil, err
			}
		}

		storeIdent, err := kvserver.ReadStoreIdent(ctx, eng)
		if errors.HasType(err, (*kvserver.NotBootstrappedError)(nil)) {
			uninitializedEngines = append(uninitializedEngines, eng)
			continue
		} else if err != nil {
			return nil, err
		}

		if clusterID != uuid.Nil && clusterID != storeIdent.ClusterID {
			return nil, errors.Errorf("conflicting store ClusterIDs: %s, %s", storeIdent.ClusterID, clusterID)
		}
		clusterID = storeIdent.ClusterID

		if storeIdent.StoreID == 0 || storeIdent.NodeID == 0 || storeIdent.ClusterID == uuid.Nil {
			return nil, errors.Errorf("partially initialized store: %+v", storeIdent)
		}

		if nodeID != 0 && nodeID != storeIdent.NodeID {
			return nil, errors.Errorf("conflicting store NodeIDs: %s, %s", storeIdent.NodeID, nodeID)
		}
		nodeID = storeIdent.NodeID

		initializedEngines = append(initializedEngines, eng)
	}
	clusterVersion, err := kvserver.SynthesizeClusterVersionFromEngines(
		ctx, initializedEngines, binaryVersion, binaryMinSupportedVersion,
	)
	if err != nil {
		return nil, err
	}

	state := &initState{
		clusterID:            clusterID,
		nodeID:               nodeID,
		initializedEngines:   initializedEngines,
		uninitializedEngines: uninitializedEngines,
		clusterVersion:       clusterVersion,
		initialSettingsKVs:   initialSettingsKVs,
	}
	return state, nil
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

// startMonitoringForwardClockJumps starts a background task to monitor forward
// clock jumps based on a cluster setting
func (s *Server) startMonitoringForwardClockJumps(ctx context.Context) error {
	forwardJumpCheckEnabled := make(chan bool, 1)
	s.stopper.AddCloser(stop.CloserFn(func() { close(forwardJumpCheckEnabled) }))

	forwardClockJumpCheckEnabled.SetOnChange(&s.st.SV, func(context.Context) {
		forwardJumpCheckEnabled <- forwardClockJumpCheckEnabled.Get(&s.st.SV)
	})

	if err := s.clock.StartMonitoringForwardClockJumps(
		ctx,
		forwardJumpCheckEnabled,
		time.NewTicker,
		nil, /* tick callback */
	); err != nil {
		return errors.Wrap(err, "monitoring forward clock jumps")
	}

	log.Ops.Info(ctx, "monitoring forward clock jumps based on server.clock.forward_jump_check_enabled")
	return nil
}

// ensureClockMonotonicity sleeps till the wall time reaches
// prevHLCUpperBound. prevHLCUpperBound > 0 implies we need to guarantee HLC
// monotonicity across server restarts. prevHLCUpperBound is the last
// successfully persisted timestamp greater then any wall time used by the
// server.
//
// If prevHLCUpperBound is 0, the function sleeps up to max offset.
func ensureClockMonotonicity(
	ctx context.Context,
	clock *hlc.Clock,
	startTime time.Time,
	prevHLCUpperBound int64,
	sleepUntilFn func(context.Context, hlc.Timestamp) error,
) {
	var sleepUntil int64
	if prevHLCUpperBound != 0 {
		// Sleep until previous HLC upper bound to ensure wall time monotonicity
		sleepUntil = prevHLCUpperBound + 1
	} else {
		// Previous HLC Upper bound is not known
		// We might have to sleep a bit to protect against this node producing non-
		// monotonic timestamps. Before restarting, its clock might have been driven
		// by other nodes' fast clocks, but when we restarted, we lost all this
		// information. For example, a client might have written a value at a
		// timestamp that's in the future of the restarted node's clock, and if we
		// don't do something, the same client's read would not return the written
		// value. So, we wait up to MaxOffset; we couldn't have served timestamps more
		// than MaxOffset in the future (assuming that MaxOffset was not changed, see
		// #9733).
		//
		// As an optimization for tests, we don't sleep if all the stores are brand
		// new. In this case, the node will not serve anything anyway until it
		// synchronizes with other nodes.
		sleepUntil = startTime.UnixNano() + int64(clock.MaxOffset()) + 1
	}

	currentWallTime := clock.Now().WallTime
	delta := time.Duration(sleepUntil - currentWallTime)
	if delta > 0 {
		log.Ops.Infof(
			ctx,
			"Sleeping till wall time %v to catches up to %v to ensure monotonicity. Delta: %v",
			currentWallTime,
			sleepUntil,
			delta,
		)
		_ = sleepUntilFn(ctx, hlc.Timestamp{WallTime: sleepUntil})
	}
}

// periodicallyPersistHLCUpperBound periodically persists an upper bound of
// the HLC's wall time. The interval for persisting is read from
// persistHLCUpperBoundIntervalCh. An interval of 0 disables persisting.
//
// persistHLCUpperBoundFn is used to persist the hlc upper bound, and should
// return an error if the persist fails.
//
// tickerFn is used to create the ticker used for persisting
//
// tickCallback is called whenever a tick is processed
func periodicallyPersistHLCUpperBound(
	clock *hlc.Clock,
	persistHLCUpperBoundIntervalCh chan time.Duration,
	persistHLCUpperBoundFn func(int64) error,
	tickerFn func(d time.Duration) *time.Ticker,
	stopCh <-chan struct{},
	tickCallback func(),
) {
	// Create a ticker which can be used in selects.
	// This ticker is turned on / off based on persistHLCUpperBoundIntervalCh
	ticker := tickerFn(time.Hour)
	ticker.Stop()

	// persistInterval is the interval used for persisting the
	// an upper bound of the HLC
	var persistInterval time.Duration
	var ok bool

	persistHLCUpperBound := func() {
		if err := clock.RefreshHLCUpperBound(
			persistHLCUpperBoundFn,
			int64(persistInterval*3), /* delta to compute upper bound */
		); err != nil {
			log.Ops.Fatalf(
				context.Background(),
				"error persisting HLC upper bound: %v",
				err,
			)
		}
	}

	for {
		select {
		case persistInterval, ok = <-persistHLCUpperBoundIntervalCh:
			ticker.Stop()
			if !ok {
				return
			}

			if persistInterval > 0 {
				ticker = tickerFn(persistInterval)
				persistHLCUpperBound()
				log.Ops.Info(context.Background(), "persisting HLC upper bound is enabled")
			} else {
				if err := clock.ResetHLCUpperBound(persistHLCUpperBoundFn); err != nil {
					log.Ops.Fatalf(
						context.Background(),
						"error resetting hlc upper bound: %v",
						err,
					)
				}
				log.Ops.Info(context.Background(), "persisting HLC upper bound is disabled")
			}

		case <-ticker.C:
			if persistInterval > 0 {
				persistHLCUpperBound()
			}

		case <-stopCh:
			ticker.Stop()
			return
		}

		if tickCallback != nil {
			tickCallback()
		}
	}
}

// startPersistingHLCUpperBound starts a goroutine to persist an upper bound
// to the HLC.
//
// persistHLCUpperBoundFn is used to persist upper bound of the HLC, and should
// return an error if the persist fails
//
// tickerFn is used to create a new ticker
//
// tickCallback is called whenever persistHLCUpperBoundCh or a ticker tick is
// processed
func (s *Server) startPersistingHLCUpperBound(
	ctx context.Context,
	hlcUpperBoundExists bool,
	persistHLCUpperBoundFn func(int64) error,
	tickerFn func(d time.Duration) *time.Ticker,
) error {
	persistHLCUpperBoundIntervalCh := make(chan time.Duration, 1)
	persistHLCUpperBoundInterval.SetOnChange(&s.st.SV, func(context.Context) {
		persistHLCUpperBoundIntervalCh <- persistHLCUpperBoundInterval.Get(&s.st.SV)
	})

	if hlcUpperBoundExists {
		// The feature to persist upper bounds to wall times is enabled.
		// Persist a new upper bound to continue guaranteeing monotonicity
		// Going forward the goroutine launched below will take over persisting
		// the upper bound
		if err := s.clock.RefreshHLCUpperBound(
			persistHLCUpperBoundFn,
			int64(5*time.Second),
		); err != nil {
			return errors.Wrap(err, "refreshing HLC upper bound")
		}
	}

	_ = s.stopper.RunAsyncTask(
		ctx,
		"persist-hlc-upper-bound",
		func(context.Context) {
			periodicallyPersistHLCUpperBound(
				s.clock,
				persistHLCUpperBoundIntervalCh,
				persistHLCUpperBoundFn,
				tickerFn,
				s.stopper.ShouldQuiesce(),
				nil, /* tick callback */
			)
		},
	)
	return nil
}

// getServerEndpointCounter returns a telemetry Counter corresponding to the
// given grpc method.
func getServerEndpointCounter(method string) telemetry.Counter {
	const counterPrefix = "http.grpc-gateway"
	return telemetry.GetCounter(fmt.Sprintf("%s.%s", counterPrefix, method))
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
	connManager := netutil.MakeServer(s.stopper, uiTLSConfig, s)

	// Start a context for the asynchronous network workers.
	workersCtx := s.AnnotateCtx(context.Background())

	// Start the admin UI server. This opens the HTTP listen socket,
	// optionally sets up TLS, and dispatches the server worker for the
	// web UI.
	if err := s.startServeUI(ctx, workersCtx, connManager, uiTLSConfig); err != nil {
		return err
	}

	// Initialize the external storage builders configuration params now that the
	// engines have been created. The object can be used to create ExternalStorage
	// objects hereafter.
	fileTableInternalExecutor := sql.MakeInternalExecutor(ctx, s.PGServer().SQLServer, sql.MemoryMetrics{}, s.st)
	s.externalStorageBuilder.init(s.cfg.ExternalIODirConfig, s.st,
		blobs.NewBlobClientFactory(s.nodeIDContainer.Get(),
			s.nodeDialer, s.st.ExternalIODir), &fileTableInternalExecutor, s.db)

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
	pgL, startRPCServer, err := StartListenRPCAndSQL(ctx, workersCtx, s.cfg.BaseConfig, s.stopper, s.grpc)
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
	gwMux, gwCtx, conn, err := ConfigureGRPCGateway(
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
	s.mux.Handle("/health", gwMux)

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

	// NB: This needs to come after `startListenRPCAndSQL`, which determines
	// what the advertised addr is going to be if nothing is explicitly
	// provided.
	advAddrU := util.NewUnresolvedAddr("tcp", s.cfg.AdvertiseAddr)

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
	if err := s.refreshSettings(state.initialSettingsKVs); err != nil {
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

	// If the server is being restarted, sleep to ensure monotonicity of the HLC
	// clock. This can be skipped on server bootstrap because the server has never
	// been used before.
	var hlcUpperBoundExists bool
	if !initialStart {
		hlcUpperBound, err := kvserver.ReadMaxHLCUpperBound(ctx, s.engines)
		if err != nil {
			return errors.Wrap(err, "reading max HLC upper bound")
		}
		hlcUpperBoundExists = hlcUpperBound > 0

		ensureClockMonotonicity(
			ctx,
			s.clock,
			s.startTime,
			hlcUpperBound,
			s.clock.SleepUntil,
		)
	}

	// Record a walltime that is lower than the lowest hlc timestamp this current
	// instance of the node can use. We do not use startTime because it is lower
	// than the timestamp used to create the bootstrap schema.
	//
	// TODO(tbg): clarify the contract here and move closer to usage if possible.
	orphanedLeasesTimeThresholdNanos := s.clock.Now().WallTime

	onSuccessfulReturnFn()

	// We're going to need to start gossip before we spin up Node below.
	s.gossip.Start(advAddrU, filtered)
	log.Event(ctx, "started gossip")

	// Now that we have a monotonic HLC wrt previous incarnations of the process,
	// init all the replicas. At this point *some* store has been initialized or
	// we're joining an existing cluster for the first time.
	advSQLAddrU := util.NewUnresolvedAddr("tcp", s.cfg.SQLAdvertiseAddr)
	if err := s.node.start(
		ctx,
		advAddrU,
		advSQLAddrU,
		*state,
		initialStart,
		s.cfg.ClusterName,
		s.cfg.NodeAttributes,
		s.cfg.Locality,
		s.cfg.LocalityAddresses,
		s.sqlServer.execCfg.DistSQLPlanner.SetNodeInfo,
	); err != nil {
		return err
	}
	// Stores have been initialized, so Node can now provide Pebble metrics.
	s.storeGrantCoords.SetPebbleMetricsProvider(ctx, s.node)

	log.Event(ctx, "started node")
	if err := s.startPersistingHLCUpperBound(
		ctx,
		hlcUpperBoundExists,
		func(t int64) error { /* function to persist upper bound of HLC to all stores */
			return s.node.SetHLCUpperBound(context.Background(), t)
		},
		time.NewTicker,
	); err != nil {
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
	if err := startSampleEnvironment(s.AnnotateCtx(ctx), sampleEnvironmentCfg{
		st:                   s.ClusterSettings(),
		stopper:              s.stopper,
		minSampleInterval:    base.DefaultMetricsSampleInterval,
		goroutineDumpDirName: s.cfg.GoroutineDumpDirName,
		heapProfileDirName:   s.cfg.HeapProfileDirName,
		runtime:              s.runtime,
		sessionRegistry:      s.status.sessionRegistry,
	}); err != nil {
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

	// Start the protected timestamp subsystem.
	if err := s.protectedtsProvider.Start(ctx, s.stopper); err != nil {
		return err
	}
	if err := s.protectedtsReconciler.Start(ctx, s.stopper); err != nil {
		return err
	}

	if s.cfg.SpanConfigsEnabled && s.spanConfigSubscriber != nil {
		if err := s.spanConfigSubscriber.Start(ctx); err != nil {
			return err
		}
	}
	// Start garbage collecting system events.
	//
	// NB: As written, this falls awkwardly between SQL and KV. KV is used only
	// to make sure this runs only on one node. SQL is used to actually GC. We
	// count it as a KV operation since it grooms cluster-wide data, not
	// something associated to SQL tenants.
	s.startSystemLogsGC(ctx)

	// OIDC Configuration must happen prior to the UI Handler being defined below so that we have
	// the system settings initialized for it to pick up from the oidcAuthenticationServer.
	oidc, err := ConfigureOIDC(
		ctx, s.ClusterSettings(), s.cfg.Locality,
		&s.mux, s.authentication.UserLoginFromSSO, s.cfg.AmbientCtx, s.ClusterID(),
	)
	if err != nil {
		return err
	}
	s.oidc = oidc

	// Serve UI assets.
	//
	// The authentication mux used here is created in "allow anonymous" mode so that the UI
	// assets are served up whether or not there is a session. If there is a session, the mux
	// adds it to the context, and it is templated into index.html so that the UI can show
	// the username of the currently-logged-in user.
	authenticatedUIHandler := newAuthenticationMuxAllowAnonymous(
		s.authentication,
		ui.Handler(ui.Config{
			ExperimentalUseLogin: s.cfg.EnableWebSessionAuthentication,
			LoginEnabled:         s.cfg.RequireWebSession(),
			NodeID:               s.nodeIDContainer,
			OIDC:                 oidc,
			GetUser: func(ctx context.Context) *string {
				if u, ok := ctx.Value(webSessionUserKey{}).(string); ok {
					return &u
				}
				return nil
			},
		}),
	)
	s.mux.Handle("/", authenticatedUIHandler)

	// Register gRPC-gateway endpoints used by the admin UI.
	var authHandler http.Handler = gwMux
	if s.cfg.RequireWebSession() {
		authHandler = newAuthenticationMux(s.authentication, authHandler)
	}

	s.mux.Handle(adminPrefix, authHandler)
	// Exempt the health check endpoint from authentication.
	// This mirrors the handling of /health above.
	s.mux.Handle("/_admin/v1/health", gwMux)
	s.mux.Handle(ts.URLPrefix, authHandler)
	s.mux.Handle(statusPrefix, authHandler)
	// The /login endpoint is, by definition, available pre-authentication.
	s.mux.Handle(loginPath, gwMux)
	s.mux.Handle(logoutPath, authHandler)

	if s.cfg.EnableDemoLoginEndpoint {
		s.mux.Handle(DemoLoginPath, http.HandlerFunc(s.authentication.demoLogin))
	}

	// The /_status/vars endpoint is not authenticated either. Useful for monitoring.
	s.mux.Handle(statusVars, http.HandlerFunc(s.status.handleVars))
	// Register debugging endpoints.
	var debugHandler http.Handler = s.debug
	if s.cfg.RequireWebSession() {
		// TODO(bdarnell): Refactor our authentication stack.
		// authenticationMux guarantees that we have a non-empty user
		// session, but our machinery for verifying the roles of a user
		// lives on adminServer and is tied to GRPC metadata.
		debugHandler = newAuthenticationMux(s.authentication, http.HandlerFunc(
			func(w http.ResponseWriter, req *http.Request) {
				md := forwardAuthenticationMetadata(req.Context(), req)
				authCtx := metadata.NewIncomingContext(req.Context(), md)
				_, err := s.admin.requireAdminUser(authCtx)
				if errors.Is(err, errRequiresAdmin) {
					http.Error(w, "admin privilege required", http.StatusUnauthorized)
					return
				} else if err != nil {
					log.Ops.Infof(authCtx, "web session error: %s", err)
					http.Error(w, "error checking authentication", http.StatusInternalServerError)
					return
				}
				s.debug.ServeHTTP(w, req)
			}))
	}
	s.mux.Handle(debug.Endpoint, debugHandler)

	apiServer := newAPIV2Server(ctx, s)
	s.mux.Handle(apiV2Path, apiServer)

	log.Event(ctx, "added http endpoints")

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

	s.ctSender.Run(ctx, state.nodeID)

	// Attempt to upgrade cluster version now that the sql server has been
	// started. At this point we know that all startupmigrations have successfully
	// been run so it is safe to upgrade to the binary's current version.
	s.startAttemptUpgrade(ctx)

	if err := s.kvProber.Start(ctx, s.stopper); err != nil {
		return errors.Wrapf(err, "failed to start KV prober")
	}

	log.Event(ctx, "server initialized")

	// Begin recording time series data collected by the status monitor.
	// This will perform the first write synchronously, which is now
	// acceptable.
	s.tsDB.PollSource(
		s.cfg.AmbientCtx, s.recorder, base.DefaultMetricsSampleInterval, ts.Resolution10s, s.stopper,
	)

	return maybeImportTS(ctx, s)
}

// ConfigureGRPCGateway initializes services necessary for running the
// GRPC Gateway services proxied against the server at `grpcSrv`.
//
// The connection between the reverse proxy provided by grpc-gateway
// and our grpc server uses a loopback-based listener to create
// connections between the two.
//
// The function returns 3 arguments that are necessary to call
// `RegisterGateway` which generated for each of your gRPC services
// by grpc-gateway.
func ConfigureGRPCGateway(
	ctx, workersCtx context.Context,
	ambientCtx log.AmbientContext,
	rpcContext *rpc.Context,
	stopper *stop.Stopper,
	grpcSrv *grpcServer,
	GRPCAddr string,
) (*gwruntime.ServeMux, context.Context, *grpc.ClientConn, error) {
	jsonpb := &protoutil.JSONPb{
		EnumsAsInts:  true,
		EmitDefaults: true,
		Indent:       "  ",
	}
	protopb := new(protoutil.ProtoPb)
	gwMux := gwruntime.NewServeMux(
		gwruntime.WithMarshalerOption(gwruntime.MIMEWildcard, jsonpb),
		gwruntime.WithMarshalerOption(httputil.JSONContentType, jsonpb),
		gwruntime.WithMarshalerOption(httputil.AltJSONContentType, jsonpb),
		gwruntime.WithMarshalerOption(httputil.ProtoContentType, protopb),
		gwruntime.WithMarshalerOption(httputil.AltProtoContentType, protopb),
		gwruntime.WithOutgoingHeaderMatcher(authenticationHeaderMatcher),
		gwruntime.WithMetadata(forwardAuthenticationMetadata),
	)
	gwCtx, gwCancel := context.WithCancel(ambientCtx.AnnotateCtx(context.Background()))
	stopper.AddCloser(stop.CloserFn(gwCancel))

	// loopback handles the HTTP <-> RPC loopback connection.
	loopback := newLoopbackListener(workersCtx, stopper)

	waitQuiesce := func(context.Context) {
		<-stopper.ShouldQuiesce()
		_ = loopback.Close()
	}
	if err := stopper.RunAsyncTask(workersCtx, "gw-quiesce", waitQuiesce); err != nil {
		waitQuiesce(workersCtx)
	}

	_ = stopper.RunAsyncTask(workersCtx, "serve-loopback", func(context.Context) {
		netutil.FatalIfUnexpected(grpcSrv.Serve(loopback))
	})

	// Eschew `(*rpc.Context).GRPCDial` to avoid unnecessary moving parts on the
	// uniquely in-process connection.
	dialOpts, err := rpcContext.GRPCDialOptions()
	if err != nil {
		return nil, nil, nil, err
	}

	callCountInterceptor := func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		telemetry.Inc(getServerEndpointCounter(method))
		return invoker(ctx, method, req, reply, cc, opts...)
	}
	conn, err := grpc.DialContext(ctx, GRPCAddr, append(append(
		dialOpts,
		grpc.WithUnaryInterceptor(callCountInterceptor)),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return loopback.Connect(ctx)
		}),
	)...)
	if err != nil {
		return nil, nil, nil, err
	}
	{
		waitQuiesce := func(workersCtx context.Context) {
			<-stopper.ShouldQuiesce()
			// NB: we can't do this as a Closer because (*Server).ServeWith is
			// running in a worker and usually sits on accept() which unblocks
			// only when the listener closes. In other words, the listener needs
			// to close when quiescing starts to allow that worker to shut down.
			err := conn.Close() // nolint:grpcconnclose
			if err != nil {
				log.Ops.Fatalf(workersCtx, "%v", err)
			}
		}
		if err := stopper.RunAsyncTask(workersCtx, "wait-quiesce", waitQuiesce); err != nil {
			waitQuiesce(workersCtx)
		}
	}
	return gwMux, gwCtx, conn, nil
}

func maybeImportTS(ctx context.Context, s *Server) error {
	knobs, _ := s.cfg.TestingKnobs.Server.(*TestingKnobs)
	if knobs == nil {
		return nil
	}
	tsImport := knobs.ImportTimeseriesFile
	if tsImport == "" {
		return nil
	}

	// In practice we only allow populating time series in `start-single-node` due
	// to complexities detailed below. Additionally, we allow it only on a fresh
	// single-node single-store cluster and we also guard against join flags even
	// though there shouldn't be any.
	if !s.InitialStart() || len(s.cfg.JoinList) > 0 || len(s.cfg.Stores.Specs) != 1 {
		return errors.New("cannot import timeseries into an existing cluster or a multi-{store,node} cluster")
	}

	// Also do a best effort at disabling the timeseries of the local node to cause
	// confusion.
	ts.TimeseriesStorageEnabled.Override(ctx, &s.ClusterSettings().SV, false)

	// Suppress writing of node statuses for the local node (n1). If it wrote one,
	// and the imported data also contains n1 but with a different set of stores,
	// we'd effectively clobber the timeseries display for n1 (which relies on the
	// store statuses to map the store timeseries to the node under which they
	// fall). An alternative to this is setting a FirstStoreID and FirstNodeID that
	// is not in use in the data set to import.
	s.node.suppressNodeStatus.Set(true)

	f, err := os.Open(tsImport)
	if err != nil {
		return err
	}
	defer f.Close()

	b := &kv.Batch{}
	var n int
	maybeFlush := func(force bool) error {
		if n == 0 {
			return nil
		}
		if n < 100 && !force {
			return nil
		}
		err := s.db.Run(ctx, b)
		if err != nil {
			return err
		}
		log.Infof(ctx, "imported %d ts pairs\n", n)
		*b, n = kv.Batch{}, 0
		return nil
	}

	nodeIDs := map[string]struct{}{}
	storeIDs := map[string]struct{}{}
	dec := gob.NewDecoder(f)
	for {
		var v roachpb.KeyValue
		err := dec.Decode(&v)
		if err != nil {
			if err == io.EOF {
				if err := maybeFlush(true /* force */); err != nil {
					return err
				}
				break
			}
			return err
		}

		name, source, _, _, err := ts.DecodeDataKey(v.Key)
		if err != nil {
			return err
		}
		if strings.HasPrefix(name, "cr.node.") {
			nodeIDs[source] = struct{}{}
		} else if strings.HasPrefix(name, "cr.store.") {
			storeIDs[source] = struct{}{}
		} else {
			return errors.Errorf("unknown metric %s", name)
		}

		p := roachpb.NewPut(v.Key, v.Value)
		p.(*roachpb.PutRequest).Inline = true
		b.AddRawRequest(p)
		n++
		if err := maybeFlush(false /* force */); err != nil {
			return err
		}
	}

	if knobs.ImportTimeseriesMappingFile == "" {
		return errors.Errorf("need to specify COCKROACH_DEBUG_TS_IMPORT_MAPPING_FILE; it should point at " +
			"a YAML file that maps StoreID to NodeID. For example, if s1 is on n1 and s2 is on n5:\n\n1: 1\n2:5")
	}
	mapBytes, err := ioutil.ReadFile(knobs.ImportTimeseriesMappingFile)
	if err != nil {
		return err
	}
	storeToNode := map[roachpb.StoreID]roachpb.NodeID{}
	if err := yaml.NewDecoder(bytes.NewReader(mapBytes)).Decode(&storeToNode); err != nil {
		return err
	}

	fakeStatuses, err := makeFakeNodeStatuses(storeToNode)
	if err != nil {
		return err
	}
	if err := checkFakeStatuses(fakeStatuses, storeIDs); err != nil {
		return errors.Wrapf(err, "please provide an updated mapping file %s", knobs.ImportTimeseriesMappingFile)
	}

	// All checks passed, write the statuses.
	for _, status := range fakeStatuses {
		key := keys.NodeStatusKey(status.Desc.NodeID)
		if err := s.db.PutInline(ctx, key, &status); err != nil {
			return err
		}
	}

	return nil
}

func makeFakeNodeStatuses(
	storeToNode map[roachpb.StoreID]roachpb.NodeID,
) ([]statuspb.NodeStatus, error) {
	var sl []statuspb.NodeStatus
	nodeToStore := map[roachpb.NodeID][]roachpb.StoreID{}
	for sid, nid := range storeToNode {
		nodeToStore[nid] = append(nodeToStore[nid], sid)
	}

	for nodeID, storeIDs := range nodeToStore {
		sort.Slice(storeIDs, func(i, j int) bool {
			return storeIDs[i] < storeIDs[j]
		})
		nodeStatus := statuspb.NodeStatus{
			Desc: roachpb.NodeDescriptor{
				NodeID: nodeID,
			},
		}
		for _, storeID := range storeIDs {
			nodeStatus.StoreStatuses = append(nodeStatus.StoreStatuses, statuspb.StoreStatus{Desc: roachpb.StoreDescriptor{
				Node:    nodeStatus.Desc, // don't want cycles here
				StoreID: storeID,
			}})
		}

		sl = append(sl, nodeStatus)
	}
	sort.Slice(sl, func(i, j int) bool {
		return sl[i].Desc.NodeID < sl[j].Desc.NodeID
	})
	return sl, nil
}

func checkFakeStatuses(fakeStatuses []statuspb.NodeStatus, storeIDs map[string]struct{}) error {
	for _, status := range fakeStatuses {
		for _, ss := range status.StoreStatuses {
			storeID := ss.Desc.StoreID
			strID := fmt.Sprint(storeID)
			if _, ok := storeIDs[strID]; !ok {
				// This is likely an mistake and where it isn't (for example since store
				// is long gone and hasn't supplied metrics in a long time) the user can
				// react by removing the assignment from the yaml file and trying again.
				return errors.Errorf(
					"s%d supplied in input mapping, but no timeseries found for it",
					ss.Desc.StoreID,
				)
			}

			delete(storeIDs, strID)
		}
	}
	if len(storeIDs) > 0 {
		return errors.Errorf(
			"need to map the remaining stores %v to nodes", storeIDs)
	}
	return nil
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

// StartListenRPCAndSQL starts the RPC and SQL listeners.
// It returns the SQL listener, which can be used
// to start the SQL server when initialization has completed.
// It also returns a function that starts the RPC server,
// when the cluster is known to have bootstrapped or
// when waiting for init().
func StartListenRPCAndSQL(
	ctx, workersCtx context.Context, cfg BaseConfig, stopper *stop.Stopper, grpc *grpcServer,
) (sqlListener net.Listener, startRPCServer func(ctx context.Context), err error) {
	rpcChanName := "rpc/sql"
	if cfg.SplitListenSQL {
		rpcChanName = "rpc"
	}
	var ln net.Listener
	if k := cfg.TestingKnobs.Server; k != nil {
		knobs := k.(*TestingKnobs)
		ln = knobs.RPCListener
	}
	if ln == nil {
		var err error
		ln, err = ListenAndUpdateAddrs(ctx, &cfg.Addr, &cfg.AdvertiseAddr, rpcChanName)
		if err != nil {
			return nil, nil, err
		}
		log.Eventf(ctx, "listening on port %s", cfg.Addr)
	}

	var pgL net.Listener
	if cfg.SplitListenSQL {
		pgL, err = ListenAndUpdateAddrs(ctx, &cfg.SQLAddr, &cfg.SQLAdvertiseAddr, "sql")
		if err != nil {
			return nil, nil, err
		}
		// The SQL listener shutdown worker, which closes everything under
		// the SQL port when the stopper indicates we are shutting down.
		waitQuiesce := func(ctx context.Context) {
			<-stopper.ShouldQuiesce()
			// NB: we can't do this as a Closer because (*Server).ServeWith is
			// running in a worker and usually sits on accept() which unblocks
			// only when the listener closes. In other words, the listener needs
			// to close when quiescing starts to allow that worker to shut down.
			if err := pgL.Close(); err != nil {
				log.Ops.Fatalf(ctx, "%v", err)
			}
		}
		if err := stopper.RunAsyncTask(workersCtx, "wait-quiesce", waitQuiesce); err != nil {
			waitQuiesce(workersCtx)
			return nil, nil, err
		}
		log.Eventf(ctx, "listening on sql port %s", cfg.SQLAddr)
	}

	// serveOnMux is used to ensure that the mux gets listened on eventually,
	// either via the returned startRPCServer() or upon stopping.
	var serveOnMux sync.Once

	m := cmux.New(ln)

	if !cfg.SplitListenSQL {
		// If the pg port is split, it will be opened above. Otherwise,
		// we make it hang off the RPC listener via cmux here.
		pgL = m.Match(func(r io.Reader) bool {
			return pgwire.Match(r)
		})
		// Also if the pg port is not split, the actual listen
		// and advertise addresses for SQL become equal to that
		// of RPC, regardless of what was configured.
		cfg.SQLAddr = cfg.Addr
		cfg.SQLAdvertiseAddr = cfg.AdvertiseAddr
	}

	anyL := m.Match(cmux.Any())
	if serverTestKnobs, ok := cfg.TestingKnobs.Server.(*TestingKnobs); ok {
		if serverTestKnobs.ContextTestingKnobs.ArtificialLatencyMap != nil {
			anyL = rpc.NewDelayingListener(anyL)
		}
	}

	// The remainder shutdown worker.
	waitForQuiesce := func(context.Context) {
		<-stopper.ShouldQuiesce()
		// TODO(bdarnell): Do we need to also close the other listeners?
		netutil.FatalIfUnexpected(anyL.Close())
	}
	stopper.AddCloser(stop.CloserFn(func() {
		grpc.Stop()
		serveOnMux.Do(func() {
			// The cmux matches don't shut down properly unless serve is called on the
			// cmux at some point. Use serveOnMux to ensure it's called during shutdown
			// if we wouldn't otherwise reach the point where we start serving on it.
			netutil.FatalIfUnexpected(m.Serve())
		})
	}))
	if err := stopper.RunAsyncTask(
		workersCtx, "grpc-quiesce", waitForQuiesce,
	); err != nil {
		return nil, nil, err
	}

	// startRPCServer starts the RPC server. We do not do this
	// immediately because we want the cluster to be ready (or ready to
	// initialize) before we accept RPC requests. The caller
	// (Server.Start) will call this at the right moment.
	startRPCServer = func(ctx context.Context) {
		// Serve the gRPC endpoint.
		_ = stopper.RunAsyncTask(workersCtx, "serve-grpc", func(context.Context) {
			netutil.FatalIfUnexpected(grpc.Serve(anyL))
		})

		_ = stopper.RunAsyncTask(ctx, "serve-mux", func(context.Context) {
			serveOnMux.Do(func() {
				netutil.FatalIfUnexpected(m.Serve())
			})
		})
	}

	return pgL, startRPCServer, nil
}

func (s *Server) startServeUI(
	ctx, workersCtx context.Context, connManager netutil.Server, uiTLSConfig *tls.Config,
) error {
	httpLn, err := ListenAndUpdateAddrs(ctx, &s.cfg.HTTPAddr, &s.cfg.HTTPAdvertiseAddr, "http")
	if err != nil {
		return err
	}
	log.Eventf(ctx, "listening on http port %s", s.cfg.HTTPAddr)

	// The HTTP listener shutdown worker, which closes everything under
	// the HTTP port when the stopper indicates we are shutting down.
	waitQuiesce := func(ctx context.Context) {
		// NB: we can't do this as a Closer because (*Server).ServeWith is
		// running in a worker and usually sits on accept() which unblocks
		// only when the listener closes. In other words, the listener needs
		// to close when quiescing starts to allow that worker to shut down.
		<-s.stopper.ShouldQuiesce()
		if err := httpLn.Close(); err != nil {
			log.Ops.Fatalf(ctx, "%v", err)
		}
	}
	if err := s.stopper.RunAsyncTask(workersCtx, "wait-quiesce", waitQuiesce); err != nil {
		waitQuiesce(workersCtx)
		return err
	}

	if uiTLSConfig != nil {
		httpMux := cmux.New(httpLn)
		clearL := httpMux.Match(cmux.HTTP1())
		tlsL := httpMux.Match(cmux.Any())

		// Dispatch incoming requests to either clearL or tlsL.
		if err := s.stopper.RunAsyncTask(workersCtx, "serve-ui", func(context.Context) {
			netutil.FatalIfUnexpected(httpMux.Serve())
		}); err != nil {
			return err
		}

		// Serve the plain HTTP (non-TLS) connection over clearL.
		// This produces a HTTP redirect to the `https` URL for the path /,
		// handles the request normally (via s.ServeHTTP) for the path /health,
		// and produces 404 for anything else.
		if err := s.stopper.RunAsyncTask(workersCtx, "serve-health", func(context.Context) {
			mux := http.NewServeMux()
			mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, "https://"+r.Host+r.RequestURI, http.StatusTemporaryRedirect)
			})
			mux.Handle("/health", s)

			plainRedirectServer := netutil.MakeServer(s.stopper, uiTLSConfig, mux)

			netutil.FatalIfUnexpected(plainRedirectServer.Serve(clearL))
		}); err != nil {
			return err
		}

		httpLn = tls.NewListener(tlsL, uiTLSConfig)
	}

	// Serve the HTTP endpoint. This will be the original httpLn
	// listening on --http-addr without TLS if uiTLSConfig was
	// nil, or overridden above if uiTLSConfig was not nil to come from
	// the TLS negotiation over the HTTP port.
	return s.stopper.RunAsyncTask(workersCtx, "server-http", func(context.Context) {
		netutil.FatalIfUnexpected(connManager.Serve(httpLn))
	})
}

// TODO(tbg): move into server_sql.go.
func (s *SQLServer) startServeSQL(
	ctx context.Context,
	stopper *stop.Stopper,
	connManager netutil.Server,
	pgL net.Listener,
	socketFile string,
) error {
	log.Ops.Info(ctx, "serving sql connections")
	// Start servicing SQL connections.

	pgCtx := s.pgServer.AmbientCtx.AnnotateCtx(context.Background())
	tcpKeepAlive := tcpKeepAliveManager{
		tcpKeepAlive: envutil.EnvOrDefaultDuration("COCKROACH_SQL_TCP_KEEP_ALIVE", time.Minute),
	}

	_ = stopper.RunAsyncTaskEx(pgCtx,
		stop.TaskOpts{TaskName: "pgwire-listener", SpanOpt: stop.SterileRootSpan},
		func(ctx context.Context) {
			err := connManager.ServeWith(ctx, stopper, pgL, func(ctx context.Context, conn net.Conn) {
				connCtx := s.pgServer.AnnotateCtxForIncomingConn(ctx, conn)
				tcpKeepAlive.configure(connCtx, conn)

				if err := s.pgServer.ServeConn(connCtx, conn, pgwire.SocketTCP); err != nil {
					log.Ops.Errorf(connCtx, "serving SQL client conn: %v", err)
				}
			})
			netutil.FatalIfUnexpected(err)
		})

	// If a unix socket was requested, start serving there too.
	if len(socketFile) != 0 {
		log.Ops.Infof(ctx, "starting postgres server at unix:%s", socketFile)

		// Unix socket enabled: postgres protocol only.
		unixLn, err := net.Listen("unix", socketFile)
		if err != nil {
			return err
		}

		waitQuiesce := func(ctx context.Context) {
			<-stopper.ShouldQuiesce()
			// NB: we can't do this as a Closer because (*Server).ServeWith is
			// running in a worker and usually sits on accept() which unblocks
			// only when the listener closes. In other words, the listener needs
			// to close when quiescing starts to allow that worker to shut down.
			if err := unixLn.Close(); err != nil {
				log.Ops.Fatalf(ctx, "%v", err)
			}
		}
		if err := stopper.RunAsyncTaskEx(ctx,
			stop.TaskOpts{TaskName: "unix-ln-close", SpanOpt: stop.SterileRootSpan},
			func(ctx context.Context) {
				waitQuiesce(ctx)
			}); err != nil {
			waitQuiesce(ctx)
			return err
		}

		if err := stopper.RunAsyncTaskEx(pgCtx,
			stop.TaskOpts{TaskName: "unix-listener", SpanOpt: stop.SterileRootSpan},
			func(ctx context.Context) {
				err := connManager.ServeWith(ctx, stopper, unixLn, func(ctx context.Context, conn net.Conn) {
					connCtx := s.pgServer.AnnotateCtxForIncomingConn(ctx, conn)
					if err := s.pgServer.ServeConn(connCtx, conn, pgwire.SocketUnix); err != nil {
						log.Ops.Errorf(connCtx, "%v", err)
					}
				})
				netutil.FatalIfUnexpected(err)
			}); err != nil {
			return err
		}
	}

	s.acceptingClients.Set(true)

	return nil
}

// Decommission idempotently sets the decommissioning flag for specified nodes.
func (s *Server) Decommission(
	ctx context.Context, targetStatus livenesspb.MembershipStatus, nodeIDs []roachpb.NodeID,
) error {
	// If we're asked to decommission ourself we may lose access to cluster RPC,
	// so we decommission ourself last. We copy the slice to avoid mutating the
	// input slice.
	if targetStatus == livenesspb.MembershipStatus_DECOMMISSIONED {
		orderedNodeIDs := make([]roachpb.NodeID, len(nodeIDs))
		copy(orderedNodeIDs, nodeIDs)
		sort.SliceStable(orderedNodeIDs, func(i, j int) bool {
			return orderedNodeIDs[j] == s.NodeID()
		})
		nodeIDs = orderedNodeIDs
	}

	var event eventpb.EventPayload
	var nodeDetails *eventpb.CommonNodeDecommissionDetails
	if targetStatus.Decommissioning() {
		ev := &eventpb.NodeDecommissioning{}
		nodeDetails = &ev.CommonNodeDecommissionDetails
		event = ev
	} else if targetStatus.Decommissioned() {
		ev := &eventpb.NodeDecommissioned{}
		nodeDetails = &ev.CommonNodeDecommissionDetails
		event = ev
	} else if targetStatus.Active() {
		ev := &eventpb.NodeRecommissioned{}
		nodeDetails = &ev.CommonNodeDecommissionDetails
		event = ev
	} else {
		panic("unexpected target membership status")
	}
	event.CommonDetails().Timestamp = timeutil.Now().UnixNano()
	nodeDetails.RequestingNodeID = int32(s.NodeID())

	for _, nodeID := range nodeIDs {
		statusChanged, err := s.nodeLiveness.SetMembershipStatus(ctx, nodeID, targetStatus)
		if err != nil {
			if errors.Is(err, liveness.ErrMissingRecord) {
				return grpcstatus.Error(codes.NotFound, liveness.ErrMissingRecord.Error())
			}
			return err
		}
		if statusChanged {
			nodeDetails.TargetNodeID = int32(nodeID)
			// Ensure an entry is produced in the external log in all cases.
			log.StructuredEvent(ctx, event)

			// If we die right now or if this transaction fails to commit, the
			// membership event will not be recorded to the event log. While we
			// could insert the event record in the same transaction as the liveness
			// update, this would force a 2PC and potentially leave write intents in
			// the node liveness range. Better to make the event logging best effort
			// than to slow down future node liveness transactions.
			if err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				return sql.InsertEventRecord(
					ctx,
					s.sqlServer.execCfg.InternalExecutor,
					txn,
					int32(s.NodeID()), /* reporting ID: the node where the event is logged */
					sql.LogToSystemTable|sql.LogToDevChannelIfVerbose, /* we already call log.StructuredEvent above */
					int32(nodeID), /* target ID: the node that we wee a membership change for */
					event,
				)
			}); err != nil {
				log.Ops.Errorf(ctx, "unable to record event: %+v: %+v", event, err)
			}
		}

		// Similarly to the log event above, we may not be able to clean up the
		// status entry if we crash or fail -- the status entry is inline, and
		// thus cannot be transactional. However, since decommissioning is
		// idempotent, we can attempt to remove the key regardless of whether
		// the status changed, such that a stale key can be removed by
		// decommissioning the node again.
		if targetStatus.Decommissioned() {
			if err := s.db.PutInline(ctx, keys.NodeStatusKey(nodeID), nil); err != nil {
				log.Errorf(ctx, "unable to clean up node status data for node %d: %s", nodeID, err)
			}
		}
	}
	return nil
}

type sampleEnvironmentCfg struct {
	st                   *cluster.Settings
	stopper              *stop.Stopper
	minSampleInterval    time.Duration
	goroutineDumpDirName string
	heapProfileDirName   string
	runtime              *status.RuntimeStatSampler
	sessionRegistry      *sql.SessionRegistry
}

// startSampleEnvironment starts a periodic loop that samples the environment and,
// when appropriate, creates goroutine and/or heap dumps.
func startSampleEnvironment(ctx context.Context, cfg sampleEnvironmentCfg) error {
	// Immediately record summaries once on server startup.

	// Initialize a goroutine dumper if we have an output directory
	// specified.
	var goroutineDumper *goroutinedumper.GoroutineDumper
	if cfg.goroutineDumpDirName != "" {
		hasValidDumpDir := true
		if err := os.MkdirAll(cfg.goroutineDumpDirName, 0755); err != nil {
			// This is possible when running with only in-memory stores;
			// in that case the start-up code sets the output directory
			// to the current directory (.). If running the process
			// from a directory which is not writable, we won't
			// be able to create a sub-directory here.
			log.Warningf(ctx, "cannot create goroutine dump dir -- goroutine dumps will be disabled: %v", err)
			hasValidDumpDir = false
		}
		if hasValidDumpDir {
			var err error
			goroutineDumper, err = goroutinedumper.NewGoroutineDumper(ctx, cfg.goroutineDumpDirName, cfg.st)
			if err != nil {
				return errors.Wrap(err, "starting goroutine dumper worker")
			}
		}
	}

	// Initialize a heap profiler if we have an output directory
	// specified.
	var heapProfiler *heapprofiler.HeapProfiler
	var nonGoAllocProfiler *heapprofiler.NonGoAllocProfiler
	var statsProfiler *heapprofiler.StatsProfiler
	var queryProfiler *heapprofiler.ActiveQueryProfiler
	if cfg.heapProfileDirName != "" {
		hasValidDumpDir := true
		if err := os.MkdirAll(cfg.heapProfileDirName, 0755); err != nil {
			// This is possible when running with only in-memory stores;
			// in that case the start-up code sets the output directory
			// to the current directory (.). If wrunning the process
			// from a directory which is not writable, we won't
			// be able to create a sub-directory here.
			log.Warningf(ctx, "cannot create memory dump dir -- memory profile dumps will be disabled: %v", err)
			hasValidDumpDir = false
		}

		if hasValidDumpDir {
			var err error
			heapProfiler, err = heapprofiler.NewHeapProfiler(ctx, cfg.heapProfileDirName, cfg.st)
			if err != nil {
				return errors.Wrap(err, "starting heap profiler worker")
			}
			nonGoAllocProfiler, err = heapprofiler.NewNonGoAllocProfiler(ctx, cfg.heapProfileDirName, cfg.st)
			if err != nil {
				return errors.Wrap(err, "starting non-go alloc profiler worker")
			}
			statsProfiler, err = heapprofiler.NewStatsProfiler(ctx, cfg.heapProfileDirName, cfg.st)
			if err != nil {
				return errors.Wrap(err, "starting memory stats collector worker")
			}
			queryProfiler, err = heapprofiler.NewActiveQueryProfiler(ctx, cfg.heapProfileDirName, cfg.st)
			if err != nil {
				log.Warningf(ctx, "failed to start query profiler worker: %v", err)
			}
		}
	}

	return cfg.stopper.RunAsyncTaskEx(ctx,
		stop.TaskOpts{TaskName: "mem-logger", SpanOpt: stop.SterileRootSpan},
		func(ctx context.Context) {
			var goMemStats atomic.Value // *status.GoMemStats
			goMemStats.Store(&status.GoMemStats{})
			var collectingMemStats int32 // atomic, 1 when stats call is ongoing

			timer := timeutil.NewTimer()
			defer timer.Stop()
			timer.Reset(cfg.minSampleInterval)

			for {
				select {
				case <-cfg.stopper.ShouldQuiesce():
					return
				case <-timer.C:
					timer.Read = true
					timer.Reset(cfg.minSampleInterval)

					// We read the heap stats on another goroutine and give up after 1s.
					// This is necessary because as of Go 1.12, runtime.ReadMemStats()
					// "stops the world" and that requires first waiting for any current GC
					// run to finish. With a large heap and under extreme conditions, a
					// single GC run may take longer than the default sampling period of
					// 10s. Under normal operations and with more recent versions of Go,
					// this hasn't been observed to be a problem.
					statsCollected := make(chan struct{})
					if atomic.CompareAndSwapInt32(&collectingMemStats, 0, 1) {
						if err := cfg.stopper.RunAsyncTaskEx(ctx,
							stop.TaskOpts{TaskName: "get-mem-stats"},
							func(ctx context.Context) {
								var ms status.GoMemStats
								runtime.ReadMemStats(&ms.MemStats)
								ms.Collected = timeutil.Now()
								log.VEventf(ctx, 2, "memstats: %+v", ms)

								goMemStats.Store(&ms)
								atomic.StoreInt32(&collectingMemStats, 0)
								close(statsCollected)
							}); err != nil {
							close(statsCollected)
						}
					}

					select {
					case <-statsCollected:
						// Good; we managed to read the Go memory stats quickly enough.
					case <-time.After(time.Second):
					}

					curStats := goMemStats.Load().(*status.GoMemStats)
					cgoStats := status.GetCGoMemStats(ctx)
					cfg.runtime.SampleEnvironment(ctx, curStats, cgoStats)

					if goroutineDumper != nil {
						goroutineDumper.MaybeDump(ctx, cfg.st, cfg.runtime.Goroutines.Value())
					}
					if heapProfiler != nil {
						heapProfiler.MaybeTakeProfile(ctx, cfg.runtime.GoAllocBytes.Value())
						nonGoAllocProfiler.MaybeTakeProfile(ctx, cfg.runtime.CgoTotalBytes.Value())
						statsProfiler.MaybeTakeProfile(ctx, cfg.runtime.RSSBytes.Value(), curStats, cgoStats)
					}
					if queryProfiler != nil {
						queryProfiler.MaybeDumpQueries(ctx, cfg.sessionRegistry, cfg.st)
					}
				}
			}
		})
}

// Stop stops the server.
func (s *Server) Stop() {
	s.stopper.Stop(context.Background())
}

// ServeHTTP is necessary to implement the http.Handler interface.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Disable caching of responses.
	w.Header().Set("Cache-control", "no-cache")

	ae := r.Header.Get(httputil.AcceptEncodingHeader)
	switch {
	case strings.Contains(ae, httputil.GzipEncoding):
		w.Header().Set(httputil.ContentEncodingHeader, httputil.GzipEncoding)
		gzw := newGzipResponseWriter(w)
		defer func() {
			// Certain requests must not have a body, yet closing the gzip writer will
			// attempt to write the gzip header. Avoid logging a warning in this case.
			// This is notably triggered by:
			//
			// curl -H 'Accept-Encoding: gzip' \
			// 	    -H 'If-Modified-Since: Thu, 29 Mar 2018 22:36:32 GMT' \
			//      -v http://localhost:8080/favicon.ico > /dev/null
			//
			// which results in a 304 Not Modified.
			if err := gzw.Close(); err != nil && !errors.Is(err, http.ErrBodyNotAllowed) {
				ctx := s.AnnotateCtx(r.Context())
				log.Ops.Warningf(ctx, "error closing gzip response writer: %v", err)
			}
		}()
		w = gzw
	}

	// This is our base handler.
	// Intercept all panics, log them, and return an internal server error as a response.
	defer func() {
		if p := recover(); p != nil {
			// Note: use of a background context here so we can log even with the absence of a client.
			// Assumes appropriate timeouts are used.
			logcrash.ReportPanic(context.Background(), &s.st.SV, p, 1 /* depth */)
			http.Error(w, errAPIInternalErrorString, http.StatusInternalServerError)
		}
	}()

	s.mux.ServeHTTP(w, r)
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
	s.updates.PeriodicallyCheckForUpdates(ctx)
	s.sqlServer.StartDiagnostics(ctx)
}

// TODO(benesch): Use https://github.com/NYTimes/gziphandler instead.
// gzipResponseWriter reinvents the wheel and is not as robust.
type gzipResponseWriter struct {
	gz gzip.Writer
	http.ResponseWriter
}

func newGzipResponseWriter(rw http.ResponseWriter) *gzipResponseWriter {
	var w *gzipResponseWriter
	if wI := gzipResponseWriterPool.Get(); wI == nil {
		w = new(gzipResponseWriter)
	} else {
		w = wI.(*gzipResponseWriter)
	}
	w.Reset(rw)
	return w
}

func (w *gzipResponseWriter) Reset(rw http.ResponseWriter) {
	w.gz.Reset(rw)
	w.ResponseWriter = rw
}

func (w *gzipResponseWriter) Write(b []byte) (int, error) {
	// The underlying http.ResponseWriter can't sniff gzipped data properly, so we
	// do our own sniffing on the uncompressed data.
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", http.DetectContentType(b))
	}
	return w.gz.Write(b)
}

// Flush implements http.Flusher as required by grpc-gateway for clients
// which access streaming endpoints (as exercised by the acceptance tests
// at time of writing).
func (w *gzipResponseWriter) Flush() {
	// If Flush returns an error, we'll see it on the next call to Write or
	// Close as well, so we can ignore it here.
	if err := w.gz.Flush(); err == nil {
		// Flush the wrapped ResponseWriter as well, if possible.
		if f, ok := w.ResponseWriter.(http.Flusher); ok {
			f.Flush()
		}
	}
}

// Close implements the io.Closer interface. It is not safe to use the
// writer after calling Close.
func (w *gzipResponseWriter) Close() error {
	err := w.gz.Close()
	w.Reset(nil) // release ResponseWriter reference.
	gzipResponseWriterPool.Put(w)
	return err
}

func init() {
	tracing.RegisterTagRemapping("n", "node")
}

// configure attempts to set TCP keep-alive on
// connection. Does not fail on errors.
func (k *tcpKeepAliveManager) configure(ctx context.Context, conn net.Conn) {
	if k.tcpKeepAlive == 0 {
		return
	}

	muxConn, ok := conn.(*cmux.MuxConn)
	if !ok {
		return
	}
	tcpConn, ok := muxConn.Conn.(*net.TCPConn)
	if !ok {
		return
	}

	// Only log success/failure once.
	doLog := atomic.CompareAndSwapInt32(&k.loggedKeepAliveStatus, 0, 1)
	if err := tcpConn.SetKeepAlive(true); err != nil {
		if doLog {
			log.Ops.Warningf(ctx, "failed to enable TCP keep-alive for pgwire: %v", err)
		}
		return

	}
	if err := tcpConn.SetKeepAlivePeriod(k.tcpKeepAlive); err != nil {
		if doLog {
			log.Ops.Warningf(ctx, "failed to set TCP keep-alive duration for pgwire: %v", err)
		}
		return
	}

	if doLog {
		log.VEventf(ctx, 2, "setting TCP keep-alive to %s for pgwire", k.tcpKeepAlive)
	}
}

type tcpKeepAliveManager struct {
	// The keepalive duration.
	tcpKeepAlive time.Duration
	// loggedKeepAliveStatus ensures that errors about setting the TCP
	// keepalive status are only reported once.
	loggedKeepAliveStatus int32
}

// ListenAndUpdateAddrs starts a TCP listener on the specified address
// then updates the address and advertised address fields based on the
// actual interface address resolved by the OS during the Listen()
// call.
func ListenAndUpdateAddrs(
	ctx context.Context, addr, advertiseAddr *string, connName string,
) (net.Listener, error) {
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return nil, &ListenError{
			cause: err,
			Addr:  *addr,
		}
	}
	if err := base.UpdateAddrs(ctx, addr, advertiseAddr, ln.Addr()); err != nil {
		return nil, errors.Wrapf(err, "internal error: cannot parse %s listen address", connName)
	}
	return ln, nil
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

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
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"path/filepath"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	circuit "github.com/cockroachdb/circuitbreaker"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvtenant"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptprovider"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/contention"
	"github.com/cockroachdb/cockroach/pkg/sql/optionalnodeliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sqlmigrations"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc"
)

// makeTestConfig returns a config for testing. It overrides the
// Certs with the test certs directory.
// We need to override the certs loader.
func makeTestConfig(st *cluster.Settings) Config {
	return Config{
		BaseConfig: makeTestBaseConfig(st),
		KVConfig:   makeTestKVConfig(),
		SQLConfig:  makeTestSQLConfig(st, roachpb.SystemTenantID),
	}
}

func makeTestBaseConfig(st *cluster.Settings) BaseConfig {
	baseCfg := MakeBaseConfig(st)
	// Test servers start in secure mode by default.
	baseCfg.Insecure = false
	// Configure test storage engine.
	baseCfg.StorageEngine = storage.DefaultStorageEngine
	// Load test certs. In addition, the tests requiring certs
	// need to call security.SetAssetLoader(securitytest.EmbeddedAssets)
	// in their init to mock out the file system calls for calls to AssetFS,
	// which has the test certs compiled in. Typically this is done
	// once per package, in main_test.go.
	baseCfg.SSLCertsDir = security.EmbeddedCertsDir
	// Addr defaults to localhost with port set at time of call to
	// Start() to an available port. May be overridden later (as in
	// makeTestConfigFromParams). Call TestServer.ServingRPCAddr() and
	// .ServingSQLAddr() for the full address (including bound port).
	baseCfg.Addr = util.TestAddr.String()
	baseCfg.AdvertiseAddr = util.TestAddr.String()
	baseCfg.SQLAddr = util.TestAddr.String()
	baseCfg.SQLAdvertiseAddr = util.TestAddr.String()
	baseCfg.SplitListenSQL = true
	baseCfg.HTTPAddr = util.TestAddr.String()
	// Set standard user for intra-cluster traffic.
	baseCfg.User = security.NodeUserName()
	return baseCfg
}

func makeTestKVConfig() KVConfig {
	kvCfg := MakeKVConfig(base.DefaultTestStoreSpec)
	// Enable web session authentication.
	kvCfg.EnableWebSessionAuthentication = true
	return kvCfg
}

func makeTestSQLConfig(st *cluster.Settings, tenID roachpb.TenantID) SQLConfig {
	sqlCfg := MakeSQLConfig(tenID, base.DefaultTestTempStorageConfig(st))
	// Configure the default in-memory temp storage for all tests unless
	// otherwise configured.
	sqlCfg.TempStorageConfig = base.DefaultTestTempStorageConfig(st)
	return sqlCfg
}

// makeTestConfigFromParams creates a Config from a TestServerParams.
func makeTestConfigFromParams(params base.TestServerArgs) Config {
	st := params.Settings
	if params.Settings == nil {
		st = cluster.MakeClusterSettings()
		// TODO(sumeer): re-introduce this randomization.
		// enabledSeparated := rand.Intn(2) == 0
		// log.Infof(context.Background(),
		//	"test Config is randomly setting enabledSeparated: %t",
		//	enabledSeparated)
		// storage.SeparatedIntentsEnabled.Override(&st.SV, enabledSeparated)
	}
	st.ExternalIODir = params.ExternalIODir
	cfg := makeTestConfig(st)
	cfg.TestingKnobs = params.Knobs
	cfg.RaftConfig = params.RaftConfig
	cfg.RaftConfig.SetDefaults()
	if params.LeaseManagerConfig != nil {
		cfg.LeaseManagerConfig = params.LeaseManagerConfig
	} else {
		cfg.LeaseManagerConfig = base.NewLeaseManagerConfig()
	}
	if params.JoinAddr != "" {
		cfg.JoinList = []string{params.JoinAddr}
	}
	cfg.ClusterName = params.ClusterName
	cfg.Insecure = params.Insecure
	cfg.AutoInitializeCluster = !params.NoAutoInitializeCluster
	cfg.SocketFile = params.SocketFile
	cfg.RetryOptions = params.RetryOptions
	cfg.Locality = params.Locality
	if knobs := params.Knobs.Store; knobs != nil {
		if mo := knobs.(*kvserver.StoreTestingKnobs).MaxOffset; mo != 0 {
			cfg.MaxOffset = MaxOffsetType(mo)
		}
	}
	if params.Knobs.Server != nil {
		if zoneConfig := params.Knobs.Server.(*TestingKnobs).DefaultZoneConfigOverride; zoneConfig != nil {
			cfg.DefaultZoneConfig = *zoneConfig
		}
		if systemZoneConfig := params.Knobs.Server.(*TestingKnobs).DefaultSystemZoneConfigOverride; systemZoneConfig != nil {
			cfg.DefaultSystemZoneConfig = *systemZoneConfig
		}
	}
	if params.ScanInterval != 0 {
		cfg.ScanInterval = params.ScanInterval
	}
	if params.ScanMinIdleTime != 0 {
		cfg.ScanMinIdleTime = params.ScanMinIdleTime
	}
	if params.ScanMaxIdleTime != 0 {
		cfg.ScanMaxIdleTime = params.ScanMaxIdleTime
	}
	if params.SSLCertsDir != "" {
		cfg.SSLCertsDir = params.SSLCertsDir
	}
	if params.TimeSeriesQueryWorkerMax != 0 {
		cfg.TimeSeriesServerConfig.QueryWorkerMax = params.TimeSeriesQueryWorkerMax
	}
	if params.TimeSeriesQueryMemoryBudget != 0 {
		cfg.TimeSeriesServerConfig.QueryMemoryMax = params.TimeSeriesQueryMemoryBudget
	}
	if params.DisableEventLog {
		cfg.EventLogEnabled = false
	}
	if params.SQLMemoryPoolSize != 0 {
		cfg.MemoryPoolSize = params.SQLMemoryPoolSize
	}
	if params.CacheSize != 0 {
		cfg.CacheSize = params.CacheSize
	}

	if params.JoinAddr != "" {
		cfg.JoinList = []string{params.JoinAddr}
	}
	if cfg.Insecure {
		// Whenever we can (i.e. in insecure mode), use IsolatedTestAddr
		// to prevent issues that can occur when running a test under
		// stress.
		cfg.Addr = util.IsolatedTestAddr.String()
		cfg.AdvertiseAddr = util.IsolatedTestAddr.String()
		cfg.SQLAddr = util.IsolatedTestAddr.String()
		cfg.SQLAdvertiseAddr = util.IsolatedTestAddr.String()
		cfg.HTTPAddr = util.IsolatedTestAddr.String()
	}
	if params.Addr != "" {
		cfg.Addr = params.Addr
		cfg.AdvertiseAddr = params.Addr
	}
	if params.SQLAddr != "" {
		cfg.SQLAddr = params.SQLAddr
		cfg.SQLAdvertiseAddr = params.SQLAddr
		cfg.SplitListenSQL = true
	}
	if params.HTTPAddr != "" {
		cfg.HTTPAddr = params.HTTPAddr
	}
	cfg.DisableTLSForHTTP = params.DisableTLSForHTTP
	if params.DisableWebSessionAuthentication {
		cfg.EnableWebSessionAuthentication = false
	}
	if params.EnableDemoLoginEndpoint {
		cfg.EnableDemoLoginEndpoint = true
	}

	// Ensure we have the correct number of engines. Add in-memory ones where
	// needed. There must be at least one store/engine.
	if len(params.StoreSpecs) == 0 {
		params.StoreSpecs = []base.StoreSpec{base.DefaultTestStoreSpec}
	}
	// Validate the store specs.
	for _, storeSpec := range params.StoreSpecs {
		if storeSpec.InMemory {
			if storeSpec.Size.Percent > 0 {
				panic(fmt.Sprintf("test server does not yet support in memory stores based on percentage of total memory: %s", storeSpec))
			}
		} else {
			// The default store spec is in-memory, so if this one is on-disk then
			// one specific test must have requested it. A failure is returned if
			// the Path field is empty, which means the test is then forced to pick
			// the dir (and the test is then responsible for cleaning it up, not
			// TestServer).

			// HeapProfileDirName and GoroutineDumpDirName are normally set by the
			// cli, once, to the path of the first store.
			if cfg.HeapProfileDirName == "" {
				cfg.HeapProfileDirName = filepath.Join(storeSpec.Path, "logs", base.HeapProfileDir)
			}
			if cfg.GoroutineDumpDirName == "" {
				cfg.GoroutineDumpDirName = filepath.Join(storeSpec.Path, "logs", base.GoroutineDumpDir)
			}
		}
	}
	cfg.Stores = base.StoreSpecList{Specs: params.StoreSpecs}
	if params.TempStorageConfig.InMemory || params.TempStorageConfig.Path != "" {
		cfg.TempStorageConfig = params.TempStorageConfig
	}

	if cfg.TestingKnobs.Store == nil {
		cfg.TestingKnobs.Store = &kvserver.StoreTestingKnobs{}
	}
	cfg.TestingKnobs.Store.(*kvserver.StoreTestingKnobs).SkipMinSizeCheck = true

	if params.Knobs.SQLExecutor == nil {
		cfg.TestingKnobs.SQLExecutor = &sql.ExecutorTestingKnobs{}
	}

	// For test servers, leave interleaved tables enabled by default. We'll remove
	// this when we remove interleaved tables altogether.
	sql.InterleavedTablesEnabled.Override(&cfg.Settings.SV, true)

	return cfg
}

// A TestServer encapsulates an in-memory instantiation of a cockroach node with
// a single store. It provides tests with access to Server internals.
// Where possible, it should be used through the
// testingshim.TestServerInterface.
//
// Example usage of a TestServer:
//
//   s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
//   defer s.Stopper().Stop()
//   // If really needed, in tests that can depend on server, downcast to
//   // server.TestServer:
//   ts := s.(*server.TestServer)
//
type TestServer struct {
	Cfg    *Config
	params base.TestServerArgs
	// server is the embedded Cockroach server struct.
	*Server
	// authClient is an http.Client that has been authenticated to access the
	// Admin UI.
	authClient [2]struct {
		httpClient http.Client
		cookie     *serverpb.SessionCookie
		once       sync.Once
		err        error
	}
}

// Node returns the Node as an interface{}.
func (ts *TestServer) Node() interface{} {
	return ts.node
}

// NodeID returns the ID of this node within its cluster.
func (ts *TestServer) NodeID() roachpb.NodeID {
	return ts.rpcContext.NodeID.Get()
}

// Stopper returns the embedded server's Stopper.
func (ts *TestServer) Stopper() *stop.Stopper {
	return ts.stopper
}

// GossipI is part of TestServerInterface.
func (ts *TestServer) GossipI() interface{} {
	return ts.Gossip()
}

// Gossip is like GossipI but returns the real type instead of interface{}.
func (ts *TestServer) Gossip() *gossip.Gossip {
	if ts != nil {
		return ts.gossip
	}
	return nil
}

// RangeFeedFactory is part of serverutils.TestServerInterface.
func (ts *TestServer) RangeFeedFactory() interface{} {
	if ts != nil {
		return ts.sqlServer.execCfg.RangeFeedFactory
	}
	return (*rangefeed.Factory)(nil)
}

// Clock returns the clock used by the TestServer.
func (ts *TestServer) Clock() *hlc.Clock {
	if ts != nil {
		return ts.clock
	}
	return nil
}

// SQLLivenessProvider returns the sqlliveness.Provider as an interface{}.
func (ts *TestServer) SQLLivenessProvider() interface{} {
	if ts != nil {
		return ts.sqlServer.execCfg.SQLLivenessReader
	}
	return nil
}

// JobRegistry returns the *jobs.Registry as an interface{}.
func (ts *TestServer) JobRegistry() interface{} {
	if ts != nil {
		return ts.sqlServer.jobRegistry
	}
	return nil
}

// SQLMigrationsManager returns the *sqlmigrations.Manager as an interface{}.
func (ts *TestServer) SQLMigrationsManager() interface{} {
	if ts != nil {
		return ts.sqlServer.sqlmigrationsMgr
	}
	return nil
}

// NodeLiveness exposes the NodeLiveness instance used by the TestServer as an
// interface{}.
func (ts *TestServer) NodeLiveness() interface{} {
	if ts != nil {
		return ts.nodeLiveness
	}
	return nil
}

// HeartbeatNodeLiveness heartbeats the server's NodeLiveness record.
func (ts *TestServer) HeartbeatNodeLiveness() error {
	if ts == nil {
		return errors.New("no node liveness instance")
	}
	nl := ts.nodeLiveness
	l, ok := nl.Self()
	if !ok {
		return errors.New("liveness not found")
	}

	var err error
	ctx := context.Background()
	for r := retry.StartWithCtx(ctx, retry.Options{MaxRetries: 5}); r.Next(); {
		if err = nl.Heartbeat(ctx, l); !errors.Is(err, liveness.ErrEpochIncremented) {
			break
		}
	}
	return err
}

// RPCContext returns the rpc context used by the TestServer.
func (ts *TestServer) RPCContext() *rpc.Context {
	if ts != nil {
		return ts.rpcContext
	}
	return nil
}

// TsDB returns the ts.DB instance used by the TestServer.
func (ts *TestServer) TsDB() *ts.DB {
	if ts != nil {
		return ts.tsDB
	}
	return nil
}

// DB returns the client.DB instance used by the TestServer.
func (ts *TestServer) DB() *kv.DB {
	if ts != nil {
		return ts.db
	}
	return nil
}

// PGServer returns the pgwire.Server used by the TestServer.
func (ts *TestServer) PGServer() *pgwire.Server {
	if ts != nil {
		return ts.sqlServer.pgServer
	}
	return nil
}

// RaftTransport returns the RaftTransport used by the TestServer.
func (ts *TestServer) RaftTransport() *kvserver.RaftTransport {
	if ts != nil {
		return ts.raftTransport
	}
	return nil
}

// NodeDialer returns the NodeDialer used by the TestServer.
func (ts *TestServer) NodeDialer() *nodedialer.Dialer {
	if ts != nil {
		return ts.nodeDialer
	}
	return nil
}

// Start starts the TestServer by bootstrapping an in-memory store
// (defaults to maximum of 100M). The server is started, launching the
// node RPC server and all HTTP endpoints. Use the value of
// TestServer.ServingRPCAddr() after Start() for client connections.
// Use TestServer.Stopper().Stop() to shutdown the server after the test
// completes.
func (ts *TestServer) Start(ctx context.Context) error {
	return ts.Server.Start(ctx)
}

type dummyProtectedTSProvider struct {
	protectedts.Provider
}

func (d dummyProtectedTSProvider) Protect(context.Context, *kv.Txn, *ptpb.Record) error {
	return errors.New("fake protectedts.Provider")
}

func makeSQLServerArgs(
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

	dsCfg := kvcoord.DistSenderConfig{
		AmbientCtx:        baseCfg.AmbientCtx,
		Settings:          st,
		Clock:             clock,
		NodeDescs:         tenantConnect,
		RPCRetryOptions:   &rpcRetryOptions,
		RPCContext:        rpcContext,
		NodeDialer:        nodeDialer,
		RangeDescriptorDB: tenantConnect,
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

	const sqlInstanceID = base.SQLInstanceID(10001)
	idContainer := base.NewSQLIDContainer(sqlInstanceID, nil /* nodeID */)

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

	esb.init(base.ExternalIODirConfig{}, baseCfg.Settings, nil, circularInternalExecutor, db)

	// We don't need this for anything except some services that want a gRPC
	// server to register against (but they'll never get RPCs at the time of
	// writing): the blob service and DistSQL.
	dummyRPCServer := grpc.NewServer()
	sessionRegistry := sql.NewSessionRegistry()
	contentionRegistry := contention.NewRegistry()
	return sqlServerArgs{
		sqlServerOptionalKVArgs: sqlServerOptionalKVArgs{
			nodesStatusServer: serverpb.MakeOptionalNodesStatusServer(nil),
			nodeLiveness:      optionalnodeliveness.MakeContainer(nil),
			gossip:            gossip.MakeOptionalGossip(nil),
			grpcServer:        dummyRPCServer,
			isMeta1Leaseholder: func(_ context.Context, _ hlc.ClockTimestamp) (bool, error) {
				return false, errors.New("isMeta1Leaseholder is not available to secondary tenants")
			},
			nodeIDContainer:        idContainer,
			externalStorage:        externalStorage,
			externalStorageFromURI: externalStorageFromURI,
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
		nodeDialer:               nodeDialer,
		distSender:               ds,
		db:                       db,
		registry:                 registry,
		recorder:                 recorder,
		sessionRegistry:          sessionRegistry,
		contentionRegistry:       contentionRegistry,
		circularInternalExecutor: circularInternalExecutor,
		circularJobRegistry:      &jobs.Registry{},
		protectedtsProvider:      protectedTSProvider,
		rangeFeedFactory:         rangeFeedFactory,
	}, nil
}

// TestTenant is an in-memory instantiation of the SQL-only process created for
// each active Cockroach tenant. TestTenant provides tests with access to
// internal methods and state on SQLServer. It is typically started in tests by
// calling the TestServerInterface.StartTenant method or by calling the wrapper
// serverutils.StartTenant method.
type TestTenant struct {
	*SQLServer
	sqlAddr  string
	httpAddr string
}

// SQLAddr is part of the TestTenantInterface interface.
func (t *TestTenant) SQLAddr() string {
	return t.sqlAddr
}

// HTTPAddr is part of the TestTenantInterface interface.
func (t *TestTenant) HTTPAddr() string {
	return t.httpAddr
}

// PGServer is part of the TestTenantInterface interface.
func (t *TestTenant) PGServer() interface{} {
	return t.pgServer
}

// DiagnosticsReporter is part of the TestTenantInterface interface.
func (t *TestTenant) DiagnosticsReporter() interface{} {
	return t.diagnosticsReporter
}

// StatusServer is part of the TestTenantInterface interface.
func (t *TestTenant) StatusServer() interface{} {
	return t.execCfg.SQLStatusServer
}

// SetupIdleMonitor will monitor the active connections and if there are none,
// will activate a `defaultCountdownDuration` countdown timer and terminate
// the application. The monitoring will start after a warmup period
// specified by warmupDuration. If the warmupDuration is zero, the idle
// detection will be turned off.
func SetupIdleMonitor(
	ctx context.Context,
	stopper *stop.Stopper,
	warmupDuration time.Duration,
	server netutil.Server,
	countdownDuration ...time.Duration,
) *IdleMonitor {
	if warmupDuration != 0 {
		log.VEventf(ctx, 2, "idle exit will activate after warmup duration of %s", warmupDuration)
		oldConnStateHandler := server.ConnState
		idleMonitor := MakeIdleMonitor(ctx, warmupDuration,
			func() {
				log.VEventf(ctx, 2, "idle exiting")
				stopper.Stop(ctx)
			},
			countdownDuration...,
		)
		server.ConnState = func(conn net.Conn, state http.ConnState) {
			if state == http.StateNew {
				defer oldConnStateHandler(conn, state)
				idleMonitor.NewConnection(ctx)
			} else if state == http.StateClosed {
				defer idleMonitor.CloseConnection(ctx)
				oldConnStateHandler(conn, state)
			}
		}
		return idleMonitor
	}
	return nil
}

// StartTenant starts a SQL tenant communicating with this TestServer.
func (ts *TestServer) StartTenant(
	params base.TestTenantArgs,
) (serverutils.TestTenantInterface, error) {
	ctx := context.Background()

	if !params.Existing {
		if _, err := ts.InternalExecutor().(*sql.InternalExecutor).Exec(
			ctx, "testserver-create-tenant", nil /* txn */, "SELECT crdb_internal.create_tenant($1)", params.TenantID.ToUint64(),
		); err != nil {
			return nil, err
		}
	}

	st := cluster.MakeTestingClusterSettings()
	sqlCfg := makeTestSQLConfig(st, params.TenantID)
	sqlCfg.TenantKVAddrs = []string{ts.ServingRPCAddr()}
	baseCfg := makeTestBaseConfig(st)
	baseCfg.TestingKnobs = params.TestingKnobs
	baseCfg.IdleExitAfter = params.IdleExitAfter
	if params.AllowSettingClusterSettings {
		baseCfg.TestingKnobs.TenantTestingKnobs = &sql.TenantTestingKnobs{
			ClusterSettingsUpdater: st.MakeUpdater(),
		}
	}
	stopper := params.Stopper
	if stopper == nil {
		stopper = ts.Stopper()
	}
	sqlServer, addr, httpAddr, err := StartTenant(
		ctx,
		stopper,
		ts.Cfg.ClusterName,
		baseCfg,
		sqlCfg,
	)
	return &TestTenant{SQLServer: sqlServer, sqlAddr: addr, httpAddr: httpAddr}, err
}

// ExpectedInitialRangeCount returns the expected number of ranges that should
// be on the server after initial (asynchronous) splits have been completed,
// assuming no additional information is added outside of the normal bootstrap
// process.
func (ts *TestServer) ExpectedInitialRangeCount() (int, error) {
	return ExpectedInitialRangeCount(ts.DB(), &ts.cfg.DefaultZoneConfig, &ts.cfg.DefaultSystemZoneConfig)
}

// ExpectedInitialRangeCount returns the expected number of ranges that should
// be on the server after bootstrap.
func ExpectedInitialRangeCount(
	db *kv.DB, defaultZoneConfig *zonepb.ZoneConfig, defaultSystemZoneConfig *zonepb.ZoneConfig,
) (int, error) {
	descriptorIDs, err := sqlmigrations.ExpectedDescriptorIDs(
		context.Background(), db, keys.SystemSQLCodec, defaultZoneConfig, defaultSystemZoneConfig,
	)
	if err != nil {
		return 0, err
	}

	// System table splits occur at every possible table boundary between the end
	// of the system config ID space (keys.MaxSystemConfigDescID) and the system
	// table with the maximum ID (maxSystemDescriptorID), even when an ID within
	// the span does not have an associated descriptor.
	maxSystemDescriptorID := descriptorIDs[0]
	for _, descID := range descriptorIDs {
		if descID > maxSystemDescriptorID && descID <= keys.MaxReservedDescID {
			maxSystemDescriptorID = descID
		}
	}
	if maxSystemDescriptorID < descpb.ID(keys.MaxPseudoTableID) {
		maxSystemDescriptorID = descpb.ID(keys.MaxPseudoTableID)
	}
	systemTableSplits := int(maxSystemDescriptorID - keys.MaxSystemConfigDescID)

	// `n` splits create `n+1` ranges.
	return len(config.StaticSplits()) + systemTableSplits + 1, nil
}

// Stores returns the collection of stores from this TestServer's node.
func (ts *TestServer) Stores() *kvserver.Stores {
	return ts.node.stores
}

// GetStores is part of TestServerInterface.
func (ts *TestServer) GetStores() interface{} {
	return ts.node.stores
}

// ClusterSettings returns the ClusterSettings.
func (ts *TestServer) ClusterSettings() *cluster.Settings {
	return ts.Cfg.Settings
}

// Engines returns the TestServer's engines.
func (ts *TestServer) Engines() []storage.Engine {
	return ts.engines
}

// ServingRPCAddr returns the server's RPC address. Should be used by clients.
func (ts *TestServer) ServingRPCAddr() string {
	return ts.cfg.AdvertiseAddr
}

// ServingSQLAddr returns the server's SQL address. Should be used by clients.
func (ts *TestServer) ServingSQLAddr() string {
	return ts.cfg.SQLAdvertiseAddr
}

// HTTPAddr returns the server's HTTP address. Should be used by clients.
func (ts *TestServer) HTTPAddr() string {
	return ts.cfg.HTTPAddr
}

// RPCAddr returns the server's listening RPC address.
// Note: use ServingRPCAddr() instead unless there is a specific reason not to.
func (ts *TestServer) RPCAddr() string {
	return ts.cfg.Addr
}

// SQLAddr returns the server's listening SQL address.
// Note: use ServingSQLAddr() instead unless there is a specific reason not to.
func (ts *TestServer) SQLAddr() string {
	return ts.cfg.SQLAddr
}

// DrainClients exports the drainClients() method for use by tests.
func (ts *TestServer) DrainClients(ctx context.Context) error {
	return ts.drainClients(ctx, nil /* reporter */)
}

// Readiness returns nil when the server's health probe reports
// readiness, a readiness error otherwise.
func (ts *TestServer) Readiness(ctx context.Context) error {
	return ts.admin.checkReadinessForHealthCheck(ctx)
}

// WriteSummaries implements TestServerInterface.
func (ts *TestServer) WriteSummaries() error {
	return ts.node.writeNodeStatus(context.TODO(), time.Hour, false)
}

// AdminURL implements TestServerInterface.
func (ts *TestServer) AdminURL() string {
	return ts.Cfg.AdminURL().String()
}

// GetHTTPClient implements TestServerInterface.
func (ts *TestServer) GetHTTPClient() (http.Client, error) {
	return ts.Server.rpcContext.GetHTTPClient()
}

// UpdateChecker implements TestServerInterface.
func (ts *TestServer) UpdateChecker() interface{} {
	return ts.Server.updates
}

// DiagnosticsReporter implements TestServerInterface.
func (ts *TestServer) DiagnosticsReporter() interface{} {
	return ts.Server.sqlServer.diagnosticsReporter
}

const authenticatedUser = "authentic_user"

func authenticatedUserName() security.SQLUsername {
	return security.MakeSQLUsernameFromPreNormalizedString(authenticatedUser)
}

const authenticatedUserNoAdmin = "authentic_user_noadmin"

func authenticatedUserNameNoAdmin() security.SQLUsername {
	return security.MakeSQLUsernameFromPreNormalizedString(authenticatedUserNoAdmin)
}

// GetAdminAuthenticatedHTTPClient implements the TestServerInterface.
func (ts *TestServer) GetAdminAuthenticatedHTTPClient() (http.Client, error) {
	httpClient, _, err := ts.getAuthenticatedHTTPClientAndCookie(authenticatedUserName(), true)
	return httpClient, err
}

// GetAuthenticatedHTTPClient implements the TestServerInterface.
func (ts *TestServer) GetAuthenticatedHTTPClient(isAdmin bool) (http.Client, error) {
	authUser := authenticatedUserName()
	if !isAdmin {
		authUser = authenticatedUserNameNoAdmin()
	}
	httpClient, _, err := ts.getAuthenticatedHTTPClientAndCookie(authUser, isAdmin)
	return httpClient, err
}

type v2AuthDecorator struct {
	http.RoundTripper

	session string
}

func (v *v2AuthDecorator) RoundTrip(r *http.Request) (*http.Response, error) {
	r.Header.Add(apiV2AuthHeader, v.session)
	return v.RoundTripper.RoundTrip(r)
}

func (ts *TestServer) getAuthenticatedHTTPClientAndCookie(
	authUser security.SQLUsername, isAdmin bool,
) (http.Client, *serverpb.SessionCookie, error) {
	authIdx := 0
	if isAdmin {
		authIdx = 1
	}
	authClient := &ts.authClient[authIdx]
	authClient.once.Do(func() {
		// Create an authentication session for an arbitrary admin user.
		authClient.err = func() error {
			// The user needs to exist as the admin endpoints will check its role.
			if err := ts.createAuthUser(authUser, isAdmin); err != nil {
				return err
			}

			id, secret, err := ts.authentication.newAuthSession(context.TODO(), authUser)
			if err != nil {
				return err
			}
			rawCookie := &serverpb.SessionCookie{
				ID:     id,
				Secret: secret,
			}
			// Encode a session cookie and store it in a cookie jar.
			cookie, err := EncodeSessionCookie(rawCookie, false /* forHTTPSOnly */)
			if err != nil {
				return err
			}
			cookieJar, err := cookiejar.New(nil)
			if err != nil {
				return err
			}
			url, err := url.Parse(ts.AdminURL())
			if err != nil {
				return err
			}
			cookieJar.SetCookies(url, []*http.Cookie{cookie})
			// Create an httpClient and attach the cookie jar to the client.
			authClient.httpClient, err = ts.rpcContext.GetHTTPClient()
			if err != nil {
				return err
			}
			rawCookieBytes, err := protoutil.Marshal(rawCookie)
			if err != nil {
				return err
			}
			authClient.httpClient.Transport = &v2AuthDecorator{
				RoundTripper: authClient.httpClient.Transport,
				session:      base64.StdEncoding.EncodeToString(rawCookieBytes),
			}
			authClient.httpClient.Jar = cookieJar
			authClient.cookie = rawCookie
			return nil
		}()
	})

	return authClient.httpClient, authClient.cookie, authClient.err
}

func (ts *TestServer) createAuthUser(userName security.SQLUsername, isAdmin bool) error {
	if _, err := ts.Server.sqlServer.internalExecutor.ExecEx(context.TODO(),
		"create-auth-user", nil,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		"CREATE USER $1", userName.Normalized(),
	); err != nil {
		return err
	}
	if isAdmin {
		// We can't use the GRANT statement here because we don't want
		// to rely on CCL code.
		if _, err := ts.Server.sqlServer.internalExecutor.ExecEx(context.TODO(),
			"grant-admin", nil,
			sessiondata.InternalExecutorOverride{User: security.RootUserName()},
			"INSERT INTO system.role_members (role, member, \"isAdmin\") VALUES ('admin', $1, true)", userName.Normalized(),
		); err != nil {
			return err
		}
	}
	return nil
}

// MustGetSQLCounter implements TestServerInterface.
func (ts *TestServer) MustGetSQLCounter(name string) int64 {
	var c int64
	var found bool

	ts.registry.Each(func(n string, v interface{}) {
		if name == n {
			switch t := v.(type) {
			case *metric.Counter:
				c = t.Count()
				found = true
			case *metric.Gauge:
				c = t.Value()
				found = true
			}
		}
	})
	if !found {
		panic(fmt.Sprintf("couldn't find metric %s", name))
	}
	return c
}

// MustGetSQLNetworkCounter implements TestServerInterface.
func (ts *TestServer) MustGetSQLNetworkCounter(name string) int64 {
	var c int64
	var found bool

	reg := metric.NewRegistry()
	for _, m := range ts.sqlServer.pgServer.Metrics() {
		reg.AddMetricStruct(m)
	}
	reg.Each(func(n string, v interface{}) {
		if name == n {
			switch t := v.(type) {
			case *metric.Counter:
				c = t.Count()
				found = true
			case *metric.Gauge:
				c = t.Value()
				found = true
			}
		}
	})
	if !found {
		panic(fmt.Sprintf("couldn't find metric %s", name))
	}
	return c
}

// LeaseManager is part of TestServerInterface.
func (ts *TestServer) LeaseManager() interface{} {
	return ts.sqlServer.leaseMgr
}

// InternalExecutor is part of TestServerInterface.
func (ts *TestServer) InternalExecutor() interface{} {
	return ts.sqlServer.internalExecutor
}

// GetNode exposes the Server's Node.
func (ts *TestServer) GetNode() *Node {
	return ts.node
}

// DistSenderI is part of DistSendeInterface.
func (ts *TestServer) DistSenderI() interface{} {
	return ts.distSender
}

// DistSender is like DistSenderI(), but returns the real type instead of
// interface{}.
func (ts *TestServer) DistSender() *kvcoord.DistSender {
	return ts.DistSenderI().(*kvcoord.DistSender)
}

// MigrationServer is part of TestServerInterface.
func (ts *TestServer) MigrationServer() interface{} {
	return ts.migrationServer
}

// SQLServer is part of TestServerInterface.
func (ts *TestServer) SQLServer() interface{} {
	return ts.PGServer().SQLServer
}

// DistSQLServer is part of TestServerInterface.
func (ts *TestServer) DistSQLServer() interface{} {
	return ts.sqlServer.distSQLServer
}

// SetDistSQLSpanResolver is part of TestServerInterface.
func (s *Server) SetDistSQLSpanResolver(spanResolver interface{}) {
	s.sqlServer.execCfg.DistSQLPlanner.SetSpanResolver(spanResolver.(physicalplan.SpanResolver))
}

// GetFirstStoreID is part of TestServerInterface.
func (ts *TestServer) GetFirstStoreID() roachpb.StoreID {
	firstStoreID := roachpb.StoreID(-1)
	err := ts.Stores().VisitStores(func(s *kvserver.Store) error {
		if firstStoreID == -1 {
			firstStoreID = s.Ident.StoreID
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	return firstStoreID
}

// LookupRange returns the descriptor of the range containing key.
func (ts *TestServer) LookupRange(key roachpb.Key) (roachpb.RangeDescriptor, error) {
	rs, _, err := kv.RangeLookup(context.Background(), ts.DB().NonTransactionalSender(),
		key, roachpb.CONSISTENT, 0 /* prefetchNum */, false /* reverse */)
	if err != nil {
		return roachpb.RangeDescriptor{}, errors.Errorf(
			"%q: lookup range unexpected error: %s", key, err)
	}
	return rs[0], nil
}

// MergeRanges merges the range containing leftKey with the range to its right.
func (ts *TestServer) MergeRanges(leftKey roachpb.Key) (roachpb.RangeDescriptor, error) {

	ctx := context.Background()
	mergeReq := roachpb.AdminMergeRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: leftKey,
		},
	}
	_, pErr := kv.SendWrapped(ctx, ts.DB().NonTransactionalSender(), &mergeReq)
	if pErr != nil {
		return roachpb.RangeDescriptor{},
			errors.Errorf(
				"%q: merge unexpected error: %s", leftKey, pErr)
	}
	return ts.LookupRange(leftKey)
}

// SplitRangeWithExpiration splits the range containing splitKey with a sticky
// bit expiring at expirationTime.
// The right range created by the split starts at the split key and extends to the
// original range's end key.
// Returns the new descriptors of the left and right ranges.
//
// splitKey must correspond to a SQL table key (it must end with a family ID /
// col ID).
func (ts *TestServer) SplitRangeWithExpiration(
	splitKey roachpb.Key, expirationTime hlc.Timestamp,
) (roachpb.RangeDescriptor, roachpb.RangeDescriptor, error) {
	ctx := context.Background()
	splitRKey, err := keys.Addr(splitKey)
	if err != nil {
		return roachpb.RangeDescriptor{}, roachpb.RangeDescriptor{}, err
	}
	splitReq := roachpb.AdminSplitRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: splitKey,
		},
		SplitKey:       splitKey,
		ExpirationTime: expirationTime,
	}
	_, pErr := kv.SendWrapped(ctx, ts.DB().NonTransactionalSender(), &splitReq)
	if pErr != nil {
		return roachpb.RangeDescriptor{}, roachpb.RangeDescriptor{},
			errors.Errorf(
				"%q: split unexpected error: %s", splitReq.SplitKey, pErr)
	}

	// The split point may not be exactly at the key we requested (we request
	// splits at valid table keys, and the split point corresponds to the row's
	// prefix). We scan for the range that includes the key we requested and the
	// one that precedes it.

	// We use a transaction so that we get consistent results between the two
	// scans (in case there are other splits happening).
	var leftRangeDesc, rightRangeDesc roachpb.RangeDescriptor

	// Errors returned from scanMeta cannot be wrapped or retryable errors won't
	// be retried. Instead, the message to wrap is stored in case of
	// non-retryable failures and then wrapped when the full transaction fails.
	var wrappedMsg string
	if err := ts.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		scanMeta := func(key roachpb.RKey, reverse bool) (desc roachpb.RangeDescriptor, err error) {
			var kvs []kv.KeyValue
			if reverse {
				// Find the last range that ends at or before key.
				kvs, err = txn.ReverseScan(
					ctx, keys.Meta2Prefix, keys.RangeMetaKey(key.Next()), 1, /* one result */
				)
			} else {
				// Find the first range that ends after key.
				kvs, err = txn.Scan(
					ctx, keys.RangeMetaKey(key.Next()), keys.Meta2Prefix.PrefixEnd(), 1, /* one result */
				)
			}
			if err != nil {
				return desc, err
			}
			if len(kvs) != 1 {
				return desc, fmt.Errorf("expected 1 result, got %d", len(kvs))
			}
			err = kvs[0].ValueProto(&desc)
			return desc, err
		}

		rightRangeDesc, err = scanMeta(splitRKey, false /* reverse */)
		if err != nil {
			wrappedMsg = "could not look up right-hand side descriptor"
			return err
		}

		leftRangeDesc, err = scanMeta(splitRKey, true /* reverse */)
		if err != nil {
			wrappedMsg = "could not look up left-hand side descriptor"
			return err
		}

		if !leftRangeDesc.EndKey.Equal(rightRangeDesc.StartKey) {
			return errors.Errorf(
				"inconsistent left (%v) and right (%v) descriptors", leftRangeDesc, rightRangeDesc,
			)
		}
		return nil
	}); err != nil {
		if len(wrappedMsg) > 0 {
			err = errors.Wrapf(err, "%s", wrappedMsg)
		}
		return roachpb.RangeDescriptor{}, roachpb.RangeDescriptor{}, err
	}

	return leftRangeDesc, rightRangeDesc, nil
}

// SplitRange is exactly like SplitRangeWithExpiration, except that it creates a
// split with a sticky bit that never expires.
func (ts *TestServer) SplitRange(
	splitKey roachpb.Key,
) (roachpb.RangeDescriptor, roachpb.RangeDescriptor, error) {
	return ts.SplitRangeWithExpiration(splitKey, hlc.MaxTimestamp)
}

// GetRangeLease returns the current lease for the range containing key, and a
// timestamp taken from the node.
//
// The lease is returned regardless of its status.
func (ts *TestServer) GetRangeLease(
	ctx context.Context, key roachpb.Key,
) (_ roachpb.Lease, now hlc.ClockTimestamp, _ error) {
	leaseReq := roachpb.LeaseInfoRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
	}
	leaseResp, pErr := kv.SendWrappedWith(
		ctx,
		ts.DB().NonTransactionalSender(),
		roachpb.Header{
			// INCONSISTENT read, since we want to make sure that the node used to
			// send this is the one that processes the command, for the hint to
			// matter.
			ReadConsistency: roachpb.INCONSISTENT,
		},
		&leaseReq,
	)
	if pErr != nil {
		return roachpb.Lease{}, hlc.ClockTimestamp{}, pErr.GoError()
	}
	return leaseResp.(*roachpb.LeaseInfoResponse).Lease, ts.Clock().NowAsClockTimestamp(), nil

}

// ExecutorConfig is part of the TestServerInterface.
func (ts *TestServer) ExecutorConfig() interface{} {
	return *ts.sqlServer.execCfg
}

// Tracer is part of the TestServerInterface.
func (ts *TestServer) Tracer() interface{} {
	return ts.node.storeCfg.AmbientCtx.Tracer
}

// GCSystemLog deletes entries in the given system log table between
// timestamp and timestampUpperBound if the server is the lease holder
// for range 1.
// Leaseholder constraint is present so that only one node in the cluster
// performs gc.
// The system log table is expected to have a "timestamp" column.
// It returns the timestampLowerBound to be used in the next iteration, number
// of rows affected and error (if any).
func (ts *TestServer) GCSystemLog(
	ctx context.Context, table string, timestampLowerBound, timestampUpperBound time.Time,
) (time.Time, int64, error) {
	return ts.gcSystemLog(ctx, table, timestampLowerBound, timestampUpperBound)
}

// ForceTableGC is part of TestServerInterface.
func (ts *TestServer) ForceTableGC(
	ctx context.Context, database, table string, timestamp hlc.Timestamp,
) error {
	tableIDQuery := `
 SELECT tables.id FROM system.namespace tables
   JOIN system.namespace dbs ON dbs.id = tables."parentID"
   WHERE dbs.name = $1 AND tables.name = $2
 `
	row, err := ts.sqlServer.internalExecutor.QueryRowEx(
		ctx, "resolve-table-id", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		tableIDQuery, database, table)
	if err != nil {
		return err
	}
	if row == nil {
		return errors.Errorf("table not found")
	}
	if len(row) != 1 {
		return errors.AssertionFailedf("expected 1 column from internal query")
	}
	tableID := uint32(*row[0].(*tree.DInt))
	tblKey := keys.SystemSQLCodec.TablePrefix(tableID)
	gcr := roachpb.GCRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    tblKey,
			EndKey: tblKey.PrefixEnd(),
		},
		Threshold: timestamp,
	}
	_, pErr := kv.SendWrapped(ctx, ts.distSender, &gcr)
	return pErr.GoError()
}

// ScratchRange is like ScratchRangeEx, but only returns the start key of the
// new range instead of the range descriptor.
func (ts *TestServer) ScratchRange() (roachpb.Key, error) {
	_, desc, err := ts.ScratchRangeEx()
	if err != nil {
		return nil, err
	}
	return desc.StartKey.AsRawKey(), nil
}

// ScratchRangeEx splits off a range suitable to be used as KV scratch space.
// (it doesn't overlap system spans or SQL tables).
func (ts *TestServer) ScratchRangeEx() (roachpb.RangeDescriptor, roachpb.RangeDescriptor, error) {
	scratchKey := keys.TableDataMax
	return ts.SplitRange(scratchKey)
}

// ScratchRangeWithExpirationLease is like ScratchRangeWithExpirationLeaseEx but
// returns a key for the RHS ranges, instead of both descriptors from the split.
func (ts *TestServer) ScratchRangeWithExpirationLease() (roachpb.Key, error) {
	_, desc, err := ts.ScratchRangeWithExpirationLeaseEx()
	if err != nil {
		return nil, err
	}
	return desc.StartKey.AsRawKey(), nil
}

// ScratchRangeWithExpirationLeaseEx is like ScratchRange but creates a range with
// an expiration based lease.
func (ts *TestServer) ScratchRangeWithExpirationLeaseEx() (
	roachpb.RangeDescriptor,
	roachpb.RangeDescriptor,
	error,
) {
	scratchKey := roachpb.Key(bytes.Join([][]byte{keys.SystemPrefix,
		roachpb.RKey("\x00aaa-testing")}, nil))
	return ts.SplitRange(scratchKey)
}

// MetricsRecorder periodically records node-level and store-level metrics.
func (ts *TestServer) MetricsRecorder() *status.MetricsRecorder {
	return ts.node.recorder
}

type testServerFactoryImpl struct{}

// TestServerFactory can be passed to serverutils.InitTestServerFactory
var TestServerFactory = testServerFactoryImpl{}

// New is part of TestServerFactory interface.
func (testServerFactoryImpl) New(params base.TestServerArgs) (interface{}, error) {
	cfg := makeTestConfigFromParams(params)
	ts := &TestServer{Cfg: &cfg, params: params}

	if params.Stopper == nil {
		params.Stopper = stop.NewStopper()
	}

	if !params.PartOfCluster {
		ts.Cfg.DefaultZoneConfig.NumReplicas = proto.Int32(1)
	}

	// Needs to be called before NewServer to ensure resolvers are initialized.
	ctx := context.Background()
	if err := ts.Cfg.InitNode(ctx); err != nil {
		params.Stopper.Stop(ctx)
		return nil, err
	}

	var err error
	ts.Server, err = NewServer(*ts.Cfg, params.Stopper)
	if err != nil {
		params.Stopper.Stop(ctx)
		return nil, err
	}

	// Create a breaker which never trips and never backs off to avoid
	// introducing timing-based flakes.
	ts.rpcContext.BreakerFactory = func() *circuit.Breaker {
		return circuit.NewBreakerWithOptions(&circuit.Options{
			BackOff: &backoff.ZeroBackOff{},
		})
	}

	// Our context must be shared with our server.
	ts.Cfg = &ts.Server.cfg

	return ts, nil
}

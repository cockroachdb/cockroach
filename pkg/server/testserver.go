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
	"github.com/cockroachdb/cockroach/pkg/gossip/resolver"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sqlmigrations"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

const (
	// TestUser is a fixed user used in unittests.
	// It has valid embedded client certs.
	TestUser = "testuser"
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
	// Resolve the storage engine to a specific type if it's the default value.
	if baseCfg.StorageEngine == enginepb.EngineTypeDefault {
		baseCfg.StorageEngine = enginepb.EngineTypePebble
	}
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
	baseCfg.SQLAddr = util.TestAddr.String()
	baseCfg.AdvertiseAddr = util.TestAddr.String()
	baseCfg.SQLAdvertiseAddr = util.TestAddr.String()
	baseCfg.SplitListenSQL = true
	baseCfg.HTTPAddr = util.TestAddr.String()
	// Set standard user for intra-cluster traffic.
	baseCfg.User = security.NodeUser
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
	} else {
		cfg.Addr = util.TestAddr.String()
		cfg.AdvertiseAddr = util.TestAddr.String()
		cfg.SQLAddr = util.TestAddr.String()
		cfg.SQLAdvertiseAddr = util.TestAddr.String()
		cfg.HTTPAddr = util.TestAddr.String()
	}
	if params.Addr != "" {
		cfg.Addr = params.Addr
		cfg.AdvertiseAddr = params.Addr
	}
	if params.SQLAddr != "" {
		cfg.SQLAddr = params.SQLAddr
		cfg.SQLAdvertiseAddr = params.SQLAddr
	}
	if params.HTTPAddr != "" {
		cfg.HTTPAddr = params.HTTPAddr
	}
	cfg.DisableTLSForHTTP = params.DisableTLSForHTTP
	if params.DisableWebSessionAuthentication {
		cfg.EnableWebSessionAuthentication = false
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
	Cfg *Config
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

// Clock returns the clock used by the TestServer.
func (ts *TestServer) Clock() *hlc.Clock {
	if ts != nil {
		return ts.clock
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

// MigrationManager returns the *sqlmigrations.Manager as an interface{}.
func (ts *TestServer) MigrationManager() interface{} {
	if ts != nil {
		return ts.sqlServer.migMgr
	}
	return nil
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

// Start starts the TestServer by bootstrapping an in-memory store
// (defaults to maximum of 100M). The server is started, launching the
// node RPC server and all HTTP endpoints. Use the value of
// TestServer.ServingRPCAddr() after Start() for client connections.
// Use TestServer.Stopper().Stop() to shutdown the server after the test
// completes.
func (ts *TestServer) Start(params base.TestServerArgs) error {
	if ts.Cfg == nil {
		panic("Cfg not set")
	}

	if params.Stopper == nil {
		params.Stopper = stop.NewStopper()
	}

	if !params.PartOfCluster {
		ts.Cfg.DefaultZoneConfig.NumReplicas = proto.Int32(1)
	}

	ctx := context.Background()

	// Needs to be called before NewServer to ensure resolvers are initialized.
	if err := ts.Cfg.InitNode(ctx); err != nil {
		return err
	}

	var err error
	ts.Server, err = NewServer(*ts.Cfg, params.Stopper)
	if err != nil {
		return err
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

	return ts.Server.Start(ctx)
}

type dummyProtectedTSProvider struct {
	protectedts.Provider
}

func (d dummyProtectedTSProvider) Protect(context.Context, *kv.Txn, *ptpb.Record) error {
	return errors.New("fake protectedts.Provider")
}

const fakeNodeID = roachpb.NodeID(123456789)

func makeSQLServerArgs(
	stopper *stop.Stopper, kvClusterName string, baseCfg BaseConfig, sqlCfg SQLConfig,
) sqlServerArgs {
	st := baseCfg.Settings
	baseCfg.AmbientCtx.AddLogTag("sql", nil)
	// TODO(tbg): this is needed so that the RPC heartbeats between the testcluster
	// and this tenant work.
	//
	// TODO(tbg): address this when we introduce the real tenant RPCs in:
	// https://github.com/cockroachdb/cockroach/issues/47898
	baseCfg.ClusterName = kvClusterName

	clock := hlc.NewClock(hlc.UnixNano, time.Duration(baseCfg.MaxOffset))

	var rpcTestingKnobs rpc.ContextTestingKnobs
	if p, ok := baseCfg.TestingKnobs.Server.(*TestingKnobs); ok {
		rpcTestingKnobs = p.ContextTestingKnobs
	}
	rpcContext := rpc.NewContext(rpc.ContextOptions{
		AmbientCtx: baseCfg.AmbientCtx,
		Config:     baseCfg.Config,
		Clock:      clock,
		Stopper:    stopper,
		Settings:   st,
		Knobs:      rpcTestingKnobs,
	})

	// TODO(tbg): expose this registry via prometheus. See:
	// https://github.com/cockroachdb/cockroach/issues/47905
	registry := metric.NewRegistry()

	var dsKnobs kvcoord.ClientTestingKnobs
	if dsKnobsP, ok := baseCfg.TestingKnobs.DistSQL.(*kvcoord.ClientTestingKnobs); ok {
		dsKnobs = *dsKnobsP
	}
	rpcRetryOptions := base.DefaultRetryOptions()

	// TODO(nvb): this use of Gossip needs to go. Tracked in:
	// https://github.com/cockroachdb/cockroach/issues/47909
	var g *gossip.Gossip
	{
		var nodeID base.NodeIDContainer
		nodeID.Set(context.Background(), fakeNodeID)
		var clusterID base.ClusterIDContainer
		dummyGossipGRPC := rpc.NewServer(rpcContext) // never Serve()s anything
		g = gossip.New(
			baseCfg.AmbientCtx,
			&clusterID,
			&nodeID,
			rpcContext,
			dummyGossipGRPC,
			stopper,
			registry,
			baseCfg.Locality,
			&baseCfg.DefaultZoneConfig,
		)
	}

	nodeDialer := nodedialer.New(
		rpcContext,
		gossip.AddressResolver(g), // TODO(nvb): break gossip dep
	)
	dsCfg := kvcoord.DistSenderConfig{
		AmbientCtx:         baseCfg.AmbientCtx,
		Settings:           st,
		Clock:              clock,
		NodeDescs:          g,
		RPCRetryOptions:    &rpcRetryOptions,
		RPCContext:         rpcContext,
		NodeDialer:         nodeDialer,
		RangeDescriptorDB:  nil, // use DistSender itself
		FirstRangeProvider: g,
		TestingKnobs:       dsKnobs,
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
	db := kv.NewDB(
		baseCfg.AmbientCtx,
		tcsFactory,
		clock,
	)

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

	dummyRecorder := &status.MetricsRecorder{}

	var c base.NodeIDContainer
	c.Set(context.Background(), fakeNodeID)
	const sqlInstanceID = base.SQLInstanceID(10001)
	idContainer := base.NewSQLIDContainer(sqlInstanceID, &c, false /* exposed */)

	// We don't need this for anything except some services that want a gRPC
	// server to register against (but they'll never get RPCs at the time of
	// writing): the blob service and DistSQL.
	dummyRPCServer := rpc.NewServer(rpcContext)
	noStatusServer := serverpb.MakeOptionalStatusServer(nil)
	return sqlServerArgs{
		sqlServerOptionalArgs: sqlServerOptionalArgs{
			rpcContext:   rpcContext,
			distSender:   ds,
			statusServer: noStatusServer,
			nodeLiveness: sqlbase.MakeOptionalNodeLiveness(nil),
			gossip:       gossip.MakeUnexposedGossip(g),
			nodeDialer:   nodeDialer,
			grpcServer:   dummyRPCServer,
			recorder:     dummyRecorder,
			isMeta1Leaseholder: func(timestamp hlc.Timestamp) (bool, error) {
				return false, errors.New("isMeta1Leaseholder is not available to secondary tenants")
			},
			nodeIDContainer: idContainer,
			externalStorage: func(ctx context.Context, dest roachpb.ExternalStorage) (cloud.ExternalStorage, error) {
				return nil, errors.New("external storage is not available to secondary tenants")
			},
			externalStorageFromURI: func(ctx context.Context,
				uri, user string) (cloud.ExternalStorage, error) {
				return nil, errors.New("external uri storage is not available to secondary tenants")
			},
		},
		SQLConfig:                &sqlCfg,
		BaseConfig:               &baseCfg,
		stopper:                  stopper,
		clock:                    clock,
		runtime:                  status.NewRuntimeStatSampler(context.Background(), clock),
		db:                       db,
		registry:                 registry,
		sessionRegistry:          sql.NewSessionRegistry(),
		circularInternalExecutor: circularInternalExecutor,
		circularJobRegistry:      &jobs.Registry{},
		protectedtsProvider:      protectedTSProvider,
	}
}

// StartTenant starts a SQL tenant communicating with this TestServer.
func (ts *TestServer) StartTenant(params base.TestTenantArgs) (pgAddr string, _ error) {
	ctx := context.Background()

	if _, err := ts.InternalExecutor().(*sql.InternalExecutor).Exec(
		ctx, "testserver-create-tenant", nil /* txn */, fmt.Sprintf("SELECT crdb_internal.create_tenant(%d)", params.TenantID.ToUint64()),
	); err != nil {
		return "", err
	}

	st := cluster.MakeTestingClusterSettings()
	sqlCfg := makeTestSQLConfig(st, params.TenantID)
	baseCfg := makeTestBaseConfig(st)
	if params.AllowSettingClusterSettings {
		baseCfg.TestingKnobs.TenantTestingKnobs = &sql.TenantTestingKnobs{
			ClusterSettingsUpdater: st.MakeUpdater(),
		}
	}
	sqlCfg.TenantKVAddrs = []string{ts.RPCAddr()}
	return StartTenant(
		ctx,
		ts.Stopper(),
		ts.Cfg.ClusterName,
		baseCfg,
		sqlCfg,
	)
}

// StartTenant starts a stand-alone SQL server against a KV backend.
func StartTenant(
	ctx context.Context,
	stopper *stop.Stopper,
	kvClusterName string, // NB: gone after https://github.com/cockroachdb/cockroach/issues/42519
	baseCfg BaseConfig,
	sqlCfg SQLConfig,
) (pgAddr string, _ error) {
	args := makeSQLServerArgs(stopper, kvClusterName, baseCfg, sqlCfg)
	s, err := newSQLServer(ctx, args)
	if err != nil {
		return "", err
	}

	// NB: this should no longer be necessary after #47902. Right now it keeps
	// the tenant from crashing.
	//
	// NB: this NodeID is actually used by the DistSQL planner.
	s.execCfg.DistSQLPlanner.SetNodeInfo(roachpb.NodeDescriptor{NodeID: fakeNodeID})

	connManager := netutil.MakeServer(
		args.stopper,
		// The SQL server only uses connManager.ServeWith. The both below
		// are unused.
		nil, // tlsConfig
		nil, // handler
	)

	pgL, err := net.Listen("tcp", args.Config.SQLAddr)
	if err != nil {
		return "", err
	}
	args.stopper.RunWorker(ctx, func(ctx context.Context) {
		<-args.stopper.ShouldQuiesce()
		// NB: we can't do this as a Closer because (*Server).ServeWith is
		// running in a worker and usually sits on accept(pgL) which unblocks
		// only when pgL closes. In other words, pgL needs to close when
		// quiescing starts to allow that worker to shut down.
		_ = pgL.Close()
	})

	const (
		socketFile = "" // no unix socket
	)
	orphanedLeasesTimeThresholdNanos := args.clock.Now().WallTime

	{
		rs := make([]resolver.Resolver, len(sqlCfg.TenantKVAddrs))
		for i := range rs {
			var err error
			rs[i], err = resolver.NewResolver(sqlCfg.TenantKVAddrs[i])
			if err != nil {
				return "", err
			}
		}
		// NB: gossip server is not bound to any address, so the advertise addr does
		// not matter.
		args.gossip.Start(pgL.Addr(), rs)
	}

	if err := s.start(ctx,
		args.stopper,
		args.TestingKnobs,
		connManager,
		pgL,
		socketFile,
		orphanedLeasesTimeThresholdNanos,
	); err != nil {
		return "", err
	}
	return pgL.Addr().String(), nil
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
	if maxSystemDescriptorID < sqlbase.ID(keys.MaxPseudoTableID) {
		maxSystemDescriptorID = sqlbase.ID(keys.MaxPseudoTableID)
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

// DrainClients exports the drainClients() method for use by tests.
func (ts *TestServer) DrainClients(ctx context.Context) error {
	return ts.drainClients(ctx, nil /* reporter */)
}

// SQLAddr returns the server's listening SQL address.
// Note: use ServingSQLAddr() instead unless there is a specific reason not to.
func (ts *TestServer) SQLAddr() string {
	return ts.cfg.SQLAddr
}

// WriteSummaries implements TestServerInterface.
func (ts *TestServer) WriteSummaries() error {
	return ts.node.writeNodeStatus(context.TODO(), time.Hour)
}

// AdminURL implements TestServerInterface.
func (ts *TestServer) AdminURL() string {
	return ts.Cfg.AdminURL().String()
}

// GetHTTPClient implements TestServerInterface.
func (ts *TestServer) GetHTTPClient() (http.Client, error) {
	return ts.Server.rpcContext.GetHTTPClient()
}

const authenticatedUserName = "authentic_user"
const authenticatedUserNameNoAdmin = "authentic_user_noadmin"

// GetAdminAuthenticatedHTTPClient implements the TestServerInterface.
func (ts *TestServer) GetAdminAuthenticatedHTTPClient() (http.Client, error) {
	httpClient, _, err := ts.getAuthenticatedHTTPClientAndCookie(authenticatedUserName, true)
	return httpClient, err
}

// GetAuthenticatedHTTPClient implements the TestServerInterface.
func (ts *TestServer) GetAuthenticatedHTTPClient(isAdmin bool) (http.Client, error) {
	authUser := authenticatedUserName
	if !isAdmin {
		authUser = authenticatedUserNameNoAdmin
	}
	httpClient, _, err := ts.getAuthenticatedHTTPClientAndCookie(authUser, isAdmin)
	return httpClient, err
}

func (ts *TestServer) getAuthenticatedHTTPClientAndCookie(
	authUser string, isAdmin bool,
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
			authClient.httpClient.Jar = cookieJar
			authClient.cookie = rawCookie
			return nil
		}()
	})

	return authClient.httpClient, authClient.cookie, authClient.err
}

func (ts *TestServer) createAuthUser(userName string, isAdmin bool) error {
	if _, err := ts.Server.sqlServer.internalExecutor.ExecEx(context.TODO(),
		"create-auth-user", nil,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		"CREATE USER $1", userName,
	); err != nil {
		return err
	}
	if isAdmin {
		// We can't use the GRANT statement here because we don't want
		// to rely on CCL code.
		if _, err := ts.Server.sqlServer.internalExecutor.ExecEx(context.TODO(),
			"grant-admin", nil,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			"INSERT INTO system.role_members (role, member, \"isAdmin\") VALUES ('admin', $1, true)", userName,
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

// SplitRange splits the range containing splitKey.
// The right range created by the split starts at the split key and extends to the
// original range's end key.
// Returns the new descriptors of the left and right ranges.
//
// splitKey must correspond to a SQL table key (it must end with a family ID /
// col ID).
func (ts *TestServer) SplitRange(
	splitKey roachpb.Key,
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
		ExpirationTime: hlc.MaxTimestamp,
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

// GetRangeLease returns the current lease for the range containing key, and a
// timestamp taken from the node.
//
// The lease is returned regardless of its status.
func (ts *TestServer) GetRangeLease(
	ctx context.Context, key roachpb.Key,
) (_ roachpb.Lease, now hlc.Timestamp, _ error) {
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
		return roachpb.Lease{}, hlc.Timestamp{}, pErr.GoError()
	}
	return leaseResp.(*roachpb.LeaseInfoResponse).Lease, ts.Clock().Now(), nil

}

// ExecutorConfig is part of the TestServerInterface.
func (ts *TestServer) ExecutorConfig() interface{} {
	return *ts.sqlServer.execCfg
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
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
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

type testServerFactoryImpl struct{}

// TestServerFactory can be passed to serverutils.InitTestServerFactory
var TestServerFactory = testServerFactoryImpl{}

// New is part of TestServerFactory interface.
func (testServerFactoryImpl) New(params base.TestServerArgs) interface{} {
	cfg := makeTestConfigFromParams(params)
	return &TestServer{Cfg: &cfg}
}

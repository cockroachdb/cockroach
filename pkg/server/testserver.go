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
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/cenkalti/backoff"
	circuit "github.com/cockroachdb/circuitbreaker"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	addrutil "github.com/cockroachdb/cockroach/pkg/util/netutil/addr"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

// makeTestConfig returns a config for testing. It overrides the
// Certs with the test certs directory.
// We need to override the certs loader.
func makeTestConfig(st *cluster.Settings, tr *tracing.Tracer) Config {
	if tr == nil {
		panic("nil Tracer")
	}
	return Config{
		BaseConfig: makeTestBaseConfig(st, tr),
		KVConfig:   makeTestKVConfig(),
		SQLConfig:  makeTestSQLConfig(st, roachpb.SystemTenantID),
	}
}

func makeTestBaseConfig(st *cluster.Settings, tr *tracing.Tracer) BaseConfig {
	if tr == nil {
		panic("nil Tracer")
	}
	baseCfg := MakeBaseConfig(st, tr)
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
	// Enable web session authentication.
	baseCfg.EnableWebSessionAuthentication = true
	return baseCfg
}

func makeTestKVConfig() KVConfig {
	kvCfg := MakeKVConfig(base.DefaultTestStoreSpec)
	return kvCfg
}

func makeTestSQLConfig(st *cluster.Settings, tenID roachpb.TenantID) SQLConfig {
	return MakeSQLConfig(tenID, base.DefaultTestTempStorageConfig(st))
}

func initTraceDir(dir string) error {
	if dir == "" {
		return nil
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return errors.Wrap(err, "cannot create trace dir; traces will not be dumped")
	}
	return nil
}

// makeTestConfigFromParams creates a Config from a TestServerParams.
func makeTestConfigFromParams(params base.TestServerArgs) Config {
	st := params.Settings
	if params.Settings == nil {
		st = cluster.MakeClusterSettings()
	}
	st.ExternalIODir = params.ExternalIODir
	tr := params.Tracer
	if params.Tracer == nil {
		tr = tracing.NewTracerWithOpt(context.TODO(), tracing.WithClusterSettings(&st.SV), tracing.WithTracingMode(params.TracingDefault))
	}
	cfg := makeTestConfig(st, tr)
	cfg.TestingKnobs = params.Knobs
	cfg.RaftConfig = params.RaftConfig
	cfg.RaftConfig.SetDefaults()
	if params.JoinAddr != "" {
		cfg.JoinList = []string{params.JoinAddr}
	}
	cfg.ClusterName = params.ClusterName
	cfg.ExternalIODirConfig = params.ExternalIODirConfig
	cfg.Insecure = params.Insecure
	cfg.AutoInitializeCluster = !params.NoAutoInitializeCluster
	cfg.SocketFile = params.SocketFile
	cfg.RetryOptions = params.RetryOptions
	cfg.Locality = params.Locality
	if params.TraceDir != "" {
		if err := initTraceDir(params.TraceDir); err == nil {
			cfg.InflightTraceDirName = params.TraceDir
		}
	}
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
	if params.DisableSpanConfigs {
		cfg.SpanConfigsDisabled = true
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
			if cfg.InflightTraceDirName == "" {
				cfg.InflightTraceDirName = filepath.Join(storeSpec.Path, "logs", base.InflightTraceDir)
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

	if params.Knobs.AdmissionControl == nil {
		cfg.TestingKnobs.AdmissionControl = &admission.Options{}
	}

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
	// httpTestServer provides the HTTP APIs of TestTenantInterface.
	*httpTestServer
}

var _ serverutils.TestServerInterface = &TestServer{}

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
		return ts.sqlServer.execCfg.SQLLiveness
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

// StartupMigrationsManager returns the *startupmigrations.Manager as an interface{}.
func (ts *TestServer) StartupMigrationsManager() interface{} {
	if ts != nil {
		return ts.sqlServer.startupMigrationsMgr
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

// NodeDialer returns the NodeDialer used by the TestServer.
func (ts *TestServer) NodeDialer() interface{} {
	if ts != nil {
		return ts.nodeDialer
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

// SQLInstanceID is part of TestServerInterface.
func (ts *TestServer) SQLInstanceID() base.SQLInstanceID {
	return ts.sqlServer.sqlIDContainer.SQLInstanceID()
}

// StatusServer is part of TestServerInterface.
func (ts *TestServer) StatusServer() interface{} {
	return ts.status
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

// PGServer exposes the pgwire.Server instance used by the TestServer as an
// interface{}.
func (ts *TestServer) PGServer() interface{} {
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

// AmbientCtx implements serverutils.TestTenantInterface. This
// retrieves the ambient context for this server. This is intended for
// exclusive use by test code.
func (ts *TestServer) AmbientCtx() log.AmbientContext {
	return ts.Cfg.AmbientCtx
}

// TestingKnobs returns the TestingKnobs used by the TestServer.
func (ts *TestServer) TestingKnobs() *base.TestingKnobs {
	if ts != nil {
		return &ts.Cfg.TestingKnobs
	}
	return nil
}

// TenantStatusServer returns the TenantStatusServer used by the TestServer.
func (ts *TestServer) TenantStatusServer() interface{} {
	return ts.status
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

type tenantProtectedTSProvider struct {
	protectedts.Provider
	st *cluster.Settings
}

func (d tenantProtectedTSProvider) Protect(
	ctx context.Context, txn *kv.Txn, rec *ptpb.Record,
) error {
	if !d.st.Version.IsActive(ctx, clusterversion.EnableProtectedTimestampsForTenant) {
		return errors.Newf("%s is inactive, tenant cannot write protected timestamp records",
			clusterversion.EnableProtectedTimestampsForTenant.String())
	}
	return d.Provider.Protect(ctx, txn, rec)
}

// TestTenant is an in-memory instantiation of the SQL-only process created for
// each active Cockroach tenant. TestTenant provides tests with access to
// internal methods and state on SQLServer. It is typically started in tests by
// calling the TestServerInterface.StartTenant method or by calling the wrapper
// serverutils.StartTenant method.
type TestTenant struct {
	*SQLServer
	Cfg      *BaseConfig
	sqlAddr  string
	httpAddr string
	*httpTestServer
	drain *drainServer
}

var _ serverutils.TestTenantInterface = &TestTenant{}

// SQLAddr is part of TestTenantInterface interface.
func (t *TestTenant) SQLAddr() string {
	return t.sqlAddr
}

// HTTPAddr is part of TestTenantInterface interface.
func (t *TestTenant) HTTPAddr() string {
	return t.httpAddr
}

// RPCAddr is part of the TestTenantInterface interface.
func (t *TestTenant) RPCAddr() string {
	// The RPC and SQL functionality for tenants is multiplexed
	// on the same address. Having a separate interface to access
	// for the two addresses makes it easier to distinguish
	// the use case for which the address is being used.
	// This also provides parity between SQL only servers and
	// regular servers.
	return t.sqlAddr
}

// PGServer is part of TestTenantInterface.
func (t *TestTenant) PGServer() interface{} {
	return t.pgServer
}

// DiagnosticsReporter is part of TestTenantInterface.
func (t *TestTenant) DiagnosticsReporter() interface{} {
	return t.diagnosticsReporter
}

// StatusServer is part of TestTenantInterface.
func (t *TestTenant) StatusServer() interface{} {
	return t.execCfg.SQLStatusServer
}

// TenantStatusServer is part of TestTenantInterface.
func (t *TestTenant) TenantStatusServer() interface{} {
	return t.execCfg.TenantStatusServer
}

// DistSQLServer is part of TestTenantInterface.
func (t *TestTenant) DistSQLServer() interface{} {
	return t.SQLServer.distSQLServer
}

// RPCContext is part of TestTenantInterface.
func (t *TestTenant) RPCContext() *rpc.Context {
	return t.execCfg.RPCContext
}

// JobRegistry is part of TestTenantInterface.
func (t *TestTenant) JobRegistry() interface{} {
	return t.SQLServer.jobRegistry
}

// ExecutorConfig is part of TestTenantInterface.
func (t *TestTenant) ExecutorConfig() interface{} {
	return *t.SQLServer.execCfg
}

// RangeFeedFactory is part of TestTenantInterface.
func (t *TestTenant) RangeFeedFactory() interface{} {
	return t.SQLServer.execCfg.RangeFeedFactory
}

// ClusterSettings is part of TestTenantInterface.
func (t *TestTenant) ClusterSettings() *cluster.Settings {
	return t.Cfg.Settings
}

// Stopper is part of TestTenantInterface.
func (t *TestTenant) Stopper() *stop.Stopper {
	return t.stopper
}

// Clock is part of TestTenantInterface.
func (t *TestTenant) Clock() *hlc.Clock {
	return t.SQLServer.execCfg.Clock
}

// AmbientCtx implements serverutils.TestTenantInterface. This
// retrieves the ambient context for this server. This is intended for
// exclusive use by test code.
func (t *TestTenant) AmbientCtx() log.AmbientContext {
	return t.Cfg.AmbientCtx
}

// TestingKnobs is part TestTenantInterface.
func (t *TestTenant) TestingKnobs() *base.TestingKnobs {
	return &t.Cfg.TestingKnobs
}

// SpanConfigKVAccessor is part TestTenantInterface.
func (t *TestTenant) SpanConfigKVAccessor() interface{} {
	return t.SQLServer.tenantConnect
}

// SpanConfigReconciler is part TestTenantInterface.
func (t *TestTenant) SpanConfigReconciler() interface{} {
	return t.SQLServer.spanconfigMgr.Reconciler
}

// SpanConfigSQLTranslator is part TestTenantInterface.
func (t *TestTenant) SpanConfigSQLTranslator() interface{} {
	return t.SQLServer.spanconfigSQLTranslator
}

// SpanConfigSQLWatcher is part TestTenantInterface.
func (t *TestTenant) SpanConfigSQLWatcher() interface{} {
	return t.SQLServer.spanconfigSQLWatcher
}

// SystemConfigProvider is part TestTenantInterface.
func (t *TestTenant) SystemConfigProvider() config.SystemConfigProvider {
	return t.SQLServer.systemConfigWatcher
}

// DrainClients exports the drainClients() method for use by tests.
func (t *TestTenant) DrainClients(ctx context.Context) error {
	return t.drain.drainClients(ctx, nil /* reporter */)
}

// StartTenant starts a SQL tenant communicating with this TestServer.
func (ts *TestServer) StartTenant(
	ctx context.Context, params base.TestTenantArgs,
) (serverutils.TestTenantInterface, error) {
	if !params.Existing {
		if _, err := ts.InternalExecutor().(*sql.InternalExecutor).Exec(
			ctx, "testserver-create-tenant", nil /* txn */, "SELECT crdb_internal.create_tenant($1)", params.TenantID.ToUint64(),
		); err != nil {
			return nil, err
		}
	}

	if !params.SkipTenantCheck {
		rowCount, err := ts.InternalExecutor().(*sql.InternalExecutor).Exec(
			ctx, "testserver-check-tenant-active", nil,
			"SELECT 1 FROM system.tenants WHERE id=$1 AND active=true",
			params.TenantID.ToUint64(),
		)

		if err != nil {
			return nil, err
		}
		if rowCount == 0 {
			return nil, errors.New("not found")
		}
	}
	st := params.Settings
	if st == nil {
		st = cluster.MakeTestingClusterSettings()
	}

	st.ExternalIODir = params.ExternalIODir
	sqlCfg := makeTestSQLConfig(st, params.TenantID)
	sqlCfg.TenantKVAddrs = []string{ts.ServingRPCAddr()}
	sqlCfg.ExternalIODirConfig = params.ExternalIODirConfig
	if params.MemoryPoolSize != 0 {
		sqlCfg.MemoryPoolSize = params.MemoryPoolSize
	}
	if params.TempStorageConfig != nil {
		sqlCfg.TempStorageConfig = *params.TempStorageConfig
	}

	stopper := params.Stopper
	if stopper == nil {
		// We don't share the stopper with the server because we want their Tracers
		// to be different, to simulate them being different processes.
		tr := tracing.NewTracerWithOpt(ctx, tracing.WithClusterSettings(&st.SV), tracing.WithTracingMode(params.TracingDefault))
		stopper = stop.NewStopper(stop.WithTracer(tr))
		// The server's stopper stops the tenant, for convenience.
		ts.Stopper().AddCloser(stop.CloserFn(func() { stopper.Stop(context.Background()) }))
	} else if stopper.Tracer() == nil {
		tr := tracing.NewTracerWithOpt(ctx, tracing.WithClusterSettings(&st.SV), tracing.WithTracingMode(params.TracingDefault))
		stopper.SetTracer(tr)
	}

	baseCfg := makeTestBaseConfig(st, stopper.Tracer())
	baseCfg.TestingKnobs = params.TestingKnobs
	baseCfg.Insecure = params.ForceInsecure
	baseCfg.Locality = params.Locality
	baseCfg.HeapProfileDirName = params.HeapProfileDirName
	baseCfg.GoroutineDumpDirName = params.GoroutineDumpDirName
	if params.SSLCertsDir != "" {
		baseCfg.SSLCertsDir = params.SSLCertsDir
	}
	if params.StartingSQLPort > 0 {
		addr, _, err := addrutil.SplitHostPort(baseCfg.SQLAddr, strconv.Itoa(params.StartingSQLPort))
		if err != nil {
			return nil, err
		}
		newAddr := net.JoinHostPort(addr, strconv.Itoa(params.StartingSQLPort+int(params.TenantID.ToUint64())))
		baseCfg.SQLAddr = newAddr
		baseCfg.SQLAdvertiseAddr = newAddr
	}
	if params.StartingHTTPPort > 0 {
		addr, _, err := addrutil.SplitHostPort(baseCfg.SQLAddr, strconv.Itoa(params.StartingHTTPPort))
		if err != nil {
			return nil, err
		}
		newAddr := net.JoinHostPort(addr, strconv.Itoa(params.StartingHTTPPort+int(params.TenantID.ToUint64())))
		baseCfg.HTTPAddr = newAddr
		baseCfg.HTTPAdvertiseAddr = newAddr
	}
	if params.AllowSettingClusterSettings {
		tenantKnobs, ok := baseCfg.TestingKnobs.TenantTestingKnobs.(*sql.TenantTestingKnobs)
		if !ok {
			tenantKnobs = &sql.TenantTestingKnobs{}
			baseCfg.TestingKnobs.TenantTestingKnobs = tenantKnobs
		}
		if tenantKnobs.ClusterSettingsUpdater == nil {
			tenantKnobs.ClusterSettingsUpdater = st.MakeUpdater()
		}
	}
	if params.RPCHeartbeatInterval != 0 {
		baseCfg.RPCHeartbeatInterval = params.RPCHeartbeatInterval
	}
	sqlServer, authServer, drainServer, addr, httpAddr, err := startTenantInternal(
		ctx,
		stopper,
		ts.Cfg.ClusterName,
		baseCfg,
		sqlCfg,
	)
	if err != nil {
		return nil, err
	}

	hts := &httpTestServer{}
	hts.t.authentication = authServer
	hts.t.sqlServer = sqlServer

	return &TestTenant{
		SQLServer:      sqlServer,
		Cfg:            &baseCfg,
		sqlAddr:        addr,
		httpAddr:       httpAddr,
		httpTestServer: hts,
		drain:          drainServer,
	}, err
}

// ExpectedInitialRangeCount returns the expected number of ranges that should
// be on the server after initial (asynchronous) splits have been completed,
// assuming no additional information is added outside of the normal bootstrap
// process.
func (ts *TestServer) ExpectedInitialRangeCount() (int, error) {
	return ExpectedInitialRangeCount(
		ts.sqlServer.execCfg.Codec,
		&ts.cfg.DefaultZoneConfig,
		&ts.cfg.DefaultSystemZoneConfig,
	)
}

// ExpectedInitialRangeCount returns the expected number of ranges that should
// be on the server after bootstrap.
func ExpectedInitialRangeCount(
	codec keys.SQLCodec,
	defaultZoneConfig *zonepb.ZoneConfig,
	defaultSystemZoneConfig *zonepb.ZoneConfig,
) (int, error) {
	_, splits := bootstrap.MakeMetadataSchema(codec, defaultZoneConfig, defaultSystemZoneConfig).GetInitialValues()
	// N splits means N+1 ranges.
	return len(config.StaticSplits()) + len(splits) + 1, nil
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
	return ts.drain.drainClients(ctx, nil /* reporter */)
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

type v2AuthDecorator struct {
	http.RoundTripper

	session string
}

func (v *v2AuthDecorator) RoundTrip(r *http.Request) (*http.Response, error) {
	r.Header.Add(apiV2AuthHeader, v.session)
	return v.RoundTripper.RoundTrip(r)
}

// MustGetSQLCounter implements TestServerInterface.
func (ts *TestServer) MustGetSQLCounter(name string) int64 {
	var c int64
	var found bool

	type (
		int64Valuer  interface{ Value() int64 }
		int64Counter interface{ Count() int64 }
	)

	ts.registry.Each(func(n string, v interface{}) {
		if name == n {
			switch t := v.(type) {
			case *metric.Counter:
				c = t.Count()
				found = true
			case *metric.Gauge:
				c = t.Value()
				found = true
			case int64Valuer:
				c = t.Value()
				found = true
			case int64Counter:
				c = t.Count()
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

// Locality returns the Locality used by the TestServer.
func (ts *TestServer) Locality() *roachpb.Locality {
	return &ts.cfg.Locality
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

// DistSenderI is part of DistSenderInterface.
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

// SpanConfigKVAccessor is part of TestServerInterface.
func (ts *TestServer) SpanConfigKVAccessor() interface{} {
	return ts.Server.node.spanConfigAccessor
}

// SpanConfigReconciler is part of TestServerInterface.
func (ts *TestServer) SpanConfigReconciler() interface{} {
	if ts.sqlServer.spanconfigMgr == nil {
		panic("uninitialized; see EnableSpanConfigs testing knob to use span configs")
	}
	return ts.sqlServer.spanconfigMgr.Reconciler
}

// SpanConfigSQLTranslator is part of TestServerInterface.
func (ts *TestServer) SpanConfigSQLTranslator() interface{} {
	if ts.sqlServer.spanconfigSQLTranslator == nil {
		panic("uninitialized; see EnableSpanConfigs testing knob to use span configs")
	}
	return ts.sqlServer.spanconfigSQLTranslator
}

// SpanConfigSQLWatcher is part of TestServerInterface.
func (ts *TestServer) SpanConfigSQLWatcher() interface{} {
	if ts.sqlServer.spanconfigSQLWatcher == nil {
		panic("uninitialized; see EnableSpanConfigs testing knob to use span configs")
	}
	return ts.sqlServer.spanconfigSQLWatcher
}

// SQLServer is part of TestServerInterface.
func (ts *TestServer) SQLServer() interface{} {
	return ts.sqlServer.pgServer.SQLServer
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
		return roachpb.RangeDescriptor{}, errors.Wrapf(
			err, "%q: lookup range unexpected error", key)
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
		leftRangeDesc, rightRangeDesc = roachpb.RangeDescriptor{}, roachpb.RangeDescriptor{}

		// Discovering the RHS is easy, but the LHS is more difficult. The easiest way to
		// get both in one operation is to do a reverse range lookup on splitKey.Next();
		// we need the .Next() because in reverse mode, the end key of a range is inclusive,
		// i.e. looking up key `c` will match range [a,c), not [c, d).
		// The result will be the right descriptor, and the first prefetched result will
		// be the left neighbor, i.e. the resulting left hand side of the split.
		rs, more, err := kv.RangeLookup(ctx, txn, splitKey.Next(), roachpb.CONSISTENT, 1, true /* reverse */)
		if err != nil {
			return err
		}
		if len(rs) == 0 {
			// This is a bug.
			return errors.AssertionFailedf("no descriptor found for key %s", splitKey)
		}
		if len(more) == 0 {
			return errors.Errorf("looking up post-split descriptor returned first range: %+v", rs[0])
		}
		leftRangeDesc = more[0]
		rightRangeDesc = rs[0]

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

// LeaseInfo describes a range's current and potentially future lease.
type LeaseInfo struct {
	cur, next roachpb.Lease
}

// Current returns the range's current lease.
func (l LeaseInfo) Current() roachpb.Lease {
	return l.cur
}

// CurrentOrProspective returns the range's potential next lease, if a lease
// request is in progress, or the current lease otherwise.
func (l LeaseInfo) CurrentOrProspective() roachpb.Lease {
	if !l.next.Empty() {
		return l.next
	}
	return l.cur
}

// LeaseInfoOpt enumerates options for GetRangeLease.
type LeaseInfoOpt int

const (
	// AllowQueryToBeForwardedToDifferentNode specifies that, if the current node
	// doesn't have a voter replica, the lease info can come from a different
	// node.
	AllowQueryToBeForwardedToDifferentNode LeaseInfoOpt = iota
	// QueryLocalNodeOnly specifies that an error should be returned if the node
	// is not able to serve the lease query (because it doesn't have a voting
	// replica).
	QueryLocalNodeOnly
)

// GetRangeLease returns information on the lease for the range containing key, and a
// timestamp taken from the node. The lease is returned regardless of its status.
//
// queryPolicy specifies if its OK to forward the request to a different node.
func (ts *TestServer) GetRangeLease(
	ctx context.Context, key roachpb.Key, queryPolicy LeaseInfoOpt,
) (_ LeaseInfo, now hlc.ClockTimestamp, _ error) {
	leaseReq := roachpb.LeaseInfoRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
	}
	leaseResp, pErr := kv.SendWrappedWith(
		ctx,
		ts.DB().NonTransactionalSender(),
		roachpb.Header{
			// INCONSISTENT read with a NEAREST routing policy, since we want to make
			// sure that the node used to send this is the one that processes the
			// command, regardless of whether it is the leaseholder, for the hint to
			// matter.
			ReadConsistency: roachpb.INCONSISTENT,
			RoutingPolicy:   roachpb.RoutingPolicy_NEAREST,
		},
		&leaseReq,
	)
	if pErr != nil {
		return LeaseInfo{}, hlc.ClockTimestamp{}, pErr.GoError()
	}
	// Adapt the LeaseInfoResponse format to LeaseInfo.
	resp := leaseResp.(*roachpb.LeaseInfoResponse)
	if queryPolicy == QueryLocalNodeOnly && resp.EvaluatedBy != ts.GetFirstStoreID() {
		// TODO(andrei): Figure out how to deal with nodes with multiple stores.
		// This API should permit addressing the query to a particular store.
		return LeaseInfo{}, hlc.ClockTimestamp{}, errors.Errorf(
			"request not evaluated locally; evaluated by s%d instead of local s%d",
			resp.EvaluatedBy, ts.GetFirstStoreID())
	}
	var l LeaseInfo
	if resp.CurrentLease != nil {
		l.cur = *resp.CurrentLease
		l.next = resp.Lease
	} else {
		l.cur = resp.Lease
	}
	return l, ts.Clock().NowAsClockTimestamp(), nil
}

// ExecutorConfig is part of the TestServerInterface.
func (ts *TestServer) ExecutorConfig() interface{} {
	return *ts.sqlServer.execCfg
}

// TracerI is part of the TestServerInterface.
func (ts *TestServer) TracerI() interface{} {
	return ts.Tracer()
}

// Tracer is like TracerI(), but returns the actual type.
func (ts *TestServer) Tracer() *tracing.Tracer {
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
	scratchKey := keys.ScratchRangeMin
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

// CollectionFactory is part of the TestServerInterface.
func (ts *TestServer) CollectionFactory() interface{} {
	return ts.sqlServer.execCfg.CollectionFactory
}

// SystemTableIDResolver is part of the TestServerInterface.
func (ts *TestServer) SystemTableIDResolver() interface{} {
	return ts.sqlServer.execCfg.SystemTableIDResolver
}

// SpanConfigKVSubscriber is part of the TestServerInterface.
func (ts *TestServer) SpanConfigKVSubscriber() interface{} {
	return ts.node.storeCfg.SpanConfigSubscriber
}

// SystemConfigProvider is part of the TestServerInterface.
func (ts *TestServer) SystemConfigProvider() config.SystemConfigProvider {
	return ts.node.storeCfg.SystemConfigProvider
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

	// The HTTP APIs on TestTenantInterface are implemented by
	// httpTestServer.
	ts.httpTestServer = &httpTestServer{}
	ts.httpTestServer.t.authentication = ts.Server.authentication
	ts.httpTestServer.t.sqlServer = ts.Server.sqlServer

	return ts, nil
}

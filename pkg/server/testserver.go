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
	gosql "database/sql"
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
	"github.com/cockroachdb/cockroach/pkg/base/serverident"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvprober"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/plan"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security/certnames"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/deprecatedshowranges"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	addrutil "github.com/cockroachdb/cockroach/pkg/util/netutil/addr"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc"
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
	baseCfg := MakeBaseConfig(st, tr, base.DefaultTestStoreSpec)
	// Test servers start in secure mode by default.
	baseCfg.Insecure = false
	// Configure test storage engine.
	baseCfg.StorageEngine = storage.DefaultStorageEngine
	// Load test certs. In addition, the tests requiring certs
	// need to call securityassets.SetLoader(securitytest.EmbeddedAssets)
	// in their init to mock out the file system calls for calls to AssetFS,
	// which has the test certs compiled in. Typically this is done
	// once per package, in main_test.go.
	baseCfg.SSLCertsDir = certnames.EmbeddedCertsDir
	// Addr defaults to localhost with port set at time of call to
	// Start() to an available port. May be overridden later (as in
	// makeTestConfigFromParams). Call testServer.AdvRPCAddr() and
	// .AdvSQLAddr() for the full address (including bound port).
	baseCfg.Addr = util.TestAddr.String()
	baseCfg.AdvertiseAddr = util.TestAddr.String()
	baseCfg.SQLAddr = util.TestAddr.String()
	baseCfg.SQLAdvertiseAddr = util.TestAddr.String()
	baseCfg.SplitListenSQL = true
	baseCfg.HTTPAddr = util.TestAddr.String()
	// Set standard user for intra-cluster traffic.
	baseCfg.User = username.NodeUserName()
	return baseCfg
}

func makeTestKVConfig() KVConfig {
	kvCfg := MakeKVConfig()
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

	// Needed for backward-compat on crdb_internal.ranges{_no_leases}.
	// Remove in v23.2.
	deprecatedshowranges.ShowRangesDeprecatedBehaviorSetting.Override(
		context.TODO(), &st.SV,
		// In unit tests, we exercise the new behavior.
		false)

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
	cfg.StartDiagnosticsReporting = params.StartDiagnosticsReporting
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
	if params.SecondaryTenantPortOffset != 0 {
		cfg.SecondaryTenantPortOffset = params.SecondaryTenantPortOffset
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
	cfg.TestingInsecureWebAccess = params.InsecureWebAccess
	if params.EnableDemoLoginEndpoint {
		cfg.EnableDemoLoginEndpoint = true
	}
	if params.SnapshotApplyLimit != 0 {
		cfg.SnapshotApplyLimit = params.SnapshotApplyLimit
	}
	if params.SnapshotSendLimit != 0 {
		cfg.SnapshotSendLimit = params.SnapshotSendLimit
	}
	if params.AutoConfigProvider != nil {
		cfg.AutoConfigProvider = params.AutoConfigProvider
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
			// testServer).

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
			if cfg.CPUProfileDirName == "" {
				cfg.CPUProfileDirName = filepath.Join(storeSpec.Path, "logs", base.CPUProfileDir)
			}
		}
	}
	cfg.Stores = base.StoreSpecList{Specs: params.StoreSpecs}
	if params.TempStorageConfig.InMemory || params.TempStorageConfig.Path != "" {
		cfg.TempStorageConfig = params.TempStorageConfig
		cfg.TempStorageConfig.Settings = st
	}

	// TODO(#76378): Review this assignment to ensure it does not interfere with randomization.
	cfg.DisableDefaultTestTenant = params.DefaultTestTenant.TestTenantAlwaysDisabled()

	if cfg.TestingKnobs.Store == nil {
		cfg.TestingKnobs.Store = &kvserver.StoreTestingKnobs{}
	}
	cfg.TestingKnobs.Store.(*kvserver.StoreTestingKnobs).SkipMinSizeCheck = true

	if params.Knobs.SQLExecutor == nil {
		cfg.TestingKnobs.SQLExecutor = &sql.ExecutorTestingKnobs{}
	}

	if params.Knobs.AdmissionControlOptions == nil {
		cfg.TestingKnobs.AdmissionControlOptions = &admission.Options{}
	}

	cfg.ObsServiceAddr = params.ObsServiceAddr

	return cfg
}

// A testServer encapsulates an in-memory instantiation of a cockroach node with
// a single store. It provides tests with access to Server internals.
// Where possible, it should be used through the
// serverutils.TestServerInterface.
//
// Example usage of a testServer:
//
//	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
//	defer s.Stopper().Stop()
type testServer struct {
	Cfg    *Config
	params base.TestServerArgs
	// server is the embedded Cockroach server struct.
	*topLevelServer
	// httpTestServer provides the HTTP APIs of the
	// serverutils.ApplicationLayerInterface.
	*httpTestServer
	// The test tenants associated with this server, and used for probabilistic
	// testing within tenants. Currently, there is only one test tenant created
	// by default, but longer term we may allow for the creation of multiple
	// test tenants for more advanced testing.
	testTenants []serverutils.ApplicationLayerInterface
	// disableStartTenantError is set to an error if the test server should
	// prevent starting any tenants manually. This is used to prevent tests that
	// have not explicitly disabled probabilistic testing, or opted in to it, from
	// starting a tenant to avoid unexpected behavior.
	disableStartTenantError error
}

var _ serverutils.TestServerInterface = &testServer{}

// Node returns the Node as an interface{}.
func (ts *testServer) Node() interface{} {
	return ts.node
}

// NodeID returns the ID of this node within its cluster.
func (ts *testServer) NodeID() roachpb.NodeID {
	return ts.rpcContext.NodeID.Get()
}

// Stopper returns the embedded server's Stopper.
func (ts *testServer) Stopper() *stop.Stopper {
	return ts.stopper
}

// GossipI is part of the serverutils.StorageLayerInterface.
func (ts *testServer) GossipI() interface{} {
	return ts.topLevelServer.gossip
}

// RangeFeedFactory is part of serverutils.ApplicationLayerInterface.
func (ts *testServer) RangeFeedFactory() interface{} {
	if ts != nil {
		return ts.sqlServer.execCfg.RangeFeedFactory
	}
	return (*rangefeed.Factory)(nil)
}

// Clock returns the clock used by the testServer.
func (ts *testServer) Clock() *hlc.Clock {
	if ts != nil {
		return ts.clock
	}
	return nil
}

// SQLLivenessProvider returns the sqlliveness.Provider as an interface{}.
func (ts *testServer) SQLLivenessProvider() interface{} {
	if ts != nil {
		return ts.sqlServer.execCfg.SQLLiveness
	}
	return nil
}

// JobRegistry returns the *jobs.Registry as an interface{}.
func (ts *testServer) JobRegistry() interface{} {
	if ts != nil {
		return ts.sqlServer.jobRegistry
	}
	return nil
}

// NodeLiveness exposes the NodeLiveness instance used by the testServer as an
// interface{}.
func (ts *testServer) NodeLiveness() interface{} {
	if ts != nil {
		return ts.nodeLiveness
	}
	return nil
}

// NodeDialer returns the NodeDialer used by the testServer.
func (ts *testServer) NodeDialer() interface{} {
	if ts != nil {
		return ts.nodeDialer
	}
	return nil
}

// HeartbeatNodeLiveness heartbeats the server's NodeLiveness record.
func (ts *testServer) HeartbeatNodeLiveness() error {
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

// SQLInstanceID is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) SQLInstanceID() base.SQLInstanceID {
	return ts.sqlServer.sqlIDContainer.SQLInstanceID()
}

// StatusServer is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) StatusServer() interface{} {
	return ts.status
}

// RPCContext returns the rpc context used by the testServer.
func (ts *testServer) RPCContext() *rpc.Context {
	if ts != nil {
		return ts.rpcContext
	}
	return nil
}

// TsDB returns the ts.DB instance used by the testServer.
func (ts *testServer) TsDB() interface{} {
	return ts.tsDB
}

// SQLConn is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) SQLConn(test serverutils.TestFataler, dbName string) *gosql.DB {
	return ts.SQLConnForUser(test, username.RootUser, dbName)
}

// SQLConnForUser is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) SQLConnForUser(
	test serverutils.TestFataler, userName, dbName string,
) *gosql.DB {
	db, err := ts.SQLConnForUserE(userName, dbName)
	if err != nil {
		test.Fatal(err)
	}
	return db
}

// SQLConnE is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) SQLConnE(dbName string) (*gosql.DB, error) {
	return ts.SQLConnForUserE(username.RootUser, dbName)
}

// SQLConnForUserE is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) SQLConnForUserE(userName string, dbName string) (*gosql.DB, error) {
	return openTestSQLConn(userName, dbName, ts.Stopper(),
		ts.topLevelServer.loopbackPgL,
		ts.cfg.SQLAdvertiseAddr,
		ts.cfg.Insecure,
	)
}

// DB returns the client.DB instance used by the testServer.
func (ts *testServer) DB() *kv.DB {
	if ts != nil {
		return ts.db
	}
	return nil
}

// PGServer exposes the pgwire.Server instance used by the testServer as an
// interface{}.
func (ts *testServer) PGServer() interface{} {
	if ts != nil {
		return ts.sqlServer.pgServer
	}
	return nil
}

// PGPreServer exposes the pgwire.PreServeConnHandler instance used by
// the testServer.
func (ts *testServer) PGPreServer() interface{} {
	if ts != nil {
		return ts.pgPreServer
	}
	return nil
}

// RaftTransport is part of the serverutils.StorageLayerInterface.
func (ts *testServer) RaftTransport() interface{} {
	if ts != nil {
		return ts.raftTransport
	}
	return nil
}

// AmbientCtx implements serverutils.ApplicationLayerInterface. This
// retrieves the ambient context for this server. This is intended for
// exclusive use by test code.
func (ts *testServer) AmbientCtx() log.AmbientContext {
	return ts.Cfg.AmbientCtx
}

// TestingKnobs returns the TestingKnobs used by the testServer.
func (ts *testServer) TestingKnobs() *base.TestingKnobs {
	if ts != nil {
		return &ts.Cfg.TestingKnobs
	}
	return nil
}

// SQLServerInternal is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) SQLServerInternal() interface{} {
	return ts.sqlServer
}

// TenantStatusServer returns the TenantStatusServer used by the testServer.
func (ts *testServer) TenantStatusServer() interface{} {
	return ts.status
}

// TestTenants provides information to tenant(s) that _may_ have been created
func (ts *testServer) TestTenants() []serverutils.ApplicationLayerInterface {
	return ts.testTenants
}

// DefaultTestTenantDisabled is part of the serverutils.TenantControlInterface.
func (ts *testServer) DefaultTestTenantDisabled() bool {
	return ts.cfg.DisableDefaultTestTenant
}

// DisableDefaultTestTenant is part of the serverutils.TenantControlInterface.
func (ts *testServer) DisableDefaultTestTenant() {
	ts.cfg.DisableDefaultTestTenant = true
}

// maybeStartDefaultTestTenant might start a test tenant. This can then be used
// for multi-tenant testing, where the default SQL connection will be made to
// this tenant instead of to the system tenant. Note that we will
// currently only attempt to start a test tenant if we're running in an
// enterprise enabled build. This is due to licensing restrictions on the MT
// capabilities.
func (ts *testServer) maybeStartDefaultTestTenant(ctx context.Context) error {
	// If the flag has been set to disable the default test tenant, don't start
	// it here.
	if ts.params.DefaultTestTenant.TestTenantAlwaysDisabled() || ts.cfg.DisableDefaultTestTenant {
		return nil
	}

	clusterID := ts.sqlServer.execCfg.NodeInfo.LogicalClusterID
	if err := base.CheckEnterpriseEnabled(ts.st, clusterID(), "SQL servers"); err != nil {
		log.Shoutf(ctx, severity.WARNING, "test tenant requested by configuration, but code organization prevents start!\n%v", err)
		// If not enterprise enabled, we won't be able to use SQL Servers so eat
		// the error and return without creating/starting a SQL server.
		ts.cfg.DisableDefaultTestTenant = true
		return nil // nolint:returnerrcheck
	}

	tempStorageConfig := base.DefaultTestTempStorageConfig(cluster.MakeTestingClusterSettings())
	params := base.TestTenantArgs{
		// Currently, all the servers leverage the same tenant ID. We may
		// want to change this down the road, for more elaborate testing.
		TenantID:                  serverutils.TestTenantID(),
		MemoryPoolSize:            ts.params.SQLMemoryPoolSize,
		TempStorageConfig:         &tempStorageConfig,
		Locality:                  ts.params.Locality,
		ExternalIODir:             ts.params.ExternalIODir,
		ExternalIODirConfig:       ts.params.ExternalIODirConfig,
		ForceInsecure:             ts.Insecure(),
		UseDatabase:               ts.params.UseDatabase,
		SSLCertsDir:               ts.params.SSLCertsDir,
		TestingKnobs:              ts.params.Knobs,
		StartDiagnosticsReporting: ts.params.StartDiagnosticsReporting,
		Settings:                  ts.params.Settings,
	}

	// Since we're creating a tenant, it doesn't make sense to pass through the
	// Server testing knobs, since the bulk of them only apply to the system
	// tenant. Any remaining knobs which are required by the tenant should be
	// passed through here.
	params.TestingKnobs.Server = &TestingKnobs{}

	if ts.params.Knobs.Server != nil {
		params.TestingKnobs.Server.(*TestingKnobs).DiagnosticsTestingKnobs = ts.params.Knobs.Server.(*TestingKnobs).DiagnosticsTestingKnobs
	}

	// Temporarily disable the error that is returned if a tenant should not be started manually,
	// so that we can start the default test tenant internally here.
	disableStartTenantError := ts.disableStartTenantError
	if ts.disableStartTenantError != nil {
		ts.disableStartTenantError = nil
	}
	defer func() {
		if disableStartTenantError != nil {
			ts.disableStartTenantError = disableStartTenantError
		}
	}()

	tenant, err := ts.StartTenant(ctx, params)
	if err != nil {
		return err
	}

	if len(ts.testTenants) == 0 {
		ts.testTenants = make([]serverutils.ApplicationLayerInterface, 1)
		ts.testTenants[0] = tenant
	} else {
		// We restrict the creation of multiple default tenants because if
		// we allow for more than one to be created, it's not clear what we
		// should return in AdvSQLAddr() as the default SQL address. Panic
		// here to prevent more than one from being added. If you're hitting
		// this panic it's likely that you're trying to expose multiple default
		// test tenants, in which case, you should evaluate what to do about
		// returning a default SQL address in AdvSQLAddr().
		return errors.AssertionFailedf("invalid number of test SQL servers %d", len(ts.testTenants))
	}
	return nil
}

// Start starts the testServer by bootstrapping an in-memory store
// (defaults to maximum of 100M). The server is started, launching the
// node RPC server and all HTTP endpoints. Use the value of
// testServer.AdvRPCAddr() after Start() for client connections.
// Use testServer.Stopper().Stop() to shutdown the server after the test
// completes.
func (ts *testServer) Start(ctx context.Context) error {
	if err := ts.topLevelServer.PreStart(ctx); err != nil {
		return err
	}
	if err := ts.topLevelServer.AcceptInternalClients(ctx); err != nil {
		return err
	}
	// In tests we need some, but not all of RunInitialSQL functionality.
	if err := ts.topLevelServer.RunInitialSQL(
		ctx, false /* startSingleNode */, "" /* adminUser */, "", /* adminPassword */
	); err != nil {
		return err
	}
	if err := ts.topLevelServer.AcceptClients(ctx); err != nil {
		return err
	}

	if err := ts.maybeStartDefaultTestTenant(ctx); err != nil {
		// We're failing the call to this function but we've already started
		// the testServer above. Stop it here to avoid leaking the server.
		ts.Stopper().Stop(context.Background())
		return err
	}
	go func() {
		// If the server requests a shutdown, do that simply by stopping the
		// stopper.
		select {
		case req := <-ts.topLevelServer.ShutdownRequested():
			shutdownCtx := ts.topLevelServer.AnnotateCtx(context.Background())
			log.Infof(shutdownCtx, "server requesting spontaneous shutdown: %v", req.ShutdownCause())
			ts.Stopper().Stop(shutdownCtx)
		case <-ts.Stopper().ShouldQuiesce():
		}
	}()
	return nil
}

// Stop is part of the serverutils.TestServerInterface.
func (ts *testServer) Stop(ctx context.Context) {
	ctx = ts.topLevelServer.AnnotateCtx(ctx)
	ts.topLevelServer.stopper.Stop(ctx)
}

// testTenant is an in-memory instantiation of the SQL-only process created for
// each active Cockroach tenant. testTenant provides tests with access to
// internal methods and state on SQLServer. It is typically started in tests by
// calling the TestServerInterface.StartTenant method or by calling the wrapper
// serverutils.StartTenant method.
type testTenant struct {
	sql    *SQLServer
	Cfg    *BaseConfig
	SQLCfg *SQLConfig
	*httpTestServer
	drain *drainServer

	pgL *netutil.LoopbackListener

	// pgPreServer handles SQL connections prior to routing them to a
	// specific tenant.
	pgPreServer *pgwire.PreServeConnHandler
}

var _ serverutils.ApplicationLayerInterface = &testTenant{}

// AnnotateCtx is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) AnnotateCtx(ctx context.Context) context.Context {
	return t.sql.AnnotateCtx(ctx)
}

// SQLInstanceID is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) SQLInstanceID() base.SQLInstanceID {
	return t.sql.SQLInstanceID()
}

// AdvRPCAddr is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) AdvRPCAddr() string {
	return t.Cfg.AdvertiseAddr
}

// AdvSQLAddr is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) AdvSQLAddr() string {
	return t.Cfg.SQLAdvertiseAddr
}

// SQLAddr is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) SQLAddr() string {
	return t.Cfg.SQLAddr
}

// HTTPAddr is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) HTTPAddr() string {
	return t.Cfg.HTTPAddr
}

// RPCAddr is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) RPCAddr() string {
	return t.Cfg.Addr
}

// SQLConn is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) SQLConn(test serverutils.TestFataler, dbName string) *gosql.DB {
	return t.SQLConnForUser(test, username.RootUser, dbName)
}

// SQLConnForUser is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) SQLConnForUser(
	test serverutils.TestFataler, userName, dbName string,
) *gosql.DB {
	db, err := t.SQLConnForUserE(userName, dbName)
	if err != nil {
		test.Fatal(err)
	}
	return db
}

// SQLConnE is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) SQLConnE(dbName string) (*gosql.DB, error) {
	return t.SQLConnForUserE(username.RootUser, dbName)
}

// SQLConnForUserE is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) SQLConnForUserE(userName string, dbName string) (*gosql.DB, error) {
	return openTestSQLConn(userName, dbName, t.Stopper(),
		t.pgL,
		t.Cfg.SQLAdvertiseAddr,
		t.Cfg.Insecure,
	)
}

// DB is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) DB() *kv.DB {
	return t.sql.execCfg.DB
}

// PGServer is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) PGServer() interface{} {
	return t.sql.pgServer
}

// PGPreServer exposes the pgwire.PreServeConnHandler instance used by
// the testServer.
func (ts *testTenant) PGPreServer() interface{} {
	if ts != nil {
		return ts.pgPreServer
	}
	return nil
}

// DiagnosticsReporter is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) DiagnosticsReporter() interface{} {
	return t.sql.diagnosticsReporter
}

// StatusServer is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) StatusServer() interface{} {
	return t.t.status
}

// TenantStatusServer is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) TenantStatusServer() interface{} {
	return t.t.status
}

// SQLServer is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) SQLServer() interface{} {
	return t.sql.pgServer.SQLServer
}

// DistSQLServer is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) DistSQLServer() interface{} {
	return t.sql.distSQLServer
}

// SetDistSQLSpanResolver is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) SetDistSQLSpanResolver(spanResolver interface{}) {
	t.sql.execCfg.DistSQLPlanner.SetSpanResolver(spanResolver.(physicalplan.SpanResolver))
}

// DistSenderI is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) DistSenderI() interface{} {
	return t.sql.execCfg.DistSender
}

// InternalDB is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) InternalDB() interface{} {
	return t.sql.internalDB
}

// LeaseManager is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) LeaseManager() interface{} {
	return t.sql.leaseMgr
}

// InternalExecutor is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) InternalExecutor() interface{} {
	return t.sql.internalExecutor
}

// RPCContext is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) RPCContext() *rpc.Context {
	return t.sql.execCfg.RPCContext
}

// JobRegistry is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) JobRegistry() interface{} {
	return t.sql.jobRegistry
}

// ExecutorConfig is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) ExecutorConfig() interface{} {
	return *t.sql.execCfg
}

// RangeFeedFactory is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) RangeFeedFactory() interface{} {
	return t.sql.execCfg.RangeFeedFactory
}

// ClusterSettings is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) ClusterSettings() *cluster.Settings {
	return t.Cfg.Settings
}

// Stopper is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) Stopper() *stop.Stopper {
	return t.sql.stopper
}

// Clock is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) Clock() *hlc.Clock {
	return t.sql.execCfg.Clock
}

// AmbientCtx implements serverutils.ApplicationLayerInterface. This
// retrieves the ambient context for this server. This is intended for
// exclusive use by test code.
func (t *testTenant) AmbientCtx() log.AmbientContext {
	return t.Cfg.AmbientCtx
}

// TestingKnobs is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) TestingKnobs() *base.TestingKnobs {
	return &t.Cfg.TestingKnobs
}

// SQLServerInternal is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) SQLServerInternal() interface{} {
	return t.sql
}

// SpanConfigKVAccessor is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) SpanConfigKVAccessor() interface{} {
	return t.sql.tenantConnect
}

// SpanConfigReporter is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) SpanConfigReporter() interface{} {
	return t.sql.tenantConnect
}

// SpanConfigReconciler is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) SpanConfigReconciler() interface{} {
	return t.sql.spanconfigMgr.Reconciler
}

// SpanConfigSQLTranslatorFactory is part of the
// serverutils.ApplicationLayerInterface.
func (t *testTenant) SpanConfigSQLTranslatorFactory() interface{} {
	return t.sql.spanconfigSQLTranslatorFactory
}

// SpanConfigSQLWatcher is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) SpanConfigSQLWatcher() interface{} {
	return t.sql.spanconfigSQLWatcher
}

// SystemConfigProvider is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) SystemConfigProvider() config.SystemConfigProvider {
	return t.sql.systemConfigWatcher
}

// DrainClients exports the drainClients() method for use by tests.
func (t *testTenant) DrainClients(ctx context.Context) error {
	return t.drain.drainClients(ctx, nil /* reporter */)
}

// Readiness is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) Readiness(ctx context.Context) error {
	return t.t.admin.checkReadinessForHealthCheck(ctx)
}

// MustGetSQLCounter implements the serverutils.ApplicationLayerInterface.
func (t *testTenant) MustGetSQLCounter(name string) int64 {
	return mustGetSQLCounterForRegistry(t.sql.metricsRegistry, name)
}

// MustGetSQLNetworkCounter implements the serverutils.ApplicationLayerInterface.
func (t *testTenant) MustGetSQLNetworkCounter(name string) int64 {
	reg := metric.NewRegistry()
	for _, m := range t.sql.pgServer.Metrics() {
		reg.AddMetricStruct(m)
	}
	return mustGetSQLCounterForRegistry(reg, name)
}

// RangeDescIteratorFactory implements the serverutils.ApplicationLayerInterface.
func (t *testTenant) RangeDescIteratorFactory() interface{} {
	return t.sql.execCfg.RangeDescIteratorFactory
}

// Codec is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) Codec() keys.SQLCodec {
	return t.sql.execCfg.Codec
}

// Tracer is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) Tracer() *tracing.Tracer {
	return t.sql.ambientCtx.Tracer
}

// TracerI is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) TracerI() interface{} {
	return t.Tracer()
}

// ForceTableGC is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) ForceTableGC(
	ctx context.Context, database, table string, timestamp hlc.Timestamp,
) error {
	return internalForceTableGC(ctx, t, database, table, timestamp)
}

// DefaultZoneConfig is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) DefaultZoneConfig() zonepb.ZoneConfig {
	return *t.SystemConfigProvider().GetSystemConfig().DefaultZoneConfig
}

// SettingsWatcher is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) SettingsWatcher() interface{} {
	return t.sql.settingsWatcher
}

// StartSharedProcessTenant is part of the serverutils.TenantControlInterface.
func (ts *testServer) StartSharedProcessTenant(
	ctx context.Context, args base.TestSharedProcessTenantArgs,
) (serverutils.ApplicationLayerInterface, *gosql.DB, error) {
	if err := args.TenantName.IsValid(); err != nil {
		return nil, nil, err
	}

	// Save the args for use if the server needs to be created.
	ts.topLevelServer.serverController.testArgs[args.TenantName] = args

	tenantRow, err := ts.InternalExecutor().(*sql.InternalExecutor).QueryRow(
		ctx, "testserver-check-tenant-active", nil, /* txn */
		"SELECT id FROM system.tenants WHERE name=$1 AND active=true",
		args.TenantName,
	)
	if err != nil {
		return nil, nil, err
	}
	tenantExists := tenantRow != nil

	justCreated := false
	var tenantID roachpb.TenantID
	if tenantExists {
		// A tenant with the given name already exists; let's check that
		// it matches the ID that this call wants (if any).
		id := uint64(*tenantRow[0].(*tree.DInt))
		if args.TenantID.IsSet() && args.TenantID.ToUint64() != id {
			return nil, nil, errors.Newf("a tenant with name %q exists, but its ID is %d instead of %d",
				args.TenantName, id, args.TenantID)
		}
		tenantID = roachpb.MustMakeTenantID(id)
	} else {
		// The tenant doesn't exist; let's create it.
		justCreated = true
		if args.TenantID.IsSet() {
			// Create with name and ID.
			_, err := ts.InternalExecutor().(*sql.InternalExecutor).ExecEx(
				ctx,
				"create-tenant",
				nil, /* txn */
				sessiondata.NodeUserSessionDataOverride,
				"SELECT crdb_internal.create_tenant($1,$2)",
				args.TenantID.ToUint64(), args.TenantName,
			)
			if err != nil {
				return nil, nil, err
			}
			tenantID = args.TenantID
		} else {
			// Create with name alone; allocate an ID automatically.
			row, err := ts.InternalExecutor().(*sql.InternalExecutor).QueryRowEx(
				ctx,
				"create-tenant",
				nil, /* txn */
				sessiondata.NodeUserSessionDataOverride,
				"SELECT crdb_internal.create_tenant($1)",
				args.TenantName,
			)
			if err != nil {
				return nil, nil, err
			}
			id := uint64(*row[0].(*tree.DInt))
			tenantID = roachpb.MustMakeTenantID(id)
		}
	}

	if justCreated {
		// Also mark it for shared-process execution.
		_, err := ts.InternalExecutor().(*sql.InternalExecutor).ExecEx(
			ctx,
			"start-tenant-shared-service",
			nil, /* txn */
			sessiondata.NodeUserSessionDataOverride,
			"ALTER TENANT $1 START SERVICE SHARED",
			args.TenantName,
		)
		if err != nil {
			return nil, nil, err
		}
	}

	// Wait for the rangefeed to catch up.
	if err := ts.WaitForTenantReadiness(ctx, tenantID); err != nil {
		return nil, nil, err
	}

	// Instantiate the tenant server.
	s, err := ts.topLevelServer.serverController.startAndWaitForRunningServer(ctx, args.TenantName)
	if err != nil {
		return nil, nil, err
	}

	sqlServerWrapper := s.(*tenantServerWrapper).server
	sqlServer := sqlServerWrapper.sqlServer
	hts := &httpTestServer{}
	hts.t.authentication = sqlServerWrapper.authentication
	hts.t.sqlServer = sqlServer
	hts.t.tenantName = args.TenantName
	hts.t.admin = sqlServerWrapper.tenantAdmin
	hts.t.status = sqlServerWrapper.tenantStatus

	tt := &testTenant{
		sql:            sqlServer,
		Cfg:            sqlServer.cfg,
		SQLCfg:         sqlServerWrapper.sqlCfg,
		pgPreServer:    sqlServerWrapper.pgPreServer,
		pgL:            sqlServerWrapper.loopbackPgL,
		httpTestServer: hts,
		drain:          sqlServerWrapper.drainServer,
	}

	sqlDB, err := ts.SQLConnE("cluster:" + string(args.TenantName) + "/" + args.UseDatabase)
	if err != nil {
		return nil, nil, err
	}
	return tt, sqlDB, err
}

// DisableStartTenant is part of the serverutils.TenantControlInterface.
func (ts *testServer) DisableStartTenant(reason error) {
	ts.disableStartTenantError = reason
}

// MigrationServer is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) MigrationServer() interface{} {
	return t.sql.migrationServer
}

// CollectionFactory is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) CollectionFactory() interface{} {
	return t.sql.execCfg.CollectionFactory
}

// SystemTableIDResolver is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) SystemTableIDResolver() interface{} {
	return t.sql.execCfg.SystemTableIDResolver
}

// QueryDatabaseID is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) QueryDatabaseID(
	ctx context.Context, userName username.SQLUsername, dbName string,
) (descpb.ID, error) {
	return t.t.admin.queryDatabaseID(ctx, userName, dbName)
}

// QueryTableID is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) QueryTableID(
	ctx context.Context, userName username.SQLUsername, dbName, tbName string,
) (descpb.ID, error) {
	return t.t.admin.queryTableID(ctx, userName, dbName, tbName)
}

// StatsForSpans is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) StatsForSpan(
	ctx context.Context, span roachpb.Span,
) (*serverpb.TableStatsResponse, error) {
	return t.t.admin.statsForSpan(ctx, span)
}

// SetReady is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) SetReady(ready bool) {
	t.sql.isReady.Set(ready)
}

// SetAcceptSQLWithoutTLS is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) SetAcceptSQLWithoutTLS(accept bool) {
	t.Cfg.AcceptSQLWithoutTLS = accept
}

// PrivilegeChecker is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) PrivilegeChecker() interface{} {
	return t.t.admin.privilegeChecker
}

// HTTPAuthServer is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) HTTPAuthServer() interface{} {
	return t.t.authentication
}

func (ts *testServer) waitForTenantReadinessImpl(
	ctx context.Context, tenantID roachpb.TenantID,
) error {
	_, infoWatcher, err := ts.node.waitForTenantWatcherReadiness(ctx)
	if err != nil {
		return err
	}

	// Ignore the tenant capabilities entry as it may be served from a stale
	// version of the in-RAM cache. We use the returned channel to monitor when
	// the watcher has completed its initial scan and hydrated the in-RAM cache
	// with the most up-to-date values.
	_, infoCh, _ := infoWatcher.GetInfo(tenantID)

	// Restarting the watcher forces a new initial scan which is faster than
	// waiting out the closed timestamp interval required to see new updates.
	ts.node.tenantInfoWatcher.TestingRestart()

	log.Infof(ctx, "waiting for rangefeed to catch up with record for tenant %v", tenantID)

	// Wait for the watcher to handle the complete update from the initial scan
	// and notify our previously registered listener.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-infoCh:
	}
	for {
		info, infoCh, found := infoWatcher.GetInfo(tenantID)
		if found && info.ServiceMode != mtinfopb.ServiceModeNone {
			log.Infof(ctx, "cached record found for tenant %v", tenantID)
			return nil
		}
		// Not found: wait and try again.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-infoCh:
			continue
		}
	}
}

// WaitForTenantReadiness is part of serverutils.TenantControlInterface..
func (ts *testServer) WaitForTenantReadiness(ctx context.Context, tenantID roachpb.TenantID) error {
	// Two minutes should be sufficient for the in-RAM caches to be hydrated with
	// the tenant record.
	return timeutil.RunWithTimeout(ctx, "waitForTenantReadiness", 2*time.Minute, func(ctx context.Context) error {
		return ts.waitForTenantReadinessImpl(ctx, tenantID)
	})
}

// StartTenant is part of the serverutils.TenantControlInterface.
func (ts *testServer) StartTenant(
	ctx context.Context, params base.TestTenantArgs,
) (serverutils.ApplicationLayerInterface, error) {
	if ts.disableStartTenantError != nil {
		return nil, ts.disableStartTenantError
	}
	// Determine if we need to create the tenant before starting it.

	ie := ts.InternalExecutor().(*sql.InternalExecutor)
	if !params.DisableCreateTenant {
		rowCount, err := ie.Exec(
			ctx, "testserver-check-tenant-active", nil,
			"SELECT 1 FROM system.tenants WHERE id=$1 AND active=true",
			params.TenantID.ToUint64(),
		)
		if err != nil {
			return nil, err
		}
		if rowCount == 0 {
			// Tenant doesn't exist. Create it.
			if _, err := ie.Exec(
				ctx, "testserver-create-tenant", nil /* txn */, "SELECT crdb_internal.create_tenant($1, $2)",
				params.TenantID.ToUint64(), params.TenantName,
			); err != nil {
				return nil, err
			}
		} else if params.TenantName != "" {
			_, err := ie.Exec(ctx, "rename-test-tenant", nil,
				`ALTER TENANT [$1] RENAME TO $2`,
				params.TenantID.ToUint64(), params.TenantName)
			if err != nil {
				return nil, err
			}
		}
		// Mark it for external execution. This is needed before we can spawn a server.
		if _, err := ts.InternalExecutor().(*sql.InternalExecutor).Exec(
			ctx, "testserver-set-tenant-service-mode", nil, /* txn */
			"ALTER TENANT [$1] START SERVICE EXTERNAL",
			params.TenantID.ToUint64(),
		); err != nil {
			return nil, err
		}
	} else if !params.SkipTenantCheck {
		requestedID := uint64(0)
		if params.TenantID.IsSet() {
			requestedID = params.TenantID.ToUint64()
		}
		rows, err := ie.QueryBuffered(
			ctx, "testserver-check-tenant-active", nil,
			"SELECT id, name FROM system.tenants WHERE ($1 <> 0 AND id=$1) OR ($2 <> '' AND name = $2) AND active=true",
			requestedID, string(params.TenantName),
		)
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			return nil, errors.Newf("no tenant found with ID %d or name %q",
				requestedID, params.TenantName)
		}
		if len(rows) > 1 {
			return nil, errors.Newf("ambiguous tenant spec: found separate entries for tenant ID %d and name %q\n%+v",
				requestedID, params.TenantName, rows)
		}
		row := rows[0]
		// Check that the name passed in via params matches the name persisted in
		// the system.tenants table.
		if params.TenantName != "" {
			if row[1] == tree.DNull || string(params.TenantName) != string(tree.MustBeDString(row[1])) {
				return nil, errors.Newf("name mismatch; tenant %d has name %q, but params specifies name %q",
					row[0], row[1], params.TenantName)
			}
		}
		if params.TenantID.IsSet() {
			if params.TenantID.ToUint64() != uint64(tree.MustBeDInt(row[0])) {
				return nil, errors.Newf("ID mismatch; tenant %q has ID %d, but params specifies ID %d",
					row[1], row[0], params.TenantID.ToUint64())
			}
		}
		if row[1] != tree.DNull {
			params.TenantName = roachpb.TenantName(tree.MustBeDString(row[1]))
		}
		if row[0] != tree.DNull {
			params.TenantID = roachpb.MustMakeTenantID(uint64(tree.MustBeDInt(row[0])))
		}
	}

	if !params.SkipWaitForTenantCache {
		// Wait until the rangefeed has caught up with the tenant creation.
		if err := ts.WaitForTenantReadiness(ctx, params.TenantID); err != nil {
			return nil, err
		}
	}

	st := params.Settings
	if st == nil {
		st = cluster.MakeTestingClusterSettings()
	}
	// Verify that the settings object that was passed in has
	// initialized the version setting. This is pretty much necessary
	// for secondary tenants. See the comments at the beginning of
	// `runStartSQL()` in cli/mt_start_sql.go and
	// `makeSharedProcessTenantServerConfig()` in
	// server_controller_new_server.go.
	//
	// The version is initialized in MakeTestingClusterSettings(). This
	// assertion is there to prevent inadvertent changes to
	// MakeTestingClusterSettings() and as a guardrail for tests that
	// pass a custom params.Settings.
	clusterversion.AssertInitialized(ctx, &st.SV)

	// Needed for backward-compat on crdb_internal.ranges{_no_leases}.
	// Remove in v23.2.
	deprecatedshowranges.ShowRangesDeprecatedBehaviorSetting.Override(
		context.TODO(), &st.SV,
		// In unit tests, we exercise the new behavior.
		false)

	st.ExternalIODir = params.ExternalIODir
	sqlCfg := makeTestSQLConfig(st, params.TenantID)
	sqlCfg.TenantKVAddrs = []string{ts.AdvRPCAddr()}
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
		// Use main server quiesce as a signal to stop tenants stopper. In the
		// perfect world, we want to have tenant stopped before the main server.  In
		// order to stop the tenant when main server stops, we need to propagate
		// quiesce signal to the tenant. Note that using ts.Stopper().AddCloser() to
		// propagate signal does not work since this signal would be delivered too
		// late: for example the tenant may get stuck (or become slow) during
		// shutdown since closers are the very last thing that runs -- and by then,
		// the tenant maybe stuck, or re-trying an operation (e.g. to resolve
		// tenant ranges).
		if err := ts.Stopper().RunAsyncTask(ctx, "propagate-cancellation-to-tenant", func(ctx context.Context) {
			<-ts.Stopper().ShouldQuiesce()
			stopper.Stop(ctx)
		}); err != nil {
			return nil, err
		}
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
	baseCfg.ClusterName = ts.Cfg.ClusterName
	baseCfg.StartDiagnosticsReporting = params.StartDiagnosticsReporting
	baseCfg.DisableTLSForHTTP = params.DisableTLSForHTTP
	baseCfg.EnableDemoLoginEndpoint = params.EnableDemoLoginEndpoint

	// Waiting for capabilities can time To avoid paying this cost in all
	// cases, we only set the nodelocal storage capability if the caller has
	// configured an ExternalIODir since nodelocal storage only works with
	// that configured.
	//
	// TODO(ssd): We do not set this capability in
	// StartSharedProcessTenant.
	shouldGrantNodelocalCap := ts.params.ExternalIODir != ""
	canGrantNodelocalCap := ts.ClusterSettings().Version.IsActive(ctx, clusterversion.V23_1TenantCapabilities)
	if canGrantNodelocalCap && shouldGrantNodelocalCap {
		_, err := ie.Exec(ctx, "testserver-alter-tenant-cap", nil,
			"ALTER TENANT [$1] GRANT CAPABILITY can_use_nodelocal_storage", params.TenantID.ToUint64())
		if err != nil {
			if params.SkipTenantCheck {
				log.Infof(ctx, "ignoring error granting capability because SkipTenantCheck is true: %v", err)
			} else {
				return nil, err
			}
		} else {
			// Restart the capabilities watcher. Restarting the watcher
			// forces a new initial scan which is faster than waiting out
			// the closed timestamp interval required to see new updates.
			ts.tenantCapabilitiesWatcher.TestingRestart()
			if err := testutils.SucceedsSoonError(func() error {
				capabilities, found := ts.TenantCapabilitiesReader().GetCapabilities(params.TenantID)
				if !found {
					return errors.Newf("capabilities not yet ready")
				}
				if !tenantcapabilities.MustGetBoolByID(
					capabilities, tenantcapabilities.CanUseNodelocalStorage,
				) {
					return errors.Newf("capabilities not yet ready")
				}
				return nil
			}); err != nil {
				return nil, err
			}
		}
	}

	// For now, we don't support split RPC/SQL ports for secondary tenants
	// in test servers.
	baseCfg.SplitListenSQL = true

	if params.SSLCertsDir != "" {
		baseCfg.SSLCertsDir = params.SSLCertsDir
	}
	if params.StartingRPCAndSQLPort > 0 {
		log.Infof(ctx, "computing tenant server sql/rpc addr from %d", params.StartingRPCAndSQLPort)
		baseCfg.SplitListenSQL = false
		addr, _, err := addrutil.SplitHostPort(baseCfg.Addr, strconv.Itoa(params.StartingRPCAndSQLPort))
		if err != nil {
			return nil, err
		}
		newAddr := net.JoinHostPort(addr, strconv.Itoa(params.StartingRPCAndSQLPort+int(params.TenantID.ToUint64())))
		baseCfg.Addr = newAddr
		baseCfg.AdvertiseAddr = newAddr
		baseCfg.SQLAddr = newAddr
		baseCfg.SQLAdvertiseAddr = newAddr
	}
	if params.StartingHTTPPort > 0 {
		log.Infof(ctx, "computing tenant server http addr from %d", params.StartingHTTPPort)
		addr, _, err := addrutil.SplitHostPort(baseCfg.HTTPAddr, strconv.Itoa(params.StartingHTTPPort))
		if err != nil {
			return nil, err
		}
		newAddr := net.JoinHostPort(addr, strconv.Itoa(params.StartingHTTPPort+int(params.TenantID.ToUint64())))
		baseCfg.HTTPAddr = newAddr
		baseCfg.HTTPAdvertiseAddr = newAddr
	}

	log.Infof(ctx, "tenant server configuration (no controller): rpc %v/%v sql %v/%v http %v/%v",
		baseCfg.Addr, baseCfg.AdvertiseAddr,
		baseCfg.SQLAddr, baseCfg.SQLAdvertiseAddr,
		baseCfg.HTTPAddr, baseCfg.HTTPAdvertiseAddr,
	)
	sw, err := NewSeparateProcessTenantServer(
		ctx,
		stopper,
		baseCfg,
		sqlCfg,
		roachpb.NewTenantNameContainer(params.TenantName),
	)
	if err != nil {
		return nil, err
	}
	go func() {
		// If the server requests a shutdown, do that simply by stopping the
		// tenant's stopper.
		select {
		case req := <-sw.ShutdownRequested():
			shutdownCtx := sw.AnnotateCtx(context.Background())
			log.Infof(shutdownCtx, "server requesting spontaneous shutdown: %v", req.ShutdownCause())
			stopper.Stop(shutdownCtx)
		case <-stopper.ShouldQuiesce():
		}
	}()

	if err := sw.Start(ctx); err != nil {
		return nil, err
	}

	hts := &httpTestServer{}
	hts.t.authentication = sw.authentication
	hts.t.sqlServer = sw.sqlServer
	hts.t.admin = sw.tenantAdmin
	hts.t.status = sw.tenantStatus

	return &testTenant{
		sql:            sw.sqlServer,
		Cfg:            &baseCfg,
		SQLCfg:         &sqlCfg,
		pgPreServer:    sw.pgPreServer,
		httpTestServer: hts,
		drain:          sw.drainServer,
		pgL:            sw.loopbackPgL,
	}, err
}

// ExpectedInitialRangeCount returns the expected number of ranges that should
// be on the server after initial (asynchronous) splits have been completed,
// assuming no additional information is added outside of the normal bootstrap
// process.
func (ts *testServer) ExpectedInitialRangeCount() (int, error) {
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

// GetStores is part of the serverutils.StorageLayerInterface.
func (ts *testServer) GetStores() interface{} {
	return ts.node.stores
}

// ClusterSettings returns the ClusterSettings.
func (ts *testServer) ClusterSettings() *cluster.Settings {
	return ts.Cfg.Settings
}

// SettingsWatcher is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) SettingsWatcher() interface{} {
	return ts.sqlServer.settingsWatcher
}

// Engines returns the testServer's engines.
func (ts *testServer) Engines() []storage.Engine {
	return ts.engines
}

// AdvRPCAddr returns the server's RPC address. Should be used by clients.
func (ts *testServer) AdvRPCAddr() string {
	return ts.cfg.AdvertiseAddr
}

// AdvSQLAddr returns the server's SQL address. Should be used by clients.
func (ts *testServer) AdvSQLAddr() string {
	return ts.cfg.SQLAdvertiseAddr
}

// HTTPAddr returns the server's HTTP address. Should be used by clients.
func (ts *testServer) HTTPAddr() string {
	return ts.cfg.HTTPAddr
}

// RPCAddr returns the server's listening RPC address.
// Note: use AdvRPCAddr() instead unless there is a specific reason not to.
func (ts *testServer) RPCAddr() string {
	return ts.cfg.Addr
}

// SQLAddr returns the server's listening SQL address.
// Note: use AdvSQLAddr() instead unless there is a specific reason not to.
func (ts *testServer) SQLAddr() string {
	return ts.cfg.SQLAddr
}

// DrainClients exports the drainClients() method for use by tests.
func (ts *testServer) DrainClients(ctx context.Context) error {
	return ts.drain.drainClients(ctx, nil /* reporter */)
}

// Readiness is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) Readiness(ctx context.Context) error {
	return ts.admin.checkReadinessForHealthCheck(ctx)
}

// SetReadyFn is part of TestServerInterface.
func (ts *testServer) SetReadyFn(fn func(bool)) {
	ts.topLevelServer.cfg.ReadyFn = fn
}

// WriteSummaries implements the serverutils.StorageLayerInterface.
func (ts *testServer) WriteSummaries() error {
	return ts.node.writeNodeStatus(context.TODO(), time.Hour, false)
}

// UpdateChecker implements the serverutils.StorageLayerInterface.
func (ts *testServer) UpdateChecker() interface{} {
	return ts.topLevelServer.updates
}

// DiagnosticsReporter implements the serverutils.ApplicationLayerInterface.
func (ts *testServer) DiagnosticsReporter() interface{} {
	return ts.topLevelServer.sqlServer.diagnosticsReporter
}

type v2AuthDecorator struct {
	http.RoundTripper

	session string
}

func (v *v2AuthDecorator) RoundTrip(r *http.Request) (*http.Response, error) {
	r.Header.Add(authserver.APIV2AuthHeader, v.session)
	return v.RoundTripper.RoundTrip(r)
}

// MustGetSQLCounter implements serverutils.ApplicationLayerInterface.
func (ts *testServer) MustGetSQLCounter(name string) int64 {
	return mustGetSQLCounterForRegistry(ts.registry, name)
}

// MustGetSQLNetworkCounter implements the serverutils.ApplicationLayerInterface.
func (ts *testServer) MustGetSQLNetworkCounter(name string) int64 {
	reg := metric.NewRegistry()
	for _, m := range ts.sqlServer.pgServer.Metrics() {
		reg.AddMetricStruct(m)
	}
	return mustGetSQLCounterForRegistry(reg, name)
}

// Locality is part of the serverutils.StorageLayerInterface.
func (ts *testServer) Locality() *roachpb.Locality {
	return &ts.cfg.Locality
}

// LeaseManager is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) LeaseManager() interface{} {
	return ts.sqlServer.leaseMgr
}

// InternalExecutor is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) InternalExecutor() interface{} {
	return ts.sqlServer.internalExecutor
}

// InternalDB is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) InternalDB() interface{} {
	return ts.sqlServer.internalDB
}

// GetNode exposes the Server's Node.
func (ts *testServer) GetNode() *Node {
	return ts.node
}

// DistSenderI is part of DistSenderInterface.
func (ts *testServer) DistSenderI() interface{} {
	return ts.distSender
}

// MigrationServer is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) MigrationServer() interface{} {
	return ts.topLevelServer.migrationServer
}

// SpanConfigKVAccessor is part of the serverutils.StorageLayerInterface.
func (ts *testServer) SpanConfigKVAccessor() interface{} {
	return ts.topLevelServer.node.spanConfigAccessor
}

// SpanConfigReporter is part of the serverutils.StorageLayerInterface.
func (ts *testServer) SpanConfigReporter() interface{} {
	return ts.topLevelServer.node.spanConfigReporter
}

// SpanConfigReconciler is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) SpanConfigReconciler() interface{} {
	if ts.sqlServer.spanconfigMgr == nil {
		panic("uninitialized; see EnableSpanConfigs testing knob to use span configs")
	}
	return ts.sqlServer.spanconfigMgr.Reconciler
}

// SpanConfigSQLTranslatorFactory is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) SpanConfigSQLTranslatorFactory() interface{} {
	if ts.sqlServer.spanconfigSQLTranslatorFactory == nil {
		panic("uninitialized; see EnableSpanConfigs testing knob to use span configs")
	}
	return ts.sqlServer.spanconfigSQLTranslatorFactory
}

// SpanConfigSQLWatcher is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) SpanConfigSQLWatcher() interface{} {
	if ts.sqlServer.spanconfigSQLWatcher == nil {
		panic("uninitialized; see EnableSpanConfigs testing knob to use span configs")
	}
	return ts.sqlServer.spanconfigSQLWatcher
}

// SQLServer is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) SQLServer() interface{} {
	return ts.sqlServer.pgServer.SQLServer
}

// DistSQLServer is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) DistSQLServer() interface{} {
	return ts.sqlServer.distSQLServer
}

// SetDistSQLSpanResolver is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) SetDistSQLSpanResolver(spanResolver interface{}) {
	ts.sqlServer.execCfg.DistSQLPlanner.SetSpanResolver(spanResolver.(physicalplan.SpanResolver))
}

// GetFirstStoreID is part of the serverutils.StorageLayerInterface.
func (ts *testServer) GetFirstStoreID() roachpb.StoreID {
	firstStoreID := roachpb.StoreID(-1)
	err := ts.GetStores().(*kvserver.Stores).VisitStores(func(s *kvserver.Store) error {
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
func (ts *testServer) LookupRange(key roachpb.Key) (roachpb.RangeDescriptor, error) {
	rs, _, err := kv.RangeLookup(context.Background(), ts.DB().NonTransactionalSender(),
		key, kvpb.CONSISTENT, 0 /* prefetchNum */, false /* reverse */)
	if err != nil {
		return roachpb.RangeDescriptor{}, errors.Wrapf(
			err, "%q: lookup range unexpected error", key)
	}
	return rs[0], nil
}

// MergeRanges merges the range containing leftKey with the range to its right.
func (ts *testServer) MergeRanges(leftKey roachpb.Key) (roachpb.RangeDescriptor, error) {

	ctx := context.Background()
	mergeReq := kvpb.AdminMergeRequest{
		RequestHeader: kvpb.RequestHeader{
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

// SplitRangeWithExpiration is part of the serverutils.StorageLayerInterface.
func (ts *testServer) SplitRangeWithExpiration(
	splitKey roachpb.Key, expirationTime hlc.Timestamp,
) (roachpb.RangeDescriptor, roachpb.RangeDescriptor, error) {
	ctx := context.Background()
	splitReq := kvpb.AdminSplitRequest{
		RequestHeader: kvpb.RequestHeader{
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
		rs, more, err := kv.RangeLookup(ctx, txn, splitKey.Next(), kvpb.CONSISTENT, 1, true /* reverse */)
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
func (ts *testServer) SplitRange(
	splitKey roachpb.Key,
) (roachpb.RangeDescriptor, roachpb.RangeDescriptor, error) {
	return ts.SplitRangeWithExpiration(splitKey, hlc.MaxTimestamp)
}

// GetRangeLease is part of severutils.StorageLayerInterface.
func (ts *testServer) GetRangeLease(
	ctx context.Context, key roachpb.Key, queryPolicy roachpb.LeaseInfoOpt,
) (_ roachpb.LeaseInfo, now hlc.ClockTimestamp, _ error) {
	leaseReq := kvpb.LeaseInfoRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: key,
		},
	}
	leaseResp, pErr := kv.SendWrappedWith(
		ctx,
		ts.DB().NonTransactionalSender(),
		kvpb.Header{
			// INCONSISTENT read with a NEAREST routing policy, since we want to make
			// sure that the node used to send this is the one that processes the
			// command, regardless of whether it is the leaseholder, for the hint to
			// matter.
			ReadConsistency: kvpb.INCONSISTENT,
			RoutingPolicy:   kvpb.RoutingPolicy_NEAREST,
		},
		&leaseReq,
	)
	if pErr != nil {
		return roachpb.LeaseInfo{}, hlc.ClockTimestamp{}, pErr.GoError()
	}
	// Adapt the LeaseInfoResponse format to LeaseInfo.
	resp := leaseResp.(*kvpb.LeaseInfoResponse)
	if queryPolicy == roachpb.QueryLocalNodeOnly && resp.EvaluatedBy != ts.GetFirstStoreID() {
		// TODO(andrei): Figure out how to deal with nodes with multiple stores.
		// This API should permit addressing the query to a particular store.
		return roachpb.LeaseInfo{}, hlc.ClockTimestamp{}, errors.Errorf(
			"request not evaluated locally; evaluated by s%d instead of local s%d",
			resp.EvaluatedBy, ts.GetFirstStoreID())
	}
	var l roachpb.LeaseInfo
	if resp.CurrentLease != nil {
		l = roachpb.MakeLeaseInfo(*resp.CurrentLease, resp.Lease)
	} else {
		l = roachpb.MakeLeaseInfo(resp.Lease, roachpb.Lease{})
	}
	return l, ts.Clock().NowAsClockTimestamp(), nil
}

// ExecutorConfig is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) ExecutorConfig() interface{} {
	return *ts.sqlServer.execCfg
}

// StartedDefaultTestTenant is part of the serverutils.TenantControlInterface.
func (ts *testServer) StartedDefaultTestTenant() bool {
	return len(ts.testTenants) > 0
}

// ApplicationLayer is part of the serverutils.TestServerInterface.
func (ts *testServer) ApplicationLayer() serverutils.ApplicationLayerInterface {
	if ts.StartedDefaultTestTenant() {
		return ts.testTenants[0]
	}
	return ts
}

// StorageLayer is part of the serverutils.TestServerInterface.
func (ts *testServer) StorageLayer() serverutils.StorageLayerInterface {
	return ts
}

// TenantController is part of the serverutils.TestServerInterface.
func (ts *testServer) TenantController() serverutils.TenantControlInterface {
	return ts
}

// SystemLayer is part of the serverutils.TestServerInterface.
func (ts *testServer) SystemLayer() serverutils.ApplicationLayerInterface {
	return ts
}

// TracerI is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) TracerI() interface{} {
	return ts.Tracer()
}

// Tracer is like TracerI(), but returns the actual type.
func (ts *testServer) Tracer() *tracing.Tracer {
	return ts.node.storeCfg.AmbientCtx.Tracer
}

// ForceTableGC is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) ForceTableGC(
	ctx context.Context, database, table string, timestamp hlc.Timestamp,
) error {
	return internalForceTableGC(ctx, ts.SystemLayer(), database, table, timestamp)
}

func internalForceTableGC(
	ctx context.Context,
	app serverutils.ApplicationLayerInterface,
	database, table string,
	timestamp hlc.Timestamp,
) error {
	tableID, err := app.QueryTableID(ctx, username.RootUserName(), database, table)
	if err != nil {
		return err
	}

	tblKey := app.Codec().TablePrefix(uint32(tableID))
	gcr := kvpb.GCRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    tblKey,
			EndKey: tblKey.PrefixEnd(),
		},
		Threshold: timestamp,
	}
	_, pErr := kv.SendWrapped(ctx, app.DistSenderI().(kv.Sender), &gcr)
	return pErr.GoError()
}

// DefaultZoneConfig is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) DefaultZoneConfig() zonepb.ZoneConfig {
	return *ts.SystemConfigProvider().GetSystemConfig().DefaultZoneConfig
}

// DefaultSystemZoneConfig is part of the serverutils.StorageLayerInterface.
func (ts *testServer) DefaultSystemZoneConfig() zonepb.ZoneConfig {
	return ts.topLevelServer.cfg.DefaultSystemZoneConfig
}

// ScratchRange is part of the serverutils.StorageLayerInterface.
func (ts *testServer) ScratchRange() (roachpb.Key, error) {
	_, desc, err := ts.ScratchRangeEx()
	if err != nil {
		return nil, err
	}
	return desc.StartKey.AsRawKey(), nil
}

// ScratchRangeEx is part of the serverutils.StorageLayerInterface.
func (ts *testServer) ScratchRangeEx() (roachpb.RangeDescriptor, roachpb.RangeDescriptor, error) {
	scratchKey := keys.ScratchRangeMin
	return ts.SplitRange(scratchKey)
}

// ScratchRangeWithExpirationLease is part of the serverutils.StorageLayerInterface.
func (ts *testServer) ScratchRangeWithExpirationLease() (roachpb.Key, error) {
	_, desc, err := ts.ScratchRangeWithExpirationLeaseEx()
	if err != nil {
		return nil, err
	}
	return desc.StartKey.AsRawKey(), nil
}

// ScratchRangeWithExpirationLeaseEx is part of the serverutils.StorageLayerInterface.
func (ts *testServer) ScratchRangeWithExpirationLeaseEx() (
	roachpb.RangeDescriptor,
	roachpb.RangeDescriptor,
	error,
) {
	scratchKey := roachpb.Key(bytes.Join([][]byte{keys.SystemPrefix,
		roachpb.RKey("\x00aaa-testing")}, nil))
	return ts.SplitRange(scratchKey)
}

// RaftConfig is part of the serverutils.StorageLayerInterface.
func (ts *testServer) RaftConfig() base.RaftConfig {
	return ts.Cfg.RaftConfig
}

// MetricsRecorder periodically records node-level and store-level metrics.
func (ts *testServer) MetricsRecorder() *status.MetricsRecorder {
	return ts.node.recorder
}

// CollectionFactory is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) CollectionFactory() interface{} {
	return ts.sqlServer.execCfg.CollectionFactory
}

// SystemTableIDResolver is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) SystemTableIDResolver() interface{} {
	return ts.sqlServer.execCfg.SystemTableIDResolver
}

// SpanConfigKVSubscriber is part of the serverutils.StorageLayerInterface.
func (ts *testServer) SpanConfigKVSubscriber() interface{} {
	return ts.node.storeCfg.SpanConfigSubscriber
}

// SystemConfigProvider is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) SystemConfigProvider() config.SystemConfigProvider {
	return ts.node.storeCfg.SystemConfigProvider
}

// KVFlowController is part of the serverutils.StorageLayerInterface.
func (ts *testServer) KVFlowController() interface{} {
	return ts.node.storeCfg.KVFlowController
}

// KVFlowHandles is part of the serverutils.StorageLayerInterface.
func (ts *testServer) KVFlowHandles() interface{} {
	return ts.node.storeCfg.KVFlowHandles
}

// Codec is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) Codec() keys.SQLCodec {
	return ts.ExecutorConfig().(sql.ExecutorConfig).Codec
}

// RangeDescIteratorFactory is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) RangeDescIteratorFactory() interface{} {
	return ts.sqlServer.execCfg.RangeDescIteratorFactory
}

// BinaryVersionOverride is part of the serverutils.TestServerInterface.
func (ts *testServer) BinaryVersionOverride() roachpb.Version {
	knobs := ts.TestingKnobs().Server
	if knobs == nil {
		return roachpb.Version{}
	}
	return knobs.(*TestingKnobs).BinaryVersionOverride
}

// KvProber is part of the serverutils.StorageLayerInterface.
func (ts *testServer) KvProber() *kvprober.Prober {
	return ts.topLevelServer.kvProber
}

// QueryDatabaseID is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) QueryDatabaseID(
	ctx context.Context, userName username.SQLUsername, dbName string,
) (descpb.ID, error) {
	return ts.admin.queryDatabaseID(ctx, userName, dbName)
}

// QueryTableID is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) QueryTableID(
	ctx context.Context, userName username.SQLUsername, dbName, tbName string,
) (descpb.ID, error) {
	return ts.admin.queryTableID(ctx, userName, dbName, tbName)
}

// StatsForSpans is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) StatsForSpan(
	ctx context.Context, span roachpb.Span,
) (*serverpb.TableStatsResponse, error) {
	return ts.admin.statsForSpan(ctx, span)
}

// SetReady is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) SetReady(ready bool) {
	ts.sqlServer.isReady.Set(ready)
}

// SetAcceptSQLWithoutTLS is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) SetAcceptSQLWithoutTLS(accept bool) {
	ts.Cfg.AcceptSQLWithoutTLS = accept
}

// PrivilegeChecker is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) PrivilegeChecker() interface{} {
	return ts.admin.privilegeChecker
}

// HTTPAuthServer is part of the ApplicationLayerInterface.
func (ts *testServer) HTTPAuthServer() interface{} {
	return ts.t.authentication
}

type testServerFactoryImpl struct{}

// TestServerFactory can be passed to serverutils.InitTestServerFactory
// and rangetestutils.InitTestServerFactory.
var TestServerFactory = testServerFactoryImpl{}

// MakeRangeTestServerargs is part of the rangetestutils.TestServerFactory interface.
func (testServerFactoryImpl) MakeRangeTestServerArgs() base.TestServerArgs {
	return base.TestServerArgs{
		StoreSpecs: []base.StoreSpec{
			base.DefaultTestStoreSpec,
			base.DefaultTestStoreSpec,
			base.DefaultTestStoreSpec,
		},
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				// Now that we allow same node rebalances, disable it in these tests,
				// as they dont expect replicas to move.
				ReplicaPlannerKnobs: plan.ReplicaPlannerTestingKnobs{
					DisableReplicaRebalancing: true,
				},
			},
		},
	}
}

// PrepareRangeTestServer is part of the rangetestutils.TestServerFactory interface.
func (testServerFactoryImpl) PrepareRangeTestServer(srv interface{}) error {
	ts := srv.(serverutils.TestServerInterface)
	kvDB := ts.ApplicationLayer().DB()

	// Make sure the range is spun up with an arbitrary read command. We do not
	// expect a specific response.
	if _, err := kvDB.Get(context.Background(), "a"); err != nil {
		return err
	}

	// Make sure the node status is available. This is done by forcing stores to
	// publish their status, synchronizing to the event feed with a canary
	// event, and then forcing the server to write summaries immediately.
	if err := ts.Node().(*Node).computeMetricsPeriodically(context.Background(), map[*kvserver.Store]*storage.MetricsForInterval{}, 0); err != nil {
		return errors.Wrap(err, "error publishing store statuses")
	}

	if err := ts.WriteSummaries(); err != nil {
		return errors.Wrap(err, "error writing summaries")
	}

	return nil
}

// New is part of TestServerFactory interface.
func (testServerFactoryImpl) New(params base.TestServerArgs) (interface{}, error) {
	if params.Knobs.JobsTestingKnobs != nil {
		if params.Knobs.JobsTestingKnobs.(*jobs.TestingKnobs).DisableAdoptions {
			if params.Knobs.UpgradeManager == nil || !params.Knobs.UpgradeManager.(*upgradebase.TestingKnobs).DontUseJobs {
				return nil, errors.AssertionFailedf("DontUseJobs needs to be set when DisableAdoptions is set")
			}
		}
	}

	cfg := makeTestConfigFromParams(params)
	ts := &testServer{Cfg: &cfg, params: params}

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

	srv, err := NewServer(*ts.Cfg, params.Stopper)
	if err != nil {
		params.Stopper.Stop(ctx)
		return nil, err
	}
	ts.topLevelServer = srv.(*topLevelServer)

	// Create a breaker which never trips and never backs off to avoid
	// introducing timing-based flakes.
	ts.rpcContext.BreakerFactory = func() *circuit.Breaker {
		return circuit.NewBreakerWithOptions(&circuit.Options{
			BackOff: &backoff.ZeroBackOff{},
		})
	}

	// Our context must be shared with our server.
	ts.Cfg = &ts.topLevelServer.cfg

	// The HTTP APIs on ApplicationLayerInterface are implemented by
	// httpTestServer.
	ts.httpTestServer = &httpTestServer{}
	ts.httpTestServer.t.authentication = ts.topLevelServer.authentication
	ts.httpTestServer.t.sqlServer = ts.topLevelServer.sqlServer
	ts.httpTestServer.t.admin = ts.topLevelServer.admin.adminServer
	ts.httpTestServer.t.status = ts.topLevelServer.status.statusServer

	return ts, nil
}

func mustGetSQLCounterForRegistry(registry *metric.Registry, name string) int64 {
	var c int64
	var found bool

	type (
		int64Valuer  interface{ Value() int64 }
		int64Counter interface{ Count() int64 }
	)

	registry.Each(func(n string, v interface{}) {
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

// TestingMakeLoggingContexts is exposed for use in tests.
func TestingMakeLoggingContexts(
	appTenantID roachpb.TenantID,
) (sysContext, appContext context.Context) {
	ctxSysTenant := context.Background()
	ctxSysTenant = context.WithValue(ctxSysTenant, serverident.ServerIdentificationContextKey{}, &idProvider{
		tenantID:  roachpb.SystemTenantID,
		clusterID: &base.ClusterIDContainer{},
		serverID:  &base.NodeIDContainer{},
	})
	ctxAppTenant := context.Background()
	ctxAppTenant = context.WithValue(ctxAppTenant, serverident.ServerIdentificationContextKey{}, &idProvider{
		tenantID:  appTenantID,
		clusterID: &base.ClusterIDContainer{},
		serverID:  &base.NodeIDContainer{},
	})
	return ctxSysTenant, ctxAppTenant
}

// NewClientRPCContext is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) NewClientRPCContext(
	ctx context.Context, user username.SQLUsername,
) *rpc.Context {
	return newClientRPCContext(ctx, user,
		ts.topLevelServer.cfg.Config,
		ts.topLevelServer.cfg.TestingKnobs.Server,
		ts.topLevelServer.cfg.ClusterIDContainer,
		ts)
}

// RPCClientConn is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) RPCClientConn(
	test serverutils.TestFataler, user username.SQLUsername,
) *grpc.ClientConn {
	conn, err := ts.RPCClientConnE(user)
	if err != nil {
		test.Fatal(err)
	}
	return conn
}

// RPCClientConnE is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) RPCClientConnE(user username.SQLUsername) (*grpc.ClientConn, error) {
	ctx := context.Background()
	rpcCtx := ts.NewClientRPCContext(ctx, user)
	return rpcCtx.GRPCDialNode(ts.AdvRPCAddr(), ts.NodeID(), rpc.DefaultClass).Connect(ctx)
}

// GetAdminClient is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) GetAdminClient(test serverutils.TestFataler) serverpb.AdminClient {
	conn := ts.RPCClientConn(test, username.RootUserName())
	return serverpb.NewAdminClient(conn)
}

// GetStatusClient is part of the serverutils.ApplicationLayerInterface.
func (ts *testServer) GetStatusClient(test serverutils.TestFataler) serverpb.StatusClient {
	conn := ts.RPCClientConn(test, username.RootUserName())
	return serverpb.NewStatusClient(conn)
}

// NewClientRPCContext is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) NewClientRPCContext(
	ctx context.Context, user username.SQLUsername,
) *rpc.Context {
	return newClientRPCContext(ctx, user,
		t.Cfg.Config,
		t.Cfg.TestingKnobs.Server,
		t.Cfg.ClusterIDContainer,
		t)
}

// RPCClientConn is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) RPCClientConn(
	test serverutils.TestFataler, user username.SQLUsername,
) *grpc.ClientConn {
	conn, err := t.RPCClientConnE(user)
	if err != nil {
		test.Fatal(err)
	}
	return conn
}

// RPCClientConnE is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) RPCClientConnE(user username.SQLUsername) (*grpc.ClientConn, error) {
	ctx := context.Background()
	rpcCtx := t.NewClientRPCContext(ctx, user)
	return rpcCtx.GRPCDialPod(t.AdvRPCAddr(), t.SQLInstanceID(), rpc.DefaultClass).Connect(ctx)
}

// GetAdminClient is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) GetAdminClient(test serverutils.TestFataler) serverpb.AdminClient {
	conn := t.RPCClientConn(test, username.RootUserName())
	return serverpb.NewAdminClient(conn)
}

// GetStatusClient is part of the serverutils.ApplicationLayerInterface.
func (t *testTenant) GetStatusClient(test serverutils.TestFataler) serverpb.StatusClient {
	conn := t.RPCClientConn(test, username.RootUserName())
	return serverpb.NewStatusClient(conn)
}

func newClientRPCContext(
	ctx context.Context,
	user username.SQLUsername,
	cfg *base.Config,
	sknobs base.ModuleTestingKnobs,
	cid *base.ClusterIDContainer,
	s serverutils.ApplicationLayerInterface,
) *rpc.Context {
	ctx = logtags.AddTag(ctx, "testclient", nil)
	ctx = logtags.AddTag(ctx, "user", user)
	ctx = logtags.AddTag(ctx, "nsql", s.SQLInstanceID())

	stopper := s.Stopper()
	if ctx.Done() == nil {
		// The RPCContext initialization wants a cancellable context,
		// since that will be used to stop async goroutines. Help
		// the test by making one.
		cctx, cancel := context.WithCancel(ctx)
		stopper.AddCloser(stop.CloserFn(cancel))
		ctx = cctx
	}
	var knobs rpc.ContextTestingKnobs
	if sknobs != nil {
		knobs = sknobs.(*TestingKnobs).ContextTestingKnobs
	}
	ccfg := rpc.MakeClientConnConfigFromBaseConfig(*cfg,
		user,
		// We pass nil as tracer parameter below, instead of the server's
		// tracer, because the tracer used for the incoming context and
		// passed to NewClientContext may not be using the same tracer and
		// we cannot mix and match tracers.
		nil, /* tracer */
		s.ClusterSettings(),
		s.Clock(),
		knobs,
	)

	rpcCtx, clientStopper := rpc.NewClientContext(ctx, ccfg)
	// Ensure that the RPC client context validates the server cluster ID.
	// This ensures that a test where the server is restarted will not let
	// its test RPC client talk to a server started by an unrelated concurrent test.
	rpcCtx.StorageClusterID.Set(ctx, cid.Get())

	stopper.AddCloser(stop.CloserFn(func() { clientStopper.Stop(ctx) }))
	return rpcCtx
}

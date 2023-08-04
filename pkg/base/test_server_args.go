// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package base

import (
	"context"
	"net"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/autoconfig/acprovider"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/listenerutil"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// TestServerArgs contains the parameters one can set when creating a test
// server. Notably, TestServerArgs are passed to serverutils.StartServer().
// They're defined in base because they need to be shared between
// testutils/serverutils (test code) and server.TestServer (non-test code).
//
// The zero value is suitable for most tests.
type TestServerArgs struct {
	// Knobs for the test server.
	Knobs TestingKnobs

	*cluster.Settings
	RaftConfig

	// PartOfCluster must be set if the TestServer is joining others in a cluster.
	// If not set (and hence the server is the only one in the cluster), the
	// default zone config will be overridden to disable all replication - so that
	// tests don't get log spam about ranges not being replicated enough. This
	// is always set to true when the server is started via a TestCluster.
	PartOfCluster bool

	// Listener (if nonempty) is the listener to use for all incoming RPCs.
	// If a listener is installed, it informs the RPC `Addr` used below. The
	// Server itself knows to close it out. This is useful for when a test wants
	// manual control over how the join flags (`JoinAddr`) are populated, and
	// installs listeners manually to know which addresses to point to.
	Listener net.Listener

	// Addr (if nonempty) is the RPC address to use for the test server.
	Addr string
	// SQLAddr (if nonempty) is the SQL address to use for the test server.
	SQLAddr string
	// HTTPAddr (if nonempty) is the HTTP address to use for the test server.
	HTTPAddr string
	// DisableTLSForHTTP if set, disables TLS for the HTTP interface.
	DisableTLSForHTTP bool

	// SecondaryTenantPortOffset if non-zero forces the network addresses
	// generated for servers started by the serverController to be offset
	// from the base addressed by the specified amount.
	SecondaryTenantPortOffset int

	// JoinAddr is the address of a node we are joining.
	//
	// If left empty and the TestServer is being added to a nonempty cluster, this
	// will be set to the address of the cluster's first node.
	JoinAddr string

	// StoreSpecs define the stores for this server. If you want more than
	// one store per node, populate this array with StoreSpecs each
	// representing a store. If no StoreSpecs are provided then a single
	// DefaultTestStoreSpec will be used.
	StoreSpecs []StoreSpec

	// Locality is optional and will set the server's locality.
	Locality roachpb.Locality

	// TempStorageConfig defines parameters for the temp storage used as
	// working memory for distributed operations and CSV importing.
	// If not initialized, will default to DefaultTestTempStorageConfig.
	TempStorageConfig TempStorageConfig

	// ExternalIODir is used to initialize field in cluster.Settings.
	ExternalIODir string

	// ExternalIODirConfig is used to initialize the same-named
	// field on the server.Config struct.
	ExternalIODirConfig ExternalIODirConfig

	// Fields copied to the server.Config.
	Insecure                    bool
	RetryOptions                retry.Options // TODO(tbg): make testing knob.
	SocketFile                  string
	ScanInterval                time.Duration
	ScanMinIdleTime             time.Duration
	ScanMaxIdleTime             time.Duration
	SSLCertsDir                 string
	TimeSeriesQueryWorkerMax    int
	TimeSeriesQueryMemoryBudget int64
	SQLMemoryPoolSize           int64
	CacheSize                   int64
	SnapshotSendLimit           int64
	SnapshotApplyLimit          int64

	// By default, test servers have AutoInitializeCluster=true set in
	// their config. If NoAutoInitializeCluster is set, that behavior is disabled
	// and the test becomes responsible for initializing the cluster.
	NoAutoInitializeCluster bool

	// If set, this will be appended to the Postgres URL by functions that
	// automatically open a connection to the server. That's equivalent to running
	// SET DATABASE=foo, which works even if the database doesn't (yet) exist.
	UseDatabase string

	// If set, this will be configured in the test server to check connections
	// from other test servers and to report in the SQL introspection.
	ClusterName string

	// Stopper can be used to stop the server. If not set, a stopper will be
	// constructed and it can be gotten through TestServerInterface.Stopper().
	Stopper *stop.Stopper

	// If set, the recording of events to the event log tables is disabled.
	DisableEventLog bool

	// If set, web session authentication will be disabled, even if the server
	// is running in secure mode.
	InsecureWebAccess bool

	// IF set, the demo login endpoint will be enabled.
	EnableDemoLoginEndpoint bool

	// Tracer, if set, will be used by the Server for creating Spans.
	Tracer *tracing.Tracer
	// TracingDefault kicks in if Tracer is not set. It is passed to the Tracer
	// that will be created for the server.
	TracingDefault tracing.TracingMode
	// If set, a TraceDir is initialized at the provided path.
	TraceDir string

	// TestServer will probabilistically start a single test tenant on each node
	// for multi-tenant testing, and default all connections through that tenant.
	// Use this flag to change this behavior. You might want/need to alter this
	// behavior if your test case is already leveraging tenants, or if some of the
	// functionality being tested is not accessible from within tenants. See
	// DefaultTestTenantOptions for alternative options that suits your test case.
	DefaultTestTenant DefaultTestTenantOptions

	// StartDiagnosticsReporting checks cluster.TelemetryOptOut(), and
	// if not disabled starts the asynchronous goroutine that checks for
	// CockroachDB upgrades and periodically reports diagnostics to
	// Cockroach Labs. Should remain disabled during unit testing.
	StartDiagnosticsReporting bool

	// ObsServiceAddr is the address to which events will be exported over OTLP.
	// If empty, exporting events is inhibited.
	ObsServiceAddr string

	// AutoConfigProvider provides auto-configuration tasks to apply on
	// the cluster during server initialization.
	AutoConfigProvider acprovider.Provider
}

// TestClusterArgs contains the parameters one can set when creating a test
// cluster. It contains a TestServerArgs instance which will be copied over to
// every server.
//
// The zero value means "ReplicationAuto".
type TestClusterArgs struct {
	// ServerArgs will be copied and potentially adjusted according to the
	// ReplicationMode for each constituent TestServer. Used for all the servers
	// not overridden in ServerArgsPerNode.
	ServerArgs TestServerArgs
	// ReplicationMode controls how replication is to be done in the cluster.
	ReplicationMode TestClusterReplicationMode
	// If true, nodes will be started in parallel. This is useful in
	// testing certain recovery scenarios, although it makes store/node
	// IDs unpredictable. Even in ParallelStart mode, StartTestCluster
	// waits for all nodes to start before returning.
	ParallelStart bool

	// ServerArgsPerNode override the default ServerArgs with the value in this
	// map. The map's key is an index within TestCluster.Servers. If there is
	// no entry in the map for a particular server, the default ServerArgs are
	// used.
	//
	// These are indexes: the key 0 corresponds to the first node.
	//
	// A copy of an entry from this map will be copied to each individual server
	// and potentially adjusted according to ReplicationMode.
	ServerArgsPerNode map[int]TestServerArgs

	// If set, listeners will be created from the below registry and they will be
	// retained across restarts (i.e. servers are kept on the same ports, but
	// avoiding races where another process grabs the port while the server is
	// down). It's also possible not to set this field but set a *ReusableListener
	// directly in TestServerArgs.Listener. If a non-reusable listener is set in
	// that field, RestartServer will return an error to guide the developer
	// towards a non-flaky pattern.
	ReusableListenerReg *listenerutil.ListenerRegistry
}

// DefaultTestTenantOptions specifies the conditions under which the default
// test tenant will be started.
type DefaultTestTenantOptions struct {
	testBehavior testBehavior

	// Whether the testserver will allow or block attempts to create
	// additional secondary tenants. (Default is to block.)
	allowAdditionalTenants bool

	// If test tenant is disabled, issue and label to link in log message.
	issueNum int
	label    string
}

type testBehavior int8

const (
	ttProb testBehavior = iota
	ttEnabled
	ttDisabled
)

var (
	// TestTenantProbabilisticOnly will start the default test tenant on a
	// probabilistic basis. It will also prevent the starting of additional
	// tenants by raising an error if it is attempted.
	// This is the default behavior.
	TestTenantProbabilisticOnly = DefaultTestTenantOptions{testBehavior: ttProb, allowAdditionalTenants: false}
	// TestTenantProbabilistic will start the default test tenant on a
	// probabilistic basis. It allows the starting of additional tenants.
	TestTenantProbabilistic = DefaultTestTenantOptions{testBehavior: ttProb, allowAdditionalTenants: true}
	// TestTenantAlwaysEnabled will always start the default test tenant. This is useful
	// for quickly verifying that a test works with tenants enabled.
	//
	// Note: this value should not be used for checked in test code
	// unless there is a good reason to do so. We want the common case
	// to use TestTenantProbabilistic or TestTenantProbabilisticOnly.
	TestTenantAlwaysEnabled = DefaultTestTenantOptions{testBehavior: ttEnabled, allowAdditionalTenants: true}

	// TODOTestTenantDisabled should not be used anymore. Use the
	// other values instead.
	// TODO(#76378): Review existing tests and use the proper value instead.
	TODOTestTenantDisabled = DefaultTestTenantOptions{testBehavior: ttDisabled, allowAdditionalTenants: true}

	// TestControlsTenantsExplicitly is used when the test wants to
	// manage its own secondary tenants and tenant servers.
	TestControlsTenantsExplicitly = DefaultTestTenantOptions{testBehavior: ttDisabled, allowAdditionalTenants: true}

	// TestIsSpecificToStorageLayerAndNeedsASystemTenant is used when
	// the test needs to be given access to a SQL conn to a tenant with
	// sufficient capabilities to access all the storage layer.
	// (Initially that'd be "the" system tenant.)
	TestIsSpecificToStorageLayerAndNeedsASystemTenant = DefaultTestTenantOptions{testBehavior: ttDisabled, allowAdditionalTenants: true}

	// TestNeedsTightIntegrationBetweenAPIsAndTestingKnobs is used when
	// a test wants to use a single set of testing knobs for both the
	// storage layer and the SQL layer and for simplicity of the test
	// code we want to give that test a simplified environment.
	//
	// Note: it is debatable whether the gain in test code simplicity is
	// worth the cost of never running that test with the virtualization
	// layer active.
	TestNeedsTightIntegrationBetweenAPIsAndTestingKnobs = TestIsSpecificToStorageLayerAndNeedsASystemTenant

	// InternalNonDefaultDecision is a sentinel value used inside a
	// mechanism in serverutils. Should not be used by tests directly.
	//
	// TODO(#76378): Investigate how we can remove the need for this
	// sentinel value.
	InternalNonDefaultDecision = DefaultTestTenantOptions{testBehavior: ttDisabled, allowAdditionalTenants: true}
)

func (do DefaultTestTenantOptions) AllowAdditionalTenants() bool {
	return do.allowAdditionalTenants
}

func (do DefaultTestTenantOptions) TestTenantAlwaysEnabled() bool {
	return do.testBehavior == ttEnabled
}

func (do DefaultTestTenantOptions) TestTenantAlwaysDisabled() bool {
	return do.testBehavior == ttDisabled
}

func (do DefaultTestTenantOptions) IssueRef() (int, string) {
	return do.issueNum, do.label
}

// TestDoesNotWorkWithSecondaryTenantsButWeDontKnowWhyYet can be used
// to disable virtualization because the test doesn't appear to be compatible
// with it, and we don't understand it yet.
// It should link to a github issue with label C-test-failure.
func TestDoesNotWorkWithSecondaryTenantsButWeDontKnowWhyYet(
	issueNumber int,
) DefaultTestTenantOptions {
	return DefaultTestTenantOptions{
		testBehavior:           ttDisabled,
		allowAdditionalTenants: true,
		issueNum:               issueNumber,
		label:                  "C-test-failure",
	}
}

// TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet can be
// used to disable virtualization because the test exercises a feature
// known not to work with virtualization enabled yet but we wish it to
// eventually.
//
// It should link to a github issue with label C-bug
// and the issue should be linked to an epic under INI-213 or INI-214.
func TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(
	issueNumber int,
) DefaultTestTenantOptions {
	return DefaultTestTenantOptions{
		testBehavior:           ttDisabled,
		allowAdditionalTenants: true,
		issueNum:               issueNumber,
		label:                  "C-bug",
	}
}

var (
	// DefaultTestStoreSpec is just a single in memory store of 512 MiB
	// with no special attributes.
	DefaultTestStoreSpec = StoreSpec{
		InMemory: true,
		Size: SizeSpec{
			InBytes: 512 << 20,
		},
	}
)

// DefaultTestTempStorageConfig is the associated temp storage for
// DefaultTestStoreSpec that is in-memory.
// It has a maximum size of 100MiB.
func DefaultTestTempStorageConfig(st *cluster.Settings) TempStorageConfig {
	return DefaultTestTempStorageConfigWithSize(st, DefaultInMemTempStorageMaxSizeBytes)
}

// DefaultTestTempStorageConfigWithSize is the associated temp storage for
// DefaultTestStoreSpec that is in-memory with the customized maximum size.
func DefaultTestTempStorageConfigWithSize(
	st *cluster.Settings, maxSizeBytes int64,
) TempStorageConfig {
	monitor := mon.NewMonitor(
		"in-mem temp storage",
		mon.DiskResource,
		nil,             /* curCount */
		nil,             /* maxHist */
		1024*1024,       /* increment */
		maxSizeBytes/10, /* noteworthy */
		st,
	)
	monitor.Start(context.Background(), nil /* pool */, mon.NewStandaloneBudget(maxSizeBytes))
	return TempStorageConfig{
		InMemory: true,
		Mon:      monitor,
		Settings: st,
	}
}

// TestSharedProcessTenantArgs are the arguments to
// TestServer.StartSharedProcessTenant.
type TestSharedProcessTenantArgs struct {
	// TenantName is the name of the tenant to be created. It must be set.
	TenantName roachpb.TenantName
	// TenantID is the ID of the tenant to be created. If not set, an ID is
	// assigned automatically.
	TenantID roachpb.TenantID

	Knobs TestingKnobs

	// If set, this will be appended to the Postgres URL by functions that
	// automatically open a connection to the server. That's equivalent to running
	// SET DATABASE=foo, which works even if the database doesn't (yet) exist.
	UseDatabase string
}

// TestTenantArgs are the arguments to TestServer.StartTenant.
type TestTenantArgs struct {
	TenantName roachpb.TenantName

	TenantID roachpb.TenantID

	// DisableCreateTenant disables the explicit creation of a tenant when
	// StartTenant is attempted. It's used in cases where we want to validate
	// that a tenant doesn't start if it isn't existing.
	DisableCreateTenant bool

	// Settings allows the caller to control the settings object used for the
	// tenant cluster.
	Settings *cluster.Settings

	// Stopper, if not nil, is used to stop the tenant manually otherwise the
	// TestServer stopper will be used.
	Stopper *stop.Stopper

	// TestingKnobs for the test server.
	TestingKnobs TestingKnobs

	// Test server starts with secure mode by default. When this is set to true
	// it will switch to insecure
	ForceInsecure bool

	// MemoryPoolSize is the amount of memory in bytes that can be used by SQL
	// clients to store row data in server RAM.
	MemoryPoolSize int64

	// TempStorageConfig is used to configure temp storage, which stores
	// ephemeral data when processing large queries.
	TempStorageConfig *TempStorageConfig

	// ExternalIODirConfig is used to initialize the same-named
	// field on the server.Config struct.
	ExternalIODirConfig ExternalIODirConfig

	// ExternalIODir is used to initialize the same-named field on
	// the params.Settings struct.
	ExternalIODir string

	// If set, this will be appended to the Postgres URL by functions that
	// automatically open a connection to the server. That's equivalent to running
	// SET DATABASE=foo, which works even if the database doesn't (yet) exist.
	UseDatabase string

	// Skip check for tenant existence when running the test.
	SkipTenantCheck bool

	// Do not wait for tenant record cache to be populated before
	// starting a tenant server.
	SkipWaitForTenantCache bool

	// Locality is used to initialize the same-named field on the server.Config
	// struct.
	Locality roachpb.Locality

	// SSLCertsDir is a path to a custom certs dir. If empty, will use the default
	// embedded certs.
	SSLCertsDir string

	// DisableTLSForHTTP, if set, disables TLS for the HTTP listener.
	DisableTLSForHTTP bool

	// EnableDemoLoginEndpoint enables the HTTP GET endpoint for user logins,
	// which a feature unique to the demo shell.
	EnableDemoLoginEndpoint bool

	// StartingRPCAndSQLPort, if it is non-zero, is added to the tenant ID in order to
	// determine the tenant's SQL+RPC port.
	// If set, force disables SplitListenSQL.
	StartingRPCAndSQLPort int

	// StartingHTTPPort, if it is non-zero, is added to the tenant ID in order to
	// determine the tenant's HTTP port.
	StartingHTTPPort int

	// TracingDefault controls whether the tracing will be on or off by default.
	TracingDefault tracing.TracingMode

	// GoroutineDumpDirName is used to initialize the same named field on the
	// SQLServer.BaseConfig field. It is used as the directory name for
	// goroutine dumps using goroutinedumper. If set, this directory should
	// be cleaned up once the test completes.
	GoroutineDumpDirName string

	// HeapProfileDirName is used to initialize the same named field on the
	// SQLServer.BaseConfig field. It is the directory name for heap profiles using
	// heapprofiler. If empty, no heap profiles will be collected during the test.
	// If set, this directory should be cleaned up after the test completes.
	HeapProfileDirName string

	// StartDiagnosticsReporting checks cluster.TelemetryOptOut(), and
	// if not disabled starts the asynchronous goroutine that checks for
	// CockroachDB upgrades and periodically reports diagnostics to
	// Cockroach Labs. Should remain disabled during unit testing.
	StartDiagnosticsReporting bool
}

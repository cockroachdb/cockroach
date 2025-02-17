// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package base

import (
	"context"
	"net"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
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

	// Settings for the server.
	//
	// When TestServerArgs is used for a multi-node test cluster, the Settings
	// object is cloned for each node (see cluster.TestingCloneClusterSettings).
	// To effect a change in a node's settings, ClusterSettings() should be used.
	Settings *cluster.Settings

	RaftConfig RaftConfig

	// PartOfCluster must be set if the TestServer is joining others in a cluster.
	// If not set (and hence the server is the only one in the cluster), the
	// default zone config will be overridden to disable all replication - so that
	// tests don't get log spam about ranges not being replicated enough. This
	// is always set to true when the server is started via a TestCluster, unless
	// the StartSingleNode TestClusterArgs is set.
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

	// ApplicationInternalRPCPortMin/PortMax define the range of TCP ports
	// used to start the internal RPC service for application-level
	// servers. This service is used for node-to-node RPC traffic and to
	// serve data for 'debug zip'.
	ApplicationInternalRPCPortMin int
	ApplicationInternalRPCPortMax int

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

	// ExternalIODirConfig is used to initialize the same-named
	// field on the server.Config struct.
	ExternalIODirConfig ExternalIODirConfig

	// ExternalIODir is used to initialize the same-named field on
	// the server.Config struct.
	ExternalIODir string

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

	// If set, don't start the SQL service for this test.
	DisableSQLServer bool

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

	// DefaultTestTenant determines whether a test's application
	// workload will be redirected to a virtual cluster (secondary
	// tenant) automatically.
	//
	// The default behavior is to do this probabilistically. See
	// DefaultTestTenantOptions and its various instances defined
	// below for alternative options that suits your test case.
	DefaultTestTenant DefaultTestTenantOptions

	// DefaultTenantName is the name of the tenant created implicitly according
	// to DefaultTestTenant. It is typically `test-tenant` for unit tests and
	// always `demoapp` for the cockroach demo.
	DefaultTenantName roachpb.TenantName

	// StartDiagnosticsReporting checks cluster.TelemetryOptOut(), and
	// if not disabled starts the asynchronous goroutine that checks for
	// CockroachDB upgrades and periodically reports diagnostics to
	// Cockroach Labs. Should remain disabled during unit testing.
	StartDiagnosticsReporting bool
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
	// StartSingleNode will initialize the cluster like 'cockroach
	// start-single-node'. Attempts to add more than one node to the cluster will
	// fail.
	StartSingleNode bool

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

// DefaultTestTenantOptions specifies the conditions under which a
// virtual cluster (secondary tenant) will be automatically started to
// bear load for a unit test.
type DefaultTestTenantOptions struct {
	testBehavior testBehavior

	// Whether the testserver will allow or block attempts to create
	// additional virtual clusters. (Default is to block.)
	allowAdditionalTenants bool

	// Whether implicit uses of the ApplicationLayerInterface or
	// StorageLayerInterface result in warnings/notices.
	//
	// We use a "no" boolean, so that the default value results in "do
	// warn".
	noWarnImplicitInterfaces bool

	// If test tenant is disabled, issue and label to link in log message.
	issueNum int
	label    string
}

type testBehavior int16

const (
	ttEnabled testBehavior = 1 << iota
	ttDisabled
	ttSharedProcess
	ttExternalProcess
)

var (
	// TestTenantProbabilisticOnly starts the test under a virtual
	// cluster on a probabilistic basis. It will also prevent the
	// starting of additional virtual clusters by raising an error if it
	// is attempted. This is the default behavior.
	TestTenantProbabilisticOnly = DefaultTestTenantOptions{allowAdditionalTenants: false}

	// TestTenantProbabilistic starts the test under a virtual
	// cluster on a probabilistic basis. It allows the starting of
	// additional virtual clusters.
	TestTenantProbabilistic = DefaultTestTenantOptions{allowAdditionalTenants: true}

	// TestTenantAlwaysEnabled will always redirect the test workload to
	// a virtual cluster. This is useful for quickly verifying that a
	// test works under cluster virtualization.
	//
	// Note: this value should not be used for checked in test code
	// unless there is a good reason to do so. We want the common case
	// to use TestTenantProbabilistic or TestTenantProbabilisticOnly.
	TestTenantAlwaysEnabled = DefaultTestTenantOptions{testBehavior: ttEnabled, allowAdditionalTenants: true}

	// ExternalTestTenantAlwaysEnabled will always redirect the test workload to
	// an external process virtual cluster. This is useful for quickly verifying that a
	// test works under cluster virtualization.
	//
	// Note: this value should not be used for checked in test code
	// unless there is a good reason to do so. We want the common case
	// to use TestTenantProbabilistic or TestTenantProbabilisticOnly.
	ExternalTestTenantAlwaysEnabled = DefaultTestTenantOptions{testBehavior: ttEnabled | ttExternalProcess, allowAdditionalTenants: true}

	// SharedTestTenantAlwaysEnabled will always redirect the test workload to
	// a shared process virtual cluster. This is useful for quickly verifying that a
	// test works under cluster virtualization.
	//
	// Note: this value should not be used for checked in test code
	// unless there is a good reason to do so. We want the common case
	// to use TestTenantProbabilistic or TestTenantProbabilisticOnly.
	SharedTestTenantAlwaysEnabled = DefaultTestTenantOptions{testBehavior: ttEnabled | ttSharedProcess, allowAdditionalTenants: true}

	// TODOTestTenantDisabled should not be used anymore. Use the
	// other values instead.
	// TODO(#76378): Review existing tests and use the proper value instead.
	TODOTestTenantDisabled = DefaultTestTenantOptions{testBehavior: ttDisabled, allowAdditionalTenants: true}

	// TestRequiresExplicitSQLConnection is used when the test is unable to pass
	// the cluster as an option in the connection URL. The test could still
	// probabilistically use an external process test virtual cluster, but
	// disables the selection of a shared process test virtual cluster.
	TestRequiresExplicitSQLConnection = DefaultTestTenantOptions{
		testBehavior:             ttExternalProcess,
		allowAdditionalTenants:   true,
		noWarnImplicitInterfaces: true,
	}

	// TestControlsTenantsExplicitly is used when the test wants to
	// manage its own secondary tenants and tenant servers.
	TestControlsTenantsExplicitly = DefaultTestTenantOptions{
		testBehavior:             ttDisabled,
		allowAdditionalTenants:   true,
		noWarnImplicitInterfaces: true,
	}

	// TestIsSpecificToStorageLayerAndNeedsASystemTenant is used when
	// the test needs to be given access to a SQL conn to a tenant with
	// sufficient capabilities to access all the storage layer.
	// (Initially that'd be "the" system tenant.)
	TestIsSpecificToStorageLayerAndNeedsASystemTenant = DefaultTestTenantOptions{
		testBehavior:             ttDisabled,
		allowAdditionalTenants:   true,
		noWarnImplicitInterfaces: true,
	}

	// TestNeedsTightIntegrationBetweenAPIsAndTestingKnobs is used when
	// a test wants to use a single set of testing knobs for both the
	// storage layer and the SQL layer and for simplicity of the test
	// code we want to give that test a simplified environment.
	//
	// Note: it is debatable whether the gain in test code simplicity is
	// worth the cost of never running that test with the virtualization
	// layer active.
	TestNeedsTightIntegrationBetweenAPIsAndTestingKnobs = TestIsSpecificToStorageLayerAndNeedsASystemTenant
)

func (do DefaultTestTenantOptions) AllowAdditionalTenants() bool {
	return do.allowAdditionalTenants
}

func (do DefaultTestTenantOptions) TestTenantAlwaysEnabled() bool {
	return do.testBehavior&ttEnabled != 0
}

func (do DefaultTestTenantOptions) TestTenantAlwaysDisabled() bool {
	return do.testBehavior&ttDisabled != 0
}

func (do DefaultTestTenantOptions) TestTenantNoDecisionMade() bool {
	// Exactly one of ttEnabled or ttDisabled must be set.
	if (do.testBehavior&ttEnabled != 0) == (do.testBehavior&ttDisabled != 0) {
		return true
	}
	if do.testBehavior&ttEnabled != 0 {
		// If ttEnabled is set, then exactly one of ttSharedProcess or
		// ttExternalProcess must be set.
		if (do.testBehavior&ttExternalProcess != 0) == (do.testBehavior&ttSharedProcess != 0) {
			return true
		}
	}
	return false
}

func (do DefaultTestTenantOptions) SharedProcessMode() bool {
	return do.testBehavior&ttSharedProcess != 0
}

func (do DefaultTestTenantOptions) ExternalProcessMode() bool {
	return do.testBehavior&ttExternalProcess != 0
}

// WarnImplicitInterfaces indicates whether to warn when the test code
// uses ApplicationLayerInterface or StorageLayerInterface
// implicitly.
func (do DefaultTestTenantOptions) WarnImplicitInterfaces() bool {
	return !do.noWarnImplicitInterfaces
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
// known not to work with virtualization enabled yet, but we wish it to
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

// TestDoesNotWorkWithSharedProcessModeButWeDontKnowWhyYet can be used to
// disable selecting a shared process test virtual cluster probabilistically
// because the test doesn't appear to be compatible with it, and we don't
// understand it yet.
//
// The `baseOptions` are adjusted to restrict the default test virtual cluster
// process mode selection to an external process virtual cluster only. Using
// this function with `baseOptions` that explicitly disable a test virtual
// cluster, or opt specifically for a shared process test virtual cluster does
// not make sense and will cause a panic.
//
// It should link to a github issue with label C-test-failure.
func TestDoesNotWorkWithSharedProcessModeButWeDontKnowWhyYet(
	baseOptions DefaultTestTenantOptions, issueNumber int,
) DefaultTestTenantOptions {
	if baseOptions.testBehavior&ttDisabled != 0 {
		panic("test behavior cannot be disabled, please refer to one of the other options to disable secondary virtual clusters with a reason")
	}
	if baseOptions.testBehavior&ttSharedProcess != 0 {
		panic("test behavior cannot be set to a shared process.")
	}
	return DefaultTestTenantOptions{
		testBehavior:           baseOptions.testBehavior | ttExternalProcess,
		allowAdditionalTenants: baseOptions.allowAdditionalTenants,
		issueNum:               issueNumber,
		label:                  "C-test-failure",
	}
}

// TestIsForStuffThatShouldWorkWithSharedProcessModeButDoesntYet can be used to
// disable selecting a shared process test virtual cluster probabilistically
// because the test exercises a feature known not to work with a shared process
// virtual cluster, but we wish it to eventually.
//
// The `baseOptions` are adjusted to restrict the default test virtual cluster
// process mode selection to an external process virtual cluster only. Using
// this function with `baseOptions` that explicitly disable a test virtual
// cluster, or opt specifically for a shared process test virtual cluster does
// not make sense and will cause a panic.
//
// It should link to a github issue with label C-bug and the issue should be
// linked to an epic under INI-213 or INI-214.
func TestIsForStuffThatShouldWorkWithSharedProcessModeButDoesntYet(
	baseOptions DefaultTestTenantOptions, issueNumber int,
) DefaultTestTenantOptions {
	if baseOptions.testBehavior&ttDisabled != 0 {
		panic("test behavior cannot be disabled, please refer to one of the other options to disable secondary virtual clusters with a reason")
	}
	if baseOptions.testBehavior&ttSharedProcess != 0 {
		panic("test behavior cannot be set to a shared process.")
	}
	return DefaultTestTenantOptions{
		testBehavior:           baseOptions.testBehavior | ttExternalProcess,
		allowAdditionalTenants: baseOptions.allowAdditionalTenants,
		issueNum:               issueNumber,
		label:                  "C-bug",
	}
}

// InternalNonDefaultDecision builds a sentinel value used inside a
// mechanism in serverutils. Should not be used by tests directly.
func InternalNonDefaultDecision(
	baseArg DefaultTestTenantOptions, enable bool, shared bool,
) DefaultTestTenantOptions {
	var tb testBehavior
	if enable {
		tb |= ttEnabled
		if shared {
			tb |= ttSharedProcess
		} else {
			tb |= ttExternalProcess
		}
	} else {
		tb |= ttDisabled
	}
	baseArg.testBehavior = tb
	return baseArg
}

var (
	// DefaultTestStoreSpec is just a single in memory store of 512 MiB
	// with no special attributes.
	DefaultTestStoreSpec = StoreSpec{
		InMemory: true,
		Size: storagepb.SizeSpec{
			Capacity: 512 << 20,
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
	monitor := mon.NewMonitor(mon.Options{
		Name:      mon.MakeMonitorName("in-mem temp storage"),
		Res:       mon.DiskResource,
		Increment: 1024 * 1024,
		Settings:  st,
	})
	monitor.Start(context.Background(), nil /* pool */, mon.NewStandaloneBudget(maxSizeBytes))
	return TempStorageConfig{
		InMemory: true,
		Mon:      monitor,
		Spec:     DefaultTestStoreSpec,
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

	// Skip check for tenant existence when running the test.
	SkipTenantCheck bool

	Settings *cluster.Settings
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
	// the server.Config struct.
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

	// Tracer, if set, will be used by the Server for creating Spans.
	Tracer *tracing.Tracer

	// TracingDefault controls whether the tracing will be on or off by default,
	// if Tracer is not set.
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

	// CPUProfileDirName is used to initialize the same named field on the
	// SQLServer.BaseConfig field. It is the directory name for cpu profiles
	// using cpuprofiler. If empty, no cpu profiles will be collected during the
	// test. If set, this directory should be cleaned up after the test
	// completes.
	CPUProfileDirName string

	// StartDiagnosticsReporting checks cluster.TelemetryOptOut(), and
	// if not disabled starts the asynchronous goroutine that checks for
	// CockroachDB upgrades and periodically reports diagnostics to
	// Cockroach Labs. Should remain disabled during unit testing.
	StartDiagnosticsReporting bool
}

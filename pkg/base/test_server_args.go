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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
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
	// TenantAddr is the tenant KV address to use for the test server. If this
	// is nil, the tenant server will be set up using a random port. If this
	// is the empty string, no tenant server will be set up.
	TenantAddr *string
	// HTTPAddr (if nonempty) is the HTTP address to use for the test server.
	HTTPAddr string
	// DisableTLSForHTTP if set, disables TLS for the HTTP interface.
	DisableTLSForHTTP bool

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
	DisableWebSessionAuthentication bool

	// IF set, the demo login endpoint will be enabled.
	EnableDemoLoginEndpoint bool

	// Tracer, if set, will be used by the Server for creating Spans.
	Tracer *tracing.Tracer
	// TracingDefault kicks in if Tracer is not set. It is passed to the Tracer
	// that will be created for the server.
	TracingDefault tracing.TracingMode
	// If set, a TraceDir is initialized at the provided path.
	TraceDir string

	// DisableSpanConfigs disables the use of the span configs infrastructure
	// (in favor of the gossiped system config span). It's equivalent to setting
	// COCKROACH_DISABLE_SPAN_CONFIGS, and is only intended for tests written
	// with the system config span in mind.
	//
	// TODO(irfansharif): Remove all uses of this when we rip out the system
	// config span.
	DisableSpanConfigs bool
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
	monitor.Start(context.Background(), nil /* pool */, mon.MakeStandaloneBudget(maxSizeBytes))
	return TempStorageConfig{
		InMemory: true,
		Mon:      monitor,
		Settings: st,
	}
}

// TestClusterReplicationMode represents the replication settings for a TestCluster.
type TestClusterReplicationMode int

//go:generate stringer -type=TestClusterReplicationMode

const (
	// ReplicationAuto means that ranges are replicated according to the
	// production default zone config. Replication is performed as in
	// production, by the replication queue.
	// If ReplicationAuto is used, StartTestCluster() blocks until the initial
	// ranges are fully replicated.
	ReplicationAuto TestClusterReplicationMode = iota
	// ReplicationManual means that the split, merge and replication queues of all
	// servers are stopped, and the test must manually control splitting, merging
	// and replication through the TestServer.
	// Note that the server starts with a number of system ranges,
	// all with a single replica on node 1.
	ReplicationManual
)

// TestTenantArgs are the arguments used when creating a tenant from a
// TestServer.
type TestTenantArgs struct {
	TenantID roachpb.TenantID

	// Existing, if true, indicates an existing tenant, rather than a new tenant
	// to be created by StartTenant.
	Existing bool

	// Settings allows the caller to control the settings object used for the
	// tenant cluster.
	Settings *cluster.Settings

	// AllowSettingClusterSettings, if true, allows the tenant to set in-memory
	// cluster settings.
	AllowSettingClusterSettings bool

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

	// Locality is used to initialize the same-named field on the server.Config
	// struct.
	Locality roachpb.Locality

	// SSLCertsDir is a path to a custom certs dir. If empty, will use the default
	// embedded certs.
	SSLCertsDir string

	// StartingSQLPort, if it is non-zero, is added to the tenant ID in order to
	// determine the tenant's SQL port.
	StartingSQLPort int

	// StartingHTTPPort, if it is non-zero, is added to the tenant ID in order to
	// determine the tenant's HTTP port.
	StartingHTTPPort int

	// TracingDefault controls whether the tracing will be on or off by default.
	TracingDefault tracing.TracingMode

	// RPCHeartbeatInterval controls how often the tenant sends Ping requests.
	RPCHeartbeatInterval time.Duration

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
}

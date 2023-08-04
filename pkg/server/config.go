// Copyright 2015 The Cockroach Authors.
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
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/base/serverident"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/docs"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/autoconfig/acprovider"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/redact"
)

// Context defaults.
const (
	// DefaultCacheSize is the default size of the Pebble cache. We default the
	// cache size to 128MiB and SQL memory pool size to 256 MiB. Larger values
	// might provide significantly better performance, but we're not sure what
	// type of system we're running on (development or production or some shared
	// environment). Production users should almost certainly override these
	// settings and we'll warn in the logs about doing so.
	DefaultCacheSize         = 128 << 20 // 128 MiB
	defaultSQLMemoryPoolSize = 256 << 20 // 256 MiB
	defaultScanInterval      = 10 * time.Minute
	defaultScanMinIdleTime   = 10 * time.Millisecond
	defaultScanMaxIdleTime   = 1 * time.Second

	DefaultStorePath = "cockroach-data"
	// TempDirPrefix is the filename prefix of any temporary subdirectory
	// created.
	TempDirPrefix = "cockroach-temp"
	// TempDirsRecordFilename is the filename for the record file
	// that keeps track of the paths of the temporary directories created.
	TempDirsRecordFilename = "temp-dirs-record.txt"
	defaultEventLogEnabled = true

	maximumMaxClockOffset = 5 * time.Second

	// toleratedOffsetMultiplier is the MaxOffset multiplier used for
	// ToleratedOffset, which determines the tolerated clock skew between this
	// node and the cluster before self-terminating. It is conservatively set to
	// 80% to avoid exceeding MaxOffset.
	toleratedOffsetMultiplier = 0.8

	minimumNetworkFileDescriptors     = 256
	recommendedNetworkFileDescriptors = 5000

	defaultSQLTableStatCacheSize = 256

	// This comes out to 1024 cache entries.
	defaultSQLQueryCacheSize = 8 * 1024 * 1024
)

var productionSettingsWebpage = fmt.Sprintf(
	"please see %s for more details",
	docs.URL("recommended-production-settings.html"),
)

// MaxOffsetType stores the configured MaxOffset.
type MaxOffsetType time.Duration

// Type implements the pflag.Value interface.
func (mo *MaxOffsetType) Type() string {
	return "MaxOffset"
}

// Set implements the pflag.Value interface.
func (mo *MaxOffsetType) Set(v string) error {
	nanos, err := time.ParseDuration(v)
	if err != nil {
		return err
	}
	if nanos > maximumMaxClockOffset {
		return errors.Errorf("%s is not a valid max offset, must be less than %v.", v, maximumMaxClockOffset)
	}
	*mo = MaxOffsetType(nanos)
	return nil
}

// String implements the pflag.Value interface.
func (mo *MaxOffsetType) String() string {
	return time.Duration(*mo).String()
}

// BaseConfig holds parameters that are needed to setup either a KV or a SQL
// server.
type BaseConfig struct {
	Settings *cluster.Settings
	*base.Config

	Tracer *tracing.Tracer

	// idProvider contains the tenant and server identity.
	idProvider *idProvider

	// IDContainer is the Node ID / SQL Instance ID container
	// that will contain the ID for the server to instantiate.
	IDContainer *base.NodeIDContainer

	// ClusterIDContainer is the Cluster ID container for the server to
	// instantiate.
	ClusterIDContainer *base.ClusterIDContainer

	// AmbientCtx is used to annotate contexts used inside the server.
	AmbientCtx log.AmbientContext

	// MaxOffset is the maximum clock offset for the cluster. If real clock skew
	// exceeds this limit, it may result in linearizability violations. Increasing
	// this will increase the frequency of ReadWithinUncertaintyIntervalError and
	// the write latency of global tables.
	//
	// Nodes will self-terminate if they detect that their clock skew with other
	// nodes is too large, see ToleratedOffset().
	MaxOffset MaxOffsetType

	// DisableMaxOffsetCheck disables the MaxOffset check with other cluster nodes.
	// The operator assumes responsibility for ensuring real clock skew never
	// exceeds MaxOffset. See also ToleratedOffset().
	DisableMaxOffsetCheck bool

	// DisableRuntimeStatsMonitor prevents this server from starting the
	// async task that collects runtime stats and triggers
	// heap/goroutine dumps under high load.
	DisableRuntimeStatsMonitor bool

	// RuntimeStatSampler, if non-nil, will be used as source for
	// run-time metrics instead of constructing a fresh one.
	RuntimeStatSampler *status.RuntimeStatSampler

	// GoroutineDumpDirName is the directory name for goroutine dumps using
	// goroutinedumper. Only used if DisableRuntimeStatsMonitor is false.
	GoroutineDumpDirName string

	// HeapProfileDirName is the directory name for heap profiles using
	// heapprofiler. If empty, no heap profiles will be collected. Only
	// used if DisableRuntimeStatsMonitor is false.
	HeapProfileDirName string

	// CPUProfileDirName is the directory name for CPU profile dumps.
	// Only used if DisableRuntimeStatsMonitor is false.
	CPUProfileDirName string

	// InflightTraceDirName is the directory name for job traces.
	InflightTraceDirName string

	// DefaultZoneConfig is used to set the default zone config inside the server.
	// It can be overridden during tests by setting the DefaultZoneConfigOverride
	// server testing knob. Whatever is installed here is in turn used to
	// initialize stores, which need a default span config.
	DefaultZoneConfig zonepb.ZoneConfig

	// Locality is a description of the topography of the server.
	Locality roachpb.Locality

	// StorageEngine specifies the engine type (eg. rocksdb, pebble) to use to
	// instantiate stores.
	StorageEngine enginepb.EngineType

	// Disables the default test tenant.
	DisableDefaultTestTenant bool

	// TestingKnobs is used for internal test controls only.
	TestingKnobs base.TestingKnobs

	// TestingInsecureWebAccess enables uses of the HTTP and UI
	// endpoints without a valid authentication token. This should be
	// used only in tests what want a secure cluster with RPC
	// auth but no auth in HTTP.
	TestingInsecureWebAccess bool

	// EnableDemoLoginEndpoint enables the HTTP GET endpoint for user logins,
	// which a feature unique to the demo shell.
	EnableDemoLoginEndpoint bool

	// ReadyFn is called when the server has started listening on its
	// sockets.
	//
	// The bool parameter is true if the server is not bootstrapped yet, will not
	// bootstrap itself and will be waiting for an `init` command or accept
	// bootstrapping from a joined node.
	//
	// This method is invoked from the main start goroutine, so it should not
	// do nontrivial work.
	ReadyFn func(waitForInit bool)

	// Stores is specified to enable durable key-value storage.
	Stores base.StoreSpecList

	// SharedStorage is specified to enable disaggregated shared storage.
	SharedStorage string
	*cloud.ExternalStorageAccessor

	// StartDiagnosticsReporting starts the asynchronous goroutine that
	// checks for CockroachDB upgrades and periodically reports
	// diagnostics to Cockroach Labs.
	// Should remain disabled during unit testing.
	StartDiagnosticsReporting bool

	// DisableHTTPListener prevents this server from starting a TCP
	// listener for the HTTP service. Instead, it is expected that some
	// other service (typically, the serverController) will accept and
	// route requests instead.
	DisableHTTPListener bool

	// DisableSQLListener prevents this server from starting a TCP
	// listener for the SQL service. Instead, it is expected that some
	// other service (typically, the serverController) will accept and
	// route SQL connections instead.
	DisableSQLListener bool

	// ObsServiceAddr is the address of the OTLP sink to send events to, if any.
	// These events are meant for the Observability Service, but they might pass
	// through an OpenTelemetry Collector.
	ObsServiceAddr string

	// AutoConfigProvider provides auto-configuration tasks to apply on
	// the cluster during server initialization.
	AutoConfigProvider acprovider.Provider
}

// MakeBaseConfig returns a BaseConfig with default values.
func MakeBaseConfig(st *cluster.Settings, tr *tracing.Tracer, storeSpec base.StoreSpec) BaseConfig {
	if tr == nil {
		panic("nil Tracer")
	}
	baseCfg := BaseConfig{Config: new(base.Config)}
	baseCfg.SetDefaults(st, tr, storeSpec)

	return baseCfg
}

// SetDefaults resets the values in BaseConfig but while preserving
// the Config reference. Enables running tests multiple times.
func (cfg *BaseConfig) SetDefaults(
	st *cluster.Settings, tr *tracing.Tracer, storeSpec base.StoreSpec,
) {
	baseCfg := cfg.Config
	*cfg = BaseConfig{Config: baseCfg}
	cfg.Tracer = tr
	cfg.Settings = st
	idsProvider := &idProvider{
		clusterID: &base.ClusterIDContainer{},
		serverID:  &base.NodeIDContainer{},
	}
	disableWebLogin := envutil.EnvOrDefaultBool("COCKROACH_DISABLE_WEB_LOGIN", false)
	cfg.idProvider = idsProvider
	cfg.IDContainer = idsProvider.serverID
	cfg.ClusterIDContainer = idsProvider.clusterID
	cfg.AmbientCtx = log.MakeServerAmbientContext(tr, idsProvider)
	cfg.MaxOffset = MaxOffsetType(base.DefaultMaxClockOffset)
	cfg.DisableMaxOffsetCheck = false
	cfg.DefaultZoneConfig = zonepb.DefaultZoneConfig()
	cfg.StorageEngine = storage.DefaultStorageEngine
	cfg.TestingInsecureWebAccess = disableWebLogin
	cfg.Stores = base.StoreSpecList{
		Specs: []base.StoreSpec{storeSpec},
	}
	cfg.AutoConfigProvider = acprovider.NoTaskProvider{}
	// We use the tag "n" here for both KV nodes and SQL instances,
	// using the knowledge that the value part of a SQL instance ID
	// container will prefix the value with the string "sql", resulting
	// in a tag that is prefixed with "nsql".
	cfg.AmbientCtx.AddLogTag("n", cfg.IDContainer)
	cfg.Config.InitDefaults()
	cfg.InitTestingKnobs()
	cfg.ExternalStorageAccessor = cloud.NewExternalStorageAccessor()
}

// InitTestingKnobs sets up any testing knobs based on e.g. envvars.
func (cfg *BaseConfig) InitTestingKnobs() {
	// If requested, write an MVCC range tombstone at the bottom of the SQL table
	// data keyspace during cluster bootstrapping, for performance and correctness
	// testing. This shouldn't affect data written above it, but activates range
	// key-specific code paths in the storage layer. We'll also have to tweak
	// rangefeeds and batcheval to not choke on it.
	if envutil.EnvOrDefaultBool("COCKROACH_GLOBAL_MVCC_RANGE_TOMBSTONE", false) {
		if cfg.TestingKnobs.Store == nil {
			cfg.TestingKnobs.Store = &kvserver.StoreTestingKnobs{}
		}
		if cfg.TestingKnobs.RangeFeed == nil {
			cfg.TestingKnobs.RangeFeed = &rangefeed.TestingKnobs{}
		}
		storeKnobs := cfg.TestingKnobs.Store.(*kvserver.StoreTestingKnobs)
		storeKnobs.GlobalMVCCRangeTombstone = true
		storeKnobs.EvalKnobs.DisableInitPutFailOnTombstones = true
		cfg.TestingKnobs.RangeFeed.(*rangefeed.TestingKnobs).IgnoreOnDeleteRangeError = true
	}

	// If requested, replace point tombstones with range tombstones on a best-effort
	// basis.
	if envutil.EnvOrDefaultBool("COCKROACH_MVCC_RANGE_TOMBSTONES_FOR_POINT_DELETES", false) {
		if cfg.TestingKnobs.Store == nil {
			cfg.TestingKnobs.Store = &kvserver.StoreTestingKnobs{}
		}
		if cfg.TestingKnobs.RangeFeed == nil {
			cfg.TestingKnobs.RangeFeed = &rangefeed.TestingKnobs{}
		}
		storeKnobs := cfg.TestingKnobs.Store.(*kvserver.StoreTestingKnobs)
		storeKnobs.EvalKnobs.UseRangeTombstonesForPointDeletes = true
		cfg.TestingKnobs.RangeFeed.(*rangefeed.TestingKnobs).IgnoreOnDeleteRangeError = true
	}
}

// ToleratedOffset returns the tolerated clock offset with other cluster nodes
// as measured via RPC heartbeats, or 0 to disable clock offset checks.
func (cfg *BaseConfig) ToleratedOffset() time.Duration {
	if cfg.DisableMaxOffsetCheck {
		return 0
	}
	return time.Duration(toleratedOffsetMultiplier * float64(cfg.MaxOffset))
}

// Config holds the parameters needed to set up a combined KV and SQL server.
type Config struct {
	BaseConfig
	KVConfig
	SQLConfig
}

// KVConfig holds the parameters that (together with a BaseConfig) allow setting
// up a KV server.
type KVConfig struct {
	base.RaftConfig

	// Attrs specifies a colon-separated list of node topography or machine
	// capabilities, used to match capabilities or location preferences specified
	// in zone configs.
	Attrs string

	// JoinList is a list of node addresses that is used to form a network of KV
	// servers. Assuming a connected graph, it suffices to initialize any server
	// in the network.
	JoinList base.JoinListType

	// JoinPreferSRVRecords, if set, causes the lookup logic for the
	// names in JoinList to prefer SRV records from DNS, if available,
	// to A/AAAA records.
	JoinPreferSRVRecords bool

	// RetryOptions controls the retry behavior of the server.
	//
	// TODO(tbg): this is only ever used in one test. Make it a testing knob.
	RetryOptions retry.Options

	// CacheSize is the amount of memory in bytes to use for caching data.
	// The value is split evenly between the stores if there are more than one.
	CacheSize int64

	// TimeSeriesServerConfig contains configuration specific to the time series
	// server.
	TimeSeriesServerConfig ts.ServerConfig

	// Parsed values.

	// NodeAttributes is the parsed representation of Attrs.
	NodeAttributes roachpb.Attributes

	// GossipBootstrapAddresses is a list of gossip addresses used
	// to find bootstrap nodes for connecting to the gossip network.
	GossipBootstrapAddresses []util.UnresolvedAddr

	// The following values can only be set via environment variables and are
	// for testing only. They are not meant to be set by the end user.

	// ScanInterval determines a duration during which each range should be
	// visited approximately once by the range scanner. Set to 0 to disable.
	// Environment Variable: COCKROACH_SCAN_INTERVAL
	ScanInterval time.Duration

	// ScanMinIdleTime is the minimum time the scanner will be idle between ranges.
	// If enabled (> 0), the scanner may complete in more than ScanInterval for large
	// stores.
	// Environment Variable: COCKROACH_SCAN_MIN_IDLE_TIME
	ScanMinIdleTime time.Duration

	// ScanMaxIdleTime is the maximum time the scanner will be idle between ranges.
	// If enabled (> 0), the scanner may complete in less than ScanInterval for small
	// stores.
	// Environment Variable: COCKROACH_SCAN_MAX_IDLE_TIME
	ScanMaxIdleTime time.Duration

	// DefaultSystemZoneConfig is used to set the default system zone config
	// inside the server. It can be overridden during tests by setting the
	// DefaultSystemZoneConfigOverride server testing knob.
	DefaultSystemZoneConfig zonepb.ZoneConfig

	// EventLogEnabled is a switch which enables recording into cockroach's SQL
	// event log tables. These tables record transactional events about changes
	// to cluster metadata, such as DDL statements and range rebalancing
	// actions.
	EventLogEnabled bool

	// DelayedBootstrapFn is called if the bootstrap process does not complete
	// in a timely fashion, typically 30s after the server starts listening.
	DelayedBootstrapFn func()

	enginesCreated bool

	// SnapshotSendLimit is the number of concurrent snapshots a store will send.
	SnapshotSendLimit int64

	// SnapshotApplyLimit is the number of concurrent snapshots a store will
	// apply. The send limit is typically higher than the apply limit for a few
	// reasons. One is that it keeps "pipelining" of requests in the case where
	// there is only a single sender and single receiver. As soon as a receiver
	// finishes a request, there will be another one to start. The performance
	// impact of sending snapshots is lower than applying. Finally, snapshots are
	// not sent until the receiver is ready to apply, so the cost of sending is
	// low until the receiver is ready.
	SnapshotApplyLimit int64
}

// MakeKVConfig returns a KVConfig with default values.
func MakeKVConfig() KVConfig {
	kvCfg := KVConfig{}
	kvCfg.SetDefaults()
	return kvCfg
}

// SetDefaults resets the values in KVConfig. Enables running tests
// multiple times.
func (kvCfg *KVConfig) SetDefaults() {
	*kvCfg = KVConfig{}
	kvCfg.RaftConfig.SetDefaults()
	kvCfg.DefaultSystemZoneConfig = zonepb.DefaultSystemZoneConfig()
	kvCfg.CacheSize = DefaultCacheSize
	kvCfg.ScanInterval = defaultScanInterval
	kvCfg.ScanMinIdleTime = defaultScanMinIdleTime
	kvCfg.ScanMaxIdleTime = defaultScanMaxIdleTime
	kvCfg.EventLogEnabled = defaultEventLogEnabled
	kvCfg.SnapshotSendLimit = kvserver.DefaultSnapshotSendLimit
	kvCfg.SnapshotApplyLimit = kvserver.DefaultSnapshotApplyLimit
}

// SQLConfig holds the parameters that (together with a BaseConfig) allow
// setting up a SQL server.
type SQLConfig struct {
	// The tenant that the SQL server runs on the behalf of.
	TenantID roachpb.TenantID

	// TempStorageConfig is used to configure temp storage, which stores
	// ephemeral data when processing large queries.
	TempStorageConfig base.TempStorageConfig

	// ExternalIODirConfig is used to configure external storage
	// access (http://, nodelocal://, etc)
	ExternalIODirConfig base.ExternalIODirConfig

	// MemoryPoolSize is the amount of memory in bytes that can be
	// used by SQL clients to store row data in server RAM.
	MemoryPoolSize int64

	// TableStatCacheSize is the size (number of tables) of the table
	// statistics cache.
	TableStatCacheSize int

	// QueryCacheSize is the memory size (in bytes) of the query plan cache.
	QueryCacheSize int64

	// TenantKVAddrs are the entry points to the KV layer.
	//
	// Only applies when the SQL server is deployed individually.
	TenantKVAddrs []string

	// TenantLoopbackAddr is the address to use for the tenant's loopback connection.
	// It only applies when in a shared-process configuration.
	TenantLoopbackAddr string

	// The following values can only be set via environment variables and are
	// for testing only. They are not meant to be set by the end user.

	// Enables linearizable behavior of operations on this node by making sure
	// that no commit timestamp is reported back to the client until all other
	// node clocks have necessarily passed it.
	// Environment Variable: COCKROACH_EXPERIMENTAL_LINEARIZABLE
	Linearizable bool

	// LocalKVServerInfo is set in configs for shared-process tenants. It contains
	// info for making Batch requests to the local KV server without using gRPC.
	LocalKVServerInfo *LocalKVServerInfo

	// NodeMetricsRecorder is the node's MetricRecorder; the tenant's metrics will
	// be recorded with it. Nil if this is not a shared-process tenant.
	NodeMetricsRecorder *status.MetricsRecorder
}

// LocalKVServerInfo is used to group information about the local KV server
// necessary for creating the internalClientAdapter for an in-process tenant
// talking to that server.
type LocalKVServerInfo struct {
	InternalServer     kvpb.InternalServer
	ServerInterceptors rpc.ServerInterceptorInfo
	Tracer             *tracing.Tracer

	// SameProcessCapabilityAuthorizer is the tenant capability authorizer to
	// use for servers running in the same process as the KV node.
	SameProcessCapabilityAuthorizer tenantcapabilities.Authorizer
}

// MakeSQLConfig returns a SQLConfig with default values.
func MakeSQLConfig(tenID roachpb.TenantID, tempStorageCfg base.TempStorageConfig) SQLConfig {
	sqlCfg := SQLConfig{
		TenantID: tenID,
	}
	sqlCfg.SetDefaults(tempStorageCfg)
	return sqlCfg
}

// SetDefaults resets the values in SQLConfig. Enables running tests
// multiple times.
func (sqlCfg *SQLConfig) SetDefaults(tempStorageCfg base.TempStorageConfig) {
	tenID := sqlCfg.TenantID
	*sqlCfg = SQLConfig{TenantID: tenID}
	sqlCfg.MemoryPoolSize = defaultSQLMemoryPoolSize
	sqlCfg.TableStatCacheSize = defaultSQLTableStatCacheSize
	sqlCfg.QueryCacheSize = defaultSQLQueryCacheSize
	sqlCfg.TempStorageConfig = tempStorageCfg
}

// setOpenFileLimit sets the soft limit for open file descriptors to the hard
// limit if needed. Returns an error if the hard limit is too low. Returns the
// value to set maxOpenFiles to for each store.
//
// # Minimum - 1700 per store, 256 saved for networking
//
// # Constrained - 256 saved for networking, rest divided evenly per store
//
// # Constrained (network only) - 10000 per store, rest saved for networking
//
// # Recommended - 10000 per store, 5000 for network
//
// Please note that current and max limits are commonly referred to as the soft
// and hard limits respectively.
//
// On Windows there is no need to change the file descriptor, known as handles,
// limit. This limit cannot be changed and is approximately 16,711,680. See
// https://blogs.technet.microsoft.com/markrussinovich/2009/09/29/pushing-the-limits-of-windows-handles/
func setOpenFileLimit(physicalStoreCount int) (uint64, error) {
	return setOpenFileLimitInner(physicalStoreCount)
}

// SetOpenFileLimitForOneStore sets the soft limit for open file descriptors
// when there is only one store.
func SetOpenFileLimitForOneStore() (uint64, error) {
	return setOpenFileLimit(1)
}

// MakeConfig returns a Config for the system tenant with default values.
func MakeConfig(ctx context.Context, st *cluster.Settings) Config {
	storeSpec, tempStorageCfg := makeStorageCfg(ctx, st)
	sqlCfg := MakeSQLConfig(roachpb.SystemTenantID, tempStorageCfg)
	tr := tracing.NewTracerWithOpt(ctx, tracing.WithClusterSettings(&st.SV))
	baseCfg := MakeBaseConfig(st, tr, storeSpec)
	kvCfg := MakeKVConfig()

	cfg := Config{
		BaseConfig: baseCfg,
		KVConfig:   kvCfg,
		SQLConfig:  sqlCfg,
	}

	return cfg
}

// SetDefaults initializes the Config to its default value while
// preserving the base.Config reference. Enables running tests
// multiple times.
func (cfg *Config) SetDefaults(ctx context.Context, st *cluster.Settings) {
	storeSpec, tempStorageCfg := makeStorageCfg(ctx, st)
	cfg.SQLConfig.SetDefaults(tempStorageCfg)
	cfg.KVConfig.SetDefaults()
	tr := tracing.NewTracerWithOpt(ctx, tracing.WithClusterSettings(&st.SV))
	cfg.BaseConfig.SetDefaults(st, tr, storeSpec)
}

func makeStorageCfg(
	ctx context.Context, st *cluster.Settings,
) (base.StoreSpec, base.TempStorageConfig) {
	storeSpec, err := base.NewStoreSpec(DefaultStorePath)
	if err != nil {
		panic(err)
	}
	tempStorageCfg := base.TempStorageConfigFromEnv(
		ctx, st, storeSpec, "" /* parentDir */, base.DefaultTempStorageMaxSizeBytes)
	return storeSpec, tempStorageCfg
}

// String implements the fmt.Stringer interface.
func (cfg *Config) String() string {
	var buf bytes.Buffer

	w := tabwriter.NewWriter(&buf, 2, 1, 2, ' ', 0)
	fmt.Fprintln(w, "max offset\t", cfg.MaxOffset)
	fmt.Fprintln(w, "cache size\t", humanizeutil.IBytes(cfg.CacheSize))
	fmt.Fprintln(w, "SQL memory pool size\t", humanizeutil.IBytes(cfg.MemoryPoolSize))
	fmt.Fprintln(w, "scan interval\t", cfg.ScanInterval)
	fmt.Fprintln(w, "scan min idle time\t", cfg.ScanMinIdleTime)
	fmt.Fprintln(w, "scan max idle time\t", cfg.ScanMaxIdleTime)
	fmt.Fprintln(w, "event log enabled\t", cfg.EventLogEnabled)
	if cfg.Linearizable {
		fmt.Fprintln(w, "linearizable\t", cfg.Linearizable)
	}
	_ = w.Flush()

	return buf.String()
}

// Report logs an overview of the server configuration parameters via
// the given context.
func (cfg *Config) Report(ctx context.Context) {
	if memSize, err := status.GetTotalMemory(ctx); err != nil {
		log.Infof(ctx, "unable to retrieve system total memory: %v", err)
	} else {
		log.Infof(ctx, "system total memory: %s", humanizeutil.IBytes(memSize))
	}
	log.Infof(ctx, "server configuration:\n%s", log.SafeManaged(cfg))
}

// Engines is a container of engines, allowing convenient closing.
type Engines []storage.Engine

// Close closes all the Engines.
// This method has a pointer receiver so that the following pattern works:
//
//	func f() {
//		engines := Engines(engineSlice)
//		defer engines.Close()  // make sure the engines are Closed if this
//		                       // function returns early.
//		... do something with engines, pass ownership away...
//		engines = nil  // neutralize the preceding defer
//	}
func (e *Engines) Close() {
	for _, eng := range *e {
		eng.Close()
	}
	*e = nil
}

// CreateEngines creates Engines based on the specs in cfg.Stores.
func (cfg *Config) CreateEngines(ctx context.Context) (Engines, error) {
	var engines Engines
	defer engines.Close()

	if cfg.enginesCreated {
		return Engines{}, errors.Errorf("engines already created")
	}
	cfg.enginesCreated = true

	var details []redact.RedactableString
	detail := func(msg redact.RedactableString) {
		details = append(details, msg)
	}
	detail(redact.Sprintf("Pebble cache size: %s", humanizeutil.IBytes(cfg.CacheSize)))
	pebbleCache := pebble.NewCache(cfg.CacheSize)
	defer pebbleCache.Unref()

	var sharedStorage cloud.ExternalStorage
	if cfg.SharedStorage != "" {
		var err error
		// Note that we don't pass an io interceptor here. Instead, we record shared
		// storage metrics on a per-store basis; see storage.Metrics.
		sharedStorage, err = cloud.ExternalStorageFromURI(ctx, cfg.SharedStorage,
			base.ExternalIODirConfig{}, cfg.Settings, nil, cfg.User, nil,
			nil, cloud.NilMetrics)
		if err != nil {
			return nil, err
		}
	}

	var physicalStores int
	for _, spec := range cfg.Stores.Specs {
		if !spec.InMemory {
			physicalStores++
		}
	}
	openFileLimitPerStore, err := setOpenFileLimit(physicalStores)
	if err != nil {
		return Engines{}, err
	}

	log.Event(ctx, "initializing engines")

	var tableCache *pebble.TableCache
	// TODO(radu): use the tableCache for in-memory stores as well.
	if physicalStores > 0 {
		perStoreLimit := pebble.TableCacheSize(int(openFileLimitPerStore))
		totalFileLimit := perStoreLimit * physicalStores
		tableCache = pebble.NewTableCache(pebbleCache, runtime.GOMAXPROCS(0), totalFileLimit)
	}

	var storeKnobs kvserver.StoreTestingKnobs
	if s := cfg.TestingKnobs.Store; s != nil {
		storeKnobs = *s.(*kvserver.StoreTestingKnobs)
	}

	for i, spec := range cfg.Stores.Specs {
		log.Eventf(ctx, "initializing %+v", spec)

		if spec.InMemory && spec.StickyVFSID != "" {
			if cfg.TestingKnobs.Server == nil {
				return Engines{}, errors.AssertionFailedf("Could not create a sticky " +
					"engine no server knobs available to get a registry. " +
					"Please use Knobs.Server.StickyVFSRegistry to provide one.")
			}
			knobs := cfg.TestingKnobs.Server.(*TestingKnobs)
			if knobs.StickyVFSRegistry == nil {
				return Engines{}, errors.Errorf("Could not create a sticky " +
					"engine no registry available. Please use " +
					"Knobs.Server.StickyVFSRegistry to provide one.")
			}
			eng, err := knobs.StickyVFSRegistry.Open(ctx, cfg, spec)
			if err != nil {
				return Engines{}, err
			}
			detail(redact.Sprintf("store %d: %+v", i, eng.Properties()))
			engines = append(engines, eng)
			continue
		}

		var location storage.Location
		storageConfigOpts := []storage.ConfigOption{
			storage.Attributes(spec.Attributes),
			storage.EncryptionAtRest(spec.EncryptionOptions),
			storage.If(storeKnobs.SmallEngineBlocks, storage.BlockSize(1)),
		}
		if len(storeKnobs.EngineKnobs) > 0 {
			storageConfigOpts = append(storageConfigOpts, storeKnobs.EngineKnobs...)
		}
		addCfgOpt := func(opt storage.ConfigOption) {
			storageConfigOpts = append(storageConfigOpts, opt)
		}

		if spec.InMemory {
			location = storage.InMemory()
			var sizeInBytes = spec.Size.InBytes
			if spec.Size.Percent > 0 {
				sysMem, err := status.GetTotalMemory(ctx)
				if err != nil {
					return Engines{}, errors.Errorf("could not retrieve system memory")
				}
				sizeInBytes = int64(float64(sysMem) * spec.Size.Percent / 100)
			}
			if sizeInBytes != 0 && !storeKnobs.SkipMinSizeCheck && sizeInBytes < base.MinimumStoreSize {
				return Engines{}, errors.Errorf("%f%% of memory is only %s bytes, which is below the minimum requirement of %s",
					spec.Size.Percent, humanizeutil.IBytes(sizeInBytes), humanizeutil.IBytes(base.MinimumStoreSize))
			}
			addCfgOpt(storage.MaxSize(sizeInBytes))
			addCfgOpt(storage.CacheSize(cfg.CacheSize))

			detail(redact.Sprintf("store %d: in-memory, size %s", i, humanizeutil.IBytes(sizeInBytes)))
		} else {
			location = storage.Filesystem(spec.Path)
			if err := vfs.Default.MkdirAll(spec.Path, 0755); err != nil {
				return Engines{}, errors.Wrap(err, "creating store directory")
			}
			du, err := vfs.Default.GetDiskUsage(spec.Path)
			if err != nil {
				return Engines{}, errors.Wrap(err, "retrieving disk usage")
			}
			var sizeInBytes = spec.Size.InBytes
			if spec.Size.Percent > 0 {
				sizeInBytes = int64(float64(du.TotalBytes) * spec.Size.Percent / 100)
			}
			if sizeInBytes != 0 && !storeKnobs.SkipMinSizeCheck && sizeInBytes < base.MinimumStoreSize {
				return Engines{}, errors.Errorf("%f%% of %s's total free space is only %s bytes, which is below the minimum requirement of %s",
					spec.Size.Percent, spec.Path, humanizeutil.IBytes(sizeInBytes), humanizeutil.IBytes(base.MinimumStoreSize))
			}

			detail(redact.Sprintf("store %d: max size %s, max open file limit %d", i, humanizeutil.IBytes(sizeInBytes), openFileLimitPerStore))

			addCfgOpt(storage.MaxSize(sizeInBytes))
			addCfgOpt(storage.BallastSize(storage.BallastSizeBytes(spec, du)))
			addCfgOpt(storage.Caches(pebbleCache, tableCache))
			// TODO(radu): move up all remaining settings below so they apply to in-memory stores as well.
			addCfgOpt(storage.MaxOpenFiles(int(openFileLimitPerStore)))
			addCfgOpt(storage.MaxWriterConcurrency(2))
			addCfgOpt(storage.RemoteStorageFactory(cfg.ExternalStorageAccessor))
			if sharedStorage != nil {
				addCfgOpt(storage.SharedStorage(sharedStorage))
			}
			// If the spec contains Pebble options, set those too.
			if spec.PebbleOptions != "" {
				addCfgOpt(storage.PebbleOptions(spec.PebbleOptions, &pebble.ParseHooks{
					NewFilterPolicy: func(name string) (pebble.FilterPolicy, error) {
						switch name {
						case "none":
							return nil, nil
						case "rocksdb.BuiltinBloomFilter":
							return bloom.FilterPolicy(10), nil
						}
						return nil, nil
					},
				}))
			}
			if len(spec.RocksDBOptions) > 0 {
				return nil, errors.Errorf("store %d: using Pebble storage engine but StoreSpec provides RocksDB options", i)
			}
		}
		eng, err := storage.Open(ctx, location, cfg.Settings, storageConfigOpts...)
		if err != nil {
			return Engines{}, err
		}
		detail(redact.Sprintf("store %d: %+v", i, eng.Properties()))
		engines = append(engines, eng)
	}

	if tableCache != nil {
		// Unref the table cache now that the engines hold references to it.
		if err := tableCache.Unref(); err != nil {
			return nil, err
		}
	}

	log.Infof(ctx, "%d storage engine%s initialized",
		len(engines), redact.Safe(util.Pluralize(int64(len(engines)))))
	for _, s := range details {
		log.Infof(ctx, "%v", s)
	}

	// Clear out engines because we have deferred engines.Close().
	enginesCopy := engines
	engines = nil
	return enginesCopy, nil
}

// InitSQLServer finalizes the configuration of a SQL-only node.
// It initializes additional configuration flags from the environment.
func (cfg *Config) InitSQLServer(ctx context.Context) error {
	cfg.readSQLEnvironmentVariables()
	return nil
}

// InitNode finalizes the configuration of a KV node.
// It parses node attributes and bootstrap addresses and
// initializes additional configuration flags from the environment.
func (cfg *Config) InitNode(ctx context.Context) error {
	cfg.readEnvironmentVariables()

	// Initialize attributes.
	cfg.NodeAttributes = parseAttributes(cfg.Attrs)

	// Get the gossip bootstrap addresses.
	addresses, err := cfg.parseGossipBootstrapAddresses(ctx)
	if err != nil {
		return err
	}
	if len(addresses) > 0 {
		cfg.GossipBootstrapAddresses = addresses
	}

	cfg.BaseConfig.idProvider.SetTenant(roachpb.SystemTenantID)

	return nil
}

// FilterGossipBootstrapAddresses removes any gossip bootstrap addresses which
// match either this node's listen address or its advertised host address.
func (cfg *Config) FilterGossipBootstrapAddresses(ctx context.Context) []util.UnresolvedAddr {
	var listen, advert net.Addr
	listen = util.NewUnresolvedAddr("tcp", cfg.Addr)
	advert = util.NewUnresolvedAddr("tcp", cfg.AdvertiseAddr)
	filtered := make([]util.UnresolvedAddr, 0, len(cfg.GossipBootstrapAddresses))
	addrs := make([]string, 0, len(cfg.GossipBootstrapAddresses))

	for _, addr := range cfg.GossipBootstrapAddresses {
		if addr.String() == advert.String() || addr.String() == listen.String() {
			if log.V(1) {
				log.Infof(ctx, "skipping -join address %q, because a node cannot join itself", addr)
			}
		} else {
			filtered = append(filtered, addr)
			addrs = append(addrs, addr.String())
		}
	}
	if log.V(1) {
		log.Infof(ctx, "initial addresses: %v", addrs)
	}
	return filtered
}

// InsecureWebAccess indicates whether the server should allow
// access to the HTTP endpoints without a valid auth cookie.
func (cfg *BaseConfig) InsecureWebAccess() bool {
	return cfg.Insecure || cfg.TestingInsecureWebAccess
}

func (cfg *Config) readSQLEnvironmentVariables() {
	cfg.Linearizable = envutil.EnvOrDefaultBool("COCKROACH_EXPERIMENTAL_LINEARIZABLE", cfg.Linearizable)
}

// readEnvironmentVariables populates all context values that are environment
// variable based. Note that this only happens when initializing a node and not
// when NewContext is called.
func (cfg *Config) readEnvironmentVariables() {
	cfg.readSQLEnvironmentVariables()
	cfg.ScanInterval = envutil.EnvOrDefaultDuration("COCKROACH_SCAN_INTERVAL", cfg.ScanInterval)
	cfg.ScanMinIdleTime = envutil.EnvOrDefaultDuration("COCKROACH_SCAN_MIN_IDLE_TIME", cfg.ScanMinIdleTime)
	cfg.ScanMaxIdleTime = envutil.EnvOrDefaultDuration("COCKROACH_SCAN_MAX_IDLE_TIME", cfg.ScanMaxIdleTime)
	cfg.SnapshotSendLimit = envutil.EnvOrDefaultInt64("COCKROACH_CONCURRENT_SNAPSHOT_SEND_LIMIT", cfg.SnapshotSendLimit)
	cfg.SnapshotApplyLimit = envutil.EnvOrDefaultInt64("COCKROACH_CONCURRENT_SNAPSHOT_APPLY_LIMIT", cfg.SnapshotApplyLimit)
}

// parseGossipBootstrapAddresses parses list of gossip bootstrap addresses.
func (cfg *Config) parseGossipBootstrapAddresses(
	ctx context.Context,
) ([]util.UnresolvedAddr, error) {
	var bootstrapAddresses []util.UnresolvedAddr
	for _, address := range cfg.JoinList {
		if address == "" {
			continue
		}

		if cfg.JoinPreferSRVRecords {
			// The following code substitutes the entry in --join by the
			// result of SRV resolution, if suitable SRV records are found
			// for that name.
			//
			// TODO(knz): Delay this lookup. The logic for "regular" addresses
			// is delayed until the point the connection is attempted, so that
			// fresh DNS records are used for a new connection. This makes
			// it possible to update DNS records without restarting the node.
			// The SRV logic here does not have this property (yet).
			srvAddrs, err := netutil.SRV(ctx, address)
			if err != nil {
				return nil, err
			}

			if len(srvAddrs) > 0 {
				for _, sa := range srvAddrs {
					bootstrapAddresses = append(bootstrapAddresses,
						util.MakeUnresolvedAddrWithDefaults("tcp", sa, base.DefaultPort))
				}
				continue
			}
		}

		// Otherwise, use the address.
		bootstrapAddresses = append(bootstrapAddresses,
			util.MakeUnresolvedAddrWithDefaults("tcp", address, base.DefaultPort))
	}

	return bootstrapAddresses, nil
}

// parseAttributes parses a colon-separated list of strings,
// filtering empty strings (i.e. "::" will yield no attributes.
// Returns the list of strings as Attributes.
func parseAttributes(attrsStr string) roachpb.Attributes {
	var filtered []string
	for _, attr := range strings.Split(attrsStr, ":") {
		if len(attr) != 0 {
			filtered = append(filtered, attr)
		}
	}
	return roachpb.Attributes{Attrs: filtered}
}

// idProvider connects the server ID containers in this
// package to the logging package.
//
// For each of the "main" data items, it also memoizes its
// representation as a string (the one needed by the
// serverident.ServerIdentificationPayload interface) as soon as the value is
// initialized. This saves on conversion costs.
type idProvider struct {
	// clusterID contains the cluster ID (initialized late).
	clusterID *base.ClusterIDContainer
	// clusterStr is the memoized representation of clusterID, once known.
	clusterStr atomic.Value

	// tenantID is the tenant ID for this server.
	tenantID roachpb.TenantID
	// tenantStr is the memoized representation of tenantID.
	tenantStr atomic.Value

	// serverID contains the node ID for KV nodes (when tenantID.IsSet() ==
	// false), or the SQL instance ID for SQL-only servers (when
	// tenantID.IsSet() == true).
	serverID *base.NodeIDContainer
	// serverStr is the memoized representation of serverID.
	serverStr atomic.Value
}

var _ serverident.ServerIdentificationPayload = (*idProvider)(nil)

// TenantID is part of the serverident.ServerIdentificationPayload interface.
func (s *idProvider) TenantID() interface{} {
	return s.tenantID
}

// ServerIdentityString implements the serverident.ServerIdentificationPayload interface.
func (s *idProvider) ServerIdentityString(key serverident.ServerIdentificationKey) string {
	switch key {
	case serverident.IdentifyClusterID:
		c := s.clusterStr.Load()
		cs, ok := c.(string)
		if !ok {
			cid := s.clusterID.Get()
			if cid != uuid.Nil {
				cs = cid.String()
				s.clusterStr.Store(cs)
			}
		}
		return cs

	case serverident.IdentifyTenantID:
		t := s.tenantStr.Load()
		ts, ok := t.(string)
		if !ok {
			tid := s.tenantID
			if tid.IsSet() {
				ts = strconv.FormatUint(tid.ToUint64(), 10)
				s.tenantStr.Store(ts)
			}
		}
		return ts

	case serverident.IdentifyInstanceID:
		// If tenantID is not set, this is a KV node and it has no SQL
		// instance ID.
		if !s.tenantID.IsSet() {
			return ""
		}
		return s.maybeMemoizeServerID()

	case serverident.IdentifyKVNodeID:
		// If tenantID is set, this is a SQL-only server and it has no
		// node ID.
		if s.tenantID.IsSet() && !s.tenantID.IsSystem() {
			return ""
		}
		return s.maybeMemoizeServerID()
	}

	return ""
}

// SetTenant informs the provider that it provides data for
// a SQL server.
//
// Note: this should not be called concurrently with logging which may
// invoke the method from the serverident.ServerIdentificationPayload
// interface.
func (s *idProvider) SetTenant(tenantID roachpb.TenantID) {
	if !tenantID.IsSet() {
		panic("programming error: invalid tenant ID")
	}
	if s.tenantID.IsSet() {
		panic("programming error: provider already set for tenant server")
	}
	s.tenantID = tenantID
}

// maybeMemoizeServerID saves the representation of serverID to
// serverStr if the former is initialized.
func (s *idProvider) maybeMemoizeServerID() string {
	si := s.serverStr.Load()
	sis, ok := si.(string)
	if !ok {
		sid := s.serverID.Get()
		if sid != 0 {
			sis = strconv.FormatUint(uint64(sid), 10)
			s.serverStr.Store(sis)
		}
	}
	return sis
}

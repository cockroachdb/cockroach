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
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip/resolver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/elastic/gosigar"
)

// Context defaults.
const (
	// DefaultCacheSize is the default size of the RocksDB and Pebble caches. We
	// default the cache size and SQL memory pool size to 128 MiB. Larger values
	// might provide significantly better performance, but we're not sure what
	// type of system we're running on (development or production or some shared
	// environment). Production users should almost certainly override these
	// settings and we'll warn in the logs about doing so.
	DefaultCacheSize         = 128 << 20 // 128 MB
	defaultSQLMemoryPoolSize = 128 << 20 // 128 MB
	defaultScanInterval      = 10 * time.Minute
	defaultScanMinIdleTime   = 10 * time.Millisecond
	defaultScanMaxIdleTime   = 1 * time.Second

	defaultStorePath = "cockroach-data"
	// TempDirPrefix is the filename prefix of any temporary subdirectory
	// created.
	TempDirPrefix = "cockroach-temp"
	// TempDirsRecordFilename is the filename for the record file
	// that keeps track of the paths of the temporary directories created.
	TempDirsRecordFilename = "temp-dirs-record.txt"
	defaultEventLogEnabled = true

	maximumMaxClockOffset = 5 * time.Second

	minimumNetworkFileDescriptors     = 256
	recommendedNetworkFileDescriptors = 5000

	defaultSQLTableStatCacheSize = 256

	// This comes out to 1024 cache entries.
	defaultSQLQueryCacheSize = 8 * 1024 * 1024
)

var productionSettingsWebpage = fmt.Sprintf(
	"please see %s for more details",
	base.DocsURL("recommended-production-settings.html"),
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

	// AmbientCtx is used to annotate contexts used inside the server.
	AmbientCtx log.AmbientContext

	// Maximum allowed clock offset for the cluster. If observed clock
	// offsets exceed this limit, inconsistency may result, and servers
	// will panic to minimize the likelihood of inconsistent data.
	// Increasing this value will increase time to recovery after
	// failures, and increase the frequency and impact of
	// ReadWithinUncertaintyIntervalError.
	MaxOffset MaxOffsetType

	// DefaultZoneConfig is used to set the default zone config inside the server.
	// It can be overridden during tests by setting the DefaultZoneConfigOverride
	// server testing knob.
	DefaultZoneConfig zonepb.ZoneConfig

	// Locality is a description of the topography of the server.
	Locality roachpb.Locality

	// StorageEngine specifies the engine type (eg. rocksdb, pebble) to use to
	// instantiate stores.
	StorageEngine enginepb.EngineType

	// TestingKnobs is used for internal test controls only.
	TestingKnobs base.TestingKnobs
}

// MakeBaseConfig returns a BaseConfig with default values.
func MakeBaseConfig(st *cluster.Settings) BaseConfig {
	baseCfg := BaseConfig{
		AmbientCtx:        log.AmbientContext{Tracer: st.Tracer},
		Config:            new(base.Config),
		Settings:          st,
		MaxOffset:         MaxOffsetType(base.DefaultMaxClockOffset),
		DefaultZoneConfig: zonepb.DefaultZoneConfig(),
		StorageEngine:     storage.DefaultStorageEngine,
	}
	baseCfg.InitDefaults()
	return baseCfg
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

	// Stores is specified to enable durable key-value storage.
	Stores base.StoreSpecList

	// Attrs specifies a colon-separated list of node topography or machine
	// capabilities, used to match capabilities or location preferences specified
	// in zone configs.
	Attrs string

	// JoinList is a list of node addresses that act as bootstrap hosts for
	// connecting to the gossip network.
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

	// GoroutineDumpDirName is the directory name for goroutine dumps using
	// goroutinedumper.
	GoroutineDumpDirName string

	// HeapProfileDirName is the directory name for heap profiles using
	// heapprofiler. If empty, no heap profiles will be collected.
	HeapProfileDirName string

	// Parsed values.

	// NodeAttributes is the parsed representation of Attrs.
	NodeAttributes roachpb.Attributes

	// GossipBootstrapResolvers is a list of gossip resolvers used
	// to find bootstrap nodes for connecting to the gossip network.
	GossipBootstrapResolvers []resolver.Resolver

	// The following values can only be set via environment variables and are
	// for testing only. They are not meant to be set by the end user.

	// Enables linearizable behavior of operations on this node by making sure
	// that no commit timestamp is reported back to the client until all other
	// node clocks have necessarily passed it.
	// Environment Variable: COCKROACH_EXPERIMENTAL_LINEARIZABLE
	Linearizable bool

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

	// LocalityAddresses contains private IP addresses the can only be accessed
	// in the corresponding locality.
	LocalityAddresses []roachpb.LocalityAddress

	// EventLogEnabled is a switch which enables recording into cockroach's SQL
	// event log tables. These tables record transactional events about changes
	// to cluster metadata, such as DDL statements and range rebalancing
	// actions.
	EventLogEnabled bool

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

	// DelayedBootstrapFn is called if the boostrap process does not complete
	// in a timely fashion, typically 30s after the server starts listening.
	DelayedBootstrapFn func()

	// EnableWebSessionAuthentication enables session-based authentication for
	// the Admin API's HTTP endpoints.
	EnableWebSessionAuthentication bool

	enginesCreated bool
}

// MakeKVConfig returns a KVConfig with default values.
func MakeKVConfig(storeSpec base.StoreSpec) KVConfig {
	disableWebLogin := envutil.EnvOrDefaultBool("COCKROACH_DISABLE_WEB_LOGIN", false)
	kvCfg := KVConfig{
		DefaultSystemZoneConfig:        zonepb.DefaultSystemZoneConfig(),
		CacheSize:                      DefaultCacheSize,
		ScanInterval:                   defaultScanInterval,
		ScanMinIdleTime:                defaultScanMinIdleTime,
		ScanMaxIdleTime:                defaultScanMaxIdleTime,
		EventLogEnabled:                defaultEventLogEnabled,
		EnableWebSessionAuthentication: !disableWebLogin,
		Stores: base.StoreSpecList{
			Specs: []base.StoreSpec{storeSpec},
		},
	}
	kvCfg.RaftConfig.SetDefaults()
	return kvCfg
}

// SQLConfig holds the parameters that (together with a BaseConfig) allow
// setting up a SQL server.
type SQLConfig struct {
	// The tenant that the SQL server runs on the behalf of.
	TenantID roachpb.TenantID

	// LeaseManagerConfig holds configuration values specific to the LeaseManager.
	LeaseManagerConfig *base.LeaseManagerConfig

	// SocketFile, if non-empty, sets up a TLS-free local listener using
	// a unix datagram socket at the specified path.
	SocketFile string

	// TempStorageConfig is used to configure temp storage, which stores
	// ephemeral data when processing large queries.
	TempStorageConfig base.TempStorageConfig

	// ExternalIODirConfig is used to configure external storage
	// access (http://, nodelocal://, etc)
	ExternalIODirConfig base.ExternalIODirConfig

	// MemoryPoolSize is the amount of memory in bytes that can be
	// used by SQL clients to store row data in server RAM.
	MemoryPoolSize int64

	// AuditLogDirName is the target directory name for SQL audit logs.
	AuditLogDirName *log.DirName

	// TableStatCacheSize is the size (number of tables) of the table
	// statistics cache.
	TableStatCacheSize int

	// QueryCacheSize is the memory size (in bytes) of the query plan cache.
	QueryCacheSize int64

	// TenantKVAddrs are the entry points to the KV layer.
	//
	// Only applies when the SQL server is deployed individually.
	TenantKVAddrs []string
}

// MakeSQLConfig returns a SQLConfig with default values.
func MakeSQLConfig(tenID roachpb.TenantID, tempStorageCfg base.TempStorageConfig) SQLConfig {
	sqlCfg := SQLConfig{
		TenantID:           tenID,
		MemoryPoolSize:     defaultSQLMemoryPoolSize,
		TableStatCacheSize: defaultSQLTableStatCacheSize,
		QueryCacheSize:     defaultSQLQueryCacheSize,
		TempStorageConfig:  tempStorageCfg,
		LeaseManagerConfig: base.NewLeaseManagerConfig(),
	}
	return sqlCfg
}

// setOpenFileLimit sets the soft limit for open file descriptors to the hard
// limit if needed. Returns an error if the hard limit is too low. Returns the
// value to set maxOpenFiles to for each store.
//
// Minimum - 1700 per store, 256 saved for networking
//
// Constrained - 256 saved for networking, rest divided evenly per store
//
// Constrained (network only) - 10000 per store, rest saved for networking
//
// Recommended - 10000 per store, 5000 for network
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
	storeSpec, err := base.NewStoreSpec(defaultStorePath)
	if err != nil {
		panic(err)
	}
	tempStorageCfg := base.TempStorageConfigFromEnv(
		ctx, st, storeSpec, "" /* parentDir */, base.DefaultTempStorageMaxSizeBytes)

	sqlCfg := MakeSQLConfig(roachpb.SystemTenantID, tempStorageCfg)
	baseCfg := MakeBaseConfig(st)
	kvCfg := MakeKVConfig(storeSpec)

	cfg := Config{
		BaseConfig: baseCfg,
		KVConfig:   kvCfg,
		SQLConfig:  sqlCfg,
	}

	return cfg
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
	log.Infof(ctx, "server configuration:\n%s", cfg)
}

// Engines is a container of engines, allowing convenient closing.
type Engines []storage.Engine

// Close closes all the Engines.
// This method has a pointer receiver so that the following pattern works:
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
	engines := Engines(nil)
	defer engines.Close()

	if cfg.enginesCreated {
		return Engines{}, errors.Errorf("engines already created")
	}
	cfg.enginesCreated = true

	var details []string

	var cache storage.RocksDBCache
	var pebbleCache *pebble.Cache
	if cfg.StorageEngine == enginepb.EngineTypeDefault ||
		cfg.StorageEngine == enginepb.EngineTypePebble || cfg.StorageEngine == enginepb.EngineTypeTeePebbleRocksDB {
		details = append(details, fmt.Sprintf("Pebble cache size: %s", humanizeutil.IBytes(cfg.CacheSize)))
		pebbleCache = pebble.NewCache(cfg.CacheSize)
		defer pebbleCache.Unref()
	}
	if cfg.StorageEngine == enginepb.EngineTypeRocksDB || cfg.StorageEngine == enginepb.EngineTypeTeePebbleRocksDB {
		details = append(details, fmt.Sprintf("RocksDB cache size: %s", humanizeutil.IBytes(cfg.CacheSize)))
		cache = storage.NewRocksDBCache(cfg.CacheSize)
		defer cache.Release()
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

	skipSizeCheck := cfg.TestingKnobs.Store != nil &&
		cfg.TestingKnobs.Store.(*kvserver.StoreTestingKnobs).SkipMinSizeCheck
	for i, spec := range cfg.Stores.Specs {
		log.Eventf(ctx, "initializing %+v", spec)
		var sizeInBytes = spec.Size.InBytes
		if spec.InMemory {
			if spec.Size.Percent > 0 {
				sysMem, err := status.GetTotalMemory(ctx)
				if err != nil {
					return Engines{}, errors.Errorf("could not retrieve system memory")
				}
				sizeInBytes = int64(float64(sysMem) * spec.Size.Percent / 100)
			}
			if sizeInBytes != 0 && !skipSizeCheck && sizeInBytes < base.MinimumStoreSize {
				return Engines{}, errors.Errorf("%f%% of memory is only %s bytes, which is below the minimum requirement of %s",
					spec.Size.Percent, humanizeutil.IBytes(sizeInBytes), humanizeutil.IBytes(base.MinimumStoreSize))
			}
			details = append(details, fmt.Sprintf("store %d: in-memory, size %s",
				i, humanizeutil.IBytes(sizeInBytes)))
			if spec.StickyInMemoryEngineID != "" {
				e, err := getOrCreateStickyInMemEngine(
					ctx, spec.StickyInMemoryEngineID, cfg.StorageEngine, spec.Attributes, sizeInBytes,
				)
				if err != nil {
					return Engines{}, err
				}
				engines = append(engines, e)
			} else {
				engines = append(engines, storage.NewInMem(ctx, cfg.StorageEngine, spec.Attributes, sizeInBytes))
			}
		} else {
			if spec.Size.Percent > 0 {
				fileSystemUsage := gosigar.FileSystemUsage{}
				if err := fileSystemUsage.Get(spec.Path); err != nil {
					return Engines{}, err
				}
				sizeInBytes = int64(float64(fileSystemUsage.Total) * spec.Size.Percent / 100)
			}
			if sizeInBytes != 0 && !skipSizeCheck && sizeInBytes < base.MinimumStoreSize {
				return Engines{}, errors.Errorf("%f%% of %s's total free space is only %s bytes, which is below the minimum requirement of %s",
					spec.Size.Percent, spec.Path, humanizeutil.IBytes(sizeInBytes), humanizeutil.IBytes(base.MinimumStoreSize))
			}

			details = append(details, fmt.Sprintf("store %d: RocksDB, max size %s, max open file limit %d",
				i, humanizeutil.IBytes(sizeInBytes), openFileLimitPerStore))

			var eng storage.Engine
			var err error
			storageConfig := base.StorageConfig{
				Attrs:           spec.Attributes,
				Dir:             spec.Path,
				MaxSize:         sizeInBytes,
				Settings:        cfg.Settings,
				UseFileRegistry: spec.UseFileRegistry,
				ExtraOptions:    spec.ExtraOptions,
			}
			if cfg.StorageEngine == enginepb.EngineTypePebble || cfg.StorageEngine == enginepb.EngineTypeDefault {
				// TODO(itsbilal): Tune these options, and allow them to be overridden
				// in the spec (similar to the existing spec.RocksDBOptions and others).
				pebbleConfig := storage.PebbleConfig{
					StorageConfig: storageConfig,
					Opts:          storage.DefaultPebbleOptions(),
				}
				pebbleConfig.Opts.Cache = pebbleCache
				pebbleConfig.Opts.MaxOpenFiles = int(openFileLimitPerStore)
				eng, err = storage.NewPebble(ctx, pebbleConfig)
			} else if cfg.StorageEngine == enginepb.EngineTypeRocksDB {
				rocksDBConfig := storage.RocksDBConfig{
					StorageConfig:           storageConfig,
					MaxOpenFiles:            openFileLimitPerStore,
					WarnLargeBatchThreshold: 500 * time.Millisecond,
					RocksDBOptions:          spec.RocksDBOptions,
				}

				eng, err = storage.NewRocksDB(rocksDBConfig, cache)
			} else {
				// cfg.StorageEngine == enginepb.EngineTypeTeePebbleRocksDB
				pebbleConfig := storage.PebbleConfig{
					StorageConfig: storageConfig,
					Opts:          storage.DefaultPebbleOptions(),
				}
				pebbleConfig.Dir = filepath.Join(pebbleConfig.Dir, "pebble")
				pebbleConfig.Opts.Cache = pebbleCache
				pebbleConfig.Opts.MaxOpenFiles = int(openFileLimitPerStore)
				pebbleEng, err := storage.NewPebble(ctx, pebbleConfig)
				if err != nil {
					return nil, err
				}

				rocksDBConfig := storage.RocksDBConfig{
					StorageConfig:           storageConfig,
					MaxOpenFiles:            openFileLimitPerStore,
					WarnLargeBatchThreshold: 500 * time.Millisecond,
					RocksDBOptions:          spec.RocksDBOptions,
				}
				rocksDBConfig.Dir = filepath.Join(rocksDBConfig.Dir, "rocksdb")

				rocksdbEng, err := storage.NewRocksDB(rocksDBConfig, cache)
				if err != nil {
					return nil, err
				}

				eng = storage.NewTee(ctx, rocksdbEng, pebbleEng)
			}
			if err != nil {
				return Engines{}, err
			}
			engines = append(engines, eng)
		}
	}

	log.Infof(ctx, "%d storage engine%s initialized",
		len(engines), util.Pluralize(int64(len(engines))))
	for _, s := range details {
		log.Infof(ctx, "%v", s)
	}
	enginesCopy := engines
	engines = nil
	return enginesCopy, nil
}

// InitNode parses node attributes and initializes the gossip bootstrap
// resolvers.
func (cfg *Config) InitNode(ctx context.Context) error {
	cfg.readEnvironmentVariables()

	// Initialize attributes.
	cfg.NodeAttributes = parseAttributes(cfg.Attrs)

	// Get the gossip bootstrap resolvers.
	resolvers, err := cfg.parseGossipBootstrapResolvers(ctx)
	if err != nil {
		return err
	}
	if len(resolvers) > 0 {
		cfg.GossipBootstrapResolvers = resolvers
	}

	return nil
}

// FilterGossipBootstrapResolvers removes any gossip bootstrap resolvers which
// match either this node's listen address or its advertised host address.
func (cfg *Config) FilterGossipBootstrapResolvers(
	ctx context.Context, listen, advert net.Addr,
) []resolver.Resolver {
	filtered := make([]resolver.Resolver, 0, len(cfg.GossipBootstrapResolvers))
	addrs := make([]string, 0, len(cfg.GossipBootstrapResolvers))
	for _, r := range cfg.GossipBootstrapResolvers {
		if r.Addr() == advert.String() || r.Addr() == listen.String() {
			if log.V(1) {
				log.Infof(ctx, "skipping -join address %q, because a node cannot join itself", r.Addr())
			}
		} else {
			filtered = append(filtered, r)
			addrs = append(addrs, r.Addr())
		}
	}
	if log.V(1) {
		log.Infof(ctx, "initial resolvers: %v", addrs)
	}
	return filtered
}

// RequireWebSession indicates whether the server should require authentication
// sessions when serving admin API requests.
func (cfg *Config) RequireWebSession() bool {
	return !cfg.Insecure && cfg.EnableWebSessionAuthentication
}

// readEnvironmentVariables populates all context values that are environment
// variable based. Note that this only happens when initializing a node and not
// when NewContext is called.
func (cfg *Config) readEnvironmentVariables() {
	cfg.Linearizable = envutil.EnvOrDefaultBool("COCKROACH_EXPERIMENTAL_LINEARIZABLE", cfg.Linearizable)
	cfg.ScanInterval = envutil.EnvOrDefaultDuration("COCKROACH_SCAN_INTERVAL", cfg.ScanInterval)
	cfg.ScanMinIdleTime = envutil.EnvOrDefaultDuration("COCKROACH_SCAN_MIN_IDLE_TIME", cfg.ScanMinIdleTime)
	cfg.ScanMaxIdleTime = envutil.EnvOrDefaultDuration("COCKROACH_SCAN_MAX_IDLE_TIME", cfg.ScanMaxIdleTime)
}

// parseGossipBootstrapResolvers parses list of gossip bootstrap resolvers.
func (cfg *Config) parseGossipBootstrapResolvers(ctx context.Context) ([]resolver.Resolver, error) {
	var bootstrapResolvers []resolver.Resolver
	for _, address := range cfg.JoinList {
		if cfg.JoinPreferSRVRecords {
			// The following code substitutes the entry in --join by the
			// result of SRV resolution, if suitable SRV records are found
			// for that name.
			//
			// TODO(knz): Delay this lookup. The logic for "regular" resolvers
			// is delayed until the point the connection is attempted, so that
			// fresh DNS records are used for a new connection. This makes
			// it possible to update DNS records without restarting the node.
			// The SRV logic here does not have this property (yet).
			srvAddrs, err := resolver.SRV(ctx, address)
			if err != nil {
				return nil, err
			}

			if len(srvAddrs) > 0 {
				for _, sa := range srvAddrs {
					resolver, err := resolver.NewResolver(sa)
					if err != nil {
						return nil, err
					}
					bootstrapResolvers = append(bootstrapResolvers, resolver)
				}

				continue
			}
		}

		// Otherwise, use the address.
		resolver, err := resolver.NewResolver(address)
		if err != nil {
			return nil, err
		}
		bootstrapResolvers = append(bootstrapResolvers, resolver)
	}

	return bootstrapResolvers, nil
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

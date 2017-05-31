// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Daniel Theophanes (kardianos@gmail.com)

package server

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"runtime"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/elastic/gosigar"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip/resolver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

// Context defaults.
const (
	defaultCGroupMemPath            = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
	defaultCacheSize                = 512 << 20 // 512 MB
	defaultSQLMemoryPoolSize        = 512 << 20 // 512 MB
	defaultScanInterval             = 10 * time.Minute
	defaultConsistencyCheckInterval = 24 * time.Hour
	defaultScanMaxIdleTime          = 200 * time.Millisecond
	defaultMetricsSampleInterval    = 10 * time.Second
	defaultStorePath                = "cockroach-data"
	defaultEventLogEnabled          = true

	minimumNetworkFileDescriptors     = 256
	recommendedNetworkFileDescriptors = 5000

	productionSettingsWebpage = "please see https://www.cockroachlabs.com/docs/recommended-production-settings.html for more details"
)

var timeUntilStoreDead = settings.RegisterPositiveDurationSetting(
	"server.time_until_store_dead",
	"the time after which if there is no new gossiped information about a store, it is considered dead",
	5*time.Minute)

// Config holds parameters needed to setup a server.
type Config struct {
	// Embed the base context.
	*base.Config

	// Unix socket: for postgres only.
	SocketFile string

	// Stores is specified to enable durable key-value storage.
	Stores base.StoreSpecList

	// Attrs specifies a colon-separated list of node topography or machine
	// capabilities, used to match capabilities or location preferences specified
	// in zone configs.
	Attrs string

	// JoinList is a list of node addresses that act as bootstrap hosts for
	// connecting to the gossip network. Each item in the list can actually be
	// multiple comma-separated addresses, kept for backward-compatibility.
	JoinList base.JoinListType

	// RetryOptions controls the retry behavior of the server.
	RetryOptions retry.Options

	// CacheSize is the amount of memory in bytes to use for caching data.
	// The value is split evenly between the stores if there are more than one.
	CacheSize int64

	// TimeSeriesServerConfig contains configuration specific to the time series
	// server.
	TimeSeriesServerConfig ts.ServerConfig

	// SQLMemoryPoolSize is the amount of memory in bytes that can be
	// used by SQL clients to store row data in server RAM.
	SQLMemoryPoolSize int64

	// Parsed values.

	// NodeAttributes is the parsed representation of Attrs.
	NodeAttributes roachpb.Attributes

	// GossipBootstrapResolvers is a list of gossip resolvers used
	// to find bootstrap nodes for connecting to the gossip network.
	GossipBootstrapResolvers []resolver.Resolver

	// The following values can only be set via environment variables and are
	// for testing only. They are not meant to be set by the end user.

	// Enables linearizable behaviour of operations on this node by making sure
	// that no commit timestamp is reported back to the client until all other
	// node clocks have necessarily passed it.
	// Environment Variable: COCKROACH_LINEARIZABLE
	Linearizable bool

	// Maximum allowed clock offset for the cluster. If observed clock
	// offsets exceed this limit, inconsistency may result, and servers
	// will panic to minimize the likelihood of inconsistent data.
	// Increasing this value will increase time to recovery after
	// failures, and increase the frequency and impact of
	// ReadWithinUncertaintyIntervalError.
	MaxOffset time.Duration

	// RaftTickInterval is the resolution of the Raft timer.
	RaftTickInterval time.Duration

	// RaftElectionTimeoutTicks is the number of raft ticks before the
	// previous election expires. This value is inherited by individual
	// stores unless overridden.
	RaftElectionTimeoutTicks int

	// MetricsSamplePeriod determines the time between records of
	// server internal metrics.
	// Environment Variable: COCKROACH_METRICS_SAMPLE_INTERVAL
	MetricsSampleInterval time.Duration

	// ScanInterval determines a duration during which each range should be
	// visited approximately once by the range scanner. Set to 0 to disable.
	// Environment Variable: COCKROACH_SCAN_INTERVAL
	ScanInterval time.Duration

	// ScanMaxIdleTime is the maximum time the scanner will be idle between ranges.
	// If enabled (> 0), the scanner may complete in less than ScanInterval for small
	// stores.
	// Environment Variable: COCKROACH_SCAN_MAX_IDLE_TIME
	ScanMaxIdleTime time.Duration

	// ConsistencyCheckInterval determines the time between range consistency checks.
	// Set to 0 to disable.
	// Environment Variable: COCKROACH_CONSISTENCY_CHECK_INTERVAL
	ConsistencyCheckInterval time.Duration

	// ConsistencyCheckPanicOnFailure causes the node to panic when it detects a
	// replication consistency check failure.
	ConsistencyCheckPanicOnFailure bool

	// TimeUntilStoreDead is the time after which if there is no new gossiped
	// information about a store, it is considered dead.
	// Environment Variable: COCKROACH_TIME_UNTIL_STORE_DEAD
	TimeUntilStoreDead *settings.DurationSetting

	// SendNextTimeout is the time after which an alternate replica will
	// be used to attempt sending a KV batch.
	// Environment Variable: COCKROACH_SEND_NEXT_TIMEOUT
	SendNextTimeout time.Duration

	// TestingKnobs is used for internal test controls only.
	TestingKnobs base.TestingKnobs

	// AmbientCtx is used to annotate contexts used inside the server.
	AmbientCtx log.AmbientContext

	// Locality is a description of the topography of the server.
	Locality roachpb.Locality

	// EventLogEnabled is a switch which enables recording into cockroach's SQL
	// event log tables. These tables record transactional events about changes
	// to cluster metadata, such as DDL statements and range rebalancing
	// actions.
	EventLogEnabled bool

	// ListeningURLFile indicates the file to which the server writes
	// its listening URL when it is ready.
	ListeningURLFile string

	// PIDFile indicates the file to which the server writes its PID when
	// it is ready.
	PIDFile string

	enginesCreated bool
}

// HistogramWindowInterval is used to determine the approximate length of time
// that individual samples are retained in in-memory histograms. Currently,
// it is set to the arbitrary length of six times the Metrics sample interval.
//
// The length of the window must be longer than the sampling interval due to
// issue #12998, which was causing histograms to return zero values when sampled
// because all samples had been evicted.
//
// Note that this is only intended to be a temporary fix for the above issue,
// as our current handling of metric histograms have numerous additional
// problems. These are tracked in github issue #7896, which has been given
// a relatively high priority in light of recent confusion around histogram
// metrics. For more information on the issues underlying our histogram system
// and the proposed fixes, please see issue #7896.
func (cfg Config) HistogramWindowInterval() time.Duration {
	hwi := cfg.MetricsSampleInterval * 6

	// Rudimentary overflow detection; this can result if MetricsSampleInterval
	// is set to an extremely large number, likely in the context of a test or
	// an intentional attempt to disable metrics collection. Just return
	// cfg.MetricsSampleInterval in this case.
	if hwi < cfg.MetricsSampleInterval {
		return cfg.MetricsSampleInterval
	}
	return hwi
}

// GetTotalMemory returns either the total system memory or if possible the
// cgroups available memory.
func GetTotalMemory(ctx context.Context) (int64, error) {
	totalMem, err := func() (int64, error) {
		mem := gosigar.Mem{}
		if err := mem.Get(); err != nil {
			return 0, err
		}
		if mem.Total > math.MaxInt64 {
			return 0, fmt.Errorf("inferred memory size %s exceeds maximum supported memory size %s",
				humanize.IBytes(mem.Total), humanize.Bytes(math.MaxInt64))
		}
		return int64(mem.Total), nil
	}()
	if err != nil {
		return 0, err
	}
	checkTotal := func(x int64) (int64, error) {
		if x <= 0 {
			// https://github.com/elastic/gosigar/issues/72
			return 0, fmt.Errorf("inferred memory size %d is suspicious, considering invalid", x)
		}
		return x, nil
	}
	if runtime.GOOS != "linux" {
		return checkTotal(totalMem)
	}

	var buf []byte
	if buf, err = ioutil.ReadFile(defaultCGroupMemPath); err != nil {
		log.Infof(ctx, "can't read available memory from cgroups (%s), using system memory %s instead", err,
			humanizeutil.IBytes(totalMem))
		return checkTotal(totalMem)
	}

	cgAvlMem, err := strconv.ParseUint(strings.TrimSpace(string(buf)), 10, 64)
	if err != nil {
		log.Infof(ctx, "can't parse available memory from cgroups (%s), using system memory %s instead", err,
			humanizeutil.IBytes(totalMem))
		return checkTotal(totalMem)
	}

	if cgAvlMem == 0 || cgAvlMem > math.MaxInt64 {
		log.Infof(ctx, "available memory from cgroups (%s) is unsupported, using system memory %s instead",
			humanize.IBytes(cgAvlMem), humanizeutil.IBytes(totalMem))
		return checkTotal(totalMem)
	}

	if totalMem > 0 && int64(cgAvlMem) > totalMem {
		log.Infof(ctx, "available memory from cgroups (%s) exceeds system memory %s, using system memory",
			humanize.IBytes(cgAvlMem), humanizeutil.IBytes(totalMem))
		return checkTotal(totalMem)
	}

	return checkTotal(int64(cgAvlMem))
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
func setOpenFileLimit(physicalStoreCount int) (int, error) {
	return setOpenFileLimitInner(physicalStoreCount)
}

// SetOpenFileLimitForOneStore sets the soft limit for open file descriptors
// when there is only one store.
func SetOpenFileLimitForOneStore() (int, error) {
	return setOpenFileLimit(1)
}

// MakeConfig returns a Context with default values.
func MakeConfig() Config {
	storeSpec, err := base.NewStoreSpec(defaultStorePath)
	if err != nil {
		panic(err)
	}
	cfg := Config{
		Config:                   new(base.Config),
		MaxOffset:                base.DefaultMaxClockOffset,
		CacheSize:                defaultCacheSize,
		SQLMemoryPoolSize:        defaultSQLMemoryPoolSize,
		ScanInterval:             defaultScanInterval,
		ScanMaxIdleTime:          defaultScanMaxIdleTime,
		ConsistencyCheckInterval: defaultConsistencyCheckInterval,
		MetricsSampleInterval:    defaultMetricsSampleInterval,
		TimeUntilStoreDead:       timeUntilStoreDead,
		SendNextTimeout:          base.DefaultSendNextTimeout,
		EventLogEnabled:          defaultEventLogEnabled,
		Stores: base.StoreSpecList{
			Specs: []base.StoreSpec{storeSpec},
		},
	}
	cfg.Config.InitDefaults()
	return cfg
}

// String implements the fmt.Stringer interface.
func (cfg *Config) String() string {
	var buf bytes.Buffer

	w := tabwriter.NewWriter(&buf, 2, 1, 2, ' ', 0)
	fmt.Fprintln(w, "max offset\t", cfg.MaxOffset)
	fmt.Fprintln(w, "cache size\t", humanizeutil.IBytes(cfg.CacheSize))
	fmt.Fprintln(w, "SQL memory pool size\t", humanizeutil.IBytes(cfg.SQLMemoryPoolSize))
	fmt.Fprintln(w, "scan interval\t", cfg.ScanInterval)
	fmt.Fprintln(w, "scan max idle time\t", cfg.ScanMaxIdleTime)
	fmt.Fprintln(w, "consistency check interval\t", cfg.ConsistencyCheckInterval)
	fmt.Fprintln(w, "metrics sample interval\t", cfg.MetricsSampleInterval)
	fmt.Fprintln(w, "time until store dead\t", cfg.TimeUntilStoreDead)
	fmt.Fprintln(w, "send next timeout\t", cfg.SendNextTimeout)
	fmt.Fprintln(w, "event log enabled\t", cfg.EventLogEnabled)
	fmt.Fprintln(w, "linearizable\t", cfg.Linearizable)
	if cfg.ListeningURLFile != "" {
		fmt.Fprintln(w, "listening URL file\t", cfg.ListeningURLFile)
	}
	if cfg.PIDFile != "" {
		fmt.Fprintln(w, "PID file\t", cfg.PIDFile)
	}
	_ = w.Flush()

	return buf.String()
}

// Report logs an overview of the server configuration parameters via
// the given context.
func (cfg *Config) Report(ctx context.Context) {
	if memSize, err := GetTotalMemory(ctx); err != nil {
		log.Infof(ctx, "unable to retrieve system total memory: %v", err)
	} else {
		log.Infof(ctx, "system total memory: %s", humanizeutil.IBytes(memSize))
	}
	log.Info(ctx, "server configuration:\n", cfg)
}

// Engines is a container of engines, allowing convenient closing.
type Engines []engine.Engine

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

	details = append(details, fmt.Sprintf("RocksDB cache size: %s", humanizeutil.IBytes(cfg.CacheSize)))
	cache := engine.NewRocksDBCache(cfg.CacheSize)
	defer cache.Release()

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

	skipSizeCheck := cfg.TestingKnobs.Store != nil &&
		cfg.TestingKnobs.Store.(*storage.StoreTestingKnobs).SkipMinSizeCheck
	for i, spec := range cfg.Stores.Specs {
		var sizeInBytes = spec.SizeInBytes
		if spec.InMemory {
			if spec.SizePercent > 0 {
				sysMem, err := GetTotalMemory(ctx)
				if err != nil {
					return Engines{}, errors.Errorf("could not retrieve system memory")
				}
				sizeInBytes = int64(float64(sysMem) * spec.SizePercent / 100)
			}
			if sizeInBytes != 0 && !skipSizeCheck && sizeInBytes < base.MinimumStoreSize {
				return Engines{}, errors.Errorf("%f%% of memory is only %s bytes, which is below the minimum requirement of %s",
					spec.SizePercent, humanizeutil.IBytes(sizeInBytes), humanizeutil.IBytes(base.MinimumStoreSize))
			}
			details = append(details, fmt.Sprintf("store %d: in-memory, size %s",
				i, humanizeutil.IBytes(sizeInBytes)))
			engines = append(engines, engine.NewInMem(spec.Attributes, sizeInBytes))
		} else {
			if spec.SizePercent > 0 {
				fileSystemUsage := gosigar.FileSystemUsage{}
				if err := fileSystemUsage.Get(spec.Path); err != nil {
					return Engines{}, err
				}
				sizeInBytes = int64(float64(fileSystemUsage.Total) * spec.SizePercent / 100)
			}
			if sizeInBytes != 0 && !skipSizeCheck && sizeInBytes < base.MinimumStoreSize {
				return Engines{}, errors.Errorf("%f%% of %s's total free space is only %s bytes, which is below the minimum requirement of %s",
					spec.SizePercent, spec.Path, humanizeutil.IBytes(sizeInBytes), humanizeutil.IBytes(base.MinimumStoreSize))
			}

			details = append(details, fmt.Sprintf("store %d: RocksDB, max size %s, max open file limit %d",
				i, humanizeutil.IBytes(sizeInBytes), openFileLimitPerStore))
			eng, err := engine.NewRocksDB(
				spec.Attributes,
				spec.Path,
				cache,
				sizeInBytes,
				openFileLimitPerStore,
			)
			if err != nil {
				return Engines{}, err
			}
			engines = append(engines, eng)
		}
	}

	log.Infof(ctx, "%d storage engine%s initialized",
		len(engines), util.Pluralize(int64(len(engines))))
	for _, s := range details {
		log.Info(ctx, s)
	}
	enginesCopy := engines
	engines = nil
	return enginesCopy, nil
}

// InitNode parses node attributes and initializes the gossip bootstrap
// resolvers.
func (cfg *Config) InitNode() error {
	cfg.readEnvironmentVariables()

	// Initialize attributes.
	cfg.NodeAttributes = parseAttributes(cfg.Attrs)

	// Expose HistogramWindowInterval to parts of the code that can't import the
	// server package. This code should be cleaned up within a month or two.
	cfg.Config.HistogramWindowInterval = cfg.HistogramWindowInterval()

	// Get the gossip bootstrap resolvers.
	resolvers, err := cfg.parseGossipBootstrapResolvers()
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

// readEnvironmentVariables populates all context values that are environment
// variable based. Note that this only happens when initializing a node and not
// when NewContext is called.
func (cfg *Config) readEnvironmentVariables() {
	// cockroach-linearizable
	cfg.Linearizable = envutil.EnvOrDefaultBool("COCKROACH_LINEARIZABLE", cfg.Linearizable)
	cfg.ConsistencyCheckPanicOnFailure = envutil.EnvOrDefaultBool("COCKROACH_CONSISTENCY_CHECK_PANIC_ON_FAILURE", cfg.ConsistencyCheckPanicOnFailure)
	cfg.MetricsSampleInterval = envutil.EnvOrDefaultDuration("COCKROACH_METRICS_SAMPLE_INTERVAL", cfg.MetricsSampleInterval)
	cfg.ScanInterval = envutil.EnvOrDefaultDuration("COCKROACH_SCAN_INTERVAL", cfg.ScanInterval)
	cfg.ScanMaxIdleTime = envutil.EnvOrDefaultDuration("COCKROACH_SCAN_MAX_IDLE_TIME", cfg.ScanMaxIdleTime)
	cfg.SendNextTimeout = envutil.EnvOrDefaultDuration("COCKROACH_SEND_NEXT_TIMEOUT", cfg.SendNextTimeout)
	cfg.ConsistencyCheckInterval = envutil.EnvOrDefaultDuration("COCKROACH_CONSISTENCY_CHECK_INTERVAL", cfg.ConsistencyCheckInterval)
}

// parseGossipBootstrapResolvers parses list of gossip bootstrap resolvers.
func (cfg *Config) parseGossipBootstrapResolvers() ([]resolver.Resolver, error) {
	var bootstrapResolvers []resolver.Resolver
	for _, commaSeparatedAddresses := range cfg.JoinList {
		addresses := strings.Split(commaSeparatedAddresses, ",")
		for _, address := range addresses {
			if len(address) == 0 {
				continue
			}
			resolver, err := resolver.NewResolver(address)
			if err != nil {
				return nil, err
			}
			bootstrapResolvers = append(bootstrapResolvers, resolver)
		}
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

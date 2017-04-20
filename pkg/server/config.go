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
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/elastic/gosigar"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip/resolver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/ts"
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
	defaultTimeUntilStoreDead       = 5 * time.Minute
	defaultStorePath                = "cockroach-data"
	defaultEventLogEnabled          = true

	minimumNetworkFileDescriptors     = 256
	recommendedNetworkFileDescriptors = 5000

	productionSettingsWebpage = "please see https://www.cockroachlabs.com/docs/recommended-production-settings.html for more details"
)

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
	// Environment Variable: COCKROACH_MAX_OFFSET
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
	TimeUntilStoreDead time.Duration

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
func GetTotalMemory() (int64, error) {
	mem := gosigar.Mem{}
	if err := mem.Get(); err != nil {
		return 0, err
	}
	if mem.Total > math.MaxInt64 {
		return 0, fmt.Errorf("inferred memory size %s exceeds maximum supported memory size %s",
			humanize.IBytes(mem.Total), humanize.Bytes(math.MaxInt64))
	}
	totalMem := int64(mem.Total)
	if runtime.GOOS == "linux" {
		var err error
		var buf []byte
		if buf, err = ioutil.ReadFile(defaultCGroupMemPath); err != nil {
			if log.V(1) {
				log.Infof(context.TODO(), "can't read available memory from cgroups (%s), using system memory %s instead", err,
					humanizeutil.IBytes(totalMem))
			}
			return totalMem, nil
		}
		var cgAvlMem uint64
		if cgAvlMem, err = strconv.ParseUint(strings.TrimSpace(string(buf)), 10, 64); err != nil {
			if log.V(1) {
				log.Infof(context.TODO(), "can't parse available memory from cgroups (%s), using system memory %s instead", err,
					humanizeutil.IBytes(totalMem))
			}
			return totalMem, nil
		}
		if cgAvlMem > math.MaxInt64 {
			if log.V(1) {
				log.Infof(context.TODO(), "available memory from cgroups is too large and unsupported %s using system memory %s instead",
					humanize.IBytes(cgAvlMem), humanizeutil.IBytes(totalMem))

			}
			return totalMem, nil
		}
		if cgAvlMem > mem.Total {
			if log.V(1) {
				log.Infof(context.TODO(), "available memory from cgroups %s exceeds system memory %s, using system memory",
					humanize.IBytes(cgAvlMem), humanizeutil.IBytes(totalMem))
			}
			return totalMem, nil
		}

		return int64(cgAvlMem), nil
	}
	return totalMem, nil
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
		TimeUntilStoreDead:       defaultTimeUntilStoreDead,
		SendNextTimeout:          base.DefaultSendNextTimeout,
		EventLogEnabled:          defaultEventLogEnabled,
		Stores: base.StoreSpecList{
			Specs: []base.StoreSpec{storeSpec},
		},
	}
	cfg.Config.InitDefaults()
	return cfg
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

// CreateEngines creates Engines based on the specs in ctx.Stores.
func (cfg *Config) CreateEngines() (Engines, error) {
	engines := Engines(nil)
	defer engines.Close()

	if cfg.enginesCreated {
		return Engines{}, errors.Errorf("engines already created")
	}
	cfg.enginesCreated = true

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
	for _, spec := range cfg.Stores.Specs {
		var sizeInBytes = spec.SizeInBytes
		if spec.InMemory {
			if spec.SizePercent > 0 {
				sysMem, err := GetTotalMemory()
				if err != nil {
					return Engines{}, errors.Errorf("could not retrieve system memory")
				}
				sizeInBytes = int64(float64(sysMem) * spec.SizePercent / 100)
			}
			if sizeInBytes != 0 && !skipSizeCheck && sizeInBytes < base.MinimumStoreSize {
				return Engines{}, errors.Errorf("%f%% of memory is only %s bytes, which is below the minimum requirement of %s",
					spec.SizePercent, humanizeutil.IBytes(sizeInBytes), humanizeutil.IBytes(base.MinimumStoreSize))
			}
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

	if len(engines) == 1 {
		log.Infof(context.TODO(), "1 storage engine initialized")
	} else {
		log.Infof(context.TODO(), "%d storage engines initialized", len(engines))
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
	cfg.MaxOffset = envutil.EnvOrDefaultDuration("COCKROACH_MAX_OFFSET", cfg.MaxOffset)
	cfg.MetricsSampleInterval = envutil.EnvOrDefaultDuration("COCKROACH_METRICS_SAMPLE_INTERVAL", cfg.MetricsSampleInterval)
	cfg.ScanInterval = envutil.EnvOrDefaultDuration("COCKROACH_SCAN_INTERVAL", cfg.ScanInterval)
	cfg.ScanMaxIdleTime = envutil.EnvOrDefaultDuration("COCKROACH_SCAN_MAX_IDLE_TIME", cfg.ScanMaxIdleTime)
	cfg.TimeUntilStoreDead = envutil.EnvOrDefaultDuration("COCKROACH_TIME_UNTIL_STORE_DEAD", cfg.TimeUntilStoreDead)
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

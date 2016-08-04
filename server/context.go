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
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/net/context"

	"github.com/dustin/go-humanize"
	"github.com/elastic/gosigar"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/gossip/resolver"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/envutil"
	"github.com/cockroachdb/cockroach/util/humanizeutil"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
)

// Context defaults.
const (
	defaultCGroupMemPath = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
	defaultMaxOffset     = 250 * time.Millisecond
	defaultCacheSize     = 512 << 20 // 512 MB
	// defaultMemtableBudget controls how much memory can be used for memory
	// tables. The way we initialize RocksDB, 100% (32 MB) of this setting can be
	// used for memory tables and each memory table will be 25% of the size (8
	// MB). This corresponds to the default RocksDB memtable size. Note that
	// larger values do not necessarily improve performance, so benchmark any
	// changes to this value.
	defaultMemtableBudget           = 32 << 20 // 32 MB
	defaultScanInterval             = 10 * time.Minute
	defaultConsistencyCheckInterval = 24 * time.Hour
	defaultScanMaxIdleTime          = 5 * time.Second
	defaultMetricsSampleInterval    = 10 * time.Second
	defaultTimeUntilStoreDead       = 5 * time.Minute
	defaultStorePath                = "cockroach-data"
	defaultReservationsEnabled      = true

	minimumNetworkFileDescriptors     = 256
	recommendedNetworkFileDescriptors = 5000

	productionSettingsWebpage = "please see https://www.cockroachlabs.com/docs/recommended-production-settings.html for more details"
)

// Context holds parameters needed to setup a server.
type Context struct {
	// Embed the base context.
	*base.Context

	// Unix socket: for postgres only.
	SocketFile string

	// Stores is specified to enable durable key-value storage.
	Stores StoreSpecList

	// Attrs specifies a colon-separated list of node topography or machine
	// capabilities, used to match capabilities or location preferences specified
	// in zone configs.
	Attrs string

	// JoinList is a list of node addresses that act as bootstrap hosts for
	// connecting to the gossip network. Each item in the list can actually be
	// multiple comma-separated addresses, kept for backward-compatibility.
	JoinList JoinListType

	// CacheSize is the amount of memory in bytes to use for caching data.
	// The value is split evenly between the stores if there are more than one.
	CacheSize int64

	// MemtableBudget is the amount of memory, per store, in bytes to use for
	// the memory table.
	// This value is no longer settable by the end user.
	MemtableBudget int64

	// Parsed values.

	// Engines is the storage instances specified by Stores.
	Engines []engine.Engine

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

	// Maximum clock offset for the cluster.
	// Environment Variable: COCKROACH_MAX_OFFSET
	MaxOffset time.Duration

	// RaftTickInterval is the resolution of the Raft timer.
	RaftTickInterval time.Duration

	// MetricsSamplePeriod determines the time between records of
	// server internal metrics.
	// Environment Variable: COCKROACH_METRICS_SAMPLE_INTERVAL
	MetricsSampleInterval time.Duration

	// ScanInterval determines a duration during which each range should be
	// visited approximately once by the range scanner.
	// Environment Variable: COCKROACH_SCAN_INTERVAL
	ScanInterval time.Duration

	// ScanMaxIdleTime is the maximum time the scanner will be idle between ranges.
	// If enabled (> 0), the scanner may complete in less than ScanInterval for small
	// stores.
	// Environment Variable: COCKROACH_SCAN_MAX_IDLE_TIME
	ScanMaxIdleTime time.Duration

	// ConsistencyCheckInterval determines the time between range consistency checks.
	// Environment Variable: COCKROACH_CONSISTENCY_CHECK_INTERVAL
	ConsistencyCheckInterval time.Duration

	// ConsistencyCheckPanicOnFailure causes the node to panic when it detects a
	// replication consistency check failure.
	ConsistencyCheckPanicOnFailure bool

	// TimeUntilStoreDead is the time after which if there is no new gossiped
	// information about a store, it is considered dead.
	// Environment Variable: COCKROACH_TIME_UNTIL_STORE_DEAD
	TimeUntilStoreDead time.Duration

	// ReservationsEnabled is a switch used to enable the add replica
	// reservation system.
	ReservationsEnabled bool

	// TestingKnobs is used for internal test controls only.
	TestingKnobs base.TestingKnobs
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
// Minimum - 256 per store, 256 saved for networking
// Constrained - 256 saved for networking, rest divided evenly per store
// Constrained (network only) - 5000 per store, rest saved for networking
// Recommended - 5000 per store, 5000 for network
// Also, please note that current and max limits are commonly referred to as
// the soft and hard limits respectively.
func setOpenFileLimit(physicalStoreCount int) (int, error) {
	minimumOpenFileLimit := uint64(physicalStoreCount*engine.MinimumMaxOpenFiles + minimumNetworkFileDescriptors)
	networkConstrainedFileLimit := uint64(physicalStoreCount*engine.DefaultMaxOpenFiles + minimumNetworkFileDescriptors)
	recommendedOpenFileLimit := uint64(physicalStoreCount*engine.DefaultMaxOpenFiles + recommendedNetworkFileDescriptors)
	// TODO(bram): Test this out on windows.
	var rLimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil {
		if log.V(1) {
			log.Infof(context.TODO(), "could not get rlimit; setting maxOpenFiles to the default value %d - %s", engine.DefaultMaxOpenFiles, err)
		}
		return engine.DefaultMaxOpenFiles, nil
	}

	// The max open file descriptor limit is too low.
	if rLimit.Max < minimumOpenFileLimit {
		return 0, fmt.Errorf("hard open file descriptor limit of %d is under the minimum required %d\n%s",
			rLimit.Max,
			minimumOpenFileLimit,
			productionSettingsWebpage)
	}

	// If current open file descriptor limit is higher than the recommended
	// value, we can just use the default value.
	if rLimit.Cur > recommendedOpenFileLimit {
		return engine.DefaultMaxOpenFiles, nil
	}

	// If the current limit is less than the recommended limit, set the current
	// limit to the minimum of the max limit or the recommendedOpenFileLimit.
	var newCurrent uint64
	if rLimit.Max > recommendedOpenFileLimit {
		newCurrent = recommendedOpenFileLimit
	} else {
		newCurrent = rLimit.Max
	}
	if rLimit.Cur < newCurrent {
		if log.V(1) {
			log.Infof(context.TODO(), "setting the soft limit for open file descriptors from %d to %d",
				rLimit.Cur, newCurrent)
		}
		rLimit.Cur = newCurrent
		if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil {
			return 0, err
		}
		// Sadly, the current limit is not always set as expected, (e.g. OSX)
		// so fetch the limit again to see the new current limit.
		if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil {
			return 0, err
		}
		if log.V(1) {
			log.Infof(context.TODO(), "soft open file descriptor limit is now %d", rLimit.Cur)
		}
	}

	// The current open file descriptor limit is still too low.
	if rLimit.Cur < minimumOpenFileLimit {
		return 0, fmt.Errorf("soft open file descriptor limit of %d is under the minimum required %d and cannot be increased\n%s",
			rLimit.Cur,
			minimumOpenFileLimit,
			productionSettingsWebpage)
	}

	// If we have the desired number, just use the default values.
	if rLimit.Cur >= recommendedOpenFileLimit {
		return engine.DefaultMaxOpenFiles, nil
	}

	// We're still below the recommended amount, we should always show a
	// warning.
	log.Warningf(context.TODO(), "soft open file descriptor limit %d is under the recommended limit %d; this may decrease performance\n%s",
		rLimit.Cur,
		recommendedOpenFileLimit,
		productionSettingsWebpage)

	// if we have no physical stores, return 0.
	if physicalStoreCount == 0 {
		return 0, nil
	}

	// If we have more than enough file descriptors to hit the recommend number
	// for each store, than only constrain the network ones by giving the stores
	// their full recommended number.
	if rLimit.Cur >= networkConstrainedFileLimit {
		return engine.DefaultMaxOpenFiles, nil
	}

	// Always sacrifice all but the minimum needed network descriptors to be
	// used by the stores.
	return int(rLimit.Cur-minimumNetworkFileDescriptors) / physicalStoreCount, nil
}

// SetOpenFileLimitForOneStore sets the soft limit for open file descriptors
// when there is only one store.
func SetOpenFileLimitForOneStore() (int, error) {
	return setOpenFileLimit(1)
}

// MakeContext returns a Context with default values.
func MakeContext() Context {
	ctx := Context{
		Context:                  new(base.Context),
		MaxOffset:                defaultMaxOffset,
		CacheSize:                defaultCacheSize,
		MemtableBudget:           defaultMemtableBudget,
		ScanInterval:             defaultScanInterval,
		ScanMaxIdleTime:          defaultScanMaxIdleTime,
		ConsistencyCheckInterval: defaultConsistencyCheckInterval,
		MetricsSampleInterval:    defaultMetricsSampleInterval,
		TimeUntilStoreDead:       defaultTimeUntilStoreDead,
		ReservationsEnabled:      defaultReservationsEnabled,
		Stores: StoreSpecList{
			Specs: []StoreSpec{{Path: defaultStorePath}},
		},
	}
	ctx.Context.InitDefaults()
	return ctx
}

// InitStores initializes ctx.Engines based on ctx.Stores.
func (ctx *Context) InitStores(stopper *stop.Stopper) error {
	cache := engine.NewRocksDBCache(ctx.CacheSize)
	defer cache.Release()

	var physicalStores int
	for _, spec := range ctx.Stores.Specs {
		if !spec.InMemory {
			physicalStores++
		}
	}
	openFileLimitPerStore, err := setOpenFileLimit(physicalStores)
	if err != nil {
		return err
	}

	for _, spec := range ctx.Stores.Specs {
		var sizeInBytes = spec.SizeInBytes
		if spec.InMemory {
			if spec.SizePercent > 0 {
				sysMem, err := GetTotalMemory()
				if err != nil {
					return fmt.Errorf("could not retrieve system memory")
				}
				sizeInBytes = int64(float64(sysMem) * spec.SizePercent / 100)
			}
			if sizeInBytes != 0 && sizeInBytes < minimumStoreSize {
				return fmt.Errorf("%f%% of memory is only %s bytes, which is below the minimum requirement of %s",
					spec.SizePercent, humanizeutil.IBytes(sizeInBytes), humanizeutil.IBytes(minimumStoreSize))
			}
			ctx.Engines = append(ctx.Engines, engine.NewInMem(spec.Attributes, sizeInBytes, stopper))
		} else {
			if spec.SizePercent > 0 {
				fileSystemUsage := gosigar.FileSystemUsage{}
				if err := fileSystemUsage.Get(spec.Path); err != nil {
					return err
				}
				sizeInBytes = int64(float64(fileSystemUsage.Total) * spec.SizePercent / 100)
			}
			if sizeInBytes != 0 && sizeInBytes < minimumStoreSize {
				return fmt.Errorf("%f%% of %s's total free space is only %s bytes, which is below the minimum requirement of %s",
					spec.SizePercent, spec.Path, humanizeutil.IBytes(sizeInBytes), humanizeutil.IBytes(minimumStoreSize))
			}
			ctx.Engines = append(
				ctx.Engines,
				engine.NewRocksDB(
					spec.Attributes,
					spec.Path,
					cache,
					ctx.MemtableBudget,
					sizeInBytes,
					openFileLimitPerStore,
					stopper,
				),
			)
		}
	}

	if len(ctx.Engines) == 1 {
		log.Infof(context.TODO(), "1 storage engine initialized")
	} else {
		log.Infof(context.TODO(), "%d storage engines initialized", len(ctx.Engines))
	}
	return nil
}

// InitNode parses node attributes and initializes the gossip bootstrap
// resolvers.
func (ctx *Context) InitNode() error {
	ctx.readEnvironmentVariables()

	// Initialize attributes.
	ctx.NodeAttributes = parseAttributes(ctx.Attrs)

	// Get the gossip bootstrap resolvers.
	resolvers, err := ctx.parseGossipBootstrapResolvers()
	if err != nil {
		return err
	}
	if len(resolvers) > 0 {
		ctx.GossipBootstrapResolvers = resolvers
	}

	return nil
}

// readEnvironmentVariables populates all context values that are environment
// variable based. Note that this only happens when initializing a node and not
// when NewContext is called.
func (ctx *Context) readEnvironmentVariables() {
	// cockroach-linearizable
	ctx.Linearizable = envutil.EnvOrDefaultBool("linearizable", ctx.Linearizable)
	ctx.ConsistencyCheckPanicOnFailure = envutil.EnvOrDefaultBool("consistency_check_panic_on_failure", ctx.ConsistencyCheckPanicOnFailure)
	ctx.MaxOffset = envutil.EnvOrDefaultDuration("max_offset", ctx.MaxOffset)
	ctx.MetricsSampleInterval = envutil.EnvOrDefaultDuration("metrics_sample_interval", ctx.MetricsSampleInterval)
	ctx.ScanInterval = envutil.EnvOrDefaultDuration("scan_interval", ctx.ScanInterval)
	ctx.ScanMaxIdleTime = envutil.EnvOrDefaultDuration("scan_max_idle_time", ctx.ScanMaxIdleTime)
	ctx.TimeUntilStoreDead = envutil.EnvOrDefaultDuration("time_until_store_dead", ctx.TimeUntilStoreDead)
	ctx.ConsistencyCheckInterval = envutil.EnvOrDefaultDuration("consistency_check_interval", ctx.ConsistencyCheckInterval)
	// TODO(bram): remove ReservationsEnabled once we've completed testing the
	// feature.
	ctx.ReservationsEnabled = envutil.EnvOrDefaultBool("reservations_enabled", ctx.ReservationsEnabled)
}

// parseGossipBootstrapResolvers parses list of gossip bootstrap resolvers.
func (ctx *Context) parseGossipBootstrapResolvers() ([]resolver.Resolver, error) {
	var bootstrapResolvers []resolver.Resolver
	for _, commaSeparatedAddresses := range ctx.JoinList {
		addresses := strings.Split(commaSeparatedAddresses, ",")
		for _, address := range addresses {
			if len(address) == 0 {
				continue
			}
			resolver, err := resolver.NewResolver(ctx.Context, address)
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

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
	"net/url"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/elastic/gosigar"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/gossip/resolver"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
)

// Context defaults.
const (
	defaultCGroupMemPath      = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
	defaultAddr               = ":" + base.DefaultPort
	defaultMaxOffset          = 250 * time.Millisecond
	defaultCacheSize          = 512 << 20 // 512 MB
	defaultMemtableBudget     = 512 << 20 // 512 MB
	defaultScanInterval       = 10 * time.Minute
	defaultScanMaxIdleTime    = 5 * time.Second
	defaultMetricsFrequency   = 10 * time.Second
	defaultTimeUntilStoreDead = 5 * time.Minute
	defaultBalanceMode        = storage.BalanceModeUsage
)

// Context holds parameters needed to setup a server.
// Calling "cli".initFlags(ctx *Context) will initialize Context using
// command flags. Keep in sync with "cli/flags.go".
type Context struct {
	// Embed the base context.
	base.Context

	// Addr is the host:port to bind for HTTP/RPC traffic.
	Addr string

	// Stores is specified to enable durable key-value storage.
	Stores StoreSpecList

	// Attrs specifies a colon-separated list of node topography or machine
	// capabilities, used to match capabilities or location preferences specified
	// in zone configs.
	Attrs string

	// Maximum clock offset for the cluster.
	MaxOffset time.Duration

	// JoinUsing is a comma-separated list of node addresses that
	// act as bootstrap hosts for connecting to the gossip network.
	JoinUsing string

	// Enables linearizable behaviour of operations on this node by making sure
	// that no commit timestamp is reported back to the client until all other
	// node clocks have necessarily passed it.
	Linearizable bool

	// CacheSize is the amount of memory in bytes to use for caching data.
	// The value is split evenly between the stores if there are more than one.
	CacheSize uint64

	// MemtableBudget is the amount of memory in bytes to use for the memory
	// table. The value is split evenly between the stores if there are more than one.
	MemtableBudget uint64

	// BalanceMode determines how this node makes balancing decisions.
	BalanceMode storage.BalanceMode

	// Parsed values.

	// Engines is the storage instances specified by Stores.
	Engines []engine.Engine

	// NodeAttributes is the parsed representation of Attrs.
	NodeAttributes roachpb.Attributes

	// GossipBootstrapResolvers is a list of gossip resolvers used
	// to find bootstrap nodes for connecting to the gossip network.
	GossipBootstrapResolvers []resolver.Resolver

	// ScanInterval determines a duration during which each range should be
	// visited approximately once by the range scanner.
	ScanInterval time.Duration

	// ScanMaxIdleTime is the maximum time the scanner will be idle between ranges.
	// If enabled (> 0), the scanner may complete in less than ScanInterval for small
	// stores.
	ScanMaxIdleTime time.Duration

	// MetricsFrequency determines the frequency at which the server should
	// record internal metrics.
	MetricsFrequency time.Duration

	// TimeUntilStoreDead is the time after which if there is no new gossiped
	// information about a store, it is considered dead.
	TimeUntilStoreDead time.Duration

	TestingMocker TestingMocker
}

// TestingMocker is a struct containing facilities for mocking
// or otherwise controlling various parts of the system.
type TestingMocker struct {
	StoreTestingMocker    storage.StoreTestingMocker
	ExecutorTestingMocker sql.ExecutorTestingMocker
}

// GetTotalMemory returns either the total system memory or if possible the
// cgroups available memory.
func GetTotalMemory() (uint64, error) {
	mem := gosigar.Mem{}
	if err := mem.Get(); err != nil {
		return 0, err
	}
	totalMem := mem.Total
	var cgAvlMem uint64
	if runtime.GOOS == "linux" {
		var err error
		var buf []byte
		if buf, err = ioutil.ReadFile(defaultCGroupMemPath); err != nil {
			if log.V(1) {
				log.Infof("can't read available memory from cgroups (%s)", err)
			}
			return totalMem, nil
		}
		if cgAvlMem, err = strconv.ParseUint(strings.TrimSpace(string(buf)), 10, 64); err != nil {
			if log.V(1) {
				log.Infof("can't parse available memory from cgroups (%s)", err)
			}
			return totalMem, nil
		}
		return cgAvlMem, nil
	}
	return totalMem, nil
}

// NewContext returns a Context with default values.
func NewContext() *Context {
	ctx := &Context{}
	ctx.InitDefaults()
	return ctx
}

// InitDefaults sets up the default values for a Context.
//
// Note: This method should only perform simple initialization of fields
// because it is called very early in the lifetime of a cockroach process at
// which point we do not know if we are initializing a server or using the
// cli. Do not call any functions which could log or error. In fact, it is best
// if you don't call any other functions at all.
func (ctx *Context) InitDefaults() {
	ctx.Context.InitDefaults()
	ctx.Addr = defaultAddr
	ctx.MaxOffset = defaultMaxOffset
	ctx.CacheSize = defaultCacheSize
	ctx.MemtableBudget = defaultMemtableBudget
	ctx.ScanInterval = defaultScanInterval
	ctx.ScanMaxIdleTime = defaultScanMaxIdleTime
	ctx.MetricsFrequency = defaultMetricsFrequency
	ctx.TimeUntilStoreDead = defaultTimeUntilStoreDead
	ctx.BalanceMode = defaultBalanceMode
	ctx.Stores.Specs = append(ctx.Stores.Specs, StoreSpec{Path: "cockroach-data"})
}

// InitStores initializes ctx.Engines based on ctx.Stores.
func (ctx *Context) InitStores(stopper *stop.Stopper) error {
	// TODO(peter): The comments and docs say that CacheSize and MemtableBudget
	// are split evenly if there are multiple stores, but we aren't doing that
	// currently. See #4979 and #4980.
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
					spec.SizePercent, humanize.IBytes(uint64(sizeInBytes)), humanize.IBytes(uint64(minimumStoreSize)))
			}
			ctx.Engines = append(ctx.Engines, engine.NewInMem(spec.Attributes, uint64(sizeInBytes), stopper))
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
					spec.SizePercent, spec.Path, humanize.IBytes(uint64(sizeInBytes)),
					humanize.IBytes(uint64(minimumStoreSize)))
			}
			ctx.Engines = append(ctx.Engines, engine.NewRocksDB(spec.Attributes, spec.Path,
				ctx.CacheSize/uint64(len(ctx.Stores.Specs)), ctx.MemtableBudget, sizeInBytes, stopper))
		}
	}
	if len(ctx.Engines) == 1 {
		log.Infof("1 storage engine initialized")
	} else {
		log.Infof("%d storage engines initialized", len(ctx.Engines))
	}
	return nil
}

// InitNode parses node attributes and initializes the gossip bootstrap
// resolvers.
func (ctx *Context) InitNode() error {
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

// AdminURL returns the URL for the admin UI.
func (ctx *Context) AdminURL() string {
	return fmt.Sprintf("%s://%s", ctx.HTTPRequestScheme(), ctx.Addr)
}

// PGURL returns the URL for the postgres endpoint.
func (ctx *Context) PGURL(user string) *url.URL {
	// Try to convert path to an absolute path. Failing to do so return path
	// unchanged.
	absPath := func(path string) string {
		r, err := filepath.Abs(path)
		if err != nil {
			return path
		}
		return r
	}

	options := url.Values{}
	if ctx.Insecure {
		options.Add("sslmode", "disable")
	} else {
		options.Add("sslmode", "verify-full")
		options.Add("sslcert", absPath(ctx.SSLCert))
		options.Add("sslkey", absPath(ctx.SSLCertKey))
		options.Add("sslrootcert", absPath(ctx.SSLCA))
	}
	return &url.URL{
		Scheme:   "postgresql",
		User:     url.User(user),
		Host:     ctx.Addr,
		RawQuery: options.Encode(),
	}
}

// parseGossipBootstrapResolvers parses a comma-separated list of
// gossip bootstrap resolvers.
func (ctx *Context) parseGossipBootstrapResolvers() ([]resolver.Resolver, error) {
	var bootstrapResolvers []resolver.Resolver
	addresses := strings.Split(ctx.JoinUsing, ",")
	for _, address := range addresses {
		if len(address) == 0 {
			continue
		}
		resolver, err := resolver.NewResolver(&ctx.Context, address)
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

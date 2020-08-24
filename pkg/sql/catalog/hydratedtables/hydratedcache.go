// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Pacakge hydratedtables contains logic to cache table descriptors with user
// defined types hydrated.
package hydratedtables

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

// TODO(ajwerner): Add metrics.
// TODO(ajwerner): Consider adding a mechanism to remove entries which have not
// been used in a long time.

// Cache caches table descriptors which have their user-defined types hydrated.
// The cache's contract is a bit tricky. In order to use a hydrated type, the
// caller needs to have a lease on the relevant type descriptor. The way that
// this is made to work is that the user provides a handle to a leased
// ImmutableCopy and then the cache will call a function for each
// of the
type Cache struct {
	settings *cluster.Settings
	g        singleflight.Group
	metrics  Metrics
	mu       struct {
		syncutil.RWMutex
		cache *cache.UnorderedCache
	}
}

// Metrics returns the cache's metrics.
func (c *Cache) Metrics() *Metrics {
	return &c.metrics
}

var _ metric.Struct = (*Metrics)(nil)

// Metrics exposes cache metrics.
type Metrics struct {
	Hits   *metric.Counter
	Misses *metric.Counter
}

func makeMetrics() Metrics {
	return Metrics{
		Hits:   metric.NewCounter(metaHits),
		Misses: metric.NewCounter(metaMisses),
	}
}

// MetricStruct makes Metrics a metric.Struct.
func (m *Metrics) MetricStruct() {}

var (
	metaHits = metric.Metadata{
		Name:        "sql.hydrated_table_cache.hits",
		Help:        "counter on the number of cache hits",
		Measurement: "reads",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	metaMisses = metric.Metadata{
		Name:        "sql.hydrated_table_cache.misses",
		Help:        "counter on the number of cache misses",
		Measurement: "reads",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
)

// CacheSize controls the size of the LRU cache.
var CacheSize = settings.RegisterNonNegativeIntSetting(
	"sql.catalog.hydrated_tables.cache_size",
	"number of table descriptor versions retained in the hydratedtables LRU cache",
	128)

// NewCache constructs a new Cache.
func NewCache(settings *cluster.Settings) *Cache {
	c := &Cache{
		settings: settings,
		metrics:  makeMetrics(),
	}
	c.mu.cache = cache.NewUnorderedCache(cache.Config{
		Policy: cache.CacheLRU,
		ShouldEvict: func(size int, key, value interface{}) bool {
			return size > int(CacheSize.Get(&settings.SV))
		},
	})
	return c
}

type hydratedTableDescriptor struct {
	tableDesc *tabledesc.Immutable
	typeDescs []*cachedType
}

type cachedType struct {
	name    tree.TypeName
	typDesc catalog.TypeDescriptor
}

// GetHydratedTableDescriptor returns an ImmutableCopy with the types
// hydrated. It may use a cached copy but all of the relevant type descriptors
// will be retrieved via the TypeDescriptorResolver.
func (c *Cache) GetHydratedTableDescriptor(
	ctx context.Context, table *tabledesc.Immutable, lookupFunc catalog.TypeDescriptorResolver,
) (_ *tabledesc.Immutable, err error) {
	if !table.ContainsUserDefinedTypes() {
		return table, nil
	}

	// TODO(ajwerner): This cache may thrash a bit right when a version of a type
	// changes as different callers oscillate evicting the old and new versions of
	// that type. It should converge rapidly but nevertheless, a finer-granularity
	// cache could mitigate the problem.
	key := makeIDVersion(table)
	var groupKey string // used as a proxy for cache hit
	defer func() {
		if err != nil {
			return
		}
		if groupKey == "" {
			c.metrics.Hits.Inc(1)
		} else {
			c.metrics.Misses.Inc(1)
		}
	}()
	for {
		cached, ok := c.getHydratedTableDescriptorFromCache(ctx, key)
		if ok {
			canUse, err := canUseCachedDescriptor(ctx, cached, lookupFunc)
			if err != nil {
				return nil, err
			}
			if canUse {
				return cached.tableDesc, nil
			}
		}
		if groupKey == "" {
			groupKey = fmt.Sprintf("%d@%d", key.ID, key.Version)
		}
		// Now we want to lock the cache and populate the descriptors.
		// Only the calling goroutine can actually use the result directly.
		// Furthermore, if an error is encountered, only the calling goroutine
		// should return it.
		var called bool
		res, _, err := c.g.Do(groupKey, func() (interface{}, error) {
			called = true
			lookup := cachedTypeDescriptorResolver{
				underlying: lookupFunc,
				cache:      map[descpb.ID]*cachedType{},
			}
			descBase := protoutil.Clone(table.TableDesc()).(*descpb.TableDescriptor)
			if err := typedesc.HydrateTypesInTableDescriptor(ctx, descBase, &lookup); err != nil {
				return nil, err
			}
			hydrated := tabledesc.NewImmutable(*descBase)
			types := make([]*cachedType, 0, len(lookup.cache))
			for _, typ := range lookup.cache {
				types = append(types, typ)
			}
			c.mu.Lock()
			defer c.mu.Unlock()
			c.mu.cache.Add(key, &hydratedTableDescriptor{
				tableDesc: hydrated,
				typeDescs: types,
			})
			return hydrated, nil
		})
		if !called {
			continue
		}
		if err != nil {
			return nil, err
		}
		return res.(*tabledesc.Immutable), nil
	}
}

type cachedTypeDescriptorResolver struct {
	underlying catalog.TypeDescriptorResolver
	cache      map[descpb.ID]*cachedType
}

func (c cachedTypeDescriptorResolver) GetTypeDescriptor(
	ctx context.Context, id descpb.ID,
) (tree.TypeName, catalog.TypeDescriptor, error) {
	if typ, exists := c.cache[id]; exists {
		return typ.name, typ.typDesc, nil
	}
	name, typDesc, err := c.underlying.GetTypeDescriptor(ctx, id)
	if err != nil {
		return tree.TypeName{}, nil, err
	}
	c.cache[id] = &cachedType{
		name:    name,
		typDesc: typDesc,
	}
	return name, typDesc, nil
}

func canUseCachedDescriptor(
	ctx context.Context, cached *hydratedTableDescriptor, res catalog.TypeDescriptorResolver,
) (canUse bool, _ error) {
	for _, typ := range cached.typeDescs {
		name, typDesc, err := res.GetTypeDescriptor(ctx, typ.typDesc.GetID())
		if err != nil {
			return false, err
		}
		// Use raw pointer equality to check on the type descriptor being valid.
		if typ.typDesc != typDesc ||
			// Only match on the name if the retrieved type has a qualified name.
			// Note that this behavior is important and ensures that when this
			// function is passed a resolved which looks up qualified names, it always
			// get a hydrated descriptor with qualified names in its types.
			(name.ObjectNamePrefix != (tree.ObjectNamePrefix{}) && typ.name != name) {
			return false, nil
		}
	}
	return true, nil
}

func makeIDVersion(table catalog.TableDescriptor) lease.IDVersion {
	return lease.IDVersion{
		ID:      table.GetID(),
		Version: table.GetVersion(),
	}
}

// getHydratedTableDescriptorFromCache
func (c *Cache) getHydratedTableDescriptorFromCache(
	ctx context.Context, key lease.IDVersion,
) (*hydratedTableDescriptor, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	got, ok := c.mu.cache.Get(key)
	if !ok {
		return nil, false
	}
	return got.(*hydratedTableDescriptor), true
}

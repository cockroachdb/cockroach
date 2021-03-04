// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package hydratedtables contains logic to cache table descriptors with user
// defined types hydrated.
package hydratedtables

import (
	"context"
	"fmt"
	"sync"

	"github.com/biogo/store/llrb"
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

// TODO(ajwerner): Consider adding a mechanism to remove entries which have not
// been used in a long time.

// Cache caches table descriptors which have their user-defined types hydrated.
// The cache's contract is a bit tricky. In order to use a hydrated type, the
// caller needs to have a lease on the relevant type descriptor. The way that
// this is made to work is that the user provides a handle to a leased
// ImmutableCopy and then the cache will call through to type resolver for each
// of the referenced types which ensures that user always uses properly leased
// descriptors. While all of the types will need to be resolved, they should
// already be cached so, in this way, this cache prevents the need to copy
// and re-construct the tabledesc.immutable in most cases.
type Cache struct {
	settings *cluster.Settings
	g        singleflight.Group
	metrics  Metrics
	mu       struct {
		syncutil.Mutex
		cache *cache.OrderedCache
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
var CacheSize = settings.RegisterIntSetting(
	"sql.catalog.hydrated_tables.cache_size",
	"number of table descriptor versions retained in the hydratedtables LRU cache",
	128,
	settings.NonNegativeInt,
)

// NewCache constructs a new Cache.
func NewCache(settings *cluster.Settings) *Cache {
	c := &Cache{
		settings: settings,
		metrics:  makeMetrics(),
	}
	c.mu.cache = cache.NewOrderedCache(cache.Config{
		Policy: cache.CacheLRU,
		ShouldEvict: func(size int, key, value interface{}) bool {
			return size > int(CacheSize.Get(&settings.SV))
		},
		OnEvicted: func(key, value interface{}) {
			putCacheKey(key.(*cacheKey))
		},
	})
	return c
}

type hydratedTableDescriptor struct {
	tableDesc catalog.TableDescriptor
	typeDescs []*cachedType
}

type cachedType struct {
	name    tree.TypeName
	typDesc catalog.TypeDescriptor
}

// GetHydratedTableDescriptor returns an ImmutableCopy with the types
// hydrated. It may use a cached copy but all of the relevant type descriptors
// will be retrieved via the TypeDescriptorResolver. Note that if the table
// descriptor is modified, nil will be returned. If any of the types used by
// the table or have uncommitted versions, then nil may be returned. If a nil
// descriptor is returned, it up to the caller to clone and hydrate the table
// descriptor on their own. If the table descriptor does not contain any
// user-defined types, it will be returned unchanged.
func (c *Cache) GetHydratedTableDescriptor(
	ctx context.Context, table catalog.TableDescriptor, res catalog.TypeDescriptorResolver,
) (hydrated catalog.TableDescriptor, err error) {

	// If the table has an uncommitted version, it cannot be cached. Return nil
	// forcing the caller to hydrate.
	if table.IsUncommittedVersion() {
		return nil, nil
	}

	// If the table has no user defined types, it is already effectively hydrated,
	// so just return it.
	if !table.ContainsUserDefinedTypes() {
		return table, nil
	}

	// TODO(ajwerner): This cache may thrash a bit right when a version of a type
	// changes as different callers oscillate evicting the old and new versions of
	// that type. It should converge rapidly but nevertheless, a finer-granularity
	// cache which stored descriptors by not just the table version but also by
	// the set of type-versions could mitigate the problem. The idea would be to
	// cache all tuples of table and type versions and then check if what we get
	// from the resolver matches any of them. Only long-running transactions
	// should still resolve older versions. Furthermore, the cache has a policy to
	// not evict never versions of types for older ones. The downside is that
	// long-running transactions may end up re-hydrating for every statement.
	//
	// Probably a better solution would be to cache the hydrated descriptor in the
	// descs.Collection and invalidate that cache whenever an uncommitted
	// descriptor that is relevant is added. That way any given transaction will
	// only ever hydrate a table at most once per modification of a descriptor.
	// and the cache will converge on new versions rapidly.
	var groupKey string // used as a proxy for cache hit
	defer func() {
		if hydrated != nil {
			c.recordMetrics(groupKey == "" /* hitCache */)
		}
	}()
	key := newCacheKey(table)
	defer func() {
		if key != nil {
			putCacheKey(key)
		}
	}()

	// A loop is required in cases where concurrent operations enter this method,
	// concurrently determine that they cannot use the cached value but for
	// different reasons, then serialize on the singleflight group. The goroutine
	// which does not win the singleflight race will need to verify that the
	// newly added value can be used before it can use it.
	for {
		if cached, ok := c.getFromCache(key); ok {
			canUse, skipCache, err := canUseCachedDescriptor(ctx, cached, res)
			if err != nil || skipCache {
				return nil, err
			}
			if canUse {
				return cached.tableDesc, nil
			}
		}

		// Now we want to lock this key and populate the descriptors.
		// Using a singleflight prevent concurrent attempts to update the cache for
		// a given descriptor version.
		if groupKey == "" {
			groupKey = fmt.Sprintf("%d@%d", key.ID, key.Version)
		}

		// Only the calling goroutine can actually use the result directly.
		// Furthermore, if an error is encountered, only the calling goroutine
		// should return it. Other goroutines will have to go back around and
		// attempt to read from the cache.
		var called bool
		res, _, err := c.g.Do(groupKey, func() (interface{}, error) {
			called = true
			cachedRes := cachedTypeDescriptorResolver{
				underlying: res,
				cache:      map[descpb.ID]*cachedType{},
			}
			descBase := protoutil.Clone(table.TableDesc()).(*descpb.TableDescriptor)
			if err := typedesc.HydrateTypesInTableDescriptor(ctx, descBase, &cachedRes); err != nil {
				return nil, err
			}
			hydrated := tabledesc.NewBuilder(descBase).BuildImmutableTable()

			// If any of the types resolved as part of hydration are modified, skip
			// writing this descriptor to the cache.
			if !cachedRes.haveUncommitted {
				c.addToCache(key, &hydratedTableDescriptor{
					tableDesc: hydrated,
					typeDescs: cachedRes.types,
				})

				// Prevent the key from being put back in the pool as it is now a member
				// of the cache's data structure. It will be released when the entry is
				// evicted.
				key = nil
			}

			return hydrated, nil
		})

		// Another goroutine populated the cache or failed to due to having a
		// modified descriptor, go back around and see if the	new cache entry can be
		// used, or if another round of re-population is in order.
		if !called {
			continue
		}
		if err != nil {
			return nil, err
		}
		return res.(catalog.TableDescriptor), nil
	}
}

// canUseCachedDescriptor returns whether the types stored in the cached
// descriptor are the same types which are resolved through res.
//
// The skipCache return value indicates that either one of the types returned
// from res contain uncommitted versions or that a resolved types descriptor is
// from an older version than the currently cached value. We don't want to wind
// up evicting a newer version with an older version.
func canUseCachedDescriptor(
	ctx context.Context, cached *hydratedTableDescriptor, res catalog.TypeDescriptorResolver,
) (canUse, skipCache bool, _ error) {
	for _, typ := range cached.typeDescs {
		name, typDesc, err := res.GetTypeDescriptor(ctx, typ.typDesc.GetID())
		if err != nil {
			return false, false, err
		}
		// Ensure that the type is not an uncommitted version.
		if isUncommittedVersion := typDesc.IsUncommittedVersion(); isUncommittedVersion ||
			// Ensure that the type version matches.
			typ.typDesc.GetVersion() != typDesc.GetVersion() ||
			// Only match on the name if the retrieved type has a qualified name.
			// Note that this behavior is important and ensures that when this
			// function is passed a resolved which looks up qualified names, it always
			// get a hydrated descriptor with qualified names in its types.
			// This is important as we share this cache between distsql which does
			// not resolve type names and the local planner which does. It'd be a real
			// bummer if mixes of distributed and local flows lead to thrashing of the
			// cache.
			(name.ObjectNamePrefix != (tree.ObjectNamePrefix{}) && typ.name != name) {

			// TODO(ajwerner): Make the TypeDescriptorResolver return a
			// ResolvedObjectPrefix instead of a tree.TypeName so that we can
			// determine whether the mismatched name is too old or too new.
			skipCache = isUncommittedVersion || typ.typDesc.GetVersion() > typDesc.GetVersion()
			return false, skipCache, nil
		}
	}
	return true, false, nil
}

// getFromCache locks the cache and retrieves the descriptor with the given key.
func (c *Cache) getFromCache(key *cacheKey) (*hydratedTableDescriptor, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	got, ok := c.mu.cache.Get(key)
	if !ok {
		return nil, false
	}
	return got.(*hydratedTableDescriptor), true
}

// getFromCache locks the cache and stores the descriptor with the given key.
func (c *Cache) addToCache(key *cacheKey, toCache *hydratedTableDescriptor) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.cache.Add(key, toCache)
}

func (c *Cache) recordMetrics(hitCache bool) {
	if hitCache {
		c.metrics.Hits.Inc(1)
	} else {
		c.metrics.Misses.Inc(1)
	}
}

type cachedTypeDescriptorResolver struct {
	underlying      catalog.TypeDescriptorResolver
	cache           map[descpb.ID]*cachedType
	types           []*cachedType
	haveUncommitted bool
}

func (c *cachedTypeDescriptorResolver) GetTypeDescriptor(
	ctx context.Context, id descpb.ID,
) (tree.TypeName, catalog.TypeDescriptor, error) {
	if typ, exists := c.cache[id]; exists {
		return typ.name, typ.typDesc, nil
	}
	name, typDesc, err := c.underlying.GetTypeDescriptor(ctx, id)
	if err != nil {
		return tree.TypeName{}, nil, err
	}
	typ := &cachedType{
		name:    name,
		typDesc: typDesc,
	}
	c.cache[id] = typ
	c.types = append(c.types, typ)
	c.haveUncommitted = c.haveUncommitted || typDesc.IsUncommittedVersion()
	return name, typDesc, nil
}

type cacheKey lease.IDVersion

func (c cacheKey) Compare(comparable llrb.Comparable) int {
	o := comparable.(*cacheKey)
	switch {
	case c.ID < o.ID:
		return -1
	case c.ID > o.ID:
		return 1
	default:
		switch {
		case c.Version < o.Version:
			return -1
		case c.Version > o.Version:
			return 1
		default:
			return 0
		}
	}
}

var _ llrb.Comparable = (*cacheKey)(nil)

var cacheKeySyncPool = sync.Pool{
	New: func() interface{} { return new(cacheKey) },
}

func newCacheKey(table catalog.TableDescriptor) *cacheKey {
	k := cacheKeySyncPool.Get().(*cacheKey)
	*k = cacheKey{
		ID:      table.GetID(),
		Version: table.GetVersion(),
	}
	return k
}

func putCacheKey(k *cacheKey) {
	*k = cacheKey{}
	cacheKeySyncPool.Put(k)
}

// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package hydrateddesccache contains logic to cache descriptors with user
// defined types hydrated.
package hydrateddesccache

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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

// TODO(ajwerner): Consider adding a mechanism to remove entries which have not
// been used in a long time.

// Cache caches descriptors which have their user-defined types hydrated.
// The cache's contract is a bit tricky. In order to use a hydrated type, the
// caller needs to have a lease on the relevant type descriptor. The way that
// this is made to work is that the user provides a handle to a leased
// ImmutableCopy and then the cache will call through to type resolver for each
// of the referenced types which ensures that user always uses properly leased
// descriptors. While all of the types will need to be resolved, they should
// already be cached so, in this way, this cache prevents the need to copy
// and re-construct the immutable descriptors in most cases.
type Cache struct {
	settings *cluster.Settings
	g        *singleflight.Group
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
	Hits   map[catalog.DescriptorType]*metric.Counter
	Misses map[catalog.DescriptorType]*metric.Counter
	// AllMetrics is only used to make sure that the metrics registry is able to
	// add metric metadata to record.
	AllMetrics [8]*metric.Counter
}

func makeMetrics() Metrics {
	ret := Metrics{
		Hits: map[catalog.DescriptorType]*metric.Counter{
			catalog.Table:    metric.NewCounter(tableMetaHits),
			catalog.Function: metric.NewCounter(funcMetaHits),
			catalog.Schema:   metric.NewCounter(schemaMetaHits),
			catalog.Type:     metric.NewCounter(typeMetaHits),
		},
		Misses: map[catalog.DescriptorType]*metric.Counter{
			catalog.Table:    metric.NewCounter(tableMetaMisses),
			catalog.Function: metric.NewCounter(funcMetaMisses),
			catalog.Schema:   metric.NewCounter(schemaMetaMisses),
			catalog.Type:     metric.NewCounter(typeMetaMisses),
		},
	}
	idx := 0
	for _, c := range ret.Hits {
		ret.AllMetrics[idx] = c
		idx++
	}
	for _, c := range ret.Misses {
		ret.AllMetrics[idx] = c
		idx++
	}
	return ret
}

// MetricStruct makes Metrics a metric.Struct.
func (m *Metrics) MetricStruct() {}

var (
	tableMetaHits = metric.Metadata{
		Name:        "sql.hydrated_table_cache.hits",
		Help:        "counter on the number of cache hits",
		Measurement: "reads",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	tableMetaMisses = metric.Metadata{
		Name:        "sql.hydrated_table_cache.misses",
		Help:        "counter on the number of cache misses",
		Measurement: "reads",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	funcMetaHits = metric.Metadata{
		Name:        "sql.hydrated_udf_cache.hits",
		Help:        "counter on the number of cache hits",
		Measurement: "reads",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	funcMetaMisses = metric.Metadata{
		Name:        "sql.hydrated_udf_cache.misses",
		Help:        "counter on the number of cache misses",
		Measurement: "reads",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	schemaMetaHits = metric.Metadata{
		Name:        "sql.hydrated_schema_cache.hits",
		Help:        "counter on the number of cache hits",
		Measurement: "reads",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	schemaMetaMisses = metric.Metadata{
		Name:        "sql.hydrated_schema_cache.misses",
		Help:        "counter on the number of cache misses",
		Measurement: "reads",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	typeMetaHits = metric.Metadata{
		Name:        "sql.hydrated_type_cache.hits",
		Help:        "counter on the number of cache hits",
		Measurement: "reads",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	typeMetaMisses = metric.Metadata{
		Name:        "sql.hydrated_type_cache.misses",
		Help:        "counter on the number of cache misses",
		Measurement: "reads",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
)

// CacheSize controls the size of the LRU cache.
var CacheSize = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.catalog.hydrated_tables.cache_size",
	"number of table descriptor versions retained in the hydratedtables LRU cache",
	128,
	settings.NonNegativeInt,
)

// NewCache constructs a new Cache.
func NewCache(settings *cluster.Settings) *Cache {
	c := &Cache{
		settings: settings,
		g:        singleflight.NewGroup("get-hydrated-descriptor", "key"),
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

type cachedDescriptor struct {
	desc      catalog.Descriptor
	typeDescs []*cachedType
}

type cachedType struct {
	name    tree.TypeName
	typDesc catalog.TypeDescriptor
}

// GetHydratedTableDescriptor is the same as GetHydratedDescriptor but specific
// to table descriptors.
func (c *Cache) GetHydratedTableDescriptor(
	ctx context.Context, table catalog.TableDescriptor, res catalog.TypeDescriptorResolver,
) (hydrated catalog.TableDescriptor, err error) {
	hydratedDesc, err := c.GetHydratedDescriptor(ctx, table, res)
	if err != nil {
		return nil, err
	}
	if hydratedDesc == nil {
		return nil, nil
	}
	return hydratedDesc.(catalog.TableDescriptor), nil
}

// GetHydratedFunctionDescriptor is the same as GetHydratedDescriptor but
// specific to function descriptors.
func (c *Cache) GetHydratedFunctionDescriptor(
	ctx context.Context, funDesc catalog.FunctionDescriptor, res catalog.TypeDescriptorResolver,
) (catalog.FunctionDescriptor, error) {
	hydratedDesc, err := c.GetHydratedDescriptor(ctx, funDesc, res)
	if err != nil {
		return nil, err
	}
	if hydratedDesc == nil {
		return nil, nil
	}
	return hydratedDesc.(catalog.FunctionDescriptor), nil
}

// GetHydratedSchemaDescriptor is the same as GetHydratedDescriptor but
// specific to schema descriptors.
func (c *Cache) GetHydratedSchemaDescriptor(
	ctx context.Context, funDesc catalog.SchemaDescriptor, res catalog.TypeDescriptorResolver,
) (catalog.SchemaDescriptor, error) {
	hydratedDesc, err := c.GetHydratedDescriptor(ctx, funDesc, res)
	if err != nil {
		return nil, err
	}
	if hydratedDesc == nil {
		return nil, nil
	}
	return hydratedDesc.(catalog.SchemaDescriptor), nil
}

// GetHydratedDescriptor returns a descriptor copy with the types hydrated. It
// may use a cached copy but all of the relevant type descriptors will be
// retrieved via the TypeDescriptorResolver. Note that if the given descriptor
// is modified, nil will be returned. If any of the types used by the descriptor
// or have uncommitted versions, then nil may be returned. If a nil descriptor
// is returned, it up to the caller to clone and hydrate the hydrate descriptor
// on their own. If the descriptor does not contain any user-defined types, it
// will be returned unchanged.
func (c *Cache) GetHydratedDescriptor(
	ctx context.Context, hydratable catalog.Descriptor, res catalog.TypeDescriptorResolver,
) (hydrated catalog.Descriptor, err error) {

	// If the table has an uncommitted version, it cannot be cached. Return nil
	// forcing the caller to hydrate.
	if hydratable.IsUncommittedVersion() {
		return nil, nil
	}

	// If the table has no user defined types, it is already effectively hydrated,
	// so just return it.
	if !hydratable.MaybeRequiresTypeHydration() {
		return hydratable, nil
	}

	// TODO(ajwerner): This cache may thrash a bit right when a version of a type
	// changes as different callers oscillate evicting the old and new versions of
	// that type. It should converge rapidly but nevertheless, a finer-granularity
	// cache which stored descriptors by not just the descriptor version but also by
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
			c.recordMetrics(groupKey == "" /* hitCache */, hydratable.DescriptorType())
		}
	}()
	key := newCacheKey(hydratable)
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
				return cached.desc, nil
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
		res, _, err := c.g.Do(ctx, groupKey, func(ctx context.Context) (interface{}, error) {
			called = true
			cachedRes := cachedTypeDescriptorResolver{
				underlying: res,
				cache:      map[descpb.ID]*cachedType{},
			}

			cpy := hydratable.NewBuilder().BuildImmutable()
			if err := typedesc.HydrateTypesInDescriptor(ctx, cpy, &cachedRes); err != nil {
				return nil, err
			}
			cachedDesc := cachedDescriptor{
				desc:      cpy,
				typeDescs: cachedRes.types,
			}

			// If any of the types resolved as part of hydration are modified, skip
			// writing this descriptor to the cache.
			if !cachedRes.haveUncommitted {
				c.addToCache(key, &cachedDesc)

				// Prevent the key from being put back in the pool as it is now a member
				// of the cache's data structure. It will be released when the entry is
				// evicted.
				key = nil
			}

			return cachedDesc.desc, nil
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
		return res.(catalog.Descriptor), nil
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
	ctx context.Context, cached *cachedDescriptor, res catalog.TypeDescriptorResolver,
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
func (c *Cache) getFromCache(key *cacheKey) (*cachedDescriptor, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	got, ok := c.mu.cache.Get(key)
	if !ok {
		return nil, false
	}
	return got.(*cachedDescriptor), true
}

// getFromCache locks the cache and stores the descriptor with the given key.
func (c *Cache) addToCache(key *cacheKey, toCache *cachedDescriptor) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.cache.Add(key, toCache)
}

func (c *Cache) recordMetrics(hitCache bool, descType catalog.DescriptorType) {
	var ct *metric.Counter
	if hitCache {
		ct = c.metrics.Hits[descType]
	} else {
		ct = c.metrics.Misses[descType]
	}
	if ct != nil {
		ct.Inc(1)
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

func newCacheKey(desc catalog.Descriptor) *cacheKey {
	k := cacheKeySyncPool.Get().(*cacheKey)
	*k = cacheKey{
		ID:      desc.GetID(),
		Version: desc.GetVersion(),
	}
	return k
}

func putCacheKey(k *cacheKey) {
	*k = cacheKey{}
	cacheKeySyncPool.Put(k)
}

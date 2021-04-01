// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geosprepared

import (
	"crypto/sha1"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/geo/geos"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

const maxCacheSize = 100

type hash [sha1.Size]byte

// preparedGeometryCache is a cache for PreparedGeometry objects.
type preparedGeometryCache struct {
	theCache map[hash]geos.PreparedGeometry
	keys     []hash
}

// prepareAndCache prepares a given geometry and caches it.
func (m *preparedGeometryCache) prepareAndCache(
	key hash, geo geopb.SpatialObject,
) (geos.PreparedGeometry, error) {
	prep, err := geos.PrepareGeometry(geo.EWKB)
	if err != nil {
		return nil, err
	}
	err = m.add(key, prep)
	if err != nil {
		return nil, err
	}
	return prep, nil
}

// add adds a key and prepared geometry to the cache, replacing an
// existing key if the cache is full.
func (m *preparedGeometryCache) add(key hash, prep geos.PreparedGeometry) error {
	if m.theCache == nil {
		m.theCache = make(map[hash]geos.PreparedGeometry)
	}
	if _, ok := m.theCache[key]; ok {
		return errors.Newf("cache already contains key")
	}
	if len(m.keys) == maxCacheSize {
		err := m.replaceKeyWithNewKey(key)
		if err != nil {
			return err
		}
	} else {
		m.keys = append(m.keys, key)
	}
	m.theCache[key] = prep
	if len(m.theCache) != len(m.keys) {
		return errors.Newf("number of keys in cache and keys list do not match")
	}
	return nil
}

// get returns a prepared geometry for a key if it exists.
func (m *preparedGeometryCache) get(key hash) (geos.PreparedGeometry, bool) {
	prep, ok := m.theCache[key]
	return prep, ok
}

// replaceKeyWithNewKey replaces an existing key in the keys list with a given
// new key. The current replacement strategy is random replacement.
func (m *preparedGeometryCache) replaceKeyWithNewKey(newKey hash) error {
	if len(m.keys) == 0 {
		return errors.Newf("no keys to replace")
	}
	oldIdx := rand.Intn(len(m.keys))
	oldKey := m.keys[oldIdx]
	geos.PreparedGeomDestroy(m.theCache[oldKey])
	delete(m.theCache, oldKey)
	m.keys[oldIdx] = newKey
	return nil
}

// destroy clears the contents of the cache.
func (m *preparedGeometryCache) destroy() {
	for _, g := range m.theCache {
		geos.PreparedGeomDestroy(g)
	}
	m.theCache = nil
	m.keys = nil
}

// Cache is an instance of a GEOS Cache.
type Cache struct {
	mu struct {
		lock  syncutil.Mutex
		cache preparedGeometryCache
	}
}

// Free releases all resources in the cache.
func (c *Cache) Free() {
	c.mu.lock.Lock()
	defer c.mu.lock.Unlock()
	c.mu.cache.destroy()
}

// Get will attempt to fetch from the cache the prepared geometry
// for either a or b, returning the prepared geometry of one side
// and the spatial object for the other.
// If neither is found, it will attempt to prepare one of either
// a or b.
// If neither shape should be cached, it will return a nil
// PreparedGeometry.
func (c *Cache) Get(
	a geopb.SpatialObject, b geopb.SpatialObject,
) (geos.PreparedGeometry, geopb.SpatialObject, error) {
	c.mu.lock.Lock()
	defer c.mu.lock.Unlock()

	// First check if we already have either geometry cached.
	aKey := key(a.EWKB)
	if prep, ok := c.mu.cache.get(aKey); ok {
		return prep, b, nil
	}
	bKey := key(b.EWKB)
	if prep, ok := c.mu.cache.get(bKey); ok {
		return prep, a, nil
	}

	// Now determine which one to cache. If both are valid, choose the
	// longer EWKB.
	shouldCacheA, shouldCacheB := shouldCache(a), shouldCache(b)
	if shouldCacheA && shouldCacheB {
		if len(a.EWKB) > len(b.EWKB) {
			shouldCacheB = false
		} else {
			shouldCacheA = false
		}
	}

	switch {
	case shouldCacheA:
		prep, err := c.mu.cache.prepareAndCache(aKey, a)
		if err != nil {
			return nil, geopb.SpatialObject{}, err
		}
		return prep, b, nil
	case shouldCacheB:
		prep, err := c.mu.cache.prepareAndCache(bKey, b)
		if err != nil {
			return nil, geopb.SpatialObject{}, err
		}
		return prep, a, nil
	default:
		return nil, geopb.SpatialObject{}, nil
	}
}

// key returns the hash for a geometry's EWKB.
func key(b geopb.EWKB) hash {
	return sha1.Sum(b)
}

// shouldCache returns whether the given spatial object should be cached.
func shouldCache(geo geopb.SpatialObject) bool {
	switch geo.ShapeType.To2D() {
	case geopb.ShapeType_Point, geopb.ShapeType_MultiPoint:
		return false
	default:
		return true
	}
}

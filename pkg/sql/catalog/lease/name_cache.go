// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package lease

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// nameCacheKey is a key for the descriptor cache, with the same fields
// as the system.namespace key: name is the descriptor name; parentID is
// populated for schemas, descriptors, and types; and parentSchemaID is
// populated for descriptors and types.
type nameCacheKey struct {
	parentID       descpb.ID
	parentSchemaID descpb.ID
	name           string
}

// nameCache is a cache of descriptor name -> latest version mappings.
// The Manager updates the cache every time a lease is acquired or released
// from the store. The cache maintains the latest version for each name.
// All methods are thread-safe.
type nameCache struct {
	mu          syncutil.Mutex
	descriptors map[nameCacheKey]*descriptorVersionState
}

// Resolves a (qualified) name to the descriptor's ID.
// Returns a valid descriptorVersionState for descriptor with that name,
// if the name had been previously cached and the cache has a descriptor
// version that has not expired. Returns nil otherwise.
// This method handles normalizing the descriptor name.
// The descriptor's refcount is incremented before returning, so the caller
// is responsible for releasing it to the leaseManager.
func (c *nameCache) get(
	parentID descpb.ID, parentSchemaID descpb.ID, name string, timestamp hlc.Timestamp,
) *descriptorVersionState {
	c.mu.Lock()
	desc, ok := c.descriptors[makeNameCacheKey(parentID, parentSchemaID, name)]
	c.mu.Unlock()
	if !ok {
		return nil
	}
	desc.mu.Lock()
	if desc.mu.lease == nil {
		desc.mu.Unlock()
		// This get() raced with a release operation. Remove this cache
		// entry if needed.
		c.remove(desc)
		return nil
	}

	defer desc.mu.Unlock()

	if !NameMatchesDescriptor(desc, parentID, parentSchemaID, name) {
		panic(errors.AssertionFailedf("out of sync entry in the name cache. "+
			"Cache entry: (%d, %d, %q) -> %d. Lease: (%d, %d, %q).",
			parentID, parentSchemaID, name,
			desc.GetID(),
			desc.GetParentID(), desc.GetParentSchemaID(), desc.GetName()),
		)
	}

	// Expired descriptor. Don't hand it out.
	if desc.hasExpiredLocked(timestamp) {
		return nil
	}

	desc.incRefcountLocked()
	return desc
}

func (c *nameCache) insert(desc *descriptorVersionState) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := makeNameCacheKey(desc.GetParentID(), desc.GetParentSchemaID(), desc.GetName())
	existing, ok := c.descriptors[key]
	if !ok {
		c.descriptors[key] = desc
		return
	}
	// If we already have a lease in the cache for this name, see if this one is
	// better (higher version or later expiration).
	if desc.GetVersion() > existing.GetVersion() ||
		(desc.GetVersion() == existing.GetVersion() &&
			existing.getExpiration().Less(desc.getExpiration())) {
		// Overwrite the old lease. The new one is better. From now on, we want
		// clients to use the new one.
		c.descriptors[key] = desc
	}
}

func (c *nameCache) remove(desc *descriptorVersionState) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := makeNameCacheKey(desc.GetParentID(), desc.GetParentSchemaID(), desc.GetName())
	existing, ok := c.descriptors[key]
	if !ok {
		// Descriptor for lease not found in name cache. This can happen if we had
		// a more recent lease on the descriptor in the nameCache, then the
		// descriptor gets dropped, then the more recent lease is remove()d - which
		// clears the cache.
		return
	}
	// If this was the lease that the cache had for the descriptor name, remove
	// it. If the cache had some other descriptor, this remove is a no-op.
	if existing == desc {
		delete(c.descriptors, key)
	}
}

func makeNameCacheKey(parentID descpb.ID, parentSchemaID descpb.ID, name string) nameCacheKey {
	return nameCacheKey{parentID, parentSchemaID, name}
}

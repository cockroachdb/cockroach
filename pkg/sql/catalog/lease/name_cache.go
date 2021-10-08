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
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

func makeNameCache() nameCache {
	return nameCache{descriptors: nstree.MakeMap()}
}

// nameCache is a cache of descriptor name -> latest version mappings.
// The Manager updates the cache every time a lease is acquired or released
// from the store. The cache maintains the latest version for each name.
// All methods are thread-safe.
type nameCache struct {
	mu          syncutil.Mutex
	descriptors nstree.Map
}

// Resolves a (qualified) name to the descriptor's ID.
// Returns a valid descriptorVersionState for descriptor with that name,
// if the name had been previously cached and the cache has a descriptor
// version that has not expired. Returns nil otherwise.
// This method handles normalizing the descriptor name.
// The descriptor's refcount is incremented before returning, so the caller
// is responsible for releasing it to the leaseManager.
func (c *nameCache) get(
	ctx context.Context,
	parentID descpb.ID,
	parentSchemaID descpb.ID,
	name string,
	timestamp hlc.Timestamp,
) *descriptorVersionState {
	c.mu.Lock()
	desc, ok := c.descriptors.GetByName(
		parentID, parentSchemaID, name,
	).(*descriptorVersionState)
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

	desc.incRefCountLocked(ctx)
	return desc
}

func (c *nameCache) insert(desc *descriptorVersionState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	got, ok := c.descriptors.GetByName(
		desc.GetParentID(), desc.GetParentSchemaID(), desc.GetName(),
	).(*descriptorVersionState)
	if ok && desc.getExpiration().Less(got.getExpiration()) {
		return
	}
	c.descriptors.Upsert(desc)
}

func (c *nameCache) remove(desc *descriptorVersionState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// If this was the lease that the cache had for the descriptor name, remove
	// it. If the cache had some other descriptor, this remove is a no-op.
	if got := c.descriptors.GetByID(desc.GetID()); got == desc {
		c.descriptors.Remove(desc.GetID())
	}
}

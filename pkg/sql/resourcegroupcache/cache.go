// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package resourcegroupcache resolves user-visible resource group names
// to the numeric ids used by admission control.
//
// Today the only resource groups that exist are the two built-ins seeded
// by the CPU time token WorkQueue, so the cache is backed by a static
// map. The type and its LookupByName API exist so that the session-var
// SET path resolves names through a stable interface; a later change
// replaces the static map with a rangefeed-backed cache over a system
// table that holds user-defined groups, without touching callers.
package resourcegroupcache

import "github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"

const (
	// CRDBInternalResourceGroupHigh is the name of the built-in resource
	// group for high-priority work (priority >= NormalPri in the
	// priority-derived routing).
	CRDBInternalResourceGroupHigh = "crdb_internal_resource_group_high"
	// CRDBInternalResourceGroupLow is the name of the built-in resource
	// group for low-priority work (priority < NormalPri).
	CRDBInternalResourceGroupLow = "crdb_internal_resource_group_low"
)

// builtinNameToID maps the well-known built-in names to their numeric
// ids, which are defined alongside the ResourceGroupID type so the work
// queue and this cache share a single source of truth.
var builtinNameToID = map[string]uint64{
	CRDBInternalResourceGroupHigh: uint64(admissionpb.HighResourceGroupID),
	CRDBInternalResourceGroupLow:  uint64(admissionpb.LowResourceGroupID),
}

// Cache resolves resource group names to ids. It carries no state today;
// the field exists on ExecutorConfig so the resolution point is fixed
// ahead of the rangefeed-backed implementation.
type Cache struct{}

// New constructs a Cache.
func New() *Cache {
	return &Cache{}
}

// LookupByName returns the numeric id for the named resource group. The
// lookup is case-sensitive: names must match exactly. ok is false if the
// name is not a known resource group.
func (c *Cache) LookupByName(name string) (id uint64, ok bool) {
	id, ok = builtinNameToID[name]
	return id, ok
}

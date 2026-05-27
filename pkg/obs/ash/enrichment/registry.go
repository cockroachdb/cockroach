// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package enrichment

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// caches is the process-wide registry of enrichment caches, keyed by
// tenant ID. Each sql.Server registers its Cache here at startup so
// that out-of-band consumers (notably the GetASHEnrichmentData status
// RPC handler) can resolve the right cache for an inbound request.
//
// The registry is necessary because Cache instances are owned by
// individual sql.Servers but the status server runs at the process
// level. A multi-tenant shared-process deployment has one Cache per
// tenant; the RPC handler keys on the inbound tenant context to find
// the correct one.
var caches syncutil.Map[roachpb.TenantID, Cache]

// Register publishes c into the process-wide registry under tenantID.
// It should be called once at sql.Server startup. Registering more
// than once for the same tenantID replaces the previous entry — this
// is safe in the normal lifecycle because sql.Server is a singleton
// per tenant, but tests that recreate sql.Servers will see the latest.
func Register(tenantID roachpb.TenantID, c *Cache) {
	caches.Store(tenantID, c)
}

// Unregister removes the cache for tenantID. Used during sql.Server
// shutdown so a fresh server in the same process can re-register
// cleanly.
func Unregister(tenantID roachpb.TenantID) {
	caches.Delete(tenantID)
}

// Lookup returns the cache registered for tenantID, or nil if no
// cache is registered (e.g., the enrichment subsystem hasn't been
// initialized for this tenant yet).
func Lookup(tenantID roachpb.TenantID) *Cache {
	c, _ := caches.Load(tenantID)
	return c
}

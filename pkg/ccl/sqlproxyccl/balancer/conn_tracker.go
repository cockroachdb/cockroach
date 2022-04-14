// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package balancer

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/logtags"
)

// cacheRefreshInterval refers to the interval that the tenant caches are
// periodically refreshed. Note that the cache is also updated manually whenever
// a connection gets added to or removed from the connection tracker.
const cacheRefreshInterval = 15 * time.Second

// ConnTracker tracks all connection handles associated with each tenant, and
// handles are grouped by SQL pods.
type ConnTracker struct {
	// timeSource is the source of the time, and uses timeutil.DefaultTimeSource
	// by default. This is often replaced in tests.
	timeSource timeutil.TimeSource

	// mu synchronizes access to the list of tenants.
	mu struct {
		syncutil.Mutex

		// tenants refer to a list of tenant entries.
		tenants map[roachpb.TenantID]*tenantEntry
	}
}

// NewConnTracker returns a new instance of the connection tracker. All exposed
// methods on the connection tracker are thread-safe.
func NewConnTracker(
	ctx context.Context, stopper *stop.Stopper, timeSource timeutil.TimeSource,
) (*ConnTracker, error) {
	// Ensure that ctx gets cancelled on stopper's quiescing.
	ctx, _ = stopper.WithCancelOnQuiesce(ctx)

	if timeSource == nil {
		timeSource = timeutil.DefaultTimeSource{}
	}

	t := &ConnTracker{timeSource: timeSource}
	t.mu.tenants = make(map[roachpb.TenantID]*tenantEntry)

	if err := stopper.RunAsyncTask(
		ctx, "refresh-tenant-cache", t.refreshTenantCachesLoop,
	); err != nil {
		return nil, err
	}
	return t, nil
}

// OnConnect registers the connection handle for tracking under the given
// tenant. If the handler has already been registered, this returns false.
func (t *ConnTracker) OnConnect(tenantID roachpb.TenantID, handle ConnectionHandle) bool {
	e := t.getEntry(tenantID, true /* allowCreate */)
	success := e.addHandle(handle)
	if success {
		logTrackerEvent("OnConnect", handle)
	}
	return success
}

// OnDisconnect unregisters the connection handle under the given tenant. If
// the handler cannot be found, this returns false.
func (t *ConnTracker) OnDisconnect(tenantID roachpb.TenantID, handle ConnectionHandle) bool {
	e := t.getEntry(tenantID, false /* allowCreate */)
	if e == nil {
		return false
	}
	success := e.removeHandle(handle)
	if success {
		logTrackerEvent("OnDisconnect", handle)
	}
	return success
}

// GetConnsMap returns a snapshot of connections indexed by the pod's address
// that the connection is in for the given tenant.
func (t *ConnTracker) GetConnsMap(tenantID roachpb.TenantID) map[string][]ConnectionHandle {
	e := t.getEntry(tenantID, false /* allowCreate */)
	if e == nil {
		return nil
	}
	return e.getConnsMap()
}

// GetTenantCache returns the given tenant's cache that contains cached data
// about the tenant (e.g. number of active/idle connections). This cache gets
// refreshed with the current data periodically, and updated whenever a new
// connection gets added to or removed from the connection tracker.
// GetTenantCache returns nil if the tenant does not exist in the connection
// tracker.
//
// TODO(jaylim-crl): Consider moving the connection map returned from
// GetConnsMap into the tenant cache for a cleaner API. We'll revisit this
// decision once we add the rebalancing logic to the balancer.
func (t *ConnTracker) GetTenantCache(tenantID roachpb.TenantID) TenantCache {
	e := t.getEntry(tenantID, false /* allowCreate */)
	if e == nil {
		return nil
	}
	return e.getCache()
}

// GetTenantIDs returns a list of tenant IDs that have at least one connection
// registered with the connection tracker.
func (t *ConnTracker) GetTenantIDs() []roachpb.TenantID {
	t.mu.Lock()
	defer t.mu.Unlock()

	tenants := make([]roachpb.TenantID, 0, len(t.mu.tenants))
	for tenantID, entry := range t.mu.tenants {
		if entry.getConnsCount() > 0 {
			tenants = append(tenants, tenantID)
		}
	}
	return tenants
}

// getEntry retrieves the tenantEntry instance for the given tenant. If
// allowCreate is set to false, getEntry returns nil if the entry does not
// exist for the given tenant. On the other hand, if allowCreate is set to
// true, getEntry creates an entry if one does not exist, and returns that.
// getEntry will never return nil if allowCreate is true.
func (t *ConnTracker) getEntry(tenantID roachpb.TenantID, allowCreate bool) *tenantEntry {
	t.mu.Lock()
	defer t.mu.Unlock()

	entry, ok := t.mu.tenants[tenantID]
	if !ok && allowCreate {
		entry = newTenantEntry()
		t.mu.tenants[tenantID] = entry
	}
	return entry
}

// refreshTenantCachesLoop runs on a background goroutine to continuously
// refresh all tenant caches in the connection tracker, once every
// cacheRefreshInterval.
func (t *ConnTracker) refreshTenantCachesLoop(ctx context.Context) {
	timer := t.timeSource.NewTimer()
	defer timer.Stop()
	for {
		timer.Reset(cacheRefreshInterval)
		select {
		case <-ctx.Done():
			return
		case <-timer.Ch():
			timer.MarkRead()
			t.refreshTenantCaches()
		}
	}
}

// refreshTenantCaches refreshes the cache for all the tenants associated with
// the connection tracker.
func (t *ConnTracker) refreshTenantCaches() {
	tenantIDs := t.GetTenantIDs()
	for _, tenantID := range tenantIDs {
		e := t.getEntry(tenantID, false /* allowCreate */)
		if e != nil {
			e.refreshCache()
		}
	}
}

// logTrackerEvent logs an event based on the logtags attached to the connection
// goroutine.
func logTrackerEvent(event string, handle ConnectionHandle) {
	// The right approach would be for the caller to pass in a ctx object. For
	// simplicity, we'll just use a background context here since it's only used
	// for logging. Since we want logs to tie back to the connection, we'll copy
	// the logtags associated with the handle's context.
	logCtx := logtags.WithTags(context.Background(), logtags.FromContext(handle.Context()))
	log.Infof(logCtx, "%s: %s", event, handle.ServerRemoteAddr())
}

// tenantEntry is a connection tracker entry that stores connection information
// for a single tenant. All methods on tenantEntry are thread-safe.
type tenantEntry struct {
	// cache corresponds to the tenant cache that is used to cache the number of
	// active/idle connections by pod. A cache is used here to avoid frequent
	// computations of such numbers (which would require iterating all
	// connections for the given tenant).
	cache *tenantCache

	// mu synchronizes access to the list of connections associated with this
	// tenant.
	mu struct {
		syncutil.Mutex

		// conns refers to a set of connection handles.
		conns map[ConnectionHandle]struct{}
	}
}

// newTenantEntry returns a new instance of tenantEntry.
func newTenantEntry() *tenantEntry {
	e := &tenantEntry{
		cache: newTenantCache(),
	}
	e.mu.conns = make(map[ConnectionHandle]struct{})
	return e
}

// addHandle adds the handle to the entry based on the handle's active server
// remote address. This returns true if the operation was successful, and false
// otherwise.
func (e *tenantEntry) addHandle(handle ConnectionHandle) (success bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Handle already exists.
	if _, ok := e.mu.conns[handle]; ok {
		return false
	}
	e.mu.conns[handle] = struct{}{}

	// New connections are always active.
	e.cache.updateActiveCount(handle.ServerRemoteAddr(), 1)
	return true
}

// removeHandle deletes the handle from the tenant entry. This returns true if
// the operation was successful, and false otherwise.
func (e *tenantEntry) removeHandle(handle ConnectionHandle) (success bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Handle does not exists.
	if _, ok := e.mu.conns[handle]; !ok {
		return false
	}
	delete(e.mu.conns, handle)

	// We'll update the cache based on the data that we have now. It is possible
	// that the cache was computed using a different state (e.g. cache does not
	// include this connection, or cache counted this connection as active, but
	// is idle now), resulting in us decrementing from a different map here.
	// Regardless, since this is a cache, accuracy does not matter.
	if handle.IsIdle() {
		e.cache.updateIdleCount(handle.ServerRemoteAddr(), -1)
	} else {
		e.cache.updateActiveCount(handle.ServerRemoteAddr(), -1)
	}
	return true
}

// getConnsMap returns a snapshot of connections, indexed by server's address.
// Since this a snapshot, the mappings may be stale, if the connection was
// transferred to a different pod after the snapshot was made.
func (e *tenantEntry) getConnsMap() map[string][]ConnectionHandle {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Iterating 50K entries (the number of conns that we plan to support in
	// each proxy) would be a few ms (< 10ms). Since getConnsMap will only be
	// used during rebalancing, holding onto the lock while doing the below is
	// fine. Connection goroutines that invoke addHandle or removeHandle would
	// not have the forwarding affected, so this will not affect latency on the
	// user's end. Regardless, if we'd like to improve this, we could first
	// perform a copy of e.mu.conns, followed by releasing the lock before
	// calling ServerRemoteAddr (that uses a forwarder-specific lock under the
	// hood).
	conns := make(map[string][]ConnectionHandle)
	for handle := range e.mu.conns {
		// Connection has been closed.
		if handle.Context().Err() != nil {
			continue
		}
		addr := handle.ServerRemoteAddr()
		conns[addr] = append(conns[addr], handle)
	}
	return conns
}

// getConnsCount returns the number of connections associated with the tenant.
func (e *tenantEntry) getConnsCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()

	return len(e.mu.conns)
}

// getCache returns the cache for the given tenant.
func (e *tenantEntry) getCache() TenantCache {
	return e.cache
}

// refreshCache updates the cache associated with the tenant. This iterates
// through all connections for the tenant, so use with care.
func (e *tenantEntry) refreshCache() {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Construct cache based on current data.
	cacheData := newTenantCacheData()
	for handle := range e.mu.conns {
		// Connection has been closed.
		if handle.Context().Err() != nil {
			continue
		}
		if handle.IsIdle() {
			cacheData.idleConnsCount[handle.ServerRemoteAddr()]++
		} else {
			cacheData.activeConnsCount[handle.ServerRemoteAddr()]++
		}
	}
	e.cache.refreshData(cacheData)
}

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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/logtags"
)

// ConnTracker tracks all connection handles associated with each tenant, and
// handles are grouped by SQL pods.
type ConnTracker struct {
	mu struct {
		syncutil.Mutex
		tenants map[roachpb.TenantID]*tenantEntry
	}
}

// NewConnTracker returns a new instance of the connection tracker. All exposed
// methods on the connection tracker are thread-safe.
func NewConnTracker() *ConnTracker {
	t := &ConnTracker{}
	t.mu.tenants = make(map[roachpb.TenantID]*tenantEntry)
	return t
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
	// mu synchronizes access to the list of connections associated with this
	// tenant.
	mu struct {
		syncutil.Mutex
		conns map[ConnectionHandle]struct{}
	}
}

// newTenantEntry returns a new instance of tenantEntry.
func newTenantEntry() *tenantEntry {
	e := &tenantEntry{}
	e.mu.conns = make(map[ConnectionHandle]struct{})
	return e
}

// addHandle adds the handle to the entry based on the handle's active server
// remote address. This returns true if the operation was successful, and false
// otherwise.
func (e *tenantEntry) addHandle(handle ConnectionHandle) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Handle already exists.
	if _, ok := e.mu.conns[handle]; ok {
		return false
	}
	e.mu.conns[handle] = struct{}{}
	return true
}

// removeHandle deletes the handle from the tenant entry. This returns true if
// the operation was successful, and false otherwise.
func (e *tenantEntry) removeHandle(handle ConnectionHandle) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Handle does not exists.
	if _, ok := e.mu.conns[handle]; !ok {
		return false
	}
	delete(e.mu.conns, handle)
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

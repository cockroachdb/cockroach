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
	e := t.ensureTenantEntry(tenantID)
	success := e.addHandle(handle)
	if success {
		logTrackerEvent("OnConnect", handle)
	}
	return success
}

// OnDisconnect unregisters the connection handle under the given tenant. If
// the handler cannot be found, this returns false.
func (t *ConnTracker) OnDisconnect(tenantID roachpb.TenantID, handle ConnectionHandle) bool {
	e := t.ensureTenantEntry(tenantID)
	success := e.removeHandle(handle)
	if success {
		logTrackerEvent("OnDisconnect", handle)
	}
	return success
}

// GetConns returns a snapshot of connections for the given tenant.
func (t *ConnTracker) GetConns(tenantID roachpb.TenantID) []ConnectionHandle {
	e := t.ensureTenantEntry(tenantID)
	return e.getConns()
}

// GetAllConns snapshots the connection tracker, and returns a list of tenants
// together with handles associated with them. It is guaranteed that the tenants
// in the list will have at least one connection handle each.
func (t *ConnTracker) GetAllConns() map[roachpb.TenantID][]ConnectionHandle {
	// Note that we do this because GetConns() on the tenant entry may take
	// some time, and we don't want to hold onto t.mu while copying the
	// individual tenant entries.
	snapshotTenantEntries := func() map[roachpb.TenantID]*tenantEntry {
		t.mu.Lock()
		defer t.mu.Unlock()

		m := make(map[roachpb.TenantID]*tenantEntry)
		for tenantID, entry := range t.mu.tenants {
			m[tenantID] = entry
		}
		return m
	}

	m := make(map[roachpb.TenantID][]ConnectionHandle)
	tenantsCopy := snapshotTenantEntries()
	for tenantID, entry := range tenantsCopy {
		conns := entry.getConns()
		if len(conns) == 0 {
			continue
		}
		m[tenantID] = conns
	}
	return m
}

// ensureTenantEntry ensures that an entry has been created for the given
// tenant. If an entry already exists, that will be returned instead.
func (t *ConnTracker) ensureTenantEntry(tenantID roachpb.TenantID) *tenantEntry {
	t.mu.Lock()
	defer t.mu.Unlock()

	entry, ok := t.mu.tenants[tenantID]
	if !ok {
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

// getConns returns a snapshot of connections.
func (e *tenantEntry) getConns() []ConnectionHandle {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Perform a copy to avoid races whenever the map gets mutated through
	// addHandle or removeHandle. Copying 50K entries (the number of conns that
	// we plan to support in each proxy) would be a few ms (~5ms).
	conns := make([]ConnectionHandle, 0, len(e.mu.conns))
	for handle := range e.mu.conns {
		conns = append(conns, handle)
	}
	return conns
}

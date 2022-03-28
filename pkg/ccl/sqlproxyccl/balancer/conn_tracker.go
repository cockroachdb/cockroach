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
func (t *ConnTracker) OnConnect(tenantID roachpb.TenantID, handle ConnectionHandle) (success bool) {
	defer func() {
		if success {
			logTrackerEvent("OnConnect", handle)
		}
	}()

	e := t.ensureTenantEntry(tenantID)
	return e.addHandle(handle)
}

// OnDisconnect unregisters the connection handle under the given tenant. If
// the handler cannot be found, this returns false.
func (t *ConnTracker) OnDisconnect(
	tenantID roachpb.TenantID, handle ConnectionHandle,
) (success bool) {
	defer func() {
		if success {
			logTrackerEvent("OnDisconnect", handle)
		}
	}()

	e := t.ensureTenantEntry(tenantID)
	return e.removeHandle(handle)
}

// GetConns returns a snapshot of connections for the given tenant.
func (t *ConnTracker) GetConns(tenantID roachpb.TenantID) []*Conn {
	e := t.ensureTenantEntry(tenantID)
	return e.getConns()
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
	// Note that we use a separate logging context here since there's a chance
	// where the handle's context has been cancelled.
	logCtx := logtags.WithTags(context.Background(), logtags.FromContext(handle.Context()))
	log.Infof(logCtx, "%s: %s", event, handle.ServerRemoteAddr())
	// TODO(jaylim-crl): Add metrics.
}

// tenantEntry is a connection tracker entry that stores connection information
// for a single tenant. All methods on tenantEntry are thread-safe.
type tenantEntry struct {
	// mu synchronizes access to the list of connections associated with this
	// tenant.
	mu struct {
		syncutil.Mutex
		connSet map[ConnectionHandle]struct{}
		conns   []*Conn
	}
}

// newTenantEntry returns a new instance of tenantEntry.
func newTenantEntry() *tenantEntry {
	e := &tenantEntry{}
	e.mu.connSet = make(map[ConnectionHandle]struct{})
	return e
}

// addHandle adds the handle to the entry based on the handle's active server
// remote address. This returns true if the operation was successful, and false
// otherwise.
func (e *tenantEntry) addHandle(handle ConnectionHandle) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Handle already exists.
	if _, ok := e.mu.connSet[handle]; ok {
		return false
	}
	e.mu.connSet[handle] = struct{}{}
	e.mu.conns = append(e.mu.conns, &Conn{ConnectionHandle: handle})
	return true
}

// removeHandle deletes the handle from the tenant entry. This returns true if
// the operation was successful, and false otherwise.
func (e *tenantEntry) removeHandle(handle ConnectionHandle) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Handle does not exists.
	if _, ok := e.mu.connSet[handle]; !ok {
		return false
	}

	for i, conn := range e.mu.conns {
		if conn.ConnectionHandle == handle {
			delete(e.mu.connSet, handle)
			copy(e.mu.conns[i:], e.mu.conns[i+1:])
			e.mu.conns = e.mu.conns[:len(e.mu.conns)-1]
			return true
		}
	}

	// Should not happen.
	return false
}

// getConns returns a snapshot of connections.
func (e *tenantEntry) getConns() []*Conn {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.mu.conns
}

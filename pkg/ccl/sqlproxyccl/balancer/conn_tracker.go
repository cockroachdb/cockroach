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
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/logtags"
)

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
	// ctx, _ = stopper.WithCancelOnQuiesce(ctx)

	if timeSource == nil {
		timeSource = timeutil.DefaultTimeSource{}
	}

	t := &ConnTracker{timeSource: timeSource}
	t.mu.tenants = make(map[roachpb.TenantID]*tenantEntry)
	// TODO(jaylim-crl): add a background goroutine here to refresh active/idle
	// partitions.
	return t, nil
}

// GetConnsMap returns a snapshot of connections indexed by the pod's address
// that the connection is in for the given tenant.
//
// TODO(jaylim-crl): This is currently exposed for forwarder_test.go to retrieve
// the connections. This has to be unexposed once the balancer has methods that
// will return such assignments.
func (t *ConnTracker) GetConnsMap(tenantID roachpb.TenantID) map[string][]ConnectionHandle {
	e := t.getEntry(tenantID, false /* allowCreate */)
	if e == nil {
		return nil
	}
	return e.getConnsMap()
}

// registerAssignment registers the server assignment for tracking under the
// given tenant. If the assignment has already been registered, this is a no-op.
func (t *ConnTracker) registerAssignment(tenantID roachpb.TenantID, sa *ServerAssignment) {
	e := t.getEntry(tenantID, true /* allowCreate */)
	if e.addAssignment(sa) {
		logTrackerEvent("registerAssignment", sa)
	}
}

// unregisterAssignment unregisters the server assignment under the given
// tenant. If the assignment cannot be found, this is a no-op.
func (t *ConnTracker) unregisterAssignment(tenantID roachpb.TenantID, sa *ServerAssignment) {
	e := t.getEntry(tenantID, false /* allowCreate */)
	if e != nil && e.removeAssignment(sa) {
		logTrackerEvent("unregisterAssignment", sa)
	}
}

// getTenantIDs returns a list of tenant IDs that have at least one connection
// registered with the connection tracker.
func (t *ConnTracker) getTenantIDs() []roachpb.TenantID {
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

// logTrackerEvent logs an event based on the logtags attached to the connection
// goroutine.
func logTrackerEvent(event string, sa *ServerAssignment) {
	// Tests may create an assignment without an owner.
	owner := sa.Owner()
	if owner == nil {
		return
	}
	// The right approach would be for the caller to pass in a ctx object. For
	// simplicity, we'll just use a background context here since it's only used
	// for logging. Since we want logs to tie back to the connection, we'll copy
	// the logtags associated with the owner's context.
	logCtx := logtags.WithTags(context.Background(), logtags.FromContext(owner.Context()))
	log.Infof(logCtx, "%s: %s", event, sa.Addr())
}

// tenantEntry is a connection tracker entry that stores connection information
// for a single tenant. All methods on tenantEntry are thread-safe.
type tenantEntry struct {
	// mu synchronizes access to the list of connections associated with this
	// tenant.
	mu struct {
		syncutil.Mutex

		// assignments refers to a set of ServerAssignment objects.
		//
		// TODO(jaylim-crl): Break this into active and idle partitions.
		assignments map[*ServerAssignment]struct{}
	}
}

// newTenantEntry returns a new instance of tenantEntry.
func newTenantEntry() *tenantEntry {
	e := &tenantEntry{}
	e.mu.assignments = make(map[*ServerAssignment]struct{})
	return e
}

// addAssignment adds the given ServerAssignment to the tenant entry. This
// returns true if the operation was successful, and false otherwise.
func (e *tenantEntry) addAssignment(sa *ServerAssignment) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Assignment already exists.
	if _, ok := e.mu.assignments[sa]; ok {
		return false
	}
	e.mu.assignments[sa] = struct{}{}
	return true
}

// removeAssignment deletes the given ServerAssignment from the tenant entry.
// This returns true if the operation was successful, and false otherwise.
func (e *tenantEntry) removeAssignment(sa *ServerAssignment) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Assignment does not exists.
	if _, ok := e.mu.assignments[sa]; !ok {
		return false
	}
	delete(e.mu.assignments, sa)
	return true
}

// getConnsMap returns a snapshot of connections, indexed by server's address.
// Since this a snapshot, the mappings may be stale, if the connection was
// transferred to a different pod after the snapshot was made.
func (e *tenantEntry) getConnsMap() map[string][]ConnectionHandle {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Iterating 50K entries (the number of conns that we plan to support in
	// each proxy) would be a few ms (< 5ms). Since getConnsMap will only be
	// used during rebalancing (which isn't very frequent), holding onto the
	// lock while doing the below is fine.
	//
	// Note that holding onto the lock for a long time will affect latency of
	// incoming connections.
	conns := make(map[string][]ConnectionHandle)
	for sa := range e.mu.assignments {
		conns[sa.Addr()] = append(conns[sa.Addr()], sa.Owner())
	}
	return conns
}

// getConnsCount returns the number of connections associated with the tenant.
func (e *tenantEntry) getConnsCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()

	return len(e.mu.assignments)
}

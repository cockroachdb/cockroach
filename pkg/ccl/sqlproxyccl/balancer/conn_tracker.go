// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

// partitionsRefreshInterval refers to the interval that the assignment states are
// periodically refreshed.
const partitionsRefreshInterval = 15 * time.Second

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

	// verboseLogging indicates whether verbose logging is enabled for the
	// connection tracker. We store it once here to avoid the vmodule mutex
	// each time we call log.V.
	verboseLogging bool
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

	t := &ConnTracker{timeSource: timeSource, verboseLogging: log.V(2)}
	t.mu.tenants = make(map[roachpb.TenantID]*tenantEntry)

	if err := stopper.RunAsyncTask(
		ctx, "refresh-partitions", t.refreshPartitionsLoop,
	); err != nil {
		return nil, err
	}
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
		// Explicitly use a separate `if` block to avoid unintentional short
		// circuit bugs in the future. `a && b()` vs `b() && a` are not the same.
		if t.verboseLogging {
			logTrackerEvent("registerAssignment", sa)
		}
	}
}

// unregisterAssignment unregisters the server assignment under the given
// tenant. If the assignment cannot be found, this is a no-op.
func (t *ConnTracker) unregisterAssignment(tenantID roachpb.TenantID, sa *ServerAssignment) {
	e := t.getEntry(tenantID, false /* allowCreate */)
	if e != nil && e.removeAssignment(sa) {
		// Explicitly use a separate `if` block to avoid unintentional short
		// circuit bugs in the future. `a && b()` vs `b() && a` are not the same.
		if t.verboseLogging {
			logTrackerEvent("unregisterAssignment", sa)
		}
	}
}

// getTenantIDs returns a list of tenant IDs that have at least one connection
// registered with the connection tracker.
func (t *ConnTracker) getTenantIDs() []roachpb.TenantID {
	t.mu.Lock()
	defer t.mu.Unlock()

	tenants := make([]roachpb.TenantID, 0, len(t.mu.tenants))
	for tenantID, entry := range t.mu.tenants {
		if entry.assignmentsCount() > 0 {
			tenants = append(tenants, tenantID)
		}
	}
	return tenants
}

// listAssignments returns a snapshot of both the active and idle partitions
// that contain ServerAssignment instances for the given tenant.
func (t *ConnTracker) listAssignments(
	tenantID roachpb.TenantID,
) (activeList, idleList []*ServerAssignment) {
	e := t.getEntry(tenantID, false /* allowCreate */)
	if e == nil {
		return nil, nil
	}
	return e.listAssignments()
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

// refreshPartitionsLoop runs on a background goroutine to continuously refresh
// partitions of assignments in the tracker.
func (t *ConnTracker) refreshPartitionsLoop(ctx context.Context) {
	timer := t.timeSource.NewTimer()
	defer timer.Stop()
	for {
		timer.Reset(partitionsRefreshInterval)
		select {
		case <-ctx.Done():
			return
		case <-timer.Ch():
			timer.MarkRead()
			t.refreshPartitions()
		}
	}
}

// refreshPartitions refreshes the partitions of all assignments for all tenants
// associated with the tracker.
func (t *ConnTracker) refreshPartitions() {
	tenantIDs := t.getTenantIDs()
	for _, tenantID := range tenantIDs {
		e := t.getEntry(tenantID, false /* allowCreate */)
		if e != nil {
			e.refreshPartitions()
		}
	}
}

// tenantEntry is a tracker entry that stores server assignments for a single
// tenant. All methods on tenantEntry are thread-safe.
//
// There are two partitions for server assignments: active and idle. New
// assignments are placed in the active partition. A background goroutine in the
// tracker will attempt to move assignments around based on the state of the
// assignment's owner (i.e. through the owner's IsIdle method).
type tenantEntry struct {
	mu          syncutil.Mutex
	assignments struct {
		active map[*ServerAssignment]struct{}
		idle   map[*ServerAssignment]struct{}
	}
}

// newTenantEntry returns a new instance of tenantEntry.
func newTenantEntry() *tenantEntry {
	e := &tenantEntry{}
	e.assignments.active = make(map[*ServerAssignment]struct{})
	e.assignments.idle = make(map[*ServerAssignment]struct{})
	return e
}

// addAssignment adds the given ServerAssignment instance to the active
// partition. If the instance already exists in the tenant entry, this returns
// false.
func (e *tenantEntry) addAssignment(sa *ServerAssignment) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Assignment already exists.
	_, isActive := e.assignments.active[sa]
	_, isIdle := e.assignments.idle[sa]
	if isActive || isIdle {
		return false
	}

	e.assignments.active[sa] = struct{}{}
	return true
}

// removeAssignment deletes the given ServerAssignment instance from the tenant
// entry. If the instance is not present in the entry, this returns false.
func (e *tenantEntry) removeAssignment(sa *ServerAssignment) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Assignment does not exist.
	_, isActive := e.assignments.active[sa]
	_, isIdle := e.assignments.idle[sa]
	if !isActive && !isIdle {
		return false
	}

	if isActive {
		delete(e.assignments.active, sa)
	} else {
		delete(e.assignments.idle, sa)
	}
	return true
}

// listAssignments returns a snapshot of both the active and idle partitions
// that contain ServerAssignment instances.
func (e *tenantEntry) listAssignments() (activeList, idleList []*ServerAssignment) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Note that when returning assignment snapshots, it is important that
	// snapshots for all partitions are taken together while holding the lock
	// because assignments will be moved around. If we don't do this, an
	// assignment may end up in both of the returned slices, assuming that
	// listAssignments was called during a refresh.
	//
	// Iterating 50K entries (the number of connections that we plan to support
	// in each proxy) would be a few ms (< 5ms). Since listAssignments will only
	// be mainly used during rebalancing (which isn't very frequent), holding
	// onto the lock while doing the copy is fine.
	activeList = make([]*ServerAssignment, 0, len(e.assignments.active))
	for a := range e.assignments.active {
		activeList = append(activeList, a)
	}
	idleList = make([]*ServerAssignment, 0, len(e.assignments.idle))
	for a := range e.assignments.idle {
		idleList = append(idleList, a)
	}
	return
}

// getConnsMap returns a snapshot of connections, indexed by server's address.
// Since this a snapshot, the mappings may be stale, if the connection was
// transferred to a different pod after the snapshot was made.
//
// TODO(jaylim-crl): Replace usages of getConnsMap with listAssignments.
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
	for sa := range e.assignments.active {
		if sa.Owner() != nil {
			conns[sa.Addr()] = append(conns[sa.Addr()], sa.Owner())
		}
	}
	for sa := range e.assignments.idle {
		if sa.Owner() != nil {
			conns[sa.Addr()] = append(conns[sa.Addr()], sa.Owner())
		}
	}
	return conns
}

// assignmentsCount returns the number of assignments associated with the tenant.
func (e *tenantEntry) assignmentsCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.assignments.active) + len(e.assignments.idle)
}

// refreshPartitions updates the partitions of the server assignments associated
// with this tenant. Assignments with active owners will be moved to the active
// partition, and a similar argument can be made for the idle partition with
// idle owners.
func (e *tenantEntry) refreshPartitions() {
	// moveToActive moves server assignment instances from the idle->active, and
	// active->idle partitions. If toActive is true, the given server assignment
	// is moved from the idle partition to the active partition only if the
	// assignment is present in the idle partition. If the assignment is no
	// longer present in the existing partition, this will be a no-op. A similar
	// argument can be made for toActive=false.
	moveToActive := func(toActive bool, sa *ServerAssignment) {
		e.mu.Lock()
		defer e.mu.Unlock()

		// When moving an assignment to the active partition, the assignment
		// has to exist in the idle partition. A similar argument can be made
		// when moving to the idle partition. We check this to ensure that an
		// assignment doesn't get re-added back to the tracker whenever the
		// assignment has been closed.
		if _, isIdle := e.assignments.idle[sa]; toActive && isIdle {
			delete(e.assignments.idle, sa)
			e.assignments.active[sa] = struct{}{}
		}
		if _, isActive := e.assignments.active[sa]; !toActive && isActive {
			delete(e.assignments.active, sa)
			e.assignments.idle[sa] = struct{}{}
		}
	}

	activeList, idleList := e.listAssignments()
	for _, sa := range activeList {
		if sa.Owner() != nil && sa.Owner().IsIdle() {
			moveToActive(false, sa)
		}
	}
	for _, sa := range idleList {
		if sa.Owner() != nil && !sa.Owner().IsIdle() {
			moveToActive(true, sa)
		}
	}
}

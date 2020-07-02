// Copyright 2020 The Cockroach Authors.
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
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// A unique id for a particular descriptor version.
type descVersionID struct {
	id      sqlbase.ID
	version sqlbase.DescriptorVersion
}

// LeaseRemovalTracker can be used to wait for leases to be removed from the
// store (leases are removed from the store async w.r.t. Manager
// operations).
// To use it, its LeaseRemovedNotification method must be hooked up to
// StorageTestingKnobs.LeaseReleasedEvent. Then, every time you want to wait
// for a lease, get a tracker object through TrackRemoval() before calling
// Manager.Release(), and then call WaitForRemoval() on the tracker to
// block for the removal from the store.
//
// All methods are thread-safe.
type LeaseRemovalTracker struct {
	mu syncutil.Mutex
	// map from a lease whose release we're waiting for to a tracker for that
	// lease.
	tracking map[descVersionID]RemovalTracker
}

type RemovalTracker struct {
	removed chan struct{}
	// Pointer to a shared err. *err is written when removed is closed.
	err *error
}

// NewLeaseRemovalTracker creates a LeaseRemovalTracker.
func NewLeaseRemovalTracker() *LeaseRemovalTracker {
	return &LeaseRemovalTracker{
		tracking: make(map[descVersionID]RemovalTracker),
	}
}

// TrackRemoval starts monitoring lease removals for a particular lease.
// This should be called before triggering the operation that (asynchronously)
// removes the lease.
func (w *LeaseRemovalTracker) TrackRemoval(desc catalog.Descriptor) RemovalTracker {
	id := descVersionID{
		id:      desc.GetID(),
		version: desc.GetVersion(),
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if tracker, ok := w.tracking[id]; ok {
		return tracker
	}
	tracker := RemovalTracker{removed: make(chan struct{}), err: new(error)}
	w.tracking[id] = tracker
	return tracker
}

// WaitForRemoval blocks until the lease is removed from the store.
func (t RemovalTracker) WaitForRemoval() error {
	<-t.removed
	return *t.err
}

// LeaseRemovedNotification has to be called after a lease is removed from the
// store. This should be hooked up as a callback to
// StorageTestingKnobs.LeaseReleasedEvent.
func (w *LeaseRemovalTracker) LeaseRemovedNotification(
	id sqlbase.ID, version sqlbase.DescriptorVersion, err error,
) {
	w.mu.Lock()
	defer w.mu.Unlock()

	idx := descVersionID{
		id:      id,
		version: version,
	}

	if tracker, ok := w.tracking[idx]; ok {
		*tracker.err = err
		close(tracker.removed)
		delete(w.tracking, idx)
	}
}

func (m *Manager) ExpireLeases(clock *hlc.Clock) {
	past := clock.Now().GoTime().Add(-time.Millisecond)

	m.names.mu.Lock()
	for _, desc := range m.names.descriptors {
		desc.expiration = hlc.Timestamp{WallTime: past.UnixNano()}
	}
	m.names.mu.Unlock()
}

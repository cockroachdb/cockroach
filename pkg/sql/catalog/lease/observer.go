// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package lease

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// Observer is notified of new descriptor versions. Implementations must be
// idempotent: the same (id, version) may be delivered more than once, and
// versions may be delivered out of order on registration (see
// Manager.RegisterLeaseObserver, which replays the most recently broadcast
// version of every known descriptor to a freshly registered Observer).
type Observer interface {
	// OnNewVersion Invoked when a new version of a descriptor becomes available.
	OnNewVersion(ctx context.Context, descID descpb.ID, newVersion descpb.DescriptorVersion, modificationTime hlc.Timestamp)
}

// RegisterLeaseObserver registers an observer. An unregisterFn is returned
// to remove this observer. Note: Observers are buffered, pending events may
// still fire after unregistering.
//
// After registration, the observer is delivered a synthetic OnNewVersion
// for the latest known version of each descriptor. Without this, an
// observer registered after a broadcast permanently misses it, because
// maybeAddObserverEvent's per-descriptor dedup suppresses replays. See
// #169820 for the lease-hang this caused in CDC.
func (m *Manager) RegisterLeaseObserver(
	ctx context.Context, observer Observer,
) (unregisterFn func()) {
	type observedDescriptorVersion struct {
		id      descpb.ID
		version descpb.DescriptorVersion
		modTime hlc.Timestamp
	}
	// Collected under m.mu and replayed outside it: OnNewVersion may take
	// the observer's own locks, so we don't want to hold m.mu across it.
	var observedVersions []observedDescriptorVersion
	func() {
		// Writers will need mutual exclusion to avoid clobbering.
		m.mu.Lock()
		defer m.mu.Unlock()
		// We will always allocate a new array, so readers can keep
		// using existing pointers.
		oldObservers := m.mu.observers.Load()
		var newObservers []Observer
		if oldObservers != nil {
			newObservers = make([]Observer, len(*oldObservers), len(*oldObservers)+1)
			copy(newObservers, *oldObservers)
		}
		newObservers = append(newObservers, observer)
		m.mu.observers.Store(&newObservers)
		for id, t := range m.mu.descriptors {
			t.mu.Lock()
			nv := t.mu.notifiedVersion
			t.mu.Unlock()
			if nv.version > 0 {
				observedVersions = append(observedVersions, observedDescriptorVersion{
					id: id, version: nv.version, modTime: nv.modTime,
				})
			}
		}
	}()
	for _, ov := range observedVersions {
		if ctx.Err() != nil {
			break
		}
		observer.OnNewVersion(ctx, ov.id, ov.version, ov.modTime)
	}
	return func() {
		m.unregisterLeaseObserver(observer)
	}
}

// UnregisterLeaseObserver unregisters an observer.
func (m *Manager) unregisterLeaseObserver(observer Observer) {
	// Writers will need mutual exclusion to avoid clobbering.
	m.mu.Lock()
	defer m.mu.Unlock()
	// We will always allocate a new array, so readers can keep
	// using existing pointers.
	oldObservers := m.mu.observers.Load()
	if oldObservers == nil {
		panic("observers is nil")
	}
	newObservers := make([]Observer, 0, len(*oldObservers)-1)
	for _, o := range *oldObservers {
		if o != observer {
			newObservers = append(newObservers, o)
		}
	}
	m.mu.observers.Store(&newObservers)
}

// notifyObservers notifies all registered observers of a new descriptor version.
func (m *Manager) notifyObservers(
	ctx context.Context,
	id descpb.ID,
	version descpb.DescriptorVersion,
	modificationTime hlc.Timestamp,
) {
	// Writers will always allocate a new slice, so we can
	// safely just use an atomic to load the list.
	observers := m.mu.observers.Load()
	if observers == nil {
		return
	}
	for _, observer := range *observers {
		observer.OnNewVersion(ctx, id, version, modificationTime)
	}
}

// observerEvent used internally to emit events via the observerEvent channel.
type observerEvent struct {
	id               descpb.ID
	version          descpb.DescriptorVersion
	modificationTime hlc.Timestamp
}

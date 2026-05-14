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

// Observer is an interface that allows observers to be notified of new
// versions of descriptors.
type Observer interface {
	// OnNewVersion Invoked when a new version of a descriptor becomes available.
	OnNewVersion(ctx context.Context, descID descpb.ID, newVersion descpb.DescriptorVersion, modificationTime hlc.Timestamp)
}

// RegisterLeaseObserver registers an observer. An unregisterFn is returned
// to remove this observer. Note: Observers are buffered, pending events may
// still fire after unregistering.
//
// After registration, the observer is "caught up" with the latest version
// that has been notified for each known descriptor. This is necessary because
// the per-descriptor maxVersionNotified gate in maybeAddObserverEvent
// suppresses re-notification for versions already broadcast — so without
// this catch-up, a freshly registered observer can permanently miss versions
// that were published before it registered. Caused #169820.
func (m *Manager) RegisterLeaseObserver(observer Observer) (unregisterFn func()) {
	// Snapshot the descriptors map and per-descriptor maxVersionNotified
	// while we install the observer. We collect (id, version) pairs while
	// holding m.mu, then call OnNewVersion outside the lock to avoid lock
	// inversion with descriptorState.mu.
	type catchupEntry struct {
		id      descpb.ID
		version descpb.DescriptorVersion
	}
	var catchups []catchupEntry
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
		// Collect the catch-up list under m.mu so the descriptors map is
		// consistent. The per-descriptor lock (t.mu) is acquired briefly
		// inside the loop; this nested ordering is safe because t.mu is
		// only acquired here for a short read and never re-acquires m.mu.
		for id, t := range m.mu.descriptors {
			t.mu.Lock()
			v := t.mu.maxVersionNotified
			t.mu.Unlock()
			if v > 0 {
				catchups = append(catchups, catchupEntry{id: id, version: v})
			}
		}
	}()
	// Replay the latest known version of each descriptor to the new
	// observer. modificationTime is unset because we don't track the
	// original publication timestamp on descriptorState; observers that
	// rely on modificationTime should learn it via subsequent Acquire.
	ctx := context.Background()
	for _, c := range catchups {
		observer.OnNewVersion(ctx, c.id, c.version, hlc.Timestamp{})
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

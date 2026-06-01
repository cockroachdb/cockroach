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

// Observer is notified of new descriptor versions. Versions for a single
// descriptor are delivered in strictly increasing order. State that existed
// before registration is returned from Manager.RegisterLeaseObserver instead
// of being replayed through OnNewVersion.
type Observer interface {
	// OnNewVersion is invoked when a new version of a descriptor becomes
	// available.
	OnNewVersion(ctx context.Context, descID descpb.ID, newVersion descpb.DescriptorVersion, modificationTime hlc.Timestamp)
}

// RegisterLeaseObserver registers an observer and, for each descriptor in
// ids, returns the most recently notified version that the lease manager
// knows about. The snapshot is taken atomically with adding the observer to
// the broadcast list, so a freshly registered observer can detect that a
// lease it already holds is stale without relying on a re-broadcast that
// the per-descriptor dedup in maybeAddObserverEvent would otherwise
// suppress.
//
// The returned map only contains entries for descriptors that the lease
// manager has tracked and broadcast at least once; absent ids mean "no
// prior broadcast for this descriptor, you'll see future versions via
// OnNewVersion". Pass nil for ids to skip the snapshot entirely.
//
// An unregisterFn is returned to remove the observer. Note: events queued
// before unregister may still fire afterward.
func (m *Manager) RegisterLeaseObserver(
	observer Observer, ids []descpb.ID,
) (initialVersions map[descpb.ID]descpb.DescriptorVersion, unregisterFn func()) {
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
	if len(ids) > 0 {
		initialVersions = make(map[descpb.ID]descpb.DescriptorVersion, len(ids))
		for _, id := range ids {
			t, ok := m.mu.descriptors[id]
			if !ok {
				continue
			}
			t.mu.Lock()
			v := t.mu.maxVersionNotified
			t.mu.Unlock()
			if v > 0 {
				initialVersions[id] = v
			}
		}
	}
	return initialVersions, func() {
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

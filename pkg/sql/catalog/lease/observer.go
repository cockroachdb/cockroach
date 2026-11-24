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
func (m *Manager) RegisterLeaseObserver(observer Observer) (unregisterFn func()) {
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

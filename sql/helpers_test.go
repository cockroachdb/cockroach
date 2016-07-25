// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Andrei Matei(andreimatei1@gmail.com)

package sql

import "github.com/cockroachdb/cockroach/util/syncutil"

// LeaseRemovalTracker can be used to wait for leases to be removed from the
// store (leases are removed from the store async w.r.t. LeaseManager
// operations).
// To use it, its LeaseRemovedNotification method must be hooked up to
// LeaseStoreTestingKnobs.LeaseReleasedEvent. Then, every time you want to wait
// for a lease, get a tracker object through TrackRemoval() before calling
// LeaseManager.Release(), and then call WaitForRemoval() on the tracker to
// block for the removal from the store.
//
// All methods are thread-safe.
type LeaseRemovalTracker struct {
	mu syncutil.Mutex
	// map from a lease whose release we're waiting for to a tracker for that
	// lease.
	tracking map[*LeaseState]RemovalTracker
}

type RemovalTracker struct {
	removed chan struct{}
	// Pointer to a shared err. *err is written when removed is closed.
	err *error
}

// NewLeaseRemovalTracker creates a LeaseRemovalTracker.
func NewLeaseRemovalTracker() *LeaseRemovalTracker {
	return &LeaseRemovalTracker{
		tracking: make(map[*LeaseState]RemovalTracker),
	}
}

// TrackRemoval starts monitoring lease removals for a particular lease.
// This should be called before triggering the operation that (asynchronously)
// removes the lease.
func (w *LeaseRemovalTracker) TrackRemoval(lease *LeaseState) RemovalTracker {
	w.mu.Lock()
	defer w.mu.Unlock()
	if tracker, ok := w.tracking[lease]; ok {
		return tracker
	}
	tracker := RemovalTracker{removed: make(chan struct{}), err: new(error)}
	w.tracking[lease] = tracker
	return tracker
}

// WaitForRemoval blocks until the lease is removed from the store.
func (t RemovalTracker) WaitForRemoval() error {
	<-t.removed
	return *t.err
}

// LeaseRemovedNotification has to be called after a lease is removed from the
// store. This should be hooked up as a callback to
// LeaseStoreTestingKnobs.LeaseReleasedEvent.
func (w *LeaseRemovalTracker) LeaseRemovedNotification(
	lease *LeaseState, err error,
) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if tracker, ok := w.tracking[lease]; ok {
		*tracker.err = err
		close(tracker.removed)
		delete(w.tracking, lease)
	}
}

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

import "sync"

// LeaseRemovalTracker can be used to wait for leases to be removed from the
// store (leases are removed from the store async w.r.t. LeaseManager
// operations).
// To use it, its LeaseReleasedNotification method must be hooked up to
// LeaseStoreTestingKnobs.LeaseReleasedEvent. Then, every time you want to wait
// for a lease, get a tracker object through TrackRelease() before calling
// LeaseManager.Release(), and then call WaitForRelease() on the tracker to
// block for the release.
//
// All methods are thread-safe.
type LeaseRemovalTracker struct {
	mu sync.Mutex
	// map from a lease whose release we're waiting for to a tracker for that
	// lease.
	tracking map[*LeaseState]ReleaseTracker
}

type ReleaseTracker struct {
	released chan struct{}
	// *err is written when released is closed
	err *error
}

// NewLeaseRemovalTracker createa a LeaseRemovalTracker.
func NewLeaseRemovalTracker() *LeaseRemovalTracker {
	return &LeaseRemovalTracker{
		tracking: make(map[*LeaseState]ReleaseTracker),
	}
}

// TrackRelease starts monitoring lease removals for a particular lease.
// This should be called before triggering the operation that releases
// (asynchronously) the lease.
func (w *LeaseRemovalTracker) TrackRelease(lease *LeaseState) ReleaseTracker {
	w.mu.Lock()
	defer w.mu.Unlock()
	if tracker, ok := w.tracking[lease]; ok {
		return tracker
	}
	tracker := ReleaseTracker{released: make(chan struct{}), err: new(error)}
	w.tracking[lease] = tracker
	return tracker
}

// WaitForRelease blocks until the lease is removed from the store.
func (t *ReleaseTracker) WaitForRelease() error {
	<-t.released
	if t.err == nil {
		return nil
	}
	return *t.err
}

// LeaseReleasedNotification has to be called after a lease is removed from the
// store. This should be hooked up as a callback to
// LeaseStoreTestingKnobs.LeaseReleasedEvent.
func (w *LeaseRemovalTracker) LeaseReleasedNotification(
	lease *LeaseState, err error,
) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if tracker, ok := w.tracking[lease]; ok {
		*tracker.err = err
		close(tracker.released)
		delete(w.tracking, lease)
	}
}

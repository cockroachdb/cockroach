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

import (
	"fmt"
	"sync"
)

// LeaseReleaseWaiter can be used to wait for leases to be removed from the
// store (leases are removed from the store async w.r.t. LeaseManager
// operations).
// To use it, its LeaseReleasedNotification method must be hooked up to
// LeaseStoreTestingKnobs.LeaseReleasedEvent. Then, every time you want to wait
// for a lease, call PrepareToWait() before calling LeaseManager.Release(), and
// then call WaitForRelease() for the actual blocking.
//
// All methods are thread-safe.
type LeaseReleaseWaiter struct {
	mu sync.Mutex
	// map from a lease whose release we're waiting for to a channel on which the
	// result of the release will be sent.
	waitingFor map[*LeaseState]chan error
}

// NewLeaseReleaseWaiter createa a LeaseReleaseWaiter.
func NewLeaseReleaseWaiter() *LeaseReleaseWaiter {
	return &LeaseReleaseWaiter{
		waitingFor: make(map[*LeaseState]chan error),
	}
}

// PrepareToWait regisers a lease that WaitForRelease() will later be called
// for. This should be called before triggering the operation that releases
// (asynchronously) the lease.
func (w *LeaseReleaseWaiter) PrepareToWait(lease *LeaseState) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.waitingFor[lease]; ok {
		// Already prepared to wait for this lease.
		return
	}
	w.waitingFor[lease] = make(chan error, 1)
}

// WaitForRelease blocks until a lease previously registered with
// PrepareToWait() has been removed from the store.
// It's illegal to wait twice on the same lease.
func (w *LeaseReleaseWaiter) WaitForRelease(lease *LeaseState) error {
	w.mu.Lock()
	ch, ok := w.waitingFor[lease]
	if !ok {
		w.mu.Unlock()
		panic(fmt.Sprintf("wasn't prepared to wait for %s", lease))
	}
	w.mu.Unlock()
	return <-ch
}

// LeaseReleasedNotification has to be called after a lease is removed from the
// store. This should be hooked up as a callback to
// LeaseStoreTestingKnobs.LeaseReleasedEvent.
func (w *LeaseReleaseWaiter) LeaseReleasedNotification(
	lease *LeaseState, err error,
) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if ch, ok := w.waitingFor[lease]; ok {
		ch <- err
	}
	delete(w.waitingFor, lease)
}

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

// LeaseReleaseWaiter can be used to wait for a lease to be removed from the
// store (leases are removed from the store async w.r.t. LeaseManager
// operations).
// To use it, its LeaseReleasedNotification method must be hooked up to
// LeaseStoreTestingKnobs.LeaseReleasedEvent. Then, every time you want to wait
// for a lease, call PrepareToWait() before calling LeaseManager.Release(), and
// then call Wait() for the actual blocking.
type LeaseReleaseWaiter struct {
	waitingFor *LeaseState
	doneSem    chan error
}

// PrepareToWait regisers the lease that WaitForRelease() will later be called
// for. This should be called before triggering the operation that releases
// (asynchronously) the lease.
func (w *LeaseReleaseWaiter) PrepareToWait(lease *LeaseState) {
	w.waitingFor = lease
	w.doneSem = make(chan error, 1)
}

// WaitForRelease blockes until the lease previously registered with
// PrepareToWait() has been removed from the store.
func (w *LeaseReleaseWaiter) Wait() error {
	if w.waitingFor == nil {
		panic("wasn't told what to wait for")
	}
	return <-w.doneSem
}

// LeaseReleasedNotification has to be called after a lease is removed from the
// store. This should be hooked up as a callback to
// LeaseStoreTestingKnobs.LeaseReleasedEvent.
func (w *LeaseReleaseWaiter) LeaseReleasedNotification(
	lease *LeaseState, err error,
) {
	if lease == w.waitingFor {
		w.doneSem <- err
	}
}

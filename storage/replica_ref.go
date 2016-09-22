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

package storage

import (
	"fmt"
	"runtime/debug"
	"time"

	"github.com/cockroachdb/cockroach/roachpb"
)

// ReplicaRef is a container that manages references to a *Replica. Acquire()
// returns a *Replica and a closure that must be called when the *Replica is
// no longer in use. Similarly, AcquireExclusive() returns a *Replica with
// exclusive access. It's illegal to continue using the *Replica after calling
// the closure returned from the acquisition methods; however the values
// returned from the other methods can be held onto freely.
type ReplicaRef Replica

func (ref *ReplicaRef) String() string {
	return (*Replica)(ref).String()
}

func (ref *ReplicaRef) acquire(exclusive bool) (*Replica, func(), error) {
	if !exclusive {
		ref.refMu.RLock()
	} else {
		ref.refMu.Lock()
	}

	const catchLock = false

	var ch chan struct{}
	if catchLock {
		ch = make(chan struct{})
		stack := debug.Stack()
		go func() {
			select {
			case <-time.After(time.Second):
				fmt.Printf("LEAK %t %p\n%s\n", exclusive, ref, stack)
			case <-ch:
			}
		}()
	}

	release := func() {
		if catchLock {
			close(ch)
		}
		if !exclusive {
			ref.refMu.RUnlock()
		} else {
			ref.refMu.Unlock()
		}
	}

	if err, _ := ref.refMu.destroyed.Load().(error); err != nil {
		release()
		return nil, func() {}, err
	}
	return (*Replica)(ref), release, nil
}

// Acquire returns a reference to the underlying Replica. The returned function
// must be called after the reference isn't used any more, even on error.
//
// An error (and a nil reference) is returned when the underlying Replica has
// been destroyed.
func (ref *ReplicaRef) Acquire() (*Replica, func(), error) {
	return ref.acquire(false)
}

// AcquireExclusive has the same semantics as Acquire, but grants exclusive
// access to the underlying Replica.
func (ref *ReplicaRef) AcquireExclusive() (*Replica, func(), error) {
	return ref.acquire(true)
}

// Desc calls through to (*Replica).Desc().
func (ref *ReplicaRef) Desc() *roachpb.RangeDescriptor {
	return (*Replica)(ref).Desc()
}

// RefuseProposals cancels all pending client proposals and prevents new ones.
// It should only be called when GC of the underlying Replica is imminent.
func (ref *ReplicaRef) RefuseProposals() {
	// We want an exclusive reference to the Replica underlying ReplicaRef, but
	// the Replica may be in use (and in particular, may be in use by clients
	// stuck waiting on Raft). To circumvent deadlock, we first abort existing
	// proposals and disallow new ones (which means that all future references
	// to the Replica are going to be short-lived), and then acquire the
	// exclusive reference.
	destroyedErr := roachpb.NewRangeNotFoundError(ref.Desc().RangeID)
	rep := (*Replica)(ref)
	rep.mu.Lock()
	// Clear the pending command queue.
	for _, p := range rep.mu.pendingCmds {
		p.done <- roachpb.ResponseWithError{
			Reply: &roachpb.BatchResponse{},
			Err:   roachpb.NewError(destroyedErr),
		}
	}
	// Clear the map.
	rep.mu.pendingCmds = nil // no more proposals
	rep.mu.internalRaftGroup = nil
	rep.mu.Unlock()
}

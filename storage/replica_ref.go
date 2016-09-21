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

	"github.com/cockroachdb/cockroach/roachpb"
)

// ReplicaRef is a container that manages references to a *Replica object.
// The Acquire() method must be called to receive a *Replica and returns a
// closure that releases it. Similarly, AcquireExclusive() returns a *Replica
// with exclusive access. It's illegal to continue using the *Replica after
// calling the closure returned from the acquisition methods; however the
// values returned from the other methods can be held onto freely.
type ReplicaRef interface {
	fmt.Stringer
	Acquire() (*Replica, func(), error)
	AcquireExclusive() (*Replica, func(), error)
	Desc() *roachpb.RangeDescriptor
	isReplicaRef()
}

// replicaRef implements ReplicaRef (the value type of which is used to avoid
// heap allocations).
type replicaRef struct {
	replica *Replica
}

func (ref replicaRef) String() string {
	return ref.replica.String()
}

func (replicaRef) isReplicaRef() {}

func (ref replicaRef) acquire(exclusive bool) (*Replica, func(), error) {
	if !exclusive {
		ref.replica.refMu.RLock()
	} else {
		ref.replica.refMu.Lock()
	}

	release := func() {
		if !exclusive {
			ref.replica.refMu.RUnlock()
		} else {
			ref.replica.refMu.Unlock()
		}
	}

	if err := ref.replica.refMu.destroyed; err != nil {
		release()
		return nil, func() {}, err
	}
	return ref.replica, release, nil
}

// Acquire returns a reference to the underlying Replica. The returned function
// must be called after the reference isn't used any more, even on error.
//
// An error (and a nil reference) is returned when the underlying Replica has
// been destroyed.
func (ref replicaRef) Acquire() (*Replica, func(), error) {
	return ref.acquire(false)
}

// AcquireExclusive has the same semantics as Acquire, but grants exclusive
// access to the underlying Replica.
func (ref replicaRef) AcquireExclusive() (*Replica, func(), error) {
	return ref.acquire(true)
}

func (ref replicaRef) Desc() *roachpb.RangeDescriptor {
	return ref.replica.Desc()
}

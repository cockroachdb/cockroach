// Copyright 2014 The Cockroach Authors.
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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"fmt"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
)

// ReplicaRef .
type ReplicaRef interface {
	raft.Storage
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
		atomic.AddInt32(&ref.replica.refMu.count, -1)
		if !exclusive {
			ref.replica.refMu.RUnlock()
		} else {
			ref.replica.refMu.Unlock()
		}
	}

	atomic.AddInt32(&ref.replica.refMu.count, 1)
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

func (ref replicaRef) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	r, release, err := ref.Acquire()
	defer release()
	if err != nil {
		return raftpb.HardState{}, raftpb.ConfState{}, err
	}
	return r.storageInitialState()
}

func (ref replicaRef) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	r, release, err := ref.Acquire()
	defer release()
	if err != nil {
		return nil, err
	}
	return r.storageEntries(lo, hi, maxSize)
}

func (ref replicaRef) Term(i uint64) (uint64, error) {
	r, release, err := ref.Acquire()
	defer release()
	if err != nil {
		return 0, err
	}
	return r.storageTerm(i)
}

func (ref replicaRef) LastIndex() (uint64, error) {
	r, release, err := ref.Acquire()
	defer release()
	if err != nil {
		return 0, err
	}
	return r.storageLastIndex()
}

func (ref replicaRef) FirstIndex() (uint64, error) {
	r, release, err := ref.Acquire()
	defer release()
	if err != nil {
		return 0, err
	}
	return r.storageFirstIndex()
}

func (ref replicaRef) Snapshot() (raftpb.Snapshot, error) {
	r, release, err := ref.Acquire()
	defer release()
	if err != nil {
		return raftpb.Snapshot{}, err
	}
	return r.storageSnapshot()
}

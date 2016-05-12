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
// Author: Tamir Duberstein (tamird@gmail.com)

// This file includes test-only helper methods added to types in
// package storage. These methods are only linked in to tests in this
// directory (but may be used from tests in both package storage and
// package storage_test).

package storage

import (
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
)

// ForceReplicationScanAndProcess iterates over all ranges and
// enqueues any that need to be replicated. Exposed only for testing.
func (s *Store) ForceReplicationScanAndProcess() {
	s.mu.Lock()
	for _, r := range s.mu.replicas {
		s.replicateQueue.MaybeAdd(r, s.ctx.Clock.Now())
	}
	s.mu.Unlock()

	s.replicateQueue.DrainQueue(s.ctx.Clock)
}

// DisableReplicaGCQueue disables or enables the replica GC queue.
// Exposed only for testing.
func (s *Store) DisableReplicaGCQueue(disabled bool) {
	s.replicaGCQueue.SetDisabled(disabled)
}

// ForceReplicaGCScanAndProcess iterates over all ranges and enqueues any that
// may need to be GC'd. Exposed only for testing.
func (s *Store) ForceReplicaGCScanAndProcess() {
	s.mu.Lock()
	for _, r := range s.mu.replicas {
		s.replicaGCQueue.MaybeAdd(r, s.ctx.Clock.Now())
	}
	s.mu.Unlock()

	s.replicaGCQueue.DrainQueue(s.ctx.Clock)
}

// DisableRaftLogQueue disables or enables the raft log queue.
// Exposed only for testing.
func (s *Store) DisableRaftLogQueue(disabled bool) {
	s.raftLogQueue.SetDisabled(disabled)
}

// ForceRaftLogScanAndProcess iterates over all ranges and enqueues any that
// need their raft logs truncated and then process each of them.
// Exposed only for testing.
func (s *Store) ForceRaftLogScanAndProcess() {
	// Gather the list of replicas to call MaybeAdd on to avoid locking the
	// Mutex twice.
	s.mu.Lock()
	replicas := make([]*Replica, 0, len(s.mu.replicas))
	for _, r := range s.mu.replicas {
		replicas = append(replicas, r)
	}
	s.mu.Unlock()

	// Add each replica to the queue.
	for _, r := range replicas {
		s.raftLogQueue.MaybeAdd(r, s.ctx.Clock.Now())
	}

	s.raftLogQueue.DrainQueue(s.ctx.Clock)
}

// LogReplicaChangeTest adds a fake replica change event to the log for the
// range which contains the given key. This is intended for usage only in unit tests.
func (s *Store) LogReplicaChangeTest(txn *client.Txn, changeType roachpb.ReplicaChangeType, replica roachpb.ReplicaDescriptor, desc roachpb.RangeDescriptor) error {
	return s.logChange(txn, changeType, replica, desc)
}

// GetSnapshot wraps Snapshot() but does not require the replica lock
// to be held and it will block instead of returning
// ErrSnapshotTemporaryUnavailable.
func (r *Replica) GetSnapshot() (raftpb.Snapshot, error) {
	retryOptions := retry.Options{
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     50 * time.Millisecond,
		Multiplier:     2,
	}
	for retry := retry.Start(retryOptions); retry.Next(); {
		r.mu.Lock()
		snap, err := r.Snapshot()
		snapshotChan := r.mu.snapshotChan
		r.mu.Unlock()
		if err == raft.ErrSnapshotTemporarilyUnavailable {
			if snapshotChan == nil {
				// The call to Snapshot() didn't start an async process due to
				// rate limiting. Try again later.
				continue
			}
			var ok bool
			snap, ok = <-snapshotChan
			if ok {
				return snap, nil
			}
			// Each snapshot worker's output can only be consumed once.
			// We could be racing with raft itself, so if we get a closed
			// channel loop back and try again.
		} else {
			return snap, err
		}
	}
	panic("unreachable") // due to infinite retries
}

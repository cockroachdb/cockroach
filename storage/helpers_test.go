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
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/util/hlc"
)

// HandleRaftMessage delegates to handleRaftMessage.
func (s *Store) HandleRaftMessage(req *RaftMessageRequest) error {
	return s.handleRaftMessage(req)
}

// ComputeMVCCStats immediately computes correct total MVCC usage statistics
// for the store, returning the computed values (but without modifying the
// store).
func (s *Store) ComputeMVCCStats() (enginepb.MVCCStats, error) {
	var totalStats enginepb.MVCCStats
	var err error

	visitor := newStoreRangeSet(s)
	now := s.Clock().PhysicalNow()
	visitor.Visit(func(r *Replica) bool {
		var stats enginepb.MVCCStats
		stats, err = ComputeStatsForRange(r.Desc(), s.Engine(), now)
		if err != nil {
			return false
		}
		totalStats.Add(stats)
		return true
	})
	return totalStats, err
}

// ForceReplicationScanAndProcess iterates over all ranges and
// enqueues any that need to be replicated.
func (s *Store) ForceReplicationScanAndProcess() {
	s.mu.Lock()
	for _, r := range s.mu.replicas {
		s.replicateQueue.MaybeAdd(r, s.ctx.Clock.Now())
	}
	s.mu.Unlock()

	s.replicateQueue.DrainQueue(s.ctx.Clock)
}

// ForceReplicaGCScanAndProcess iterates over all ranges and enqueues any that
// may need to be GC'd.
func (s *Store) ForceReplicaGCScanAndProcess() {
	s.mu.Lock()
	for _, r := range s.mu.replicas {
		s.replicaGCQueue.MaybeAdd(r, s.ctx.Clock.Now())
	}
	s.mu.Unlock()

	s.replicaGCQueue.DrainQueue(s.ctx.Clock)
}

// ForceRaftLogScanAndProcess iterates over all ranges and enqueues any that
// need their raft logs truncated and then process each of them.
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
// range which contains the given key.
func (s *Store) LogReplicaChangeTest(txn *client.Txn, changeType roachpb.ReplicaChangeType, replica roachpb.ReplicaDescriptor, desc roachpb.RangeDescriptor) error {
	return s.logChange(txn, changeType, replica, desc)
}

// ReplicateQueuePurgatoryLength returns the number of replicas in replicate
// queue purgatory.
func (s *Store) ReplicateQueuePurgatoryLength() int {
	return s.replicateQueue.PurgatoryLength()
}

// SetRaftLogQueueActive enables or disables the raft log queue.
func (s *Store) SetRaftLogQueueActive(active bool) {
	s.setRaftLogQueueActive(active)
}

// SetReplicaGCQueueActive enables or disables the replica GC queue.
func (s *Store) SetReplicaGCQueueActive(active bool) {
	s.setReplicaGCQueueActive(active)
}

// SetSplitQueueActive enables or disables the split queue.
func (s *Store) SetSplitQueueActive(active bool) {
	s.setSplitQueueActive(active)
}

// SetReplicaScannerActive enables or disables the scanner. Note that while
// inactive, removals are still processed.
func (s *Store) SetReplicaScannerActive(active bool) {
	s.setScannerActive(active)
}

// GetLastIndex is the same function as LastIndex but it does not require
// that the replica lock is held.
func (r *Replica) GetLastIndex() (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.LastIndex()
}

// GetLease exposes replica.getLease for tests.
func (r *Replica) GetLease() (*roachpb.Lease, *roachpb.Lease) {
	return r.getLease()
}

// GetTimestampCacheLowWater returns the timestamp cache low water mark.
func (r *Replica) GetTimestampCacheLowWater() hlc.Timestamp {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.tsCache.lowWater
}

func (r *Replica) GetLastFromReplicaDesc() roachpb.ReplicaDescriptor {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.lastFromReplica
}

// GetDeadReplicas exports s.deadReplicas for tests.
func (s *Store) GetDeadReplicas() roachpb.StoreDeadReplicas {
	return s.deadReplicas()
}

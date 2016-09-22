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
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/util/hlc"
)

// ComputeMVCCStats immediately computes correct total MVCC usage statistics
// for the store, returning the computed values (but without modifying the
// store).
func (s *Store) ComputeMVCCStats() (enginepb.MVCCStats, error) {
	var totalStats enginepb.MVCCStats
	var err error

	visitor := newStoreRangeSet(s)
	now := s.Clock().PhysicalNow()
	visitor.Visit(func(ref *ReplicaRef) bool {
		r, release, err := ref.Acquire()
		defer release()
		if err != nil {
			return true // continue
		}

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

func forceScanAndProcess(s *Store, q baseQueue) {
	newStoreRangeSet(s).Visit(func(ref *ReplicaRef) bool {
		repl, release, err := ref.Acquire()
		defer release()
		if err != nil {
			return true
		}
		q.MaybeAdd(repl, s.ctx.Clock.Now())
		return true
	})

	q.DrainQueue(s.ctx.Clock)
}

// ForceReplicationScanAndProcess iterates over all ranges and
// enqueues any that need to be replicated.
func (s *Store) ForceReplicationScanAndProcess() {
	forceScanAndProcess(s, s.replicateQueue.baseQueue)
}

// ForceReplicaGCScanAndProcess iterates over all ranges and enqueues any that
// may need to be GC'd.
func (s *Store) ForceReplicaGCScanAndProcess() {
	forceScanAndProcess(s, s.replicaGCQueue.baseQueue)
}

// ForceRaftLogScanAndProcess iterates over all ranges and enqueues any that
// need their raft logs truncated and then process each of them.
func (s *Store) ForceRaftLogScanAndProcess() {
	forceScanAndProcess(s, s.raftLogQueue.baseQueue)
}

// GetDeadReplicas exports s.deadReplicas for tests.
func (s *Store) GetDeadReplicas() roachpb.StoreDeadReplicas {
	return s.deadReplicas()
}

// LeaseExpiration returns an int64 to increment a manual clock with to
// make sure that all active range leases expire.
func (s *Store) LeaseExpiration(clock *hlc.Clock) int64 {
	// Due to lease extensions, the remaining interval can be longer than just
	// the sum of the offset (=length of stasis period) and the active
	// duration, but definitely not by 2x.
	return 2 * int64(s.ctx.rangeLeaseActiveDuration+clock.MaxOffset())
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

// EnqueueRaftUpdateCheck enqueues the replica for a Raft update check, forcing
// the replica's Raft group into existence.
func (s *Store) EnqueueRaftUpdateCheck(rangeID roachpb.RangeID) {
	s.enqueueRaftUpdateCheck(rangeID)
}

// ManualReplicaGC processes the specified replica using the store's GC queue.
func (s *Store) ManualReplicaGC(ref *ReplicaRef) error {
	cfg, ok := s.Gossip().GetSystemConfig()
	if !ok {
		return fmt.Errorf("%s: system config not yet available", s)
	}
	return s.gcQueue.process(s.Ctx(), s.Clock().Now(), ref, cfg)
}

func (r *Replica) RaftLock() {
	r.raftMu.Lock()
}

func (r *Replica) RaftUnlock() {
	r.raftMu.Unlock()
}

// GetLastIndex is the same function as LastIndex but it does not require
// that the replica lock is held.
func (r *Replica) GetLastIndex() (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.LastIndex()
}

// GetLease exposes replica.getLease for tests.
// If you just need information about the lease holder, consider issuing a
// LeaseInfoRequest instead of using this internal method.
func (r *Replica) GetLease() (*roachpb.Lease, *roachpb.Lease) {
	return r.getLease()
}

// GetTimestampCacheLowWater returns the timestamp cache low water mark.
func (r *Replica) GetTimestampCacheLowWater() hlc.Timestamp {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.tsCache.lowWater
}

// IsQuiescent returns whether the replica is quiescent or not.
func (r *Replica) IsQuiescent() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.quiescent
}

func GetGCQueueTxnCleanupThreshold() time.Duration {
	return txnCleanupThreshold
}

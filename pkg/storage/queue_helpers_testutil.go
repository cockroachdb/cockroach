// Copyright 2019 The Cockroach Authors.
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
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// Code in this file is for testing usage only. It is exported only because it
// is called from outside of the package.

func (bq *baseQueue) testingAdd(
	ctx context.Context, repl replicaInQueue, priority float64,
) (bool, error) {
	return bq.addInternal(ctx, repl.Desc(), priority)
}

func forceScanAndProcess(s *Store, q *baseQueue) error {
	// Check that the system config is available. It is needed by many queues. If
	// it's not available, some queues silently fail to process any replicas,
	// which is undesirable for this method.
	if cfg := s.Gossip().GetSystemConfig(); cfg == nil {
		return errors.Errorf("system config not available in gossip")
	}

	newStoreReplicaVisitor(s).Visit(func(repl *Replica) bool {
		q.maybeAdd(context.Background(), repl, s.cfg.Clock.Now())
		return true
	})

	q.DrainQueue(s.stopper)
	return nil
}

func mustForceScanAndProcess(ctx context.Context, s *Store, q *baseQueue) {
	if err := forceScanAndProcess(s, q); err != nil {
		log.Fatal(ctx, err)
	}
}

// ForceReplicationScanAndProcess iterates over all ranges and
// enqueues any that need to be replicated.
func (s *Store) ForceReplicationScanAndProcess() error {
	return forceScanAndProcess(s, s.replicateQueue.baseQueue)
}

// MustForceReplicaGCScanAndProcess iterates over all ranges and enqueues any that
// may need to be GC'd.
func (s *Store) MustForceReplicaGCScanAndProcess() {
	mustForceScanAndProcess(context.TODO(), s, s.replicaGCQueue.baseQueue)
}

// MustForceMergeScanAndProcess iterates over all ranges and enqueues any that
// may need to be merged.
func (s *Store) MustForceMergeScanAndProcess() {
	mustForceScanAndProcess(context.TODO(), s, s.mergeQueue.baseQueue)
}

// ForceSplitScanAndProcess iterates over all ranges and enqueues any that
// may need to be split.
func (s *Store) ForceSplitScanAndProcess() error {
	return forceScanAndProcess(s, s.splitQueue.baseQueue)
}

// MustForceRaftLogScanAndProcess iterates over all ranges and enqueues any that
// need their raft logs truncated and then process each of them.
func (s *Store) MustForceRaftLogScanAndProcess() {
	mustForceScanAndProcess(context.TODO(), s, s.raftLogQueue.baseQueue)
}

// ForceTimeSeriesMaintenanceQueueProcess iterates over all ranges, enqueuing
// any that need time series maintenance, then processes the time series
// maintenance queue.
func (s *Store) ForceTimeSeriesMaintenanceQueueProcess() error {
	return forceScanAndProcess(s, s.tsMaintenanceQueue.baseQueue)
}

// ForceRaftSnapshotQueueProcess iterates over all ranges, enqueuing
// any that need raft snapshots, then processes the raft snapshot
// queue.
func (s *Store) ForceRaftSnapshotQueueProcess() error {
	return forceScanAndProcess(s, s.raftSnapshotQueue.baseQueue)
}

// ForceConsistencyQueueProcess runs all the ranges through the consistency
// queue.
func (s *Store) ForceConsistencyQueueProcess() error {
	return forceScanAndProcess(s, s.consistencyQueue.baseQueue)
}

// The methods below can be used to control a store's queues. Stopping a queue
// is only meant to happen in tests.

func (s *Store) setGCQueueActive(active bool) {
	s.gcQueue.SetDisabled(!active)
}
func (s *Store) setMergeQueueActive(active bool) {
	s.mergeQueue.SetDisabled(!active)
}
func (s *Store) setRaftLogQueueActive(active bool) {
	s.raftLogQueue.SetDisabled(!active)
}
func (s *Store) setReplicaGCQueueActive(active bool) {
	s.replicaGCQueue.SetDisabled(!active)
}

// SetReplicateQueueActive controls the replication queue. Only
// intended for tests.
func (s *Store) SetReplicateQueueActive(active bool) {
	s.replicateQueue.SetDisabled(!active)
}
func (s *Store) setSplitQueueActive(active bool) {
	s.splitQueue.SetDisabled(!active)
}
func (s *Store) setTimeSeriesMaintenanceQueueActive(active bool) {
	s.tsMaintenanceQueue.SetDisabled(!active)
}
func (s *Store) setRaftSnapshotQueueActive(active bool) {
	s.raftSnapshotQueue.SetDisabled(!active)
}
func (s *Store) setConsistencyQueueActive(active bool) {
	s.consistencyQueue.SetDisabled(!active)
}
func (s *Store) setScannerActive(active bool) {
	s.scanner.SetDisabled(!active)
}

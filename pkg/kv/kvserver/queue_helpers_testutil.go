// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvqueue"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// Code in this file is for testing usage only. It is exported only because it
// is called from outside of the package.

//func (bq *kvqueue.BaseQueue) testingAdd(
//	ctx context.Context, repl kvqueue.Replica, priority float64,
//) (bool, error) {
//	return bq.addInternal(ctx, repl.Desc(), repl.ReplicaID(), priority)
//}

func forceScanAndProcess(ctx context.Context, s *Store, q *kvqueue.BaseQueue) error {
	// Check that the system config is available. It is needed by many queues. If
	// it's not available, some queues silently fail to process any replicas,
	// which is undesirable for this method.
	if _, err := s.GetConfReader(ctx); err != nil {
		return errors.Wrap(err, "unable to retrieve conf reader")
	}

	newStoreReplicaVisitor(s).Visit(func(repl *Replica) bool {
		q.MaybeAdd(context.Background(), repl, s.cfg.Clock.NowAsClockTimestamp())
		return true
	})

	q.DrainQueue(s.stopper)
	return nil
}

func mustForceScanAndProcess(ctx context.Context, s *Store, q *kvqueue.BaseQueue) {
	if err := forceScanAndProcess(ctx, s, q); err != nil {
		log.Fatalf(ctx, "%v", err)
	}
}

// ForceReplicationScanAndProcess iterates over all ranges and
// enqueues any that need to be replicated.
func (s *Store) ForceReplicationScanAndProcess() error {
	return forceScanAndProcess(context.TODO(), s, s.replicateQueue.BaseQueue)
}

// MustForceReplicaGCScanAndProcess iterates over all ranges and enqueues any that
// may need to be GC'd.
func (s *Store) MustForceReplicaGCScanAndProcess() {
	mustForceScanAndProcess(context.TODO(), s, s.replicaGCQueue.BaseQueue)
}

// MustForceMergeScanAndProcess iterates over all ranges and enqueues any that
// may need to be merged.
func (s *Store) MustForceMergeScanAndProcess() {
	mustForceScanAndProcess(context.TODO(), s, s.mergeQueue.BaseQueue)
}

// ForceSplitScanAndProcess iterates over all ranges and enqueues any that
// may need to be split.
func (s *Store) ForceSplitScanAndProcess() error {
	return forceScanAndProcess(context.TODO(), s, s.splitQueue.BaseQueue)
}

// MustForceRaftLogScanAndProcess iterates over all ranges and enqueues any that
// need their raft logs truncated and then process each of them.
func (s *Store) MustForceRaftLogScanAndProcess() {
	mustForceScanAndProcess(context.TODO(), s, s.raftLogQueue.BaseQueue)
}

// ForceTimeSeriesMaintenanceQueueProcess iterates over all ranges, enqueuing
// any that need time series maintenance, then processes the time series
// maintenance queue.
func (s *Store) ForceTimeSeriesMaintenanceQueueProcess() error {
	return forceScanAndProcess(context.TODO(), s, s.tsMaintenanceQueue.BaseQueue)
}

// ForceRaftSnapshotQueueProcess iterates over all ranges, enqueuing
// any that need raft snapshots, then processes the raft snapshot
// queue.
func (s *Store) ForceRaftSnapshotQueueProcess() error {
	return forceScanAndProcess(context.TODO(), s, s.raftSnapshotQueue.BaseQueue)
}

// ForceConsistencyQueueProcess runs all the ranges through the consistency
// queue.
func (s *Store) ForceConsistencyQueueProcess() error {
	return forceScanAndProcess(context.TODO(), s, s.consistencyQueue.BaseQueue)
}

// The methods below can be used to control a store's queues. Stopping a queue
// is only meant to happen in tests.

func (s *Store) setGCQueueActive(active bool) {
	s.mvccGCQueue.SetDisabled(!active)
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

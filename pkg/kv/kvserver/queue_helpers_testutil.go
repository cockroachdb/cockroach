// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// Code in this file is for testing usage only. It is exported only because it
// is called from outside of the package.

func (bq *baseQueue) testingAdd(
	ctx context.Context, repl replicaInQueue, priority float64,
) (bool, error) {
	return bq.addInternal(ctx, repl.Desc(), repl.ReplicaID(), priority)
}

func forceScanAndProcess(ctx context.Context, s *Store, q *baseQueue) error {
	// Check that the system config is available. It is needed by many queues. If
	// it's not available, some queues silently fail to process any replicas,
	// which is undesirable for this method.
	if _, err := s.GetConfReader(ctx); err != nil {
		return errors.Wrap(err, "unable to retrieve conf reader")
	}

	newStoreReplicaVisitor(s).Visit(func(repl *Replica) bool {
		q.maybeAdd(context.Background(), repl, s.cfg.Clock.NowAsClockTimestamp())
		return true
	})

	q.DrainQueue(ctx, s.stopper)
	return nil
}

func mustForceScanAndProcess(ctx context.Context, s *Store, q *baseQueue) {
	if err := forceScanAndProcess(ctx, s, q); err != nil {
		log.Fatalf(ctx, "%v", err)
	}
}

// ForceReplicationScanAndProcess iterates over all ranges and
// enqueues any that need to be replicated.
func (s *Store) ForceReplicationScanAndProcess() error {
	return forceScanAndProcess(context.TODO(), s, s.replicateQueue.baseQueue)
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
	return forceScanAndProcess(context.TODO(), s, s.splitQueue.baseQueue)
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
	return forceScanAndProcess(context.TODO(), s, s.tsMaintenanceQueue.baseQueue)
}

// ForceRaftSnapshotQueueProcess iterates over all ranges, enqueuing
// any that need raft snapshots, then processes the raft snapshot
// queue.
func (s *Store) ForceRaftSnapshotQueueProcess() error {
	return forceScanAndProcess(context.TODO(), s, s.raftSnapshotQueue.baseQueue)
}

// ForceConsistencyQueueProcess runs all the ranges through the consistency
// queue.
func (s *Store) ForceConsistencyQueueProcess() error {
	return forceScanAndProcess(context.TODO(), s, s.consistencyQueue.baseQueue)
}

// ForceLeaseQueueScanAndProcess iterates over all ranges and
// enqueues any that need to have leases transfered.
func (s *Store) ForceLeaseQueueProcess() error {
	return forceScanAndProcess(context.TODO(), s, s.leaseQueue.baseQueue)
}

// The methods below can be used to control a store's queues. Stopping a queue
// is only meant to happen in tests.

func (s *Store) testingSetGCQueueActive(active bool) {
	s.mvccGCQueue.SetDisabled(!active)
}

// TestingSetLeaseQueueActive controls activating the lease queue. Only intended for
// tests.
func (s *Store) TestingSetLeaseQueueActive(active bool) {
	s.leaseQueue.SetDisabled(!active)
}
func (s *Store) testingSetMergeQueueActive(active bool) {
	s.mergeQueue.SetDisabled(!active)
}
func (s *Store) testingSetRaftLogQueueActive(active bool) {
	s.raftLogQueue.SetDisabled(!active)
}
func (s *Store) testingSetReplicaGCQueueActive(active bool) {
	s.replicaGCQueue.SetDisabled(!active)
}

// TestingSetReplicateQueueActive controls the replication queue. Only
// intended for tests.
func (s *Store) TestingSetReplicateQueueActive(active bool) {
	s.replicateQueue.SetDisabled(!active)
}
func (s *Store) TestingSetSplitQueueActive(active bool) {
	s.splitQueue.SetDisabled(!active)
}
func (s *Store) testingSetTimeSeriesMaintenanceQueueActive(active bool) {
	s.tsMaintenanceQueue.SetDisabled(!active)
}
func (s *Store) testingSetRaftSnapshotQueueActive(active bool) {
	s.raftSnapshotQueue.SetDisabled(!active)
}
func (s *Store) testingSetConsistencyQueueActive(active bool) {
	s.consistencyQueue.SetDisabled(!active)
}
func (s *Store) testingSetScannerActive(active bool) {
	s.scanner.SetDisabled(!active)
}

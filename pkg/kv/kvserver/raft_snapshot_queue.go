// Copyright 2015 The Cockroach Authors.
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
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/tracker"
)

const (
	// raftSnapshotQueueTimerDuration is the duration between Raft snapshot of
	// queued replicas.
	raftSnapshotQueueTimerDuration = 0 // zero duration to process Raft snapshots greedily

	raftSnapshotToLeaseholderPriority float64 = 3
	raftSnapshotToVoterPriority       float64 = 2
	raftSnapshotToLearnerPriority     float64 = 1
)

// raftSnapshotQueue manages a queue of replicas which may need to catch a
// replica up with a snapshot to their range.
type raftSnapshotQueue struct {
	*baseQueue
}

// newRaftSnapshotQueue returns a new instance of raftSnapshotQueue.
func newRaftSnapshotQueue(store *Store) *raftSnapshotQueue {
	rq := &raftSnapshotQueue{}
	rq.baseQueue = newBaseQueue(
		"raftsnapshot", rq, store,
		queueConfig{
			maxSize: defaultQueueMaxSize,
			// The Raft leader (which sends Raft snapshots) may not be the
			// leaseholder. Operating on a replica without holding the lease is the
			// reason Raft snapshots cannot be performed by the replicateQueue.
			needsLease:           false,
			needsSystemConfig:    false,
			acceptsUnsplitRanges: true,
			processTimeoutFunc:   makeRateLimitedTimeoutFunc(recoverySnapshotRate),
			successes:            store.metrics.RaftSnapshotQueueSuccesses,
			failures:             store.metrics.RaftSnapshotQueueFailures,
			pending:              store.metrics.RaftSnapshotQueuePending,
			processingNanos:      store.metrics.RaftSnapshotQueueProcessingNanos,
		},
	)
	return rq
}

type snapshoter interface {
	raftWithProgressIfLeader(withProgressVisitor)
	getLeaseRLocked() (cur, next roachpb.Lease)
}

// raftSnapshotQueuePriority returns the priority for the replica to send a
// snapshot. If followers do need snapshots, the priority is a function of the
// types of followers that need snapshots. Returns -1 if the replica is not the
// leader or if no snapshots are needed.
func raftSnapshotQueuePriority(repl snapshoter) float64 {
	priority := -1.0
	repl.raftWithProgressIfLeader(func(id uint64, _ raft.ProgressType, pr tracker.Progress) {
		if pr.State == tracker.StateSnapshot {
			var curPriority float64
			// NB: repl.mu.RLock()'d by raftWithProgress.
			lease, _ := repl.getLeaseRLocked()
			if roachpb.ReplicaID(id) == lease.Replica.ReplicaID {
				// If a leaseholder needs a snapshot, give it priority over all other
				// snapshots. The leaseholder may be so far behind on its log that it
				// does not even realize that it holds the lease. In such cases, the
				// range is unavailable for reads and writes until the leaseholder
				// receives its snapshot, so send one ASAP. We don't bother to check the
				// lease validity.
				curPriority = raftSnapshotToLeaseholderPriority
			} else if !pr.IsLearner {
				// If a voter needs a snapshot, give it priority over learners because a
				// voter in need of a snapshot can not vote for new proposals, so it may
				// be needed to achieve quorum on the range for write availability.
				curPriority = raftSnapshotToVoterPriority
			} else {
				curPriority = raftSnapshotToLearnerPriority
			}
			priority = math.Max(priority, curPriority)
		}
	})
	return priority
}

func (rq *raftSnapshotQueue) shouldQueue(
	ctx context.Context, _ hlc.ClockTimestamp, repl *Replica, _ spanconfig.StoreReader,
) (shouldQueue bool, priority float64) {
	priority = raftSnapshotQueuePriority(repl)
	if priority < 0 {
		return false, 0
	}
	if log.V(2) {
		log.Infof(ctx, "raft snapshot needed, enqueuing with priority %f", priority)
	}
	return true, priority
}

func (rq *raftSnapshotQueue) process(
	ctx context.Context, repl *Replica, _ spanconfig.StoreReader,
) (processed bool, err error) {
	// If a follower requires a Raft snapshot, perform it.
	if status := repl.RaftStatus(); status != nil {
		// raft.Status.Progress is only populated on the Raft group leader.
		for id, p := range status.Progress {
			if p.State == tracker.StateSnapshot {
				if log.V(1) {
					log.Infof(ctx, "sending raft snapshot")
				}
				if err := rq.processRaftSnapshot(ctx, repl, roachpb.ReplicaID(id)); err != nil {
					return false, err
				}
				processed = true
			}
		}
	}
	return processed, nil
}

func (rq *raftSnapshotQueue) processRaftSnapshot(
	ctx context.Context, repl *Replica, id roachpb.ReplicaID,
) error {
	desc := repl.Desc()
	repDesc, ok := desc.GetReplicaDescriptorByID(id)
	if !ok {
		return errors.Errorf("%s: replica %d not present in %v", repl, id, desc.Replicas())
	}
	snapType := kvserverpb.SnapshotRequest_VIA_SNAPSHOT_QUEUE

	if typ := repDesc.GetType(); typ == roachpb.LEARNER || typ == roachpb.NON_VOTER {
		if fn := repl.store.cfg.TestingKnobs.RaftSnapshotQueueSkipReplica; fn != nil && fn() {
			return nil
		}
		if repl.hasOutstandingSnapshotInFlightToStore(repDesc.StoreID) {
			// There is a snapshot being transferred. It's probably an INITIAL snap,
			// so bail for now and try again later.
			err := errors.Errorf(
				"skipping snapshot; replica is likely a %s in the process of being added: %s",
				typ,
				repDesc,
			)
			log.VEventf(ctx, 2, "%v", err)
			// TODO(dan): This is super brittle and non-obvious. In the common case,
			// this check avoids duplicate work, but in rare cases, we send the
			// learner snap at an index before the one raft wanted here. The raft
			// leader should be able to use logs to get the rest of the way, but it
			// doesn't try. In this case, skipping the raft snapshot would mean that
			// we have to wait for the next scanner cycle of the raft snapshot queue
			// to pick it up again. So, punt the responsibility back to raft by
			// telling it that the snapshot failed. If the learner snap ends up being
			// sufficient, this message will be ignored, but if we hit the case
			// described above, this will cause raft to keep asking for a snap and at
			// some point the snapshot lock above will be released and we'll fall
			// through to the logic below.
			repl.reportSnapshotStatus(ctx, repDesc.ReplicaID, err)
			return nil
		}
	}

	err := repl.sendSnapshot(ctx, repDesc, snapType, kvserverpb.SnapshotRequest_RECOVERY)

	// NB: if the snapshot fails because of an overlapping replica on the
	// recipient which is also waiting for a snapshot, the "smart" thing is to
	// send that other snapshot with higher priority. The problem is that the
	// leader for the overlapping range may not be this node. This happens
	// typically during splits and merges when overly aggressive log truncations
	// occur.
	//
	// For splits, the overlapping range will be a replica of the pre-split
	// range that needs a snapshot to catch it up across the split trigger.
	//
	// For merges, the overlapping replicas belong to ranges since subsumed by
	// this range. In particular, there can be many of them if merges apply in
	// rapid succession. The leftmost replica is the most important one to catch
	// up, as it will absorb all of the overlapping replicas when caught up past
	// all of the merges.
	//
	// We're currently not handling this and instead rely on the quota pool to
	// make sure that log truncations won't require snapshots for healthy
	// followers.
	return err
}

func (*raftSnapshotQueue) timer(_ time.Duration) time.Duration {
	return raftSnapshotQueueTimerDuration
}

func (rq *raftSnapshotQueue) purgatoryChan() <-chan time.Time {
	return nil
}

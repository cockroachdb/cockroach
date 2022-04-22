// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvraftsnapshotqueue

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvqueue"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/storemetrics"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/raft/v3/tracker"
)

const (
	// raftSnapshotQueueTimerDuration is the duration between Raft snapshot of
	// queued replicas.
	raftSnapshotQueueTimerDuration = 0 // zero duration to process Raft snapshots greedily

	RaftSnapshotPriority float64 = 0
)

// RaftSnapshotQueue manages a queue of replicas which may need to catch a
// replica up with a snapshot to their range.
type RaftSnapshotQueue struct {
	*kvqueue.BaseQueue
}

// NewRaftSnapshotQueue returns a new instance of RaftSnapshotQueue.
func NewRaftSnapshotQueue(
	store kvqueue.Store, metrics *storemetrics.StoreMetrics, knobs *kvqueue.TestingKnobs,
) *RaftSnapshotQueue {
	rq := &RaftSnapshotQueue{}
	rq.BaseQueue = kvqueue.NewBaseQueue(
		"raftsnapshot", rq, store,
		kvqueue.Config{
			MaxSize: kvqueue.DefaultQueueMaxSize,
			// The Raft leader (which sends Raft snapshots) may not be the
			// leaseholder. Operating on a replica without holding the lease is the
			// reason Raft snapshots cannot be performed by the replicateQueue.
			NeedsLease:           false,
			NeedsSystemConfig:    false,
			AcceptsUnsplitRanges: true,
			ProcessTimeoutFunc:   kvqueue.MakeRateLimitedTimeoutFunc(kvqueue.RecoverySnapshotRate),
			Successes:            metrics.RaftSnapshotQueueSuccesses,
			Failures:             metrics.RaftSnapshotQueueFailures,
			Pending:              metrics.RaftSnapshotQueuePending,
			ProcessingNanos:      metrics.RaftSnapshotQueueProcessingNanos,
		},
		knobs,
	)
	return rq
}

func (rq *RaftSnapshotQueue) ShouldQueue(
	ctx context.Context, now hlc.ClockTimestamp, repl kvqueue.Replica, _ spanconfig.StoreReader,
) (shouldQueue bool, priority float64) {
	// If a follower needs a snapshot, enqueue at the highest priority.
	if status := repl.RaftStatus(); status != nil {
		// raft.Status.Progress is only populated on the Raft group leader.
		for _, p := range status.Progress {
			if p.State == tracker.StateSnapshot {
				if log.V(2) {
					log.Infof(ctx, "raft snapshot needed, enqueuing")
				}
				return true, RaftSnapshotPriority
			}
		}
	}
	return false, 0
}

func (rq *RaftSnapshotQueue) Process(
	ctx context.Context, repl kvqueue.Replica, _ spanconfig.StoreReader,
) (processed bool, err error) {
	// If a follower requires a Raft snapshot, perform it.
	if status := repl.RaftStatus(); status != nil {
		// raft.Status.Progress is only populated on the Raft group leader.
		for id, p := range status.Progress {
			if p.State == tracker.StateSnapshot {
				if log.V(1) {
					log.Infof(ctx, "sending raft snapshot")
				}
				if err := rq.ProcessRaftSnapshot(ctx, repl, roachpb.ReplicaID(id)); err != nil {
					return false, err
				}
				processed = true
			}
		}
	}
	return processed, nil
}

func (rq *RaftSnapshotQueue) ProcessRaftSnapshot(
	ctx context.Context, repl kvqueue.Replica, id roachpb.ReplicaID,
) error {
	desc := repl.Desc()
	repDesc, ok := desc.GetReplicaDescriptorByID(id)
	if !ok {
		return errors.Errorf("%s: replica %d not present in %v", repl, id, desc.Replicas())
	}
	snapType := kvserverpb.SnapshotRequest_VIA_SNAPSHOT_QUEUE
	skipSnapLogLimiter := log.Every(10 * time.Second)

	if typ := repDesc.GetType(); typ == roachpb.LEARNER || typ == roachpb.NON_VOTER {
		if fn := rq.Knobs.RaftSnapshotQueueSkipReplica; fn != nil && fn() {
			return nil
		}
		if index := repl.GetAndGCSnapshotLogTruncationConstraints(
			timeutil.Now(), repDesc.StoreID,
		); index > 0 {
			// There is a snapshot being transferred. It's probably an INITIAL snap,
			// so bail for now and try again later.
			err := errors.Errorf(
				"skipping snapshot; replica is likely a %s in the process of being added: %s",
				typ,
				repDesc,
			)
			if skipSnapLogLimiter.ShouldLog() {
				log.Infof(ctx, "%v", err)
			} else {
				log.VEventf(ctx, 3, "%v", err)
			}
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
			repl.ReportSnapshotStatus(ctx, repDesc.ReplicaID, err)
			return nil
		}
	}

	err := repl.SendSnapshot(ctx, repDesc, snapType, kvserverpb.SnapshotRequest_RECOVERY)

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

func (*RaftSnapshotQueue) Timer(_ time.Duration) time.Duration {
	return raftSnapshotQueueTimerDuration
}

func (rq *RaftSnapshotQueue) PurgatoryChan() <-chan time.Time {
	return nil
}

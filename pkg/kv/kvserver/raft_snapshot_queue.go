// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

const (
	// raftSnapshotQueueTimerDuration is the duration between Raft snapshot of
	// queued replicas.
	raftSnapshotQueueTimerDuration = 0 // zero duration to process Raft snapshots greedily

	raftSnapshotPriority float64 = 0
)

// raftSnapshotQueue manages a queue of replicas which may need to catch a
// replica up with a snapshot to their range.
type raftSnapshotQueue struct {
	*baseQueue
}

var _ queueImpl = &raftSnapshotQueue{}

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
			needsSpanConfigs:     false,
			acceptsUnsplitRanges: true,
			processTimeoutFunc:   makeRateLimitedTimeoutFunc(rebalanceSnapshotRate),
			successes:            store.metrics.RaftSnapshotQueueSuccesses,
			failures:             store.metrics.RaftSnapshotQueueFailures,
			pending:              store.metrics.RaftSnapshotQueuePending,
			processingNanos:      store.metrics.RaftSnapshotQueueProcessingNanos,
			disabledConfig:       kvserverbase.RaftSnapshotQueueEnabled,
		},
	)
	return rq
}

func (rq *raftSnapshotQueue) shouldQueue(
	ctx context.Context, now hlc.ClockTimestamp, repl *Replica, _ spanconfig.StoreReader,
) (shouldQueue bool, priority float64) {
	// If a follower needs a snapshot, enqueue at the highest priority.
	if status := repl.RaftStatus(); status != nil {
		// raft.Status.Progress is only populated on the Raft group leader.
		for _, p := range status.Progress {
			if p.State == tracker.StateSnapshot {
				if log.V(2) {
					log.Infof(ctx, "raft snapshot needed, enqueuing")
				}
				return true, raftSnapshotPriority
			}
		}
	}
	return false, 0
}

func (rq *raftSnapshotQueue) process(
	ctx context.Context, repl *Replica, _ spanconfig.StoreReader,
) (anyProcessed bool, _ error) {
	// If a follower requires a Raft snapshot, perform it.
	if status := repl.RaftStatus(); status != nil {
		// raft.Status.Progress is only populated on the Raft group leader.
		for id, p := range status.Progress {
			if p.State == tracker.StateSnapshot {
				if log.V(1) {
					log.Infof(ctx, "sending raft snapshot")
				}
				if processed, err := rq.processRaftSnapshot(ctx, repl, roachpb.ReplicaID(id)); err != nil {
					return false, err
				} else if processed {
					anyProcessed = true
				}
			}
		}
	}
	return anyProcessed, nil
}

func (rq *raftSnapshotQueue) processRaftSnapshot(
	ctx context.Context, repl *Replica, id roachpb.ReplicaID,
) (processed bool, _ error) {
	desc := repl.Desc()
	repDesc, ok := desc.GetReplicaDescriptorByID(id)
	if !ok {
		return false, errors.Errorf("%s: replica %d not present in %v", repl, id, desc.Replicas())
	}

	if typ := repDesc.Type; typ == roachpb.LEARNER || typ == roachpb.NON_VOTER {
		if fn := repl.store.cfg.TestingKnobs.RaftSnapshotQueueSkipReplica; fn != nil && fn() {
			return false, nil
		}
		// NB: we could pass `false` for initialOnly as well, but we are the "other"
		// possible sender.
		if _, ok := repl.hasOutstandingSnapshotInFlightToStore(repDesc.StoreID, true /* initialOnly */); ok {
			// There is an INITIAL snapshot being transferred, so bail for now and try again later.
			err := errors.Errorf(
				"skipping snapshot; replica is likely a %s in the process of being added: %s",
				typ,
				repDesc,
			)
			log.VEventf(ctx, 2, "%v", err)
			return false, nil
		}
	}

	err := repl.sendSnapshotUsingDelegate(ctx, repDesc, kvserverpb.SnapshotRequest_RAFT_SNAPSHOT_QUEUE, raftSnapshotPriority)

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
	return err == nil /* processed */, err
}

func (*raftSnapshotQueue) postProcessScheduled(
	ctx context.Context, replica replicaInQueue, priority float64,
) {
}

func (*raftSnapshotQueue) timer(_ time.Duration) time.Duration {
	return raftSnapshotQueueTimerDuration
}

func (rq *raftSnapshotQueue) purgatoryChan() <-chan time.Time {
	return nil
}

func (rq *raftSnapshotQueue) updateChan() <-chan time.Time {
	return nil
}

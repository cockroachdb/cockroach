// Copyright 2015 The Cockroach Authors.
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
	"time"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
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

// newRaftSnapshotQueue returns a new instance of raftSnapshotQueue.
func newRaftSnapshotQueue(store *Store, g *gossip.Gossip) *raftSnapshotQueue {
	rq := &raftSnapshotQueue{}
	rq.baseQueue = newBaseQueue(
		"raftsnapshot", rq, store, g,
		queueConfig{
			maxSize: defaultQueueMaxSize,
			// The Raft leader (which sends Raft snapshots) may not be the
			// leaseholder. Operating on a replica without holding the lease is the
			// reason Raft snapshots cannot be performed by the replicateQueue.
			needsLease:           false,
			needsSystemConfig:    false,
			acceptsUnsplitRanges: true,
			successes:            store.metrics.RaftSnapshotQueueSuccesses,
			failures:             store.metrics.RaftSnapshotQueueFailures,
			pending:              store.metrics.RaftSnapshotQueuePending,
			processingNanos:      store.metrics.RaftSnapshotQueueProcessingNanos,
		},
	)
	return rq
}

func (rq *raftSnapshotQueue) shouldQueue(
	ctx context.Context, now hlc.Timestamp, repl *Replica, _ *config.SystemConfig,
) (shouldQ bool, priority float64) {
	// If a follower needs a snapshot, enqueue at the highest priority.
	if status := repl.RaftStatus(); status != nil {
		// raft.Status.Progress is only populated on the Raft group leader.
		for _, p := range status.Progress {
			if p.State == raft.ProgressStateSnapshot {
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
	ctx context.Context, repl *Replica, _ *config.SystemConfig,
) error {
	// If a follower requires a Raft snapshot, perform it.
	if status := repl.RaftStatus(); status != nil {
		// raft.Status.Progress is only populated on the Raft group leader.
		for id, p := range status.Progress {
			if p.State == raft.ProgressStateSnapshot {
				if log.V(1) {
					log.Infof(ctx, "sending raft snapshot")
				}
				if err := rq.processRaftSnapshot(ctx, repl, roachpb.ReplicaID(id)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (rq *raftSnapshotQueue) processRaftSnapshot(
	ctx context.Context, repl *Replica, id roachpb.ReplicaID,
) error {
	desc := repl.Desc()
	repDesc, ok := desc.GetReplicaDescriptorByID(id)
	if !ok {
		return errors.Errorf("%s: replica %d not present in %v", repl, id, desc.Replicas())
	}
	err := repl.sendSnapshot(ctx, repDesc, snapTypeRaft, SnapshotRequest_RECOVERY)

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

	// Report the snapshot status to Raft, which expects us to do this once
	// we finish sending the snapshot.
	repl.reportSnapshotStatus(ctx, id, err)
	return err
}

func (*raftSnapshotQueue) timer(_ time.Duration) time.Duration {
	return raftSnapshotQueueTimerDuration
}

func (rq *raftSnapshotQueue) purgatoryChan() <-chan time.Time {
	return nil
}

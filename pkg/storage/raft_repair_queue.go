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
//
// Author: Ben Darnell

package storage

import (
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

const (
	// raftRepairQueueMaxSize is the max size of the Raft repair queue.
	raftRepairQueueMaxSize = 100

	// raftRepairQueueTimerDuration is the duration between Raft repair of queued
	// replicas.
	raftRepairQueueTimerDuration = 0 // zero duration to process replication greedily

	raftSnapshotPriority float64 = 10000
)

// raftRepairQueue manages a queue of replicas which may need to add an
// additional replica to their range.
type raftRepairQueue struct {
	*baseQueue
	clock *hlc.Clock
}

// newRaftRepairQueue returns a new instance of raftRepairQueue.
func newRaftRepairQueue(store *Store, g *gossip.Gossip, clock *hlc.Clock) *raftRepairQueue {
	rq := &raftRepairQueue{
		clock: clock,
	}
	rq.baseQueue = newBaseQueue(
		"raftrepair", rq, store, g,
		queueConfig{
			maxSize: raftRepairQueueMaxSize,
			// The Raft leader (which sends Raft snapshots) may not be the
			// leaseholder. Operating on a replica without holding the lease is the
			// reason Raft repair cannot be performed by the replicateQueue.
			needsLease:           false,
			acceptsUnsplitRanges: true,
			successes:            store.metrics.RaftRepairQueueSuccesses,
			failures:             store.metrics.RaftRepairQueueFailures,
			pending:              store.metrics.RaftRepairQueuePending,
			processingNanos:      store.metrics.RaftRepairQueueProcessingNanos,
		},
	)
	return rq
}

func (rq *raftRepairQueue) shouldQueue(
	ctx context.Context, now hlc.Timestamp, repl *Replica, sysCfg config.SystemConfig,
) (shouldQ bool, priority float64) {
	// If a follower needs a snapshot, enqueue at the highest priority.
	if status := repl.RaftStatus(); status != nil {
		for _, p := range status.Progress {
			if p.State == raft.ProgressStateSnapshot {
				if log.V(2) {
					log.Infof(ctx, "%s raft snapshot needed, enqueuing", repl)
				}
				return true, raftSnapshotPriority
			}
		}
	}
	return false, 0
}

func (rq *raftRepairQueue) process(
	ctx context.Context, repl *Replica, sysCfg config.SystemConfig,
) error {
	// If a follower requires a Raft snapshot, perform it.
	if status := repl.RaftStatus(); status != nil {
		for id, p := range status.Progress {
			if p.State == raft.ProgressStateSnapshot {
				log.VEventf(ctx, 1, "sending raft snapshot")
				if err := rq.processRaftSnapshot(ctx, repl, roachpb.ReplicaID(id)); err != nil {
					return err
				}
				// Enqueue this replica again to see if there are more repairs to be
				// made.
				rq.MaybeAdd(repl, rq.clock.Now())
				return nil
			}
		}
	}
	return nil
}

func (rq *raftRepairQueue) processRaftSnapshot(
	ctx context.Context, repl *Replica, id roachpb.ReplicaID,
) error {
	desc := repl.Desc()
	repDesc, ok := desc.GetReplicaDescriptorByID(id)
	if !ok {
		return errors.Errorf("%s: replica %d not present in %v", repl, id, desc.Replicas)
	}
	err := repl.sendSnapshot(ctx, repDesc, snapTypeRaft)
	// Report the snapshot status to Raft, which expects us to do this once
	// we finish sending the snapshot.
	repl.reportSnapshotStatus(uint64(id), err)
	return err
}

func (*raftRepairQueue) timer(_ time.Duration) time.Duration {
	return raftRepairQueueTimerDuration
}

func (rq *raftRepairQueue) purgatoryChan() <-chan struct{} {
	return nil
}

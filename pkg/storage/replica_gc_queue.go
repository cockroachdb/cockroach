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

	"github.com/coreos/etcd/raft"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

const (
	// replicaGCQueueTimerDuration is the duration between GCs of queued replicas.
	replicaGCQueueTimerDuration = 50 * time.Millisecond

	// ReplicaGCQueueInactivityThreshold is the inactivity duration after which
	// a range will be considered for garbage collection. Exported for testing.
	ReplicaGCQueueInactivityThreshold = 10 * 24 * time.Hour // 10 days
	// ReplicaGCQueueCandidateTimeout is the duration after which a range in
	// candidate Raft state (which is a typical sign of having been removed
	// from the group) will be considered for garbage collection.
	ReplicaGCQueueCandidateTimeout = 1 * time.Second
)

// Priorities for the replica GC queue.
const (
	replicaGCPriorityDefault = 0.0

	// Replicas that have been removed from the range spend a lot of
	// time in the candidate state, so treat them as higher priority.
	replicaGCPriorityCandidate = 1.0

	// The highest priority is used when we have definite evidence
	// (external to replicaGCQueue) that the replica has been removed.
	replicaGCPriorityRemoved = 2.0
)

var (
	metaReplicaGCQueueRemoveReplicaCount = metric.Metadata{
		Name: "queue.replicagc.removereplica",
		Help: "Number of replica removals attempted by the replica gc queue"}
)

// ReplicaGCQueueMetrics is the set of metrics for the replica GC queue.
type ReplicaGCQueueMetrics struct {
	RemoveReplicaCount *metric.Counter
}

func makeReplicaGCQueueMetrics() ReplicaGCQueueMetrics {
	return ReplicaGCQueueMetrics{
		RemoveReplicaCount: metric.NewCounter(metaReplicaGCQueueRemoveReplicaCount),
	}
}

// replicaGCQueue manages a queue of replicas to be considered for garbage
// collections. The GC process asynchronously removes local data for
// ranges that have been rebalanced away from this store.
type replicaGCQueue struct {
	*baseQueue
	metrics ReplicaGCQueueMetrics
	db      *client.DB
}

// newReplicaGCQueue returns a new instance of replicaGCQueue.
func newReplicaGCQueue(store *Store, db *client.DB, gossip *gossip.Gossip) *replicaGCQueue {
	rgcq := &replicaGCQueue{
		metrics: makeReplicaGCQueueMetrics(),
		db:      db,
	}
	store.metrics.registry.AddMetricStruct(&rgcq.metrics)
	rgcq.baseQueue = newBaseQueue(
		"replicaGC", rgcq, store, gossip,
		queueConfig{
			maxSize:                  defaultQueueMaxSize,
			needsLease:               false,
			needsSystemConfig:        false,
			acceptsUnsplitRanges:     true,
			processDestroyedReplicas: true,
			successes:                store.metrics.ReplicaGCQueueSuccesses,
			failures:                 store.metrics.ReplicaGCQueueFailures,
			pending:                  store.metrics.ReplicaGCQueuePending,
			processingNanos:          store.metrics.ReplicaGCQueueProcessingNanos,
		},
	)
	return rgcq
}

// shouldQueue determines whether a replica should be queued for GC,
// and if so at what priority. To be considered for possible GC, a
// replica's range lease must not have been active for longer than
// ReplicaGCQueueInactivityThreshold. Further, the last replica GC
// check must have occurred more than ReplicaGCQueueInactivityThreshold
// in the past.
func (rgcq *replicaGCQueue) shouldQueue(
	ctx context.Context, now hlc.Timestamp, repl *Replica, _ config.SystemConfig,
) (bool, float64) {
	lastCheck, err := repl.GetLastReplicaGCTimestamp(ctx)
	if err != nil {
		log.Errorf(ctx, "could not read last replica GC timestamp: %s", err)
		return false, 0
	}

	if _, currentMember := repl.Desc().GetReplicaDescriptor(repl.store.StoreID()); !currentMember {
		return true, replicaGCPriorityRemoved
	}

	lastActivity := hlc.Timestamp{
		WallTime: repl.store.startedAt,
	}

	if lease, _ := repl.GetLease(); lease.ProposedTS != nil {
		lastActivity.Forward(*lease.ProposedTS)
	}

	var isCandidate bool
	if raftStatus := repl.RaftStatus(); raftStatus != nil {
		isCandidate = (raftStatus.SoftState.RaftState == raft.StateCandidate ||
			raftStatus.SoftState.RaftState == raft.StatePreCandidate)
	} else {
		// If a replica doesn't have an active raft group, we should check whether
		// we're decommissioning. If so, we should process the replica because it
		// has probably already been removed from its raft group but doesn't know it.
		// Without this, node decommissioning can stall on such dormant ranges.
		// Make sure NodeLiveness isn't nil because it can be in tests/benchmarks.
		if repl.store.cfg.NodeLiveness != nil {
			if liveness, _ := repl.store.cfg.NodeLiveness.Self(); liveness != nil && liveness.Decommissioning {
				return true, replicaGCPriorityDefault
			}
		}
	}
	return replicaGCShouldQueueImpl(now, lastCheck, lastActivity, isCandidate)
}

func replicaGCShouldQueueImpl(
	now, lastCheck, lastActivity hlc.Timestamp, isCandidate bool,
) (bool, float64) {
	timeout := ReplicaGCQueueInactivityThreshold
	priority := replicaGCPriorityDefault

	if isCandidate {
		// If the range is a candidate (which happens if its former replica set
		// ignores it), let it expire much earlier.
		timeout = ReplicaGCQueueCandidateTimeout
		priority = replicaGCPriorityCandidate
	} else if now.Less(lastCheck.Add(ReplicaGCQueueInactivityThreshold.Nanoseconds(), 0)) {
		// Return false immediately if the previous check was less than the
		// check interval in the past. Note that we don't do this if the
		// replica is in candidate state, in which case we want to be more
		// aggressive - a failed rebalance attempt could have checked this
		// range, and candidate state suggests that a retry succeeded. See
		// #7489.
		return false, 0
	}

	shouldQ := lastActivity.Add(timeout.Nanoseconds(), 0).Less(now)

	if !shouldQ {
		return false, 0
	}

	return shouldQ, priority
}

// process performs a consistent lookup on the range descriptor to see if we are
// still a member of the range.
func (rgcq *replicaGCQueue) process(
	ctx context.Context, repl *Replica, _ config.SystemConfig,
) error {
	// Note that the Replicas field of desc is probably out of date, so
	// we should only use `desc` for its static fields like RangeID and
	// StartKey (and avoid rng.GetReplica() for the same reason).
	desc := repl.Desc()

	// Calls to RangeLookup typically use inconsistent reads, but we
	// want to do a consistent read here. This is important when we are
	// considering one of the metadata ranges: we must not do an inconsistent
	// lookup in our own copy of the range.
	rs, _, err := client.RangeLookupForVersion(ctx, rgcq.store.ClusterSettings(), rgcq.db.GetSender(),
		desc.StartKey.AsRawKey(), roachpb.CONSISTENT, 0 /* prefetchNum */, false /* reverse */)
	if err != nil {
		return err
	}
	if len(rs) != 1 {
		return errors.Errorf("expected 1 range descriptor, got %d", len(rs))
	}
	replyDesc := rs[0]

	if currentDesc, currentMember := replyDesc.GetReplicaDescriptor(repl.store.StoreID()); !currentMember {
		// We are no longer a member of this range; clean up our local data.
		rgcq.metrics.RemoveReplicaCount.Inc(1)
		if log.V(1) {
			log.Infof(ctx, "destroying local data")
		}
		if err := repl.store.RemoveReplica(ctx, repl, replyDesc, true); err != nil {
			return err
		}
	} else if desc.RangeID != replyDesc.RangeID {
		// If we get a different range ID back, then the range has been merged
		// away. But currentMember is true, so we are still a member of the
		// subsuming range. Shut down raft processing for the former range
		// and delete any remaining metadata, but do not delete the data.
		rgcq.metrics.RemoveReplicaCount.Inc(1)
		if log.V(1) {
			log.Infof(ctx, "removing merged range")
		}
		if err := repl.store.RemoveReplica(ctx, repl, replyDesc, false); err != nil {
			return err
		}

		// TODO(bdarnell): remove raft logs and other metadata (while leaving a
		// tombstone). Add tests for GC of merged ranges.
	} else {
		// This replica is a current member of the raft group. Set the last replica
		// GC check time to avoid re-processing for another check interval.
		//
		// TODO(tschottdorf): should keep stats in particular on this outcome
		// but also on how good a job the queue does at inspecting every
		// Replica (see #8111) when inactive ones can be starved by
		// event-driven additions.
		if log.V(1) {
			log.Infof(ctx, "not gc'able, replica is still in range descriptor: %v", currentDesc)
		}
		if err := repl.setLastReplicaGCTimestamp(ctx, repl.store.Clock().Now()); err != nil {
			return err
		}
	}

	return nil
}

func (*replicaGCQueue) timer(_ time.Duration) time.Duration {
	return replicaGCQueueTimerDuration
}

// purgatoryChan returns nil.
func (*replicaGCQueue) purgatoryChan() <-chan struct{} {
	return nil
}

// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package storage

import (
	"context"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
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
		Name:        "queue.replicagc.removereplica",
		Help:        "Number of replica removals attempted by the replica gc queue",
		Measurement: "Replica Removals",
		Unit:        metric.Unit_COUNT,
	}
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
			needsRaftInitialized:     true,
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
	ctx context.Context, now hlc.Timestamp, repl *Replica, _ *config.SystemConfig,
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
	ctx context.Context, repl *Replica, _ *config.SystemConfig,
) error {
	// Note that the Replicas field of desc is probably out of date, so
	// we should only use `desc` for its static fields like RangeID and
	// StartKey (and avoid rng.GetReplica() for the same reason).
	desc := repl.Desc()

	// Now get an updated descriptor for the range. Note that this may
	// not be _our_ range but instead some earlier range if our range has
	// been merged. See below.

	// Calls to RangeLookup typically use inconsistent reads, but we
	// want to do a consistent read here. This is important when we are
	// considering one of the metadata ranges: we must not do an inconsistent
	// lookup in our own copy of the range.
	rs, _, err := client.RangeLookup(ctx, rgcq.db.NonTransactionalSender(), desc.StartKey.AsRawKey(),
		roachpb.CONSISTENT, 0 /* prefetchNum */, false /* reverse */)
	if err != nil {
		return err
	}
	if len(rs) != 1 {
		// Regardless of whether ranges were merged, we're guaranteed one answer.
		//
		// TODO(knz): we should really have a separate type for assertion
		// errors that trigger telemetry, like
		// errors.AssertionFailedf() does.
		return errors.Errorf("expected 1 range descriptor, got %d", len(rs))
	}
	replyDesc := rs[0]

	// Now check whether the replica is meant to still exist.
	// Maybe it was deleted "under us" by being moved.
	currentDesc, currentMember := replyDesc.GetReplicaDescriptor(repl.store.StoreID())
	if desc.RangeID == replyDesc.RangeID && currentMember {
		// This replica is a current member of the raft group. Set the last replica
		// GC check time to avoid re-processing for another check interval.
		//
		// TODO(tschottdorf): should keep stats in particular on this outcome
		// but also on how good a job the queue does at inspecting every
		// Replica (see #8111) when inactive ones can be starved by
		// event-driven additions.
		log.VEventf(ctx, 1, "not gc'able, replica is still in range descriptor: %v", currentDesc)
		if err := repl.setLastReplicaGCTimestamp(ctx, repl.store.Clock().Now()); err != nil {
			return err
		}
	} else if desc.RangeID == replyDesc.RangeID {
		// We are no longer a member of this range, but the range still exists.
		// Clean up our local data.

		repl.mu.RLock()
		replicaID := repl.mu.replicaID
		ticks := repl.mu.ticks
		repl.mu.RUnlock()

		if replicaID == 0 {
			// This is a preemptive replica. GC'ing a preemptive replica is a
			// good idea if and only if the up-replication that it was a part of
			// did *NOT* commit. If it *did* commit and we're removing the
			// preemptive snapshot, the newly added follower will first need a
			// Raft snapshot to catch up, and that snapshot will be delayed by
			// #31875.
			// Log if the replica hasn't been around for very long.
			//
			// TODO(tschottdorf): avoid these, ideally without a time-based mechanism.
			// The replica carrying out the replication change could keep the
			// snapshot alive until it has either committed or aborted the txn.
			// Or we try to use Raft learners for this purpose.
			if ticks < 10 {
				log.Infof(ctx, "removing young preemptive snapshot (%d ticks)", ticks)
			}
		} else if replyDesc.EndKey.Less(desc.EndKey) {
			// The meta records indicate that the range has split but that this
			// replica hasn't processed the split trigger yet. By removing this
			// replica, we're also wiping out the data of what would become the
			// right hand side of the split (which may or may not still have a
			// replica on this store), and will need a Raft snapshot. Even worse,
			// the mechanism introduced in #31875 will artificially delay this
			// snapshot by seconds, during which time the RHS may see more splits
			// and incur more snapshots.
			//
			// TODO(tschottdorf): we can look up the range descriptor for the
			// RHS of the split (by querying with replyDesc.EndKey) and fetch
			// the local replica (which will be uninitialized, i.e. we have to
			// look it up by RangeID) to disable the mechanism in #31875 for it.
			// We should be able to use prefetching unconditionally to have this
			// desc ready whenever we need it.
			//
			// NB: there's solid evidence that this phenomenon can actually lead
			// to a large spike in Raft snapshots early in the life of a cluster
			// (in particular when combined with a restore operation) when the
			// removed replica has many pending splits and thus incurs a Raft
			// snapshot for *each* of them. This typically happens for the last
			// range:
			// [n1,replicaGC,s1,r33/1:/{Table/53/1/3…-Max}] removing replica [...]
			log.Infof(ctx, "removing replica with pending split; will incur Raft snapshot for right hand side")
		}

		rgcq.metrics.RemoveReplicaCount.Inc(1)
		log.VEventf(ctx, 1, "destroying local data")
		// Note that this seems racy - we didn't hold any locks between reading
		// the range descriptor above and deciding to remove the replica - but
		// we pass in the NextReplicaID to detect situations in which the
		// replica became "non-gc'able" in the meantime by checking (with raftMu
		// held throughout) whether the replicaID is still smaller than the
		// NextReplicaID.
		if err := repl.store.RemoveReplica(ctx, repl, replyDesc.NextReplicaID, RemoveOptions{
			DestroyData: true,
		}); err != nil {
			return err
		}
	} else {
		// This case is tricky. This range has been merged away, so it is likely
		// that we can GC this replica, but we need to be careful. If this store has
		// a replica of the subsuming range that has not yet applied the merge
		// trigger, we must not GC this replica.
		//
		// We can't just ask our local left neighbor whether it has an unapplied
		// merge, as if it's a slow follower it might not have learned about the
		// merge yet! What we can do, though, is check whether the generation of our
		// local left neighbor matches the generation of its meta2 descriptor. If it
		// is generationally up-to-date, it has applied all splits and merges, and
		// it is thus safe to remove this replica.
		leftRepl := repl.store.lookupPrecedingReplica(desc.StartKey)
		if leftRepl != nil {
			leftDesc := leftRepl.Desc()
			rs, _, err := client.RangeLookup(ctx, rgcq.db.NonTransactionalSender(), leftDesc.StartKey.AsRawKey(),
				roachpb.CONSISTENT, 0 /* prefetchNum */, false /* reverse */)
			if err != nil {
				return err
			}
			if len(rs) != 1 {
				return errors.Errorf("expected 1 range descriptor, got %d", len(rs))
			}
			if leftReplyDesc := rs[0]; !leftDesc.Equal(leftReplyDesc) {
				log.VEventf(ctx, 1, "left neighbor %s not up-to-date with meta descriptor %s; cannot safely GC range yet",
					leftDesc, leftReplyDesc)
				// Chances are that the left replica needs to be GC'd. Since we don't
				// have definitive proof, queue it with a low priority.
				rgcq.AddAsync(ctx, leftRepl, replicaGCPriorityDefault)
				return nil
			}
		}

		// We don't have the last NextReplicaID for the subsumed range, nor can we
		// obtain it, but that's OK: we can just be conservative and use the maximum
		// possible replica ID. store.RemoveReplica will write a tombstone using
		// this maximum possible replica ID, which would normally be problematic, as
		// it would prevent this store from ever having a new replica of the removed
		// range. In this case, however, it's copacetic, as subsumed ranges _can't_
		// have new replicas.
		const nextReplicaID = math.MaxInt32
		if err := repl.store.RemoveReplica(ctx, repl, nextReplicaID, RemoveOptions{
			DestroyData: true,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (*replicaGCQueue) timer(_ time.Duration) time.Duration {
	return replicaGCQueueTimerDuration
}

// purgatoryChan returns nil.
func (*replicaGCQueue) purgatoryChan() <-chan time.Time {
	return nil
}

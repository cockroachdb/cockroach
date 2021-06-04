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
	"time"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/raft/v3"
)

const (
	// replicaGCQueueTimerDuration is the duration between GCs of queued replicas.
	replicaGCQueueTimerDuration = 100 * time.Millisecond

	// ReplicaGCQueueCheckInterval is the inactivity duration after which
	// a range will be considered for garbage collection. Exported for testing.
	ReplicaGCQueueCheckInterval = 12 * time.Hour
	// ReplicaGCQueueSuspectCheckInterval is the duration after which a Replica
	// which is suspected to be removed should be considered for garbage
	// collection. See replicaIsSuspect() for details on what makes a replica
	// suspect.
	ReplicaGCQueueSuspectCheckInterval = 3 * time.Second
)

// Priorities for the replica GC queue.
const (
	replicaGCPriorityDefault = 0.0

	// Replicas that have been removed from the range spend a lot of
	// time in the candidate state, so treat them as higher priority.
	// Learner replicas which have been removed never enter the candidate state
	// but in the common case a replica should not be a learner for long so
	// treat it the same as a candidate.
	replicaGCPrioritySuspect = 1.0

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
	db      *kv.DB
}

// newReplicaGCQueue returns a new instance of replicaGCQueue.
func newReplicaGCQueue(store *Store, db *kv.DB, gossip *gossip.Gossip) *replicaGCQueue {
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
	ctx context.Context, now hlc.ClockTimestamp, repl *Replica, _ *config.SystemConfig,
) (shouldQ bool, prio float64) {
	if _, currentMember := repl.Desc().GetReplicaDescriptor(repl.store.StoreID()); !currentMember {
		return true, replicaGCPriorityRemoved
	}
	lastCheck, err := repl.GetLastReplicaGCTimestamp(ctx)
	if err != nil {
		log.Errorf(ctx, "could not read last replica GC timestamp: %+v", err)
		return false, 0
	}
	isSuspect := replicaIsSuspect(repl)

	return replicaGCShouldQueueImpl(now.ToTimestamp(), lastCheck, isSuspect)
}

func replicaIsSuspect(repl *Replica) bool {
	// It is critical to think of the replica as suspect if it is a learner as
	// it both shouldn't be a learner for long but will never become a candidate.
	// It is less critical to consider joint configuration members as suspect
	// but in cases where a replica is removed but only ever hears about the
	// command which sets it to VOTER_OUTGOING we would conservatively wait
	// 12 hours before removing the node. Finally we consider replicas which are
	// VOTER_INCOMING as suspect because no replica should stay in that state for
	// too long and being conservative here doesn't seem worthwhile.
	replDesc, ok := repl.Desc().GetReplicaDescriptor(repl.store.StoreID())
	if !ok {
		return true
	}
	if t := replDesc.GetType(); t != roachpb.VOTER_FULL && t != roachpb.NON_VOTER {
		return true
	}

	// NodeLiveness can be nil in tests/benchmarks.
	if repl.store.cfg.NodeLiveness == nil {
		return false
	}

	// If a replica doesn't have an active raft group, we should check whether
	// or not the node is active. If not, we should consider the replica suspect
	// because it has probably already been removed from its raft group but
	// doesn't know it. Without this, node decommissioning can stall on such
	// dormant ranges.
	raftStatus := repl.RaftStatus()
	if raftStatus == nil {
		liveness, ok := repl.store.cfg.NodeLiveness.Self()
		return ok && !liveness.Membership.Active()
	}

	livenessMap := repl.store.cfg.NodeLiveness.GetIsLiveMap()
	switch raftStatus.SoftState.RaftState {
	// If a replica is a candidate, then by definition it has lost contact with
	// its leader and possibly the rest of the Raft group, so consider it suspect.
	case raft.StateCandidate, raft.StatePreCandidate:
		return true

	// If the replica is a follower, check that the leader is in our range
	// descriptor and that we're still in touch with it. This handles e.g. a
	// non-voting replica which has lost its leader. It also attempts to handle
	// a quiesced follower which was partitioned away from the Raft group during
	// its own removal from the range -- this case is vulnerable to race
	// conditions, but if it fails it will be GCed within 12 hours anyway.
	case raft.StateFollower:
		leadDesc, ok := repl.Desc().GetReplicaDescriptorByID(roachpb.ReplicaID(raftStatus.Lead))
		if !ok || !livenessMap[leadDesc.NodeID].IsLive {
			return true
		}

	// If the replica is a leader, check that it has a quorum. This handles e.g.
	// a stuck leader with a lost quorum being replaced via Node.ResetQuorum,
	// which must cause the stale leader to relinquish its lease and GC itself.
	case raft.StateLeader:
		if !repl.Desc().Replicas().CanMakeProgress(func(d roachpb.ReplicaDescriptor) bool {
			return livenessMap[d.NodeID].IsLive
		}) {
			return true
		}
	}

	return false
}

func replicaGCShouldQueueImpl(now, lastCheck hlc.Timestamp, isSuspect bool) (bool, float64) {
	timeout := ReplicaGCQueueCheckInterval
	priority := replicaGCPriorityDefault

	if isSuspect {
		timeout = ReplicaGCQueueSuspectCheckInterval
		priority = replicaGCPrioritySuspect
	}

	// Only queue for GC if the timeout interval has passed since the last check.
	if !lastCheck.Add(timeout.Nanoseconds(), 0).Less(now) {
		return false, 0
	}
	return true, priority
}

// process performs a consistent lookup on the range descriptor to see if we are
// still a member of the range.
func (rgcq *replicaGCQueue) process(
	ctx context.Context, repl *Replica, _ *config.SystemConfig,
) (processed bool, err error) {
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
	rs, _, err := kv.RangeLookup(ctx, rgcq.db.NonTransactionalSender(), desc.StartKey.AsRawKey(),
		roachpb.CONSISTENT, 0 /* prefetchNum */, false /* reverse */)
	if err != nil {
		return false, err
	}
	if len(rs) != 1 {
		// Regardless of whether ranges were merged, we're guaranteed one answer.
		//
		// TODO(knz): we should really have a separate type for assertion
		// errors that trigger telemetry, like
		// errors.AssertionFailedf() does.
		return false, errors.Errorf("expected 1 range descriptor, got %d", len(rs))
	}
	replyDesc := rs[0]

	// Now check whether the replica is meant to still exist.
	// Maybe it was deleted "under us" by being moved.
	currentDesc, currentMember := replyDesc.GetReplicaDescriptor(repl.store.StoreID())
	sameRange := desc.RangeID == replyDesc.RangeID
	if sameRange && currentMember {
		// This replica is a current member of the raft group. Set the last replica
		// GC check time to avoid re-processing for another check interval.
		//
		// TODO(tschottdorf): should keep stats in particular on this outcome
		// but also on how good a job the queue does at inspecting every
		// Replica (see #8111) when inactive ones can be starved by
		// event-driven additions.
		log.VEventf(ctx, 1, "not gc'able, replica is still in range descriptor: %v", currentDesc)
		if err := repl.setLastReplicaGCTimestamp(ctx, repl.store.Clock().Now()); err != nil {
			return false, err
		}
		// Nothing to do, so return without having processed anything.
		//
		// Note that we do not check the replicaID at this point. If our
		// local replica ID is behind the one in the meta descriptor, we
		// could safely delete our local copy, but this would just force
		// the use of a snapshot when catching up to the new replica ID.
		// We don't normally expect to have a *higher* local replica ID
		// than the one in the meta descriptor, but it's possible after
		// recovering with unsafe-remove-dead-replicas.
		return false, nil
	} else if sameRange {
		// We are no longer a member of this range, but the range still exists.
		// Clean up our local data.

		if replyDesc.EndKey.Less(desc.EndKey) {
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

		nextReplicaID := replyDesc.NextReplicaID
		// Note that this seems racy - we didn't hold any locks between reading
		// the range descriptor above and deciding to remove the replica - but
		// we pass in the NextReplicaID to detect situations in which the
		// replica became "non-gc'able" in the meantime by checking (with raftMu
		// held throughout) whether the replicaID is still smaller than the
		// NextReplicaID. Given non-zero replica IDs don't change, this is only
		// possible if we currently think we're processing a pre-emptive snapshot
		// but discover in RemoveReplica that this range has since been added and
		// knows that.
		if err := repl.store.RemoveReplica(ctx, repl, nextReplicaID, RemoveOptions{
			DestroyData: true,
		}); err != nil {
			return false, err
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
			rs, _, err := kv.RangeLookup(ctx, rgcq.db.NonTransactionalSender(), leftDesc.StartKey.AsRawKey(),
				roachpb.CONSISTENT, 0 /* prefetchNum */, false /* reverse */)
			if err != nil {
				return false, err
			}
			if len(rs) != 1 {
				return false, errors.Errorf("expected 1 range descriptor, got %d", len(rs))
			}
			if leftReplyDesc := &rs[0]; !leftDesc.Equal(leftReplyDesc) {
				log.VEventf(ctx, 1, "left neighbor %s not up-to-date with meta descriptor %s; cannot safely GC range yet",
					leftDesc, leftReplyDesc)
				// Chances are that the left replica needs to be GC'd. Since we don't
				// have definitive proof, queue it with a low priority.
				rgcq.AddAsync(ctx, leftRepl, replicaGCPriorityDefault)
				return false, nil
			}
		}

		// A tombstone is written with a value of mergedTombstoneReplicaID because
		// we know the range to have been merged. See the Merge case of
		// runPreApplyTriggers() for details.
		if err := repl.store.RemoveReplica(ctx, repl, mergedTombstoneReplicaID, RemoveOptions{
			DestroyData: true,
		}); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (*replicaGCQueue) timer(_ time.Duration) time.Duration {
	return replicaGCQueueTimerDuration
}

// purgatoryChan returns nil.
func (*replicaGCQueue) purgatoryChan() <-chan time.Time {
	return nil
}

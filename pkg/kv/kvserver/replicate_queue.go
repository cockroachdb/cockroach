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
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/raft"
)

const (
	// replicateQueueTimerDuration is the duration between replication of queued
	// replicas.
	replicateQueueTimerDuration = 0 // zero duration to process replication greedily

	// newReplicaGracePeriod is the amount of time that we allow for a new
	// replica's raft state to catch up to the leader's before we start
	// considering it to be behind for the sake of rebalancing. We choose a
	// large value here because snapshots of large replicas can take a while
	// in high latency clusters, and not allowing enough of a cushion can
	// make rebalance thrashing more likely (#17879).
	newReplicaGracePeriod = 5 * time.Minute
)

// minLeaseTransferInterval controls how frequently leases can be transferred
// for rebalancing. It does not prevent transferring leases in order to allow
// a replica to be removed from a range.
var minLeaseTransferInterval = settings.RegisterNonNegativeDurationSetting(
	"kv.allocator.min_lease_transfer_interval",
	"controls how frequently leases can be transferred for rebalancing. "+
		"It does not prevent transferring leases in order to allow a "+
		"replica to be removed from a range.",
	1*time.Second,
)

var (
	metaReplicateQueueAddReplicaCount = metric.Metadata{
		Name:        "queue.replicate.addreplica",
		Help:        "Number of replica additions attempted by the replicate queue",
		Measurement: "Replica Additions",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueRemoveReplicaCount = metric.Metadata{
		Name:        "queue.replicate.removereplica",
		Help:        "Number of replica removals attempted by the replicate queue (typically in response to a rebalancer-initiated addition)",
		Measurement: "Replica Removals",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueRemoveDeadReplicaCount = metric.Metadata{
		Name:        "queue.replicate.removedeadreplica",
		Help:        "Number of dead replica removals attempted by the replicate queue (typically in response to a node outage)",
		Measurement: "Replica Removals",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueRemoveLearnerReplicaCount = metric.Metadata{
		Name:        "queue.replicate.removelearnerreplica",
		Help:        "Number of learner replica removals attempted by the replicate queue (typically due to internal race conditions)",
		Measurement: "Replica Removals",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueRebalanceReplicaCount = metric.Metadata{
		Name:        "queue.replicate.rebalancereplica",
		Help:        "Number of replica rebalancer-initiated additions attempted by the replicate queue",
		Measurement: "Replica Additions",
		Unit:        metric.Unit_COUNT,
	}
	metaReplicateQueueTransferLeaseCount = metric.Metadata{
		Name:        "queue.replicate.transferlease",
		Help:        "Number of range lease transfers attempted by the replicate queue",
		Measurement: "Lease Transfers",
		Unit:        metric.Unit_COUNT,
	}
)

// quorumError indicates a retryable error condition which sends replicas being
// processed through the replicate queue into purgatory so that they can be
// retried quickly as soon as nodes come online.
type quorumError struct {
	msg string
}

func newQuorumError(f string, args ...interface{}) *quorumError {
	return &quorumError{
		msg: fmt.Sprintf(f, args...),
	}
}

func (e *quorumError) Error() string {
	return e.msg
}

func (*quorumError) purgatoryErrorMarker() {}

// ReplicateQueueMetrics is the set of metrics for the replicate queue.
type ReplicateQueueMetrics struct {
	AddReplicaCount           *metric.Counter
	RemoveReplicaCount        *metric.Counter
	RemoveDeadReplicaCount    *metric.Counter
	RemoveLearnerReplicaCount *metric.Counter
	RebalanceReplicaCount     *metric.Counter
	TransferLeaseCount        *metric.Counter
}

func makeReplicateQueueMetrics() ReplicateQueueMetrics {
	return ReplicateQueueMetrics{
		AddReplicaCount:           metric.NewCounter(metaReplicateQueueAddReplicaCount),
		RemoveReplicaCount:        metric.NewCounter(metaReplicateQueueRemoveReplicaCount),
		RemoveDeadReplicaCount:    metric.NewCounter(metaReplicateQueueRemoveDeadReplicaCount),
		RemoveLearnerReplicaCount: metric.NewCounter(metaReplicateQueueRemoveLearnerReplicaCount),
		RebalanceReplicaCount:     metric.NewCounter(metaReplicateQueueRebalanceReplicaCount),
		TransferLeaseCount:        metric.NewCounter(metaReplicateQueueTransferLeaseCount),
	}
}

// replicateQueue manages a queue of replicas which may need to add an
// additional replica to their range.
type replicateQueue struct {
	*baseQueue
	metrics           ReplicateQueueMetrics
	allocator         Allocator
	updateChan        chan time.Time
	lastLeaseTransfer atomic.Value // read and written by scanner & queue goroutines
}

// newReplicateQueue returns a new instance of replicateQueue.
func newReplicateQueue(store *Store, g *gossip.Gossip, allocator Allocator) *replicateQueue {
	rq := &replicateQueue{
		metrics:    makeReplicateQueueMetrics(),
		allocator:  allocator,
		updateChan: make(chan time.Time, 1),
	}
	store.metrics.registry.AddMetricStruct(&rq.metrics)
	rq.baseQueue = newBaseQueue(
		"replicate", rq, store, g,
		queueConfig{
			maxSize:              defaultQueueMaxSize,
			needsLease:           true,
			needsSystemConfig:    true,
			acceptsUnsplitRanges: store.TestingKnobs().ReplicateQueueAcceptsUnsplit,
			// The processing of the replicate queue often needs to send snapshots
			// so we use the raftSnapshotQueueTimeoutFunc. This function sets a
			// timeout based on the range size and the sending rate in addition
			// to consulting the setting which controls the minimum timeout.
			processTimeoutFunc: makeRateLimitedTimeoutFunc(rebalanceSnapshotRate),
			successes:          store.metrics.ReplicateQueueSuccesses,
			failures:           store.metrics.ReplicateQueueFailures,
			pending:            store.metrics.ReplicateQueuePending,
			processingNanos:    store.metrics.ReplicateQueueProcessingNanos,
			purgatory:          store.metrics.ReplicateQueuePurgatory,
		},
	)

	updateFn := func() {
		select {
		case rq.updateChan <- timeutil.Now():
		default:
		}
	}

	// Register gossip and node liveness callbacks to signal that
	// replicas in purgatory might be retried.
	if g != nil { // gossip is nil for some unittests
		g.RegisterCallback(gossip.MakePrefixPattern(gossip.KeyStorePrefix), func(key string, _ roachpb.Value) {
			if !rq.store.IsStarted() {
				return
			}
			// Because updates to our store's own descriptor won't affect
			// replicas in purgatory, skip updating the purgatory channel
			// in this case.
			if storeID, err := gossip.StoreIDFromKey(key); err == nil && storeID == rq.store.StoreID() {
				return
			}
			updateFn()
		})
	}
	if nl := store.cfg.NodeLiveness; nl != nil { // node liveness is nil for some unittests
		nl.RegisterCallback(func(_ roachpb.NodeID) {
			updateFn()
		})
	}

	return rq
}

func (rq *replicateQueue) shouldQueue(
	ctx context.Context, now hlc.Timestamp, repl *Replica, sysCfg *config.SystemConfig,
) (shouldQ bool, priority float64) {
	desc, zone := repl.DescAndZone()
	action, priority := rq.allocator.ComputeAction(ctx, zone, desc)

	// For simplicity, the first thing the allocator does is remove learners, so
	// it can do all of its reasoning about only voters. We do the same here so
	// the executions of the allocator's decisions can be in terms of voters.
	if action == AllocatorRemoveLearner {
		return true, priority
	}
	voterReplicas := desc.Replicas().Voters()

	if action == AllocatorNoop {
		log.VEventf(ctx, 2, "no action to take")
		return false, 0
	} else if action != AllocatorConsiderRebalance {
		log.VEventf(ctx, 2, "repair needed (%s), enqueuing", action)
		return true, priority
	}

	if !rq.store.TestingKnobs().DisableReplicaRebalancing {
		rangeUsageInfo := rangeUsageInfoForRepl(repl)
		_, _, _, ok := rq.allocator.RebalanceTarget(
			ctx, zone, repl.RaftStatus(), voterReplicas, rangeUsageInfo, storeFilterThrottled)
		if ok {
			log.VEventf(ctx, 2, "rebalance target found, enqueuing")
			return true, 0
		}
		log.VEventf(ctx, 2, "no rebalance target found, not enqueuing")
	}

	// If the lease is valid, check to see if we should transfer it.
	if lease, _ := repl.GetLease(); repl.IsLeaseValid(lease, now) {
		if rq.canTransferLease() &&
			rq.allocator.ShouldTransferLease(
				ctx, zone, voterReplicas, lease.Replica.StoreID, repl.leaseholderStats) {
			log.VEventf(ctx, 2, "lease transfer needed, enqueuing")
			return true, 0
		}
	}

	return false, 0
}

func (rq *replicateQueue) process(
	ctx context.Context, repl *Replica, sysCfg *config.SystemConfig,
) (processed bool, err error) {
	retryOpts := retry.Options{
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     1 * time.Second,
		Multiplier:     2,
		MaxRetries:     5,
	}

	// Use a retry loop in order to backoff in the case of snapshot errors,
	// usually signaling that a rebalancing reservation could not be made with the
	// selected target.
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		for {
			requeue, err := rq.processOneChange(ctx, repl, rq.canTransferLease, false /* dryRun */)
			if IsSnapshotError(err) {
				// If ChangeReplicas failed because the snapshot failed, we log the
				// error but then return success indicating we should retry the
				// operation. The most likely causes of the snapshot failing are a
				// declined reservation or the remote node being unavailable. In either
				// case we don't want to wait another scanner cycle before reconsidering
				// the range.
				log.Infof(ctx, "%v", err)
				break
			}

			if err != nil {
				return false, err
			}

			if testingAggressiveConsistencyChecks {
				if _, err := rq.store.consistencyQueue.process(ctx, repl, sysCfg); err != nil {
					log.Warningf(ctx, "%v", err)
				}
			}

			if !requeue {
				return true, nil
			}

			log.VEventf(ctx, 1, "re-processing")
		}
	}

	return false, errors.Errorf("failed to replicate after %d retries", retryOpts.MaxRetries)
}

func (rq *replicateQueue) processOneChange(
	ctx context.Context, repl *Replica, canTransferLease func() bool, dryRun bool,
) (requeue bool, _ error) {
	// Check lease and destroy status here. The queue does this higher up already, but
	// adminScatter (and potential other future callers) also call this method and don't
	// perform this check, which could lead to infinite loops.
	if _, err := repl.IsDestroyed(); err != nil {
		return false, err
	}
	if _, pErr := repl.redirectOnOrAcquireLease(ctx); pErr != nil {
		return false, pErr.GoError()
	}

	desc, zone := repl.DescAndZone()

	// Avoid taking action if the range has too many dead replicas to make
	// quorum.
	voterReplicas := desc.Replicas().Voters()
	liveVoterReplicas, deadVoterReplicas := rq.allocator.storePool.liveAndDeadReplicas(voterReplicas)
	{
		unavailable := !desc.Replicas().CanMakeProgress(func(rDesc roachpb.ReplicaDescriptor) bool {
			for _, inner := range liveVoterReplicas {
				if inner.ReplicaID == rDesc.ReplicaID {
					return true
				}
			}
			return false
		})
		if unavailable {
			return false, newQuorumError(
				"range requires a replication change, but live replicas %v don't constitute a quorum for %v:",
				liveVoterReplicas,
				desc.Replicas().All(),
			)
		}
	}

	action, _ := rq.allocator.ComputeAction(ctx, zone, desc)
	log.VEventf(ctx, 1, "next replica action: %s", action)

	// For simplicity, the first thing the allocator does is remove learners, so
	// it can do all of its reasoning about only voters. We do the same here so
	// the executions of the allocator's decisions can be in terms of voters.
	if action == AllocatorRemoveLearner {
		return rq.removeLearner(ctx, repl, dryRun)
	}

	switch action {
	case AllocatorNoop, AllocatorRangeUnavailable:
		// We're either missing liveness information or the range is known to have
		// lost quorum. Either way, it's not a good idea to make changes right now.
		// Let the scanner requeue it again later.
		return false, nil
	case AllocatorAdd:
		return rq.addOrReplace(ctx, repl, voterReplicas, liveVoterReplicas, -1 /* removeIdx */, dryRun)
	case AllocatorRemove:
		return rq.remove(ctx, repl, voterReplicas, dryRun)
	case AllocatorReplaceDead:
		if len(deadVoterReplicas) == 0 {
			// Nothing to do.
			return false, nil
		}
		removeIdx := -1 // guaranteed to be changed below
		for i, rDesc := range voterReplicas {
			if rDesc.StoreID == deadVoterReplicas[0].StoreID {
				removeIdx = i
				break
			}
		}
		if removeIdx < 0 {
			return false, errors.AssertionFailedf(
				"dead voter %v unexpectedly not found in %v",
				deadVoterReplicas[0], voterReplicas)
		}
		return rq.addOrReplace(ctx, repl, voterReplicas, liveVoterReplicas, removeIdx, dryRun)
	case AllocatorReplaceDecommissioning:
		decommissioningReplicas := rq.allocator.storePool.decommissioningReplicas(voterReplicas)
		if len(decommissioningReplicas) == 0 {
			// Nothing to do.
			return false, nil
		}
		removeIdx := -1 // guaranteed to be changed below
		for i, rDesc := range voterReplicas {
			if rDesc.StoreID == decommissioningReplicas[0].StoreID {
				removeIdx = i
				break
			}
		}
		if removeIdx < 0 {
			return false, errors.AssertionFailedf(
				"decommissioning voter %v unexpectedly not found in %v",
				decommissioningReplicas[0], voterReplicas)
		}
		return rq.addOrReplace(ctx, repl, voterReplicas, liveVoterReplicas, removeIdx, dryRun)
	case AllocatorRemoveDecommissioning:
		// NB: this path will only be hit when the range is over-replicated and
		// has decommissioning replicas; in the common case we'll hit
		// AllocatorReplaceDecommissioning above.
		return rq.removeDecommissioning(ctx, repl, dryRun)
	case AllocatorRemoveDead:
		// NB: this path will only be hit when the range is over-replicated and
		// has dead replicas; in the common case we'll hit AllocatorReplaceDead
		// above.
		return rq.removeDead(ctx, repl, deadVoterReplicas, dryRun)
	case AllocatorRemoveLearner:
		return rq.removeLearner(ctx, repl, dryRun)
	case AllocatorConsiderRebalance:
		return rq.considerRebalance(ctx, repl, voterReplicas, canTransferLease, dryRun)
	case AllocatorFinalizeAtomicReplicationChange:
		_, err := maybeLeaveAtomicChangeReplicasAndRemoveLearners(ctx, repl.store, repl.Desc())
		// Requeue because either we failed to transition out of a joint state
		// (bad) or we did and there might be more to do for that range.
		return true, err
	default:
		return false, errors.Errorf("unknown allocator action %v", action)
	}
}

// addOrReplace adds or replaces a replica. If removeIdx is -1, an addition is
// carried out. Otherwise, removeIdx must be a valid index into existingReplicas
// and specifies which replica to replace with a new one.
//
// The method preferably issues an atomic replica swap, but may not be able to
// do this in all cases, such as when atomic replication changes are not
// available, or when the range consists of a single replica. As a fall back,
// only the addition is carried out; the removal is then a follow-up step for
// the next scanner cycle.
func (rq *replicateQueue) addOrReplace(
	ctx context.Context,
	repl *Replica,
	existingReplicas []roachpb.ReplicaDescriptor,
	liveVoterReplicas []roachpb.ReplicaDescriptor,
	removeIdx int, // -1 for no removal
	dryRun bool,
) (requeue bool, _ error) {
	if len(existingReplicas) == 1 {
		// If only one replica remains, that replica is the leaseholder and
		// we won't be able to swap it out. Ignore the removal and simply add
		// a replica.
		removeIdx = -1
	}
	st := rq.store.cfg.Settings
	if !st.Version.IsActive(ctx, clusterversion.VersionAtomicChangeReplicas) {
		// If we can't swap yet, don't.
		removeIdx = -1
	}

	remainingLiveReplicas := liveVoterReplicas
	if removeIdx >= 0 {
		replToRemove := existingReplicas[removeIdx]
		for i, r := range liveVoterReplicas {
			if r.ReplicaID == replToRemove.ReplicaID {
				remainingLiveReplicas = append(liveVoterReplicas[:i:i], liveVoterReplicas[i+1:]...)
				break
			}
		}
		// See about transferring the lease away if we're about to remove the
		// leaseholder.
		done, err := rq.maybeTransferLeaseAway(ctx, repl, existingReplicas[removeIdx].StoreID, dryRun)
		if err != nil {
			return false, err
		}
		if done {
			// Lease was transferred away. Next leaseholder is going to take over.
			return false, nil
		}
	}

	desc, zone := repl.DescAndZone()
	// Allocate a target assuming that the replica we're replacing (if any) is
	// already gone. The allocator should not try to re-add this replica since
	// there is a reason we're removing it (i.e. dead or decommissioning). If we
	// left the replica in the slice, the allocator would not be guaranteed to
	// pick a replica that fills the gap removeRepl leaves once it's gone.
	newStore, details, err := rq.allocator.AllocateTarget(
		ctx,
		zone,
		remainingLiveReplicas,
	)
	if err != nil {
		return false, err
	}
	if removeIdx >= 0 && newStore.StoreID == existingReplicas[removeIdx].StoreID {
		return false, errors.AssertionFailedf("allocator suggested to replace replica on s%d with itself", newStore.StoreID)
	}
	newReplica := roachpb.ReplicationTarget{
		NodeID:  newStore.Node.NodeID,
		StoreID: newStore.StoreID,
	}

	clusterNodes := rq.allocator.storePool.ClusterNodeCount()
	need := GetNeededReplicas(*zone.NumReplicas, clusterNodes)

	// Only up-replicate if there are suitable allocation targets such that,
	// either the replication goal is met, or it is possible to get to the next
	// odd number of replicas. A consensus group of size 2n has worse failure
	// tolerance properties than a group of size 2n - 1 because it has a larger
	// quorum. For example, up-replicating from 1 to 2 replicas only makes sense
	// if it is possible to be able to go to 3 replicas.
	//
	// NB: If willHave > need, then always allow up-replicating as that
	// will be the case when up-replicating a range with a decommissioning
	// replica.
	//
	// We skip this check if we're swapping a replica, since that does not
	// change the quorum size.
	if willHave := len(existingReplicas) + 1; removeIdx < 0 && willHave < need && willHave%2 == 0 {
		// This means we are going to up-replicate to an even replica state.
		// Check if it is possible to go to an odd replica state beyond it.
		oldPlusNewReplicas := append([]roachpb.ReplicaDescriptor(nil), existingReplicas...)
		oldPlusNewReplicas = append(oldPlusNewReplicas, roachpb.ReplicaDescriptor{
			NodeID:  newStore.Node.NodeID,
			StoreID: newStore.StoreID,
		})
		_, _, err := rq.allocator.AllocateTarget(
			ctx,
			zone,
			oldPlusNewReplicas,
		)
		if err != nil {
			// It does not seem possible to go to the next odd replica state. Note
			// that AllocateTarget returns an allocatorError (a purgatoryError)
			// when purgatory is requested.
			return false, errors.Wrap(err, "avoid up-replicating to fragile quorum")
		}
	}
	rq.metrics.AddReplicaCount.Inc(1)
	ops := roachpb.MakeReplicationChanges(roachpb.ADD_REPLICA, newReplica)
	if removeIdx < 0 {
		log.VEventf(ctx, 1, "adding replica %+v: %s",
			newReplica, rangeRaftProgress(repl.RaftStatus(), existingReplicas))
	} else {
		rq.metrics.RemoveReplicaCount.Inc(1)
		removeReplica := existingReplicas[removeIdx]
		log.VEventf(ctx, 1, "replacing replica %s with %+v: %s",
			removeReplica, newReplica, rangeRaftProgress(repl.RaftStatus(), existingReplicas))
		ops = append(ops,
			roachpb.MakeReplicationChanges(roachpb.REMOVE_REPLICA, roachpb.ReplicationTarget{
				StoreID: removeReplica.StoreID,
				NodeID:  removeReplica.NodeID,
			})...)
	}

	if err := rq.changeReplicas(
		ctx,
		repl,
		ops,
		desc,
		SnapshotRequest_RECOVERY,
		kvserverpb.ReasonRangeUnderReplicated,
		details,
		dryRun,
	); err != nil {
		return false, err
	}
	// Always requeue to see if more work needs to be done.
	return true, nil
}

// findRemoveTarget takes a list of replicas and picks one to remove, making
// sure to not remove a newly added replica or to violate the zone configs in
// the progress.
func (rq *replicateQueue) findRemoveTarget(
	ctx context.Context,
	repl interface {
		DescAndZone() (*roachpb.RangeDescriptor, *zonepb.ZoneConfig)
		LastReplicaAdded() (roachpb.ReplicaID, time.Time)
		RaftStatus() *raft.Status
	},
	existingReplicas []roachpb.ReplicaDescriptor,
) (roachpb.ReplicaDescriptor, string, error) {
	_, zone := repl.DescAndZone()
	// This retry loop involves quick operations on local state, so a
	// small MaxBackoff is good (but those local variables change on
	// network time scales as raft receives responses).
	//
	// TODO(bdarnell): There's another retry loop at process(). It
	// would be nice to combine these, but I'm keeping them separate
	// for now so we can tune the options separately.
	retryOpts := retry.Options{
		InitialBackoff: time.Millisecond,
		MaxBackoff:     200 * time.Millisecond,
		Multiplier:     2,
	}

	var candidates []roachpb.ReplicaDescriptor
	deadline := timeutil.Now().Add(2 * base.NetworkTimeout)
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next() && timeutil.Now().Before(deadline); {
		lastReplAdded, lastAddedTime := repl.LastReplicaAdded()
		if timeutil.Since(lastAddedTime) > newReplicaGracePeriod {
			lastReplAdded = 0
		}
		raftStatus := repl.RaftStatus()
		if raftStatus == nil || raftStatus.RaftState != raft.StateLeader {
			// If we've lost raft leadership, we're unlikely to regain it so give up immediately.
			return roachpb.ReplicaDescriptor{}, "", &benignError{errors.Errorf("not raft leader while range needs removal")}
		}
		candidates = filterUnremovableReplicas(ctx, raftStatus, existingReplicas, lastReplAdded)
		log.VEventf(ctx, 3, "filtered unremovable replicas from %v to get %v as candidates for removal: %s",
			existingReplicas, candidates, rangeRaftProgress(raftStatus, existingReplicas))
		if len(candidates) > 0 {
			break
		}
		if len(raftStatus.Progress) <= 2 {
			// HACK(bdarnell): Downreplicating to a single node from
			// multiple nodes is not really supported. There are edge
			// cases in which the two peers stop communicating with each
			// other too soon and we don't reach a satisfactory
			// resolution. However, some tests (notably
			// TestRepartitioning) get into this state, and if the
			// replication queue spends its entire timeout waiting for the
			// downreplication to finish the test will time out. As a
			// hack, just fail-fast when we're trying to go down to a
			// single replica.
			break
		}
		// After upreplication, the candidates for removal could still
		// be catching up. The allocator determined that the range was
		// over-replicated, and it's important to clear that state as
		// quickly as we can (because over-replicated ranges may be
		// under-diversified). If we return an error here, this range
		// probably won't be processed again until the next scanner
		// cycle, which is too long, so we retry here.
	}
	if len(candidates) == 0 {
		// If we timed out and still don't have any valid candidates, give up.
		return roachpb.ReplicaDescriptor{}, "", &benignError{errors.Errorf("no removable replicas from range that needs a removal: %s",
			rangeRaftProgress(repl.RaftStatus(), existingReplicas))}
	}

	return rq.allocator.RemoveTarget(ctx, zone, candidates, existingReplicas)
}

// maybeTransferLeaseAway is called whenever a replica on a given store is
// slated for removal. If the store corresponds to the store of the caller
// (which is very likely to be the leaseholder), then this removal would fail.
// Instead, this method will attempt to transfer the lease away, and returns
// true to indicate to the caller that it should not pursue the current
// replication change further because it is no longer the leaseholder. When the
// returned bool is false, it should continue. On error, the caller should also
// stop.
func (rq *replicateQueue) maybeTransferLeaseAway(
	ctx context.Context, repl *Replica, removeStoreID roachpb.StoreID, dryRun bool,
) (done bool, _ error) {
	if removeStoreID != repl.store.StoreID() {
		return false, nil
	}
	desc, zone := repl.DescAndZone()
	// The local replica was selected as the removal target, but that replica
	// is the leaseholder, so transfer the lease instead. We don't check that
	// the current store has too many leases in this case under the
	// assumption that replica balance is a greater concern. Also note that
	// AllocatorRemove action takes preference over AllocatorConsiderRebalance
	// (rebalancing) which is where lease transfer would otherwise occur. We
	// need to be able to transfer leases in AllocatorRemove in order to get
	// out of situations where this store is overfull and yet holds all the
	// leases. The fullness checks need to be ignored for cases where
	// a replica needs to be removed for constraint violations.
	return rq.findTargetAndTransferLease(
		ctx,
		repl,
		desc,
		zone,
		transferLeaseOptions{
			dryRun: dryRun,
		},
	)
}

func (rq *replicateQueue) remove(
	ctx context.Context, repl *Replica, existingReplicas []roachpb.ReplicaDescriptor, dryRun bool,
) (requeue bool, _ error) {
	removeReplica, details, err := rq.findRemoveTarget(ctx, repl, existingReplicas)
	if err != nil {
		return false, err
	}
	done, err := rq.maybeTransferLeaseAway(ctx, repl, removeReplica.StoreID, dryRun)
	if err != nil {
		return false, err
	}
	if done {
		// Lease is now elsewhere, so we're not in charge any more.
		return false, nil
	}

	// Remove a replica.
	rq.metrics.RemoveReplicaCount.Inc(1)
	log.VEventf(ctx, 1, "removing replica %+v due to over-replication: %s",
		removeReplica, rangeRaftProgress(repl.RaftStatus(), existingReplicas))
	target := roachpb.ReplicationTarget{
		NodeID:  removeReplica.NodeID,
		StoreID: removeReplica.StoreID,
	}
	desc, _ := repl.DescAndZone()
	if err := rq.changeReplicas(
		ctx,
		repl,
		roachpb.MakeReplicationChanges(roachpb.REMOVE_REPLICA, target),
		desc,
		SnapshotRequest_UNKNOWN, // unused
		kvserverpb.ReasonRangeOverReplicated,
		details,
		dryRun,
	); err != nil {
		return false, err
	}
	return true, nil
}

func (rq *replicateQueue) removeDecommissioning(
	ctx context.Context, repl *Replica, dryRun bool,
) (requeue bool, _ error) {
	desc, _ := repl.DescAndZone()
	decommissioningReplicas := rq.allocator.storePool.decommissioningReplicas(desc.Replicas().All())
	if len(decommissioningReplicas) == 0 {
		log.VEventf(ctx, 1, "range of replica %s was identified as having decommissioning replicas, "+
			"but no decommissioning replicas were found", repl)
		return true, nil
	}
	decommissioningReplica := decommissioningReplicas[0]
	done, err := rq.maybeTransferLeaseAway(ctx, repl, decommissioningReplica.StoreID, dryRun)
	if err != nil {
		return false, err
	}
	if done {
		// Not leaseholder any more.
		return false, nil
	}
	// Remove the decommissioning replica.
	rq.metrics.RemoveReplicaCount.Inc(1)
	log.VEventf(ctx, 1, "removing decommissioning replica %+v from store", decommissioningReplica)
	target := roachpb.ReplicationTarget{
		NodeID:  decommissioningReplica.NodeID,
		StoreID: decommissioningReplica.StoreID,
	}
	if err := rq.changeReplicas(
		ctx,
		repl,
		roachpb.MakeReplicationChanges(roachpb.REMOVE_REPLICA, target),
		desc,
		SnapshotRequest_UNKNOWN, // unused
		kvserverpb.ReasonStoreDecommissioning, "", dryRun,
	); err != nil {
		return false, err
	}
	// We removed a replica, so check if there's more to do.
	return true, nil
}

func (rq *replicateQueue) removeDead(
	ctx context.Context, repl *Replica, deadVoterReplicas []roachpb.ReplicaDescriptor, dryRun bool,
) (requeue bool, _ error) {
	desc := repl.Desc()
	if len(deadVoterReplicas) == 0 {
		log.VEventf(ctx, 1, "range of replica %s was identified as having dead replicas, but no dead replicas were found", repl)
		return true, nil
	}
	deadReplica := deadVoterReplicas[0]
	rq.metrics.RemoveDeadReplicaCount.Inc(1)
	log.VEventf(ctx, 1, "removing dead replica %+v from store", deadReplica)
	target := roachpb.ReplicationTarget{
		NodeID:  deadReplica.NodeID,
		StoreID: deadReplica.StoreID,
	}
	// NB: we don't check whether to transfer the lease away because if the removal target
	// is dead, it's not us (and if for some reason that happens, the removal is simply
	// going to fail).
	if err := rq.changeReplicas(
		ctx,
		repl,
		roachpb.MakeReplicationChanges(roachpb.REMOVE_REPLICA, target),
		desc,
		SnapshotRequest_UNKNOWN, // unused
		kvserverpb.ReasonStoreDead,
		"",
		dryRun,
	); err != nil {
		return false, err
	}
	return true, nil
}

func (rq *replicateQueue) removeLearner(
	ctx context.Context, repl *Replica, dryRun bool,
) (requeue bool, _ error) {
	desc := repl.Desc()
	learnerReplicas := desc.Replicas().Learners()
	if len(learnerReplicas) == 0 {
		log.VEventf(ctx, 1, "range of replica %s was identified as having learner replicas, "+
			"but no learner replicas were found", repl)
		return true, nil
	}
	learnerReplica := learnerReplicas[0]
	rq.metrics.RemoveLearnerReplicaCount.Inc(1)
	log.VEventf(ctx, 1, "removing learner replica %+v from store", learnerReplica)
	target := roachpb.ReplicationTarget{
		NodeID:  learnerReplica.NodeID,
		StoreID: learnerReplica.StoreID,
	}
	// NB: we don't check whether to transfer the lease away because we're very unlikely
	// to be the learner (and if so, we don't have the lease any more, so after the removal
	// fails the situation will have rectified itself).
	if err := rq.changeReplicas(
		ctx,
		repl,
		roachpb.MakeReplicationChanges(roachpb.REMOVE_REPLICA, target),
		desc,
		SnapshotRequest_UNKNOWN,
		kvserverpb.ReasonAbandonedLearner,
		"",
		dryRun,
	); err != nil {
		return false, err
	}
	return true, nil
}

func (rq *replicateQueue) considerRebalance(
	ctx context.Context,
	repl *Replica,
	existingReplicas []roachpb.ReplicaDescriptor,
	canTransferLease func() bool,
	dryRun bool,
) (requeue bool, _ error) {
	desc, zone := repl.DescAndZone()
	// The Noop case will result if this replica was queued in order to
	// rebalance. Attempt to find a rebalancing target.
	if !rq.store.TestingKnobs().DisableReplicaRebalancing {
		rangeUsageInfo := rangeUsageInfoForRepl(repl)
		addTarget, removeTarget, details, ok := rq.allocator.RebalanceTarget(
			ctx, zone, repl.RaftStatus(), existingReplicas, rangeUsageInfo,
			storeFilterThrottled)
		if !ok {
			log.VEventf(ctx, 1, "no suitable rebalance target")
		} else if done, err := rq.maybeTransferLeaseAway(ctx, repl, removeTarget.StoreID, dryRun); err != nil {
			log.VEventf(ctx, 1, "want to remove self, but failed to transfer lease away: %s", err)
		} else if done {
			// Lease is now elsewhere, so we're not in charge any more.
			return false, nil
		} else {
			// We have a replica to remove and one we can add, so let's swap them
			// out.
			chgs := []roachpb.ReplicationChange{
				// NB: we place the addition first because in the case of
				// atomic replication changes being turned off, the changes
				// will be executed individually in the order in which they
				// appear.
				{Target: addTarget, ChangeType: roachpb.ADD_REPLICA},
				{Target: removeTarget, ChangeType: roachpb.REMOVE_REPLICA},
			}

			if len(existingReplicas) == 1 {
				// If there's only one replica, the removal target is the
				// leaseholder and this is unsupported and will fail. However,
				// this is also the only way to rebalance in a single-replica
				// range. If we try the atomic swap here, we'll fail doing
				// nothing, and so we stay locked into the current distribution
				// of replicas. (Note that maybeTransferLeaseAway above will not
				// have found a target, and so will have returned (false, nil).
				//
				// Do the best thing we can, which is carry out the addition
				// only, which should succeed, and the next time we touch this
				// range, we will have one more replica and hopefully it will
				// take the lease and remove the current leaseholder.
				//
				// It's possible that "rebalancing deadlock" can occur in other
				// scenarios, it's really impossible to tell from the code given
				// the constraints we support. However, the lease transfer often
				// does not happen spuriously, and we can't enter dangerous
				// configurations sporadically, so this code path is only hit
				// when we know it's necessary, picking the smaller of two evils.
				//
				// See https://github.com/cockroachdb/cockroach/issues/40333.
				chgs = chgs[:1]
				log.VEventf(ctx, 1, "can't swap replica due to lease; falling back to add")
			}

			rq.metrics.RebalanceReplicaCount.Inc(1)
			log.VEventf(ctx, 1, "rebalancing %+v to %+v: %s",
				removeTarget, addTarget, rangeRaftProgress(repl.RaftStatus(), existingReplicas))

			if err := rq.changeReplicas(
				ctx,
				repl,
				chgs,
				desc,
				SnapshotRequest_REBALANCE,
				kvserverpb.ReasonRebalance,
				details,
				dryRun,
			); err != nil {
				return false, err
			}
			return true, nil
		}
	}

	if canTransferLease() {
		// We require the lease in order to process replicas, so
		// repl.store.StoreID() corresponds to the lease-holder's store ID.
		transferred, err := rq.findTargetAndTransferLease(
			ctx,
			repl,
			desc,
			zone,
			transferLeaseOptions{
				checkTransferLeaseSource: true,
				checkCandidateFullness:   true,
				dryRun:                   dryRun,
			},
		)
		if err != nil {
			return false, err
		}
		// Do not requeue as we transferred our lease away.
		if transferred {
			return false, nil
		}
	}

	// No action was necessary and no rebalance target was found. Return
	// without re-queuing this replica.
	return false, nil
}

type transferLeaseOptions struct {
	checkTransferLeaseSource bool
	checkCandidateFullness   bool
	dryRun                   bool
}

func (rq *replicateQueue) findTargetAndTransferLease(
	ctx context.Context,
	repl *Replica,
	desc *roachpb.RangeDescriptor,
	zone *zonepb.ZoneConfig,
	opts transferLeaseOptions,
) (bool, error) {
	// Learner replicas aren't allowed to become the leaseholder or raft leader,
	// so only consider the `Voters` replicas.
	target := rq.allocator.TransferLeaseTarget(
		ctx,
		zone,
		desc.Replicas().Voters(),
		repl.store.StoreID(),
		repl.leaseholderStats,
		opts.checkTransferLeaseSource,
		opts.checkCandidateFullness,
		false, /* alwaysAllowDecisionWithoutStats */
	)
	if target == (roachpb.ReplicaDescriptor{}) {
		return false, nil
	}

	if opts.dryRun {
		log.VEventf(ctx, 1, "transferring lease to s%d", target.StoreID)
		return false, nil
	}

	avgQPS, qpsMeasurementDur := repl.leaseholderStats.avgQPS()
	if qpsMeasurementDur < MinStatsDuration {
		avgQPS = 0
	}
	err := rq.transferLease(ctx, repl, target, avgQPS)
	return err == nil, err
}

func (rq *replicateQueue) transferLease(
	ctx context.Context, repl *Replica, target roachpb.ReplicaDescriptor, rangeQPS float64,
) error {
	rq.metrics.TransferLeaseCount.Inc(1)
	log.VEventf(ctx, 1, "transferring lease to s%d", target.StoreID)
	if err := repl.AdminTransferLease(ctx, target.StoreID); err != nil {
		return errors.Wrapf(err, "%s: unable to transfer lease to s%d", repl, target.StoreID)
	}
	rq.lastLeaseTransfer.Store(timeutil.Now())
	rq.allocator.storePool.updateLocalStoresAfterLeaseTransfer(
		repl.store.StoreID(), target.StoreID, rangeQPS)
	return nil
}

func (rq *replicateQueue) changeReplicas(
	ctx context.Context,
	repl *Replica,
	chgs roachpb.ReplicationChanges,
	desc *roachpb.RangeDescriptor,
	priority SnapshotRequest_Priority,
	reason kvserverpb.RangeLogEventReason,
	details string,
	dryRun bool,
) error {
	if dryRun {
		return nil
	}
	if _, err := repl.ChangeReplicas(ctx, desc, priority, reason, details, chgs); err != nil {
		return err
	}
	rangeUsageInfo := rangeUsageInfoForRepl(repl)
	for _, chg := range chgs {
		rq.allocator.storePool.updateLocalStoreAfterRebalance(
			chg.Target.StoreID, rangeUsageInfo, chg.ChangeType)
	}
	return nil
}

func (rq *replicateQueue) canTransferLease() bool {
	if lastLeaseTransfer := rq.lastLeaseTransfer.Load(); lastLeaseTransfer != nil {
		minInterval := minLeaseTransferInterval.Get(&rq.store.cfg.Settings.SV)
		return timeutil.Since(lastLeaseTransfer.(time.Time)) > minInterval
	}
	return true
}

func (*replicateQueue) timer(_ time.Duration) time.Duration {
	return replicateQueueTimerDuration
}

// purgatoryChan returns the replicate queue's store update channel.
func (rq *replicateQueue) purgatoryChan() <-chan time.Time {
	return rq.updateChan
}

// rangeRaftStatus pretty-prints the Raft progress (i.e. Raft log position) of
// the replicas.
func rangeRaftProgress(raftStatus *raft.Status, replicas []roachpb.ReplicaDescriptor) string {
	if raftStatus == nil {
		return "[no raft status]"
	} else if len(raftStatus.Progress) == 0 {
		return "[no raft progress]"
	}
	var buf bytes.Buffer
	buf.WriteString("[")
	for i, r := range replicas {
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, "%d", r.ReplicaID)
		if uint64(r.ReplicaID) == raftStatus.Lead {
			buf.WriteString("*")
		}
		if progress, ok := raftStatus.Progress[uint64(r.ReplicaID)]; ok {
			fmt.Fprintf(&buf, ":%d", progress.Match)
		} else {
			buf.WriteString(":?")
		}
	}
	buf.WriteString("]")
	return buf.String()
}

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
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const (
	// replicateQueueTimerDuration is the duration between replication of queued
	// replicas.
	replicateQueueTimerDuration = 0 // zero duration to process replication greedily

	// minLeaseTransferInterval controls how frequently leases can be transferred
	// for rebalancing. It does not prevent transferring leases in order to allow
	// a replica to be removed from a range.
	minLeaseTransferInterval = time.Second

	// newReplicaGracePeriod is the amount of time that we allow for a new
	// replica's raft state to catch up to the leader's before we start
	// considering it to be behind for the sake of rebalancing. We choose a
	// large value here because snapshots of large replicas can take a while
	// in high latency clusters, and not allowing enough of a cushion can
	// make rebalance thrashing more likely (#17879).
	newReplicaGracePeriod = 5 * time.Minute
)

var (
	metaReplicateQueueAddReplicaCount = metric.Metadata{
		Name: "queue.replicate.addreplica",
		Help: "Number of replica additions attempted by the replicate queue"}
	metaReplicateQueueRemoveReplicaCount = metric.Metadata{
		Name: "queue.replicate.removereplica",
		Help: "Number of replica removals attempted by the replicate queue (typically in response to a rebalancer-initiated addition)"}
	metaReplicateQueueRemoveDeadReplicaCount = metric.Metadata{
		Name: "queue.replicate.removedeadreplica",
		Help: "Number of dead replica removals attempted by the replicate queue (typically in response to a node outage)"}
	metaReplicateQueueRebalanceReplicaCount = metric.Metadata{
		Name: "queue.replicate.rebalancereplica",
		Help: "Number of replica rebalancer-initiated additions attempted by the replicate queue"}
	metaReplicateQueueTransferLeaseCount = metric.Metadata{
		Name: "queue.replicate.transferlease",
		Help: "Number of range lease transfers attempted by the replicate queue"}
)

// ReplicateQueueMetrics is the set of metrics for the replicate queue.
type ReplicateQueueMetrics struct {
	AddReplicaCount        *metric.Counter
	RemoveReplicaCount     *metric.Counter
	RemoveDeadReplicaCount *metric.Counter
	RebalanceReplicaCount  *metric.Counter
	TransferLeaseCount     *metric.Counter
}

func makeReplicateQueueMetrics() ReplicateQueueMetrics {
	return ReplicateQueueMetrics{
		AddReplicaCount:        metric.NewCounter(metaReplicateQueueAddReplicaCount),
		RemoveReplicaCount:     metric.NewCounter(metaReplicateQueueRemoveReplicaCount),
		RemoveDeadReplicaCount: metric.NewCounter(metaReplicateQueueRemoveDeadReplicaCount),
		RebalanceReplicaCount:  metric.NewCounter(metaReplicateQueueRebalanceReplicaCount),
		TransferLeaseCount:     metric.NewCounter(metaReplicateQueueTransferLeaseCount),
	}
}

// replicateQueue manages a queue of replicas which may need to add an
// additional replica to their range.
type replicateQueue struct {
	*baseQueue
	metrics           ReplicateQueueMetrics
	allocator         Allocator
	updateChan        chan struct{}
	lastLeaseTransfer atomic.Value // read and written by scanner & queue goroutines
}

// newReplicateQueue returns a new instance of replicateQueue.
func newReplicateQueue(store *Store, g *gossip.Gossip, allocator Allocator) *replicateQueue {
	rq := &replicateQueue{
		metrics:    makeReplicateQueueMetrics(),
		allocator:  allocator,
		updateChan: make(chan struct{}, 1),
	}
	store.metrics.registry.AddMetricStruct(&rq.metrics)
	rq.baseQueue = newBaseQueue(
		"replicate", rq, store, g,
		queueConfig{
			maxSize:              defaultQueueMaxSize,
			needsLease:           true,
			needsSystemConfig:    true,
			acceptsUnsplitRanges: store.TestingKnobs().ReplicateQueueAcceptsUnsplit,
			successes:            store.metrics.ReplicateQueueSuccesses,
			failures:             store.metrics.ReplicateQueueFailures,
			pending:              store.metrics.ReplicateQueuePending,
			processingNanos:      store.metrics.ReplicateQueueProcessingNanos,
			purgatory:            store.metrics.ReplicateQueuePurgatory,
		},
	)

	updateFn := func() {
		select {
		case rq.updateChan <- struct{}{}:
		default:
		}
	}

	// Register gossip and node liveness callbacks to signal that
	// replicas in purgatory might be retried.
	if g != nil { // gossip is nil for some unittests
		g.RegisterCallback(gossip.MakePrefixPattern(gossip.KeyStorePrefix), func(_ string, _ roachpb.Value) {
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
	ctx context.Context, now hlc.Timestamp, repl *Replica, sysCfg config.SystemConfig,
) (shouldQ bool, priority float64) {
	if !repl.store.splitQueue.Disabled() && repl.needsSplitBySize() {
		// If the range exceeds the split threshold, let that finish first.
		// Ranges must fit in memory on both sender and receiver nodes while
		// being replicated. This supplements the check provided by
		// acceptsUnsplitRanges, which looks at zone config boundaries rather
		// than data size.
		//
		// This check is ignored if the split queue is disabled, since in that
		// case, the split will never come.
		return
	}

	// Find the zone config for this range.
	desc := repl.Desc()
	zone, err := sysCfg.GetZoneConfigForKey(desc.StartKey)
	if err != nil {
		log.Error(ctx, err)
		return
	}

	rangeInfo := rangeInfoForRepl(repl, desc)
	action, priority := rq.allocator.ComputeAction(ctx, zone, rangeInfo, false)
	if action == AllocatorNoop {
		log.VEventf(ctx, 2, "no action to take")
		return false, 0
	} else if action != AllocatorConsiderRebalance {
		log.VEventf(ctx, 2, "repair needed (%s), enqueuing", action)
		return true, priority
	}

	if !rq.store.TestingKnobs().DisableReplicaRebalancing {
		target, _ := rq.allocator.RebalanceTarget(ctx, zone, repl.RaftStatus(), rangeInfo, storeFilterThrottled, false)
		if target != nil {
			log.VEventf(ctx, 2, "rebalance target found, enqueuing")
			return true, 0
		}
		log.VEventf(ctx, 2, "no rebalance target found, not enqueuing")
	}

	// If the lease is valid, check to see if we should transfer it.
	if lease, _ := repl.GetLease(); repl.IsLeaseValid(lease, now) {
		if rq.canTransferLease() &&
			rq.allocator.ShouldTransferLease(
				ctx, zone, desc.Replicas, lease.Replica.StoreID, desc.RangeID, repl.leaseholderStats) {
			log.VEventf(ctx, 2, "lease transfer needed, enqueuing")
			return true, 0
		}
	}

	return false, 0
}

func (rq *replicateQueue) process(
	ctx context.Context, repl *Replica, sysCfg config.SystemConfig,
) error {
	retryOpts := retry.Options{
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     1 * time.Second,
		Multiplier:     2,
		MaxRetries:     5,
	}

	// Use a retry loop in order to backoff in the case of preemptive
	// snapshot errors, usually signaling that a rebalancing
	// reservation could not be made with the selected target.
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		if requeue, err := rq.processOneChange(ctx, repl, sysCfg, rq.canTransferLease, false /* dryRun */, false /* disableStatsBasedRebalancing */); err != nil {
			if IsSnapshotError(err) {
				// If ChangeReplicas failed because the preemptive snapshot failed, we
				// log the error but then return success indicating we should retry the
				// operation. The most likely causes of the preemptive snapshot failing are
				// a declined reservation or the remote node being unavailable. In either
				// case we don't want to wait another scanner cycle before reconsidering
				// the range.
				log.Info(ctx, err)
				continue
			}
			return err
		} else if requeue {
			// Enqueue this replica again to see if there are more changes to be made.
			rq.MaybeAdd(repl, rq.store.Clock().Now())
		}
		return nil
	}
	return errors.Errorf("failed to replicate after %d retries", retryOpts.MaxRetries)
}

func (rq *replicateQueue) processOneChange(
	ctx context.Context,
	repl *Replica,
	sysCfg config.SystemConfig,
	canTransferLease func() bool,
	dryRun bool,
	disableStatsBasedRebalancing bool,
) (requeue bool, _ error) {
	desc := repl.Desc()

	// Avoid taking action if the range has too many dead replicas to make
	// quorum.
	liveReplicas, deadReplicas := rq.allocator.storePool.liveAndDeadReplicas(desc.RangeID, desc.Replicas)
	{
		quorum := computeQuorum(len(desc.Replicas))
		if lr := len(liveReplicas); lr < quorum {
			return false, errors.Errorf(
				"range requires a replication change, but lacks a quorum of live replicas (%d/%d)", lr, quorum)
		}
	}

	zone, err := sysCfg.GetZoneConfigForKey(desc.StartKey)
	if err != nil {
		return false, err
	}

	rangeInfo := rangeInfoForRepl(repl, desc)
	switch action, _ := rq.allocator.ComputeAction(ctx, zone, rangeInfo, disableStatsBasedRebalancing); action {
	case AllocatorNoop:
		break
	case AllocatorAdd:
		log.VEventf(ctx, 1, "adding a new replica")
		newStore, details, err := rq.allocator.AllocateTarget(
			ctx,
			zone,
			desc.Replicas,
			rangeInfo,
			disableStatsBasedRebalancing,
		)
		if err != nil {
			return false, err
		}
		newReplica := roachpb.ReplicationTarget{
			NodeID:  newStore.Node.NodeID,
			StoreID: newStore.StoreID,
		}

		need := int(zone.NumReplicas)
		willHave := len(desc.Replicas) + 1

		// Only up-replicate if there are suitable allocation targets such
		// that, either the replication goal is met, or it is possible to get to the
		// next odd number of replicas. A consensus group of size 2n has worse
		// failure tolerance properties than a group of size 2n - 1 because it has a
		// larger quorum. For example, up-replicating from 1 to 2 replicas only
		// makes sense if it is possible to be able to go to 3 replicas.
		//
		// NB: If willHave > need, then always allow up-replicating as that will be
		// the case when up-replicating a range with a decommissioning replica.
		if willHave < need && willHave%2 == 0 {
			// This means we are going to up-replicate to an even replica state.
			// Check if it is possible to go to an odd replica state beyond it.
			oldPlusNewReplicas := append([]roachpb.ReplicaDescriptor(nil), desc.Replicas...)
			oldPlusNewReplicas = append(oldPlusNewReplicas, roachpb.ReplicaDescriptor{
				NodeID:  newStore.Node.NodeID,
				StoreID: newStore.StoreID,
			})
			_, _, err := rq.allocator.AllocateTarget(
				ctx,
				zone,
				oldPlusNewReplicas,
				rangeInfo,
				disableStatsBasedRebalancing,
			)
			if err != nil {
				// Does not seem possible to go to the next odd replica state. Return an
				// error so that the operation gets queued into the purgatory.
				return false, errors.Wrap(err, "avoid up-replicating to fragile quorum")
			}
		}
		rq.metrics.AddReplicaCount.Inc(1)
		log.VEventf(ctx, 1, "adding replica %+v due to under-replication: %s",
			newReplica, rangeRaftProgress(repl.RaftStatus(), desc.Replicas))
		if err := rq.addReplica(
			ctx,
			repl,
			newReplica,
			desc,
			SnapshotRequest_RECOVERY,
			ReasonRangeUnderReplicated,
			details,
			dryRun,
		); err != nil {
			return false, err
		}
	case AllocatorRemove:
		log.VEventf(ctx, 1, "removing a replica")
		lastReplAdded, lastAddedTime := repl.LastReplicaAdded()
		if timeutil.Since(lastAddedTime) > newReplicaGracePeriod {
			lastReplAdded = 0
		}
		candidates := filterUnremovableReplicas(repl.RaftStatus(), desc.Replicas, lastReplAdded)
		log.VEventf(ctx, 3, "filtered unremovable replicas from %v to get %v as candidates for removal",
			desc.Replicas, candidates)
		if len(candidates) == 0 {
			return false, errors.Errorf("no removable replicas from range that needs a removal: %s",
				rangeRaftProgress(repl.RaftStatus(), desc.Replicas))
		}
		removeReplica, details, err := rq.allocator.RemoveTarget(ctx, zone, candidates, rangeInfo, disableStatsBasedRebalancing)
		if err != nil {
			return false, err
		}
		if removeReplica.StoreID == repl.store.StoreID() {
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
			transferred, err := rq.transferLease(
				ctx,
				repl,
				desc,
				zone,
				transferLeaseOptions{
					dryRun: dryRun,
				},
			)
			if err != nil {
				return false, err
			}
			// Do not requeue as we transferred our lease away.
			if transferred {
				return false, nil
			}
		} else {
			rq.metrics.RemoveReplicaCount.Inc(1)
			log.VEventf(ctx, 1, "removing replica %+v due to over-replication: %s",
				removeReplica, rangeRaftProgress(repl.RaftStatus(), desc.Replicas))
			target := roachpb.ReplicationTarget{
				NodeID:  removeReplica.NodeID,
				StoreID: removeReplica.StoreID,
			}
			if err := rq.removeReplica(
				ctx, repl, target, desc, ReasonRangeOverReplicated, details, dryRun,
			); err != nil {
				return false, err
			}
		}
	case AllocatorRemoveDecommissioning:
		log.VEventf(ctx, 1, "removing a decommissioning replica")
		decommissioningReplicas := rq.allocator.storePool.decommissioningReplicas(desc.RangeID, desc.Replicas)
		if len(decommissioningReplicas) == 0 {
			log.VEventf(ctx, 1, "range of replica %s was identified as having decommissioning replicas, "+
				"but no decommissioning replicas were found", repl)
			break
		}
		decommissioningReplica := decommissioningReplicas[0]
		if decommissioningReplica.StoreID == repl.store.StoreID() {
			// As in the AllocatorRemove case, if we're trying to remove ourselves, we
			// we must first transfer our lease away.
			if dryRun {
				return false, nil
			}
			transferred, err := rq.transferLease(
				ctx,
				repl,
				desc,
				zone,
				transferLeaseOptions{
					dryRun: dryRun,
				},
			)
			if err != nil {
				return false, err
			}
			// Do not requeue as we transferred our lease away.
			if transferred {
				return false, nil
			}
		} else {
			rq.metrics.RemoveReplicaCount.Inc(1)
			log.VEventf(ctx, 1, "removing decommissioning replica %+v from store", decommissioningReplica)
			target := roachpb.ReplicationTarget{
				NodeID:  decommissioningReplica.NodeID,
				StoreID: decommissioningReplica.StoreID,
			}
			if err := rq.removeReplica(
				ctx, repl, target, desc, ReasonStoreDecommissioning, "", dryRun,
			); err != nil {
				return false, err
			}
		}
	case AllocatorRemoveDead:
		log.VEventf(ctx, 1, "removing a dead replica")
		if len(deadReplicas) == 0 {
			log.VEventf(ctx, 1, "range of replica %s was identified as having dead replicas, but no dead replicas were found", repl)
			break
		}
		deadReplica := deadReplicas[0]
		rq.metrics.RemoveDeadReplicaCount.Inc(1)
		log.VEventf(ctx, 1, "removing dead replica %+v from store", deadReplica)
		target := roachpb.ReplicationTarget{
			NodeID:  deadReplica.NodeID,
			StoreID: deadReplica.StoreID,
		}
		if err := rq.removeReplica(
			ctx, repl, target, desc, ReasonStoreDead, "", dryRun,
		); err != nil {
			return false, err
		}
	case AllocatorConsiderRebalance:
		// The Noop case will result if this replica was queued in order to
		// rebalance. Attempt to find a rebalancing target.
		log.VEventf(ctx, 1, "allocator noop - considering a rebalance or lease transfer")

		if !rq.store.TestingKnobs().DisableReplicaRebalancing {
			rebalanceStore, details := rq.allocator.RebalanceTarget(
				ctx, zone, repl.RaftStatus(), rangeInfo, storeFilterThrottled, disableStatsBasedRebalancing)
			if rebalanceStore == nil {
				log.VEventf(ctx, 1, "no suitable rebalance target")
			} else {
				rebalanceReplica := roachpb.ReplicationTarget{
					NodeID:  rebalanceStore.Node.NodeID,
					StoreID: rebalanceStore.StoreID,
				}
				rq.metrics.RebalanceReplicaCount.Inc(1)
				log.VEventf(ctx, 1, "rebalancing to %+v: %s",
					rebalanceReplica, rangeRaftProgress(repl.RaftStatus(), desc.Replicas))
				if err := rq.addReplica(
					ctx,
					repl,
					rebalanceReplica,
					desc,
					SnapshotRequest_REBALANCE,
					ReasonRebalance,
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
			transferred, err := rq.transferLease(
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

	return true, nil
}

type transferLeaseOptions struct {
	checkTransferLeaseSource bool
	checkCandidateFullness   bool
	dryRun                   bool
}

func (rq *replicateQueue) transferLease(
	ctx context.Context,
	repl *Replica,
	desc *roachpb.RangeDescriptor,
	zone config.ZoneConfig,
	opts transferLeaseOptions,
) (bool, error) {
	candidates := filterBehindReplicas(repl.RaftStatus(), desc.Replicas, 0 /* brandNewReplicaID */)
	if target := rq.allocator.TransferLeaseTarget(
		ctx,
		zone,
		candidates,
		repl.store.StoreID(),
		desc.RangeID,
		repl.leaseholderStats,
		opts.checkTransferLeaseSource,
		opts.checkCandidateFullness,
		false, /* alwaysAllowDecisionWithoutStats */
	); target != (roachpb.ReplicaDescriptor{}) {
		rq.metrics.TransferLeaseCount.Inc(1)
		log.VEventf(ctx, 1, "transferring lease to s%d", target.StoreID)
		if opts.dryRun {
			return false, nil
		}
		if err := repl.AdminTransferLease(ctx, target.StoreID); err != nil {
			return false, errors.Wrapf(err, "%s: unable to transfer lease to s%d", repl, target.StoreID)
		}
		rq.lastLeaseTransfer.Store(timeutil.Now())
		return true, nil
	}
	return false, nil
}

func (rq *replicateQueue) addReplica(
	ctx context.Context,
	repl *Replica,
	target roachpb.ReplicationTarget,
	desc *roachpb.RangeDescriptor,
	priority SnapshotRequest_Priority,
	reason RangeLogEventReason,
	details string,
	dryRun bool,
) error {
	if dryRun {
		return nil
	}
	if err := repl.changeReplicas(ctx, roachpb.ADD_REPLICA, target, desc, priority, reason, details); err != nil {
		return err
	}
	rangeInfo := rangeInfoForRepl(repl, desc)
	rq.allocator.storePool.updateLocalStoreAfterRebalance(target.StoreID, rangeInfo, roachpb.ADD_REPLICA)
	return nil
}

func (rq *replicateQueue) removeReplica(
	ctx context.Context,
	repl *Replica,
	target roachpb.ReplicationTarget,
	desc *roachpb.RangeDescriptor,
	reason RangeLogEventReason,
	details string,
	dryRun bool,
) error {
	if dryRun {
		return nil
	}
	if err := repl.ChangeReplicas(ctx, roachpb.REMOVE_REPLICA, target, desc, reason, details); err != nil {
		return err
	}
	rangeInfo := rangeInfoForRepl(repl, desc)
	rq.allocator.storePool.updateLocalStoreAfterRebalance(target.StoreID, rangeInfo, roachpb.REMOVE_REPLICA)
	return nil
}

func (rq *replicateQueue) canTransferLease() bool {
	if lastLeaseTransfer := rq.lastLeaseTransfer.Load(); lastLeaseTransfer != nil {
		return timeutil.Since(lastLeaseTransfer.(time.Time)) > minLeaseTransferInterval
	}
	return true
}

func (*replicateQueue) timer(_ time.Duration) time.Duration {
	return replicateQueueTimerDuration
}

// purgatoryChan returns the replicate queue's store update channel.
func (rq *replicateQueue) purgatoryChan() <-chan struct{} {
	return rq.updateChan
}

// rangeRaftStatus pretty-prints the Raft progress (i.e. Raft log position) of
// the replicas.
func rangeRaftProgress(raftStatus *raft.Status, replicas []roachpb.ReplicaDescriptor) string {
	if raftStatus == nil || len(raftStatus.Progress) == 0 {
		return "[raft progress unknown]"
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

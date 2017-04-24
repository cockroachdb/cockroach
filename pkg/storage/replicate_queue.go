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
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

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
	clock             *hlc.Clock
	updateChan        chan struct{}
	lastLeaseTransfer atomic.Value // read and written by scanner & queue goroutines
}

// newReplicateQueue returns a new instance of replicateQueue.
func newReplicateQueue(
	store *Store, g *gossip.Gossip, allocator Allocator, clock *hlc.Clock,
) *replicateQueue {
	rq := &replicateQueue{
		metrics:    makeReplicateQueueMetrics(),
		allocator:  allocator,
		clock:      clock,
		updateChan: make(chan struct{}, 1),
	}
	store.metrics.registry.AddMetricStruct(&rq.metrics)
	rq.baseQueue = newBaseQueue(
		"replicate", rq, store, g,
		queueConfig{
			maxSize:              defaultQueueMaxSize,
			needsLease:           true,
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

	// Register a gossip and node liveness callbacks to signal queue
	// that replicas in purgatory might be retried.
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

	action, priority := rq.allocator.ComputeAction(ctx, zone, desc)
	if action != AllocatorNoop {
		if log.V(2) {
			log.Infof(ctx, "repair needed (%s), enqueuing", action)
		}
		return true, priority
	}

	// If the lease is valid, check to see if we should transfer it.
	if lease, _ := repl.getLease(); lease != nil && repl.IsLeaseValid(lease, now) {
		if rq.canTransferLease() &&
			rq.allocator.ShouldTransferLease(
				ctx, zone.Constraints, desc.Replicas, lease.Replica.StoreID, desc.RangeID, repl.stats) {
			if log.V(2) {
				log.Infof(ctx, "lease transfer needed, enqueuing")
			}
			return true, 0
		}
	}

	// Check for a rebalancing opportunity. Note that leaseStoreID will be 0 if
	// the range doesn't currently have a lease which will allow the current
	// replica to be considered a rebalancing source.
	target, err := rq.allocator.RebalanceTarget(
		ctx,
		zone.Constraints,
		desc.Replicas,
		desc.RangeID,
	)
	if err != nil {
		log.ErrEventf(ctx, "rebalance target failed: %s", err)
		return false, 0
	}
	if log.V(2) {
		if target != nil {
			log.Infof(ctx, "rebalance target found, enqueuing")
		} else {
			log.Infof(ctx, "no rebalance target found, not enqueuing")
		}
	}
	return target != nil, 0
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
	// snapshot errors, usually signalling that a rebalancing
	// reservation could not be made with the selected target.
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		if requeue, err := rq.processOneChange(ctx, repl, sysCfg); err != nil {
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
			rq.MaybeAdd(repl, rq.clock.Now())
		}
		return nil
	}
	return errors.Errorf("failed to replicate %s after %d retries", repl, retryOpts.MaxRetries)
}

func (rq *replicateQueue) processOneChange(
	ctx context.Context, repl *Replica, sysCfg config.SystemConfig,
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

	switch action, _ := rq.allocator.ComputeAction(ctx, zone, desc); action {
	case AllocatorAdd:
		if log.V(1) {
			log.Infof(ctx, "adding a new replica")
		}
		newStore, err := rq.allocator.AllocateTarget(
			ctx,
			zone.Constraints,
			desc.Replicas,
			desc.RangeID,
			true, /* relaxConstraints */
		)
		if err != nil {
			return false, err
		}
		newReplica := roachpb.ReplicationTarget{
			NodeID:  newStore.Node.NodeID,
			StoreID: newStore.StoreID,
		}

		rq.metrics.AddReplicaCount.Inc(1)
		if log.V(1) {
			log.Infof(ctx, "adding replica to %+v due to under-replication", newReplica)
		}
		if err := rq.addReplica(
			ctx, repl, newReplica, desc, SnapshotRequest_RECOVERY); err != nil {
			return false, err
		}
	case AllocatorRemove:
		if log.V(1) {
			log.Infof(ctx, "removing a replica")
		}
		removeReplica, err := rq.allocator.RemoveTarget(
			ctx,
			zone.Constraints,
			desc.Replicas,
		)
		if err != nil {
			return false, err
		}
		if removeReplica.StoreID == repl.store.StoreID() {
			// The local replica was selected as the removal target, but that replica
			// is the leaseholder, so transfer the lease instead. We don't check that
			// the current store has too many leases in this case under the
			// assumption that replica balance is a greater concern. Also note that
			// AllocatorRemove action takes preference over AllocatorNoop
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
				false, /* checkTransferLeaseSource */
				false, /* checkCandidateFullness */
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
			if log.V(1) {
				log.Infof(ctx, "removing replica %+v due to over-replication", removeReplica)
			}
			target := roachpb.ReplicationTarget{
				NodeID:  removeReplica.NodeID,
				StoreID: removeReplica.StoreID,
			}
			if err := rq.removeReplica(ctx, repl, target, desc); err != nil {
				return false, err
			}
		}
	case AllocatorRemoveDead:
		if log.V(1) {
			log.Infof(ctx, "removing a dead replica")
		}
		if len(deadReplicas) == 0 {
			if log.V(1) {
				log.Warningf(ctx, "range of replica %s was identified as having dead replicas, but no dead replicas were found", repl)
			}
			break
		}
		deadReplica := deadReplicas[0]
		rq.metrics.RemoveDeadReplicaCount.Inc(1)
		if log.V(1) {
			log.Infof(ctx, "removing dead replica %+v from store", deadReplica)
		}
		target := roachpb.ReplicationTarget{
			NodeID:  deadReplica.NodeID,
			StoreID: deadReplica.StoreID,
		}
		if err := rq.removeReplica(ctx, repl, target, desc); err != nil {
			return false, err
		}
	case AllocatorNoop:
		// The Noop case will result if this replica was queued in order to
		// rebalance. Attempt to find a rebalancing target.
		if log.V(1) {
			log.Infof(ctx, "considering a rebalance")
		}

		if rq.canTransferLease() {
			// We require the lease in order to process replicas, so
			// repl.store.StoreID() corresponds to the lease-holder's store ID.
			transferred, err := rq.transferLease(
				ctx,
				repl,
				desc,
				zone,
				true, /* checkTransferLeaseSource */
				true, /* checkCandidateFullness */
			)
			if err != nil {
				return false, err
			}
			// Do not requeue as we transferred our lease away.
			if transferred {
				return false, nil
			}
		}

		rebalanceStore, err := rq.allocator.RebalanceTarget(
			ctx,
			zone.Constraints,
			desc.Replicas,
			desc.RangeID,
		)
		if err != nil {
			log.ErrEventf(ctx, "rebalance target failed %s", err)
			return false, nil
		}
		if rebalanceStore == nil {
			if log.V(1) {
				log.Infof(ctx, "no suitable rebalance target")
			}
			// No action was necessary and no rebalance target was found. Return
			// without re-queuing this replica.
			return false, nil
		}
		rebalanceReplica := roachpb.ReplicationTarget{
			NodeID:  rebalanceStore.Node.NodeID,
			StoreID: rebalanceStore.StoreID,
		}
		rq.metrics.RebalanceReplicaCount.Inc(1)
		if log.V(1) {
			log.Infof(ctx, "rebalancing to %+v", rebalanceReplica)
		}
		if err := rq.addReplica(
			ctx, repl, rebalanceReplica, desc, SnapshotRequest_REBALANCE); err != nil {
			return false, err
		}
	}

	return true, nil
}

func (rq *replicateQueue) transferLease(
	ctx context.Context,
	repl *Replica,
	desc *roachpb.RangeDescriptor,
	zone config.ZoneConfig,
	checkTransferLeaseSource bool,
	checkCandidateFullness bool,
) (bool, error) {
	candidates := filterBehindReplicas(repl.RaftStatus(), desc.Replicas)
	if target := rq.allocator.TransferLeaseTarget(
		ctx,
		zone.Constraints,
		candidates,
		repl.store.StoreID(),
		desc.RangeID,
		repl.stats,
		checkTransferLeaseSource,
		checkCandidateFullness,
	); target != (roachpb.ReplicaDescriptor{}) {
		rq.metrics.TransferLeaseCount.Inc(1)
		if log.V(1) {
			log.Infof(ctx, "transferring lease to s%d", target.StoreID)
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
) error {
	return repl.changeReplicas(ctx, roachpb.ADD_REPLICA, target, desc, priority)
}

func (rq *replicateQueue) removeReplica(
	ctx context.Context,
	repl *Replica,
	target roachpb.ReplicationTarget,
	desc *roachpb.RangeDescriptor,
) error {
	return repl.ChangeReplicas(ctx, roachpb.REMOVE_REPLICA, target, desc)
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

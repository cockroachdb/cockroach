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

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

const (
	// replicateQueueMaxSize is the max size of the replicate queue.
	replicateQueueMaxSize = 100

	// replicateQueueTimerDuration is the duration between replication of queued
	// replicas.
	replicateQueueTimerDuration = 0 // zero duration to process replication greedily
)

// replicateQueue manages a queue of replicas which may need to add an
// additional replica to their range.
type replicateQueue struct {
	*baseQueue
	allocator  Allocator
	clock      *hlc.Clock
	updateChan chan struct{}
}

// newReplicateQueue returns a new instance of replicateQueue.
func newReplicateQueue(
	store *Store, g *gossip.Gossip, allocator Allocator, clock *hlc.Clock, options AllocatorOptions,
) *replicateQueue {
	rq := &replicateQueue{
		allocator:  allocator,
		clock:      clock,
		updateChan: make(chan struct{}, 1),
	}
	rq.baseQueue = newBaseQueue(
		"replicate", rq, store, g,
		queueConfig{
			maxSize:              replicateQueueMaxSize,
			needsLease:           true,
			acceptsUnsplitRanges: store.TestingKnobs().ReplicateQueueAcceptsUnsplit,
			successes:            store.metrics.ReplicateQueueSuccesses,
			failures:             store.metrics.ReplicateQueueFailures,
			pending:              store.metrics.ReplicateQueuePending,
			processingNanos:      store.metrics.ReplicateQueueProcessingNanos,
			purgatory:            store.metrics.ReplicateQueuePurgatory,
		},
	)

	if g != nil { // gossip is nil for some unittests
		// Register a gossip callback to signal queue that replicas in
		// purgatory might be retried due to new store gossip.
		g.RegisterCallback(gossip.MakePrefixPattern(gossip.KeyStorePrefix), func(_ string, _ roachpb.Value) {
			select {
			case rq.updateChan <- struct{}{}:
			default:
			}
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

	action, priority := rq.allocator.ComputeAction(zone, desc)
	if action != AllocatorNoop {
		if log.V(2) {
			log.Infof(ctx, "%s repair needed (%s), enqueuing", repl, action)
		}
		return true, priority
	}
	// See if there is a rebalancing opportunity present.
	leaseStoreID := repl.store.StoreID()
	if lease, _ := repl.getLease(); lease != nil {
		leaseStoreID = lease.Replica.StoreID
	}
	if rq.allocator.TransferLeaseSource(
		zone.Constraints, leaseStoreID, desc.RangeID) {
		if log.V(2) {
			log.Infof(ctx, "%s lease transfer needed, enqueuing", repl)
		}
		return true, 0
	}
	target := rq.allocator.RebalanceTarget(
		zone.Constraints,
		desc.Replicas,
		leaseStoreID,
		desc.RangeID,
	)
	if log.V(2) {
		if target != nil {
			log.Infof(ctx, "%s rebalance target found, enqueuing", repl)
		} else {
			log.Infof(ctx, "%s no rebalance target found, not enqueuing", repl)
		}
	}
	return target != nil, 0
}

func (rq *replicateQueue) process(
	ctx context.Context, now hlc.Timestamp, repl *Replica, sysCfg config.SystemConfig,
) error {
	desc := repl.Desc()
	// Find the zone config for this range.
	zone, err := sysCfg.GetZoneConfigForKey(desc.StartKey)
	if err != nil {
		return err
	}
	action, _ := rq.allocator.ComputeAction(zone, desc)

	// Avoid taking action if the range has too many dead replicas to make
	// quorum.
	deadReplicas := rq.allocator.storePool.deadReplicas(desc.RangeID, desc.Replicas)
	quorum := computeQuorum(len(desc.Replicas))
	liveReplicaCount := len(desc.Replicas) - len(deadReplicas)
	if liveReplicaCount < quorum {
		return errors.Errorf("range requires a replication change, but lacks a quorum of live nodes.")
	}

	switch action {
	case AllocatorAdd:
		log.Event(ctx, "adding a new replica")
		newStore, err := rq.allocator.AllocateTarget(
			zone.Constraints,
			desc.Replicas,
			desc.RangeID,
			true,
		)
		if err != nil {
			return err
		}
		newReplica := roachpb.ReplicaDescriptor{
			NodeID:  newStore.Node.NodeID,
			StoreID: newStore.StoreID,
		}

		log.VEventf(ctx, 1, "adding replica to %+v due to under-replication", newReplica)
		if err = repl.ChangeReplicas(ctx, roachpb.ADD_REPLICA, newReplica, desc); err != nil {
			return err
		}
	case AllocatorRemove:
		log.Event(ctx, "removing a replica")
		removeReplica, err := rq.allocator.RemoveTarget(desc.Replicas, 0)
		if err != nil {
			return err
		}
		// We require the lease in order to process replicas, so
		// repl.store.StoreID() corresponds to the lease-holder's store ID.
		if removeReplica.StoreID == repl.store.StoreID() {
			// The local replica was selected as the removal target, but that replica
			// is the leaseholder, so transfer the lease instead. We don't check that
			// the current store has too many leases in this case under the
			// assumption that replica balance is a greater concern. Also note that
			// AllocatorRemove action takes preference over AllocatorNoop
			// (rebalancing) which is where lease transfer would otherwise occur. We
			// need to be able to transfer leases in AllocatorRemove in order to get
			// out of situations where this store is overfull and yet holds all the
			// leases.
			target := rq.allocator.TransferLeaseTarget(
				zone.Constraints, desc.Replicas, repl.store.StoreID(), desc.RangeID,
				false /* check-transfer-lease-source */)
			if target.StoreID != 0 {
				log.VEventf(ctx, 1, "transferring lease to s%d", target.StoreID)
				if err := repl.AdminTransferLease(target.StoreID); err != nil {
					return errors.Wrapf(err, "%s: unable to transfer lease", repl)
				}
				// Do not requeue as we transferred our lease away.
				return nil
			}
		} else {
			log.VEventf(ctx, 1, "removing replica %+v due to over-replication", removeReplica)
			if err = repl.ChangeReplicas(ctx, roachpb.REMOVE_REPLICA, removeReplica, desc); err != nil {
				return err
			}
		}
	case AllocatorRemoveDead:
		log.Event(ctx, "removing a dead replica")
		if len(deadReplicas) == 0 {
			if log.V(1) {
				log.Warningf(ctx, "Range of replica %s was identified as having dead replicas, but no dead replicas were found.", repl)
			}
			break
		}
		deadReplica := deadReplicas[0]
		log.VEventf(ctx, 1, "removing dead replica %+v from store", deadReplica)
		if err = repl.ChangeReplicas(ctx, roachpb.REMOVE_REPLICA, deadReplica, desc); err != nil {
			return err
		}
	case AllocatorNoop:
		// The Noop case will result if this replica was queued in order to
		// rebalance. Attempt to find a rebalancing target.
		log.Event(ctx, "considering a rebalance")

		// We require the lease in order to process replicas, so
		// repl.store.StoreID() corresponds to the lease-holder's store ID.
		target := rq.allocator.TransferLeaseTarget(
			zone.Constraints, desc.Replicas, repl.store.StoreID(), desc.RangeID,
			true /* check-transfer-lease-source */)
		if target.StoreID != 0 {
			log.VEventf(ctx, 1, "transferring lease to s%d", target.StoreID)
			if err := repl.AdminTransferLease(target.StoreID); err != nil {
				return errors.Wrapf(err, "%s: unable to transfer lease", repl)
			}
			// Do not requeue as we transferred our lease away.
			return nil
		}

		rebalanceStore := rq.allocator.RebalanceTarget(
			zone.Constraints,
			desc.Replicas,
			repl.store.StoreID(),
			desc.RangeID,
		)
		if rebalanceStore == nil {
			log.VEventf(ctx, 1, "no suitable rebalance target")
			// No action was necessary and no rebalance target was found. Return
			// without re-queuing this replica.
			return nil
		}
		rebalanceReplica := roachpb.ReplicaDescriptor{
			NodeID:  rebalanceStore.Node.NodeID,
			StoreID: rebalanceStore.StoreID,
		}
		log.VEventf(ctx, 1, "rebalancing to %+v", rebalanceReplica)
		if err = repl.ChangeReplicas(ctx, roachpb.ADD_REPLICA, rebalanceReplica, desc); err != nil {
			return err
		}
	}

	// Enqueue this replica again to see if there are more changes to be made.
	rq.MaybeAdd(repl, rq.clock.Now())
	return nil
}

func (*replicateQueue) timer() time.Duration {
	return replicateQueueTimerDuration
}

// purgatoryChan returns the replicate queue's store update channel.
func (rq *replicateQueue) purgatoryChan() <-chan struct{} {
	return rq.updateChan
}

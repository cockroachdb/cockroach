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
	"fmt"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
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
	baseQueue
	allocator  Allocator
	clock      *hlc.Clock
	updateChan chan struct{}
}

// newReplicateQueue returns a new instance of replicateQueue.
func newReplicateQueue(g *gossip.Gossip, allocator Allocator, clock *hlc.Clock,
	options AllocatorOptions) *replicateQueue {
	rq := &replicateQueue{
		allocator:  allocator,
		clock:      clock,
		updateChan: make(chan struct{}, 1),
	}
	rq.baseQueue = makeBaseQueue("replicate", rq, g, queueConfig{
		maxSize:              replicateQueueMaxSize,
		needsLease:           true,
		acceptsUnsplitRanges: false,
	})

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
	now hlc.Timestamp,
	repl *Replica,
	sysCfg config.SystemConfig,
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
		log.Error(err)
		return
	}

	action, priority := rq.allocator.ComputeAction(*zone, desc)
	if action != AllocatorNoop {
		return true, priority
	}
	// See if there is a rebalancing opportunity present.
	shouldRebalance := rq.allocator.ShouldRebalance(repl.store.StoreID())
	return shouldRebalance, 0
}

func (rq *replicateQueue) process(
	ctx context.Context,
	now hlc.Timestamp,
	repl *Replica,
	sysCfg config.SystemConfig,
) error {
	desc := repl.Desc()
	// Find the zone config for this range.
	zone, err := sysCfg.GetZoneConfigForKey(desc.StartKey)
	if err != nil {
		return err
	}
	action, _ := rq.allocator.ComputeAction(*zone, desc)

	// Avoid taking action if the range has too many dead replicas to make
	// quorum.
	deadReplicas := rq.allocator.storePool.deadReplicas(desc.Replicas)
	quorum := computeQuorum(len(desc.Replicas))
	liveReplicaCount := len(desc.Replicas) - len(deadReplicas)
	if liveReplicaCount < quorum {
		return errors.Errorf("range requires a replication change, but lacks a quorum of live nodes.")
	}

	switch action {
	case AllocatorAdd:
		log.Trace(ctx, "adding a new replica")
		newStore, err := rq.allocator.AllocateTarget(zone.ReplicaAttrs[0], desc.Replicas, true, nil)
		if err != nil {
			return err
		}
		newReplica := roachpb.ReplicaDescriptor{
			NodeID:  newStore.Node.NodeID,
			StoreID: newStore.StoreID,
		}

		if log.V(1) {
			log.Infof("range %d: adding replica to %+v due to under-replication", repl.RangeID, newReplica)
		}
		log.Trace(ctx, fmt.Sprintf("adding replica to %+v due to under-replication", newReplica))
		if err = repl.ChangeReplicas(ctx, roachpb.ADD_REPLICA, newReplica, desc); err != nil {
			return err
		}
	case AllocatorRemove:
		log.Trace(ctx, "removing a replica")
		removeReplica, err := rq.allocator.RemoveTarget(desc.Replicas)
		if err != nil {
			return err
		}
		// Check to make sure we don't try to remove the replica with the range
		// lease.
		lease, _ := repl.getLease()
		if lease == nil {
			return errors.Errorf("range %d: could not get current lease", repl.RangeID)
		}
		if lease.OwnedBy(removeReplica.StoreID) {
			return errors.Errorf("range %d: can't remove the replica with the current range lease", repl.RangeID)
		}
		if log.V(1) {
			log.Infof("range %d: removing replica %+v due to over-replication", repl.RangeID, removeReplica)
		}
		log.Trace(ctx, fmt.Sprintf("removing replica %+v due to over-replication", removeReplica))
		if err = repl.ChangeReplicas(ctx, roachpb.REMOVE_REPLICA, removeReplica, desc); err != nil {
			return err
		}
		// Do not requeue if we removed ourselves.
		if removeReplica.StoreID == repl.store.StoreID() {
			return nil
		}
	case AllocatorRemoveDead:
		log.Trace(ctx, "removing a dead replica")
		if len(deadReplicas) == 0 {
			if log.V(1) {
				log.Warningf("Range of replica %s was identified as having dead replicas, but no dead replicas were found.", repl)
			}
			break
		}
		deadReplica := deadReplicas[0]
		if log.V(1) {
			log.Infof("range %d: removing replica %+v from dead store", repl.RangeID, deadReplica)
		}
		log.Trace(ctx, fmt.Sprintf("removing replica %+v from dead store", deadReplica))
		if err = repl.ChangeReplicas(ctx, roachpb.REMOVE_REPLICA, deadReplica, desc); err != nil {
			return err
		}
	case AllocatorNoop:
		log.Trace(ctx, "considering a rebalance")
		// The Noop case will result if this replica was queued in order to
		// rebalance. Attempt to find a rebalancing target.
		rebalanceStore := rq.allocator.RebalanceTarget(repl.store.StoreID(), zone.ReplicaAttrs[0], desc.Replicas)
		if rebalanceStore == nil {
			if log.V(1) {
				log.Infof("range %d: no suitable rebalance target", repl.RangeID)
			}
			log.Trace(ctx, "no suitable rebalance target")
			// No action was necessary and no rebalance target was found. Return
			// without re-queuing this replica.
			return nil
		}
		rebalanceReplica := roachpb.ReplicaDescriptor{
			NodeID:  rebalanceStore.Node.NodeID,
			StoreID: rebalanceStore.StoreID,
		}
		if log.V(1) {
			log.Infof("range %d: rebalancing to %+v", repl.RangeID, rebalanceReplica)
		}
		log.Trace(ctx, fmt.Sprintf("rebalancing to %+v", rebalanceReplica))
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

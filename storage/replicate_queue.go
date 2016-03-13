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

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
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
	rq.baseQueue = makeBaseQueue("replicate", rq, g, replicateQueueMaxSize)

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

func (*replicateQueue) needsLeaderLease() bool {
	return true
}

// acceptsUnsplitRanges is false because the proper replication
// policy cannot be determined for ranges that span zone configs.
func (*replicateQueue) acceptsUnsplitRanges() bool {
	return false
}

func (rq *replicateQueue) shouldQueue(now roachpb.Timestamp, repl *Replica,
	sysCfg config.SystemConfig) (shouldQ bool, priority float64) {

	desc := repl.Desc()
	if len(sysCfg.ComputeSplitKeys(desc.StartKey, desc.EndKey)) > 0 {
		// If the replica's range needs splitting, wait until done.
		return
	}

	// Find the zone config for this range.
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

func (rq *replicateQueue) process(now roachpb.Timestamp, repl *Replica, sysCfg config.SystemConfig) error {
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
		return util.Errorf("range requires a replication change, but lacks a quorum of live nodes.")
	}

	switch action {
	case AllocatorAdd:
		newStore, err := rq.allocator.AllocateTarget(zone.ReplicaAttrs[0], desc.Replicas, true, nil)
		if err != nil {
			return err
		}
		newReplica := roachpb.ReplicaDescriptor{
			NodeID:  newStore.Node.NodeID,
			StoreID: newStore.StoreID,
		}
		if err = repl.ChangeReplicas(roachpb.ADD_REPLICA, newReplica, desc); err != nil {
			return err
		}
	case AllocatorRemove:
		removeReplica, err := rq.allocator.RemoveTarget(desc.Replicas)
		if err != nil {
			return err
		}
		if err = repl.ChangeReplicas(roachpb.REMOVE_REPLICA, removeReplica, desc); err != nil {
			return err
		}
		// Do not requeue if we removed ourselves.
		if removeReplica.StoreID == repl.store.StoreID() {
			return nil
		}
	case AllocatorRemoveDead:
		if len(deadReplicas) == 0 {
			if log.V(1) {
				log.Warningf("Range of replica %s was identified as having dead replicas, but no dead replicas were found.", repl)
			}
			break
		}
		if err = repl.ChangeReplicas(roachpb.REMOVE_REPLICA, deadReplicas[0], desc); err != nil {
			return err
		}
	case AllocatorNoop:
		// The Noop case will result if this replica was queued in order to
		// rebalance. Attempt to find a rebalancing target.
		rebalanceStore := rq.allocator.RebalanceTarget(repl.store.StoreID(), zone.ReplicaAttrs[0], desc.Replicas)
		if rebalanceStore == nil {
			// No action was necessary and no rebalance target was found. Return
			// without re-queuing this replica.
			return nil
		}
		rebalanceReplica := roachpb.ReplicaDescriptor{
			NodeID:  rebalanceStore.Node.NodeID,
			StoreID: rebalanceStore.StoreID,
		}
		if err = repl.ChangeReplicas(roachpb.ADD_REPLICA, rebalanceReplica, desc); err != nil {
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

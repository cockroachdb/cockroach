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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Ben Darnell

package storage

import (
	"time"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/proto"
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
	*baseQueue
	gossip    *gossip.Gossip
	allocator Allocator
	clock     *hlc.Clock
}

// makeReplicateQueue returns a new instance of replicateQueue.
func makeReplicateQueue(gossip *gossip.Gossip, allocator Allocator, clock *hlc.Clock) replicateQueue {
	rq := replicateQueue{
		gossip:    gossip,
		allocator: allocator,
		clock:     clock,
	}
	// rq must be a pointer in order to setup the reference cycle.
	rq.baseQueue = newBaseQueue("replicate", &rq, replicateQueueMaxSize)
	return rq
}

func (rq replicateQueue) needsLeaderLease() bool {
	return true
}

func (rq replicateQueue) shouldQueue(now proto.Timestamp, repl *Replica) (shouldQ bool, priority float64) {
	// If the replica's range spans multiple zones, ignore it until the split
	// queue has processed it.
	if len(computeSplitKeys(rq.gossip, repl)) > 0 {
		return
	}

	// Load the zone config to find the desired replica attributes.
	zone, err := lookupZoneConfig(rq.gossip, repl)
	if err != nil {
		log.Error(err)
		return
	}

	action, priority := rq.allocator.ComputeAction(zone, repl.Desc())
	if action == aaNoop {
		return false, 0
	}
	return true, priority
}

func (rq replicateQueue) process(now proto.Timestamp, repl *Replica) error {
	zone, err := lookupZoneConfig(rq.gossip, repl)
	if err != nil {
		return err
	}

	desc := repl.Desc()
	action, _ := rq.allocator.ComputeAction(zone, desc)
	if action == aaNoop {
		// No action to take, return without re-queueing.
		return nil
	}

	// Avoid taking action if the range has too many dead replicas to make
	// quorum.
	deadReplicas := rq.allocator.storePool.deadReplicas(desc.Replicas)
	quorum := computeQuorum(len(desc.Replicas))
	liveReplicaCount := len(desc.Replicas) - len(deadReplicas)
	if liveReplicaCount < quorum {
		return util.Errorf("range requires a replication change, but lacks a quorum of live nodes.")
	}

	switch action {
	case aaAdd:
		newStore, err := rq.allocator.AllocateTarget(zone.ReplicaAttrs[0], desc.Replicas, true, nil)
		if err != nil {
			return err
		}
		newReplica := proto.Replica{
			NodeID:  newStore.Node.NodeID,
			StoreID: newStore.StoreID,
		}
		if err = repl.ChangeReplicas(proto.ADD_REPLICA, newReplica, desc); err != nil {
			return err
		}
	case aaRemove:
		removeReplica, err := rq.allocator.RemoveTarget(desc.Replicas)
		if err != nil {
			return err
		}
		if err = repl.ChangeReplicas(proto.REMOVE_REPLICA, removeReplica, desc); err != nil {
			return err
		}
	case aaRemoveDead:
		if len(deadReplicas) == 0 {
			if log.V(1) {
				log.Warningf("Range of replica %s was identified as having dead replicas, but no dead replicas were found.", repl)
			}
			break
		}
		if err = repl.ChangeReplicas(proto.REMOVE_REPLICA, deadReplicas[0], desc); err != nil {
			return err
		}
	}

	// Enqueue this replica again to see if there are more changes to be made.
	rq.MaybeAdd(repl, rq.clock.Now())
	return nil
}

func (rq replicateQueue) timer() time.Duration {
	return replicateQueueTimerDuration
}

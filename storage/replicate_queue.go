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

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/proto"
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
	allocator allocator
	clock     *hlc.Clock
}

// makeReplicateQueue returns a new instance of replicateQueue.
func makeReplicateQueue(gossip *gossip.Gossip, allocator allocator, clock *hlc.Clock) replicateQueue {
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

	delta := rq.replicaDelta(zone, repl, repl.Desc())
	if delta == 0 {
		if log.V(1) {
			log.Infof("%s has the correct number of nodes", repl)
		}
		return false, 0
	}
	if delta > 0 {
		if log.V(1) {
			log.Infof("%s needs to add %d nodes", repl, delta)
		}
		// For ranges which need additional replicas, increase the priority
		return true, float64(delta + 10)
	}
	if log.V(1) {
		log.Infof("%s needs to remove %d nodes", repl, 0-delta)
	}
	// For ranges which have too many replicas, priority is absolute value of
	// the delta.
	return true, float64(0 - delta)
}

func (rq *replicateQueue) replicaDelta(zone config.ZoneConfig, repl *Replica,
	desc *proto.RangeDescriptor) int {
	// TODO(bdarnell): handle non-empty ReplicaAttrs.
	need := len(zone.ReplicaAttrs)
	have := len(desc.Replicas)

	return need - have
}

func (rq replicateQueue) process(now proto.Timestamp, repl *Replica) error {
	zone, err := lookupZoneConfig(rq.gossip, repl)
	if err != nil {
		return err
	}

	desc := repl.Desc()
	delta := rq.replicaDelta(zone, repl, desc)
	if delta == 0 {
		// Something changed between shouldQueue and process.
		return nil
	}

	// TODO(bdarnell): handle non-homogenous ReplicaAttrs.
	if delta > 0 {
		// Allow constraints to be relaxed if necessary.
		newReplica, err := rq.allocator.allocateTarget(zone.ReplicaAttrs[0], desc.Replicas, true, nil)
		if err != nil {
			return err
		}

		replica := proto.Replica{
			NodeID:  newReplica.Node.NodeID,
			StoreID: newReplica.StoreID,
		}
		if err = repl.ChangeReplicas(proto.ADD_REPLICA, replica, desc); err != nil {
			return err
		}
	} else {
		removeReplica, err := rq.allocator.removeTarget(desc.Replicas)
		if err != nil {
			return err
		}
		if log.V(1) {
			log.Infof("Remove replica %s", removeReplica)
		}

		if err = repl.ChangeReplicas(proto.REMOVE_REPLICA, removeReplica, desc); err != nil {
			if log.V(1) {
				log.Infof("Error removing replica %s", err)
			}
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

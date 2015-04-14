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
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
)

const (
	// replicateQueueMaxSize is the max size of the split queue.
	replicateQueueMaxSize = 100

	// replicateQueueTimerDuration is the duration between replication of queued ranges.
	replicateQueueTimerDuration = 0 * time.Second // zero duration to process replication greedily
)

// replicateQueue manages a queue of ranges to have their replicas
// change to match the zone config.
type replicateQueue struct {
	*baseQueue
	gossip    *gossip.Gossip
	allocator *allocator
	clock     *hlc.Clock
	disabled  bool
}

// newReplicateQueue returns a new instance of replicateQueue.
func newReplicateQueue(gossip *gossip.Gossip, allocator *allocator,
	clock *hlc.Clock) *replicateQueue {
	rq := &replicateQueue{
		gossip:    gossip,
		allocator: allocator,
		clock:     clock,
	}
	rq.baseQueue = newBaseQueue("replicate", rq, replicateQueueMaxSize)
	return rq
}

func (rq *replicateQueue) shouldQueue(now proto.Timestamp, rng *Range) (
	shouldQ bool, priority float64) {
	// Only queue for replication if this replica has the leader lease.
	if !rng.HasLeaderLease() {
		return
	}

	// If the range spans multiple zones, ignore it until the split queue has processed it.
	if len(computeSplitKeys(rq.gossip, rng)) > 0 {
		return
	}

	// Load the zone config to find the desired replica attributes.
	zone, err := lookupZoneConfig(rq.gossip, rng)
	if err != nil {
		log.Error(err)
		return
	}

	return rq.needsReplication(zone, rng)
}

func (rq *replicateQueue) needsReplication(zone proto.ZoneConfig, rng *Range) (bool, float64) {
	// TODO(bdarnell): handle non-empty ReplicaAttrs.
	need := len(zone.ReplicaAttrs)
	have := len(rng.Desc().Replicas)
	if need > have {
		return true, float64(need - have)
	}

	return false, 0
}

func (rq *replicateQueue) process(now proto.Timestamp, rng *Range) error {
	zone, err := lookupZoneConfig(rq.gossip, rng)
	if err != nil {
		return err
	}

	if needs, _ := rq.needsReplication(zone, rng); !needs {
		// Something changed between shouldQueue and process.
		return nil
	}

	// TODO(bdarnell): handle non-homogenous ReplicaAttrs.
	newReplica, err := rq.allocator.allocate(zone.ReplicaAttrs[0], rng.Desc().Replicas)
	if err != nil {
		return err
	}

	err = rng.ChangeReplicas(proto.ADD_REPLICA,
		proto.Replica{
			NodeID:  newReplica.Node.NodeID,
			StoreID: newReplica.StoreID,
			Attrs:   newReplica.Attrs,
		})

	// Enqueue this range again to see if there are more changes to be made.
	go rq.MaybeAdd(rng, rq.clock.Now())

	return err
}

func (rq *replicateQueue) timer() time.Duration {
	return replicateQueueTimerDuration
}

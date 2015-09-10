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
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package storage

import (
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
)

const (
	// repairQueueMaxSize is the max size of the replicate queue.
	repairQueueMaxSize = 100

	// replicaQueueTimerDuration is the duration between removals of queued
	// replicas.
	repairQueueTimerDuration = 0 // zero duration to process removals greedily
)

// repairQueue manages a queue of replicas which may have one of its other
// replica on a dead store.
type repairQueue struct {
	*baseQueue
	storePool      *StorePool
	replicateQueue *replicateQueue
	clock          *hlc.Clock
}

// makeRepairQueue returns a new instance of repairQueue.
func makeRepairQueue(storePool *StorePool, replicateQueue *replicateQueue, clock *hlc.Clock) repairQueue {
	rq := repairQueue{
		storePool:      storePool,
		replicateQueue: replicateQueue,
		clock:          clock,
	}
	// rq must be a pointer in order to setup the reference cycle.
	rq.baseQueue = newBaseQueue("repair", &rq, repairQueueMaxSize)
	return rq
}

// needsLeaderLease implements the queueImpl interface.
// This should not require a leader lease, but if we find that there is too
// much contention on the replica removals we can revisit that decision.
func (rq repairQueue) needsLeaderLease() bool {
	return false
}

// shouldQueue implements the queueImpl interface.
func (rq repairQueue) shouldQueue(now proto.Timestamp, repl *Replica) (shouldQ bool, priority float64) {
	shouldQ, priority, _ = rq.needsRepair(repl)
	return
}

// needsRepair determines if any replicas that belong to the same range as repl
// belong to a dead store. If so, it returns the priority (the number of
// replicas on dead stores) and the list of these 'dead' replicas.
func (rq repairQueue) needsRepair(repl *Replica) (bool, float64, []proto.Replica) {
	rangeDesc := repl.Desc()
	var deadReplicas []proto.Replica
	for _, otherRepl := range rangeDesc.Replicas {
		if rq.storePool.getStoreDetail(otherRepl.StoreID).dead {
			deadReplicas = append(deadReplicas, otherRepl)
		}
	}

	if len(deadReplicas) == 0 {
		return false, 0, deadReplicas
	}

	quorum := (len(rangeDesc.Replicas) / 2) + 1
	liveReplicas := len(rangeDesc.Replicas) - len(deadReplicas)

	// Priority is calculated based on need. By basing this off of the number
	// of live replicas over he quorum amount ensures that in the case where a
	// 5 replica range is only missing one it gets a lower priority than a 3
	// replica range missing 1. Note that this may produce negative priorities,
	// but these are correctly handled by baseQueue.
	priority := float64(quorum - liveReplicas)
	// Does the range still have a quorum of live replicas? We won't be able
	// to remove any replicas without at least a quorum.
	return liveReplicas >= quorum, priority, deadReplicas
}

// process implements the queueImpl interface.
// If the replica still requires repairing, it removes the replicas on dead
// stores (one at a time) and then queues up the replica for replication on the
// repliateQueue.
func (rq repairQueue) process(now proto.Timestamp, repl *Replica) error {
	shouldQ, _, deadReplicas := rq.needsRepair(repl)
	if !shouldQ {
		// There are no more dead replicas on this range, so enqueue the
		// replica onto the replicate queue so any missing replicas can be
		// added.
		rq.replicateQueue.MaybeAdd(repl, rq.clock.Now())
		return nil
	}

	// Only deal with one dead replica at a time. This way, we can re-evaluate
	// the range again to see if any other repairs are required.
	if len(deadReplicas) < 1 {
		return util.Errorf("no dead replicas, this shouldn't happen.")
	}
	log.Warningf("The replica %s is being removed from its dead store.", deadReplicas[0])
	if err := repl.ChangeReplicas(proto.REMOVE_REPLICA, deadReplicas[0], repl.Desc()); err != nil {
		return err
	}

	// Re-enqueue to handle any other dead replicas.
	rq.MaybeAdd(repl, rq.clock.Now())
	return nil
}

// timer implements the queueImpl interface.
func (rq repairQueue) timer() time.Duration {
	return repairQueueTimerDuration
}

// Copyright 2016 The Cockroach Authors.
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
// Author: Vivek Menezes (vivek@cockroachlabs.com)

package storage

import (
	"time"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/log"
)

const (
	replicaConsistencyQueueSize = 100
)

type replicaConsistencyQueue struct {
	baseQueue
}

// newReplicaConsistencyQueue returns a new instance of replicaConsistencyQueue.
func newReplicaConsistencyQueue(gossip *gossip.Gossip) *replicaConsistencyQueue {
	rcq := &replicaConsistencyQueue{}
	rcq.baseQueue = makeBaseQueue("replica consistency checker", rcq, gossip, replicaConsistencyQueueSize)
	return rcq
}

func (*replicaConsistencyQueue) needsLeaderLease() bool {
	return true
}

func (*replicaConsistencyQueue) acceptsUnsplitRanges() bool {
	return true
}

func (*replicaConsistencyQueue) shouldQueue(now roachpb.Timestamp, rng *Replica,
	_ config.SystemConfig) (bool, float64) {
	return true, 1.0
}

// process() is called on every range for which this node is a leader.
func (q *replicaConsistencyQueue) process(_ roachpb.Timestamp, rng *Replica, _ config.SystemConfig) error {
	req := roachpb.CheckConsistencyRequest{}
	_, pErr := rng.CheckConsistency(req, rng.Desc())
	if pErr != nil {
		log.Error(pErr.GoError())
	}
	return nil
}

func (*replicaConsistencyQueue) timer() time.Duration {
	// Some interval between replicas.
	return 10 * time.Second
}

// purgatoryChan returns nil.
func (*replicaConsistencyQueue) purgatoryChan() <-chan struct{} {
	return nil
}

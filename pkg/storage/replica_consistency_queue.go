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

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

const (
	replicaConsistencyQueueSize = 100
)

type replicaConsistencyQueue struct {
	*baseQueue
}

// newReplicaConsistencyQueue returns a new instance of replicaConsistencyQueue.
func newReplicaConsistencyQueue(store *Store, gossip *gossip.Gossip) *replicaConsistencyQueue {
	rcq := &replicaConsistencyQueue{}
	rcq.baseQueue = newBaseQueue(
		store.Ctx(), "replica consistency checker", rcq, store, gossip,
		queueConfig{
			maxSize:              replicaConsistencyQueueSize,
			needsLease:           true,
			acceptsUnsplitRanges: true,
			successes:            store.metrics.ConsistencyQueueSuccesses,
			failures:             store.metrics.ConsistencyQueueFailures,
			pending:              store.metrics.ConsistencyQueuePending,
			processingNanos:      store.metrics.ConsistencyQueueProcessingNanos,
		},
	)
	return rcq
}

func (*replicaConsistencyQueue) shouldQueue(
	_ hlc.Timestamp, _ *Replica, _ config.SystemConfig,
) (bool, float64) {
	return true, 1.0
}

// process() is called on every range for which this node is a lease holder.
func (q *replicaConsistencyQueue) process(
	ctx context.Context, _ hlc.Timestamp, r *Replica, _ config.SystemConfig,
) error {
	req := roachpb.CheckConsistencyRequest{}
	_, pErr := r.CheckConsistency(ctx, req, r.Desc())
	if pErr != nil {
		log.Error(ctx, pErr.GoError())
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

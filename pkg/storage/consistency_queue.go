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
	consistencyQueueSize = 100
)

type consistencyQueue struct {
	*baseQueue
}

// newConsistencyQueue returns a new instance of consistencyQueue.
func newConsistencyQueue(store *Store, gossip *gossip.Gossip) *consistencyQueue {
	rcq := &consistencyQueue{}
	rcq.baseQueue = newBaseQueue(
		"replica consistency checker", rcq, store, gossip,
		queueConfig{
			maxSize:              consistencyQueueSize,
			needsLease:           true,
			acceptsUnsplitRanges: true,
			minInterval:          store.cfg.ConsistencyCheckInterval,
			successes:            store.metrics.ConsistencyQueueSuccesses,
			failures:             store.metrics.ConsistencyQueueFailures,
			pending:              store.metrics.ConsistencyQueuePending,
			processingNanos:      store.metrics.ConsistencyQueueProcessingNanos,
		},
	)
	return rcq
}

func (q *consistencyQueue) shouldQueue(
	_ context.Context, now hlc.Timestamp, r *Replica, _ config.SystemConfig,
) (bool, float64) {
	return true, 0
}

// process() is called on every range for which this node is a lease holder.
func (q *consistencyQueue) process(
	ctx context.Context, now hlc.Timestamp, r *Replica, _ config.SystemConfig,
) error {
	req := roachpb.CheckConsistencyRequest{}
	_, pErr := r.CheckConsistency(ctx, req, r.Desc())
	if pErr != nil {
		log.Error(ctx, pErr.GoError())
	}
	return nil
}

func (*consistencyQueue) timer() time.Duration {
	// Some interval between replicas.
	return 10 * time.Second
}

// purgatoryChan returns nil.
func (*consistencyQueue) purgatoryChan() <-chan struct{} {
	return nil
}

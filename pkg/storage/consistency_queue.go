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

package storage

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

var consistencyCheckInterval = settings.RegisterNonNegativeDurationSetting(
	"server.consistency_check.interval",
	"the time between range consistency checks; set to 0 to disable consistency checking",
	24*time.Hour,
)

type consistencyQueue struct {
	*baseQueue
	interval       func() time.Duration
	replicaCountFn func() int
}

// newConsistencyQueue returns a new instance of consistencyQueue.
func newConsistencyQueue(store *Store, gossip *gossip.Gossip) *consistencyQueue {
	q := &consistencyQueue{
		interval: func() time.Duration {
			return consistencyCheckInterval.Get(&store.ClusterSettings().SV)
		},
		replicaCountFn: store.ReplicaCount,
	}
	q.baseQueue = newBaseQueue(
		"replica consistency checker", q, store, gossip,
		queueConfig{
			maxSize:              defaultQueueMaxSize,
			needsLease:           true,
			needsSystemConfig:    false,
			acceptsUnsplitRanges: true,
			successes:            store.metrics.ConsistencyQueueSuccesses,
			failures:             store.metrics.ConsistencyQueueFailures,
			pending:              store.metrics.ConsistencyQueuePending,
			processingNanos:      store.metrics.ConsistencyQueueProcessingNanos,
		},
	)
	return q
}

func (q *consistencyQueue) shouldQueue(
	ctx context.Context, now hlc.Timestamp, repl *Replica, _ config.SystemConfig,
) (bool, float64) {
	interval := q.interval()
	if interval <= 0 {
		return false, 0
	}

	shouldQ, priority := true, float64(0)
	if !repl.store.cfg.TestingKnobs.DisableLastProcessedCheck {
		lpTS, err := repl.getQueueLastProcessed(ctx, q.name)
		if err != nil {
			return false, 0
		}
		if shouldQ, priority = shouldQueueAgain(now, lpTS, interval); !shouldQ {
			return false, 0
		}
	}
	// Check if all replicas are live. Some tests run without a NodeLiveness configured.
	if repl.store.cfg.NodeLiveness != nil {
		for _, rep := range repl.Desc().Replicas {
			if live, err := repl.store.cfg.NodeLiveness.IsLive(rep.NodeID); err != nil {
				log.VErrEventf(ctx, 3, "node %d liveness failed: %s", rep.NodeID, err)
				return false, 0
			} else if !live {
				return false, 0
			}
		}
	}
	return true, priority
}

// process() is called on every range for which this node is a lease holder.
func (q *consistencyQueue) process(
	ctx context.Context, repl *Replica, _ config.SystemConfig,
) error {
	if q.interval() <= 0 {
		return nil
	}
	req := roachpb.CheckConsistencyRequest{}
	if _, pErr := repl.CheckConsistency(ctx, req); pErr != nil {
		_, shouldQuiesce := <-repl.store.Stopper().ShouldQuiesce()
		if !shouldQuiesce || !grpcutil.IsClosedConnection(pErr.GoError()) {
			// Suppress noisy errors about closed GRPC connections when the
			// server is quiescing.
			log.Error(ctx, pErr.GoError())
		}
	}
	// Update the last processed time for this queue.
	if err := repl.setQueueLastProcessed(ctx, q.name, repl.store.Clock().Now()); err != nil {
		log.VErrEventf(ctx, 2, "failed to update last processed time: %v", err)
	}
	return nil
}

func (q *consistencyQueue) timer(duration time.Duration) time.Duration {
	// An interval between replicas to space consistency checks out over
	// the check interval.
	replicaCount := q.replicaCountFn()
	if replicaCount == 0 {
		return 0
	}
	replInterval := q.interval() / time.Duration(replicaCount)
	if replInterval < duration {
		return 0
	}
	return replInterval - duration
}

// purgatoryChan returns nil.
func (*consistencyQueue) purgatoryChan() <-chan struct{} {
	return nil
}

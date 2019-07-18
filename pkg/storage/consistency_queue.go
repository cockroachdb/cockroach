// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

var consistencyCheckInterval = settings.RegisterNonNegativeDurationSetting(
	"server.consistency_check.interval",
	"the time between range consistency checks; set to 0 to disable consistency checking",
	24*time.Hour,
)

var testingAggressiveConsistencyChecks = envutil.EnvOrDefaultBool("COCKROACH_CONSISTENCY_AGGRESSIVE", false)

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
		"consistencyChecker", q, store, gossip,
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
	ctx context.Context, now hlc.Timestamp, repl *Replica, _ *config.SystemConfig,
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
		for _, rep := range repl.Desc().Replicas().All() {
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
	ctx context.Context, repl *Replica, _ *config.SystemConfig,
) error {
	if q.interval() <= 0 {
		return nil
	}

	// Call setQueueLastProcessed because the consistency checker targets a much
	// longer cycle time than other queues. That it ignores errors is likely a
	// historical accident that should be revisited.
	if err := repl.setQueueLastProcessed(ctx, q.name, repl.store.Clock().Now()); err != nil {
		log.VErrEventf(ctx, 2, "failed to update last processed time: %v", err)
	}

	req := roachpb.CheckConsistencyRequest{
		// Tell CheckConsistency that the caller is the queue. This triggers
		// code to handle inconsistencies by recomputing with a diff and exiting
		// with a fatal error, and triggers a stats readjustment if there is no
		// inconsistency but the persisted stats are found to disagree with
		// those reflected in the data. All of this really ought to be lifted
		// into the queue in the future.
		Mode: roachpb.ChecksumMode_CHECK_VIA_QUEUE,
	}
	resp, pErr := repl.CheckConsistency(ctx, req)
	if pErr != nil {
		var shouldQuiesce bool
		select {
		case <-repl.store.Stopper().ShouldQuiesce():
			shouldQuiesce = true
		default:
		}

		if !shouldQuiesce || !grpcutil.IsClosedConnection(pErr.GoError()) {
			// Suppress noisy errors about closed GRPC connections when the
			// server is quiescing.
			log.Error(ctx, pErr.GoError())
			return pErr.GoError()
		}
		return nil
	}
	if fn := repl.store.cfg.TestingKnobs.ConsistencyTestingKnobs.ConsistencyQueueResultHook; fn != nil {
		fn(resp)
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
func (*consistencyQueue) purgatoryChan() <-chan time.Time {
	return nil
}

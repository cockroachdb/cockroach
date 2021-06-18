// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

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

var consistencyCheckInterval = settings.RegisterDurationSetting(
	"server.consistency_check.interval",
	"the time between range consistency checks; set to 0 to disable consistency checking."+
		" Note that intervals that are too short can negatively impact performance.",
	24*time.Hour,
	settings.NonNegativeDuration,
)

var consistencyCheckRate = settings.RegisterByteSizeSetting(
	"server.consistency_check.max_rate",
	"the rate limit (bytes/sec) to use for consistency checks; used in "+
		"conjunction with server.consistency_check.interval to control the "+
		"frequency of consistency checks. Note that setting this too high can "+
		"negatively impact performance.",
	8<<20, // 8MB
	validatePositive,
).WithPublic()

// consistencyCheckRateBurstFactor we use this to set the burst parameter on the
// quotapool.RateLimiter. It seems overkill to provide a user setting for this,
// so we use a factor to scale the burst setting based on the rate defined above.
const consistencyCheckRateBurstFactor = 8

// consistencyCheckRateMinWait is the minimum time to wait once the rate limit
// is reached. We check the limit on every key/value pair, which can lead to
// a lot of nano-second waits because each pair could be very small. Instead we
// force a larger pause every time the timer is breached to reduce the
// churn on timers.
const consistencyCheckRateMinWait = 100 * time.Millisecond

var testingAggressiveConsistencyChecks = envutil.EnvOrDefaultBool("COCKROACH_CONSISTENCY_AGGRESSIVE", false)

type consistencyQueue struct {
	*baseQueue
	interval       func() time.Duration
	replicaCountFn func() int
}

// A data wrapper to allow for the shouldQueue method to be easier to test.
type consistencyShouldQueueData struct {
	desc                      *roachpb.RangeDescriptor
	getQueueLastProcessed     func(ctx context.Context) (hlc.Timestamp, error)
	isNodeAvailable           func(nodeID roachpb.NodeID) bool
	disableLastProcessedCheck bool
	interval                  time.Duration
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
			processTimeoutFunc:   makeRateLimitedTimeoutFunc(consistencyCheckRate),
		},
	)
	return q
}

func (q *consistencyQueue) shouldQueue(
	ctx context.Context, now hlc.ClockTimestamp, repl *Replica, _ *config.SystemConfig,
) (bool, float64) {
	return consistencyQueueShouldQueueImpl(ctx, now,
		consistencyShouldQueueData{
			desc: repl.Desc(),
			getQueueLastProcessed: func(ctx context.Context) (hlc.Timestamp, error) {
				return repl.getQueueLastProcessed(ctx, q.name)
			},
			isNodeAvailable: func(nodeID roachpb.NodeID) bool {
				if repl.store.cfg.NodeLiveness != nil {
					return repl.store.cfg.NodeLiveness.IsAvailableNotDraining(nodeID)
				}
				// Some tests run without a NodeLiveness configured.
				return true
			},
			disableLastProcessedCheck: repl.store.cfg.TestingKnobs.DisableLastProcessedCheck,
			interval:                  q.interval(),
		})
}

// ConsistencyQueueShouldQueueImpl is exposed for testability without having
// to setup a fully fledged replica.
func consistencyQueueShouldQueueImpl(
	ctx context.Context, now hlc.ClockTimestamp, data consistencyShouldQueueData,
) (bool, float64) {
	if data.interval <= 0 {
		return false, 0
	}

	shouldQ, priority := true, float64(0)
	if !data.disableLastProcessedCheck {
		lpTS, err := data.getQueueLastProcessed(ctx)
		if err != nil {
			return false, 0
		}
		if shouldQ, priority = shouldQueueAgain(now.ToTimestamp(), lpTS, data.interval); !shouldQ {
			return false, 0
		}
	}
	// Check if all replicas are available.
	for _, rep := range data.desc.Replicas().Descriptors() {
		if !data.isNodeAvailable(rep.NodeID) {
			return false, 0
		}
	}
	return true, priority
}

// process() is called on every range for which this node is a lease holder.
func (q *consistencyQueue) process(
	ctx context.Context, repl *Replica, _ *config.SystemConfig,
) (bool, error) {
	if q.interval() <= 0 {
		return false, nil
	}

	// Call setQueueLastProcessed because the consistency checker targets a much
	// longer cycle time than other queues. That it ignores errors is likely a
	// historical accident that should be revisited.
	if err := repl.setQueueLastProcessed(ctx, q.name, repl.store.Clock().Now()); err != nil {
		log.VErrEventf(ctx, 2, "failed to update last processed time: %v", err)
	}

	req := roachpb.CheckConsistencyRequest{
		// Tell CheckConsistency that the caller is the queue. This triggers
		// code to handle inconsistencies by recomputing with a diff and
		// instructing the nodes in the minority to terminate with a fatal
		// error. It also triggers a stats readjustment if there is no
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

		if shouldQuiesce && grpcutil.IsClosedConnection(pErr.GoError()) {
			// Suppress noisy errors about closed GRPC connections when the
			// server is quiescing.
			return false, nil
		}
		err := pErr.GoError()
		log.Errorf(ctx, "%v", err)
		return false, err
	}
	if fn := repl.store.cfg.TestingKnobs.ConsistencyTestingKnobs.ConsistencyQueueResultHook; fn != nil {
		fn(resp)
	}
	return true, nil
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

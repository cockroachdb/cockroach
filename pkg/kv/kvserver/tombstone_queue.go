// Copyright 2014 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

const (
	// tombstoneQueueDefaultTimerDuration is the default duration between
	// tombstone checks of queued replicas.
	tombstoneQueueDefaultTimerDuration = 1 * time.Second
	// tombstoneQueueTimeout is the timeout for a single MVCC GC run.
	tombstoneQueueTimeout = 10 * time.Minute
)

// tombstoneQueueInterval is a setting that controls how long the tombstone queue
// waits between processing replicas.
var tombstoneQueueInterval = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kv.tombstone.queue_interval",
	"how long the tombstone queue waits between processing replicas",
	tombstoneQueueDefaultTimerDuration,
	settings.NonNegativeDuration,
)

// tombstoneQueue manages a queue of replicas slated to be checked if compaction
// against the range is necessary. The tombstone queue checks if the amount of
// data in the underlying storage layer is a significant multiple of the amount
// of data in the range.

// The shouldQueue function combines the need for the above tasks into a
// single priority. If any task is overdue, shouldQueue returns true.
type tombstoneQueue struct {
	*baseQueue
}

var _ queueImpl = &tombstoneQueue{}

// newTombstoneQueue returns a new instance of tombstoneQueue.
func newTombstoneQueue(store *Store) *tombstoneQueue {
	tq := &tombstoneQueue{}
	tq.baseQueue = newBaseQueue(
		"tombstone", tq, store,
		queueConfig{
			maxSize:              defaultQueueMaxSize,
			needsLease:           false,
			needsSpanConfigs:     false,
			acceptsUnsplitRanges: true,
			processTimeoutFunc: func(st *cluster.Settings, _ replicaInQueue) time.Duration {
				timeout := tombstoneQueueTimeout
				if d := queueGuaranteedProcessingTimeBudget.Get(&st.SV); d > timeout {
					timeout = d
				}
				return timeout
			},
			successes:       store.metrics.TombstoneQueueSuccesses,
			failures:        store.metrics.TombstoneQueueFailures,
			storeFailures:   store.metrics.StoreFailures,
			pending:         store.metrics.TombstoneQueuePending,
			processingNanos: store.metrics.TombstoneQueueProcessingNanos,
			disabledConfig:  kvserverbase.TombstoneQueueEnabled,
		},
	)
	return tq
}

// shouldQueue determines whether a replica should be queued for garbage
// collection, and if so, at what priority. Returns true for shouldQ
// in the event that the cumulative ages of GC'able bytes or extant
// intents exceed thresholds.
func (tq *tombstoneQueue) shouldQueue(
	ctx context.Context, _ hlc.ClockTimestamp, repl *Replica, _ spanconfig.StoreReader,
) (bool, float64) {
	mvccBytes := repl.GetMVCCStats().Total()
	storageBytes, err := repl.GetRangeApproximateDiskBytes()
	if err != nil {
		// TODO(baptist): Log error
		return false, 0
	}
	// TODO(baptist): make configurable
	minUsage := uint64(10 << 20) // 10MiB
	overhead := 10.0

	if storageBytes > minUsage && float64(storageBytes)/float64(mvccBytes) > overhead {
		// TODO: Log the current values
		return true, 0
	}
	return false, 0
}

func (tq *tombstoneQueue) process(
	ctx context.Context, repl *Replica, _ spanconfig.StoreReader,
) (bool, error) {
	err := repl.CompactStorage(ctx)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (tq *tombstoneQueue) timer(time.Duration) time.Duration {
	return tombstoneQueueInterval.Get(&tq.store.ClusterSettings().SV)
}

func (*tombstoneQueue) postProcessScheduled(context.Context, replicaInQueue, float64) {
}

func (*tombstoneQueue) purgatoryChan() <-chan time.Time {
	return nil
}

func (*tombstoneQueue) updateChan() <-chan time.Time {
	return nil
}

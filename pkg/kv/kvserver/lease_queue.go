// Copyright 2024 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

const (
	// leaseQueueTimerDuration is the duration between (potential) lease transfers of queued
	// replicas. Use a zero duration to process lease transfers greedily.
	leaseQueueTimerDuration = 0
	// leaseQueuePurgatoryCheckInterval is the interval at which replicas in
	// the lease queue purgatory are re-attempted.
	leaseQueuePurgatoryCheckInterval = 10 * time.Second
)

type leaseQueue struct {
	allocator allocatorimpl.Allocator
	storePool storepool.AllocatorStorePool
	purgCh    <-chan time.Time
	*baseQueue
}

var _ queueImpl = &leaseQueue{}

func newLeaseQueue(store *Store, allocator allocatorimpl.Allocator) *leaseQueue {
	var storePool storepool.AllocatorStorePool
	if store.cfg.StorePool != nil {
		storePool = store.cfg.StorePool
	}
	lq := &leaseQueue{
		allocator: allocator,
		storePool: storePool,
		purgCh:    time.NewTicker(leaseQueuePurgatoryCheckInterval).C,
	}

	lq.baseQueue = newBaseQueue("lease", lq, store,
		queueConfig{
			maxSize:              defaultQueueMaxSize,
			needsLease:           true,
			needsSpanConfigs:     true,
			acceptsUnsplitRanges: false,
			successes:            store.metrics.LeaseQueueSuccesses,
			failures:             store.metrics.LeaseQueueFailures,
			pending:              store.metrics.LeaseQueuePending,
			processingNanos:      store.metrics.LeaseQueueProcessingNanos,
			purgatory:            store.metrics.LeaseQueuePurgatory,
			disabledConfig:       kvserverbase.LeaseQueueEnabled,
		})

	return lq
}

func (lq *leaseQueue) shouldQueue(
	ctx context.Context, now hlc.ClockTimestamp, repl *Replica, confReader spanconfig.StoreReader,
) (shouldQueue bool, priority float64) {
	// TODO(kvoli): Check whether the lease should be transferred away and at
	// what priority. Tracked in #118966.
	return false, 0
}

func (lq *leaseQueue) process(
	ctx context.Context, repl *Replica, confReader spanconfig.StoreReader,
) (processed bool, err error) {
	// TODO(kvoli): Search for a lease transfer target, if one exists then
	// transfer the lease away. Tracked in #118966.
	return true, nil
}

func (lq *leaseQueue) postProcessScheduled(
	ctx context.Context, replica replicaInQueue, priority float64,
) {
}

func (lq *leaseQueue) timer(time.Duration) time.Duration {
	return leaseQueueTimerDuration
}

func (lq *leaseQueue) purgatoryChan() <-chan time.Time {
	return lq.purgCh
}

func (lq *leaseQueue) updateChan() <-chan time.Time {
	// TODO(kvoli): implement an update callback on gossiped store descriptors,
	// similar to what is done in the replicate queue.
	return nil
}

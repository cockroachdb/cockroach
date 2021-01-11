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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

const (
	// TimeSeriesMaintenanceInterval is the minimum interval between two
	// time series maintenance runs on a replica.
	TimeSeriesMaintenanceInterval = 24 * time.Hour // daily

	// TimeSeriesMaintenanceMemoryBudget is the maximum amount of memory that
	// should be consumed by time series maintenance operations at any one time.
	TimeSeriesMaintenanceMemoryBudget = int64(8 * 1024 * 1024) // 8MB
)

// TimeSeriesDataStore is an interface defined in the storage package that can
// be implemented by the higher-level time series system. This allows the
// storage queues to run periodic time series maintenance; importantly, this
// maintenance can then be informed by data from the local store.
type TimeSeriesDataStore interface {
	ContainsTimeSeries(roachpb.RKey, roachpb.RKey) bool
	MaintainTimeSeries(
		context.Context,
		storage.Reader,
		roachpb.RKey,
		roachpb.RKey,
		*kv.DB,
		*mon.BytesMonitor,
		int64,
		hlc.Timestamp,
	) error
}

// timeSeriesMaintenanceQueue identifies replicas that contain time series
// data and performs necessary data maintenance on the time series located in
// the replica. Currently, maintenance involves pruning time series data older
// than a certain threshold.
//
// Logic for time series maintenance is implemented in a higher level time
// series package; this queue uses the TimeSeriesDataStore interface to call
// into that logic.
//
// Once a replica is selected for processing, data changes are executed against
// the cluster using a KV client; changes are not restricted to the data in the
// replica being processed. These tasks could therefore be performed without a
// replica queue; however, a replica queue has been chosen to initiate this task
// for a few reasons:
// * The queue naturally distributes the workload across the cluster in
// proportion to the number of ranges containing time series data.
// * Access to the local replica is a convenient way to discover the names of
// time series stored on the cluster; the names are required in order to
// effectively prune time series without expensive distributed scans.
//
// Data changes executed by this queue are idempotent; it is explicitly safe
// for multiple nodes to attempt to prune the same time series concurrently.
// In this situation, each node would compute the same delete range based on
// the current timestamp; the first will succeed, all others will become
// a no-op.
type timeSeriesMaintenanceQueue struct {
	*baseQueue
	tsData         TimeSeriesDataStore
	replicaCountFn func() int
	db             *kv.DB
	mem            *mon.BytesMonitor
}

// newTimeSeriesMaintenanceQueue returns a new instance of
// timeSeriesMaintenanceQueue.
func newTimeSeriesMaintenanceQueue(
	store *Store, db *kv.DB, g *gossip.Gossip, tsData TimeSeriesDataStore,
) *timeSeriesMaintenanceQueue {
	q := &timeSeriesMaintenanceQueue{
		tsData:         tsData,
		replicaCountFn: store.ReplicaCount,
		db:             db,
		mem: mon.NewUnlimitedMonitor(
			context.Background(),
			"timeseries-maintenance-queue",
			mon.MemoryResource,
			nil,
			nil,
			// Begin logging messages if we exceed our planned memory usage by
			// more than triple.
			TimeSeriesMaintenanceMemoryBudget*3,
			store.cfg.Settings,
		),
	}
	q.baseQueue = newBaseQueue(
		"timeSeriesMaintenance", q, store, g,
		queueConfig{
			maxSize:              defaultQueueMaxSize,
			needsLease:           true,
			needsSystemConfig:    false,
			acceptsUnsplitRanges: true,
			successes:            store.metrics.TimeSeriesMaintenanceQueueSuccesses,
			failures:             store.metrics.TimeSeriesMaintenanceQueueFailures,
			pending:              store.metrics.TimeSeriesMaintenanceQueuePending,
			processingNanos:      store.metrics.TimeSeriesMaintenanceQueueProcessingNanos,
		},
	)

	return q
}

func (q *timeSeriesMaintenanceQueue) shouldQueue(
	ctx context.Context, now hlc.ClockTimestamp, repl *Replica, _ *config.SystemConfig,
) (shouldQ bool, priority float64) {
	if !repl.store.cfg.TestingKnobs.DisableLastProcessedCheck {
		lpTS, err := repl.getQueueLastProcessed(ctx, q.name)
		if err != nil {
			return false, 0
		}
		shouldQ, priority = shouldQueueAgain(now.ToTimestamp(), lpTS, TimeSeriesMaintenanceInterval)
		if !shouldQ {
			return
		}
	}
	desc := repl.Desc()
	if q.tsData.ContainsTimeSeries(desc.StartKey, desc.EndKey) {
		return
	}
	return false, 0
}

func (q *timeSeriesMaintenanceQueue) process(
	ctx context.Context, repl *Replica, _ *config.SystemConfig,
) (processed bool, err error) {
	desc := repl.Desc()
	snap := repl.store.Engine().NewSnapshot()
	now := repl.store.Clock().Now()
	defer snap.Close()
	if err := q.tsData.MaintainTimeSeries(
		ctx, snap, desc.StartKey, desc.EndKey, q.db, q.mem, TimeSeriesMaintenanceMemoryBudget, now,
	); err != nil {
		return false, err
	}
	// Update the last processed time for this queue.
	if err := repl.setQueueLastProcessed(ctx, q.name, now); err != nil {
		log.VErrEventf(ctx, 2, "failed to update last processed time: %v", err)
	}
	return true, nil
}

func (q *timeSeriesMaintenanceQueue) timer(duration time.Duration) time.Duration {
	// An interval between replicas to space consistency checks out over
	// the check interval.
	replicaCount := q.replicaCountFn()
	if replicaCount == 0 {
		return 0
	}
	replInterval := TimeSeriesMaintenanceInterval / time.Duration(replicaCount)
	if replInterval < duration {
		return 0
	}
	return replInterval - duration
}

func (*timeSeriesMaintenanceQueue) purgatoryChan() <-chan time.Time {
	return nil
}

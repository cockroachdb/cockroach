// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// The number of concurrent migrateLockTableRequests requests to run. This
// is effectively a cluster-wide setting as the actual legwork of the migration
// happens when the destination replica(s) are sending replies back to the
// original node.
//
// TODO(bilal): Add logic to make this concurrency limit a per-leaseholder limit
// as opposed to a cluster-wide limit. That way, we could limit MigrateLockTable
// requests to 1 per leaseholder as opposed to 4 for the entire cluster,
// avoiding the case where all 4 ranges at a time could have the same node
// as their leaseholder.
const concurrentMigrateLockTableRequests = 4

// The maximum number of times to retry a migrateLockTableRequest before failing
// the migration.
const migrateLockTableRetries = 3

// migrateLockTableRequest represents one request for a migrateLockTable
// command. This will correspond to one range at the time of running the
// IterateRangeDescriptors command. If range boundaries change during the
// course of the migration, that is okay as the MigrateLockTable command
// being a range command takes care of that. The debugRangeID is the range ID
// for this range at the time of the range descriptor iteration, and is
// present solely for observability / logging purposes.
type migrateLockTableRequest struct {
	start, end   roachpb.RKey
	debugRangeID roachpb.RangeID
}

type migrateLockTablePool struct {
	requests chan migrateLockTableRequest
	wg       sync.WaitGroup
	stopper  *stop.Stopper
	db       *kv.DB
	done     chan bool
	status   [concurrentMigrateLockTableRequests]int64

	mu struct {
		syncutil.Mutex

		errorCount  int
		combinedErr error
	}
}

func (m *migrateLockTablePool) run(ctx context.Context, workerIdx int) {
	defer m.wg.Done()
	ctx, cancel := m.stopper.WithCancelOnQuiesce(ctx)
	defer cancel()

	var retryRequest *migrateLockTableRequest
	retryAttempt := 0
	statusSlot := &m.status[workerIdx]
	atomic.StoreInt64(statusSlot, 0)

	for {
		if retryRequest == nil {
			// Pull a new request out of the channel.
			select {
			case r, ok := <-m.requests:
				if !ok {
					return
				}
				retryRequest = &r
				retryAttempt = 0
			case <-ctx.Done():
				log.Warningf(ctx, "lock table migration canceled")
				return
			}
		}

		if ctx.Err() != nil {
			log.Warningf(ctx, "lock table migration canceled on range r%d", retryRequest.debugRangeID)
			return
		}

		atomic.StoreInt64(statusSlot, int64(retryRequest.debugRangeID))
		err := m.db.MigrateLockTable(ctx, retryRequest.start, retryRequest.end)
		if err != nil {
			log.Errorf(ctx, "error when running migrate lock table command for range r%d: %s",
				retryRequest.debugRangeID, err)
			retryAttempt++
			if retryAttempt >= migrateLockTableRetries {
				// Report this error to the migration manager. This will cause the
				// whole migration to be retried later. In the meantime, continue
				// migrating any other ranges in the queue, instead of stalling the
				// pipeline.
				m.mu.Lock()
				// Limit the number of errors chained. This prevents excessive memory
				// usage in case of error blowup (rangeCount * migrateLockTableRetries).
				if m.mu.errorCount < 16 {
					m.mu.combinedErr = errors.CombineErrors(m.mu.combinedErr, err)
				}
				m.mu.errorCount++
				m.mu.Unlock()

				retryAttempt = 0
				retryRequest = nil
				atomic.StoreInt64(statusSlot, 0)
			}
		} else {
			retryAttempt = 0
			retryRequest = nil
			atomic.StoreInt64(statusSlot, 0)
		}
	}
}

func (m *migrateLockTablePool) startStatusLogger(ctx context.Context) {
	m.done = make(chan bool)
	_ = m.stopper.RunAsyncTask(ctx, "migrate-lock-table-status", m.runStatusLogger)
}

func (m *migrateLockTablePool) stopStatusLogger(ctx context.Context) {
	close(m.done)
}

func (m *migrateLockTablePool) runStatusLogger(ctx context.Context) {
	ctx, cancel := m.stopper.WithCancelOnQuiesce(ctx)
	defer cancel()

	const statusTickDuration = 5 * time.Second
	ticker := time.NewTicker(statusTickDuration)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			var ranges strings.Builder
			for i := 0; i < concurrentMigrateLockTableRequests; i++ {
				rangeID := atomic.LoadInt64(&m.status[i])
				if rangeID == 0 {
					continue
				}
				if ranges.Len() != 0 {
					fmt.Fprintf(&ranges, ", ")
				}
				fmt.Fprintf(&ranges, "%s", roachpb.RangeID(rangeID))
			}

			if ranges.Len() > 0 {
				log.Infof(ctx, "currently migrating lock table on ranges %s", ranges.String())
			}

		case <-m.done:
			return
		case <-ctx.Done():
			return
		}
	}
}

func separatedIntentsMigration(
	ctx context.Context, cv clusterversion.ClusterVersion, h migration.Cluster,
) error {
	var batchIdx, numMigratedRanges int
	stopper := h.Stopper()
	if stopper == nil {
		// This is a SQL-only node and would not need a migration.
		log.Infof(ctx,
			"skipping separated intents migration as running on a SQL only node",
		)
		return nil
	}

	workerPool := migrateLockTablePool{
		requests: make(chan migrateLockTableRequest),
		stopper:  h.Stopper(),
		db:       h.DB(),
	}

	workerPool.wg.Add(concurrentMigrateLockTableRequests)
	for i := 0; i < concurrentMigrateLockTableRequests; i++ {
		idx := i // Copy for closure below.
		taskName := fmt.Sprintf("migrate-lock-table-%d", i)
		if err := stopper.RunAsyncTask(ctx, taskName, func(ctx context.Context) {
			workerPool.run(ctx, idx)
		}); err != nil {
			return err
		}
	}
	init := func() { batchIdx, numMigratedRanges = 1, 0 }
	if err := h.IterateRangeDescriptors(ctx, defaultPageSize, init, func(descriptors ...roachpb.RangeDescriptor) error {
		for _, desc := range descriptors {
			start, end := desc.StartKey, desc.EndKey
			if bytes.Compare(desc.StartKey, keys.LocalMax) < 0 {
				start, _ = keys.Addr(keys.LocalMax)
			}
			// Check if this range only contains timeseries keys. If it is, we can
			// just skip it - it will not contain any intents.
			if bytes.HasPrefix(start, keys.TimeseriesPrefix) && bytes.HasPrefix(end, keys.TimeseriesPrefix) {
				continue
			}
			workerPool.requests <- migrateLockTableRequest{
				start:        start,
				end:          end,
				debugRangeID: desc.RangeID,
			}

			// Also enqueue a request for range local keys.
			var rangeKeyStart, rangeKeyEnd roachpb.RKey
			var err error
			rangeKeyStart, err = keys.Addr(keys.MakeRangeKeyPrefix(start))
			if err != nil {
				return errors.Wrap(err, "error when constructing range key")
			}
			rangeKeyEnd, err = keys.Addr(keys.MakeRangeKeyPrefix(end))
			if err != nil {
				return errors.Wrap(err, "error when constructing range key")
			}

			workerPool.requests <- migrateLockTableRequest{
				start:        rangeKeyStart,
				end:          rangeKeyEnd,
				debugRangeID: desc.RangeID,
			}
		}
		numMigratedRanges += len(descriptors)
		log.Infof(ctx, "[batch %d/??] started lock table migrations for %d ranges", batchIdx, numMigratedRanges)
		batchIdx++
		return nil
	}); err != nil {
		return err
	}

	workerPool.startStatusLogger(ctx)

	close(workerPool.requests)
	workerPool.wg.Wait()
	workerPool.stopStatusLogger(ctx)

	if workerPool.mu.combinedErr != nil {
		return workerPool.mu.combinedErr
	}

	log.Infof(ctx, "[batch %d/%d] migrated lock table for %d ranges", batchIdx, batchIdx, numMigratedRanges)

	return nil
}

func postSeparatedIntentsMigration(
	ctx context.Context, cv clusterversion.ClusterVersion, h migration.Cluster,
) error {
	var batchIdx, numMigratedRanges int
	init := func() { batchIdx, numMigratedRanges = 1, 0 }

	// Issue no-op Migrate commands to all ranges. This has the only
	// purpose of clearing out any orphaned replicas, preventing interleaved
	// intents in them from resurfacing.
	if err := h.IterateRangeDescriptors(ctx, defaultPageSize, init, func(descriptors ...roachpb.RangeDescriptor) error {
		for _, desc := range descriptors {
			start, end := desc.StartKey, desc.EndKey
			if bytes.Compare(desc.StartKey, keys.LocalMax) < 0 {
				start, _ = keys.Addr(keys.LocalMax)
			}
			// Check if this range is a timeseries range. If it is, we can just skip
			// it - it will not contain any intents.
			if bytes.HasPrefix(start, keys.TimeseriesPrefix) && bytes.HasPrefix(end, keys.TimeseriesPrefix) {
				continue
			}
			if err := h.DB().Migrate(ctx, start, end, cv.Version); err != nil {
				return err
			}
		}
		numMigratedRanges += len(descriptors)
		log.Infof(ctx, "[batch %d/??] started no-op migrations for %d ranges", batchIdx, numMigratedRanges)
		batchIdx++
		return nil
	}); err != nil {
		return err
	}

	log.Infof(ctx, "[batch %d/%d] finished no-op migrations for %d ranges", batchIdx, batchIdx, numMigratedRanges)

	return nil
}

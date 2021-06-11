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
	"sync"

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
const concurrentMigrateLockTableRequests = 4

// The maximum number of times to retry a migrateLockTableRequest before failing
// the migration.
const migrateLockTableRetries = 3

// migrateLockTableRequest represents one request for a migrateLockTable
// command. This will correspond to one range at the time of running the
// IterateRangeDescriptors command. As long as
type migrateLockTableRequest struct {
	start, end roachpb.RKey
}

type migrateLockTablePool struct {
	requests chan migrateLockTableRequest
	wg       sync.WaitGroup
	stopper  *stop.Stopper
	db       *kv.DB

	mu struct {
		syncutil.Mutex

		combinedErr error
	}
}

func (m *migrateLockTablePool) run(ctx context.Context) {
	defer m.wg.Done()
	ctx, cancel := m.stopper.WithCancelOnQuiesce(ctx)
	defer cancel()

	var retryRequest *migrateLockTableRequest
	retryAttempt := 0

	for {
		if retryRequest == nil {
			// Pull a new request out of the channel.
			select {
			case r, ok := <-m.requests:
				if !ok && r.end == nil && r.start == nil {
					return
				}
				retryRequest = &r
				retryAttempt = 0
			case <-ctx.Done():
				log.Warningf(ctx, "lock table migration canceled")
				return
			}
		}

		select {
		case <-ctx.Done():
			log.Warningf(ctx, "lock table migration canceled")
			return
		default:
		}

		err := m.db.MigrateLockTable(ctx, retryRequest.start, retryRequest.end)
		if err != nil {
			retryAttempt++
			if retryAttempt >= migrateLockTableRetries {
				// Report this error to the migration manager. This will cause the
				// whole migration to be retried later. In the meantime, continue
				// migrating any other ranges in the queue, instead of stalling the
				// pipeline.
				m.mu.Lock()
				m.mu.combinedErr = errors.CombineErrors(m.mu.combinedErr, err)
				m.mu.Unlock()

				retryAttempt = 0
				retryRequest = nil
			}
		} else {
			retryAttempt = 0
			retryRequest = nil
		}
	}
}

func separatedIntentsMigration(
	ctx context.Context, cv clusterversion.ClusterVersion, h migration.Cluster,
) error {
	var batchIdx, numMigratedRanges int
	stopper := h.Stopper()
	if stopper == nil {
		// This is a tenant cluster and would not need a migration.
		log.Infof(ctx,
			"skipping separated intents migration as running on a tenant cluster",
			batchIdx, numMigratedRanges,
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
		taskName := fmt.Sprintf("migrate-lock-table-%d", i)
		if err := stopper.RunAsyncTask(ctx, taskName, workerPool.run); err != nil {
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
			// Check if this range is a timeseries range. If it is, we can just skip
			// it - it will not contain any intents.
			if bytes.HasPrefix(start, keys.TimeseriesPrefix) && bytes.HasPrefix(end, keys.TimeseriesPrefix) {
				continue
			}
			workerPool.requests <- migrateLockTableRequest{
				start: start,
				end:   end,
			}
		}
		numMigratedRanges += len(descriptors)
		log.Infof(ctx, "[batch %d/??] started lock table migrations for %d ranges", batchIdx, numMigratedRanges)
		batchIdx++
		return nil
	}); err != nil {
		return err
	}

	close(workerPool.requests)
	workerPool.wg.Wait()

	if workerPool.mu.combinedErr != nil {
		return workerPool.mu.combinedErr
	}

	log.Infof(ctx, "[batch %d/%d] migrated lock table for %d ranges", batchIdx, batchIdx, numMigratedRanges)

	// Part 2 - Issue no-op Migrate commands to all ranges. This has the only
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

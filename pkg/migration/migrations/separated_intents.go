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
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/intentresolver"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// The number of concurrent migrateLockTableRequests requests to run. This
// is effectively a cluster-wide setting as the actual legwork of the migration
// happens when the destination replica(s) are sending replies back to the
// original node.
//
// TODO(bilal): Add logic to make this concurrency limit a per-leaseholder limit
// as opposed to a cluster-wide limit. That way, we could limit
// migrateLockTableRequests to 1 per leaseholder as opposed to 4 for the entire
// cluster, avoiding the case where all 4 ranges at a time could have the same node
// as their leaseholder.
const concurrentMigrateLockTableRequests = 4

// The maximum number of times to retry a migrateLockTableRequest before failing
// the migration.
const migrateLockTableRetries = 3

// migrateLockTableRequest represents migration of one slice of the keyspace. As
// part of this request, multiple non-transactional requests would need to be
// run: a Barrier, a ScanInterleavedIntents, then multiple txn pushes and intent
// resolutions.
//
// One request will correspond to one range at the time of running the
// IterateRangeDescriptors command. If range boundaries change during the
// course of the migration, that is okay as the migration logic does not rely on
// that assumption. The debugRangeID is the range ID for this range at the time
// of the range descriptor iteration, and is
// present solely for observability / logging purposes.
type migrateLockTableRequest struct {
	start, end   roachpb.Key
	debugRangeID roachpb.RangeID
	barrierDone  bool
	barrierTS    hlc.Timestamp
}

type intentResolver interface {
	PushTransaction(
		ctx context.Context, pushTxn *enginepb.TxnMeta, h roachpb.Header, pushType roachpb.PushTxnType,
	) (*roachpb.Transaction, *roachpb.Error)

	ResolveIntents(
		ctx context.Context, intents []roachpb.LockUpdate, opts intentresolver.ResolveOptions,
	) (pErr *roachpb.Error)
}

type migrateLockTablePool struct {
	requests chan migrateLockTableRequest
	wg       sync.WaitGroup
	stopper  *stop.Stopper
	ir       intentResolver
	db       *kv.DB
	clock    *hlc.Clock
	done     chan bool
	status   [concurrentMigrateLockTableRequests]int64
	finished uint64

	mu struct {
		syncutil.Mutex

		errorCount  int
		combinedErr error
	}
}

func (m *migrateLockTablePool) runMigrateRequestsForRanges(
	ctx context.Context, ri rangeIterator, concurrentRequests int,
) (int, error) {
	var numMigratedRanges int
	m.wg.Add(concurrentRequests)
	for i := 0; i < concurrentRequests; i++ {
		idx := i // Copy for closure below.
		taskName := fmt.Sprintf("migrate-lock-table-%d", i)
		if err := m.stopper.RunAsyncTask(ctx, taskName, func(ctx context.Context) {
			m.run(ctx, idx)
		}); err != nil {
			return 0, err
		}
	}
	m.startStatusLogger(ctx)
	defer m.wg.Wait()
	defer m.stopStatusLogger()
	rs := roachpb.RSpan{Key: roachpb.RKeyMin, EndKey: roachpb.RKeyMax}
	for ri.Seek(ctx, roachpb.RKeyMin, kvcoord.Ascending); ri.Valid(); ri.Next(ctx) {
		desc := ri.Desc()
		start, end := desc.StartKey, desc.EndKey

		{
			// Enqueue a request for range local keys.
			rangeKeyStart := keys.MakeRangeKeyPrefix(desc.StartKey)
			rangeKeyEnd := keys.MakeRangeKeyPrefix(desc.EndKey)

			request := migrateLockTableRequest{
				start:        rangeKeyStart,
				end:          rangeKeyEnd,
				debugRangeID: desc.RangeID,
			}
			select {
			case m.requests <- request:
			case <-ctx.Done():
				return numMigratedRanges, errors.Wrap(ctx.Err(), "lock table migration canceled")
			}
		}

		// See if this range's global keys need a migration. Range-local keys always
		// need a migration, so we issue the above request regardless.
		if ignoreSeparatedIntentsMigrationForRange(start, end) {
			numMigratedRanges++
			continue
		}

		{
			// Enqueue a request for the range's global keys.
			startKeyRaw := desc.StartKey.AsRawKey()
			if bytes.Compare(desc.StartKey, keys.LocalMax) < 0 {
				startKeyRaw = keys.LocalMax
			}
			request := migrateLockTableRequest{
				start:        startKeyRaw,
				end:          end.AsRawKey(),
				debugRangeID: desc.RangeID,
			}
			select {
			case m.requests <- request:
			case <-ctx.Done():
				return numMigratedRanges, errors.Wrap(ctx.Err(), "lock table migration canceled")
			}
		}

		numMigratedRanges++

		if !ri.NeedAnother(rs) {
			break
		}
	}
	if err := ri.Error(); err != nil {
		log.Errorf(ctx, "error when iterating through ranges in lock table migration: %s", err)
		close(m.requests)
		return numMigratedRanges, err
	}
	close(m.requests)
	return numMigratedRanges, nil
}

func (m *migrateLockTablePool) attemptMigrateRequest(
	ctx context.Context, req *migrateLockTableRequest,
) (nextReq *migrateLockTableRequest, err error) {
	// The barrier command needs to be invoked if it hasn't been invoked on this
	// key range yet. This command does not return a resume span, so once it has
	// returned successfully, it doesn't need to be called again unless there's
	// an error.
	barrierTS := req.barrierTS
	if !req.barrierDone {
		var err error
		barrierTS, err = m.db.Barrier(ctx, req.start, req.end)
		if err != nil {
			return nil, errors.Wrap(err, "error when invoking Barrier command")
		}
	}
	barrierTS.Forward(m.clock.Now())
	req.barrierDone = true

	intents, resumeSpan, err := m.db.ScanInterleavedIntents(ctx, req.start, req.end, barrierTS)
	if err != nil {
		return nil, errors.Wrap(err, "error when invoking ScanInterleavedIntents command")
	}

	txnIntents := make(map[uuid.UUID][]roachpb.Intent)
	for _, intent := range intents {
		txnIntents[intent.Txn.ID] = append(txnIntents[intent.Txn.ID], intent)
	}
	for _, intents := range txnIntents {
		txn := &intents[0].Txn

		// Create a request for a PushTxn request of type PUSH_ABORT. If this
		// transaction is still running, it will abort. The retry of that
		// transaction will then write separated intents.
		h := roachpb.Header{
			Timestamp:    m.clock.Now(),
			UserPriority: roachpb.MinUserPriority,
		}
		pushedTxn, err := m.ir.PushTransaction(ctx, txn, h, roachpb.PUSH_ABORT)
		if err != nil {
			return nil, err.GoError()
		}
		lockUpdates := make([]roachpb.LockUpdate, 0, len(intents))
		for _, intent := range intents {
			resolve := roachpb.MakeLockUpdate(pushedTxn, roachpb.Span{Key: intent.Key})
			lockUpdates = append(lockUpdates, resolve)
		}
		opts := intentresolver.ResolveOptions{Poison: true}
		if err := m.ir.ResolveIntents(ctx, lockUpdates, opts); err != nil {
			return nil, err.GoError()
		}
	}
	if resumeSpan != nil {
		nextReq = req
		nextReq.start = resumeSpan.Key
		nextReq.end = resumeSpan.EndKey
		nextReq.barrierDone = true
		nextReq.barrierTS = barrierTS
	}
	return nextReq, nil
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
		handleError := func(err error) {
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
		}

		nextReq, err := m.attemptMigrateRequest(ctx, retryRequest)

		if err != nil {
			handleError(err)
			continue
		} else {
			retryRequest = nextReq
			if nextReq == nil {
				// This range has been migrated.
				atomic.AddUint64(&m.finished, 1)
			}
			retryAttempt = 0
			atomic.StoreInt64(statusSlot, 0)
		}
	}
}

func (m *migrateLockTablePool) startStatusLogger(ctx context.Context) {
	m.done = make(chan bool)
	m.wg.Add(1)
	_ = m.stopper.RunAsyncTask(ctx, "migrate-lock-table-status", m.runStatusLogger)
}

func (m *migrateLockTablePool) stopStatusLogger() {
	close(m.done)
}

func (m *migrateLockTablePool) runStatusLogger(ctx context.Context) {
	defer m.wg.Done()
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

			finished := atomic.LoadUint64(&m.finished)
			log.Infof(ctx, "%d ranges have completed lock table migration", finished)
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

// rangeIterator provides a not-necessarily-transactional view of KV ranges
// spanning a key range.
type rangeIterator interface {
	Desc() *roachpb.RangeDescriptor
	Error() error
	NeedAnother(rs roachpb.RSpan) bool
	Next(ctx context.Context)
	Seek(ctx context.Context, key roachpb.RKey, scanDir kvcoord.ScanDirection)
	Valid() bool
}

// ignoreSeparatedIntentsMigrationForRange returns true if the migration should
// be skipped for this range. Only returns true for range containing timeseries
// keys; those ranges are guaranteed to not contain intents.
func ignoreSeparatedIntentsMigrationForRange(start, end roachpb.RKey) bool {
	return bytes.HasPrefix(start, keys.TimeseriesPrefix) && bytes.HasPrefix(end, keys.TimeseriesPrefix)
}

func runSeparatedIntentsMigration(
	ctx context.Context,
	clock *hlc.Clock,
	stopper *stop.Stopper,
	db *kv.DB,
	ri rangeIterator,
	ir intentResolver,
) error {
	workerPool := migrateLockTablePool{
		requests: make(chan migrateLockTableRequest, concurrentMigrateLockTableRequests),
		stopper:  stopper,
		db:       db,
		ir:       ir,
		clock:    clock,
	}
	migratedRanges, err := workerPool.runMigrateRequestsForRanges(ctx, ri, concurrentMigrateLockTableRequests)
	if err != nil {
		return err
	}

	if workerPool.mu.combinedErr != nil {
		return workerPool.mu.combinedErr
	}

	log.Infof(ctx, "finished lock table migrations for %d ranges", migratedRanges)
	return nil
}

func separatedIntentsMigration(
	ctx context.Context, cv clusterversion.ClusterVersion, deps migration.SystemDeps,
) error {
	ir := intentresolver.New(intentresolver.Config{
		Clock:                deps.DB.Clock(),
		Stopper:              deps.Stopper,
		RangeDescriptorCache: deps.DistSender.RangeDescriptorCache(),
		DB:                   deps.DB,
	})
	ri := kvcoord.NewRangeIterator(deps.DistSender)

	return runSeparatedIntentsMigration(ctx, deps.DB.Clock(), deps.Stopper, deps.DB, ri, ir)
}

func postSeparatedIntentsMigration(
	ctx context.Context, cv clusterversion.ClusterVersion, deps migration.SystemDeps,
) error {
	var batchIdx, numMigratedRanges int
	init := func() { batchIdx, numMigratedRanges = 1, 0 }

	// Issue no-op Migrate commands to all ranges. This has the only
	// purpose of clearing out any orphaned replicas, preventing interleaved
	// intents in them from resurfacing.
	if err := deps.Cluster.IterateRangeDescriptors(ctx, defaultPageSize, init, func(descriptors ...roachpb.RangeDescriptor) error {
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
			if err := deps.DB.Migrate(ctx, start, end, cv.Version); err != nil {
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

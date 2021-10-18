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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/intentresolver"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// The number of concurrent lock table migration related requests (eg. barrier,
// ScanInterleavedIntents) to run, as a multiplier of the number of nodes. If
// this cluster has 15 nodes, 15 * concurrentMigrateLockTableRequests requests
// will be executed at once.
//
// Note that some of the requests (eg. ScanInterleavedIntents) do throttling on
// the receiving end, to reduce the chances of a large number of requests
// being directed at a few nodes.
const concurrentMigrateLockTableRequests = 4

// The maximum number of times to retry a migrateLockTableRequest before failing
// the migration.
const migrateLockTableRetries = 3

// migrateLockTableRequest represents migration of one slice of the keyspace. As
// part of this request, multiple non-transactional requests would need to be
// run: a Barrier, a ScanInterleavedIntents, then multiple txn pushes and intent
// resolutions.
//
// One or more requests will correspond to one range at the time of running the
// IterateRangeDescriptors command. If range boundaries change during the
// course of the migration, that is okay as the migration logic does not rely on
// that assumption. All of the requests required to migrate a range are bundled
// in a migrateLockTableRange.
type migrateLockTableRequest struct {
	start, end  roachpb.Key
	barrierDone bool
	barrierTS   hlc.Timestamp
}

// migrateLockTableRange bundles the migrateLockTableRequests for a given range.
// The debugRangeID is the range ID for this range at the time of the range
// descriptor iteration, and is present solely for observability / logging
// purposes.
type migrateLockTableRange struct {
	debugRangeID roachpb.RangeID
	rangeStart   roachpb.RKey
	local        *migrateLockTableRequest
	global       *migrateLockTableRequest
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
	stopper *stop.Stopper
	ir      intentResolver
	db      *kv.DB
	clock   *hlc.Clock
	done    chan bool
	wg      sync.WaitGroup
	status  []int64

	ranges         chan migrateLockTableRange
	finishedAtomic uint64
	total          uint64

	mu struct {
		syncutil.Mutex

		lowWater    []roachpb.RKey
		errorCount  int
		combinedErr error
	}

	job *jobs.Job
}

func (m *migrateLockTablePool) runMigrateRequestsForRanges(
	ctx context.Context, ri rangeIterator, concurrentRequests int, startFrom roachpb.RKey,
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
	for ri.Seek(ctx, startFrom, kvcoord.Ascending); ri.Valid(); ri.Next(ctx) {
		desc := ri.Desc()
		start, end := desc.StartKey, desc.EndKey

		request := migrateLockTableRange{
			debugRangeID: desc.RangeID,
			rangeStart:   desc.StartKey,
		}

		request.local = &migrateLockTableRequest{
			start: keys.MakeRangeKeyPrefix(desc.StartKey),
			end:   keys.MakeRangeKeyPrefix(desc.EndKey),
		}

		// See if this range's global keys need a migration. Range-local keys always
		// need a migration, so we issue the above request regardless.
		if !ignoreSeparatedIntentsMigrationForRange(start, end) {
			startKeyRaw := desc.StartKey.AsRawKey()
			if bytes.Compare(desc.StartKey, keys.LocalMax) < 0 {
				startKeyRaw = keys.LocalMax
			}
			request.global = &migrateLockTableRequest{
				start: startKeyRaw,
				end:   end.AsRawKey(),
			}
		}

		select {
		case m.ranges <- request:
		case <-ctx.Done():
			return numMigratedRanges, errors.Wrap(ctx.Err(), "lock table migration canceled")
		}

		numMigratedRanges++

		if !ri.NeedAnother(rs) {
			break
		}
	}
	if err := ri.Error(); err != nil {
		log.Errorf(ctx, "error when iterating through ranges in lock table migration: %s", err)
		close(m.ranges)
		return numMigratedRanges, err
	}
	close(m.ranges)
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

	var currentRange *migrateLockTableRange
	retryAttempt := 0
	statusSlot := &m.status[workerIdx]
	atomic.StoreInt64(statusSlot, 0)

	for {
		if currentRange == nil {
			// Pull a new request out of the channel.
			select {
			case r, ok := <-m.ranges:
				if !ok {
					return
				}
				currentRange = &r
				retryAttempt = 0
			case <-ctx.Done():
				log.Warningf(ctx, "lock table migration canceled")
				return
			}
		}

		if ctx.Err() != nil {
			log.Warningf(ctx, "lock table migration canceled on range r%d", currentRange.debugRangeID)
			return
		}

		atomic.StoreInt64(statusSlot, int64(currentRange.debugRangeID))
		handleError := func(err error) {
			log.Errorf(ctx, "error when running migrate lock table command for range r%d: %s",
				currentRange.debugRangeID, err)
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
				currentRange = nil
				atomic.StoreInt64(statusSlot, 0)
			}
		}

		if currentRange.local != nil {
			nextReq, err := m.attemptMigrateRequest(ctx, currentRange.local)
			if err != nil {
				handleError(err)
				continue
			} else {
				currentRange.local = nextReq
			}
		}

		if currentRange.global != nil {
			nextReq, err := m.attemptMigrateRequest(ctx, currentRange.global)
			if err != nil {
				handleError(err)
				continue
			} else {
				currentRange.global = nextReq
			}
		}

		// If we made it here, neither one error'ed, so reset the counter.
		retryAttempt = 0
		atomic.StoreInt64(statusSlot, 0)

		if currentRange.local == nil && currentRange.global == nil {
			// This range has been fully migrated.
			atomic.AddUint64(&m.finishedAtomic, 1)
			m.mu.Lock()
			m.mu.lowWater[workerIdx] = currentRange.rangeStart
			m.mu.Unlock()
			currentRange = nil
		}
	}
}

func (m *migrateLockTablePool) lowWaterMark() roachpb.RKey {
	if len(m.mu.lowWater) < 1 {
		return roachpb.RKeyMin
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	min := m.mu.lowWater[0]
	for i := range m.mu.lowWater {
		if i > 0 && m.mu.lowWater[i].Less(min) {
			min = m.mu.lowWater[i]
		}
	}
	return min
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

	var lastProgress time.Time
	for {
		select {
		case <-ticker.C:
			var ranges strings.Builder
			for i := 0; i < len(m.status); i++ {
				rangeID := atomic.LoadInt64(&m.status[i])
				if rangeID == 0 {
					continue
				}
				if ranges.Len() != 0 {
					fmt.Fprintf(&ranges, ", ")
				}
				fmt.Fprintf(&ranges, "%s", roachpb.RangeID(rangeID))
			}

			finished := atomic.LoadUint64(&m.finishedAtomic)
			total := m.total // TODO(dt): count in background and load via atomic.
			lowWaterMark := m.lowWaterMark()
			log.Infof(ctx, "%d of %d ranges, up to %s, have completed lock table migration", finished, total, lowWaterMark)
			if ranges.Len() > 0 {
				log.Infof(ctx, "currently migrating lock table on ranges %s", ranges.String())
			}
			if m.job != nil && timeutil.Since(lastProgress) > 30*time.Second {
				lastProgress = timeutil.Now()
				if err := m.job.FractionProgressed(ctx, nil, func(ctx context.Context, details jobspb.ProgressDetails) float32 {
					prog := details.(*jobspb.Progress_Migration).Migration
					prog.Watermark = lowWaterMark
					if total > 0 && finished <= total {
						return float32(finished) / float32(total)
					} else if finished > total {
						return 1.0
					}
					return 0.0
				}); err != nil {
					log.Warningf(ctx, "failed to update progress: %v", err)
				}
			}

		case <-m.done:
			return
		case <-ctx.Done():
			return
		}
	}
}

func countTotalRanges(ctx context.Context, ri rangeIterator) (uint64, error) {
	var total uint64
	rs := roachpb.RSpan{Key: roachpb.RKeyMin, EndKey: roachpb.RKeyMax}
	for ri.Seek(ctx, roachpb.RKeyMin, kvcoord.Ascending); ri.Valid(); ri.Next(ctx) {
		total++
		if !ri.NeedAnother(rs) {
			break
		}
	}
	return total, ri.Error()
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
	numNodes int,
	expectedRanges uint64,
	job *jobs.Job,
) error {
	concurrentRequests := concurrentMigrateLockTableRequests * numNodes
	workerPool := migrateLockTablePool{
		ranges:  make(chan migrateLockTableRange, concurrentRequests),
		stopper: stopper,
		db:      db,
		ir:      ir,
		clock:   clock,
		status:  make([]int64, concurrentRequests),
		job:     job,
		total:   expectedRanges,
	}

	workerPool.mu.lowWater = make([]roachpb.RKey, concurrentRequests)
	for i := range workerPool.mu.lowWater {
		workerPool.mu.lowWater[i] = roachpb.RKeyMin
	}

	startFrom := roachpb.RKeyMin
	if job != nil {
		if p := job.Progress().Details; p != nil {
			if prog := p.(*jobspb.Progress_Migration); prog != nil && len(prog.Migration.Watermark) > 0 {
				startFrom = prog.Migration.Watermark
				log.Infof(ctx, "resuming separated intent migraiton from persisted progress position %s", startFrom)
			}
		}
	}

	migratedRanges, err := workerPool.runMigrateRequestsForRanges(ctx, ri, concurrentRequests, startFrom)
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
	ctx context.Context, cv clusterversion.ClusterVersion, deps migration.SystemDeps, job *jobs.Job,
) error {
	ir := intentresolver.New(intentresolver.Config{
		Clock:                deps.DB.Clock(),
		Stopper:              deps.Stopper,
		RangeDescriptorCache: deps.DistSender.RangeDescriptorCache(),
		DB:                   deps.DB,
	})
	ri := kvcoord.NewRangeIterator(deps.DistSender)

	numNodes, err := deps.Cluster.NumNodes(ctx)
	if err != nil {
		return err
	}

	// TODO(dt): count this in background instead on own iterator.
	expected, err := countTotalRanges(ctx, ri)
	if err != nil {
		return err
	}

	return runSeparatedIntentsMigration(ctx, deps.DB.Clock(), deps.Stopper, deps.DB, ri, ir, numNodes, expected, job)
}

func postSeparatedIntentsMigration(
	ctx context.Context, cv clusterversion.ClusterVersion, deps migration.SystemDeps, _ *jobs.Job,
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
			// Try running Migrate on each range 5 times before failing the migration.
			err := retry.WithMaxAttempts(ctx, base.DefaultRetryOptions(), 5, func() error {
				err := deps.DB.Migrate(ctx, start, end, cv.Version)
				if err != nil {
					log.Infof(ctx, "[batch %d/??] error when running no-op Migrate on range r%d: %s", batchIdx, desc.RangeID, err)
				}
				return err
			})
			if err != nil {
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

// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowexec

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/backfill"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// indexBackfiller is a processor that backfills new indexes.
type indexBackfiller struct {
	backfill.IndexBackfiller

	desc catalog.TableDescriptor

	spec execinfrapb.BackfillerSpec

	flowCtx     *execinfra.FlowCtx
	processorID int32

	filter backfill.MutationFilter
}

var _ execinfra.Processor = &indexBackfiller{}

var backfillerBufferSize = settings.RegisterByteSizeSetting(
	settings.ApplicationLevel,
	"schemachanger.backfiller.buffer_size",
	"the initial size of the BulkAdder buffer handling index backfills",
	32<<20,
)

var backfillerMaxBufferSize = settings.RegisterByteSizeSetting(
	settings.ApplicationLevel,
	"schemachanger.backfiller.max_buffer_size",
	"the maximum size of the BulkAdder buffer handling index backfills",
	512<<20,
)

// indexBackfillIngestConcurrency is the number of goroutines to use for
// the ingestion step of the index backfiller; these are the goroutines
// that write the index entries in bulk. Since that is mostly I/O bound,
// adding concurrency allows some of the computational work to occur in
// parallel.
var indexBackfillIngestConcurrency = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"bulkio.index_backfill.ingest_concurrency",
	"the number of goroutines to use for bulk adding index entries",
	2,
	settings.PositiveInt, /* validateFn */
)

func newIndexBackfiller(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.BackfillerSpec,
) (*indexBackfiller, error) {
	indexBackfillerMon := execinfra.NewMonitor(ctx, flowCtx.Cfg.BackfillerMonitor,
		"index-backfill-mon")
	ib := &indexBackfiller{
		desc:        flowCtx.TableDescriptor(ctx, &spec.Table),
		spec:        spec,
		flowCtx:     flowCtx,
		processorID: processorID,
		filter:      backfill.IndexMutationFilter,
	}

	if err := ib.IndexBackfiller.InitForDistributedUse(ctx, flowCtx, ib.desc,
		ib.spec.IndexesToBackfill, indexBackfillerMon); err != nil {
		return nil, err
	}

	return ib, nil
}

func (ib *indexBackfiller) OutputTypes() []*types.T {
	// No output types.
	return nil
}

func (ib *indexBackfiller) MustBeStreaming() bool {
	return false
}

// indexEntryBatch represents a "batch" of index entries which are constructed
// and sent for ingestion. Breaking up the index entries into these batches
// serves for better progress reporting as explained in the ingestIndexEntries
// method.
type indexEntryBatch struct {
	indexEntries         []rowenc.IndexEntry
	completedSpan        roachpb.Span
	memUsedBuildingBatch int64
}

// constructIndexEntries is responsible for constructing the index entries of
// all the spans assigned to the processor. It streams batches of constructed
// index entries over the indexEntriesCh.
func (ib *indexBackfiller) constructIndexEntries(
	ctx context.Context, indexEntriesCh chan indexEntryBatch,
) error {
	var memUsedBuildingBatch int64
	var err error
	var entries []rowenc.IndexEntry
	for i := range ib.spec.Spans {
		log.VEventf(ctx, 2, "index backfiller starting span %d of %d: %s",
			i+1, len(ib.spec.Spans), ib.spec.Spans[i])
		todo := ib.spec.Spans[i]
		for todo.Key != nil {
			startKey := todo.Key
			readAsOf := ib.spec.ReadAsOf
			if readAsOf.IsEmpty() { // old gateway
				readAsOf = ib.spec.WriteAsOf
			}
			todo.Key, entries, memUsedBuildingBatch, err = ib.buildIndexEntryBatch(ctx, todo,
				readAsOf)
			if err != nil {
				return err
			}

			// Identify the Span for which we have constructed index entries. This is
			// used for reporting progress and updating the job details.
			completedSpan := ib.spec.Spans[i]
			if todo.Key != nil {
				completedSpan.Key = startKey
				completedSpan.EndKey = todo.Key
			}

			log.VEventf(ctx, 2, "index entries built for span %s", completedSpan)
			indexBatch := indexEntryBatch{completedSpan: completedSpan, indexEntries: entries,
				memUsedBuildingBatch: memUsedBuildingBatch}
			// Send index entries to be ingested into storage.
			select {
			case indexEntriesCh <- indexBatch:
			case <-ctx.Done():
				return ctx.Err()
			}

			knobs := ib.flowCtx.Cfg.TestingKnobs
			// Block until the current index entry batch has been ingested. Ingested
			// does not mean written to storage, unless we force a flush after every
			// batch.
			if knobs.SerializeIndexBackfillCreationAndIngestion != nil {
				<-knobs.SerializeIndexBackfillCreationAndIngestion
			}
		}
	}

	return nil
}

// ingestIndexEntries adds the batches of built index entries to the buffering
// adder and reports progress back to the coordinator node.
func (ib *indexBackfiller) ingestIndexEntries(
	ctx context.Context,
	indexEntryCh <-chan indexEntryBatch,
	progCh chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
) error {
	ctx, span := tracing.ChildSpan(ctx, "ingestIndexEntries")
	defer span.Finish()

	minBufferSize := backfillerBufferSize.Get(&ib.flowCtx.Cfg.Settings.SV)
	maxBufferSize := func() int64 { return backfillerMaxBufferSize.Get(&ib.flowCtx.Cfg.Settings.SV) }
	opts := kvserverbase.BulkAdderOptions{
		Name:                     ib.desc.GetName() + " backfill",
		MinBufferSize:            minBufferSize,
		MaxBufferSize:            maxBufferSize,
		SkipDuplicates:           ib.ContainsInvertedIndex(),
		BatchTimestamp:           ib.spec.ReadAsOf,
		InitialSplitsIfUnordered: int(ib.spec.InitialSplits),
		WriteAtBatchTimestamp:    ib.spec.WriteAtBatchTimestamp,
	}
	adder, err := ib.flowCtx.Cfg.BulkAdder(ctx, ib.flowCtx.Cfg.DB.KV(), ib.spec.WriteAsOf, opts)
	if err != nil {
		return err
	}
	defer adder.Close(ctx)

	// Synchronizes read and write access on completedSpans which is updated on a
	// BulkAdder flush, but is read when progress is being sent back to the
	// coordinator.
	mu := struct {
		syncutil.Mutex
		completedSpans []roachpb.Span
		addedSpans     []roachpb.Span
	}{}

	// When the bulk adder flushes, the spans which were previously marked as
	// "added" can now be considered "completed", and be sent back to the
	// coordinator node as part of the next progress report.
	adder.SetOnFlush(func(_ kvpb.BulkOpSummary) {
		mu.Lock()
		defer mu.Unlock()
		mu.completedSpans = append(mu.completedSpans, mu.addedSpans...)
		mu.addedSpans = nil
	})

	pushProgress := func() {
		mu.Lock()
		var prog execinfrapb.RemoteProducerMetadata_BulkProcessorProgress
		prog.CompletedSpans = append(prog.CompletedSpans, mu.completedSpans...)
		mu.completedSpans = nil
		mu.Unlock()

		progCh <- prog
	}

	// stopProgress will be closed when there is no more progress to report.
	stopProgress := make(chan struct{})
	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		tick := time.NewTicker(ib.getProgressReportInterval())
		defer tick.Stop()
		done := ctx.Done()
		for {
			select {
			case <-done:
				return ctx.Err()
			case <-stopProgress:
				return nil
			case <-tick.C:
				pushProgress()
			}
		}
	})

	g.GoCtx(func(ctx context.Context) error {
		defer close(stopProgress)

		for indexBatch := range indexEntryCh {
			for _, indexEntry := range indexBatch.indexEntries {
				if err := adder.Add(ctx, indexEntry.Key, indexEntry.Value.RawBytes); err != nil {
					return ib.wrapDupError(ctx, err)
				}
			}

			// Once ALL the KVs for an indexBatch have been added, we can consider the
			// span representing this indexBatch as "added". This span will be part of
			// the set of completed spans on the next bulk adder flush.
			mu.Lock()
			mu.addedSpans = append(mu.addedSpans, indexBatch.completedSpan)
			mu.Unlock()

			// After the index KVs have been copied to the underlying BulkAdder, we can
			// free the memory which was accounted when building the index entries of the
			// current chunk.
			indexBatch.indexEntries = nil
			ib.ShrinkBoundAccount(ctx, indexBatch.memUsedBuildingBatch)

			knobs := &ib.flowCtx.Cfg.TestingKnobs
			if knobs.BulkAdderFlushesEveryBatch {
				if err := adder.Flush(ctx); err != nil {
					return ib.wrapDupError(ctx, err)
				}
				pushProgress()
			}

			if knobs.RunAfterBackfillChunk != nil {
				knobs.RunAfterBackfillChunk()
			}

			// Unblock the index creation of the next batch once it has been ingested.
			if knobs.SerializeIndexBackfillCreationAndIngestion != nil {
				knobs.SerializeIndexBackfillCreationAndIngestion <- struct{}{}
			}
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return err
	}

	if err := adder.Flush(ctx); err != nil {
		return ib.wrapDupError(ctx, err)
	}

	// Push the final set of completed spans as progress.
	pushProgress()

	return nil
}

func (ib *indexBackfiller) runBackfill(
	ctx context.Context, progCh chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
) error {
	const indexEntriesChBufferSize = 10
	ingestConcurrency := indexBackfillIngestConcurrency.Get(&ib.flowCtx.Cfg.Settings.SV)

	// Used to send index entries to the KV layer.
	indexEntriesCh := make(chan indexEntryBatch, indexEntriesChBufferSize)

	// This group holds the go routines that are responsible for producing index
	// entries and ingesting the KVs into storage.
	group := ctxgroup.WithContext(ctx)

	// Construct index entries for the spans.
	group.GoCtx(func(ctx context.Context) error {
		defer close(indexEntriesCh)
		ctx, span := tracing.ChildSpan(ctx, "buildIndexEntries")
		defer span.Finish()
		err := ib.constructIndexEntries(ctx, indexEntriesCh)
		if err != nil {
			return errors.Wrap(err, "failed to construct index entries during backfill")
		}
		return nil
	})

	// Ingest the index entries that are emitted to the chan. We use multiple
	// goroutines since ingestion is mostly I/O bound, making it easier to
	// perform the computational work concurrently.
	for range ingestConcurrency {
		group.GoCtx(func(ctx context.Context) error {
			err := ib.ingestIndexEntries(ctx, indexEntriesCh, progCh)
			if err != nil {
				return errors.Wrap(err, "failed to ingest index entries during backfill")
			}
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return err
	}

	return nil
}

func (ib *indexBackfiller) Run(ctx context.Context, output execinfra.RowReceiver) {
	opName := "indexBackfillerProcessor"
	ctx = logtags.AddTag(ctx, opName, int(ib.spec.Table.ID))
	ctx, span := execinfra.ProcessorSpan(ctx, ib.flowCtx, opName, ib.processorID)
	defer span.Finish()
	defer output.ProducerDone()
	defer execinfra.SendTraceData(ctx, ib.flowCtx, output)
	defer ib.Close(ctx)

	progCh := make(chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress)
	var err error
	// We don't have to worry about this go routine leaking because next we loop
	// over progCh which is closed only after the go routine returns.
	go func() {
		defer close(progCh)
		err = ib.runBackfill(ctx, progCh)
	}()

	for prog := range progCh {
		// Take a copy so that we can send the progress address to the output processor.
		p := prog
		if p.CompletedSpans != nil {
			log.VEventf(ctx, 2, "sending coordinator completed spans: %+v", p.CompletedSpans)
		}
		output.Push(nil, &execinfrapb.ProducerMetadata{BulkProcessorProgress: &p})
	}

	if err != nil {
		output.Push(nil, &execinfrapb.ProducerMetadata{Err: err})
		return
	}
}

func (ib *indexBackfiller) wrapDupError(ctx context.Context, orig error) error {
	if orig == nil {
		return nil
	}
	var typed *kvserverbase.DuplicateKeyError
	if !errors.As(orig, &typed) {
		return orig
	}

	desc, err := ib.desc.MakeFirstMutationPublic()
	if err != nil {
		return err
	}
	v := &roachpb.Value{RawBytes: typed.Value}
	return row.NewUniquenessConstraintViolationError(ctx, desc, typed.Key, v)
}

const indexBackfillProgressReportInterval = 10 * time.Second

func (ib *indexBackfiller) getProgressReportInterval() time.Duration {
	knobs := &ib.flowCtx.Cfg.TestingKnobs
	if knobs.IndexBackfillProgressReportInterval > 0 {
		return knobs.IndexBackfillProgressReportInterval
	}

	return indexBackfillProgressReportInterval
}

// buildIndexEntryBatch constructs the index entries for a single indexBatch.
func (ib *indexBackfiller) buildIndexEntryBatch(
	tctx context.Context, sp roachpb.Span, readAsOf hlc.Timestamp,
) (roachpb.Key, []rowenc.IndexEntry, int64, error) {
	knobs := &ib.flowCtx.Cfg.TestingKnobs
	if knobs.RunBeforeBackfillChunk != nil {
		if err := knobs.RunBeforeBackfillChunk(sp); err != nil {
			return nil, nil, 0, err
		}
	}

	var memUsedBuildingBatch int64
	var key roachpb.Key
	var entries []rowenc.IndexEntry

	br := indexBatchRetry{
		nextChunkSize: ib.spec.ChunkSize,
		// Memory used while building index entries is released by another goroutine
		// once those entries are processed. However, with wide rows and/or limited
		// memory, memory pressure issues can arise. To address this, we retry with
		// a smaller chunk size after an exponential backoff. Although the maximum
		// wait time between retries may seem lengthy, it is significantly faster
		// than allowing the entire schema operation to fail and restart.
		retryOpts: retry.Options{
			InitialBackoff: 500 * time.Millisecond,
			Multiplier:     2,
			MaxRetries:     15,
			MaxBackoff:     1 * time.Minute,
		},
		resetForNextAttempt: func(ctx context.Context) error {
			if entries != nil {
				return errors.AssertionFailedf("expected entries to be nil on error")
			}
			// There is no need to track the memory we allocated in the last failed
			// attempt. We will allocate new memory on the next iteration.
			if memUsedBuildingBatch > 0 {
				ib.ShrinkBoundAccount(ctx, memUsedBuildingBatch)
				memUsedBuildingBatch = 0
			}
			return nil
		},
	}
	br.buildIndexChunk = func(ctx context.Context, txn isql.Txn) error {
		if err := txn.KV().SetFixedTimestamp(ctx, readAsOf); err != nil {
			return err
		}
		// If this is a retry that succeeds, save the smaller chunk size to use as
		// the starting size for the next batch. If memory pressure occurred with a
		// larger batch size, it's prudent not to revert to it for subsequent batches.
		ib.spec.ChunkSize = br.nextChunkSize

		// TODO(knz): do KV tracing in DistSQL processors.
		var err error
		entries, key, memUsedBuildingBatch, err = ib.BuildIndexEntriesChunk(
			ctx, txn.KV(), ib.desc, sp, br.nextChunkSize, false, /* traceKV */
		)
		return err
	}

	ctx, traceSpan := tracing.ChildSpan(tctx, "indexBatch")
	defer traceSpan.Finish()
	start := timeutil.Now()
	if err := br.buildBatchWithRetry(ctx, ib.flowCtx.Cfg.DB); err != nil {
		return nil, nil, 0, err
	}
	prepTime := timeutil.Since(start)
	log.VEventf(ctx, 3, "index backfill stats: entries %d, prepare %+v",
		len(entries), prepTime)

	return key, entries, memUsedBuildingBatch, nil
}

// Resume is part of the execinfra.Processor interface.
func (ib *indexBackfiller) Resume(output execinfra.RowReceiver) {
	panic("not implemented")
}

// Close is part of the execinfra.Processor interface.
func (ib *indexBackfiller) Close(ctx context.Context) {
	ib.IndexBackfiller.Close(ctx)
}

type indexBatchRetry struct {
	nextChunkSize       int64
	buildIndexChunk     func(ctx context.Context, txn isql.Txn) error
	resetForNextAttempt func(ctx context.Context) error
	retryOpts           retry.Options
}

// buildBatchWithRetry constructs a batch of index entries with a retry mechanism
// to handle out-of-memory errors.
func (b *indexBatchRetry) buildBatchWithRetry(ctx context.Context, db isql.DB) error {
	r := retry.StartWithCtx(ctx, b.retryOpts)
	for {
		if err := db.Txn(ctx, b.buildIndexChunk, isql.WithPriority(admissionpb.BulkNormalPri)); err != nil {
			// Retry for any out of memory error. We want to wait for the goroutine
			// that processes the prior batches of index entries to free memory.
			if sqlerrors.IsOutOfMemoryError(err) && b.nextChunkSize > 1 {
				// Callback to clear out any state acquired in the last attempt
				if resetErr := b.resetForNextAttempt(ctx); resetErr != nil {
					return errors.CombineErrors(err, resetErr)
				}

				if !r.Next() {
					// If we have exhausted all retries, fail with the out of memory error.
					return errors.Wrapf(err, "failed after %d retries", r.CurrentAttempt())
				}
				b.nextChunkSize = max(1, b.nextChunkSize/2)
				log.Infof(ctx,
					"out of memory while building index entries; retrying with batch size %d. Silencing error: %v",
					b.nextChunkSize, err)
				continue
			}
			return err
		}
		break // Batch completed successfully, no need for a retry.
	}
	return nil
}

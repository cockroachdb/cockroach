// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/backfill"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// indexBackfiller is a processor that backfills new indexes.
type indexBackfiller struct {
	backfill.IndexBackfiller

	adder kvserverbase.BulkAdder

	desc catalog.TableDescriptor

	spec execinfrapb.BackfillerSpec

	out execinfra.ProcOutputHelper

	flowCtx *execinfra.FlowCtx

	output execinfra.RowReceiver

	filter backfill.MutationFilter
}

var _ execinfra.Processor = &indexBackfiller{}

var backfillerBufferSize = settings.RegisterByteSizeSetting(
	"schemachanger.backfiller.buffer_size", "the initial size of the BulkAdder buffer handling index backfills", 32<<20,
)

var backfillerMaxBufferSize = settings.RegisterByteSizeSetting(
	"schemachanger.backfiller.max_buffer_size", "the maximum size of the BulkAdder buffer handling index backfills", 512<<20,
)

var backfillerBufferIncrementSize = settings.RegisterByteSizeSetting(
	"schemachanger.backfiller.buffer_increment", "the size by which the BulkAdder attempts to grow its buffer before flushing", 32<<20,
)

var backillerSSTSize = settings.RegisterByteSizeSetting(
	"schemachanger.backfiller.max_sst_size", "target size for ingested files during backfills", 16<<20,
)

func newIndexBackfiller(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.BackfillerSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*indexBackfiller, error) {
	indexBackfillerMon := execinfra.NewMonitor(ctx, flowCtx.Cfg.BackfillerMonitor,
		"index-backfill-mon")
	ib := &indexBackfiller{
		desc:    spec.BuildTableDescriptor(),
		spec:    spec,
		flowCtx: flowCtx,
		output:  output,
		filter:  backfill.IndexMutationFilter,
	}

	if err := ib.IndexBackfiller.InitForDistributedUse(ctx, flowCtx, ib.desc,
		indexBackfillerMon); err != nil {
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
			i+1, len(ib.spec.Spans), ib.spec.Spans[i].Span)
		todo := ib.spec.Spans[i].Span
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
			completedSpan := ib.spec.Spans[i].Span
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
	sstSize := func() int64 { return backillerSSTSize.Get(&ib.flowCtx.Cfg.Settings.SV) }
	stepSize := backfillerBufferIncrementSize.Get(&ib.flowCtx.Cfg.Settings.SV)
	opts := kvserverbase.BulkAdderOptions{
		SSTSize:        sstSize,
		MinBufferSize:  minBufferSize,
		MaxBufferSize:  maxBufferSize,
		StepBufferSize: stepSize,
		SkipDuplicates: ib.ContainsInvertedIndex(),
		BatchTimestamp: ib.spec.ReadAsOf,
	}
	adder, err := ib.flowCtx.Cfg.BulkAdder(ctx, ib.flowCtx.Cfg.DB, ib.spec.WriteAsOf, opts)
	if err != nil {
		return err
	}
	ib.adder = adder
	defer ib.adder.Close(ctx)

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
	adder.SetOnFlush(func() {
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
				if err := ib.adder.Add(ctx, indexEntry.Key, indexEntry.Value.RawBytes); err != nil {
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
				if err := ib.adder.Flush(ctx); err != nil {
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

	if err := ib.adder.Flush(ctx); err != nil {
		return ib.wrapDupError(ctx, err)
	}

	// Push the final set of completed spans as progress.
	pushProgress()

	return nil
}

func (ib *indexBackfiller) runBackfill(
	ctx context.Context, progCh chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
) error {
	// Used to send index entries to the KV layer.
	indexEntriesCh := make(chan indexEntryBatch, 10)

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

	// Ingest the index entries that are emitted to the chan.
	group.GoCtx(func(ctx context.Context) error {
		err := ib.ingestIndexEntries(ctx, indexEntriesCh, progCh)
		if err != nil {
			return errors.Wrap(err, "failed to ingest index entries during backfill")
		}
		return nil
	})

	if err := group.Wait(); err != nil {
		return err
	}

	return nil
}

func (ib *indexBackfiller) Run(ctx context.Context) {
	opName := "indexBackfillerProcessor"
	ctx = logtags.AddTag(ctx, opName, int(ib.spec.Table.ID))
	ctx, span := execinfra.ProcessorSpan(ctx, opName)
	defer span.Finish()
	defer ib.output.ProducerDone()
	defer execinfra.SendTraceData(ctx, ib.output)
	defer ib.Close(ctx)

	progCh := make(chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress)

	semaCtx := tree.MakeSemaContext()
	if err := ib.out.Init(&execinfrapb.PostProcessSpec{}, nil, &semaCtx, ib.flowCtx.NewEvalCtx()); err != nil {
		ib.output.Push(nil, &execinfrapb.ProducerMetadata{Err: err})
		return
	}

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
		ib.output.Push(nil, &execinfrapb.ProducerMetadata{BulkProcessorProgress: &p})
	}

	if err != nil {
		ib.output.Push(nil, &execinfrapb.ProducerMetadata{Err: err})
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

	desc, err := ib.desc.MakeFirstMutationPublic(catalog.IncludeConstraints)
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
	var memUsedBuildingBatch int64
	if knobs.RunBeforeBackfillChunk != nil {
		if err := knobs.RunBeforeBackfillChunk(sp); err != nil {
			return nil, nil, 0, err
		}
	}
	var key roachpb.Key

	ctx, traceSpan := tracing.ChildSpan(tctx, "indexBatch")
	defer traceSpan.Finish()
	start := timeutil.Now()
	var entries []rowenc.IndexEntry
	if err := ib.flowCtx.Cfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		txn.SetFixedTimestamp(ctx, readAsOf)

		// TODO(knz): do KV tracing in DistSQL processors.
		var err error
		entries, key, memUsedBuildingBatch, err = ib.BuildIndexEntriesChunk(ctx, txn, ib.desc, sp,
			ib.spec.ChunkSize, false /*traceKV*/)
		return err
	}); err != nil {
		return nil, nil, 0, err
	}
	prepTime := timeutil.Now().Sub(start)
	log.VEventf(ctx, 3, "index backfill stats: entries %d, prepare %+v",
		len(entries), prepTime)

	return key, entries, memUsedBuildingBatch, nil
}

// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowexec

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/bulk"
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
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	gogotypes "github.com/gogo/protobuf/types"
)

// indexBackfiller is a processor that backfills new indexes.
type indexBackfiller struct {
	backfill.IndexBackfiller

	desc catalog.TableDescriptor

	spec execinfrapb.BackfillerSpec

	flowCtx     *execinfra.FlowCtx
	processorID int32

	filter backfill.MutationFilter

	bulkAdderFactory indexBackfillBulkAdderFactory
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

// indexBackfillIngestConcurrency controls the number of goroutines for the
// ingestion step of the index backfiller. When set to 0 (auto), uses
// GOMAXPROCS/2 for distributed merge, or 2 for the legacy path.
// When set to a positive value, uses that explicit value.
var indexBackfillIngestConcurrency = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"bulkio.index_backfill.ingest_concurrency",
	"the number of goroutines to use for bulk adding index entries: "+
		"0 assigns a reasonable default based on CPU count for distributed merge "+
		"or 2 for the legacy path, >0 assigns the setting value",
	0, // auto mode by default
	settings.NonNegativeInt,
)

var indexBackfillElasticCPUControlEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"bulkio.index_backfill.elastic_control.enabled",
	"determines whether index backfill operations integrate with elastic CPU control",
	true,
)

// indexBackfillSink abstracts the destination for index backfill output so the
// ingestion pipeline can route built KVs either to the legacy BulkAdder path or
// to future sinks (e.g. distributed-merge SST writers) without rewriting the
// DistSQL processor. All sinks share the same Add/Flush/progress contract.
type indexBackfillSink interface {
	// Add enqueues a single KV pair for eventual persistence in the sink-specific
	// backing store.
	Add(ctx context.Context, key roachpb.Key, value []byte) error
	// Flush forces any buffered state to be persisted.
	Flush(ctx context.Context) error
	// Close releases resources owned by the sink. Implementations should be
	// idempotent and safe to call even if Flush returns an error.
	Close(ctx context.Context)
	// SetOnFlush installs a callback that is invoked after the sink writes a
	// batch (mirrors kvserverbase.BulkAdder semantics so existing progress
	// plumbing can be reused).
	SetOnFlush(func(summary kvpb.BulkOpSummary))
	// ConsumeFlushManifests returns any SST manifests produced since the last
	// flush. This is only relevant for sinks that produce SSTs.
	ConsumeFlushManifests() []jobspb.IndexBackfillSSTManifest
}

// indexBackfillBulkAdderFactory mirrors kvserverbase.BulkAdderFactory but is
// injected so tests can swap in fakes and future sinks can reuse the backfiller
// without referencing execinfra.ServerConfig directly.
type indexBackfillBulkAdderFactory func(
	ctx context.Context, writeAsOf hlc.Timestamp, opts kvserverbase.BulkAdderOptions,
) (kvserverbase.BulkAdder, error)

// bulkAdderIndexBackfillSink is the default sink implementation backed by
// kvserverbase.BulkAdder.
type bulkAdderIndexBackfillSink struct {
	kvserverbase.BulkAdder
}

var _ indexBackfillSink = (*bulkAdderIndexBackfillSink)(nil)

// ConsumeFlushManifests implements the indexBackfillSink interface.
func (b *bulkAdderIndexBackfillSink) ConsumeFlushManifests() []jobspb.IndexBackfillSSTManifest {
	// The BulkAdder does not produce SST manifests.
	return nil
}

func newIndexBackfiller(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.BackfillerSpec,
) (*indexBackfiller, error) {
	indexBackfillerMon := execinfra.NewMonitor(
		ctx, flowCtx.Cfg.BackfillerMonitor, mon.MakeName("index-backfill-mon"),
	)
	ib := &indexBackfiller{
		desc:        flowCtx.TableDescriptor(ctx, &spec.Table),
		spec:        spec,
		flowCtx:     flowCtx,
		processorID: processorID,
		filter:      backfill.IndexMutationFilter,
		bulkAdderFactory: func(
			ctx context.Context, writeAsOf hlc.Timestamp, opts kvserverbase.BulkAdderOptions,
		) (kvserverbase.BulkAdder, error) {
			return flowCtx.Cfg.BulkAdder(ctx, flowCtx.Cfg.DB.KV(), writeAsOf, opts)
		},
	}

	if err := ib.IndexBackfiller.InitForDistributedUse(
		ctx, flowCtx, ib.desc, ib.spec.IndexesToBackfill, ib.spec.SourceIndexID, indexBackfillerMon,
	); err != nil {
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
			// Pick an arbitrary timestamp close to now as the read timestamp for the
			// backfill. It's safe to use this timestamp to read even if we've
			// partially backfilled at an earlier timestamp because other writing
			// transactions have been writing at the appropriate timestamps
			// in-between.
			readAsOf := ib.flowCtx.Cfg.DB.KV().Clock().Now().AddDuration(-30 * time.Second)
			if readAsOf.Less(ib.spec.WriteAsOf) {
				readAsOf = ib.spec.WriteAsOf
			}
			todo.Key, entries, memUsedBuildingBatch, err = ib.buildIndexEntryBatch(ctx, todo, readAsOf)
			if err != nil {
				return err
			}

			// Identify the Span for which we have constructed index entries. This is
			// used for reporting progress and updating the job details.
			completedSpan := roachpb.Span{Key: startKey, EndKey: ib.spec.Spans[i].EndKey}
			if todo.Key != nil {
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

func (ib *indexBackfiller) maybeReencodeAndWriteVectorIndexEntry(
	ctx context.Context, tmpEntry *rowenc.IndexEntry, indexEntry *rowenc.IndexEntry,
) (bool, error) {
	indexID, _, err := rowenc.DecodeIndexKeyPrefix(ib.flowCtx.EvalCtx.Codec, ib.desc.GetID(), indexEntry.Key)
	if err != nil {
		return false, err
	}

	vih, ok := ib.VectorIndexes[indexID]
	if !ok {
		return false, nil
	}

	// Initialize the tmpEntry. This will store the input entry that we are encoding
	// so that we can overwrite the indexEntry we were given with the output
	// encoding. This allows us to preserve the initial template indexEntry across
	// transaction retries.
	if tmpEntry.Key == nil {
		tmpEntry.Key = make(roachpb.Key, len(indexEntry.Key))
		tmpEntry.Value.RawBytes = make([]byte, len(indexEntry.Value.RawBytes))
	}
	tmpEntry.Key = append(tmpEntry.Key[:0], indexEntry.Key...)
	tmpEntry.Value.RawBytes = append(tmpEntry.Value.RawBytes[:0], indexEntry.Value.RawBytes...)
	tmpEntry.Family = indexEntry.Family

	firstAttempt := true
	err = ib.flowCtx.Cfg.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		// If the first attempt failed, we need to provide a new buffer to KV for
		// subsequent attempts because KV might still be using the previous buffer.
		var entryBuffer *rowenc.IndexEntry
		if firstAttempt {
			entryBuffer = indexEntry
			firstAttempt = false
		} else {
			entryBuffer = &rowenc.IndexEntry{
				Key: make(roachpb.Key, len(tmpEntry.Key)),
				Value: roachpb.Value{
					RawBytes: make([]byte, len(tmpEntry.Value.RawBytes)),
				},
				Family: tmpEntry.Family,
			}
		}
		entry, err := vih.ReEncodeVector(ctx, txn.KV(), *tmpEntry, entryBuffer)
		if err != nil {
			return err
		}

		if ib.flowCtx.Cfg.TestingKnobs.RunDuringReencodeVectorIndexEntry != nil {
			err = ib.flowCtx.Cfg.TestingKnobs.RunDuringReencodeVectorIndexEntry(txn.KV())
			if err != nil {
				return err
			}
		}

		b := txn.KV().NewBatch()
		// If the backfill job was interrupted and restarted, we may redo work, so allow
		// that we may see duplicate keys here.
		b.CPutAllowingIfNotExists(entry.Key, &entry.Value, entry.Value.TagAndDataBytes())
		return txn.KV().Run(ctx, b)
	})
	if err != nil {
		return false, err
	}

	return true, nil
}

// makeIndexBackfillSink materializes whatever sink the current backfill should
// use (legacy BulkAdder or distributed-merge sink). The choice is driven by
// execinfrapb.BackfillerSpec.
func (ib *indexBackfiller) makeIndexBackfillSink(ctx context.Context) (indexBackfillSink, error) {
	if ib.spec.UseDistributedMergeSink {
		log.Dev.Infof(ctx, "distributed merge: map phase starting with %d spans", len(ib.spec.Spans))

		// Construct the full nodelocal URI using this processors's node ID. The spec
		// stores just the path portion so that each processor writes to its own
		// node's local storage.
		nodeID := ib.flowCtx.NodeID.SQLInstanceID()
		prefix := fmt.Sprintf("nodelocal://%d/%s", nodeID, ib.spec.DistributedMergeFilePrefix)

		checkDuplicates := ib.ContainsUniqueIndex()
		return newSSTIndexBackfillSink(
			ctx, ib.flowCtx, prefix, ib.spec.WriteAsOf, ib.processorID, checkDuplicates)
	}

	minBufferSize := backfillerBufferSize.Get(&ib.flowCtx.Cfg.Settings.SV)
	maxBufferSize := func() int64 { return backfillerMaxBufferSize.Get(&ib.flowCtx.Cfg.Settings.SV) }
	opts := kvserverbase.BulkAdderOptions{
		Name:                     ib.desc.GetName() + " backfill",
		MinBufferSize:            minBufferSize,
		MaxBufferSize:            maxBufferSize,
		SkipDuplicates:           ib.ContainsInvertedIndex(),
		BatchTimestamp:           ib.spec.WriteAsOf,
		InitialSplitsIfUnordered: int(ib.spec.InitialSplits),
		WriteAtBatchTimestamp:    ib.spec.WriteAtBatchTimestamp,
	}

	adderFactory := ib.bulkAdderFactory
	if adderFactory == nil {
		return nil, errors.AssertionFailedf("index backfiller bulk adder factory must be configured")
	}
	adder, err := adderFactory(ctx, ib.spec.WriteAsOf, opts)
	if err != nil {
		return nil, err
	}
	return &bulkAdderIndexBackfillSink{BulkAdder: adder}, nil
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

	sink, err := ib.makeIndexBackfillSink(ctx)
	if err != nil {
		return err
	}
	defer sink.Close(ctx)

	// Synchronizes read and write access on completedSpans which is updated on a
	// BulkAdder flush, but is read when progress is being sent back to the
	// coordinator.
	mu := struct {
		syncutil.Mutex
		completedSpans []roachpb.Span
		addedSpans     []roachpb.Span
		manifests      []jobspb.IndexBackfillSSTManifest
	}{}

	// When the bulk adder flushes, the spans which were previously marked as
	// "added" can now be considered "completed", and be sent back to the
	// coordinator node as part of the next progress report.
	flushAddedSpansAndSSTManifests := func(_ kvpb.BulkOpSummary) {
		mu.Lock()
		defer mu.Unlock()
		mu.completedSpans = append(mu.completedSpans, mu.addedSpans...)
		mu.addedSpans = nil
		mu.manifests = append(mu.manifests, sink.ConsumeFlushManifests()...)
	}
	sink.SetOnFlush(flushAddedSpansAndSSTManifests)

	pushProgress := func() error {
		mu.Lock()
		var prog execinfrapb.RemoteProducerMetadata_BulkProcessorProgress
		prog.CompletedSpans = append(prog.CompletedSpans, mu.completedSpans...)
		mu.completedSpans = nil
		manifests := append([]jobspb.IndexBackfillSSTManifest(nil), mu.manifests...)
		mu.manifests = nil
		mu.Unlock()

		if len(prog.CompletedSpans) == 0 && len(manifests) == 0 {
			return nil
		}
		if len(manifests) > 0 {
			progress := execinfrapb.IndexBackfillMapProgress{
				SSTManifests: manifests,
			}
			any, err := gogotypes.MarshalAny(&progress)
			if err != nil {
				return err
			}
			prog.ProgressDetails = *any
		}

		progCh <- prog
		return nil
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
				if err := pushProgress(); err != nil {
					// Log the error but keep going. We don't want to fail the operation.
					log.Dev.Warningf(ctx, "failed to push progress: %v", err)
				}
			}
		}
	})

	g.GoCtx(func(ctx context.Context) error {
		defer close(stopProgress)

		// Create a pacer for admission control for index entry processing.
		pacer := bulk.NewCPUPacer(ctx, ib.flowCtx.Cfg.DB.KV(), indexBackfillElasticCPUControlEnabled)
		defer pacer.Close()

		var vectorInputEntry rowenc.IndexEntry
		for indexBatch := range indexEntryCh {
			addedToVectorIndex := false
			for _, indexEntry := range indexBatch.indexEntries {
				// Pace the admission control before processing each index entry.
				if _, err := pacer.Pace(ctx); err != nil {
					return err
				}

				// If there is at least one vector index being written, we need to check to see
				// if this IndexEntry is going to a vector index and then re-encode it for that
				// index if so.
				//
				// TODO(mw5h): batch up multiple index entries into a single batch.
				// As is, we insert a single vector per batch, which is very slow.
				if len(ib.VectorIndexes) > 0 {
					isVectorIndex, err := ib.maybeReencodeAndWriteVectorIndexEntry(ctx, &vectorInputEntry, &indexEntry)
					if err != nil {
						return ib.wrapDupError(ctx, err)
					} else if isVectorIndex {
						addedToVectorIndex = true
						continue
					}
				}

				if err := sink.Add(ctx, indexEntry.Key, indexEntry.Value.RawBytes); err != nil {
					return ib.wrapDupError(ctx, err)
				}
			}

			// Once ALL the KVs for an indexBatch have been added, we can consider the
			// span representing this indexBatch as "added". This span will be part of
			// the set of completed spans on the next bulk adder flush.
			mu.Lock()
			mu.addedSpans = append(mu.addedSpans, indexBatch.completedSpan)
			mu.Unlock()
			// Vector indexes don't add to the bulk adder and take a long time to add
			// entries, so flush progress manually after every indexBatch is processed
			// that contained vector index entries.
			if addedToVectorIndex {
				flushAddedSpansAndSSTManifests(kvpb.BulkOpSummary{})
			}

			// After the index KVs have been copied to the underlying BulkAdder, we can
			// free the memory which was accounted when building the index entries of the
			// current chunk.
			indexBatch.indexEntries = nil
			ib.ShrinkBoundAccount(ctx, indexBatch.memUsedBuildingBatch)

			knobs := &ib.flowCtx.Cfg.TestingKnobs
			if knobs.BulkAdderFlushesEveryBatch {
				if err := sink.Flush(ctx); err != nil {
					return ib.wrapDupError(ctx, err)
				}
				if err := pushProgress(); err != nil {
					return err
				}
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
	if err := sink.Flush(ctx); err != nil {
		return ib.wrapDupError(ctx, err)
	}
	// Push the final set of completed spans as progress.
	if err := pushProgress(); err != nil {
		return err
	}

	return nil
}

func (ib *indexBackfiller) runBackfill(
	ctx context.Context, progCh chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
) error {
	const indexEntriesChBufferSize = 10
	ingestConcurrency := indexBackfillIngestConcurrency.Get(&ib.flowCtx.Cfg.Settings.SV)
	if ingestConcurrency == 0 {
		if ib.spec.UseDistributedMergeSink {
			// Auto mode for distributed merge: scale with CPU count.
			ingestConcurrency = max(1, int64(runtime.GOMAXPROCS(0)/2))
		} else {
			// Auto mode for legacy path: use historic default.
			ingestConcurrency = 2
		}
	}

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

	// Handle DuplicateKeyError from within-batch duplicate detection.
	var dupErr *kvserverbase.DuplicateKeyError
	if errors.As(orig, &dupErr) {
		desc, err := ib.desc.MakeFirstMutationPublic()
		if err != nil {
			return err
		}
		v := &roachpb.Value{RawBytes: dupErr.Value}
		return row.NewUniquenessConstraintViolationError(ctx, desc, dupErr.Key, v)
	}

	return orig
}

const indexBackfillProgressReportInterval = 10 * time.Second

func (ib *indexBackfiller) getProgressReportInterval() time.Duration {
	knobs := &ib.flowCtx.Cfg.TestingKnobs
	if knobs.IndexBackfillProgressReportInterval > 0 {
		return knobs.IndexBackfillProgressReportInterval
	}

	return indexBackfillProgressReportInterval
}

// buildIndexEntryBatch constructs the index entries for a single indexBatch for
// the given span. A non-nil resumeKey is returned when there is still work to
// be done in this span (sp.Key < resumeKey < sp.EndKey), which happens if the
// entire span does not fit within the batch size.
func (ib *indexBackfiller) buildIndexEntryBatch(
	tctx context.Context, sp roachpb.Span, readAsOf hlc.Timestamp,
) (resumeKey roachpb.Key, entries []rowenc.IndexEntry, memUsedBuildingBatch int64, err error) {
	knobs := &ib.flowCtx.Cfg.TestingKnobs
	if knobs.RunBeforeBackfillChunk != nil {
		if err := knobs.RunBeforeBackfillChunk(sp); err != nil {
			return nil, nil, 0, err
		}
	}

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
		entries, resumeKey, memUsedBuildingBatch, err = ib.BuildIndexEntriesChunk(
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

	return resumeKey, entries, memUsedBuildingBatch, nil
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
				log.Dev.Infof(ctx,
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

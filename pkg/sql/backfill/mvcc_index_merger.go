// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backfill

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// indexBackfillMergeBatchSize is the maximum number of rows we attempt to merge
// in a single transaction during the merging process.
var indexBackfillMergeBatchSize = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"bulkio.index_backfill.merge_batch_size",
	"the number of rows we merge between temporary and adding indexes in a single batch",
	1000,
	settings.NonNegativeInt, /* validateFn */
)

// indexBackfillMergeBatchBytes is the maximum number of bytes we attempt to
// merge from the temporary index in a single transaction during the merging
// process.
var indexBackfillMergeBatchBytes = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"bulkio.index_backfill.merge_batch_bytes",
	"the max number of bytes we merge between temporary and adding indexes in a single batch",
	16<<20,
	settings.NonNegativeInt,
)

// indexBackfillMergeNumWorkers is the number of parallel merges per node in the
// cluster used for the merge step of the index backfill. It is currently
// default to 4 as higher values didn't seem to improve the index build times in
// the schemachange/index/tpcc/w=1000 roachtest.
var indexBackfillMergeNumWorkers = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"bulkio.index_backfill.merge_num_workers",
	"the number of parallel merges per node in the cluster",
	4,
	settings.PositiveInt,
)

// keyBatch will manage merging a batch of keys. Potentially splitting the batch
// across multiple transactions depending on contention.
type keyBatch struct {
	sourceKeys    []roachpb.Key
	processedKeys int
	batchSize     int
}

// IndexBackfillMerger is a processor that merges entries from the corresponding
// temporary index to a new index.
type IndexBackfillMerger struct {
	processorID    int32
	spec           execinfrapb.IndexBackfillMergerSpec
	desc           catalog.TableDescriptor
	flowCtx        *execinfra.FlowCtx
	muBoundAccount muBoundAccount
}

// OutputTypes is always nil.
func (ibm *IndexBackfillMerger) OutputTypes() []*types.T {
	return nil
}

// MustBeStreaming is always false.
func (ibm *IndexBackfillMerger) MustBeStreaming() bool {
	return false
}

const indexBackfillMergeProgressReportInterval = 10 * time.Second

// Run runs the processor.
func (ibm *IndexBackfillMerger) Run(ctx context.Context, output execinfra.RowReceiver) {
	opName := "IndexBackfillMerger"
	ctx = logtags.AddTag(ctx, opName, int(ibm.spec.Table.ID))
	ctx, span := execinfra.ProcessorSpan(ctx, ibm.flowCtx, opName, ibm.processorID)
	defer span.Finish()
	// This method blocks until all worker goroutines exit, so it's safe to
	// close memory monitoring infra in defers.
	mergerMon := execinfra.NewMonitor(ctx, ibm.flowCtx.Cfg.BackfillerMonitor, "index-backfiller-merger-mon")
	defer mergerMon.Stop(ctx)
	ibm.muBoundAccount.boundAccount = mergerMon.MakeBoundAccount()
	defer func() {
		ibm.muBoundAccount.Lock()
		defer ibm.muBoundAccount.Unlock()
		ibm.muBoundAccount.boundAccount.Close(ctx)
	}()
	defer output.ProducerDone()
	defer execinfra.SendTraceData(ctx, ibm.flowCtx, output)

	mu := struct {
		syncutil.Mutex
		completedSpans   []roachpb.Span
		completedSpanIdx []int32
	}{}

	storeChunkProgress := func(chunk mergeChunk) {
		mu.Lock()
		defer mu.Unlock()
		mu.completedSpans = append(mu.completedSpans, chunk.completedSpan)
		mu.completedSpanIdx = append(mu.completedSpanIdx, chunk.spanIdx)
	}

	getStoredProgressForPush := func() execinfrapb.RemoteProducerMetadata_BulkProcessorProgress {
		var prog execinfrapb.RemoteProducerMetadata_BulkProcessorProgress
		mu.Lock()
		defer mu.Unlock()

		prog.CompletedSpans = append(prog.CompletedSpans, mu.completedSpans...)
		prog.CompletedSpanIdx = append(prog.CompletedSpanIdx, mu.completedSpanIdx...)
		mu.completedSpans, mu.completedSpanIdx = nil, nil
		return prog
	}

	var outputMu syncutil.Mutex
	pushProgress := func() {
		p := getStoredProgressForPush()
		if p.CompletedSpans != nil {
			log.VEventf(ctx, 2, "sending coordinator completed spans: %+v", p.CompletedSpans)
		}
		// Even though the contract of execinfra.RowReceiver says that Push is
		// thread-safe, in reality it's not always the case, so we protect it
		// with a mutex. At the time of writing, the only source of concurrency
		// for Push()ing is present due to testing knobs though.
		outputMu.Lock()
		defer outputMu.Unlock()
		output.Push(nil, &execinfrapb.ProducerMetadata{BulkProcessorProgress: &p})
	}

	numWorkers := int(indexBackfillMergeNumWorkers.Get(&ibm.flowCtx.Cfg.Settings.SV))
	mergeCh := make(chan mergeChunk)
	mergeTimestamp := ibm.spec.MergeTimestamp

	g := ctxgroup.WithContext(ctx)
	runWorker := func(ctx context.Context) error {
		for mergeChunk := range mergeCh {
			err := ibm.merge(ctx, ibm.flowCtx.Codec(), ibm.desc, ibm.spec.TemporaryIndexes[mergeChunk.spanIdx],
				ibm.spec.AddedIndexes[mergeChunk.spanIdx], mergeChunk.keys, mergeChunk.completedSpan)
			if err != nil {
				return err
			}

			storeChunkProgress(mergeChunk)

			// After the keys have been merged, we can free the memory used by the
			// chunk.
			mergeChunk.keys = nil
			ibm.shrinkBoundAccount(ctx, mergeChunk.memUsed)

			if knobs, ok := ibm.flowCtx.Cfg.TestingKnobs.IndexBackfillMergerTestingKnobs.(*IndexBackfillMergerTestingKnobs); ok {
				if knobs != nil {
					if knobs.PushesProgressEveryChunk {
						pushProgress()
					}

					if knobs.RunAfterMergeChunk != nil {
						knobs.RunAfterMergeChunk()
					}
				}
			}
		}
		return nil
	}

	for worker := 0; worker < numWorkers; worker++ {
		g.GoCtx(runWorker)
	}

	g.GoCtx(func(ctx context.Context) error {
		defer close(mergeCh)
		for i := range ibm.spec.Spans {
			sp := ibm.spec.Spans[i]
			idx := ibm.spec.SpanIdx[i]

			key := sp.Key
			for key != nil {
				chunk, nextKey, err := ibm.scan(ctx, idx, key, sp.EndKey, mergeTimestamp)
				if err != nil {
					return err
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case mergeCh <- chunk:
				}
				key = nextKey

				if knobs, ok := ibm.flowCtx.Cfg.TestingKnobs.IndexBackfillMergerTestingKnobs.(*IndexBackfillMergerTestingKnobs); ok {
					if knobs != nil && knobs.RunAfterScanChunk != nil {
						knobs.RunAfterScanChunk()
					}
				}
			}
		}
		return nil
	})

	workersDoneCh := make(chan error)
	go func() { workersDoneCh <- g.Wait() }()

	tick := time.NewTicker(indexBackfillMergeProgressReportInterval)
	defer tick.Stop()
	var err error
	for {
		select {
		case <-tick.C:
			pushProgress()
		case err = <-workersDoneCh:
			if err != nil {
				output.Push(nil, &execinfrapb.ProducerMetadata{Err: err})
			}
			return
		}
	}
}

var _ execinfra.Processor = &IndexBackfillMerger{}

type mergeChunk struct {
	completedSpan roachpb.Span
	keys          []roachpb.Key
	spanIdx       int32
	memUsed       int64
}

func (ibm *IndexBackfillMerger) scan(
	ctx context.Context,
	spanIdx int32,
	startKey roachpb.Key,
	endKey roachpb.Key,
	readAsOf hlc.Timestamp,
) (mergeChunk, roachpb.Key, error) {
	if knobs, ok := ibm.flowCtx.Cfg.TestingKnobs.IndexBackfillMergerTestingKnobs.(*IndexBackfillMergerTestingKnobs); ok {
		if knobs != nil && knobs.RunBeforeScanChunk != nil {
			if err := knobs.RunBeforeScanChunk(startKey); err != nil {
				return mergeChunk{}, nil, err
			}
		}
	}
	chunkSize := indexBackfillMergeBatchSize.Get(&ibm.flowCtx.Cfg.Settings.SV)
	chunkBytes := indexBackfillMergeBatchBytes.Get(&ibm.flowCtx.Cfg.Settings.SV)

	var br *kvpb.BatchResponse
	if err := ibm.flowCtx.Cfg.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		if err := txn.KV().SetFixedTimestamp(ctx, readAsOf); err != nil {
			return err
		}
		// For now just grab all of the destination KVs and merge the corresponding entries.
		log.VInfof(ctx, 2, "scanning batch [%s, %s) at %v to merge", startKey, endKey, readAsOf)
		ba := &kvpb.BatchRequest{}
		ba.TargetBytes = chunkBytes
		if err := ibm.growBoundAccount(ctx, chunkBytes); err != nil {
			return errors.Wrap(err, "failed to fetch keys to merge from temp index")
		}
		defer ibm.shrinkBoundAccount(ctx, chunkBytes)

		ba.MaxSpanRequestKeys = chunkSize
		ba.Add(&kvpb.ScanRequest{
			RequestHeader: kvpb.RequestHeader{
				Key:    startKey,
				EndKey: endKey,
			},
			ScanFormat: kvpb.KEY_VALUES,
		})
		var pErr *kvpb.Error
		br, pErr = txn.KV().Send(ctx, ba)
		if pErr != nil {
			return pErr.GoError()
		}
		return nil
	}, isql.WithPriority(admissionpb.BulkNormalPri)); err != nil {
		return mergeChunk{}, nil, err
	}

	resp := br.Responses[0].GetScan()
	chunk := mergeChunk{
		spanIdx: spanIdx,
	}
	var chunkMem int64
	if len(resp.Rows) > 0 {
		if err := func() error {
			ibm.muBoundAccount.Lock()
			defer ibm.muBoundAccount.Unlock()
			for i := range resp.Rows {
				chunk.keys = append(chunk.keys, resp.Rows[i].Key)
				if err := ibm.muBoundAccount.boundAccount.Grow(ctx, int64(len(resp.Rows[i].Key))); err != nil {
					return errors.Wrap(err, "failed to allocate space for merge keys")
				}
				chunkMem += int64(len(resp.Rows[i].Key))
			}
			return nil
		}(); err != nil {
			return mergeChunk{}, nil, err
		}
	}
	chunk.memUsed = chunkMem
	var nextStart roachpb.Key
	if resp.ResumeSpan == nil {
		chunk.completedSpan = roachpb.Span{Key: startKey, EndKey: endKey}
	} else {
		nextStart = resp.ResumeSpan.Key
		chunk.completedSpan = roachpb.Span{Key: startKey, EndKey: nextStart}
	}
	return chunk, nextStart, nil
}

// merge merges the latest values for sourceKeys from the index with sourceID
// into the index with destinationID.
func (ibm *IndexBackfillMerger) merge(
	ctx context.Context,
	codec keys.SQLCodec,
	table catalog.TableDescriptor,
	sourceID descpb.IndexID,
	destinationID descpb.IndexID,
	sourceKeys []roachpb.Key,
	sourceSpan roachpb.Span,
) error {
	sourcePrefix := rowenc.MakeIndexKeyPrefix(codec, table.GetID(), sourceID)
	destPrefix := rowenc.MakeIndexKeyPrefix(codec, table.GetID(), destinationID)

	batch := &keyBatch{sourceKeys: sourceKeys}

	err := retryWithReducedBatchWhenAutoRetryLimitExceeded(ctx, batch,
		func(ctx context.Context, keys []roachpb.Key) error {
			return ibm.flowCtx.Cfg.DB.Txn(ctx, func(
				ctx context.Context, txn isql.Txn,
			) error {
				var deletedCount int
				txn.KV().AddCommitTrigger(func(ctx context.Context) {
					commitTs, _ := txn.KV().CommitTimestamp()
					log.VInfof(ctx, 2, "merged batch of %d keys (%d deletes) (span: %s) (commit timestamp: %s)",
						len(keys),
						deletedCount,
						sourceSpan,
						commitTs,
					)
				})
				if len(keys) == 0 {
					return nil
				}

				wb, memUsedInMerge, deletedKeys, err := ibm.constructMergeBatch(
					ctx, txn.KV(), keys, sourcePrefix, destPrefix,
				)
				if err != nil {
					return err
				}

				defer ibm.shrinkBoundAccount(ctx, memUsedInMerge)
				deletedCount = deletedKeys
				if err := txn.KV().Run(ctx, wb); err != nil {
					return err
				}

				if knobs, ok := ibm.flowCtx.Cfg.TestingKnobs.IndexBackfillMergerTestingKnobs.(*IndexBackfillMergerTestingKnobs); ok {
					if knobs != nil && knobs.RunDuringMergeTxn != nil {
						if err := knobs.RunDuringMergeTxn(ctx, txn.KV(), sourceSpan.Key, sourceSpan.EndKey); err != nil {
							return err
						}
					}
				}
				return nil
			},
				isql.WithPriority(admissionpb.BulkNormalPri))
		})

	return err
}

func (ibm *IndexBackfillMerger) constructMergeBatch(
	ctx context.Context,
	txn *kv.Txn,
	sourceKeys []roachpb.Key,
	sourcePrefix []byte,
	destPrefix []byte,
) (*kv.Batch, int64, int, error) {
	rb := txn.NewBatch()
	for i := range sourceKeys {
		rb.Get(sourceKeys[i])
	}
	if err := txn.Run(ctx, rb); err != nil {
		return nil, 0, 0, err
	}

	// We acquire the bound account lock for the entirety of the merge batch
	// construction so that we don't have to acquire the lock every time we want
	// to grow the account.
	var memUsedInMerge int64
	ibm.muBoundAccount.Lock()
	defer ibm.muBoundAccount.Unlock()
	for i := range rb.Results {
		// Since the source index is delete-preserving, reading the latest value for
		// a key that has existed in the past should always return a value.
		if rb.Results[i].Rows[0].Value == nil {
			return nil, 0, 0, errors.AssertionFailedf("expected value to be present in temp index for key=%s", rb.Results[i].Rows[0].Key)
		}
		rowMem := int64(len(rb.Results[i].Rows[0].Key)) + int64(len(rb.Results[i].Rows[0].Value.RawBytes))
		if err := ibm.muBoundAccount.boundAccount.Grow(ctx, rowMem); err != nil {
			return nil, 0, 0, errors.Wrap(err, "failed to allocate space to read latest keys from temp index")
		}
		memUsedInMerge += rowMem
	}

	prefixLen := len(sourcePrefix)
	destKey := make([]byte, len(destPrefix))
	var deletedCount int
	wb := txn.NewBatch()
	for i := range rb.Results {
		sourceKV := &rb.Results[i].Rows[0]
		if len(sourceKV.Key) < prefixLen {
			return nil, 0, 0, errors.Errorf("key for index entry %v does not start with prefix %v", sourceKV, sourcePrefix)
		}

		destKey = destKey[:0]
		destKey = append(destKey, destPrefix...)
		destKey = append(destKey, sourceKV.Key[prefixLen:]...)

		mergedEntry, deleted, err := mergeEntry(sourceKV, destKey)
		if err != nil {
			return nil, 0, 0, err
		}

		entryBytes := mergedEntryBytes(mergedEntry, deleted)
		if err := ibm.muBoundAccount.boundAccount.Grow(ctx, entryBytes); err != nil {
			return nil, 0, 0, errors.Wrap(err, "failed to allocate space to merge entry from temp index")
		}
		memUsedInMerge += entryBytes
		if deleted {
			deletedCount++
			wb.Del(mergedEntry.Key)
		} else {
			wb.Put(mergedEntry.Key, mergedEntry.Value)
		}
	}

	return wb, memUsedInMerge, deletedCount, nil
}

func mergedEntryBytes(entry *kv.KeyValue, deleted bool) int64 {
	if deleted {
		return int64(len(entry.Key))
	}

	return int64(len(entry.Key) + len(entry.Value.RawBytes))
}

func mergeEntry(sourceKV *kv.KeyValue, destKey roachpb.Key) (*kv.KeyValue, bool, error) {
	var destTagAndData []byte
	var deleted bool

	tempWrapper, err := rowenc.DecodeWrapper(sourceKV.Value)
	if err != nil {
		return nil, false, err
	}

	if tempWrapper.Deleted {
		deleted = true
	} else {
		destTagAndData = tempWrapper.Value
	}

	value := &roachpb.Value{}
	value.SetTagAndData(destTagAndData)

	return &kv.KeyValue{
		Key:   destKey.Clone(),
		Value: value,
	}, deleted, nil
}

func (ibm *IndexBackfillMerger) growBoundAccount(ctx context.Context, growBy int64) error {
	ibm.muBoundAccount.Lock()
	defer ibm.muBoundAccount.Unlock()
	return ibm.muBoundAccount.boundAccount.Grow(ctx, growBy)
}

func (ibm *IndexBackfillMerger) shrinkBoundAccount(ctx context.Context, shrinkBy int64) {
	ibm.muBoundAccount.Lock()
	defer ibm.muBoundAccount.Unlock()
	ibm.muBoundAccount.boundAccount.Shrink(ctx, shrinkBy)
}

// Resume is part of the execinfra.Processor interface.
func (ibm *IndexBackfillMerger) Resume(output execinfra.RowReceiver) {
	panic("not implemented")
}

// Close is part of the execinfra.Processor interface.
func (*IndexBackfillMerger) Close(context.Context) {}

// NewIndexBackfillMerger creates a new IndexBackfillMerger.
func NewIndexBackfillMerger(
	flowCtx *execinfra.FlowCtx, processorID int32, spec execinfrapb.IndexBackfillMergerSpec,
) *IndexBackfillMerger {
	return &IndexBackfillMerger{
		processorID: processorID,
		spec:        spec,
		desc:        tabledesc.NewUnsafeImmutable(&spec.Table),
		flowCtx:     flowCtx,
	}
}

// IndexBackfillMergerTestingKnobs is for testing the distributed processors for
// the index backfill merge step.
type IndexBackfillMergerTestingKnobs struct {
	// RunBeforeScanChunk is called once before the scan of each chunk. It is
	// called with starting key of the chunk.
	RunBeforeScanChunk func(startKey roachpb.Key) error

	// RunAfterScanChunk is called once after a chunk has been successfully scanned.
	RunAfterScanChunk func()

	RunDuringMergeTxn func(ctx context.Context, txn *kv.Txn, startKey roachpb.Key, endKey roachpb.Key) error

	// PushesProgressEveryChunk forces the process to push the merge process after
	// every chunk.
	PushesProgressEveryChunk bool

	// RunAfterMergeChunk is called once after a chunk has been successfully merged.
	RunAfterMergeChunk func()
}

var _ base.ModuleTestingKnobs = &IndexBackfillMergerTestingKnobs{}

// ModuleTestingKnobs implements the base.ModuleTestingKnobs interface.
func (*IndexBackfillMergerTestingKnobs) ModuleTestingKnobs() {}

// retryWithReducedBatchWhenAutoRetryLimitExceeded is a helper that will
// continually try to merge a batch. If the KV internal retry limit is reached,
// it will keep retrying the batch but with a progressively smaller batch size
// each time.
func retryWithReducedBatchWhenAutoRetryLimitExceeded(
	ctx context.Context, batch *keyBatch, f func(context.Context, []roachpb.Key) error,
) error {
	const minBatchSize = 1

	for batch.hasAnotherBatch() {
		err := f(ctx, batch.getKeysForNextBatch())
		if err == nil {
			batch.setLastBatchComplete()
			continue
		}
		// We stop retrying if we still couldn't complete the batch at the minimum
		// size. We will return the error and let a higher level handle it.
		if batch.batchSize == minBatchSize {
			return err
		}
		// If we reached the auto retry limit, we reduce the keys for the next batch.
		if kv.IsAutoRetryLimitExhaustedError(err) {
			batch.reduceForRetry()
			log.Infof(ctx,
				"kv auto retry limit was reached, retrying with a reduced batch size of %d keys. kv error: %v",
				batch.batchSize, err)
			continue
		}
		return err
	}
	return nil
}

// reduceForRetry will reduce the size of the next batch due to a KV retry
// related to contention.
func (k *keyBatch) reduceForRetry() {
	if k.batchSize > 0 {
		k.batchSize /= 2
		k.batchSize = max(k.batchSize, 1)
	}
}

// getKeysForNextBatch returns a set of keys to merge in the next batch.
func (k *keyBatch) getKeysForNextBatch() []roachpb.Key {
	// Init to the full set of keys in the first attempt
	if k.batchSize == 0 {
		k.batchSize = len(k.sourceKeys)
	}
	batchEnd := min(k.processedKeys+k.batchSize, len(k.sourceKeys))
	return k.sourceKeys[k.processedKeys:batchEnd]
}

// setLastBatchComplete is a helper for when we successfully merged the last
// batch of keys.
func (k *keyBatch) setLastBatchComplete() {
	k.processedKeys += k.batchSize
}

// hasAnotherBatch returns true if there are keys in the batch that still need
// to be merged.
func (k *keyBatch) hasAnotherBatch() bool {
	return k.processedKeys < len(k.sourceKeys)
}

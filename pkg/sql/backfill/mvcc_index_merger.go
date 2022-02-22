// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package backfill

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// IndexBackfillMerger is a processor that merges entries from the corresponding
// temporary index to a new index.
type IndexBackfillMerger struct {
	spec execinfrapb.IndexBackfillMergerSpec

	desc catalog.TableDescriptor

	out execinfra.ProcOutputHelper

	flowCtx *execinfra.FlowCtx

	evalCtx *tree.EvalContext

	output execinfra.RowReceiver
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
func (ibm *IndexBackfillMerger) Run(ctx context.Context) {
	opName := "IndexBackfillMerger"
	ctx = logtags.AddTag(ctx, opName, int(ibm.spec.Table.ID))
	ctx, span := execinfra.ProcessorSpan(ctx, opName)
	defer span.Finish()
	defer ibm.output.ProducerDone()
	defer execinfra.SendTraceData(ctx, ibm.output)

	mu := struct {
		syncutil.Mutex
		completedSpans   []roachpb.Span
		completedSpanIdx []int32
	}{}

	progCh := make(chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress)
	pushProgress := func() {
		mu.Lock()
		var prog execinfrapb.RemoteProducerMetadata_BulkProcessorProgress
		prog.CompletedSpans = append(prog.CompletedSpans, mu.completedSpans...)
		mu.completedSpans = nil
		prog.CompletedSpanIdx = append(prog.CompletedSpanIdx, mu.completedSpanIdx...)
		mu.completedSpanIdx = nil
		mu.Unlock()

		progCh <- prog
	}

	semaCtx := tree.MakeSemaContext()
	if err := ibm.out.Init(&execinfrapb.PostProcessSpec{}, nil, &semaCtx, ibm.flowCtx.NewEvalCtx()); err != nil {
		ibm.output.Push(nil, &execinfrapb.ProducerMetadata{Err: err})
		return
	}

	// stopProgress will be closed when there is no more progress to report.
	stopProgress := make(chan struct{})
	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		tick := time.NewTicker(indexBackfillMergeProgressReportInterval)
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
		// First, we tighten the span bounds by looking for the last key in the
		// span. The goal of this is to make it more likely that we terminate our
		// merge process, for workloads that write sequentially into their primary
		// key space. Note, however, that this can't help with all workloads and we
		// still depend on merging our temporary index faster than the inbound write
		// rate.
		for i := range ibm.spec.Spans {
			sp := ibm.spec.Spans[i]
			endKey, err := ibm.findExistingEndKeyForSpan(ctx, sp)
			if err != nil {
				return err
			}
			var completedSpan roachpb.Span
			if endKey == nil {
				// No keys found in this span, we can mark the entire span as
				// complete.
				completedSpan = sp
			} else {
				// A key was found, mark [endKey, sp.EndKey) as complete. Note that
				// the endKey returned above is the key after the last found key and
				// will be used as the exclusive bound below.
				completedSpan = roachpb.Span{Key: endKey, EndKey: sp.EndKey}
			}
			mu.Lock()
			mu.completedSpans = append(mu.completedSpans, completedSpan)
			mu.completedSpanIdx = append(mu.completedSpanIdx, ibm.spec.SpanIdx[i])
			mu.Unlock()
			ibm.spec.Spans[i].EndKey = endKey
		}
		pushProgress()

		// TODO(rui): some room for improvement on single threaded
		// implementation, e.g. run merge for spec spans in parallel.
		for i := range ibm.spec.Spans {
			sp := ibm.spec.Spans[i]
			idx := ibm.spec.SpanIdx[i]
			key := sp.Key
			// NB: EndKey will be nil above in the case that this entire span was empty
			// and already marked completed.
			if sp.EndKey == nil {
				continue
			}
			for key != nil && !sp.EndKey.Equal(key) {
				nextKey, err := ibm.Merge(ctx, ibm.evalCtx.Codec, ibm.desc, ibm.spec.TemporaryIndexes[idx], ibm.spec.AddedIndexes[idx],
					key, sp.EndKey, ibm.spec.ChunkSize)
				if err != nil {
					return err
				}

				completedSpan := roachpb.Span{}
				if nextKey == nil {
					completedSpan.Key = key
					completedSpan.EndKey = sp.EndKey
				} else {
					completedSpan.Key = key
					completedSpan.EndKey = nextKey
				}

				mu.Lock()
				mu.completedSpans = append(mu.completedSpans, completedSpan)
				mu.completedSpanIdx = append(mu.completedSpanIdx, idx)
				mu.Unlock()

				if knobs, ok := ibm.flowCtx.Cfg.TestingKnobs.IndexBackfillMergerTestingKnobs.(*IndexBackfillMergerTestingKnobs); ok {
					if knobs != nil && knobs.PushesProgressEveryChunk {
						pushProgress()
					}
				}

				key = nextKey
			}
		}
		return nil
	})

	var err error
	go func() {
		defer close(progCh)
		err = g.Wait()
	}()

	for prog := range progCh {
		p := prog
		if p.CompletedSpans != nil {
			log.VEventf(ctx, 2, "sending coordinator completed spans: %+v", p.CompletedSpans)
		}
		ibm.output.Push(nil, &execinfrapb.ProducerMetadata{BulkProcessorProgress: &p})
	}

	if err != nil {
		ibm.output.Push(nil, &execinfrapb.ProducerMetadata{Err: err})
	}
}

var _ execinfra.Processor = &IndexBackfillMerger{}

// Merge merges the entries from startKey to endKey from the index with sourceID
// into the index with destinationID, up to a maximum of chunkSize entries.
func (ibm *IndexBackfillMerger) Merge(
	ctx context.Context,
	codec keys.SQLCodec,
	table catalog.TableDescriptor,
	sourceID descpb.IndexID,
	destinationID descpb.IndexID,
	startKey roachpb.Key,
	endKey roachpb.Key,
	chunkSize int64,
) (roachpb.Key, error) {
	sourcePrefix := rowenc.MakeIndexKeyPrefix(codec, table.GetID(), sourceID)
	prefixLen := len(sourcePrefix)
	destPrefix := rowenc.MakeIndexKeyPrefix(codec, table.GetID(), destinationID)

	destKey := make([]byte, len(destPrefix))

	if knobs, ok := ibm.flowCtx.Cfg.TestingKnobs.IndexBackfillMergerTestingKnobs.(*IndexBackfillMergerTestingKnobs); ok {
		if knobs != nil && knobs.RunBeforeMergeChunk != nil {
			if err := knobs.RunBeforeMergeChunk(startKey); err != nil {
				return nil, err
			}
		}
	}
	var nextStart roachpb.Key
	err := ibm.flowCtx.Cfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// For now just grab all of the destination KVs and merge the corresponding entries.
		log.VInfof(ctx, 2, "merging batch [%s, %s) into index %d", startKey, endKey, destinationID)
		kvs, err := txn.Scan(ctx, startKey, endKey, chunkSize)
		if err != nil {
			return err
		}

		var deletedCount int
		txn.AddCommitTrigger(func(ctx context.Context) {
			log.VInfof(ctx, 2, "merged batch of %d keys (%d deletes) (nextStart: %s) (commit timestamp: %s)",
				len(kvs),
				deletedCount,
				nextStart,
				txn.CommitTimestamp(),
			)
		})
		if len(kvs) == 0 {
			nextStart = nil
			return nil
		}

		destKeys := make([]roachpb.Key, len(kvs))
		for i := range kvs {
			sourceKV := &kvs[i]

			if len(sourceKV.Key) < prefixLen {
				return errors.Errorf("Key for index entry %v does not start with prefix %v", sourceKV, sourcePrefix)
			}

			destKey = destKey[:0]
			destKey = append(destKey, destPrefix...)
			destKey = append(destKey, sourceKV.Key[prefixLen:]...)
			destKeys[i] = make([]byte, len(destKey))
			copy(destKeys[i], destKey)
		}

		wb := txn.NewBatch()
		for i := range kvs {
			mergedEntry, deleted, err := mergeEntry(&kvs[i], destKeys[i])
			if err != nil {
				return err
			}

			if deleted {
				deletedCount++
				wb.Del(mergedEntry.Key)
			} else {
				wb.Put(mergedEntry.Key, mergedEntry.Value)
			}
		}

		if err := txn.Run(ctx, wb); err != nil {
			return err
		}

		nextStart = kvs[len(kvs)-1].Key.Next()

		if knobs, ok := ibm.flowCtx.Cfg.TestingKnobs.IndexBackfillMergerTestingKnobs.(*IndexBackfillMergerTestingKnobs); ok {
			if knobs != nil && knobs.RunDuringMergeTxn != nil {
				if err := knobs.RunDuringMergeTxn(ctx, txn, startKey, endKey); err != nil {
					return err
				}
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return nextStart, nil
}

// findExistingEndKeyForSpan reverse scans the given span and returns an new exclusive EndKey for
// the span. A nil key is returned if the span is empty.
func (ibm *IndexBackfillMerger) findExistingEndKeyForSpan(
	ctx context.Context, sp roachpb.Span,
) (roachpb.Key, error) {
	lastKey := sp.EndKey
	err := ibm.flowCtx.Cfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		kvs, err := txn.ReverseScan(ctx, sp.Key, sp.EndKey, 1)
		if err != nil {
			return err
		}
		if len(kvs) == 0 {
			lastKey = nil
		} else {
			lastKey = kvs[len(kvs)-1].Key.Next()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return lastKey, nil
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
		Key:   destKey,
		Value: value,
	}, deleted, nil
}

// NewIndexBackfillMerger creates a new IndexBackfillMerger.
func NewIndexBackfillMerger(
	flowCtx *execinfra.FlowCtx,
	spec execinfrapb.IndexBackfillMergerSpec,
	output execinfra.RowReceiver,
) (*IndexBackfillMerger, error) {
	return &IndexBackfillMerger{
		spec:    spec,
		desc:    tabledesc.NewUnsafeImmutable(&spec.Table),
		flowCtx: flowCtx,
		evalCtx: flowCtx.NewEvalCtx(),
		output:  output,
	}, nil
}

// IndexBackfillMergerTestingKnobs is for testing the distributed processors for
// the index backfill merge step.
type IndexBackfillMergerTestingKnobs struct {
	// RunBeforeMergeChunk is called once before the merge of each chunk. It is
	// called with starting key of the chunk.
	RunBeforeMergeChunk func(startKey roachpb.Key) error

	RunDuringMergeTxn func(ctx context.Context, txn *kv.Txn, startKey roachpb.Key, endKey roachpb.Key) error

	// PushesProgressEveryChunk forces the process to push the merge process after
	// every chunk.
	PushesProgressEveryChunk bool
}

var _ base.ModuleTestingKnobs = &IndexBackfillMergerTestingKnobs{}

// ModuleTestingKnobs implements the base.ModuleTestingKnobs interface.
func (*IndexBackfillMergerTestingKnobs) ModuleTestingKnobs() {}

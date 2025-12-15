// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkmerge

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/bulk"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/bulksst"
	"github.com/cockroachdb/cockroach/pkg/sql/bulkutil"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/taskset"
	"github.com/cockroachdb/errors"
)

var (
	_ execinfra.Processor = &bulkMergeProcessor{}
	_ execinfra.RowSource = &bulkMergeProcessor{}
)

// So long as bulkingest is using AddSSTable(), we need to ensure that
// merged SSTables can be applied within raft's 64MB limit, including
// RPC overhead.
var (
	targetFileSize = settings.RegisterByteSizeSetting(
		settings.ApplicationLevel,
		"bulkio.merge.file_size",
		"target size for individual data files produced during merge phase",
		60<<20,
		settings.WithPublic)
)

// Output row format for the bulk merge processor. The third column contains
// a marshaled BulkMergeSpec_Output protobuf with the list of output SSTs.
var bulkMergeProcessorOutputTypes = []*types.T{
	types.Bytes, // The encoded SQL Instance ID used for routing
	types.Int4,  // Task ID
	types.Bytes, // Encoded list of output SSTs (BulkMergeSpec_Output protobuf)
}

// bulkMergeProcessor accepts rows that include an assigned task id and emits
// rows that are (taskID, []output_sst) where output_sst is the name of SSTs
// that were produced by the merged output.
//
// The task ids are used to pick output [start, end) ranges to merge from the
// spec.spans.
//
// Task n is to process the input range from [spans[n].Key, spans[n].EndKey).
type bulkMergeProcessor struct {
	execinfra.ProcessorBase
	spec       execinfrapb.BulkMergeSpec
	input      execinfra.RowSource
	flowCtx    *execinfra.FlowCtx
	storageMux *bulkutil.ExternalStorageMux
}

type mergeProcessorInput struct {
	sqlInstanceID string
	taskID        taskset.TaskID
}

func parseMergeProcessorInput(
	row rowenc.EncDatumRow, typs []*types.T,
) (mergeProcessorInput, error) {
	if len(row) != 2 {
		return mergeProcessorInput{}, errors.Newf("expected 2 columns, got %d", len(row))
	}
	if err := row[0].EnsureDecoded(typs[0], nil); err != nil {
		return mergeProcessorInput{}, err
	}
	if err := row[1].EnsureDecoded(typs[1], nil); err != nil {
		return mergeProcessorInput{}, err
	}
	sqlInstanceID, ok := row[0].Datum.(*tree.DBytes)
	if !ok {
		return mergeProcessorInput{}, errors.Newf("expected bytes column for sqlInstanceID, got %s", row[0].Datum.String())
	}
	taskID, ok := row[1].Datum.(*tree.DInt)
	if !ok {
		return mergeProcessorInput{}, errors.Newf("expected int4 column for taskID, got %s", row[1].Datum.String())
	}
	return mergeProcessorInput{
		sqlInstanceID: string(*sqlInstanceID),
		taskID:        taskset.TaskID(*taskID),
	}, nil
}

func newBulkMergeProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.BulkMergeSpec,
	post *execinfrapb.PostProcessSpec,
	input execinfra.RowSource,
) (execinfra.Processor, error) {
	mp := &bulkMergeProcessor{
		input:      input,
		spec:       spec,
		flowCtx:    flowCtx,
		storageMux: bulkutil.NewExternalStorageMux(flowCtx.Cfg.ExternalStorageFromURI, flowCtx.EvalCtx.SessionData().User()),
	}
	err := mp.Init(
		ctx, mp, post, bulkMergeProcessorOutputTypes, flowCtx, processorID, nil,
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{input},
		},
	)
	if err != nil {
		return nil, err
	}
	return mp, nil
}

// Next implements execinfra.RowSource.
func (m *bulkMergeProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for m.State == execinfra.StateRunning {
		row, meta := m.input.Next()
		switch {
		case row == nil && meta == nil:
			m.MoveToDraining(nil /* err */)
		case meta != nil && meta.Err != nil:
			m.MoveToDraining(meta.Err)
		case meta != nil:
			// If there is non-nil meta, we pass it up the processor chain. It might
			// be something like a trace.
			return nil, meta
		case row != nil:
			output, err := m.handleRow(row)
			if err != nil {
				log.Dev.Errorf(m.Ctx(), "merge processor error: %+v", err)
				m.MoveToDraining(err)
			} else {
				return output, nil
			}
		}
	}
	return nil, m.DrainHelper()
}

func (m *bulkMergeProcessor) handleRow(row rowenc.EncDatumRow) (rowenc.EncDatumRow, error) {
	input, err := parseMergeProcessorInput(row, m.input.OutputTypes())
	if err != nil {
		return nil, err
	}

	if knobs, ok := m.flowCtx.Cfg.TestingKnobs.BulkMergeTestingKnobs.(*TestingKnobs); ok {
		if knobs.RunBeforeMergeTask != nil {
			if err := knobs.RunBeforeMergeTask(m.Ctx(), m.flowCtx.ID, input.taskID); err != nil {
				return nil, err
			}
		}
	}

	results, err := m.mergeSSTs(m.Ctx(), input.taskID)
	if err != nil {
		return nil, err
	}

	marshaled, err := protoutil.Marshal(&results)
	if err != nil {
		return nil, err
	}

	return rowenc.EncDatumRow{
		rowenc.EncDatum{Datum: tree.NewDBytes(tree.DBytes(input.sqlInstanceID))},
		rowenc.EncDatum{Datum: tree.NewDInt(tree.DInt(input.taskID))},
		rowenc.EncDatum{Datum: tree.NewDBytes(tree.DBytes(marshaled))},
	}, nil
}

// Start implements execinfra.RowSource.
func (m *bulkMergeProcessor) Start(ctx context.Context) {
	ctx = m.StartInternal(ctx, "bulkMergeProcessor")
	m.input.Start(ctx)
}

func (m *bulkMergeProcessor) Close(ctx context.Context) {
	err := m.storageMux.Close()
	if err != nil {
		log.Dev.Errorf(ctx, "failed to close external storage mux: %v", err)
	}
	m.ProcessorBase.Close(ctx)
}

// isOverlapping returns true if the sst may contribute keys to the merge span.
// The merge span is a [start, end) range. The SST is a [start, end] range.
func isOverlapping(mergeSpan roachpb.Span, sst execinfrapb.BulkMergeSpec_SST) bool {
	// No overlap if sst.EndKey < mergeSpan.Key (SST is entirely left).
	if bytes.Compare(sst.EndKey, mergeSpan.Key) < 0 {
		return false
	}

	// No overlap if mergeSpan.EndKey <= sst.StartKey (SST is entirely right).
	if bytes.Compare(mergeSpan.EndKey, sst.StartKey) <= 0 {
		return false
	}

	return true
}

func (m *bulkMergeProcessor) mergeSSTs(
	ctx context.Context, taskID taskset.TaskID,
) (execinfrapb.BulkMergeSpec_Output, error) {
	mergeSpan := m.spec.Spans[taskID]
	var storeFiles []storageccl.StoreFile

	// Find the ssts that overlap with the given task's key range.
	for _, sst := range m.spec.SSTs {
		if !isOverlapping(mergeSpan, sst) {
			continue
		}
		file, err := m.storageMux.StoreFile(ctx, sst.URI)
		if err != nil {
			return execinfrapb.BulkMergeSpec_Output{}, err
		}
		storeFiles = append(storeFiles, file)
	}

	// It's possible that there are no ssts to merge for this task. In that case,
	// we return an empty output.
	if len(storeFiles) == 0 {
		return execinfrapb.BulkMergeSpec_Output{}, nil
	}

	sstTargetSize := targetFileSize.Get(&m.flowCtx.EvalCtx.Settings.SV)
	iterOpts := storage.IterOptions{
		KeyTypes:   storage.IterKeyTypePointsAndRanges,
		LowerBound: mergeSpan.Key,
		UpperBound: mergeSpan.EndKey,
	}
	iter, err := storageccl.ExternalSSTReader(ctx, storeFiles, nil, iterOpts)
	if err != nil {
		return execinfrapb.BulkMergeSpec_Output{}, err
	}
	defer iter.Close()

	if m.spec.Iteration == m.spec.MaxIterations {
		return m.ingestFinalIteration(ctx, iter, mergeSpan)
	}

	destStore, err := m.flowCtx.Cfg.ExternalStorage(ctx, m.spec.OutputStorage)
	if err != nil {
		return execinfrapb.BulkMergeSpec_Output{}, err
	}
	defer destStore.Close()
	destFileAllocator := bulksst.NewExternalFileAllocator(destStore, m.spec.OutputStorage.URI,
		m.flowCtx.Cfg.DB.KV().Clock())

	writer, err := newExternalStorageWriter(
		ctx,
		m.flowCtx.EvalCtx.Settings,
		destFileAllocator,
		sstTargetSize,
	)
	if err != nil {
		return execinfrapb.BulkMergeSpec_Output{}, err
	}
	defer writer.Close(ctx)

	return processMergedData(ctx, iter, mergeSpan, writer)
}

// processMergedData is a unified function for iterating over merged data and
// writing it using a mergeWriter implementation.
func processMergedData(
	ctx context.Context, iter storage.SimpleMVCCIterator, mergeSpan roachpb.Span, writer mergeWriter,
) (execinfrapb.BulkMergeSpec_Output, error) {
	var endKey roachpb.Key
	for iter.SeekGE(storage.MVCCKey{Key: mergeSpan.Key}); ; iter.NextKey() {
		ok, err := iter.Valid()
		if err != nil {
			return execinfrapb.BulkMergeSpec_Output{}, err
		}
		if !ok {
			break
		}
		key := iter.UnsafeKey()
		if key.Key.Compare(mergeSpan.EndKey) >= 0 {
			break
		}
		val, err := iter.UnsafeValue()
		if err != nil {
			return execinfrapb.BulkMergeSpec_Output{}, err
		}

		// If we've selected an endKey and this key is at or beyond that point,
		// complete the current output unit before adding this key.
		if endKey != nil && key.Key.Compare(endKey) >= 0 {
			if _, err := writer.Complete(ctx, endKey); err != nil {
				return execinfrapb.BulkMergeSpec_Output{}, err
			}
			endKey = nil
		}

		shouldSplit, err := writer.Add(ctx, key, val)
		if err != nil {
			return execinfrapb.BulkMergeSpec_Output{}, err
		}

		// If the writer wants to split and we haven't selected an endKey yet,
		// pick a safe split point after the current key.
		if shouldSplit && endKey == nil {
			safeKey, err := keys.EnsureSafeSplitKey(key.Key)
			if err != nil {
				return execinfrapb.BulkMergeSpec_Output{}, err
			}
			endKey = safeKey.PrefixEnd()
		}
	}

	return writer.Finish(ctx, mergeSpan.EndKey)
}

func (m *bulkMergeProcessor) ingestFinalIteration(
	ctx context.Context, iter storage.SimpleMVCCIterator, mergeSpan roachpb.Span,
) (execinfrapb.BulkMergeSpec_Output, error) {
	writeTS := m.spec.WriteTimestamp
	if writeTS.IsEmpty() {
		writeTS = m.flowCtx.Cfg.DB.KV().Clock().Now()
	}

	// Use SSTBatcher directly instead of BufferingAdder since the data is
	// already sorted from the merge iterator. This avoids the unnecessary
	// sorting overhead in BufferingAdder.
	batcher, err := bulk.MakeSSTBatcher(
		ctx,
		"bulk-merge-final",
		m.flowCtx.Cfg.DB.KV(),
		m.flowCtx.EvalCtx.Settings,
		hlc.Timestamp{}, // disallowShadowingBelow
		false,           // writeAtBatchTs
		false,           // scatterSplitRanges
		m.flowCtx.Cfg.BackupMonitor.MakeConcurrentBoundAccount(),
		m.flowCtx.Cfg.BulkSenderLimiter,
		nil, // range cache
	)
	if err != nil {
		return execinfrapb.BulkMergeSpec_Output{}, err
	}

	writer := newKVStorageWriter(batcher, writeTS)
	defer writer.Close(ctx)
	return processMergedData(ctx, iter, mergeSpan, writer)
}

func init() {
	rowexec.NewBulkMergeProcessor = newBulkMergeProcessor
}

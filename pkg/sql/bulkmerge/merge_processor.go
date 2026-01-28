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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
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
	iter       storage.SimpleMVCCIterator
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

	var err error
	m.iter, err = m.createIter(ctx)
	if err != nil {
		m.MoveToDraining(err)
		return
	}
}

func (m *bulkMergeProcessor) Close(ctx context.Context) {
	if m.iter != nil {
		m.iter.Close()
	}
	err := m.storageMux.Close()
	if err != nil {
		log.Dev.Errorf(ctx, "failed to close external storage mux: %v", err)
	}
	m.ProcessorBase.Close(ctx)
}

func (m *bulkMergeProcessor) mergeSSTs(
	ctx context.Context, taskID taskset.TaskID,
) (execinfrapb.BulkMergeSpec_Output, error) {
	// If there's no iterator (no SSTs to merge), return an empty output.
	if m.iter == nil {
		return execinfrapb.BulkMergeSpec_Output{}, nil
	}

	mergeSpan := m.spec.Spans[taskID]

	// Seek the iterator if it's not positioned within the current task's span.
	// The spans are disjoint, so the only way the iterator would be contained
	// within the span is if the previous task's span preceded it.
	if ok, _ := m.iter.Valid(); !(ok && containsKey(mergeSpan, m.iter.UnsafeKey().Key)) {
		m.iter.SeekGE(storage.MVCCKey{Key: mergeSpan.Key})
	}

	if m.spec.Iteration == m.spec.MaxIterations {
		return m.ingestFinalIteration(ctx, m.iter, mergeSpan)
	}

	sstTargetSize := targetFileSize.Get(&m.flowCtx.EvalCtx.Settings.SV)
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

	return m.processMergedData(ctx, m.iter, mergeSpan, writer)
}

// processMergedData iterates over merged data and writes it using the provided
// mergeWriter. The iterator must already be positioned at or before the start
// of mergeSpan.
//
// When EnforceUniqueness is true and we're using suffixed iterators (multiple
// SSTs), this function detects cross-SST duplicates by comparing consecutive
// base keys (with suffixes removed) and returns DuplicateKeyError if found.
func (m *bulkMergeProcessor) processMergedData(
	ctx context.Context, iter storage.SimpleMVCCIterator, mergeSpan roachpb.Span, writer mergeWriter,
) (execinfrapb.BulkMergeSpec_Output, error) {
	knobs, _ := m.flowCtx.Cfg.TestingKnobs.BulkMergeTestingKnobs.(*TestingKnobs)
	var endKey roachpb.Key
	duplicateInjected := false

	// When enforcing uniqueness with multiple SSTs, keys are suffixed to prevent
	// shadowing. Track previous base key for duplicate detection.
	usingSuffixedIter := m.spec.EnforceUniqueness && len(m.spec.SSTs) > 1
	var prevBaseKeyBuf []byte
	var baseKey roachpb.Key
	var keyToWrite storage.MVCCKey

	for {
		ok, err := iter.Valid()
		if err != nil {
			return execinfrapb.BulkMergeSpec_Output{}, err
		}
		if !ok {
			break
		}

		key := iter.UnsafeKey()
		val, err := iter.UnsafeValue()
		if err != nil {
			return execinfrapb.BulkMergeSpec_Output{}, err
		}

		// Extract base key and check for duplicates when using suffixed iterators.
		baseKey, keyToWrite, prevBaseKeyBuf, err = m.extractKeyAndCheckDuplicate(
			key, val, usingSuffixedIter, prevBaseKeyBuf,
		)
		if err != nil {
			return execinfrapb.BulkMergeSpec_Output{}, err
		}

		// Check span boundary using the base key (without suffix).
		if mergeSpan.EndKey.Compare(baseKey) <= 0 {
			// We've reached the end of the span.
			break
		}

		// If we've selected an endKey and this key is at or beyond that point,
		// complete the current output unit before adding this key.
		if endKey != nil && baseKey.Compare(endKey) >= 0 {
			if _, err := writer.Complete(ctx, endKey); err != nil {
				return execinfrapb.BulkMergeSpec_Output{}, err
			}
			endKey = nil
		}

		shouldSplit, err := writer.Add(ctx, keyToWrite, val)
		if err != nil {
			return execinfrapb.BulkMergeSpec_Output{}, err
		}

		// If the writer wants to split and we haven't selected an endKey yet,
		// pick a safe split point after the current key.
		if shouldSplit && endKey == nil {
			safeKey, err := keys.EnsureSafeSplitKey(baseKey)
			if err != nil {
				return execinfrapb.BulkMergeSpec_Output{}, err
			}
			endKey = safeKey.PrefixEnd()
		}

		// Testing hook: if duplicate requested and not already injected for this
		// key, skip advancing the iterator so the key is processed again.
		if !duplicateInjected && knobs != nil && knobs.InjectDuplicateKey != nil {
			if knobs.InjectDuplicateKey(m.spec.Iteration, m.spec.MaxIterations) {
				duplicateInjected = true
				continue
			}
		}

		iter.NextKey()
		duplicateInjected = false
	}

	return writer.Finish(ctx, mergeSpan.EndKey)
}

// extractKeyAndCheckDuplicate extracts the base key from a potentially
// suffixed key and checks for duplicates when enforcing uniqueness.
func (m *bulkMergeProcessor) extractKeyAndCheckDuplicate(
	key storage.MVCCKey, val []byte, usingSuffixedIter bool, prevBaseKeyBuf []byte,
) (roachpb.Key, storage.MVCCKey, []byte, error) {
	if !usingSuffixedIter {
		return key.Key, key, prevBaseKeyBuf, nil
	}

	// Remove suffix to get base key.
	baseKey, err := removeKeySuffix(key.Key)
	if err != nil {
		return nil, storage.MVCCKey{}, prevBaseKeyBuf, err
	}

	// Check for cross-SST duplicate.
	if len(prevBaseKeyBuf) > 0 && baseKey.Equal(prevBaseKeyBuf) {
		return nil, storage.MVCCKey{}, prevBaseKeyBuf, kvserverbase.NewDuplicateKeyError(baseKey, val)
	}

	prevBaseKeyBuf = append(prevBaseKeyBuf[:0], baseKey...)
	return baseKey, storage.MVCCKey{Key: baseKey, Timestamp: key.Timestamp}, prevBaseKeyBuf, nil
}

func (m *bulkMergeProcessor) ingestFinalIteration(
	ctx context.Context, iter storage.SimpleMVCCIterator, mergeSpan roachpb.Span,
) (execinfrapb.BulkMergeSpec_Output, error) {
	writeTS := m.spec.WriteTimestamp
	if writeTS.IsEmpty() {
		writeTS = m.flowCtx.Cfg.DB.KV().Clock().Now()
	}

	// For unique indexes, enable duplicate detection by setting
	// disallowShadowingBelow to the write timestamp. This catches conflicts
	// with pre-existing KV data. Cross-SST duplicates within the same merge
	// are detected earlier in processMergedData using suffixed iterators.
	disallowShadowingBelow := hlc.Timestamp{}
	if m.spec.EnforceUniqueness {
		disallowShadowingBelow = writeTS
	}

	// Use SSTBatcher directly instead of BufferingAdder since the data is
	// already sorted from the merge iterator. This avoids the unnecessary
	// sorting overhead in BufferingAdder.
	batcher, err := bulk.MakeSSTBatcher(
		ctx,
		"bulk-merge-final",
		m.flowCtx.Cfg.DB.KV(),
		m.flowCtx.EvalCtx.Settings,
		disallowShadowingBelow,
		false, // writeAtBatchTs
		false, // scatterSplitRanges
		m.flowCtx.Cfg.BackupMonitor.MakeConcurrentBoundAccount(),
		m.flowCtx.Cfg.BulkSenderLimiter,
		nil, // range cache
	)
	if err != nil {
		return execinfrapb.BulkMergeSpec_Output{}, err
	}

	writer := newKVStorageWriter(batcher, writeTS)
	defer writer.Close(ctx)
	return m.processMergedData(ctx, iter, mergeSpan, writer)
}

// createIter builds an iterator over all input SSTs. When EnforceUniqueness
// is true with multiple SSTs, individual iterators are wrapped with suffixes
// and merged using a custom iterator to ensure duplicate keys are surfaced.
func (m *bulkMergeProcessor) createIter(ctx context.Context) (storage.SimpleMVCCIterator, error) {
	if len(m.spec.SSTs) == 0 {
		return nil, nil
	}
	if len(m.spec.Spans) == 0 {
		return nil, errors.AssertionFailedf("no spans specified for merge processor")
	}

	iterOpts := storage.IterOptions{
		KeyTypes: storage.IterKeyTypePointsAndRanges,
		// Bounds are required by iterator validation. Use full span range.
		LowerBound: m.spec.Spans[0].Key,
		UpperBound: m.spec.Spans[len(m.spec.Spans)-1].EndKey,
	}

	// Use suffixed iterators for cross-SST duplicate detection.
	if m.spec.EnforceUniqueness && len(m.spec.SSTs) > 1 {
		return m.createSuffixedIter(ctx, iterOpts)
	}

	// Standard merged iterator for non-unique indexes or single SST.
	return m.createStandardIter(ctx, iterOpts)
}

// createStandardIter creates a standard merged iterator over all SSTs.
func (m *bulkMergeProcessor) createStandardIter(
	ctx context.Context, iterOpts storage.IterOptions,
) (storage.SimpleMVCCIterator, error) {
	var storeFiles []storageccl.StoreFile
	for _, sst := range m.spec.SSTs {
		file, err := m.storageMux.StoreFile(ctx, sst.URI)
		if err != nil {
			return nil, err
		}
		storeFiles = append(storeFiles, file)
	}
	return storageccl.ExternalSSTReader(ctx, storeFiles, nil, iterOpts)
}

// createSuffixedIter creates individual iterators for each SST, wraps them
// with suffixing iterators, and merges them using a custom iterator to ensure
// duplicate keys are surfaced. This is used when EnforceUniqueness is true.
func (m *bulkMergeProcessor) createSuffixedIter(
	ctx context.Context, iterOpts storage.IterOptions,
) (storage.SimpleMVCCIterator, error) {
	var iters []storage.SimpleMVCCIterator

	// Cleanup any opened iterators on error.
	cleanup := func() {
		for _, it := range iters {
			it.Close()
		}
	}

	for _, sstSpec := range m.spec.SSTs {
		file, err := m.storageMux.StoreFile(ctx, sstSpec.URI)
		if err != nil {
			cleanup()
			return nil, err
		}

		// Create iterator for single SST.
		baseIter, err := storageccl.ExternalSSTReader(
			ctx, []storageccl.StoreFile{file}, nil, iterOpts,
		)
		if err != nil {
			cleanup()
			return nil, err
		}

		// Wrap with suffix adder using SST URI (globally unique).
		iters = append(iters, newSuffixingIterator(baseIter, sstSpec.URI))
	}

	// Merge all suffixed iterators using our custom merging iterator
	// that surfaces all keys (including duplicates).
	return newMergingIterator(iters, iterOpts), nil
}

// containsKey returns true if the given key is within the mergeSpan.
func containsKey(mergeSpan roachpb.Span, key roachpb.Key) bool {
	// key is to left
	if bytes.Compare(key, mergeSpan.Key) < 0 {
		return false
	}

	// key is to right
	if bytes.Compare(mergeSpan.EndKey, key) <= 0 {
		return false
	}

	return true
}

func init() {
	rowexec.NewBulkMergeProcessor = newBulkMergeProcessor
}

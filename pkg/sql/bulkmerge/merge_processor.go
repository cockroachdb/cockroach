// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkmerge

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/taskset"
	"github.com/cockroachdb/errors"
)

var (
	_ execinfra.Processor = &bulkMergeProcessor{}
	_ execinfra.RowSource = &bulkMergeProcessor{}
)

var (
	targetFileSize = settings.RegisterByteSizeSetting(
		settings.ApplicationLevel,
		"bulkio.merge.file_size",
		"target size for individual data files produced during merge phase",
		60<<20,
		settings.WithPublic)
)

var bulkMergeProcessorOutputTypes = []*types.T{
	types.Bytes, // The encoded SQL Instance ID used for routing
	types.Int4,  // Task ID
	types.Bytes, // Encoded list of output SSTs
}

// bulkMergeProcessor accepts rows that include an assigned task id and emits
// rows that are (taskID, []ouput_sst) where output_sst is the name of SSTs
// that were rpo the merged output.
//
// The task ids are used to pick output [start, end) ranges to merge from the
// spec.splits.
//
// Task 0 is to process the input range from [nil, split[0]),
// Task n is to process the input range from [split[n-1], split[n]).
// The last task is to process the input range from [split(len(split)-1), nil).
type bulkMergeProcessor struct {
	execinfra.ProcessorBase
	spec    execinfrapb.BulkMergeSpec
	input   execinfra.RowSource
	flowCtx *execinfra.FlowCtx
	writer  *bulksst.MergeWriter
}

type mergeProcessorInput struct {
	sqlInstanceID string
	taskID        taskset.TaskId
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
		taskID:        taskset.TaskId(*taskID),
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
		input:   input,
		spec:    spec,
		flowCtx: flowCtx,
		// TODO(jeffswenson): should this use the root user or the user executing the job?
	}
	err := mp.Init(ctx, mp, post, bulkMergeProcessorOutputTypes, flowCtx, processorID, nil, execinfra.ProcStateOpts{
		InputsToDrain: []execinfra.RowSource{input},
	})
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
			return nil, m.DrainHelper()
		case meta != nil && meta.Err != nil:
			m.MoveToDraining(meta.Err)
			return nil, m.DrainHelper()
		case meta != nil:
			// If there is non-nil meta, we pass it up the processor chain. It might
			// be something like a trace.
			return nil, meta
		case row != nil:
			output, err := m.handleRow(row)
			if err != nil {
				log.Errorf(m.Ctx(), "merge processor error: %+v", err)
				m.MoveToDraining(err)
				return nil, m.DrainHelper()
			}
			return output, nil
		}
	}
	return nil, m.DrainHelper()
}

func (m *bulkMergeProcessor) handleRow(row rowenc.EncDatumRow) (rowenc.EncDatumRow, error) {
	input, err := parseMergeProcessorInput(row, m.input.OutputTypes())
	if err != nil {
		return nil, err
	}

	log.Infof(m.Ctx(), "merging ssts for task %d [%s, %s)", input.taskID, m.spec.Spans[input.taskID].Key, m.spec.Spans[input.taskID].EndKey)

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

	store, err := m.flowCtx.Cfg.ExternalStorageFromURI(ctx, m.spec.OutputUri, username.RootUserName())
	if err != nil {
		m.MoveToDraining(err)
		return
	}

	allocator := bulksst.NewExternalFileAllocator(store, m.spec.OutputUri)
	targetSize := targetFileSize.Get(&m.flowCtx.EvalCtx.Settings.SV)

	m.writer = bulksst.NewInorderWriter(allocator, targetSize, m.flowCtx.EvalCtx.Settings)
}

func (m *bulkMergeProcessor) Close(ctx context.Context) {
	m.ProcessorBase.Close(ctx)
}

// isOverlapping returns true if the sst may contribute keys to the merge span.
// The merge span is a [start, end) range. The SST is a [start, end] range.
func isOverlapping(mergeSpan roachpb.Span, sst execinfrapb.BulkMergeSpec_SST) bool {
	// No overlap if sst.EndKey < mergeSpan.Key (SST is entirely left).
	if bytes.Compare(sst.EndKey, mergeSpan.Key) < 0 {
		return false
	}

	// No overlap if sst.EndKey < mergeSpan.Key (SST is entirely left).
	if bytes.Compare(mergeSpan.EndKey, sst.StartKey) <= 0 {
		return false
	}

	return true
}

func (m *bulkMergeProcessor) mergeSSTs(
	ctx context.Context, taskID taskset.TaskId,
) (execinfrapb.BulkMergeSpec_Output, error) {
	cloudMux := bulkutil.NewCloudStorageMux(m.flowCtx.Cfg.ExternalStorageFromURI, username.RootUserName())
	defer func() {
		err := cloudMux.Close()
		if err != nil {
			log.Errorf(ctx, "error closing cloudMux: %+v", err)
		}
	}()

	mergeSpan := m.spec.Spans[taskID]
	var storeFiles []storageccl.StoreFile

	// Find the ssts that overlap with the given task's key range.
	for _, sst := range m.spec.Ssts {
		if !isOverlapping(mergeSpan, sst) {
			continue
		}
		file, err := cloudMux.StoreFile(ctx, sst.Uri)
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

	iterCtx, cancel := context.WithCancel(context.Background()) // TODO(jeffswenson): testing if the context is leaking buffers
	defer cancel()
	iterOpts := storage.IterOptions{
		KeyTypes:   storage.IterKeyTypePointsAndRanges,
		LowerBound: mergeSpan.Key,
		UpperBound: mergeSpan.EndKey,
	}
	iter, err := storageccl.ExternalSSTReader(iterCtx, storeFiles, nil, iterOpts)
	if err != nil {
		return execinfrapb.BulkMergeSpec_Output{}, err
	}
	defer iter.Close()

	// Write all KVs
	for iter.SeekGE(storage.MVCCKey{Key: mergeSpan.Key}); ; iter.NextKey() {
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

		if err := m.writer.PutRawMVCCValue(ctx, key, val); err != nil {
			return execinfrapb.BulkMergeSpec_Output{}, err
		}
	}

	ssts, err := m.writer.Flush(ctx)
	if err != nil {
		return execinfrapb.BulkMergeSpec_Output{}, err
	}

	// TODO(jeffswenson): harminize the SST encoding used by import/merge/ingest.
	var mergedSSTs execinfrapb.BulkMergeSpec_Output
	for _, sst := range ssts.SST {
		mergedSSTs.Ssts = append(mergedSSTs.Ssts, execinfrapb.BulkMergeSpec_SST{
			StartKey: sst.StartKey,
			EndKey:   sst.EndKey,
			Uri:      sst.URI,
		})
	}
	return mergedSSTs, nil
}

func init() {
	rowexec.NewBulkMergeProcessor = newBulkMergeProcessor
}

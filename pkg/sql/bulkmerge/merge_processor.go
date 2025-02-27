// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkmerge

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"path"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/taskset"
	"github.com/cockroachdb/errors"
)

var (
	_ execinfra.Processor = &bulkMergeProcessor{}
	_ execinfra.RowSource = &bulkMergeProcessor{}
)

// TODO(jeffswenson/annie): pick an encoding for the output SSTs
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
	}
	// TODO(jeffswenson): do I need more here?
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
			m.MoveToDraining(errors.Newf("unexpected meta: %v", meta))
			return nil, m.DrainHelper()
		case row != nil:
			output, err := m.handleRow(row)
			if err != nil {
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
}

func getKeysForTask(task int64, splits []roachpb.Key) (roachpb.Key, roachpb.Key) {
	if task == 0 {
		if len(splits) == 0 {
			return nil, roachpb.KeyMax
		}
		return nil, splits[0]
	}
	last := int64(len(splits) - 1)
	if task == last {
		return splits[task], roachpb.KeyMax
	}
	if task > last {
		panic(errors.AssertionFailedf("bad"))
	}
	return splits[task-1], splits[task]
}

// This is stolen from roachpb/data.go where we check if spans overlap.
func isOverlapping(startKey, endKey roachpb.Key, sst execinfrapb.BulkMergeSpec_SST) bool {
	otherStartKey, otherEndKey := sst.StartKey, sst.EndKey
	if len(endKey) == 0 {
		return bytes.Compare(startKey, otherStartKey) >= 0 && bytes.Compare(startKey, otherEndKey) < 0
	}
	return bytes.Compare(endKey, otherStartKey) > 0 && bytes.Compare(startKey, otherEndKey) < 0
}

func (m *bulkMergeProcessor) mergeSSTs(
	ctx context.Context, taskID taskset.TaskId,
) (execinfrapb.BulkMergeSpec_Output, error) {
	startKey, endKey := getKeysForTask(int64(taskID), m.spec.Splits)
	var storeFiles []storageccl.StoreFile
	dest, err := m.flowCtx.Cfg.ExternalStorageFromURI(ctx, *m.spec.OutputUri, username.RootUserName())
	if err != nil {
		return execinfrapb.BulkMergeSpec_Output{}, err
	}
	// Find the ssts that overlap with the given task's key range.
	for _, sst := range m.spec.Ssts {
		uri, err := url.Parse(sst.Uri)
		if err != nil {
			return execinfrapb.BulkMergeSpec_Output{}, err
		}
		baseURI := uri.Scheme + "://" + uri.Host + path.Dir(uri.Path) + "/"
		sstFile := path.Base(sst.Uri)

		if isOverlapping(startKey, endKey, sst) {
			store, err := m.flowCtx.Cfg.ExternalStorageFromURI(ctx, baseURI, username.RootUserName())
			if err != nil {
				return execinfrapb.BulkMergeSpec_Output{}, err
			}
			storeFiles = append(storeFiles, storageccl.StoreFile{Store: store, FilePath: sstFile})
		}
	}
	iterOpts := storage.IterOptions{
		KeyTypes:   storage.IterKeyTypePointsAndRanges,
		LowerBound: startKey,
		UpperBound: endKey,
	}
	iter, err := storageccl.ExternalSSTReader(ctx, storeFiles, nil, iterOpts)
	if err != nil {
		return execinfrapb.BulkMergeSpec_Output{}, err
	}
	defer iter.Close()

	destSSTBase := fmt.Sprintf("/%d.sst", taskID)
	mergedSST, err := dest.Writer(ctx, destSSTBase)
	if err != nil {
		return execinfrapb.BulkMergeSpec_Output{}, err
	}

	sstWriter := storage.MakeIngestionSSTWriter(ctx, m.flowCtx.EvalCtx.Settings, storage.NoopFinishAbortWritable(mergedSST))
	defer sstWriter.Close()

	var mergedSSTs execinfrapb.BulkMergeSpec_Output
	// Write all KVs
	for iter.SeekGE(storage.MVCCKey{Key: startKey}); ; iter.NextKey() {
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
		// TODO(annie): flush once > 128MiB
		if err := sstWriter.PutRawMVCC(key, val); err != nil {
			return execinfrapb.BulkMergeSpec_Output{}, err
		}
	}

	// Finish writing the SST
	if err := sstWriter.Finish(); err != nil {
		return execinfrapb.BulkMergeSpec_Output{}, err
	}

	mergedSSTs.Ssts = append(mergedSSTs.Ssts, execinfrapb.BulkMergeSpec_SST{
		StartKey: startKey,
		EndKey:   endKey,
		Uri:      *m.spec.OutputUri + destSSTBase,
	})
	return mergedSSTs, nil
}

func init() {
	rowexec.NewBulkMergeProcessor = newBulkMergeProcessor
}

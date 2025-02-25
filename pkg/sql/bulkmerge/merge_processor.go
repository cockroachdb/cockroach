// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkmerge

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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

	results := execinfrapb.BulkMergeSpec_Output{}
	results.Ssts = append(results.Ssts, execinfrapb.BulkMergeSpec_SST{
		// TODO(jeffswenson): replace this with real output. For now we just
		// want to make sure each task is processed
		Uri: fmt.Sprintf("nodelocal://%d/merger/1337/%d.sst", m.flowCtx.NodeID.SQLInstanceID(), input.taskID),
	})

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
	m.StartInternal(ctx, "bulkMergeProcessor")
	m.input.Start(ctx)
}

func init() {
	rowexec.NewBulkMergeProcessor = newBulkMergeProcessor
}

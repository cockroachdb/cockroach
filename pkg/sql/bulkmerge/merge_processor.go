// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkmerge

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

var (
	_ execinfra.Processor = &bulkMergeProcessor{}
	_ execinfra.RowSource = &bulkMergeProcessor{}
)

// TODO(jeffswenson/annie): pick an encoding for the output SSTs
var bulkMergeProcessorOutputTypes = []*types.T{
	types.Int4,  // SQL Instance ID of the merge processor
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
	spec  execinfrapb.BulkMergeSpec
	input execinfra.RowSource
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
		input: input,
		spec:  spec,
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
	// TODO(jeffswenson): fully consume the input
	for m.State == execinfra.StateRunning {
		row, meta := m.input.Next()
		switch {
		case row == nil && meta == nil:
			m.MoveToDraining(nil /* err */)
			break
		case meta != nil && meta.Err != nil:
			m.MoveToDraining(meta.Err)
			break
		case meta != nil:
			m.MoveToDraining(errors.Newf("unexpected meta: %v", meta))
			break
		case row != nil:
			base := *row[0].Datum.(*tree.DBytes)
			return rowenc.EncDatumRow{
				rowenc.EncDatum{Datum: tree.NewDInt(1)},                  // TODO(jeffswenson): SQL Instance ID
				rowenc.EncDatum{Datum: tree.NewDInt(1)},                  // TODO(jeffswenson): Task ID
				rowenc.EncDatum{Datum: tree.NewDBytes(base + "->merge")}, // TODO(jeffswenson): output SST
			}, nil
		}
	}
	return nil, m.DrainHelper()
}

// Start implements execinfra.RowSource.
func (m *bulkMergeProcessor) Start(ctx context.Context) {
	m.StartInternal(ctx, "bulkMergeProcessor")
	m.input.Start(ctx)
}

func init() {
	rowexec.NewBulkMergeProcessor = newBulkMergeProcessor
}

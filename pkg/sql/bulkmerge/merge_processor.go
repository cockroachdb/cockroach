// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkmerge

import (
	"context"
	"errors"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

var (
	_ execinfra.Processor = &bulkMergeProcessor{}
	_ execinfra.RowSource = &bulkMergeProcessor{}
)

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
	spec *execinfrapb.BulkMergeSpec
}

func newBulkMergeProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.StreamIngestionDataSpec,
	post *execinfrapb.PostProcessSpec,
) (execinfra.Processor, error) {
	return nil, errors.New("unimplemented")
}

// Close implements execinfra.Processor.
func (m *bulkMergeProcessor) Close(context.Context) {
	panic("unimplemented")
}

// MustBeStreaming implements execinfra.Processor.
func (m *bulkMergeProcessor) MustBeStreaming() bool {
	panic("unimplemented")
}

// Resume implements execinfra.Processor.
func (m *bulkMergeProcessor) Resume(output execinfra.RowReceiver) {
	panic("unimplemented")
}

// Run implements execinfra.Processor.
func (m *bulkMergeProcessor) Run(context.Context, execinfra.RowReceiver) {
	panic("unimplemented")
}

// ConsumerClosed implements execinfra.RowSource.
func (m *bulkMergeProcessor) ConsumerClosed() {
	panic("unimplemented")
}

// ConsumerDone implements execinfra.RowSource.
func (m *bulkMergeProcessor) ConsumerDone() {
	panic("unimplemented")
}

// Next implements execinfra.RowSource.
func (m *bulkMergeProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	panic("unimplemented")
}

// OutputTypes implements execinfra.RowSource.
func (m *bulkMergeProcessor) OutputTypes() []*types.T {
	panic("unimplemented")
}

// Start implements execinfra.RowSource.
func (m *bulkMergeProcessor) Start(context.Context) {
	panic("unimplemented")
}

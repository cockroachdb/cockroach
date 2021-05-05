// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// ordinalityProcessor is the processor of the WITH ORDINALITY operator, which
// adds an additional ordinal column to the result.
type ordinalityProcessor struct {
	execinfra.ProcessorBase

	input  execinfra.RowSource
	curCnt int64
}

var _ execinfra.Processor = &ordinalityProcessor{}
var _ execinfra.RowSource = &ordinalityProcessor{}

const ordinalityProcName = "ordinality"

func newOrdinalityProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.OrdinalitySpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.RowSourcedProcessor, error) {
	ctx := flowCtx.EvalCtx.Ctx()
	o := &ordinalityProcessor{input: input, curCnt: 1}

	colTypes := make([]*types.T, len(input.OutputTypes())+1)
	copy(colTypes, input.OutputTypes())
	colTypes[len(colTypes)-1] = types.Int
	if err := o.Init(
		o,
		post,
		colTypes,
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{o.input},
		},
	); err != nil {
		return nil, err
	}

	if execinfra.ShouldCollectStats(ctx, flowCtx) {
		o.input = newInputStatCollector(o.input)
		o.ExecStatsForTrace = o.execStatsForTrace
	}

	return o, nil
}

// Start is part of the RowSource interface.
func (o *ordinalityProcessor) Start(ctx context.Context) {
	ctx = o.StartInternal(ctx, ordinalityProcName)
	o.input.Start(ctx)
}

// Next is part of the RowSource interface.
func (o *ordinalityProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for o.State == execinfra.StateRunning {
		row, meta := o.input.Next()

		if meta != nil {
			if meta.Err != nil {
				o.MoveToDraining(nil /* err */)
			}
			return nil, meta
		}
		if row == nil {
			o.MoveToDraining(nil /* err */)
			break
		}

		// The ordinality should increment even if the row gets filtered out.
		row = append(row, rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(o.curCnt))))
		o.curCnt++
		if outRow := o.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, o.DrainHelper()

}

// execStatsForTrace implements ProcessorBase.ExecStatsForTrace.
func (o *ordinalityProcessor) execStatsForTrace() *execinfrapb.ComponentStats {
	is, ok := getInputStats(o.input)
	if !ok {
		return nil
	}
	return &execinfrapb.ComponentStats{
		Inputs: []execinfrapb.InputStats{is},
		Output: o.OutputHelper.Stats(),
	}
}

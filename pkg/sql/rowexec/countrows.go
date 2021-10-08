// Copyright 2018 The Cockroach Authors.
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

// countAggregator is a simple processor that counts the number of rows it
// receives. It's a specialized aggregator that can be used for COUNT(*).
type countAggregator struct {
	execinfra.ProcessorBase

	input execinfra.RowSource
	count int
}

var _ execinfra.Processor = &countAggregator{}
var _ execinfra.RowSource = &countAggregator{}

const countRowsProcName = "count rows"

func newCountAggregator(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*countAggregator, error) {
	ag := &countAggregator{}
	ag.input = input

	if execinfra.ShouldCollectStats(flowCtx.EvalCtx.Ctx(), flowCtx) {
		ag.input = newInputStatCollector(input)
		ag.ExecStatsForTrace = ag.execStatsForTrace
	}

	if err := ag.Init(
		ag,
		post,
		[]*types.T{types.Int},
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{ag.input},
		},
	); err != nil {
		return nil, err
	}

	return ag, nil
}

func (ag *countAggregator) Start(ctx context.Context) {
	ctx = ag.StartInternal(ctx, countRowsProcName)
	ag.input.Start(ctx)
}

func (ag *countAggregator) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for ag.State == execinfra.StateRunning {
		row, meta := ag.input.Next()
		if meta != nil {
			if meta.Err != nil {
				ag.MoveToDraining(meta.Err)
				break
			}
			return nil, meta
		}
		if row == nil {
			ret := make(rowenc.EncDatumRow, 1)
			ret[0] = rowenc.EncDatum{Datum: tree.NewDInt(tree.DInt(ag.count))}
			rendered, _, err := ag.OutputHelper.ProcessRow(ag.Ctx, ret)
			// We're done as soon as we process our one output row, so we
			// transition into draining state. We will, however, return non-nil
			// error (if such occurs during rendering) separately below.
			ag.MoveToDraining(nil /* err */)
			if err != nil {
				return nil, &execinfrapb.ProducerMetadata{Err: err}
			}
			return rendered, nil
		}
		ag.count++
	}
	return nil, ag.DrainHelper()
}

// execStatsForTrace implements ProcessorBase.ExecStatsForTrace.
func (ag *countAggregator) execStatsForTrace() *execinfrapb.ComponentStats {
	is, ok := getInputStats(ag.input)
	if !ok {
		return nil
	}
	return &execinfrapb.ComponentStats{
		Inputs: []execinfrapb.InputStats{is},
		Output: ag.OutputHelper.Stats(),
	}
}

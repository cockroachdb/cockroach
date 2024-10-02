// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
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
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
) (*countAggregator, error) {
	ag := &countAggregator{}
	ag.input = input

	if execstats.ShouldCollectStats(ctx, flowCtx.CollectStats) {
		ag.input = newInputStatCollector(input)
		ag.ExecStatsForTrace = ag.execStatsForTrace
	}

	if err := ag.Init(
		ctx,
		ag,
		post,
		[]*types.T{types.Int},
		flowCtx,
		processorID,
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
			rendered, _, err := ag.OutputHelper.ProcessRow(ag.Ctx(), ret)
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

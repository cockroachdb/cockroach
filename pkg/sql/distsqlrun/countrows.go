// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package distsqlrun

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/opentracing/opentracing-go"
)

// countAggregator is a simple processor that counts the number of rows it
// receives. It's a specialized aggregator that can be used for COUNT(*).
type countAggregator struct {
	ProcessorBase

	input RowSource
	count int
}

var _ Processor = &countAggregator{}
var _ RowSource = &countAggregator{}

const countRowsProcName = "count rows"

var outputTypes = []types.T{*types.Int}

func newCountAggregator(
	flowCtx *FlowCtx,
	processorID int32,
	input RowSource,
	post *distsqlpb.PostProcessSpec,
	output RowReceiver,
) (*countAggregator, error) {
	ag := &countAggregator{}
	ag.input = input

	if sp := opentracing.SpanFromContext(flowCtx.EvalCtx.Ctx()); sp != nil && tracing.IsRecording(sp) {
		ag.input = NewInputStatCollector(input)
		ag.finishTrace = ag.outputStatsToTrace
	}

	if err := ag.Init(
		ag,
		post,
		outputTypes,
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		ProcStateOpts{
			InputsToDrain: []RowSource{ag.input},
		},
	); err != nil {
		return nil, err
	}

	return ag, nil
}

func (ag *countAggregator) Start(ctx context.Context) context.Context {
	ag.input.Start(ctx)
	return ag.StartInternal(ctx, countRowsProcName)
}

func (ag *countAggregator) Next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	for ag.State == StateRunning {
		row, meta := ag.input.Next()
		if meta != nil {
			if meta.Err != nil {
				ag.MoveToDraining(meta.Err)
				break
			}
			return nil, meta
		}
		if row == nil {
			ret := make(sqlbase.EncDatumRow, 1)
			ret[0] = sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(ag.count))}
			rendered, _, err := ag.out.ProcessRow(ag.Ctx, ret)
			// We're done as soon as we process our one output row.
			ag.MoveToDraining(err)
			return rendered, nil
		}
		ag.count++
	}
	return nil, ag.DrainHelper()
}

func (ag *countAggregator) ConsumerDone() {
	ag.MoveToDraining(nil /* err */)
}

func (ag *countAggregator) ConsumerClosed() {
	ag.InternalClose()
}

// outputStatsToTrace outputs the collected distinct stats to the trace. Will
// fail silently if the Distinct processor is not collecting stats.
func (ag *countAggregator) outputStatsToTrace() {
	is, ok := getInputStats(ag.flowCtx, ag.input)
	if !ok {
		return
	}
	if sp := opentracing.SpanFromContext(ag.Ctx); sp != nil {
		tracing.SetSpanStats(
			sp, &AggregatorStats{InputStats: is},
		)
	}
}

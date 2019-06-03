// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package distsqlrun

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/opentracing/opentracing-go"
)

// ordinalityProcessor is the processor of the WITH ORDINALITY operator, which
// adds an additional ordinal column to the result.
type ordinalityProcessor struct {
	ProcessorBase

	input  RowSource
	curCnt int64
}

var _ Processor = &ordinalityProcessor{}
var _ RowSource = &ordinalityProcessor{}

const ordinalityProcName = "ordinality"

func newOrdinalityProcessor(
	flowCtx *FlowCtx,
	processorID int32,
	spec *distsqlpb.OrdinalitySpec,
	input RowSource,
	post *distsqlpb.PostProcessSpec,
	output RowReceiver,
) (RowSourcedProcessor, error) {
	ctx := flowCtx.EvalCtx.Ctx()
	o := &ordinalityProcessor{input: input, curCnt: 1}
	memMonitor := NewMonitor(ctx, flowCtx.EvalCtx.Mon, "ordinality-mem")

	colTypes := make([]types.T, len(input.OutputTypes())+1)
	copy(colTypes, input.OutputTypes())
	colTypes[len(colTypes)-1] = *types.Int
	if err := o.Init(
		o,
		post,
		colTypes,
		flowCtx,
		processorID,
		output,
		memMonitor,
		ProcStateOpts{
			InputsToDrain: []RowSource{o.input},
			TrailingMetaCallback: func(context.Context) []ProducerMetadata {
				o.ConsumerClosed()
				return nil
			}},
	); err != nil {
		return nil, err
	}

	if sp := opentracing.SpanFromContext(ctx); sp != nil && tracing.IsRecording(sp) {
		o.input = NewInputStatCollector(o.input)
		o.finishTrace = o.outputStatsToTrace
	}

	return o, nil
}

// Start is part of the RowSource interface.
func (o *ordinalityProcessor) Start(ctx context.Context) context.Context {
	o.input.Start(ctx)
	return o.StartInternal(ctx, ordinalityProcName)
}

// Next is part of the RowSource interface.
func (o *ordinalityProcessor) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	for o.State == StateRunning {
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

		if outRow := o.ProcessRowHelper(row); outRow != nil {
			outRow = append(outRow, sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(o.curCnt))))
			o.curCnt++
			return outRow, nil
		}
	}
	return nil, o.DrainHelper()

}

// ConsumerClosed is part of the RowSource interface.
func (o *ordinalityProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	if o.InternalClose() {
		o.MemMonitor.Stop(o.Ctx)
	}
}

const ordinalityTagPrefix = "ordinality."

// Stats implements the SpanStats interface.
func (os *OrdinalityStats) Stats() map[string]string {
	inputStatsMap := os.InputStats.Stats(ordinalityTagPrefix)
	inputStatsMap[ordinalityTagPrefix+maxMemoryTagSuffix] = humanizeutil.IBytes(os.MaxAllocatedMem)
	return inputStatsMap
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (os *OrdinalityStats) StatsForQueryPlan() []string {
	return append(
		os.InputStats.StatsForQueryPlan(""),
		fmt.Sprintf("%s: %s", maxMemoryQueryPlanSuffix, humanizeutil.IBytes(os.MaxAllocatedMem)),
	)
}

// outputStatsToTrace outputs the collected distinct stats to the trace. Will
// fail silently if the Distinct processor is not collecting stats.
func (o *ordinalityProcessor) outputStatsToTrace() {
	is, ok := getInputStats(o.flowCtx, o.input)
	if !ok {
		return
	}
	if sp := opentracing.SpanFromContext(o.Ctx); sp != nil {
		tracing.SetSpanStats(
			sp, &OrdinalityStats{InputStats: is, MaxAllocatedMem: o.MemMonitor.MaximumBytes()},
		)
	}
}

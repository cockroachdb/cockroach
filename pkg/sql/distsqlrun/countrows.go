// Copyright 2018 The Cockroach Authors.
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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/opentracing/opentracing-go"
)

// countAggregator is a simple processor that counts the number of rows it
// receives. It's a specialized aggregator that can be used for COUNT(*).
type countAggregator struct {
	processorBase

	input RowSource
	count int
}

var _ Processor = &countAggregator{}
var _ RowSource = &countAggregator{}

const countRowsProcName = "count rows"

var outputTypes = []sqlbase.ColumnType{
	{
		SemanticType: sqlbase.ColumnType_INT,
	},
}

func newCountAggregator(
	flowCtx *FlowCtx, processorID int32, input RowSource, post *PostProcessSpec, output RowReceiver,
) (*countAggregator, error) {
	ag := &countAggregator{}
	ag.input = input

	if sp := opentracing.SpanFromContext(flowCtx.EvalCtx.Ctx()); sp != nil && tracing.IsRecording(sp) {
		ag.input = NewInputStatCollector(input)
		ag.finishTrace = func() {
			isc, ok := ag.input.(*InputStatCollector)
			if !ok {
				return
			}
			sp := opentracing.SpanFromContext(ag.ctx)
			if sp == nil {
				return
			}
			if ag.flowCtx.testingKnobs.OverrideStallTime {
				isc.InputStats.StallTime = 0
			}
			tracing.SetSpanStats(
				sp,
				&AggregatorStats{
					InputStats: isc.InputStats,
				},
			)
		}
	}

	if err := ag.init(
		post,
		outputTypes,
		flowCtx,
		processorID,
		output,
		procStateOpts{
			inputsToDrain: []RowSource{ag.input},
		},
	); err != nil {
		return nil, err
	}

	return ag, nil
}

func (ag *countAggregator) Start(ctx context.Context) context.Context {
	ag.input.Start(ctx)
	return ag.startInternal(ctx, countRowsProcName)
}

func (ag *countAggregator) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	for ag.state == stateRunning {
		row, meta := ag.input.Next()
		if meta != nil {
			if meta.Err != nil {
				ag.moveToDraining(meta.Err)
				break
			}
			return nil, meta
		}
		if row == nil {
			ag.moveToDraining(nil /* err */)
			ret := make(sqlbase.EncDatumRow, 1)
			ret[0] = sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(ag.count))}
			return ret, nil
		}
		ag.count++
	}
	return nil, ag.drainHelper()
}

func (ag *countAggregator) ConsumerDone() {
	ag.moveToDraining(nil /* err */)
}

func (ag *countAggregator) ConsumerClosed() {
	ag.internalClose()
}

func (ag *countAggregator) Run(ctx context.Context, wg *sync.WaitGroup) {
	if ag.out.output == nil {
		panic("aggregator output not initialized for emitting rows")
	}
	ctx = ag.Start(ctx)
	Run(ctx, ag, ag.out.output)
	if wg != nil {
		wg.Done()
	}
}

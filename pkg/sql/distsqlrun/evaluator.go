// Copyright 2016 The Cockroach Authors.
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
//
// Author: Irfan Sharif (irfansharif@cockroachlabs.com)

package distsqlrun

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"golang.org/x/net/context"
)

type evaluator struct {
	flowCtx *FlowCtx
	input   RowSource
	ctx     context.Context

	specExprs []Expression
	exprs     []exprHelper
	exprTypes []sqlbase.ColumnType

	out procOutputHelper
}

var _ processor = &evaluator{}

func newEvaluator(
	flowCtx *FlowCtx, spec *EvaluatorSpec, input RowSource, post *PostProcessSpec, output RowReceiver,
) (*evaluator, error) {
	ev := &evaluator{
		flowCtx:   flowCtx,
		input:     input,
		specExprs: spec.Exprs,
		ctx:       log.WithLogTag(flowCtx.Context, "eval", nil),
		exprs:     make([]exprHelper, len(spec.Exprs)),
		exprTypes: make([]sqlbase.ColumnType, len(spec.Exprs)),
	}

	// Initialize the render expressions.
	types := input.Types()
	for i, expr := range ev.specExprs {
		if err := ev.exprs[i].init(expr, types, ev.flowCtx.evalCtx); err != nil {
			return nil, err
		}
		ev.exprTypes[i] = sqlbase.DatumTypeToColumnType(ev.exprs[i].expr.ResolvedType())
	}

	if err := ev.out.init(post, ev.exprTypes, flowCtx.evalCtx, output); err != nil {
		return nil, err
	}

	return ev, nil
}

// Run is part of the processor interface.
func (ev *evaluator) Run(wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	ctx, span := tracing.ChildSpan(ev.ctx, "evaluator")
	defer tracing.FinishSpan(span)

	if log.V(2) {
		log.Infof(ctx, "starting evaluator process")
		defer log.Infof(ctx, "exiting evaluator")
	}

	outRow := make(sqlbase.EncDatumRow, len(ev.exprs))

	for {
		inputRow, err := ev.input.NextRow()
		if err != nil || inputRow == nil {
			ev.out.close(err)
			return
		}

		for i := range ev.exprs {
			datum, err := ev.exprs[i].eval(inputRow)
			if err != nil {
				ev.out.close(err)
				return
			}
			outRow[i] = sqlbase.DatumToEncDatum(ev.exprTypes[i], datum)
		}

		// Push the row to the output RowReceiver; stop if they don't need more
		// rows.
		if !ev.out.emitRow(ctx, outRow) {
			ev.out.close(nil)
			return
		}
	}
}

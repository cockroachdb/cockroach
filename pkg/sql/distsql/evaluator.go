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

package distsql

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"golang.org/x/net/context"
)

type evaluator struct {
	input  RowSource
	output RowReceiver
	ctx    context.Context
	exprs  []exprHelper

	// Buffer to store intermediate results when evaluating expressions per row
	// to avoid reallocation.
	tuple    parser.DTuple
	rowAlloc sqlbase.EncDatumRowAlloc
}

func newEvaluator(
	flowCtx *FlowCtx, spec *EvaluatorSpec, input RowSource, output RowReceiver,
) (*evaluator, error) {
	ev := &evaluator{
		input:  input,
		output: output,
		ctx:    log.WithLogTag(flowCtx.Context, "Evaluator", nil),
		exprs:  make([]exprHelper, len(spec.Exprs)),
		tuple:  make(parser.DTuple, len(spec.Exprs)),
	}

	for i, expr := range spec.Exprs {
		err := ev.exprs[i].init(expr, spec.Types, flowCtx.evalCtx)
		if err != nil {
			return nil, err
		}
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

	for {
		row, err := ev.input.NextRow()
		if err != nil || row == nil {
			ev.output.Close(err)
			return
		}

		outRow, err := ev.eval(row)
		if err != nil {
			ev.output.Close(err)
			return
		}

		if log.V(3) {
			log.Infof(ctx, "pushing %s\n", outRow)
		}
		// Push the row to the output RowReceiver; stop if they don't need more
		// rows.
		if !ev.output.PushRow(outRow) {
			if log.V(2) {
				log.Infof(ctx, "no more rows required")
			}
			ev.output.Close(nil)
			return
		}
	}
}

func (ev *evaluator) eval(row sqlbase.EncDatumRow) (sqlbase.EncDatumRow, error) {
	for i := range ev.exprs {
		datum, err := (&ev.exprs[i]).eval(row)
		if err != nil {
			return nil, err
		}
		ev.tuple[i] = datum
	}

	outRow := ev.rowAlloc.AllocRow(len(ev.tuple))
	if err := sqlbase.DTupleToEncDatumRow(outRow, ev.tuple); err != nil {
		return nil, err
	}
	return outRow, nil
}

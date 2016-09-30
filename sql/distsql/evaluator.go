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

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/log"
	"golang.org/x/net/context"
)

type evaluator struct {
	input   RowSource
	output  RowReceiver
	ctx     context.Context
	evalCtx *parser.EvalContext
	exprs   []exprHelper
	render  []parser.TypedExpr
}

func newEvaluator(
	flowCtx *FlowCtx, spec *EvaluatorSpec, input RowSource, output RowReceiver,
) (*evaluator, error) {
	ev := &evaluator{
		input:   input,
		output:  output,
		ctx:     log.WithLogTag(flowCtx.Context, "Evaluator", nil),
		evalCtx: flowCtx.evalCtx,
		exprs:   make([]exprHelper, len(spec.Exprs)),
		render:  make([]parser.TypedExpr, len(spec.Exprs)),
	}

	for i, expr := range spec.Exprs {
		err := ev.exprs[i].init(expr, spec.Types, ev.evalCtx)
		if err != nil {
			return nil, err
		}
	}

	// Loop over the expressions in our expression set and extract out fully typed expressions, this
	// will later be evaluated for each input row to construct our output row.
	for i := range ev.exprs {
		typedExpr, err := (&ev.exprs[i]).expr.TypeCheck(nil, parser.NoTypePreference)
		if err != nil {
			return nil, err
		}
		ev.render[i] = typedExpr
	}

	return ev, nil
}

// Run is part of the processor interface.
func (ev *evaluator) Run(wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	if log.V(2) {
		log.Infof(ev.ctx, "starting evaluator process")
		defer log.Infof(ev.ctx, "exiting evaluator")
	}

	for {
		row, err := ev.next()
		if err != nil || row == nil {
			ev.output.Close(err)
			return
		}
		if log.V(3) {
			log.Infof(ev.ctx, "pushing %s\n", row)
		}
		// Push the row to the output RowReceiver; stop if they don't need more rows.
		if !ev.output.PushRow(row) {
			if log.V(2) {
				log.Infof(ev.ctx, "no more rows required")
			}
			ev.output.Close(nil)
			return
		}
	}
}

func (ev *evaluator) next() (sqlbase.EncDatumRow, error) {
	row, err := ev.input.NextRow()
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return ev.extract(row)
}

func (ev *evaluator) extract(row sqlbase.EncDatumRow) (sqlbase.EncDatumRow, error) {
	tuple := make(parser.DTuple, len(ev.exprs))
	for i := range ev.exprs {
		datum, err := (&ev.exprs[i]).extract(row)
		if err != nil {
			return nil, err
		}
		tuple[i] = datum
	}
	return sqlbase.DTupleToEncDatumRow(tuple)
}

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
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package sql

import (
	"fmt"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

// valueGenerator represents a node that produces rows
// computationally, by means of a "generator function" (called
// "set-generating function" in PostgreSQL).
type valueGenerator struct {
	p *planner

	// expr holds the function call that needs to be performed,
	// including its arguments that need evaluation, to obtain the
	// generator object.
	expr parser.TypedExpr

	// gen is a reference to the generator object that produces the row
	// for this planNode.
	gen parser.ValueGenerator

	// columns is the signature of this generator.
	columns ResultColumns

	// rowCount is used for DebugValues() only.
	rowCount int
}

// makeGenerator creates a valueGenerator instance that wraps a call to a
// generator function.
func (p *planner) makeGenerator(ctx context.Context, t *parser.FuncExpr) (planNode, error) {
	origName := t.Func.String()

	if err := p.parser.AssertNoAggregationOrWindowing(t, "FROM", p.session.SearchPath); err != nil {
		return nil, err
	}

	normalized, err := p.analyzeExpr(
		ctx, t, multiSourceInfo{}, parser.IndexedVarHelper{}, parser.TypeAny, false, "FROM",
	)
	if err != nil {
		return nil, err
	}

	tType, ok := normalized.ResolvedType().(parser.TTable)
	if !ok {
		return nil, errors.Errorf("FROM expression is not a generator: %s", t)
	}

	var columns ResultColumns
	if len(tType.Cols) == 1 {
		columns = ResultColumns{ResultColumn{Name: origName, Typ: tType.Cols[0]}}
	} else {
		columns = make(ResultColumns, len(tType.Cols))
		for i, t := range tType.Cols {
			columns[i] = ResultColumn{
				Name: fmt.Sprintf("column%d", i+1),
				Typ:  t,
			}
		}
	}

	return &valueGenerator{
		p:       p,
		expr:    normalized,
		columns: columns,
	}, nil
}

func (n *valueGenerator) Start(context.Context) error {
	expr, err := n.expr.Eval(&n.p.evalCtx)
	if err != nil {
		return err
	}

	tb := expr.(*parser.DTable)
	gen := tb.ValueGenerator
	if err := gen.Start(); err != nil {
		return err
	}

	n.gen = gen
	return nil
}

func (n *valueGenerator) Next(context.Context) (bool, error) {
	n.rowCount++
	return n.gen.Next()
}

func (n *valueGenerator) Close(context.Context) {
	if n.gen != nil {
		n.gen.Close()
	}
}

func (n *valueGenerator) DebugValues() debugValues {
	row := n.gen.Values()
	return debugValues{
		rowIdx: n.rowCount,
		key:    fmt.Sprintf("%d", n.rowCount),
		value:  row.String(),
		output: debugValueRow,
	}
}

func (n *valueGenerator) Spans(context.Context) (_, _ roachpb.Spans, _ error) {
	return nil, nil, nil
}

func (n *valueGenerator) Ordering() orderingInfo  { return orderingInfo{} }
func (n *valueGenerator) Values() parser.Datums   { return n.gen.Values() }
func (n *valueGenerator) MarkDebug(_ explainMode) {}
func (n *valueGenerator) Columns() ResultColumns  { return n.columns }

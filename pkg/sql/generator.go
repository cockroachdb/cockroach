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

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/pkg/errors"
)

type valueGenerator struct {
	p       *planner
	expr    parser.TypedExpr
	gen     parser.ValueGenerator
	columns ResultColumns
	// For DebugValues() only.
	rowCount int
}

func (p *planner) makeGenerator(t *parser.FuncExpr) (planNode, string, error) {
	origName := t.Func.String()

	if err := p.parser.AssertNoAggregationOrWindowing(t, "FROM", p.session.SearchPath); err != nil {
		return nil, "", err
	}

	normalized, err := p.analyzeExpr(
		t, multiSourceInfo{}, parser.IndexedVarHelper{}, parser.TypeAny, false, "FROM",
	)
	if err != nil {
		return nil, "", err
	}

	tType, ok := normalized.ResolvedType().(parser.TTable)
	if !ok {
		return nil, "", errors.Errorf("FROM expression is not a generator: %s", t)
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
	}, origName, nil
}

func (n *valueGenerator) expandPlan() error {
	return n.p.expandSubqueryPlans(n.expr)
}

func (n *valueGenerator) Start() error {
	if err := n.p.startSubqueryPlans(n.expr); err != nil {
		return err
	}

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

func (n *valueGenerator) Next() (bool, error) {
	n.rowCount++
	return n.gen.Next()
}

func (n *valueGenerator) Close() {
	if n.gen != nil {
		n.gen.Close()
	}
}

func (n *valueGenerator) ExplainPlan(_ bool) (string, string, []planNode) {
	subplans := n.p.collectSubqueryPlans(n.expr, nil)
	return "generator", n.expr.String(), subplans
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

func (n *valueGenerator) ExplainTypes(regTypes func(string, string)) {
	regTypes("generator", parser.AsStringWithFlags(n.expr, parser.FmtShowTypes))
}

func (n *valueGenerator) Ordering() orderingInfo       { return orderingInfo{} }
func (n *valueGenerator) Values() parser.DTuple        { return n.gen.Values() }
func (n *valueGenerator) MarkDebug(_ explainMode)      {}
func (n *valueGenerator) Columns() ResultColumns       { return n.columns }
func (n *valueGenerator) SetLimitHint(_ int64, _ bool) {}

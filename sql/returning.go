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
// Author: Matt Jibson (mjibson@cockroachlabs.com)

package sql

import "github.com/cockroachdb/cockroach/sql/parser"

// returningHelper implements the logic used for statements with RETURNING clauses.
type returningHelper struct {
	p      *planner
	values *valuesNode
	qvals  qvalMap
	exprs  parser.Exprs
}

func newReturningHelper(p *planner, r parser.ReturningExprs, alias string, tablecols []ColumnDescriptor) (*returningHelper, error) {
	rh := &returningHelper{p: p, values: &valuesNode{}}
	rh.p = p
	rh.values = &valuesNode{}
	if len(r) == 0 {
		return rh, nil
	}

	rh.values.columns = make([]ResultColumn, len(r))
	table := tableInfo{
		columns: makeResultColumns(tablecols, 0),
		alias:   alias,
	}
	rh.qvals = make(qvalMap)
	rh.exprs = make([]parser.Expr, len(r))
	for i, c := range r {
		expr, err := resolveQNames(&table, rh.qvals, c.Expr)
		if err != nil {
			return nil, err
		}
		rh.exprs[i] = expr
		typ, err := expr.TypeCheck(rh.p.evalCtx.Args)
		if err != nil {
			return nil, err
		}
		name := string(c.As)
		if name == "" {
			name = expr.String()
		}
		rh.values.columns[i] = ResultColumn{
			Name: name,
			Typ:  typ,
		}
	}
	return rh, nil
}

// appends a result row using the given values.
func (rh *returningHelper) append(rowVals parser.DTuple) error {
	if rh.exprs == nil {
		rh.values.rows = append(rh.values.rows, parser.DTuple(nil))
		return nil
	}
	rh.qvals.populateQVals(rowVals)
	resrow := make(parser.DTuple, len(rh.exprs))
	for i, e := range rh.exprs {
		d, err := e.Eval(rh.p.evalCtx)
		if err != nil {
			return err
		}
		resrow[i] = d
	}
	rh.values.rows = append(rh.values.rows, resrow)
	return nil
}

// getResults returns the results as a valuesNode.
func (rh *returningHelper) getValues() *valuesNode {
	return rh.values
}

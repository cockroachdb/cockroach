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

func (p *planner) initReturning(r parser.ReturningExprs, alias string, tablecols []ColumnDescriptor) (*valuesNode, qvalMap, error) {
	result := &valuesNode{}
	if r == nil {
		return result, nil, nil
	}

	result.columns = make([]ResultColumn, len(r))
	table := tableInfo{
		columns: makeResultColumns(tablecols, 0),
		alias:   alias,
	}
	qvals := make(qvalMap)
	for i, c := range r {
		expr, err := resolveQNames(&table, qvals, c.Expr)
		if err != nil {
			return result, nil, err
		}
		r[i].Expr = expr
		typ, err := expr.TypeCheck(p.evalCtx.Args)
		if err != nil {
			return result, nil, err
		}
		name := string(c.As)
		if name == "" {
			name = expr.String()
		}
		result.columns[i] = ResultColumn{
			Name: name,
			Typ:  typ,
		}
	}
	return result, qvals, nil
}

func (p *planner) populateReturning(r parser.ReturningExprs, result *valuesNode, qvals qvalMap, rowVals parser.DTuple) error {
	if r == nil {
		return nil
	}
	qvals.populateQVals(rowVals)
	resrow := make(parser.DTuple, len(r))
	for i, c := range r {
		d, err := c.Expr.Eval(p.evalCtx)
		if err != nil {
			return err
		}
		resrow[i] = d
	}
	result.rows[len(result.rows)-1] = resrow
	return nil
}

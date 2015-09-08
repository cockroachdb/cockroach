// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"fmt"

	"github.com/cockroachdb/cockroach/sql/parser"
)

func (p *planner) expandSubqueries(stmt parser.Statement) error {
	v := subqueryVisitor{planner: p}
	parser.WalkStmt(&v, stmt)
	return v.err
}

type subqueryVisitor struct {
	*planner
	path []parser.Expr // parent expressions
	err  error
}

var _ parser.Visitor = &subqueryVisitor{}

func (v *subqueryVisitor) Visit(expr parser.Expr, pre bool) (parser.Visitor, parser.Expr) {
	if v.err != nil {
		return nil, expr
	}
	if !pre {
		v.path = v.path[:len(v.path)-1]
		return nil, expr
	}
	v.path = append(v.path, expr)

	subquery, ok := expr.(*parser.Subquery)
	if !ok {
		return v, expr
	}

	var plan planNode
	if plan, v.err = v.makePlan(subquery.Select); v.err != nil {
		return nil, expr
	}

	columns, multipleRows := v.getSubqueryContext()
	if columns != len(plan.Columns()) {
		switch columns {
		case 1:
			v.err = fmt.Errorf("subquery must return only one column")
		default:
			v.err = fmt.Errorf("subquery must return %d columns", columns)
		}
		return nil, expr
	}

	var result parser.Expr
	if multipleRows {
		var rows parser.DTuple
		for plan.Next() {
			values := plan.Values()
			switch len(values) {
			case 1:
				// This seems hokey, but if we don't do this then the subquery expands to
				// a tuple of tuples instead of a tuple of values and an expression like
				// "k IN (SELECT foo FROM bar)" will fail because we're comparing a
				// single value against a tuple. Perhaps comparison of a single value
				// against a tuple should succeed if the tuple is one element in length.
				rows = append(rows, values[0])
			default:
				// The result from plan.Values() is only valid until the next call to
				// plan.Next(), so make a copy.
				valuesCopy := make(parser.DTuple, len(values))
				copy(valuesCopy, values)
				rows = append(rows, valuesCopy)
			}
		}
		result = rows
	} else {
		result = parser.DNull
		for plan.Next() {
			values := plan.Values()
			switch len(values) {
			case 1:
				result = values[0]
			default:
				valuesCopy := make(parser.DTuple, len(values))
				copy(valuesCopy, values)
				result = valuesCopy
			}
			if plan.Next() {
				v.err = fmt.Errorf("more than one row returned by a subquery used as an expression")
				return nil, expr
			}
		}
	}

	v.err = plan.Err()
	if v.err != nil {
		return nil, expr
	}
	return v, result
}

// getSubqueryContext returns the number of columns and rows the subquery is
// allowed to have.
func (v *subqueryVisitor) getSubqueryContext() (columns int, multipleRows bool) {
	for i := len(v.path) - 1; i >= 0; i-- {
		switch e := v.path[i].(type) {
		case *parser.ComparisonExpr:
			// The subquery must occur on the right hand side of the comparison.
			//
			// TODO(pmattis): Figure out a way to lift this restriction so that we
			// can support:
			//
			//   SELECT (SELECT 1, 2) = (SELECT 1, 2)
			//
			// TODO(pmattis): Subquery expansion currently happens before expression
			// normalization. This seems wrong. Should perform subquery expansion
			// after qnames are resolved, normalization and type checking.
			columns = 1
			switch t := e.Left.(type) {
			case parser.Row:
				columns = len(t)
			case parser.Tuple:
				columns = len(t)
			case parser.DTuple:
				columns = len(t)
			}

			switch e.Operator {
			case parser.In, parser.NotIn:
				// The IN and NOT IN operators allow multipleRows.
				return columns, true
			}

			return columns, false
		}
	}
	return 1, false
}

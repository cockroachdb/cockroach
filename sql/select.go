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

import "github.com/cockroachdb/cockroach/sql/parser"

// Select selects rows from a single table.
// Privileges: READ on table
//   Notes: postgres requires SELECT. Also requires UPDATE on "FOR UPDATE".
//          mysql requires SELECT.
func (p *planner) Select(n *parser.Select) (planNode, error) {
	s := &scanNode{txn: p.txn}
	if err := s.initFrom(p, n.From); err != nil {
		return nil, err
	}
	if err := s.initWhere(n.Where); err != nil {
		return nil, err
	}
	if err := s.initTargets(n.Exprs); err != nil {
		return nil, err
	}
	return s, nil
}

type subqueryVisitor struct {
	*planner
	err error
}

var _ parser.Visitor = &subqueryVisitor{}

func (v *subqueryVisitor) Visit(expr parser.Expr) parser.Expr {
	if v.err != nil {
		return expr
	}
	subquery, ok := expr.(*parser.Subquery)
	if !ok {
		return expr
	}
	var plan planNode
	if plan, v.err = v.makePlan(subquery.Select); v.err != nil {
		return expr
	}
	var rows parser.DTuple
	for plan.Next() {
		values := plan.Values()
		switch len(values) {
		case 1:
			// TODO(pmattis): This seems hokey, but if we don't do this then the
			// subquery expands to a tuple of tuples instead of a tuple of values and
			// an expression like "k IN (SELECT foo FROM bar)" will fail because
			// we're comparing a single value against a tuple. Perhaps comparison of
			// a single value against a tuple should succeed if the tuple is one
			// element in length.
			rows = append(rows, values[0])
		default:
			// The result from plan.Values() is only valid until the next call to
			// plan.Next(), so make a copy.
			valuesCopy := make(parser.DTuple, len(values))
			copy(valuesCopy, values)
			rows = append(rows, valuesCopy)
		}
	}
	v.err = plan.Err()
	return rows
}

func (p *planner) expandSubqueries(stmt parser.Statement) error {
	v := subqueryVisitor{planner: p}
	parser.WalkStmt(&v, stmt)
	return v.err
}

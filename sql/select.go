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
	"strings"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/structured"
	"github.com/cockroachdb/cockroach/util"
)

// Select selects rows from a single table.
// Privileges: READ on table
//   Notes: postgres requires SELECT. Also requires UPDATE on "FOR UPDATE".
//          mysql requires SELECT.
func (p *planner) Select(n *parser.Select) (planNode, error) {
	var desc *structured.TableDescriptor

	switch len(n.From) {
	case 0:
		// desc remains nil.

	case 1:
		var err error
		desc, err = p.getAliasedTableDesc(n.From[0])
		if err != nil {
			return nil, err
		}

		if !desc.HasPrivilege(p.user, parser.PrivilegeRead) {
			return nil, fmt.Errorf("user %s does not have %s privilege on table %s",
				p.user, parser.PrivilegeRead, desc.Name)
		}

	default:
		return nil, util.Errorf("TODO(pmattis): unsupported FROM: %s", n.From)
	}

	// Loop over the select expressions and expand them into the expressions
	// we're going to use to generate the returned column set and the names for
	// those columns.
	exprs := make([]parser.Expr, 0, len(n.Exprs))
	columns := make([]string, 0, len(n.Exprs))
	for _, e := range n.Exprs {
		switch t := e.(type) {
		case *parser.StarExpr:
			if desc == nil {
				return nil, fmt.Errorf("* with no tables specified is not valid")
			}
			for _, col := range desc.Columns {
				columns = append(columns, col.Name)
				exprs = append(exprs, &parser.QualifiedName{Base: parser.Name(col.Name)})
			}
		case *parser.NonStarExpr:
			// If a QualifiedName has a StarIndirection suffix we need to match the
			// prefix of the qualified name to one of the tables in the query and
			// then expand the "*" into a list of columns.
			if qname, ok := t.Expr.(*parser.QualifiedName); ok && qname.IsStar() {
				if desc == nil {
					return nil, fmt.Errorf("%s with no tables specified is not valid", qname)
				}
				if t.As != "" {
					return nil, fmt.Errorf("%s cannot be aliased", qname)
				}
				if len(qname.Indirect) == 1 {
					if !strings.EqualFold(desc.Name, string(qname.Base)) {
						return nil, fmt.Errorf("table \"%s\" not found", qname.Base)
					}

					// TODO(pmattis): Refactor to share this with the parser.StarExpr
					// handling above.
					for _, col := range desc.Columns {
						columns = append(columns, col.Name)
						exprs = append(exprs, &parser.QualifiedName{Base: parser.Name(col.Name)})
					}
					break
				}
				// TODO(pmattis): Handle len(qname.Indirect) > 1: <database>.<table>.*.
			}

			exprs = append(exprs, t.Expr)
			if t.As != "" {
				columns = append(columns, string(t.As))
			} else {
				// If the expression is a qualified name, use the column name, not the
				// full qualification as the column name to return.
				switch e := t.Expr.(type) {
				case *parser.QualifiedName:
					columns = append(columns, e.Column())
				default:
					columns = append(columns, t.Expr.String())
				}
			}
		}
	}

	s := &scanNode{
		db:      p.db,
		desc:    desc,
		columns: columns,
		render:  exprs,
	}
	if n.Where != nil {
		s.filter = n.Where.Expr
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

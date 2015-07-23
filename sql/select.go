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
	"github.com/cockroachdb/cockroach/structured"
	"github.com/cockroachdb/cockroach/util"
)

// Select selects rows from a single table.
func (p *planner) Select(n *parser.Select) (planNode, error) {
	var desc *structured.TableDescriptor

	switch len(n.From) {
	case 0:
		// desc remains nil.

	case 1:
		ate, ok := n.From[0].(*parser.AliasedTableExpr)
		if !ok {
			return nil, util.Errorf("TODO(pmattis): unsupported FROM: %s", n.From)
		}
		table, ok := ate.Expr.(parser.QualifiedName)
		if !ok {
			return nil, util.Errorf("TODO(pmattis): unsupported FROM: %s", n.From)
		}
		var err error
		desc, err = p.getTableDesc(table)
		if err != nil {
			return nil, err
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
				exprs = append(exprs, parser.QualifiedName{col.Name})
			}
		case *parser.NonStarExpr:
			exprs = append(exprs, t.Expr)
			if t.As != "" {
				columns = append(columns, t.As)
			} else {
				// TODO(pmattis): Should verify at this point that any referenced
				// columns are represented in the tables being selected from.
				columns = append(columns, t.Expr.String())
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

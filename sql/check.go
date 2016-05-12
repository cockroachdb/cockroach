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

package sql

import (
	"fmt"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
)

type checkHelper struct {
	exprs []parser.TypedExpr
	qvals qvalMap
}

func (c *checkHelper) setup(p *planner, tbl *sqlbase.TableDescriptor) error {
	if len(tbl.Checks) == 0 {
		return nil
	}

	c.qvals = make(qvalMap)
	table := tableInfo{
		columns: makeResultColumns(tbl.Columns),
	}

	c.exprs = make([]parser.TypedExpr, len(tbl.Checks))
	for i, check := range tbl.Checks {
		raw, err := parser.ParseExprTraditional(check.Expr)
		if err != nil {
			return err
		}
		resolved, err := resolveQNames(raw, []*tableInfo{&table}, c.qvals, &p.qnameVisitor)
		if err != nil {
			return err
		}
		typedExpr, err := parser.TypeCheck(resolved, nil, parser.TypeBool)
		if err != nil {
			return err
		}
		if typedExpr, err = p.parser.NormalizeExpr(p.evalCtx, typedExpr); err != nil {
			return err
		}
		c.exprs[i] = typedExpr
	}

	return nil
}

// Set values in the qvalues used by the CHECK exprs.
// Any value not passed is assumed to be NULL, unless `merge` is true, in which
// case it is left unchanged (allowing updating a subset of a row's values).
func (c *checkHelper) loadRow(colIdx map[sqlbase.ColumnID]int, row parser.DTuple, merge bool) {
	if len(c.exprs) <= 0 {
		return
	}
	// Populate qvals.
	for ref, qval := range c.qvals {
		// The colIdx is 0-based, we need to change it to 1-based.
		ri, has := colIdx[sqlbase.ColumnID(ref.colIdx+1)]
		if has {
			qval.datum = row[ri]
		} else if !merge {
			qval.datum = parser.DNull
		}
	}
}

func (c *checkHelper) check(ctx parser.EvalContext) error {
	for _, expr := range c.exprs {
		if d, err := expr.Eval(ctx); err != nil {
			return err
		} else if res, err := parser.GetBool(d); err != nil {
			return err
		} else if !res && d != parser.DNull {
			// Failed to satisfy CHECK constraint.
			return fmt.Errorf("failed to satisfy CHECK constraint (%s)", expr.String())
		}
	}
	return nil
}

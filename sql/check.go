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
	"github.com/pkg/errors"
)

type checkHelper struct {
	exprs []parser.TypedExpr
	qvals qvalMap
	cols  []sqlbase.ColumnDescriptor
}

func (c *checkHelper) init(
	p *planner, tn *parser.TableName, tableDesc *sqlbase.TableDescriptor,
) error {
	if len(tableDesc.Checks) == 0 {
		return nil
	}

	c.qvals = make(qvalMap)
	c.cols = tableDesc.Columns
	sourceInfo := newSourceInfoForSingleTable(*tn, makeResultColumns(tableDesc.Columns))

	c.exprs = make([]parser.TypedExpr, len(tableDesc.Checks))
	exprStrings := make([]string, len(tableDesc.Checks))
	for i, check := range tableDesc.Checks {
		exprStrings[i] = check.Expr
	}
	exprs, err := parser.ParseExprsTraditional(exprStrings)
	if err != nil {
		return err
	}

	for i, raw := range exprs {
		typedExpr, err := p.analyzeExpr(raw, multiSourceInfo{sourceInfo}, c.qvals,
			parser.TypeBool, false, "")
		if err != nil {
			return err
		}
		c.exprs[i] = typedExpr
	}
	return nil
}

// Set values in the qvalues used by the CHECK exprs.
// Any value not passed is set to NULL, unless `merge` is true, in which
// case it is left unchanged (allowing updating a subset of a row's values).
func (c *checkHelper) loadRow(colIdx map[sqlbase.ColumnID]int, row parser.DTuple, merge bool) {
	if len(c.exprs) == 0 {
		return
	}

	// Populate qvals.
	for ref, qval := range c.qvals {
		ri, has := colIdx[c.cols[ref.colIdx].ID]
		if has {
			qval.datum = row[ri]
		} else if !merge {
			qval.datum = parser.DNull
		}
	}
}

func (c *checkHelper) check(ctx *parser.EvalContext) error {
	for _, expr := range c.exprs {
		if d, err := expr.Eval(ctx); err != nil {
			return err
		} else if res, err := parser.GetBool(d); err != nil {
			return err
		} else if !res && d != parser.DNull {
			// Failed to satisfy CHECK constraint.
			return fmt.Errorf("failed to satisfy CHECK constraint (%s)", expr)
		}
	}
	return nil
}

func (p *planner) validateCheckExpr(
	exprStr string, tableName parser.TableExpr, tableDesc *sqlbase.TableDescriptor,
) error {
	expr, err := parser.ParseExprTraditional(exprStr)
	if err != nil {
		return err
	}
	sel := &parser.SelectClause{
		Exprs: sqlbase.ColumnsSelectors(tableDesc.Columns),
		From:  &parser.From{Tables: parser.TableExprs{tableName}},
		Where: &parser.Where{Expr: &parser.NotExpr{Expr: expr}},
	}
	lim := &parser.Limit{Count: parser.NewDInt(1)}
	// This could potentially use a variant of planner.SelectClause that could
	// use the tableDesc we have, but this is a rare operation and be benefit
	// would be marginal compared to the work of the actual query, so the added
	// complexity seems unjustified.
	rows, err := p.SelectClause(sel, nil, lim, nil, publicColumns)
	if err := rows.expandPlan(); err != nil {
		return err
	}
	if err := rows.Start(); err != nil {
		return err
	}
	next, err := rows.Next()
	if err != nil {
		return err
	}
	if next {
		return errors.Errorf("validation of CHECK %q failed on row: %s",
			expr.String(), labeledRowValues(tableDesc.Columns, rows.Values()))
	}
	return nil
}

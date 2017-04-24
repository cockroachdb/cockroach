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
	"bytes"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type checkHelper struct {
	exprs        []parser.TypedExpr
	cols         []sqlbase.ColumnDescriptor
	sourceInfo   *dataSourceInfo
	ivars        []parser.IndexedVar
	curSourceRow parser.Datums
}

func (c *checkHelper) init(
	ctx context.Context, p *planner, tn *parser.TableName, tableDesc *sqlbase.TableDescriptor,
) error {
	if len(tableDesc.Checks) == 0 {
		return nil
	}

	c.cols = tableDesc.Columns
	c.sourceInfo = newSourceInfoForSingleTable(
		*tn, sqlbase.ResultColumnsFromColDescs(tableDesc.Columns),
	)

	c.exprs = make([]parser.TypedExpr, len(tableDesc.Checks))
	exprStrings := make([]string, len(tableDesc.Checks))
	for i, check := range tableDesc.Checks {
		exprStrings[i] = check.Expr
	}
	exprs, err := parser.ParseExprs(exprStrings)
	if err != nil {
		return err
	}

	ivarHelper := parser.MakeIndexedVarHelper(c, len(c.cols))
	for i, raw := range exprs {
		typedExpr, err := p.analyzeExpr(ctx, raw, multiSourceInfo{c.sourceInfo}, ivarHelper,
			parser.TypeBool, false, "")
		if err != nil {
			return err
		}
		c.exprs[i] = typedExpr
	}
	c.ivars = ivarHelper.GetIndexedVars()
	c.curSourceRow = make(parser.Datums, len(c.cols))
	return nil
}

// Set values in the IndexedVars used by the CHECK exprs.
// Any value not passed is set to NULL, unless `merge` is true, in which
// case it is left unchanged (allowing updating a subset of a row's values).
func (c *checkHelper) loadRow(
	colIdx map[sqlbase.ColumnID]int, row parser.Datums, merge bool,
) error {
	if len(c.exprs) == 0 {
		return nil
	}
	// Populate IndexedVars.
	for _, ivar := range c.ivars {
		if ivar.Idx == invalidColIdx {
			continue
		}
		ri, has := colIdx[c.cols[ivar.Idx].ID]
		if has {
			if row[ri] != parser.DNull {
				expected, provided := ivar.ResolvedType(), row[ri].ResolvedType()
				if !expected.Equivalent(provided) {
					return errors.Errorf("%s value does not match CHECK expr type %s", provided, expected)
				}
			}
			c.curSourceRow[ivar.Idx] = row[ri]
		} else if !merge {
			c.curSourceRow[ivar.Idx] = parser.DNull
		}
	}
	return nil
}

// IndexedVarEval implements the parser.IndexedVarContainer interface.
func (c *checkHelper) IndexedVarEval(idx int, ctx *parser.EvalContext) (parser.Datum, error) {
	return c.curSourceRow[idx].Eval(ctx)
}

// IndexedVarResolvedType implements the parser.IndexedVarContainer interface.
func (c *checkHelper) IndexedVarResolvedType(idx int) parser.Type {
	return c.sourceInfo.sourceColumns[idx].Typ
}

// IndexedVarFormat implements the parser.IndexedVarContainer interface.
func (c *checkHelper) IndexedVarFormat(buf *bytes.Buffer, f parser.FmtFlags, idx int) {
	c.sourceInfo.FormatVar(buf, f, idx)
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
	ctx context.Context,
	exprStr string,
	tableName parser.TableExpr,
	tableDesc *sqlbase.TableDescriptor,
) error {
	expr, err := parser.ParseExpr(exprStr)
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
	rows, err := p.SelectClause(ctx, sel, nil, lim, nil, publicColumns)
	if err != nil {
		return err
	}
	rows, err = p.optimizePlan(ctx, rows, allColumns(rows))
	if err != nil {
		return err
	}
	if err := p.startPlan(ctx, rows); err != nil {
		return err
	}
	next, err := rows.Next(ctx)
	if err != nil {
		return err
	}
	if next {
		return errors.Errorf("validation of CHECK %q failed on row: %s",
			expr.String(), labeledRowValues(tableDesc.Columns, rows.Values()))
	}
	return nil
}

func (p *planner) validateForeignKey(
	ctx context.Context, srcTable *sqlbase.TableDescriptor, srcIdx *sqlbase.IndexDescriptor,
) error {
	targetTable, err := sqlbase.GetTableDescFromID(ctx, p.txn, srcIdx.ForeignKey.Table)
	if err != nil {
		return err
	}
	targetIdx, err := targetTable.FindIndexByID(srcIdx.ForeignKey.Index)
	if err != nil {
		return err
	}

	srcName, err := p.getQualifiedTableName(ctx, srcTable)
	if err != nil {
		return err
	}

	targetName, err := p.getQualifiedTableName(ctx, targetTable)
	if err != nil {
		return err
	}

	escape := func(s string) string {
		return parser.Name(s).String()
	}

	prefix := len(srcIdx.ColumnNames)
	if p := len(targetIdx.ColumnNames); p < prefix {
		prefix = p
	}

	srcCols, targetCols := make([]string, prefix), make([]string, prefix)
	join, where := make([]string, prefix), make([]string, prefix)

	for i := 0; i < prefix; i++ {
		srcCols[i] = fmt.Sprintf("s.%s", escape(srcIdx.ColumnNames[i]))
		targetCols[i] = fmt.Sprintf("t.%s", escape(targetIdx.ColumnNames[i]))
		join[i] = fmt.Sprintf("(%s = %s OR (%s IS NULL AND %s IS NULL))",
			srcCols[i], targetCols[i], srcCols[i], targetCols[i])
		where[i] = fmt.Sprintf("(%s IS NOT NULL AND %s IS NULL)", srcCols[i], targetCols[i])
	}

	query := fmt.Sprintf(
		`SELECT %s FROM %s@%s AS s LEFT OUTER JOIN %s@%s AS t ON %s WHERE %s LIMIT 1`,
		strings.Join(srcCols, ", "),
		srcName, escape(srcIdx.Name), targetName, escape(targetIdx.Name),
		strings.Join(join, " AND "),
		strings.Join(where, " OR "),
	)

	log.Infof(ctx, "Validating FK %q (%q [%v] -> %q [%v]) with query %q",
		srcIdx.ForeignKey.Name,
		srcTable.Name, srcCols, targetTable.Name, targetCols,
		query,
	)

	values, err := p.queryRows(ctx, query)
	if err != nil {
		return err
	}

	if len(values) > 0 {
		var pairs bytes.Buffer
		for i := range values[0] {
			if i > 0 {
				pairs.WriteString(", ")
			}
			pairs.WriteString(fmt.Sprintf("%s=%v", srcIdx.ColumnNames[i], values[0][i]))
		}
		return errors.Errorf("foreign key violation: %q row %s has no match in %q",
			srcTable.Name, pairs.String(), targetTable.Name)
	}
	return nil
}

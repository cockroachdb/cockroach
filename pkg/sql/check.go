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
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

func (p *planner) validateCheckExpr(
	ctx context.Context, exprStr string, tableName tree.TableExpr, tableDesc *sqlbase.TableDescriptor,
) error {
	expr, err := parser.ParseExpr(exprStr)
	if err != nil {
		return err
	}
	sel := &tree.SelectClause{
		Exprs: sqlbase.ColumnsSelectors(tableDesc.Columns, false /* forUpdateOrDelete */),
		From:  &tree.From{Tables: tree.TableExprs{tableName}},
		Where: &tree.Where{Expr: &tree.NotExpr{Expr: expr}},
	}
	lim := &tree.Limit{Count: tree.NewDInt(1)}
	// This could potentially use a variant of planner.SelectClause that could
	// use the tableDesc we have, but this is a rare operation and be benefit
	// would be marginal compared to the work of the actual query, so the added
	// complexity seems unjustified.
	rows, err := p.SelectClause(ctx, sel, nil, lim, nil, nil, publicColumns)
	if err != nil {
		return err
	}
	rows, err = p.optimizePlan(ctx, rows, allColumns(rows))
	if err != nil {
		return err
	}
	defer rows.Close(ctx)

	params := runParams{
		ctx:             ctx,
		extendedEvalCtx: &p.extendedEvalCtx,
		p:               p,
	}
	if err := startPlan(params, rows); err != nil {
		return err
	}
	next, err := rows.Next(params)
	if err != nil {
		return err
	}
	if next {
		return errors.Errorf("validation of CHECK %q failed on row: %s",
			expr.String(), labeledRowValues(tableDesc.Columns, rows.Values()))
	}
	return nil
}

func matchFullUnacceptableKeyQuery(
	prefix int, srcName *string, srcIdx *sqlbase.IndexDescriptor,
) string {
	srcCols, srcNullClause, srcNotNullClause := make([]string, prefix), make([]string, prefix), make([]string, prefix)
	for i := 0; i < prefix; i++ {
		srcCols[i] = tree.NameString(srcIdx.ColumnNames[i])
		srcNullClause[i] = fmt.Sprintf("%s IS NULL", tree.NameString(srcIdx.ColumnNames[i]))
		srcNotNullClause[i] = fmt.Sprintf("%s IS NOT NULL", tree.NameString(srcIdx.ColumnNames[i]))
	}
	return fmt.Sprintf(
		`SELECT * FROM %s@%s WHERE NOT (%s OR %s) LIMIT 1`,
		*srcName, tree.NameString(srcIdx.Name),
		strings.Join(srcNullClause, " AND "),
		strings.Join(srcNotNullClause, " AND "),
	)
}

func nonMatchingRowQuery(
	prefix int,
	srcName *string,
	srcIdx *sqlbase.IndexDescriptor,
	targetName *string,
	targetIdx *sqlbase.IndexDescriptor,
) string {
	srcCols, srcWhere, targetCols, on := make([]string, prefix), make([]string, prefix), make([]string, prefix), make([]string, prefix)

	for i := 0; i < prefix; i++ {
		srcCols[i] = fmt.Sprintf("s.%s", tree.NameString(srcIdx.ColumnNames[i]))
		srcWhere[i] = fmt.Sprintf("%s IS NOT NULL", tree.NameString(srcIdx.ColumnNames[i]))
		targetCols[i] = fmt.Sprintf("t.%s", tree.NameString(targetIdx.ColumnNames[i]))
		on[i] = fmt.Sprintf("%s = %s", srcCols[i], targetCols[i])
	}

	return fmt.Sprintf(
		`SELECT %s FROM (SELECT * FROM %s@%s WHERE %s) AS s LEFT OUTER JOIN %s@%s AS t ON %s WHERE %s IS NULL LIMIT 1`,
		strings.Join(srcCols, ", "),
		*srcName, tree.NameString(srcIdx.Name),
		strings.Join(srcWhere, " AND "),
		*targetName, tree.NameString(targetIdx.Name),
		strings.Join(on, " AND "),
		// Sufficient to check the first column to see whether there was no matching row
		targetCols[0],
	)
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

	prefix := len(srcIdx.ColumnNames)
	if p := len(targetIdx.ColumnNames); p < prefix {
		prefix = p
	}

	if srcIdx.ForeignKey.Match == sqlbase.ForeignKeyReference_FULL {
		query := matchFullUnacceptableKeyQuery(prefix, &srcName, srcIdx)
		rows, err := p.delegateQuery(ctx, "ALTER TABLE VALIDATE", query, nil, nil)
		if err != nil {
			return err
		}

		rows, err = p.optimizePlan(ctx, rows, allColumns(rows))
		if err != nil {
			return err
		}
		defer rows.Close(ctx)

		params := runParams{
			ctx:             ctx,
			extendedEvalCtx: &p.extendedEvalCtx,
			p:               p,
		}
		if err := startPlan(params, rows); err != nil {
			return err
		}
		next, err := rows.Next(params)
		if err != nil {
			return err
		}

		if next {
			return pgerror.NewErrorf(pgerror.CodeForeignKeyViolationError,
				"foreign key violation: MATCH FULL does not allow mixing of null and nonnull values %s for %s",
				rows.Values(), srcIdx.ForeignKey.Name,
			)
		}
	}
	query := nonMatchingRowQuery(prefix, &srcName, srcIdx, &targetName, targetIdx)

	log.Infof(ctx, "Validating FK %q (%q [%v] -> %q [%v]) with query %q",
		srcIdx.ForeignKey.Name,
		srcTable.Name, srcIdx.ColumnNames, targetTable.Name, targetIdx.ColumnNames,
		query,
	)

	rows, err := p.delegateQuery(ctx, "ALTER TABLE VALIDATE", query, nil, nil)
	if err != nil {
		return err
	}

	rows, err = p.optimizePlan(ctx, rows, allColumns(rows))
	if err != nil {
		return err
	}
	defer rows.Close(ctx)

	params := runParams{
		ctx:             ctx,
		extendedEvalCtx: &p.extendedEvalCtx,
		p:               p,
	}
	if err := startPlan(params, rows); err != nil {
		return err
	}
	next, err := rows.Next(params)
	if err != nil {
		return err
	}

	if next {
		values := rows.Values()
		var pairs bytes.Buffer
		for i := range values {
			if i > 0 {
				pairs.WriteString(", ")
			}
			pairs.WriteString(fmt.Sprintf("%s=%v", srcIdx.ColumnNames[i], values[i]))
		}
		return pgerror.NewErrorf(pgerror.CodeForeignKeyViolationError,
			"foreign key violation: %q row %s has no match in %q",
			srcTable.Name, pairs.String(), targetTable.Name)
	}
	return nil
}

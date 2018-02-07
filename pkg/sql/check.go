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

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

	srcCols, targetCols := make([]string, prefix), make([]string, prefix)
	join, where := make([]string, prefix), make([]string, prefix)

	for i := 0; i < prefix; i++ {
		srcCols[i] = fmt.Sprintf("s.%s", tree.NameString(srcIdx.ColumnNames[i]))
		targetCols[i] = fmt.Sprintf("t.%s", tree.NameString(targetIdx.ColumnNames[i]))
		join[i] = fmt.Sprintf("(%s = %s OR (%s IS NULL AND %s IS NULL))",
			srcCols[i], targetCols[i], srcCols[i], targetCols[i])
		where[i] = fmt.Sprintf("(%s IS NOT NULL AND %s IS NULL)", srcCols[i], targetCols[i])
	}

	query := fmt.Sprintf(
		`SELECT %s FROM %s@%s AS s LEFT OUTER JOIN %s@%s AS t ON %s WHERE %s LIMIT 1`,
		strings.Join(srcCols, ", "),
		srcName, tree.NameString(srcIdx.Name), targetName, tree.NameString(targetIdx.Name),
		strings.Join(join, " AND "),
		strings.Join(where, " OR "),
	)

	log.Infof(ctx, "Validating FK %q (%q [%v] -> %q [%v]) with query %q",
		srcIdx.ForeignKey.Name,
		srcTable.Name, srcCols, targetTable.Name, targetCols,
		query,
	)

	values, _ /* cols */, err := p.queryRows(ctx, query)
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
		return pgerror.NewErrorf(pgerror.CodeForeignKeyViolationError,
			"foreign key violation: %q row %s has no match in %q",
			srcTable.Name, pairs.String(), targetTable.Name)
	}
	return nil
}

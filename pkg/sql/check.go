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

func validateCheckExpr(
	ctx context.Context,
	exprStr string,
	tableName tree.TableExpr,
	tableDesc *sqlbase.TableDescriptor,
	evalCtx *tree.EvalContext,
) error {
	expr, err := parser.ParseExpr(exprStr)
	if err != nil {
		return err
	}
	// Construct AST and then convert to a string, to avoid problems with escaping the check expression
	sel := &tree.SelectClause{
		Exprs: sqlbase.ColumnsSelectors(tableDesc.Columns, false /* forUpdateOrDelete */),
		From:  &tree.From{Tables: tree.TableExprs{tableName}},
		Where: &tree.Where{Type: tree.AstWhere, Expr: &tree.NotExpr{Expr: expr}},
	}
	lim := &tree.Limit{Count: tree.NewDInt(1)}
	stmt := &tree.Select{Select: sel, Limit: lim}

	queryStr := tree.AsStringWithFlags(stmt, tree.FmtParsable)

	rows, err := evalCtx.InternalExecutor.QueryRow(ctx, "validate check constraint", evalCtx.Txn, queryStr)
	if err != nil {
		return err
	}
	if rows.Len() > 0 {
		return errors.Errorf("validation of CHECK %q failed on row: %s",
			expr.String(), labeledRowValues(tableDesc.Columns, rows))
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

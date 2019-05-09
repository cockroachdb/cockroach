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

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func validateCheckExpr(
	ctx context.Context,
	exprStr string,
	tableDesc *sqlbase.TableDescriptor,
	ie tree.SessionBoundInternalExecutor,
	txn *client.Txn,
) error {
	expr, err := parser.ParseExpr(exprStr)
	if err != nil {
		return err
	}
	// Construct AST and then convert to a string, to avoid problems with escaping the check expression
	tblref := tree.TableRef{TableID: int64(tableDesc.ID), As: tree.AliasClause{Alias: "t"}}
	sel := &tree.SelectClause{
		Exprs: sqlbase.ColumnsSelectors(tableDesc.Columns, false /* forUpdateOrDelete */),
		From:  &tree.From{Tables: []tree.TableExpr{&tblref}},
		Where: &tree.Where{Type: tree.AstWhere, Expr: &tree.NotExpr{Expr: expr}},
	}
	lim := &tree.Limit{Count: tree.NewDInt(1)}
	stmt := &tree.Select{Select: sel, Limit: lim}
	queryStr := tree.AsStringWithFlags(stmt, tree.FmtParsable)
	log.Infof(ctx, "Validating check constraint %q with query %q", expr.String(), queryStr)

	rows, err := ie.QueryRow(ctx, "validate check constraint", txn, queryStr)
	if err != nil {
		return err
	}
	if rows.Len() > 0 {
		return pgerror.Newf(pgerror.CodeCheckViolationError,
			"validation of CHECK %q failed on row: %s",
			expr.String(), labeledRowValues(tableDesc.Columns, rows))
	}
	return nil
}

// matchFullUnacceptableKeyQuery generates and returns a query for rows that are
// disallowed given the specified MATCH FULL composite FK reference, i.e., rows
// in the referencing table where the key contains both null and non-null
// values.
//
// For example, a FK constraint on columns (a_id, b_id) with an index c_id on
// the table "child" would require the following query:
//
// SELECT * FROM child@c_idx
// WHERE
//   NOT ((COALESCE(a_id, b_id) IS NULL) OR (a_id IS NOT NULL AND b_id IS NOT NULL))
// LIMIT 1;
func matchFullUnacceptableKeyQuery(
	prefix int, srcID sqlbase.ID, srcIdx *sqlbase.IndexDescriptor,
) string {
	srcCols, srcNotNullClause := make([]string, prefix), make([]string, prefix)
	for i := 0; i < prefix; i++ {
		srcCols[i] = tree.NameString(srcIdx.ColumnNames[i])
		srcNotNullClause[i] = fmt.Sprintf("%s IS NOT NULL", tree.NameString(srcIdx.ColumnNames[i]))
	}
	return fmt.Sprintf(
		`SELECT * FROM [%d AS tbl]@[%d] WHERE NOT ((COALESCE(%s) IS NULL) OR (%s)) LIMIT 1`,
		srcID, srcIdx.ID,
		strings.Join(srcCols, ", "),
		strings.Join(srcNotNullClause, " AND "),
	)
}

// nonMatchingRowQuery generates and returns a query for rows that violate the
// specified FK constraint, i.e., rows in the referencing table with no matching
// key in the referenced table. Rows in the referencing table with any null
// values in the key are excluded from matching (for both MATCH FULL and MATCH
// SIMPLE).
//
// For example, a FK constraint on columns (a_id, b_id) with an index c_id on
// the table "child", referencing columns (a, b) with an index p_id on the table
// "parent", would require the following query:
//
// SELECT
//   s.a_id, s.b_id
// FROM
//   (SELECT * FROM child@c_idx WHERE a_id IS NOT NULL AND b_id IS NOT NULL) AS s
//   LEFT OUTER JOIN parent@p_idx AS t ON s.a_id = t.a AND s.b_id = t.b
// WHERE
//   t.a IS NULL
// LIMIT 1;
func nonMatchingRowQuery(
	prefix int,
	srcID sqlbase.ID,
	srcIdx *sqlbase.IndexDescriptor,
	targetID sqlbase.ID,
	targetIdx *sqlbase.IndexDescriptor,
) string {
	srcCols, srcWhere, targetCols, on := make([]string, prefix), make([]string, prefix), make([]string, prefix), make([]string, prefix)

	for i := 0; i < prefix; i++ {
		// s and t are table aliases used in the query
		srcCols[i] = fmt.Sprintf("s.%s", tree.NameString(srcIdx.ColumnNames[i]))
		srcWhere[i] = fmt.Sprintf("%s IS NOT NULL", tree.NameString(srcIdx.ColumnNames[i]))
		targetCols[i] = fmt.Sprintf("t.%s", tree.NameString(targetIdx.ColumnNames[i]))
		on[i] = fmt.Sprintf("%s = %s", srcCols[i], targetCols[i])
	}

	return fmt.Sprintf(
		`SELECT %s FROM (SELECT * FROM [%d AS src]@[%d] WHERE %s) AS s LEFT OUTER JOIN [%d AS target]@[%d] AS t ON %s WHERE %s IS NULL LIMIT 1`,
		strings.Join(srcCols, ", "),
		srcID, srcIdx.ID,
		strings.Join(srcWhere, " AND "),
		targetID, targetIdx.ID,
		strings.Join(on, " AND "),
		// Sufficient to check the first column to see whether there was no matching row
		targetCols[0],
	)
}

func validateForeignKey(
	ctx context.Context,
	srcTable *sqlbase.TableDescriptor,
	srcIdx *sqlbase.IndexDescriptor,
	ie tree.SessionBoundInternalExecutor,
	txn *client.Txn,
) error {
	targetTable, err := sqlbase.GetTableDescFromID(ctx, txn, srcIdx.ForeignKey.Table)
	if err != nil {
		return err
	}
	targetIdx, err := targetTable.FindIndexByID(srcIdx.ForeignKey.Index)
	if err != nil {
		return err
	}

	prefix := len(srcIdx.ColumnNames)
	if p := len(targetIdx.ColumnNames); p < prefix {
		prefix = p
	}

	// For MATCH FULL FKs, first check whether any disallowed keys containing both
	// null and non-null values exist.
	// (The matching options only matter for FKs with more than one column.)
	if prefix > 1 && srcIdx.ForeignKey.Match == sqlbase.ForeignKeyReference_FULL {
		query := matchFullUnacceptableKeyQuery(prefix, srcTable.ID, srcIdx)

		log.Infof(ctx, "Validating MATCH FULL FK %q (%q [%v] -> %q [%v]) with query %q",
			srcIdx.ForeignKey.Name,
			srcTable.Name, srcIdx.ColumnNames, targetTable.Name, targetIdx.ColumnNames,
			query,
		)

		rows, err := ie.QueryRow(ctx, "validate foreign key constraint", txn, query)
		if err != nil {
			return err
		}
		if rows.Len() > 0 {
			return pgerror.Newf(pgerror.CodeForeignKeyViolationError,
				"foreign key violation: MATCH FULL does not allow mixing of null and nonnull values %s for %s",
				rows, srcIdx.ForeignKey.Name,
			)
		}
	}
	query := nonMatchingRowQuery(prefix, srcTable.ID, srcIdx, targetTable.ID, targetIdx)

	log.Infof(ctx, "Validating FK %q (%q [%v] -> %q [%v]) with query %q",
		srcIdx.ForeignKey.Name,
		srcTable.Name, srcIdx.ColumnNames, targetTable.Name, targetIdx.ColumnNames,
		query,
	)

	values, err := ie.QueryRow(ctx, "validate fk constraint", txn, query)
	if err != nil {
		return err
	}
	if values.Len() > 0 {
		var pairs bytes.Buffer
		for i := range values {
			if i > 0 {
				pairs.WriteString(", ")
			}
			pairs.WriteString(fmt.Sprintf("%s=%v", srcIdx.ColumnNames[i], values[i]))
		}
		return pgerror.Newf(pgerror.CodeForeignKeyViolationError,
			"foreign key violation: %q row %s has no match in %q",
			srcTable.Name, pairs.String(), targetTable.Name)
	}
	return nil
}

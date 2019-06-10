// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package sql

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
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
		return pgerror.Newf(pgcode.CheckViolation,
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
// SELECT s.a_id, s.b_id, s.pk1, s.pk2 FROM child@c_idx
// WHERE
//   NOT ((COALESCE(a_id, b_id) IS NULL) OR (a_id IS NOT NULL AND b_id IS NOT NULL))
// LIMIT 1;
func matchFullUnacceptableKeyQuery(
	prefix int, srcTbl *sqlbase.TableDescriptor, srcIdx *sqlbase.IndexDescriptor, limitResults bool,
) (sql string, colNames []string, _ error) {
	srcCols := make([]string, prefix)
	srcNotNullClause := make([]string, prefix)
	for i := 0; i < prefix; i++ {
		srcCols[i] = tree.NameString(srcIdx.ColumnNames[i])
		srcNotNullClause[i] = fmt.Sprintf("%s IS NOT NULL", tree.NameString(srcIdx.ColumnNames[i]))
	}

	returnedCols := srcCols
	// ExtraColumns will include primary index key values not already part of the index.
	for _, id := range srcIdx.ExtraColumnIDs {
		column, err := srcTbl.FindActiveColumnByID(id)
		if err != nil {
			return "", nil, err
		}
		returnedCols = append(returnedCols, column.Name)
	}

	limit := ""
	if limitResults {
		limit = " LIMIT 1"
	}
	return fmt.Sprintf(
		`SELECT %[1]s FROM [%[2]d AS tbl]@[%[3]d] WHERE NOT ((COALESCE(%[4]s) IS NULL) OR (%[5]s)) %[6]s`,
		strings.Join(returnedCols, ","),         // 1
		srcTbl.ID,                               // 2
		srcIdx.ID,                               // 3
		strings.Join(srcCols, ", "),             // 4
		strings.Join(srcNotNullClause, " AND "), // 5
		limit,                                   // 6
	), returnedCols, nil
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
//   s.a_id, s.b_id, s.pk1, s.pk2
// FROM
//   (SELECT * FROM child@c_idx WHERE a_id IS NOT NULL AND b_id IS NOT NULL) AS s
//   LEFT OUTER JOIN parent@p_idx AS t ON s.a_id = t.a AND s.b_id = t.b
// WHERE
//   t.a IS NULL
// LIMIT 1  -- if limitResults is set
// AS OF SYSTEM TIME .. -- if asOf is not hlc.MaxTimestamp
//
// TODO(radu): change this to a query which executes as an anti-join when we
// remove the heuristic planner.
func nonMatchingRowQuery(
	prefix int,
	srcTbl *sqlbase.TableDescriptor,
	srcIdx *sqlbase.IndexDescriptor,
	targetID sqlbase.ID,
	targetIdx *sqlbase.IndexDescriptor,
	limitResults bool,
) (sql string, colNames []string, _ error) {
	colNames = append([]string(nil), srcIdx.ColumnNames...)
	// ExtraColumns will include primary index key values not already part of the index.
	for _, id := range srcIdx.ExtraColumnIDs {
		column, err := srcTbl.FindActiveColumnByID(id)
		if err != nil {
			return "", nil, err
		}
		colNames = append(colNames, column.Name)
	}

	srcCols := make([]string, len(colNames))
	qualifiedSrcCols := make([]string, len(colNames))
	for i, n := range colNames {
		srcCols[i] = tree.NameString(n)
		// s is the table alias used in the query.
		qualifiedSrcCols[i] = fmt.Sprintf("s.%s", srcCols[i])
	}

	srcWhere := make([]string, prefix)
	targetCols := make([]string, prefix)
	on := make([]string, prefix)

	for i := 0; i < prefix; i++ {
		// s and t are table aliases used in the query
		srcWhere[i] = fmt.Sprintf("%s IS NOT NULL", tree.NameString(srcIdx.ColumnNames[i]))
		targetCols[i] = fmt.Sprintf("t.%s", tree.NameString(targetIdx.ColumnNames[i]))
		on[i] = fmt.Sprintf("%s = %s", qualifiedSrcCols[i], targetCols[i])
	}

	limit := ""
	if limitResults {
		limit = " LIMIT 1"
	}
	return fmt.Sprintf(
		`SELECT %[1]s FROM 
		  (SELECT %[2]s FROM [%[3]d AS src]@{FORCE_INDEX=[%[4]d],IGNORE_FOREIGN_KEYS} WHERE %[5]s) AS s
			LEFT OUTER JOIN
			(SELECT * FROM [%[6]d AS target]@[%[7]d]) AS t
			ON %[8]s
		 WHERE %[9]s IS NULL %[10]s`,
		strings.Join(qualifiedSrcCols, ", "), // 1
		strings.Join(srcCols, ", "),          // 2
		srcTbl.ID,                            // 3
		srcIdx.ID,                            // 4
		strings.Join(srcWhere, " AND "),      // 5
		targetID,                             // 6
		targetIdx.ID,                         // 7
		strings.Join(on, " AND "),            // 8
		// Sufficient to check the first column to see whether there was no matching row
		targetCols[0], // 9
		limit,         // 10
	), colNames, nil
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
	if srcIdx.ForeignKey.SharedPrefixLen != 0 {
		prefix = int(srcIdx.ForeignKey.SharedPrefixLen)
	}

	// For MATCH FULL FKs, first check whether any disallowed keys containing both
	// null and non-null values exist.
	// (The matching options only matter for FKs with more than one column.)
	if prefix > 1 && srcIdx.ForeignKey.Match == sqlbase.ForeignKeyReference_FULL {
		query, colNames, err := matchFullUnacceptableKeyQuery(
			prefix, srcTable, srcIdx,
			true, /* limitResults */
		)
		if err != nil {
			return err
		}

		log.Infof(ctx, "Validating MATCH FULL FK %q (%q [%v] -> %q [%v]) with query %q",
			srcIdx.ForeignKey.Name,
			srcTable.Name, srcIdx.ColumnNames, targetTable.Name, targetIdx.ColumnNames,
			query,
		)

		values, err := ie.QueryRow(ctx, "validate foreign key constraint", txn, query)
		if err != nil {
			return err
		}
		if values.Len() > 0 {
			return pgerror.Newf(pgcode.ForeignKeyViolation,
				"foreign key violation: MATCH FULL does not allow mixing of null and nonnull values %s for %s",
				formatValues(colNames, values), srcIdx.ForeignKey.Name,
			)
		}
	}
	query, colNames, err := nonMatchingRowQuery(
		prefix, srcTable, srcIdx, targetTable.ID, targetIdx,
		true, /* limitResults */
	)
	if err != nil {
		return err
	}

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
		return pgerror.Newf(pgcode.ForeignKeyViolation,
			"foreign key violation: %q row %s has no match in %q",
			srcTable.Name, formatValues(colNames, values), targetTable.Name)
	}
	return nil
}

func formatValues(colNames []string, values tree.Datums) string {
	var pairs bytes.Buffer
	for i := range values {
		if i > 0 {
			pairs.WriteString(", ")
		}
		pairs.WriteString(fmt.Sprintf("%s=%v", colNames[i], values[i]))
	}
	return pairs.String()
}

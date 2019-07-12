// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	srcTbl *sqlbase.TableDescriptor, fk *sqlbase.ForeignKeyConstraint, limitResults bool,
) (sql string, colNames []string, _ error) {
	nCols := len(fk.OriginColumnIDs)
	srcCols := make([]string, nCols)
	srcNotNullClause := make([]string, nCols)
	for i := 0; i < nCols; i++ {
		col, err := srcTbl.FindColumnByID(fk.OriginColumnIDs[i])
		if err != nil {
			return "", nil, err
		}
		srcCols[i] = tree.NameString(col.Name)
		srcNotNullClause[i] = fmt.Sprintf("%s IS NOT NULL", srcCols[i])
	}

	limit := ""
	if limitResults {
		limit = " LIMIT 1"
	}
	return fmt.Sprintf(
		`SELECT %[1]s FROM [%[2]d AS tbl] WHERE NOT ((COALESCE(%[3]s) IS NULL) OR (%[4]s)) %[5]s`,
		strings.Join(srcCols, ","),              // 1
		srcTbl.ID,                               // 2
		strings.Join(srcCols, ", "),             // 3
		strings.Join(srcNotNullClause, " AND "), // 4
		limit,                                   // 5
	), srcCols, nil
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
	srcTbl *sqlbase.TableDescriptor,
	fk *sqlbase.ForeignKeyConstraint,
	targetTbl *sqlbase.TableDescriptor,
	limitResults bool,
) (sql string, originColNames []string, _ error) {
	originColNames, err := srcTbl.NamesForColumnIDs(fk.OriginColumnIDs)
	if err != nil {
		return "", nil, err
	}

	referencedColNames, err := targetTbl.NamesForColumnIDs(fk.ReferencedColumnIDs)
	if err != nil {
		return "", nil, err
	}

	nCols := len(fk.OriginColumnIDs)
	srcCols := make([]string, nCols)
	qualifiedSrcCols := make([]string, nCols)
	for i, n := range originColNames {
		srcCols[i] = tree.NameString(n)
		// s is the table alias used in the query.
		qualifiedSrcCols[i] = fmt.Sprintf("s.%s", srcCols[i])
	}

	srcWhere := make([]string, nCols)
	targetCols := make([]string, nCols)
	on := make([]string, nCols)

	for i := 0; i < nCols; i++ {
		// s and t are table aliases used in the query
		srcWhere[i] = fmt.Sprintf("%s IS NOT NULL", srcCols[i])
		targetCols[i] = fmt.Sprintf("t.%s", tree.NameString(referencedColNames[i]))
		on[i] = fmt.Sprintf("%s = %s", qualifiedSrcCols[i], targetCols[i])
	}

	limit := ""
	if limitResults {
		limit = " LIMIT 1"
	}
	return fmt.Sprintf(
		`SELECT %[1]s FROM 
		  (SELECT %[2]s FROM [%[3]d AS src]@{IGNORE_FOREIGN_KEYS} WHERE %[4]s) AS s
			LEFT OUTER JOIN
			(SELECT * FROM [%[5]d AS target]) AS t
			ON %[6]s
		 WHERE %[7]s IS NULL %[8]s`,
		strings.Join(qualifiedSrcCols, ", "), // 1
		strings.Join(srcCols, ", "),          // 2
		srcTbl.ID,                            // 3
		strings.Join(srcWhere, " AND "),      // 4
		targetTbl.ID,                         // 5
		strings.Join(on, " AND "),            // 6
		// Sufficient to check the first column to see whether there was no matching row
		targetCols[0], // 7
		limit,         // 8
	), originColNames, nil
}

func validateForeignKey(
	ctx context.Context,
	srcTable *sqlbase.TableDescriptor,
	fk *sqlbase.ForeignKeyConstraint,
	ie tree.SessionBoundInternalExecutor,
	txn *client.Txn,
) error {
	targetTable, err := sqlbase.GetTableDescFromID(ctx, txn, fk.ReferencedTableID)
	if err != nil {
		return err
	}

	nCols := len(fk.OriginColumnIDs)

	referencedColumnNames, err := targetTable.NamesForColumnIDs(fk.ReferencedColumnIDs)
	if err != nil {
		return err
	}

	// For MATCH FULL FKs, first check whether any disallowed keys containing both
	// null and non-null values exist.
	// (The matching options only matter for FKs with more than one column.)
	if nCols > 1 && fk.Match == sqlbase.ForeignKeyReference_FULL {
		query, colNames, err := matchFullUnacceptableKeyQuery(
			srcTable, fk, true, /* limitResults */
		)
		if err != nil {
			return err
		}

		log.Infof(ctx, "Validating MATCH FULL FK %q (%q [%v] -> %q [%v]) with query %q",
			fk.Name,
			srcTable.Name, colNames,
			targetTable.Name, referencedColumnNames,
			query,
		)

		values, err := ie.QueryRow(ctx, "validate foreign key constraint", txn, query)
		if err != nil {
			return err
		}
		if values.Len() > 0 {
			return pgerror.Newf(pgcode.ForeignKeyViolation,
				"foreign key violation: MATCH FULL does not allow mixing of null and nonnull values %s for %s",
				formatValues(colNames, values), fk.Name,
			)
		}
	}
	query, colNames, err := nonMatchingRowQuery(
		srcTable, fk, targetTable,
		true, /* limitResults */
	)
	if err != nil {
		return err
	}

	log.Infof(ctx, "Validating FK %q (%q [%v] -> %q [%v]) with query %q",
		fk.Name,
		srcTable.Name, colNames, targetTable.Name, referencedColumnNames,
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

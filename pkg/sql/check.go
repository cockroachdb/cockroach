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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// validateCheckExpr verifies that the given CHECK expression returns true
// for all the rows in the table.
//
// It operates entirely on the current goroutine and is thus able to
// reuse an existing client.Txn safely.
func validateCheckExpr(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	exprStr string,
	tableDesc *sqlbase.MutableTableDescriptor,
	ie *InternalExecutor,
	txn *kv.Txn,
) error {
	expr, err := schemaexpr.DeserializeTableDescExpr(ctx, semaCtx, tableDesc, exprStr)
	if err != nil {
		return err
	}
	// Construct AST and then convert to a string, to avoid problems with escaping the check expression
	tblref := tree.TableRef{TableID: int64(tableDesc.GetID()), As: tree.AliasClause{Alias: "t"}}
	sel := &tree.SelectClause{
		Exprs: sqlbase.ColumnsSelectors(tableDesc.Columns),
		From:  tree.From{Tables: []tree.TableExpr{&tblref}},
		Where: &tree.Where{Type: tree.AstWhere, Expr: &tree.NotExpr{Expr: expr}},
	}
	lim := &tree.Limit{Count: tree.NewDInt(1)}
	stmt := &tree.Select{Select: sel, Limit: lim}
	queryStr := tree.AsStringWithFlags(stmt, tree.FmtSerializable)
	log.Infof(ctx, "Validating check constraint %q with query %q", tree.SerializeForDisplay(expr), queryStr)

	rows, err := ie.QueryRow(ctx, "validate check constraint", txn, queryStr)
	if err != nil {
		return err
	}
	if rows.Len() > 0 {
		return pgerror.Newf(pgcode.CheckViolation,
			"validation of CHECK %q failed on row: %s",
			tree.SerializeForDisplay(expr), labeledRowValues(tableDesc.Columns, rows))
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
//   (a_id IS NULL OR b_id IS NULL) AND (a_id IS NOT NULL OR b_id IS NOT NULL)
// LIMIT 1;
func matchFullUnacceptableKeyQuery(
	srcTbl *sqlbase.TableDescriptor, fk *sqlbase.ForeignKeyConstraint, limitResults bool,
) (sql string, colNames []string, _ error) {
	nCols := len(fk.OriginColumnIDs)
	srcCols := make([]string, nCols)
	srcNullExistsClause := make([]string, nCols)
	srcNotNullExistsClause := make([]string, nCols)

	returnedCols := srcCols
	for i := 0; i < nCols; i++ {
		col, err := srcTbl.FindColumnByID(fk.OriginColumnIDs[i])
		if err != nil {
			return "", nil, err
		}
		srcCols[i] = tree.NameString(col.Name)
		srcNullExistsClause[i] = fmt.Sprintf("%s IS NULL", srcCols[i])
		srcNotNullExistsClause[i] = fmt.Sprintf("%s IS NOT NULL", srcCols[i])
	}

	for _, id := range srcTbl.PrimaryIndex.ColumnIDs {
		alreadyPresent := false
		for _, otherID := range fk.OriginColumnIDs {
			if id == otherID {
				alreadyPresent = true
				break
			}
		}
		if !alreadyPresent {
			col, err := srcTbl.FindActiveColumnByID(id)
			if err != nil {
				return "", nil, err
			}
			returnedCols = append(returnedCols, col.Name)
		}
	}

	limit := ""
	if limitResults {
		limit = " LIMIT 1"
	}
	return fmt.Sprintf(
		`SELECT %[1]s FROM [%[2]d AS tbl] WHERE (%[3]s) AND (%[4]s) %[5]s`,
		strings.Join(returnedCols, ","),              // 1
		srcTbl.ID,                                    // 2
		strings.Join(srcNullExistsClause, " OR "),    // 3
		strings.Join(srcNotNullExistsClause, " OR "), // 4
		limit, // 5
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
	// Get primary key columns not included in the FK
	for _, pkColID := range srcTbl.PrimaryIndex.ColumnIDs {
		found := false
		for _, id := range fk.OriginColumnIDs {
			if pkColID == id {
				found = true
				break
			}
		}
		if !found {
			column, err := srcTbl.FindActiveColumnByID(pkColID)
			if err != nil {
				return "", nil, err
			}
			originColNames = append(originColNames, column.Name)
		}
	}
	srcCols := make([]string, len(originColNames))
	qualifiedSrcCols := make([]string, len(originColNames))
	for i, n := range originColNames {
		srcCols[i] = tree.NameString(n)
		// s is the table alias used in the query.
		qualifiedSrcCols[i] = fmt.Sprintf("s.%s", srcCols[i])
	}

	referencedColNames, err := targetTbl.NamesForColumnIDs(fk.ReferencedColumnIDs)
	if err != nil {
		return "", nil, err
	}
	nCols := len(fk.OriginColumnIDs)
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

// validateForeignKey verifies that all the rows in the srcTable
// have a matching row in their referenced table.
//
// It operates entirely on the current goroutine and is thus able to
// reuse an existing client.Txn safely.
func validateForeignKey(
	ctx context.Context,
	srcTable *sqlbase.TableDescriptor,
	fk *sqlbase.ForeignKeyConstraint,
	ie *InternalExecutor,
	txn *kv.Txn,
	codec keys.SQLCodec,
) error {
	targetTable, err := sqlbase.GetTableDescFromID(ctx, txn, codec, fk.ReferencedTableID)
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

// checkSet contains a subset of checks, as ordinals into
// ImmutableTableDescriptor.ActiveChecks. These checks have boolean columns
// produced as input to mutations, indicating the result of evaluating the
// check.
//
// It is allowed to check only a subset of the active checks (the optimizer
// could in principle determine that some checks can't fail because they
// statically evaluate to true for the entire input).
type checkSet = util.FastIntSet

// When executing mutations, we calculate a boolean column for each check
// indicating if the check passed. This function verifies that each result is
// true or null.
//
// It is allowed to check only a subset of the active checks (for some, we could
// determine that they can't fail because they statically evaluate to true for
// the entire input); checkOrds contains the set of checks for which we have
// values, as ordinals into ActiveChecks(). There must be exactly one value in
// checkVals for each element in checkSet.
func checkMutationInput(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	tabDesc *sqlbase.ImmutableTableDescriptor,
	checkOrds checkSet,
	checkVals tree.Datums,
) error {
	if len(checkVals) < checkOrds.Len() {
		return errors.AssertionFailedf(
			"mismatched check constraint columns: expected %d, got %d", checkOrds.Len(), len(checkVals))
	}

	checks := tabDesc.ActiveChecks()
	colIdx := 0
	for i := range checks {
		if !checkOrds.Contains(i) {
			continue
		}

		if res, err := tree.GetBool(checkVals[colIdx]); err != nil {
			return err
		} else if !res && checkVals[colIdx] != tree.DNull {
			// Failed to satisfy CHECK constraint, so unwrap the serialized
			// check expression to display to the user.
			expr, exprErr := schemaexpr.DeserializeTableDescExpr(ctx, semaCtx, tabDesc, checks[i].Expr)
			if exprErr != nil {
				// If we ran into an error trying to read the check constraint, wrap it
				// and return.
				return errors.Wrapf(exprErr, "failed to satisfy CHECK constraint (%s)", checks[i].Expr)
			}
			return pgerror.Newf(
				pgcode.CheckViolation, "failed to satisfy CHECK constraint (%s)", tree.SerializeForDisplay(expr),
			)
		}
		colIdx++
	}
	return nil
}

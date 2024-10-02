// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/semenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

// validateCheckExpr verifies that the given CHECK expression returns true
// for all the rows in the table.
// `indexIDForValidation`, if non-zero, is used to explicit hint the
// validation query to validate against a specific index.
//
// It operates entirely on the current goroutine and is thus able to
// reuse an existing kv.Txn safely.
func validateCheckExpr(
	ctx context.Context,
	evalCtx *eval.Context,
	semaCtx *tree.SemaContext,
	txn isql.Txn,
	sessionData *sessiondata.SessionData,
	exprStr string,
	tableDesc *tabledesc.Mutable,
	indexIDForValidation descpb.IndexID,
) (violatingRow tree.Datums, formattedCkExpr string, err error) {
	formattedCkExpr, err = schemaexpr.FormatExprForDisplay(ctx, tableDesc, exprStr, evalCtx, semaCtx, sessionData, tree.FmtParsable)
	if err != nil {
		return nil, formattedCkExpr, err
	}
	colSelectors := tabledesc.ColumnsSelectors(tableDesc.AccessibleColumns())
	columns := tree.AsStringWithFlags(&colSelectors, tree.FmtSerializable)
	queryStr := fmt.Sprintf(`SELECT %s FROM [%d AS t] WHERE NOT (%s) LIMIT 1`, columns, tableDesc.GetID(), exprStr)
	if indexIDForValidation != 0 {
		queryStr = fmt.Sprintf(`SELECT %s FROM [%d AS t]@[%d] WHERE NOT (%s) LIMIT 1`, columns, tableDesc.GetID(), indexIDForValidation, exprStr)
	}
	log.Infof(ctx, "validating check constraint %q with query %q", formattedCkExpr, queryStr)
	violatingRow, err = txn.QueryRowEx(
		ctx,
		"validate check constraint",
		txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		queryStr)
	if err != nil {
		return nil, formattedCkExpr, err
	}
	return violatingRow, formattedCkExpr, nil
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
//
//	(a_id IS NULL OR b_id IS NULL) AND (a_id IS NOT NULL OR b_id IS NOT NULL)
//
// LIMIT 1;
func matchFullUnacceptableKeyQuery(
	srcTbl catalog.TableDescriptor, fk *descpb.ForeignKeyConstraint, limitResults bool,
) (sql string, colNames []string, _ error) {
	nCols := len(fk.OriginColumnIDs)
	srcCols := make([]string, nCols)
	srcNullExistsClause := make([]string, nCols)
	srcNotNullExistsClause := make([]string, nCols)

	returnedCols := srcCols
	for i := 0; i < nCols; i++ {
		col, err := catalog.MustFindColumnByID(srcTbl, fk.OriginColumnIDs[i])
		if err != nil {
			return "", nil, err
		}
		srcCols[i] = tree.NameString(col.GetName())
		srcNullExistsClause[i] = fmt.Sprintf("%s IS NULL", srcCols[i])
		srcNotNullExistsClause[i] = fmt.Sprintf("%s IS NOT NULL", srcCols[i])
	}

	for i := 0; i < srcTbl.GetPrimaryIndex().NumKeyColumns(); i++ {
		id := srcTbl.GetPrimaryIndex().GetKeyColumnID(i)
		alreadyPresent := false
		for _, otherID := range fk.OriginColumnIDs {
			if id == otherID {
				alreadyPresent = true
				break
			}
		}
		if !alreadyPresent {
			col, err := catalog.MustFindPublicColumnByID(srcTbl, id)
			if err != nil {
				return "", nil, err
			}
			returnedCols = append(returnedCols, col.GetName())
		}
	}

	limit := ""
	if limitResults {
		limit = " LIMIT 1"
	}
	return fmt.Sprintf(
		`SELECT %[1]s FROM [%[2]d AS tbl] WHERE (%[3]s) AND (%[4]s) %[5]s`,
		strings.Join(returnedCols, ","),              // 1
		srcTbl.GetID(),                               // 2
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
// For example, a FK constraint on columns (a_id, b_id) on the table "child",
// referencing columns (a, b) on the table "parent", would require the following
// query:
//
// SELECT s.a_id, s.b_id, s.rowid
//
//	FROM (
//	      SELECT a_id, b_id, rowid
//	        FROM [<ID of child> AS src]@{IGNORE_FOREIGN_KEYS}
//	       WHERE a_id IS NOT NULL AND b_id IS NOT NULL
//	     ) AS s
//	     LEFT JOIN [<id of parent> AS target] AS t ON s.a_id = t.a AND s.b_id = t.b
//
// WHERE t.a IS NULL
// LIMIT 1  -- if limitResults is set
//
// It is possible to force FK validation query to perform against a particular
// index, as specified in `indexIDForValidation` when it's non zero. This is necessary
// if we are validating a FK constraint on a primary index that's being added (e.g.
// `ADD COLUMN ... REFERENCES other_table(...)`).
//
// TODO(radu): change this to a query which executes as an anti-join when we
// remove the heuristic planner.
func nonMatchingRowQuery(
	srcTbl catalog.TableDescriptor,
	fk *descpb.ForeignKeyConstraint,
	targetTbl catalog.TableDescriptor,
	indexIDForValidation descpb.IndexID,
	limitResults bool,
) (sql string, originColNames []string, _ error) {
	originColNames, err := catalog.ColumnNamesForIDs(srcTbl, fk.OriginColumnIDs)
	if err != nil {
		return "", nil, err
	}
	// Get primary key columns not included in the FK
	for i := 0; i < srcTbl.GetPrimaryIndex().NumKeyColumns(); i++ {
		pkColID := srcTbl.GetPrimaryIndex().GetKeyColumnID(i)
		found := false
		for _, id := range fk.OriginColumnIDs {
			if pkColID == id {
				found = true
				break
			}
		}
		if !found {
			column, err := catalog.MustFindPublicColumnByID(srcTbl, pkColID)
			if err != nil {
				return "", nil, err
			}
			originColNames = append(originColNames, column.GetName())
		}
	}
	srcCols := make([]string, len(originColNames))
	qualifiedSrcCols := make([]string, len(originColNames))
	for i, n := range originColNames {
		srcCols[i] = tree.NameString(n)
		// s is the table alias used in the query.
		qualifiedSrcCols[i] = fmt.Sprintf("s.%s", srcCols[i])
	}

	referencedColNames, err := catalog.ColumnNamesForIDs(targetTbl, fk.ReferencedColumnIDs)
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
	query := fmt.Sprintf(
		`SELECT %[1]s FROM 
		  (SELECT %[2]s FROM [%[3]d AS src]@{IGNORE_FOREIGN_KEYS} WHERE %[4]s) AS s
			LEFT OUTER JOIN
			[%[5]d AS target] AS t
			ON %[6]s
		 WHERE %[7]s IS NULL %[8]s`,
		strings.Join(qualifiedSrcCols, ", "), // 1
		strings.Join(srcCols, ", "),          // 2
		srcTbl.GetID(),                       // 3
		strings.Join(srcWhere, " AND "),      // 4
		targetTbl.GetID(),                    // 5
		strings.Join(on, " AND "),            // 6
		// Sufficient to check the first column to see whether there was no matching row
		targetCols[0], // 7
		limit,         // 8
	)
	if indexIDForValidation != 0 {
		query = fmt.Sprintf(
			`SELECT %[1]s FROM 
		  (SELECT %[2]s FROM [%[3]d AS src]@{IGNORE_FOREIGN_KEYS, FORCE_INDEX=[%[4]d]} WHERE %[5]s) AS s
			LEFT OUTER JOIN
			[%[6]d AS target] AS t
			ON %[7]s
		 WHERE %[8]s IS NULL %[9]s`,
			strings.Join(qualifiedSrcCols, ", "), // 1
			strings.Join(srcCols, ", "),          // 2
			srcTbl.GetID(),                       // 3
			indexIDForValidation,                 // 4
			strings.Join(srcWhere, " AND "),      // 5
			targetTbl.GetID(),                    // 6
			strings.Join(on, " AND "),            // 7
			// Sufficient to check the first column to see whether there was no matching row
			targetCols[0], // 8
			limit,         // 9
		)
	}
	return query, originColNames, nil
}

// validateForeignKey verifies that all the rows in the srcTable
// have a matching row in their referenced table.
//
// It operates entirely on the current goroutine and is thus able to
// reuse an existing kv.Txn safely.
func validateForeignKey(
	ctx context.Context,
	txn isql.Txn,
	srcTable *tabledesc.Mutable,
	targetTable catalog.TableDescriptor,
	fk *descpb.ForeignKeyConstraint,
	indexIDForValidation descpb.IndexID,
) error {
	nCols := len(fk.OriginColumnIDs)

	referencedColumnNames, err := catalog.ColumnNamesForIDs(targetTable, fk.ReferencedColumnIDs)
	if err != nil {
		return err
	}

	// For MATCH FULL FKs, first check whether any disallowed keys containing both
	// null and non-null values exist.
	// (The matching options only matter for FKs with more than one column.)
	if nCols > 1 && fk.Match == semenumpb.Match_FULL {
		query, colNames, err := matchFullUnacceptableKeyQuery(
			srcTable, fk, true, /* limitResults */
		)
		if err != nil {
			return err
		}

		log.Infof(ctx, "validating MATCH FULL FK %q (%q [%v] -> %q [%v]) with query %q",
			fk.Name,
			srcTable.Name, colNames,
			targetTable.GetName(), referencedColumnNames,
			query,
		)

		values, err := txn.QueryRowEx(ctx, "validate foreign key constraint",
			txn.KV(),
			sessiondata.NodeUserSessionDataOverride, query)
		if err != nil {
			return err
		}
		if values.Len() > 0 {
			return pgerror.WithConstraintName(pgerror.Newf(pgcode.ForeignKeyViolation,
				"foreign key violation: MATCH FULL does not allow mixing of null and nonnull values %s for %s",
				formatValues(colNames, values), fk.Name,
			), fk.Name)
		}
	}
	query, colNames, err := nonMatchingRowQuery(srcTable, fk, targetTable, indexIDForValidation, true /* limitResults */)
	if err != nil {
		return err
	}

	log.Infof(ctx, "validating FK %q (%q [%v] -> %q [%v]) with query %q",
		fk.Name,
		srcTable.Name, colNames, targetTable.GetName(), referencedColumnNames,
		query,
	)

	values, err := txn.QueryRowEx(ctx, "validate fk constraint", txn.KV(),
		sessiondata.NodeUserSessionDataOverride, query)
	if err != nil {
		return err
	}
	if values.Len() > 0 {
		return pgerror.WithConstraintName(pgerror.Newf(pgcode.ForeignKeyViolation,
			"foreign key violation: %q row %s has no match in %q",
			srcTable.Name, formatValues(colNames, values), targetTable.GetName()), fk.Name)
	}
	return nil
}

// duplicateRowQuery generates and returns a query for column values that
// violate the specified unique constraint. Rows in the table with any null
// values in the key are excluded from matching.
//
// For example, a unique constraint on columns (a, b) on the table "tbl" would
// require the following query:
//
// SELECT a, b
// FROM tbl
// WHERE a IS NOT NULL AND b IS NOT NULL
// GROUP BY a, b
// HAVING count(*) > 1
// LIMIT 1  -- if limitResults is set
//
// The pred argument is a partial unique constraint predicate, which filters the
// subset of rows that are guaranteed unique by the constraint. If the unique
// constraint is not partial, pred should be empty.
//
// `indexIDForValidation`, if non-zero, will be used to force the sql query to
// use this particular index by hinting the query.
func duplicateRowQuery(
	srcTbl catalog.TableDescriptor,
	columnIDs []descpb.ColumnID,
	pred string,
	indexIDForValidation descpb.IndexID,
	limitResults bool,
) (sql string, colNames []string, _ error) {
	colNames, err := catalog.ColumnNamesForIDs(srcTbl, columnIDs)
	if err != nil {
		return "", nil, err
	}

	srcCols := make([]string, len(colNames))
	for i, n := range colNames {
		srcCols[i] = tree.NameString(n)
	}

	// There will be an expression in the WHERE clause for each of the columns,
	// and possibly one for pred.
	srcWhere := make([]string, 0, len(srcCols)+1)
	for i := range srcCols {
		srcWhere = append(srcWhere, fmt.Sprintf("%s IS NOT NULL", srcCols[i]))
	}

	// Wrap the predicate in parentheses.
	if pred != "" {
		srcWhere = append(srcWhere, fmt.Sprintf("(%s)", pred))
	}

	limit := ""
	if limitResults {
		limit = " LIMIT 1"
	}
	query := fmt.Sprintf(
		`SELECT %[1]s FROM [%[2]d AS tbl] WHERE %[3]s GROUP BY %[1]s HAVING count(*) > 1 %[4]s`,
		strings.Join(srcCols, ", "),     // 1
		srcTbl.GetID(),                  // 2
		strings.Join(srcWhere, " AND "), // 3
		limit,                           // 4
	)
	if indexIDForValidation != 0 {
		query = fmt.Sprintf(
			`SELECT %[1]s FROM [%[2]d AS tbl]@[%[3]d] WHERE %[4]s GROUP BY %[1]s HAVING count(*) > 1 %[5]s`,
			strings.Join(srcCols, ", "),     // 1
			srcTbl.GetID(),                  // 2
			indexIDForValidation,            // 3
			strings.Join(srcWhere, " AND "), // 4
			limit,                           // 5
		)
	}
	return query, colNames, nil
}

// RevalidateUniqueConstraintsInCurrentDB verifies that all unique constraints
// defined on tables in the current database are valid. In other words, it
// verifies that for every table in the database with one or more unique
// constraints, all rows in the table have unique values for every unique
// constraint defined on the table.
func (p *planner) RevalidateUniqueConstraintsInCurrentDB(ctx context.Context) error {
	dbName := p.CurrentDatabase()
	log.Infof(ctx, "validating unique constraints in database %s", dbName)
	db, err := p.Descriptors().ByNameWithLeased(p.Txn()).Get().Database(ctx, dbName)
	if err != nil {
		return err
	}
	inDB, err := p.Descriptors().GetAllTablesInDatabase(ctx, p.Txn(), db)
	if err != nil {
		return err
	}
	return inDB.ForEachDescriptor(func(desc catalog.Descriptor) error {
		// If the context is cancelled, then we should bail out, since
		// the actual revalidate operation might not check anything.
		if err := ctx.Err(); err != nil {
			return err
		}
		tableDesc, err := catalog.AsTableDescriptor(desc)
		if err != nil {
			return err
		}
		return RevalidateUniqueConstraintsInTable(
			ctx, p.InternalSQLTxn(), p.User(), tableDesc,
		)
	})
}

// RevalidateUniqueConstraintsInTable verifies that all unique constraints
// defined on the given table are valid. In other words, it verifies that all
// rows in the table have unique values for every unique constraint defined on
// the table.
func (p *planner) RevalidateUniqueConstraintsInTable(ctx context.Context, tableID int) error {
	tableDesc, err := p.Descriptors().ByIDWithLeased(p.Txn()).WithoutNonPublic().Get().Table(ctx, descpb.ID(tableID))
	if err != nil {
		return err
	}
	return RevalidateUniqueConstraintsInTable(
		ctx, p.InternalSQLTxn(), p.User(), tableDesc,
	)
}

// RevalidateUniqueConstraint verifies that the given unique constraint on the
// given table is valid. In other words, it verifies that all rows in the
// table have unique values for the columns in the constraint. Returns an
// error if validation fails or if constraintName is not actually a unique
// constraint on the table.
func (p *planner) RevalidateUniqueConstraint(
	ctx context.Context, tableID int, constraintName string,
) error {
	tableDesc, err := p.Descriptors().ByIDWithLeased(p.Txn()).WithoutNonPublic().Get().Table(ctx, descpb.ID(tableID))
	if err != nil {
		return err
	}

	// Check implicitly partitioned UNIQUE indexes.
	for _, index := range tableDesc.ActiveIndexes() {
		if index.GetName() == constraintName {
			if !index.IsUnique() {
				return errors.Newf("%s is not a unique constraint", constraintName)
			}
			if index.ImplicitPartitioningColumnCount() > 0 {
				return validateUniqueConstraint(
					ctx,
					tableDesc,
					index.GetName(),
					index.IndexDesc().KeyColumnIDs[index.ImplicitPartitioningColumnCount():],
					index.GetPredicate(),
					0, /* indexIDForValidation */
					p.InternalSQLTxn(),
					p.User(),
					true, /* preExisting */
				)
			}
			// We found the unique index but we don't need to bother validating it.
			return nil
		}
	}

	// Check UNIQUE WITHOUT INDEX constraints.
	for _, uc := range tableDesc.EnforcedUniqueConstraintsWithoutIndex() {
		if uc.GetName() == constraintName {
			return validateUniqueConstraint(
				ctx,
				tableDesc,
				uc.GetName(),
				uc.CollectKeyColumnIDs().Ordered(),
				uc.GetPredicate(),
				0, /* indexIDForValidation */
				p.InternalSQLTxn(),
				p.User(),
				true, /* preExisting */
			)
		}
	}

	return errors.Newf("unique constraint %s does not exist", constraintName)
}

// IsConstraintActive returns if a given constraint is currently active,
// for the current transaction.
func (p *planner) IsConstraintActive(
	ctx context.Context, tableID int, constraintName string,
) (bool, error) {
	tableDesc, err := p.Descriptors().ByIDWithLeased(p.Txn()).WithoutNonPublic().Get().Table(ctx, descpb.ID(tableID))
	if err != nil {
		return false, err
	}
	constraint := catalog.FindConstraintByName(tableDesc, constraintName)
	return constraint != nil && constraint.IsEnforced(), nil
}

// HasVirtualUniqueConstraints returns true if the table has one or more
// constraints that are validated by RevalidateUniqueConstraintsInTable.
func HasVirtualUniqueConstraints(tableDesc catalog.TableDescriptor) bool {
	for _, index := range tableDesc.ActiveIndexes() {
		if index.IsUnique() && index.ImplicitPartitioningColumnCount() > 0 {
			return true
		}
	}
	for _, uc := range tableDesc.EnforcedUniqueConstraintsWithoutIndex() {
		if uc.IsConstraintValidated() {
			return true
		}
	}
	return false
}

// RevalidateUniqueConstraintsInTable verifies that all unique constraints
// defined on the given table are valid. In other words, it verifies that all
// rows in the table have unique values for every unique constraint defined on
// the table.
//
// Note that we only need to validate UNIQUE constraints that are not already
// enforced by an index. This includes implicitly partitioned UNIQUE indexes
// and UNIQUE WITHOUT INDEX constraints.
func RevalidateUniqueConstraintsInTable(
	ctx context.Context, txn isql.Txn, user username.SQLUsername, tableDesc catalog.TableDescriptor,
) error {
	// Check implicitly partitioned UNIQUE indexes.
	for _, index := range tableDesc.ActiveIndexes() {
		if index.IsUnique() && index.ImplicitPartitioningColumnCount() > 0 {
			if err := validateUniqueConstraint(
				ctx,
				tableDesc,
				index.GetName(),
				index.IndexDesc().KeyColumnIDs[index.ImplicitPartitioningColumnCount():],
				index.GetPredicate(),
				0, /* indexIDForValidation */
				txn,
				user,
				true, /* preExisting */
			); err != nil {
				log.Errorf(ctx, "validation of unique constraints failed for table %s: %s", tableDesc.GetName(), err)
				return errors.Wrapf(err, "for table %s", tableDesc.GetName())
			}
		}
	}

	// Check UNIQUE WITHOUT INDEX constraints.
	for _, uc := range tableDesc.EnforcedUniqueConstraintsWithoutIndex() {
		if uc.IsConstraintValidated() {
			if err := validateUniqueConstraint(
				ctx,
				tableDesc,
				uc.GetName(),
				uc.CollectKeyColumnIDs().Ordered(),
				uc.GetPredicate(),
				0, /* indexIDForValidation */
				txn,
				user,
				true, /* preExisting */
			); err != nil {
				log.Errorf(ctx, "validation of unique constraints failed for table %s: %s", tableDesc.GetName(), err)
				return errors.Wrapf(err, "for table %s", tableDesc.GetName())
			}
		}
	}

	log.Infof(ctx, "validated all unique constraints in table %s", tableDesc.GetName())
	return nil
}

// validateUniqueConstraint verifies that all the rows in the srcTable
// have unique values for the given columns.
//
// `indexIDForValidation`, if non-zero, will be used to force validation
// against this particular index. This is used to facilitate the declarative
// schema changer when the validation should be against a yet non-public
// primary index.
//
// It operates entirely on the current goroutine and is thus able to
// reuse an existing kv.Txn safely.
//
// preExisting indicates whether this constraint already exists, and therefore
// informs the error message that gets produced.
func validateUniqueConstraint(
	ctx context.Context,
	srcTable catalog.TableDescriptor,
	constraintName string,
	columnIDs []descpb.ColumnID,
	pred string,
	indexIDForValidation descpb.IndexID,
	txn isql.Txn,
	user username.SQLUsername,
	preExisting bool,
) error {
	query, colNames, err := duplicateRowQuery(
		srcTable, columnIDs, pred, indexIDForValidation, true, /* limitResults */
	)
	if err != nil {
		return err
	}

	log.Infof(ctx, "validating unique constraint %q (%q [%v]) with query %q",
		constraintName,
		srcTable.GetName(),
		colNames,
		query,
	)

	sessionDataOverride := sessiondata.NoSessionDataOverride
	sessionDataOverride.User = user
	// We are likely to have performed a lot of work before getting here (e.g.
	// importing the data), so we want to make an effort in order to run the
	// validation query without error in order to not fail the whole operation.
	// Thus, we allow up to 5 retries with an exponential backoff for an
	// allowlist of errors.
	//
	// We choose to explicitly perform the retry here rather than propagate the
	// error as "job retryable" and relying on the jobs framework to do the
	// retries in order to not waste (a lot of) work that was performed before
	// we got here.
	var values tree.Datums
	retryOptions := retry.Options{
		InitialBackoff: 20 * time.Millisecond,
		Multiplier:     1.5,
		MaxRetries:     5,
	}
	for r := retry.StartWithCtx(ctx, retryOptions); r.Next(); {
		values, err = txn.QueryRowEx(ctx, "validate unique constraint", txn.KV(), sessionDataOverride, query)
		if err == nil {
			break
		}
		if pgerror.IsSQLRetryableError(err) || flowinfra.IsFlowRetryableError(err) {
			// An example error that we want to retry is "no inbound stream"
			// connection error which can occur if the node that is used for the
			// distributed query goes down.
			log.Infof(ctx, "retrying the validation query because of %v", err)
			continue
		}
		return err
	}
	if values.Len() > 0 {
		valuesStr := make([]string, len(values))
		for i := range values {
			valuesStr[i] = values[i].String()
		}
		// Note: this error message mirrors the message produced by Postgres
		// when it fails to add a unique index due to duplicated keys.
		errMsg := "could not create unique constraint"
		if preExisting {
			errMsg = "failed to validate unique constraint"
		}
		return errors.WithDetail(
			pgerror.WithConstraintName(
				pgerror.Newf(
					pgcode.UniqueViolation, "%s %q", errMsg, constraintName,
				),
				constraintName,
			),
			fmt.Sprintf(
				"Key (%s)=(%s) is duplicated.", strings.Join(colNames, ","), strings.Join(valuesStr, ","),
			),
		)
	}
	return nil
}

// ValidateTTLScheduledJobsInCurrentDB is part of the EvalPlanner interface.
func (p *planner) ValidateTTLScheduledJobsInCurrentDB(ctx context.Context) error {
	dbName := p.CurrentDatabase()
	log.Infof(ctx, "validating scheduled jobs in database %s", dbName)
	db, err := p.Descriptors().ByNameWithLeased(p.Txn()).Get().Database(ctx, dbName)
	if err != nil {
		return err
	}
	inDB, err := p.Descriptors().GetAllTablesInDatabase(ctx, p.Txn(), db)
	if err != nil {
		return err
	}
	return inDB.ForEachDescriptor(func(desc catalog.Descriptor) error {
		tableDesc, err := catalog.AsTableDescriptor(desc)
		if err != nil {
			return err
		}
		return p.validateTTLScheduledJobInTable(ctx, tableDesc)
	})
}

var invalidTableTTLScheduledJobError = errors.Newf("invalid scheduled job for table")

// validateTTLScheduledJobsInCurrentDB is part of the EvalPlanner interface.
func (p *planner) validateTTLScheduledJobInTable(
	ctx context.Context, tableDesc catalog.TableDescriptor,
) error {
	if !tableDesc.HasRowLevelTTL() {
		return nil
	}
	ttl := tableDesc.GetRowLevelTTL()

	execCfg := p.ExecCfg()
	env := JobSchedulerEnv(execCfg.JobsKnobs())

	wrapError := func(origErr error) error {
		return errors.WithHintf(
			errors.Mark(origErr, invalidTableTTLScheduledJobError),
			`use crdb_internal.repair_ttl_table_scheduled_job(%d) to repair the missing job`,
			tableDesc.GetID(),
		)
	}

	sj, err := jobs.ScheduledJobTxn(p.InternalSQLTxn()).Load(ctx, env, ttl.ScheduleID)
	if err != nil {
		if jobs.HasScheduledJobNotFoundError(err) {
			return wrapError(
				pgerror.Newf(
					pgcode.Internal,
					"table id %d maps to a non-existent schedule id %d",
					tableDesc.GetID(),
					ttl.ScheduleID,
				),
			)
		}
		return errors.Wrapf(err, "error fetching schedule id %d for table id %d", ttl.ScheduleID, tableDesc.GetID())
	}

	var args catpb.ScheduledRowLevelTTLArgs
	if err := pbtypes.UnmarshalAny(sj.ExecutionArgs().Args, &args); err != nil {
		return wrapError(
			pgerror.Wrapf(
				err,
				pgcode.Internal,
				"error unmarshalling scheduled jobs args for table id %d, schedule id %d",
				tableDesc.GetID(),
				ttl.ScheduleID,
			),
		)
	}

	if args.TableID != tableDesc.GetID() {
		return wrapError(
			pgerror.Newf(
				pgcode.Internal,
				"schedule id %d points to table id %d instead of table id %d",
				ttl.ScheduleID,
				args.TableID,
				tableDesc.GetID(),
			),
		)
	}

	return nil
}

// RepairTTLScheduledJobForTable is part of the EvalPlanner interface.
func (p *planner) RepairTTLScheduledJobForTable(ctx context.Context, tableID int64) error {
	tableDesc, err := p.Descriptors().MutableByID(p.txn).Table(ctx, descpb.ID(tableID))
	if err != nil {
		return err
	}
	validateErr := p.validateTTLScheduledJobInTable(ctx, tableDesc)
	if validateErr == nil {
		return nil
	}
	if !errors.HasType(validateErr, invalidTableTTLScheduledJobError) {
		return errors.Wrap(validateErr, "error validating TTL on table")
	}
	sj, err := CreateRowLevelTTLScheduledJob(
		ctx,
		p.ExecCfg().JobsKnobs(),
		jobs.ScheduledJobTxn(p.InternalSQLTxn()),
		p.User(),
		tableDesc,
		p.extendedEvalCtx.ClusterID,
		p.extendedEvalCtx.Settings.Version.ActiveVersion(ctx),
	)
	if err != nil {
		return err
	}
	tableDesc.RowLevelTTL.ScheduleID = sj.ScheduleID()
	return p.Descriptors().WriteDesc(
		ctx, false /* kvTrace */, tableDesc, p.txn,
	)
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
// immutable.ActiveChecks. These checks have boolean columns
// produced as input to mutations, indicating the result of evaluating the
// check.
//
// It is allowed to check only a subset of the active checks (the optimizer
// could in principle determine that some checks can't fail because they
// statically evaluate to true for the entire input).
type checkSet = intsets.Fast

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
	evalCtx *eval.Context,
	semaCtx *tree.SemaContext,
	sessionData *sessiondata.SessionData,
	tabDesc catalog.TableDescriptor,
	checkOrds checkSet,
	checkVals tree.Datums,
) error {
	if len(checkVals) < checkOrds.Len() {
		return errors.AssertionFailedf(
			"mismatched check constraint columns: expected %d, got %d", checkOrds.Len(), len(checkVals))
	}

	checks := tabDesc.EnforcedCheckConstraints()
	colIdx := 0
	for i := range checks {
		if !checkOrds.Contains(i) {
			continue
		}

		if res, err := tree.GetBool(checkVals[colIdx]); err != nil {
			return err
		} else if !res && checkVals[colIdx] != tree.DNull {
			return row.CheckFailed(ctx, evalCtx, semaCtx, sessionData, tabDesc, checks[i])
		}
		colIdx++
	}
	return nil
}

func newCheckViolationErr(
	ckExpr string, tableColumns []catalog.Column, violatingRow tree.Datums,
) error {
	return pgerror.Newf(pgcode.CheckViolation,
		"validation of CHECK %q failed on row: %s",
		ckExpr, labeledRowValues(tableColumns, violatingRow))
}

func newNotNullViolationErr(
	notNullColName string, tableColumns []catalog.Column, violatingRow tree.Datums,
) error {
	return pgerror.Newf(pgcode.NotNullViolation,
		"validation of column %q NOT NULL failed on row: %s",
		notNullColName, labeledRowValues(tableColumns, violatingRow))
}

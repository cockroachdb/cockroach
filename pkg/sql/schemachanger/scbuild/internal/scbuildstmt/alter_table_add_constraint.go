// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

func alterTableAddConstraint(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	stmt tree.Statement,
	t *tree.AlterTableAddConstraint,
) {
	switch d := t.ConstraintDef.(type) {
	case *tree.UniqueConstraintTableDef:
		if d.PrimaryKey {
			alterTableAddPrimaryKey(b, tn, tbl, stmt, t)
		} else if d.WithoutIndex {
			alterTableAddUniqueWithoutIndex(b, tn, tbl, t)
		} else {
			if t.ValidationBehavior == tree.ValidationSkip {
				panic(sqlerrors.NewUnsupportedUnvalidatedConstraintError(catconstants.ConstraintTypeUnique))
			}
			CreateIndex(b, &tree.CreateIndex{
				Name:        d.Name,
				Table:       *tn,
				Unique:      true,
				Columns:     d.Columns,
				Predicate:   d.Predicate,
				IfNotExists: d.IfNotExists,
			})
		}
	case *tree.CheckConstraintTableDef:
		alterTableAddCheck(b, tn, tbl, t)
	case *tree.ForeignKeyConstraintTableDef:
		alterTableAddForeignKey(b, tn, tbl, stmt, t)
	}
}

// alterTableAddPrimaryKey contains logics for building
// `ALTER TABLE ... ADD PRIMARY KEY`.
// It assumes `t` is such a command.
func alterTableAddPrimaryKey(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	stmt tree.Statement,
	t *tree.AlterTableAddConstraint,
) {
	if t.ValidationBehavior == tree.ValidationSkip {
		panic(sqlerrors.NewUnsupportedUnvalidatedConstraintError(catconstants.ConstraintTypePK))
	}

	d := t.ConstraintDef.(*tree.UniqueConstraintTableDef)
	// Ensure that there is a default rowid column.
	oldPrimaryIndex := mustRetrieveCurrentPrimaryIndexElement(b, tbl.TableID)
	if getPrimaryIndexDefaultRowIDColumn(
		b, tbl.TableID, oldPrimaryIndex.IndexID,
	) == nil {
		panic(scerrors.NotImplementedError(t))
	}
	alterPrimaryKey(b, tn, tbl, stmt, alterPrimaryKeySpec{
		n:             t,
		Columns:       d.Columns,
		Sharded:       d.Sharded,
		Name:          d.Name,
		StorageParams: d.StorageParams,
	})
}

// alterTableAddCheck contains logic for building
// `ALTER TABLE ... ADD CHECK ... [NOT VALID]`.
// It assumes `t` is such a command.
func alterTableAddCheck(
	b BuildCtx, tn *tree.TableName, tbl *scpb.Table, t *tree.AlterTableAddConstraint,
) {
	// 1. Validate whether the to-be-created check constraint has a name that
	// has already been used.
	if skip, err := validateConstraintNameIsNotUsed(b, tn, tbl, t); err != nil {
		panic(err)
	} else if skip {
		return
	}

	// 2. CheckDeepCopy whether this check constraint is syntactically valid.
	// See the comments of DequalifyAndValidateExprImpl for criteria.
	ckDef := t.ConstraintDef.(*tree.CheckConstraintTableDef)
	ckExpr, _, colIDs, err := schemaexpr.DequalifyAndValidateExprImpl(b, ckDef.Expr, types.Bool,
		tree.CheckConstraintExpr, b.SemaCtx(), volatility.Volatile, tn, b.ClusterSettings().Version.ActiveVersion(b),
		func() colinfo.ResultColumns {
			return getNonDropResultColumns(b, tbl.TableID)
		},
		func(columnName tree.Name) (exists bool, accessible bool, id catid.ColumnID, typ *types.T) {
			return columnLookupFn(b, tbl.TableID, columnName)
		},
	)
	if err != nil {
		panic(err)
	}
	typedCkExpr, err := parser.ParseExpr(ckExpr)
	if err != nil {
		panic(err)
	}

	// 3. Add relevant check constraint element:
	// - CheckConstraint or CheckConstraintUnvalidated
	// - ConstraintName
	constraintID := b.NextTableConstraintID(tbl.TableID)
	if t.ValidationBehavior == tree.ValidationDefault {
		ck := &scpb.CheckConstraint{
			TableID:               tbl.TableID,
			ConstraintID:          constraintID,
			ColumnIDs:             colIDs.Ordered(),
			Expression:            *b.WrapExpression(tbl.TableID, typedCkExpr),
			FromHashShardedColumn: ckDef.FromHashShardedColumn,
			IndexIDForValidation:  getIndexIDForValidationForConstraint(b, tbl.TableID),
		}
		b.Add(ck)
		b.LogEventForExistingTarget(ck)
	} else {
		ck := &scpb.CheckConstraintUnvalidated{
			TableID:      tbl.TableID,
			ConstraintID: constraintID,
			ColumnIDs:    colIDs.Ordered(),
			Expression:   *b.WrapExpression(tbl.TableID, typedCkExpr),
		}
		b.Add(ck)
		b.LogEventForExistingTarget(ck)
	}

	constraintName := string(ckDef.Name)
	if constraintName == "" {
		constraintName = generateUniqueCheckConstraintName(b, tbl.TableID, ckDef.Expr)
	}
	b.Add(&scpb.ConstraintWithoutIndexName{
		TableID:      tbl.TableID,
		ConstraintID: constraintID,
		Name:         constraintName,
	})
}

// getIndexIDForValidationForConstraint returns the index ID this check
// constraint is supposed to check against and will be used to hint the
// constraint validation query in `backfill.go`.
// Normally, it will return zero, which means we won't hint the validation
// query but instead let the optimizer pick the appropriate index to serve it.
// It will return non-zero if the constraint is added while a new column is
// being added (e.g. `ALTER TABLE t ADD COLUMN j INT DEFAULT 1 CHECK (j < 0)`),
// in which case we have to explicitly hint the validation query to check
// against the new primary index that is being added (as a result of adding a
// new column), instead of against the old primary index (which does not
// contain backfilled values for the new column and hence would mistakenly
// allow the validation query to succeed).
func getIndexIDForValidationForConstraint(b BuildCtx, tableID catid.DescID) (ret catid.IndexID) {
	b.QueryByID(tableID).ForEach(func(
		current scpb.Status, target scpb.TargetStatus, e scpb.Element,
	) {
		if pie, ok := e.(*scpb.PrimaryIndex); ok &&
			target == scpb.ToPublic && current != scpb.Status_PUBLIC {
			ret = pie.IndexID
		}
	})
	return ret
}

// alterTableAddForeignKey contains logic for building
// `ALTER TABLE ... ADD FOREIGN KEY ... [NOT VALID]`.
// It assumes `t` is such a command.
func alterTableAddForeignKey(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	stmt tree.Statement,
	t *tree.AlterTableAddConstraint,
) {
	fkDef := t.ConstraintDef.(*tree.ForeignKeyConstraintTableDef)
	// fromColsFRNames is fully resolved column names from `fkDef.FromCols`, and
	// is only used in constructing error messages to be consistent with legacy
	// schema changer.
	fromColsFRNames := getFullyResolvedColNames(b, tbl.TableID, fkDef.FromCols)

	// 1. If this FK's `ON UPDATE behavior` is not NO ACTION nor RESTRICT, and
	// any of the originColumns has an `ON UPDATE expr`, panic with error.
	if fkDef.Actions.Update != tree.NoAction && fkDef.Actions.Update != tree.Restrict {
		for _, colName := range fkDef.FromCols {
			colID := getColumnIDFromColumnName(b, tbl.TableID, colName, true /* required */)
			colHasOnUpdate := retrieveColumnOnUpdateExpressionElem(b, tbl.TableID, colID) != nil
			if colHasOnUpdate {
				panic(pgerror.Newf(
					pgcode.InvalidTableDefinition,
					"cannot specify a foreign key update action and an ON UPDATE"+
						" expression on the same column",
				))
			}
		}
	}

	// 2. If this FK has SET NULL action (ON UPDATE or ON DELETE) && any one of
	// the originColumns is NOT NULL, then panic with error.
	if fkDef.Actions.Delete == tree.SetNull || fkDef.Actions.Update == tree.SetNull {
		for i, colName := range fkDef.FromCols {
			colID := getColumnIDFromColumnName(b, tbl.TableID, colName, true /* required */)
			if isColNotNull(b, tbl.TableID, colID) {
				panic(pgerror.Newf(pgcode.InvalidForeignKey,
					"cannot add a SET NULL cascading action on column %q which has a NOT NULL constraint", fromColsFRNames[i],
				))
			}
		}
	}

	// 3. If this FK has SET DEFAULT action (ON UPDATE or ON DELETE) && any one
	// of the originColumns does not have a default expression (which implies
	// NULL will be used as the default) && that column is NOT NULL, then panic
	// with error.
	if fkDef.Actions.Delete == tree.SetDefault || fkDef.Actions.Update == tree.SetDefault {
		for i, colName := range fkDef.FromCols {
			colID := getColumnIDFromColumnName(b, tbl.TableID, colName, true /* required */)
			colHasDefault := retrieveColumnDefaultExpressionElem(b, tbl.TableID, colID) != nil
			colIsNotNull := isColNotNull(b, tbl.TableID, colID)
			if !colHasDefault && colIsNotNull {
				panic(pgerror.Newf(pgcode.InvalidForeignKey,
					"cannot add a SET DEFAULT cascading action on column %q which has a "+
						"NOT NULL constraint and a NULL default expression", fromColsFRNames[i],
				))
			}
		}
	}

	// 4. Check whether each originColumns can be used for an outbound FK
	// constraint, and no duplicates exist in originColumns.
	var originColIDs []catid.ColumnID
	var originColSet catalog.TableColSet
	for i, colName := range fkDef.FromCols {
		colID := getColumnIDFromColumnName(b, tbl.TableID, colName, true /* required */)
		ensureColCanBeUsedInOutboundFK(b, tbl.TableID, colID, fkDef.Actions)
		if originColSet.Contains(colID) {
			panic(pgerror.Newf(pgcode.InvalidForeignKey,
				"foreign key contains duplicate column %q", fromColsFRNames[i]))
		}
		originColSet.Add(colID)
		originColIDs = append(originColIDs, colID)
	}

	// 5. Resolve `t.(*ForeignKeyConstraintTableDef).Table` (i.e. referenced
	// table name) and check whether it's in the same database as the originTable
	// (i.e. tbl). Cross database FK references is a deprecated feature and is in
	// practice no longer supported. We will return an error here directly.
	referencedTableID := mustGetTableIDFromTableName(b, fkDef.Table)
	originalTableNamespaceElem := mustRetrieveNamespaceElem(b, tbl.TableID)
	referencedTableNamespaceElem := mustRetrieveNamespaceElem(b, referencedTableID)
	if originalTableNamespaceElem.DatabaseID != referencedTableNamespaceElem.DatabaseID {
		panic(scerrors.NotImplementedErrorf(t, "cross DB FK reference is a deprecated feature "+
			"and is no longer supported."))
	}
	// Disallow schema change if the FK references a table whose schema is locked.
	panicIfSchemaChangeIsDisallowed(b.QueryByID(referencedTableID), stmt)

	// 6. Check that temporary tables can only reference temporary tables, or,
	// permanent tables can only reference permanent tables.
	// In other words, we don't allow it if originTable's temporariness does not
	// match referencedTable's temporariness.
	referencedTableElem := mustRetrieveTableElem(b, referencedTableID)
	fkDef.Table.ObjectNamePrefix = b.NamePrefix(referencedTableElem)
	if tbl.IsTemporary != referencedTableElem.IsTemporary {
		persistenceType := "permanent"
		if tbl.IsTemporary {
			persistenceType = "temporary"
		}
		panic(pgerror.Newf(
			pgcode.InvalidTableDefinition,
			"constraints on %s tables may reference only %s tables",
			persistenceType,
			persistenceType,
		))
	}

	// 7. Compute referencedColumnNames, which is usually provided in
	// `t.(*ForeignKeyConstraintTableDef).ToCols`. But if it's empty, then we
	// attempt to add this FK on the PK of the referenced table, excluding
	// implicit columns.
	if len(fkDef.ToCols) == 0 {
		primaryIndexIDInReferencedTable := getCurrentPrimaryIndexID(b, referencedTableID)
		numImplicitCols := 0
		primaryIndexPartitioningElemInReferencedTable := maybeRetrieveIndexPartitioningElem(b, referencedTableID, primaryIndexIDInReferencedTable)
		if primaryIndexPartitioningElemInReferencedTable != nil {
			numImplicitCols = int(primaryIndexPartitioningElemInReferencedTable.NumImplicitColumns)
		}
		keyColIDsOfPrimaryIndexInReferencedTable, _, _ := getSortedColumnIDsInIndexByKind(b, referencedTableID, primaryIndexIDInReferencedTable)
		for i := numImplicitCols; i < len(keyColIDsOfPrimaryIndexInReferencedTable); i++ {
			fkDef.ToCols = append(
				fkDef.ToCols,
				tree.Name(mustRetrieveColumnNameElem(b, referencedTableID, keyColIDsOfPrimaryIndexInReferencedTable[i]).Name),
			)
		}
	}

	// 8. Similarly to 4, check whether each referencedColumn can be used for an
	// inbound FK constraint, and the length of referencedColumns must be equal
	// to the length of originColumns.
	var referencedColIDs []catid.ColumnID
	for _, colName := range fkDef.ToCols {
		colID := getColumnIDFromColumnName(b, referencedTableID, colName, true /*required */)
		ensureColCanBeUsedInInboundFK(b, referencedTableID, colID)
		referencedColIDs = append(referencedColIDs, colID)
	}
	if len(originColIDs) != len(referencedColIDs) {
		panic(pgerror.Newf(pgcode.Syntax,
			"%d columns must reference exactly %d columns in referenced table (found %d)",
			len(originColIDs), len(originColIDs), len(referencedColIDs)))
	}

	// 9. Check whether types of originColumns match types of referencedColumns.
	// Namely, we will panic if their column type is not "compatible" (i.e. we do
	// not insist that the column types are of exactly the same type; it's okay
	// if they are compatible but not exactly the same. E.g. Two types under the
	// same type family are usually compatible). If the column types are
	// compatible but not exactly the same, send a notice to the client about this.
	for i := range originColIDs {
		originColName := mustRetrieveColumnNameElem(b, tbl.TableID, originColIDs[i]).Name
		originColType := mustRetrieveColumnTypeElem(b, tbl.TableID, originColIDs[i]).Type
		referencedColName := mustRetrieveColumnNameElem(b, referencedTableID, referencedColIDs[i]).Name
		referencedColType := mustRetrieveColumnTypeElem(b, referencedTableID, referencedColIDs[i]).Type
		if !originColType.Equivalent(referencedColType) {
			panic(pgerror.Newf(pgcode.DatatypeMismatch,
				"type of %q (%s) does not match foreign key %q.%q (%s)", originColName, originColType.String(),
				referencedTableNamespaceElem.Name, referencedColName, referencedColType.String()))
		}
		if !originColType.Identical(referencedColType) {
			b.EvalCtx().ClientNoticeSender.BufferClientNotice(b,
				pgnotice.Newf(
					"type of foreign key column %q (%s) is not identical to referenced column %q.%q (%s)",
					originColName, originColType.SQLString(),
					referencedTableNamespaceElem.Name, referencedColName, referencedColType.SQLString()),
			)
		}
	}

	// 10. Check that the name of this to-be-added FK constraint hasn't been used;
	// Or, give it one if no name is specified.
	if skip, err := validateConstraintNameIsNotUsed(b, tn, tbl, t); err != nil {
		panic(err)
	} else if skip {
		return
	}
	if fkDef.Name == "" {
		fkDef.Name = tree.Name(tabledesc.GenerateUniqueName(
			tabledesc.ForeignKeyConstraintName(tn.Table(), fkDef.FromCols.ToStrings()),
			func(name string) bool {
				return constraintNameInUse(b, tbl.TableID, name)
			},
		))
	}

	// 11. Verify that the referencedTable guarantees uniqueness on the
	// referencedColumns. In code, this means we need to find either a
	// PRIMARY INDEX, a UNIQUE INDEX, or a UNIQUE_WITHOUT_INDEX CONSTRAINT,
	// that covers exactly referencedColumns.
	if areColsUnique := areColsUniqueInTable(b, referencedTableID, referencedColIDs); !areColsUnique {
		panic(pgerror.Newf(
			pgcode.ForeignKeyViolation,
			"there is no unique constraint matching given keys for referenced table %s",
			referencedTableNamespaceElem.Name,
		))
	}

	// 12. Adding a foreign key dependency on a table with row-level TTL enabled can
	// cause a slowdown in the TTL deletion job as the number of rows to be updated per
	// deletion can go up. In such a case, flag a notice to the user advising them to
	// update the ttl_delete_batch_size to avoid generating TTL deletion jobs with a high
	// cardinality of rows being deleted.
	// See https://github.com/cockroachdb/cockroach/issues/125103 for more details.
	if b.QueryByID(referencedTableID).FilterRowLevelTTL() != nil {
		// Use foreign key actions to determine upstream impact and flag a notice if the
		// actions for delete involve cascading deletes.
		if fkDef.Actions.Delete != tree.NoAction && fkDef.Actions.Delete != tree.Restrict {
			b.EvalCtx().ClientNoticeSender.BufferClientNotice(
				b,
				pgnotice.Newf("Table %s has row level TTL enabled. This will make TTL deletion jobs"+
					" more expensive as dependent rows will need to be updated as well."+
					" To improve performance of the TTL job, consider reducing the value of ttl_delete_batch_size.",
					referencedTableNamespaceElem.Name))
		}
	}
	// 13. (Finally!) Add a ForeignKey_Constraint, ConstraintName element to
	// builder state.
	constraintID := b.NextTableConstraintID(tbl.TableID)
	if t.ValidationBehavior == tree.ValidationDefault {
		fk := &scpb.ForeignKeyConstraint{
			TableID:                 tbl.TableID,
			ConstraintID:            constraintID,
			ColumnIDs:               originColIDs,
			ReferencedTableID:       referencedTableID,
			ReferencedColumnIDs:     referencedColIDs,
			OnUpdateAction:          tree.ForeignKeyReferenceActionValue[fkDef.Actions.Update],
			OnDeleteAction:          tree.ForeignKeyReferenceActionValue[fkDef.Actions.Delete],
			CompositeKeyMatchMethod: tree.CompositeKeyMatchMethodValue[fkDef.Match],
			IndexIDForValidation:    getIndexIDForValidationForConstraint(b, tbl.TableID),
		}
		b.Add(fk)
		b.LogEventForExistingTarget(fk)
	} else {
		fk := &scpb.ForeignKeyConstraintUnvalidated{
			TableID:                 tbl.TableID,
			ConstraintID:            constraintID,
			ColumnIDs:               originColIDs,
			ReferencedTableID:       referencedTableID,
			ReferencedColumnIDs:     referencedColIDs,
			OnUpdateAction:          tree.ForeignKeyReferenceActionValue[fkDef.Actions.Update],
			OnDeleteAction:          tree.ForeignKeyReferenceActionValue[fkDef.Actions.Delete],
			CompositeKeyMatchMethod: tree.CompositeKeyMatchMethodValue[fkDef.Match],
		}
		b.Add(fk)
		b.LogEventForExistingTarget(fk)
	}
	b.Add(&scpb.ConstraintWithoutIndexName{
		TableID:      tbl.TableID,
		ConstraintID: constraintID,
		Name:         string(fkDef.Name),
	})
}

// alterTableAddUniqueWithoutIndex contains logic for building
// `ALTER TABLE ... ADD UNIQUE WITHOUT INDEX ... [NOT VALID]`.
// It assumes `t` is such a command.
func alterTableAddUniqueWithoutIndex(
	b BuildCtx, tn *tree.TableName, tbl *scpb.Table, t *tree.AlterTableAddConstraint,
) {
	d := t.ConstraintDef.(*tree.UniqueConstraintTableDef)

	// 1. A bunch of checks.
	if !b.SessionData().EnableUniqueWithoutIndexConstraints {
		panic(pgerror.New(pgcode.FeatureNotSupported,
			"unique constraints without an index are not yet supported",
		))
	}
	if len(d.Storing) > 0 {
		panic(pgerror.New(pgcode.FeatureNotSupported,
			"unique constraints without an index cannot store columns",
		))
	}
	if d.PartitionByIndex.ContainsPartitions() {
		panic(pgerror.New(pgcode.FeatureNotSupported,
			"partitioned unique constraints without an index are not supported",
		))
	}
	if d.Invisibility.Value != 0.0 {
		// Theoretically, this should never happen because this is not supported by
		// the parser. This is just a safe check.
		panic(pgerror.Newf(pgcode.FeatureNotSupported,
			"creating a unique constraint using UNIQUE WITHOUT NOT VISIBLE INDEX is not supported",
		))
	}

	// 2. Check that columns that we want to have uniqueness should have no duplicate.
	var colSet catalog.TableColSet
	var colIDs []catid.ColumnID
	var colNames []string
	for _, col := range d.Columns {
		colID := getColumnIDFromColumnName(b, tbl.TableID, col.Column, true /*required*/)
		if colSet.Contains(colID) {
			panic(pgerror.Newf(pgcode.DuplicateColumn,
				"column %q appears twice in unique constraint", col.Column))
		}
		colSet.Add(colID)
		colIDs = append(colIDs, colID)
		colNames = append(colNames, string(col.Column))
	}

	// 3. If a name is provided, check that this name is not used; Otherwise, generate
	// a unique name for it.
	if skip, err := validateConstraintNameIsNotUsed(b, tn, tbl, t); err != nil {
		panic(err)
	} else if skip {
		return
	}
	if d.Name == "" {
		d.Name = tree.Name(tabledesc.GenerateUniqueName(
			fmt.Sprintf("unique_%s", strings.Join(colNames, "_")),
			func(name string) bool {
				return constraintNameInUse(b, tbl.TableID, name)
			},
		))
	}

	// 4. If there is a predicate, validate it.
	if d.Predicate != nil {
		predicate, _, _, err := schemaexpr.DequalifyAndValidateExprImpl(b, d.Predicate, types.Bool,
			tree.UniqueWithoutIndexPredicateExpr, b.SemaCtx(), volatility.Immutable, tn, b.ClusterSettings().Version.ActiveVersion(b),
			func() colinfo.ResultColumns {
				return getNonDropResultColumns(b, tbl.TableID)
			},
			func(columnName tree.Name) (exists bool, accessible bool, id catid.ColumnID, typ *types.T) {
				return columnLookupFn(b, tbl.TableID, columnName)
			},
		)
		if err != nil {
			panic(err)
		}
		typedPredicate, err := parser.ParseExpr(predicate)
		if err != nil {
			panic(err)
		}
		d.Predicate = typedPredicate
	}

	// 5. (Finally!) Add a UniqueWithoutIndex, ConstraintName element to builder state.
	constraintID := b.NextTableConstraintID(tbl.TableID)
	if t.ValidationBehavior == tree.ValidationDefault {
		uwi := &scpb.UniqueWithoutIndexConstraint{
			TableID:              tbl.TableID,
			ConstraintID:         constraintID,
			ColumnIDs:            colIDs,
			IndexIDForValidation: getIndexIDForValidationForConstraint(b, tbl.TableID),
		}
		if d.Predicate != nil {
			uwi.Predicate = b.WrapExpression(tbl.TableID, d.Predicate)
		}
		b.Add(uwi)
		b.LogEventForExistingTarget(uwi)
	} else {
		uwi := &scpb.UniqueWithoutIndexConstraintUnvalidated{
			TableID:      tbl.TableID,
			ConstraintID: constraintID,
			ColumnIDs:    colIDs,
		}
		if d.Predicate != nil {
			uwi.Predicate = b.WrapExpression(tbl.TableID, d.Predicate)
		}
		b.Add(uwi)
		b.LogEventForExistingTarget(uwi)
	}
	b.Add(&scpb.ConstraintWithoutIndexName{
		TableID:      tbl.TableID,
		ConstraintID: constraintID,
		Name:         string(d.Name),
	})
}

// getFullyResolvedColNames returns fully resolved column names for `colNames`.
// For each column name in `colNames`, its fully resolved name will be "db.sc.tbl.col".
// The order of column names in the return is in syc with that in the input `colNames`.
func getFullyResolvedColNames(
	b BuildCtx, tableID catid.DescID, colNames tree.NameList,
) (ret tree.NameList) {
	ns := mustRetrieveNamespaceElem(b, tableID)
	tableName := ns.Name
	schemaName := mustRetrieveNamespaceElem(b, ns.SchemaID).Name
	databaseName := mustRetrieveNamespaceElem(b, ns.DatabaseID).Name

	for _, colName := range colNames {
		fullyResolvedColName := strings.Join([]string{databaseName, schemaName, tableName, string(colName)}, ".")
		ret = append(ret, tree.Name(fullyResolvedColName))
	}
	return ret
}

// areColsUniqueInTable ensures uniqueness on columns is guaranteed on this table.
func areColsUniqueInTable(b BuildCtx, tableID catid.DescID, columnIDs []catid.ColumnID) (ret bool) {
	b.QueryByID(tableID).ForEach(func(current scpb.Status, target scpb.TargetStatus, e scpb.Element) {
		if ret {
			return
		}

		switch te := e.(type) {
		case *scpb.PrimaryIndex:
			ret = isIndexUniqueAndCanServeFK(b, &te.Index, columnIDs)
		case *scpb.SecondaryIndex:
			ret = isIndexUniqueAndCanServeFK(b, &te.Index, columnIDs)
		case *scpb.UniqueWithoutIndexConstraint:
			if te.Predicate == nil && descpb.ColumnIDs(te.ColumnIDs).PermutationOf(columnIDs) {
				ret = true
			}
		}
	})
	return ret
}

// validateConstraintNameIsNotUsed checks that the name of the constraint we're
// trying to add isn't already used, and, if it is, whether the constraint
// addition should be skipped:
// - if the name is free to use, it returns false;
// - if it's already used but IF NOT EXISTS was specified, it returns true;
// - otherwise, it returns an error.
// TODO (xiang): It is written knowing that we will only see check constraint
// for now. Complete it by also considering all other types of constraints as well.
func validateConstraintNameIsNotUsed(
	b BuildCtx, tn *tree.TableName, tbl *scpb.Table, t *tree.AlterTableAddConstraint,
) (skip bool, err error) {
	var name tree.Name
	var ifNotExists bool
	switch d := t.ConstraintDef.(type) {
	case *tree.CheckConstraintTableDef:
		name = d.Name
		ifNotExists = d.IfNotExists
	case *tree.ForeignKeyConstraintTableDef:
		name = d.Name
		ifNotExists = d.IfNotExists
	case *tree.UniqueConstraintTableDef:
		name = d.Name
		ifNotExists = d.IfNotExists
	default:
		return false, errors.AssertionFailedf(
			"unsupported constraint: %T", t.ConstraintDef)
	}

	if name == "" {
		// A name was not specified; We can give it any name later.
		return false, nil
	}

	var isInUse bool
	scpb.ForEachConstraintWithoutIndexName(b.QueryByID(tbl.TableID), func(
		_ scpb.Status, target scpb.TargetStatus, e *scpb.ConstraintWithoutIndexName,
	) {
		if target == scpb.ToPublic && e.Name == string(name) {
			isInUse = true
		}
	})

	if !isInUse {
		return false, nil
	}

	if ifNotExists {
		return true, nil
	}

	return false, pgerror.Newf(pgcode.DuplicateObject,
		"duplicate constraint name: %q", name)
}

// getNonDropResultColumns returns all public and adding columns, sorted by
// column ID in ascending order, in the format of ResultColumns.
func getNonDropResultColumns(b BuildCtx, tableID catid.DescID) (ret colinfo.ResultColumns) {
	for _, col := range getNonDropColumns(b, tableID) {
		ret = append(ret, colinfo.ResultColumn{
			Name:           mustRetrieveColumnNameElem(b, tableID, col.ColumnID).Name,
			Typ:            mustRetrieveColumnTypeElem(b, tableID, col.ColumnID).Type,
			Hidden:         col.IsHidden,
			TableID:        tableID,
			PGAttributeNum: uint32(col.PgAttributeNum),
		})
	}
	return ret
}

// columnLookupFn can look up information of a column by name.
// It should be exclusively used in DequalifyAndValidateExprImpl.
func columnLookupFn(
	b BuildCtx, tableID catid.DescID, columnName tree.Name,
) (exists bool, accessible bool, id catid.ColumnID, typ *types.T) {
	columnID := getColumnIDFromColumnName(b, tableID, columnName, false /* required */)
	if columnID == 0 {
		return false, false, 0, nil
	}

	colElem := mustRetrieveColumnElem(b, tableID, columnID)
	colTypeElem := mustRetrieveColumnTypeElem(b, tableID, columnID)
	return true, !colElem.IsInaccessible, columnID, colTypeElem.Type
}

// generateUniqueCheckConstraintName generates a unique name for check constraint.
// The default name is `check_` followed by all columns in the expression (e.g. `check_a_b_a`).
// If that name is already in use, append a number (i.e. 1, 2, ...) to it until
// it's not in use.
func generateUniqueCheckConstraintName(b BuildCtx, tableID catid.DescID, ckExpr tree.Expr) string {
	name := checkConstraintDefaultName(b, tableID, ckExpr)

	if constraintNameInUse(b, tableID, name) {
		i := 1
		for {
			numberedName := fmt.Sprintf("%s%d", name, i)
			if !constraintNameInUse(b, tableID, numberedName) {
				name = numberedName
				break
			}
			i++
		}
	}

	return name
}

// constraintNameInUse returns whether `name` has been used by any other
// non-dropping constraint.
func constraintNameInUse(b BuildCtx, tableID catid.DescID, name string) (ret bool) {
	scpb.ForEachConstraintWithoutIndexName(b.QueryByID(tableID).Filter(publicTargetFilter), func(
		_ scpb.Status, _ scpb.TargetStatus, e *scpb.ConstraintWithoutIndexName,
	) {
		if e.Name == name {
			ret = true
		}
	})
	return ret
}

// DefaultName creates a check constraint name based on the columns referenced
// in the check expression. The format is "check_col1_col2...". If columns are
// duplicated in the expression, they will be duplicated in the name.
//
// For example:
//
//	CHECK (a < 0) => check_a
//	CHECK (a < 0 AND b = 'foo') => check_a_b
//	CHECK (a < 0 AND b = 'foo' AND a < 10) => check_a_b_a
//
// Note that the generated name is not guaranteed to be unique among the other
// constraints of the table.
func checkConstraintDefaultName(b BuildCtx, tableID catid.DescID, expr tree.Expr) string {
	var nameBuf bytes.Buffer
	nameBuf.WriteString("check")

	iterateColNamesInExpr(b, tableID, expr, func(columnName string) error {
		nameBuf.WriteByte('_')
		nameBuf.WriteString(columnName)
		return nil
	})

	return nameBuf.String()
}

// Iterate over all column names shown in an expression.
func iterateColNamesInExpr(
	b BuildCtx, tableID catid.DescID, rootExpr tree.Expr, f func(columnName string) error,
) {
	_, err := tree.SimpleVisit(rootExpr, func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		vBase, ok := expr.(tree.VarName)
		if !ok {
			// Not a VarName, don't do anything to this node.
			return true, expr, nil
		}

		v, err := vBase.NormalizeVarName()
		if err != nil {
			return false, nil, err
		}

		c, ok := v.(*tree.ColumnItem)
		if !ok {
			return true, expr, nil
		}

		colElems := b.ResolveColumn(tableID, c.ColumnName, ResolveParams{
			IsExistenceOptional: false,
			RequiredPrivilege:   privilege.CREATE,
		})
		_, _, colNameElem := scpb.FindColumnName(colElems)
		if colNameElem == nil {
			panic(errors.AssertionFailedf("programming error: cannot find a Column element for column %v", c.ColumnName))
		}

		if err := f(colNameElem.Name); err != nil {
			return false, nil, err
		}
		return false, expr, err
	})

	if err != nil {
		panic(err)
	}
}

func retrieveColumnDefaultExpressionElem(
	b BuildCtx, tableID catid.DescID, columnID catid.ColumnID,
) *scpb.ColumnDefaultExpression {
	return b.QueryByID(tableID).Filter(publicTargetFilter).FilterColumnDefaultExpression().
		Filter(func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.ColumnDefaultExpression) bool {
			return e.ColumnID == columnID
		}).
		MustGetZeroOrOneElement()
}

func retrieveColumnOnUpdateExpressionElem(
	b BuildCtx, tableID catid.DescID, columnID catid.ColumnID,
) (columnOnUpdateExpression *scpb.ColumnOnUpdateExpression) {
	return b.QueryByID(tableID).Filter(publicTargetFilter).FilterColumnOnUpdateExpression().
		Filter(func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.ColumnOnUpdateExpression) bool {
			return e.ColumnID == columnID
		}).
		MustGetZeroOrOneElement()
}

// ensureColCanBeUsedInOutboundFK ensures the column can be used in an outbound
// FK reference. Panic if it cannot.
func ensureColCanBeUsedInOutboundFK(
	b BuildCtx, tableID catid.DescID, columnID catid.ColumnID, actions tree.ReferenceActions,
) {
	colNameElem := mustRetrieveColumnNameElem(b, tableID, columnID)
	colTypeElem := mustRetrieveColumnTypeElem(b, tableID, columnID)
	colElem := mustRetrieveColumnElem(b, tableID, columnID)

	if colElem.IsInaccessible {
		panic(pgerror.Newf(
			pgcode.UndefinedColumn,
			"column %q is inaccessible and cannot reference a foreign key",
			colNameElem.Name,
		))
	}

	if colTypeElem.IsVirtual {
		panic(unimplemented.NewWithIssuef(
			59671, "virtual column %q cannot reference a foreign key",
			colNameElem.Name,
		))
	}

	// When a FK has a computed column, we block any ON UPDATE or ON DELETE
	// actions that would try to change the computed value. Computed values cannot
	// be altered directly, so attempts to set them to NULL or a DEFAULT value are
	// blocked.
	computeExpr := retrieveColumnComputeExpression(b, tableID, columnID)
	if computeExpr != nil && actions.HasDisallowedActionForComputedFKCol() {
		panic(sqlerrors.NewInvalidActionOnComputedFKColumnError(actions.HasUpdateAction()))
	}
}

// ensureColCanBeUsedInInboundFK ensures the column can be used in an inbound
// FK reference. Panic if it cannot.
func ensureColCanBeUsedInInboundFK(b BuildCtx, tableID catid.DescID, columnID catid.ColumnID) {
	colNameElem := mustRetrieveColumnNameElem(b, tableID, columnID)
	colTypeElem := mustRetrieveColumnTypeElem(b, tableID, columnID)
	colElem := mustRetrieveColumnElem(b, tableID, columnID)

	if colElem.IsInaccessible {
		panic(pgerror.Newf(
			pgcode.UndefinedColumn,
			"column %q is inaccessible and cannot be referenced by a foreign key",
			colNameElem.Name,
		))
	}
	if colTypeElem.IsVirtual {
		panic(unimplemented.NewWithIssuef(
			59671, "virtual column %q cannot be referenced by a foreign key",
			colNameElem.Name,
		))
	}
}

func retrieveTableElem(b BuildCtx, tableID catid.DescID) *scpb.Table {
	return b.QueryByID(tableID).FilterTable().Filter(func(
		current scpb.Status, target scpb.TargetStatus, e *scpb.Table,
	) bool {
		return e.TableID == tableID
	}).MustGetZeroOrOneElement()
}

func mustRetrieveTableElem(b BuildCtx, tableID catid.DescID) *scpb.Table {
	tblElem := retrieveTableElem(b, tableID)
	if tblElem == nil {
		panic(errors.AssertionFailedf("programming error: cannot find a Table element for table %v", tableID))
	}
	return tblElem
}

func mustRetrieveNamespaceElem(b BuildCtx, tableID catid.DescID) *scpb.Namespace {
	_, _, ns := scpb.FindNamespace(b.QueryByID(tableID))
	if ns == nil {
		panic(errors.AssertionFailedf("programming error: cannot find a Namespace element for table %v", tableID))
	}
	return ns
}

func maybeRetrieveIndexPartitioningElem(
	b BuildCtx, tableID catid.DescID, indexID catid.IndexID,
) (ret *scpb.IndexPartitioning) {
	scpb.ForEachIndexPartitioning(b.QueryByID(tableID), func(current scpb.Status, target scpb.TargetStatus, e *scpb.IndexPartitioning) {
		if e.IndexID == indexID {
			ret = e
		}
	})
	return ret
}

func getCurrentPrimaryIndexID(b BuildCtx, tableID catid.DescID) (ret catid.IndexID) {
	b.QueryByID(tableID).ForEach(func(current scpb.Status, target scpb.TargetStatus, e scpb.Element) {
		if pie, ok := e.(*scpb.PrimaryIndex); ok && current == scpb.Status_PUBLIC {
			ret = pie.IndexID
		}
	})
	return ret
}

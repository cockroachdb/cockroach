// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachange"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/cast"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

func alterTableAlterColumnType(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	stmt tree.Statement,
	t *tree.AlterTableAlterColumnType,
) {
	colID := getColumnIDFromColumnName(b, tbl.TableID, t.Column, true /* required */)
	col := mustRetrieveColumnElem(b, tbl.TableID, colID)
	panicIfSystemColumn(col, t.Column.String())

	// Setup for the new type ahead of any checking. As we need its resolved type
	// for the checks.
	oldColType := retrieveColumnTypeElem(b, tbl.TableID, colID)
	newColType := *oldColType
	newColType.TypeT = b.ResolveTypeRef(t.ToType)

	// Check for elements depending on the column we are altering.
	walkColumnDependencies(b, col, "alter type of", "column", func(e scpb.Element, op, objType string) {
		switch e := e.(type) {
		case *scpb.Column:
			if e.TableID == col.TableID && e.ColumnID == col.ColumnID {
				return
			}
			elts := b.QueryByID(e.TableID).Filter(hasColumnIDAttrFilter(e.ColumnID))
			computedColName := elts.FilterColumnName().MustGetOneElement()
			panic(sqlerrors.NewDependentBlocksOpError(op, objType, t.Column.String(), "computed column", computedColName.Name))
		case *scpb.View:
			ns := b.QueryByID(col.TableID).FilterNamespace().MustGetOneElement()
			nsDep := b.QueryByID(e.ViewID).FilterNamespace().MustGetOneElement()
			if nsDep.DatabaseID != ns.DatabaseID || nsDep.SchemaID != ns.SchemaID {
				panic(sqlerrors.NewDependentBlocksOpError(op, objType, t.Column.String(), "view", qualifiedName(b, e.ViewID)))
			}
			panic(sqlerrors.NewDependentBlocksOpError(op, objType, t.Column.String(), "view", nsDep.Name))
		case *scpb.FunctionBody:
			fnName := b.QueryByID(e.FunctionID).FilterFunctionName().MustGetOneElement()
			panic(sqlerrors.NewDependentBlocksOpError(op, objType, t.Column.String(), "function", fnName.Name))
		case *scpb.RowLevelTTL:
			// If a duration expression is set, the column level dependency is on the
			// internal ttl column, which we are attempting to alter.
			if e.DurationExpr != "" {
				panic(sqlerrors.NewAlterDependsOnDurationExprError(op, objType, t.Column.String(), tn.Object()))
			}
			// Otherwise, it is a dependency on the column used in the expiration
			// expression.
			panic(sqlerrors.NewAlterDependsOnExpirationExprError(op, objType, t.Column.String(), tn.Object(), string(e.ExpirationExpr)))
		}
	})

	var err error
	newColType.Type, err = schemachange.ValidateAlterColumnTypeChecks(
		b, t, b.ClusterSettings(), newColType.Type,
		col.GeneratedAsIdentityType != catpb.GeneratedAsIdentityType_NOT_IDENTITY_COLUMN)
	if err != nil {
		panic(err)
	}

	validateAutomaticCastForNewType(b, tbl.TableID, colID, t.Column.String(),
		oldColType.Type, newColType.Type, t.Using != nil)

	kind, err := schemachange.ClassifyConversionFromTree(b, t, oldColType.Type, newColType.Type)
	if err != nil {
		panic(err)
	}

	switch kind {
	case schemachange.ColumnConversionTrivial:
		handleTrivialColumnConversion(b, col, oldColType, &newColType)
	case schemachange.ColumnConversionValidate:
		handleValidationOnlyColumnConversion(b, t, col, oldColType, &newColType)
	case schemachange.ColumnConversionGeneral:
		handleGeneralColumnConversion(b, stmt, t, tn, tbl, col, oldColType, &newColType)
	default:
		panic(scerrors.NotImplementedErrorf(t,
			"alter type conversion %v not handled", kind))
	}
}

// ValidateColExprForNewType will ensure that the existing expressions for
// DEFAULT and ON UPDATE will work for the new data type.
func validateAutomaticCastForNewType(
	b BuildCtx,
	tableID catid.DescID,
	colID catid.ColumnID,
	colName string,
	fromType, toType *types.T,
	hasUsingExpr bool,
) {
	if validCast := cast.ValidCast(fromType, toType, cast.ContextAssignment); validCast {
		return
	}

	// If the USING expression is missing, we will report an error with a
	// suggested hint to use one.
	if !hasUsingExpr {
		// Compute a suggested default computed expression for inclusion in the error hint.
		hintExpr, err := parser.ParseExpr(fmt.Sprintf("%s::%s", colName, toType.SQLString()))
		if err != nil {
			panic(err)
		}
		panic(errors.WithHintf(
			pgerror.Newf(
				pgcode.DatatypeMismatch,
				"column %q cannot be cast automatically to type %s",
				colName,
				toType.SQLString(),
			), "You might need to specify \"USING %s\".", tree.Serialize(hintExpr),
		))
	}

	// We have a USING clause, but if we have DEFAULT or ON UPDATE expressions,
	// then we raise an error because those expressions cannot be automatically
	// cast to the new type.
	columnElements(b, tableID, colID).ForEach(func(
		_ scpb.Status, _ scpb.TargetStatus, e scpb.Element,
	) {
		var exprType string
		switch e.(type) {
		case *scpb.ColumnDefaultExpression:
			exprType = "default"
		case *scpb.ColumnOnUpdateExpression:
			exprType = "on update"
		default:
			return
		}
		panic(pgerror.Newf(
			pgcode.DatatypeMismatch,
			"%s for column %q cannot be cast automatically to type %s",
			exprType,
			colName,
			toType.SQLString(),
		))
	})
}

// handleTrivialColumnConversion is called to just change the type in-place without
// no rewrite or validation required.
func handleTrivialColumnConversion(
	b BuildCtx, col *scpb.Column, oldColType, newColType *scpb.ColumnType,
) {
	maybeWriteNoticeForFKColTypeMismatch(b, col, newColType)
	updateColumnType(b, oldColType, newColType)
}

// handleValidationOnlyColumnConversion is called when we don't need to rewrite
// data, only validate the existing data is compatible with the type.
func handleValidationOnlyColumnConversion(
	b BuildCtx,
	t *tree.AlterTableAlterColumnType,
	col *scpb.Column,
	oldColType, newColType *scpb.ColumnType,
) {
	maybeWriteNoticeForFKColTypeMismatch(b, col, newColType)
	updateColumnType(b, oldColType, newColType)

	// To validate, we add a transient check constraint. It casts the column to the
	// new type and then back to the old type. If the cast back doesn't match the
	// original value, the check fails. This constraint is temporary and doesn't
	// need to persist beyond the ALTER operation.
	expr, err := parser.ParseExpr(fmt.Sprintf("(CAST(CAST(%s AS %s) AS %s) = %s)",
		t.Column.String(), newColType.Type.SQLString(), oldColType.Type.SQLString(), t.Column.String()))
	if err != nil {
		panic(err)
	}

	// The constraint requires a backing index to use, which we will use the
	// primary index.
	indexID := getLatestPrimaryIndex(b, newColType.TableID).IndexID

	chk := scpb.CheckConstraint{
		TableID:              newColType.TableID,
		Expression:           *b.WrapExpression(newColType.TableID, expr),
		ConstraintID:         b.NextTableConstraintID(newColType.TableID),
		IndexIDForValidation: indexID,
		ColumnIDs:            []catid.ColumnID{newColType.ColumnID},
	}
	b.AddTransient(&chk) // Adding it as transient ensures it doesn't survive past the ALTER.
}

// handleGeneralColumnConversion is called when we need to rewrite the data in order
// to complete the data type conversion.
func handleGeneralColumnConversion(
	b BuildCtx,
	stmt tree.Statement,
	t *tree.AlterTableAlterColumnType,
	tn *tree.TableName,
	tbl *scpb.Table,
	col *scpb.Column,
	oldColType, newColType *scpb.ColumnType,
) {
	failIfExperimentalSettingNotSet(b, oldColType, newColType)

	// To handle the conversion, we remove the old column and add a new one with
	// the correct type. The new column will temporarily have a computed expression
	// referring to the old column, used only for the backfill process.
	//
	// Because we need to rewrite data to change the data type, there are
	// additional validation checks required that are incompatible with this
	// process.
	walkColumnDependencies(b, col, "alter type of", "column", func(e scpb.Element, op, objType string) {
		switch e.(type) {
		case *scpb.SequenceOwner:
			panic(sqlerrors.NewAlterColumnTypeColOwnsSequenceNotSupportedErr())
		case *scpb.CheckConstraint, *scpb.CheckConstraintUnvalidated,
			*scpb.UniqueWithoutIndexConstraint, *scpb.UniqueWithoutIndexConstraintUnvalidated,
			*scpb.ForeignKeyConstraint, *scpb.ForeignKeyConstraintUnvalidated:
			panic(sqlerrors.NewAlterColumnTypeColWithConstraintNotSupportedErr())
		case *scpb.SecondaryIndex:
			panic(sqlerrors.NewAlterColumnTypeColInIndexNotSupportedErr())
		case *scpb.ColumnComputeExpression:
			// TODO(#125844): we currently lose the original computed expression.
			panic(scerrors.NotImplementedErrorf(t,
				"backfilling during ALTER COLUMN TYPE for a column "+
					"with a computed expression is not supported"))
		case *scpb.ColumnOnUpdateExpression, *scpb.ColumnDefaultExpression:
			// TODO(#132909): The use of a temporary compute expression currently
			// blocks altering types with DEFAULT or ON UPDATE expressions. We should
			// be able to add these after the backfill completes and the old column is
			// dropped by using dependency rules.
			panic(scerrors.NotImplementedErrorf(t,
				"backfilling during ALTER COLUMN TYPE for a column "+
					"with a DEFAULT or ON UPDATE expression is not supported"))
		}
	})

	// We block any attempt to alter the type of a column that is a key column in
	// the primary key. We can't use walkColumnDependencies here, as it doesn't
	// differentiate between key columns and stored columns.
	pk := mustRetrievePrimaryIndex(b, tbl.TableID)
	for _, keyCol := range getIndexColumns(b.QueryByID(tbl.TableID), pk.IndexID, scpb.IndexColumn_KEY) {
		if keyCol.ColumnID == col.ColumnID {
			panic(sqlerrors.NewAlterColumnTypeColInIndexNotSupportedErr())
		}
	}

	// TODO(#47137): Only support alter statements that only have a single command.
	switch s := stmt.(type) {
	case *tree.AlterTable:
		if len(s.Cmds) > 1 {
			panic(sqlerrors.NewAlterColTypeInCombinationNotSupportedError())
		}
	}

	// In version 25.1, we introduced the necessary dependency rules to ensure the
	// general path works. Without these rules, we encounter failures during the
	// ALTER operation. To avoid this, we revert to legacy handling if not running
	// on version 25.1.
	// TODO(25.1): Update V24_3 here once V25_1 is defined.
	if !b.EvalCtx().Settings.Version.ActiveVersion(b).IsActive(clusterversion.V24_3) {
		panic(scerrors.NotImplementedErrorf(t,
			"old active version; ALTER COLUMN TYPE requires backfill. Reverting to legacy handling"))
	}

	// Generate the ID of the new column we are adding.
	newColID := b.NextTableColumnID(tbl)
	newColType.ColumnID = newColID

	// Create a computed expression for the new column that references the old column.
	//
	// During the backfill process to populate the new column, the old column is still
	// referenced by its original name, so we use that in the expression.
	colName := mustRetrieveColumnName(b, tbl.TableID, col.ColumnID)
	expr, err := getComputeExpressionForBackfill(b, t, tn, tbl.TableID, colName.Name, newColType)
	if err != nil {
		panic(err)
	}

	// First set the target status of the old column to drop. We will replace this
	// column with a new column. This column stays visible until the second backfill.
	b.Drop(col)
	b.Drop(colName)
	b.Drop(oldColType)
	handleDropColumnPrimaryIndexes(b, tbl, col)

	// Add the spec for the new column. It will be identical to the column it is replacing,
	// except the type will differ, and it will have a transient computed expression.
	// This expression will reference the original column to facilitate the backfill.
	// This column becomes visible after the first backfill.
	spec := addColumnSpec{
		tbl: tbl,
		col: &scpb.Column{
			TableID:        tbl.TableID,
			ColumnID:       newColID,
			IsHidden:       col.IsHidden,
			IsInaccessible: col.IsInaccessible,
			IsSystemColumn: col.IsSystemColumn,
			PgAttributeNum: getPgAttributeNum(col),
		},
		name: &scpb.ColumnName{
			TableID:  tbl.TableID,
			ColumnID: newColID,
			Name:     colName.Name,
		},
		colType: newColType,
		compute: &scpb.ColumnComputeExpression{
			TableID:    tbl.TableID,
			ColumnID:   newColID,
			Expression: *b.WrapExpression(tbl.TableID, expr),
		},
		transientCompute: true,
		notNull:          retrieveColumnNotNull(b, tbl.TableID, col.ColumnID) != nil,
		// TODO(#133040): The new column will be placed in the same column family as the
		// one it's replacing, so there's no need to specify a family. However, the new
		// column will be added to the end of the family's column ID list, which changes
		// its internal ordering. This needs to be revisited as CDC may have a dependency
		// on the same ordering (see TestEventColumnOrderingWithSchemaChanges).
		fam: nil,
	}
	addColumn(b, spec, t)

	// The above operation will cause a backfill to occur twice. Once with both columns,
	// then another time with the old column removed. Since both columns will exist at
	// the same time for a short period of time, we need to rename the old column so that
	// we can access either one. We add this name as a transient so that it is cleaned up
	// prior to the old column being totally removed.
	nameExists := func(name string) bool {
		return getColumnIDFromColumnName(b, tbl.TableID, tree.Name(name), false /* required */) != 0
	}
	oldColumnRename := tabledesc.GenerateUniqueName(fmt.Sprintf("%s_shadow", colName.Name), nameExists)
	b.AddTransient(&scpb.ColumnName{
		TableID:  tbl.TableID,
		ColumnID: col.ColumnID,
		Name:     oldColumnRename,
		// If we don't complete the operation, the column won't be dropped, so we
		// need to remember the original name to preserve it.
		AbsentName: colName.Name,
	})
}

func updateColumnType(b BuildCtx, oldColType, newColType *scpb.ColumnType) {
	// Add the new type and remove the old type. The removal of the old type is a
	// no-op in opgen. But we need the drop here so that we only have 1 public
	// type for the column.
	b.Drop(oldColType)
	b.Add(newColType)
}

// failIfExperimentalSettingNotSet checks if the setting that allows altering
// types is enabled. If the setting is not enabled, this function will panic.
func failIfExperimentalSettingNotSet(b BuildCtx, oldColType, newColType *scpb.ColumnType) {
	if !b.SessionData().AlterColumnTypeGeneralEnabled {
		panic(pgerror.WithCandidateCode(
			errors.WithHint(
				errors.WithIssueLink(
					errors.Newf("ALTER COLUMN TYPE from %v to %v is only "+
						"supported experimentally",
						oldColType.Type, newColType.Type),
					errors.IssueLink{IssueURL: build.MakeIssueURL(49329)}),
				"you can enable alter column type general support by running "+
					"`SET enable_experimental_alter_column_type_general = true`"),
			pgcode.ExperimentalFeature))
	}
}

// maybeWriteNoticeForFKColTypeMismatch will find any FK cols, and if the column
// that we are changing doesn't match the column in the referenced table, we
// will write a notice. This is a similar notice that is written when a table
// has a FK constraint added to it.
func maybeWriteNoticeForFKColTypeMismatch(b BuildCtx, col *scpb.Column, colType *scpb.ColumnType) {
	writeNoticeHelper := func(columnIDs, referencedColumnIDs []catid.ColumnID, referencedTableID catid.DescID) {
		// Find the corresponding column type in the referenced table
		for i := range columnIDs {
			// We only need to check a single column, then one we are altering.
			if columnIDs[i] != col.ColumnID {
				continue
			}
			refColType := mustRetrieveColumnTypeElem(b, referencedTableID, referencedColumnIDs[i])
			if !colType.Type.Identical(refColType.Type) {
				colName := mustRetrieveColumnNameElem(b, col.TableID, col.ColumnID)
				refColName := mustRetrieveColumnNameElem(b, referencedTableID, referencedColumnIDs[i])
				referencedTableNamespaceElem := mustRetrieveNamespaceElem(b, referencedTableID)
				notice := pgnotice.Newf(
					"type of foreign key column %q (%s) is not identical to referenced column %q.%q (%s)",
					colName.Name, colType.Type.SQLString(), referencedTableNamespaceElem.Name,
					refColName.Name, refColType.Type.SQLString())
				b.EvalCtx().ClientNoticeSender.BufferClientNotice(b, notice)
			}
		}
	}

	walkColumnDependencies(b, col, "alter type of", "column", func(e scpb.Element, op, objType string) {
		switch e := e.(type) {
		case *scpb.ForeignKeyConstraint:
			writeNoticeHelper(e.ColumnIDs, e.ReferencedColumnIDs, e.ReferencedTableID)
		case *scpb.ForeignKeyConstraintUnvalidated:
			writeNoticeHelper(e.ColumnIDs, e.ReferencedColumnIDs, e.ReferencedTableID)
		}
	})
}

func getComputeExpressionForBackfill(
	b BuildCtx,
	t *tree.AlterTableAlterColumnType,
	tn *tree.TableName,
	tableID catid.DescID,
	colName string,
	newColType *scpb.ColumnType,
) (expr tree.Expr, err error) {
	// If a USING clause wasn't specified, the default expression is casting the column to the new type.
	if t.Using == nil {
		return parser.ParseExpr(fmt.Sprintf("%s::%s", colName, newColType.Type.SQLString()))
	}

	expr, err = parser.ParseExpr(t.Using.String())
	if err != nil {
		return
	}

	typedExpr, _, _, err := schemaexpr.DequalifyAndValidateExprImpl(b, expr, newColType.Type,
		tree.AlterColumnTypeUsingExpr, b.SemaCtx(), volatility.Volatile, tn, b.ClusterSettings().Version.ActiveVersion(b),
		func() colinfo.ResultColumns {
			return getNonDropResultColumns(b, tableID)
		},
		func(columnName tree.Name) (exists bool, accessible bool, id catid.ColumnID, typ *types.T) {
			return columnLookupFn(b, tableID, columnName)
		},
	)
	if err != nil {
		return
	}

	// Return the updated expression from DequalifyAndValidateExprImpl. For expressions
	// involving user-defined types like enums, the types will be resolved to ensure
	// they can be used during backfill.
	expr, err = parser.ParseExpr(typedExpr)
	return
}

// getPgAttributeNum returns the column's ordering value as stored in the catalog.
// This ensures the column keeps its position for 'SELECT *' queries when replacing
// an old column with a new one.
func getPgAttributeNum(col *scpb.Column) catid.PGAttributeNum {
	if col.PgAttributeNum != 0 {
		return col.PgAttributeNum
	}
	return catid.PGAttributeNum(col.ColumnID)
}

// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachange"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/cast"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

func alterTableAlterColumnType(
	b BuildCtx, tn *tree.TableName, tbl *scpb.Table, t *tree.AlterTableAlterColumnType,
) {
	alterColumnPreChecks(b, tn, tbl, t.Column)
	colID := getColumnIDFromColumnName(b, tbl.TableID, t.Column, true /* required */)
	col := mustRetrieveColumnElem(b, tbl.TableID, colID)
	panicIfSystemColumn(col, t.Column.String())

	// Setup for the new type ahead of any checking. As we need its resolved type
	// for the checks.
	oldColType := retrieveColumnTypeElem(b, tbl.TableID, colID)
	newColType := *oldColType
	newColType.TypeT = b.ResolveTypeRef(t.ToType)

	// TODO(spilchen): We still need to add a TTL check like we do for the legacy
	// schema changer (see ValidateTTLExpressionDoesNotDependOnColumn). It is fine
	// to leave this out for now because we only support trivial data type changes.
	// The TTL expiration must be a TIMESTAMPTZ, so the only trivial change we support
	// is a no-op (TIMESTAMPTZ -> TIMESTAMPTZ). This will be fixed in issue #126143.

	// Check for elements depending on the column we are altering.
	op := "alter type of"
	objType := "column"
	walkColumnDependencies(b, col, op, objType, func(e scpb.Element) {
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

	// We currently only support trivial conversions. Fallback to legacy schema
	// changer if not trivial.
	kind, err := schemachange.ClassifyConversionFromTree(b, t, oldColType.Type, newColType.Type)
	if err != nil {
		panic(err)
	}
	if kind != schemachange.ColumnConversionTrivial {
		// TODO(spilchen): implement support for non-trivial type changes in issue #127014
		panic(scerrors.NotImplementedErrorf(t,
			"alter type conversion not supported in the declarative schema changer"))
	}

	// Do not emit anything if the column type is not changing. The operation is a
	// no-op.
	if newColType.Equal(oldColType) {
		return
	}

	// Okay to proceed with DSC. Add the new type and remove the old type. The
	// removal of the old type is a no-op in opgen. But we need the drop here so
	// that we only have 1 public type for the column.
	b.Drop(oldColType)
	b.Add(&newColType)
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
	columnElements(b, tableID, colID).ForEach(func(
		_ scpb.Status, _ scpb.TargetStatus, e scpb.Element,
	) {
		var exprType string
		switch e.(type) {
		case *scpb.ColumnDefaultExpression:
			exprType = "default"
		case *scpb.ColumnOnUpdateExpression:
			exprType = "on update"
		}
		if exprType != "" {
			if validCast := cast.ValidCast(fromType, toType, cast.ContextAssignment); !validCast {
				// If the USING expression is missing, we will report an error with a
				// suggested hint to use one. This is mainly done for compatability
				// with the legacy schema changer.
				if !hasUsingExpr {
					// Compute a suggested default computed expression for inclusion in the error hint.
					hintExpr := tree.CastExpr{
						Expr:       &tree.ColumnItem{ColumnName: tree.Name(colName)},
						Type:       toType,
						SyntaxMode: tree.CastShort,
					}
					panic(errors.WithHintf(
						pgerror.Newf(
							pgcode.DatatypeMismatch,
							"column %q cannot be cast automatically to type %s",
							colName,
							toType.SQLString(),
						), "You might need to specify \"USING %s\".", tree.Serialize(&hintExpr),
					))
				}
				panic(pgerror.Newf(
					pgcode.DatatypeMismatch,
					"%s for column %q cannot be cast automatically to type %s",
					exprType,
					colName,
					toType.SQLString(),
				))
			}
		}
	})
}

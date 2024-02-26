// Copyright 2023 The Cockroach Authors.
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
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/seqexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
)

func alterTableSetDefault(
	b BuildCtx, tn *tree.TableName, tbl *scpb.Table, t *tree.AlterTableSetDefault,
) {
	alterColumnPreChecks(b, tn, tbl, t.Column)
	// DROP DEFAULT not yet implemented in the DSC. Fall back to legacy schema changer.
	if t.Default == nil {
		panic(scerrors.NotImplementedErrorf(t, "Dropping the default on a column is not implemented."))
	}

	colID := getColumnIDFromColumnName(b, tbl.TableID, t.Column, true /* required */)
	col := mustRetrieveColumnElem(b, tbl.TableID, colID)
	// Block alters on system columns.
	panicIfSystemColumn(col, t.Column.String())

	// If our target column already has a default expression, we want to drop it first.
	currDefaultExpr := retrieveColumnDefaultExpressionElem(b, tbl.TableID, colID)
	if currDefaultExpr != nil {
		b.Drop(currDefaultExpr)
	}
	// If our desired default expression is NULL, do nothing.
	if t.Default == tree.DNull {
		return
	}
	typedExpr := panicIfInvalidNonComputedColumnExpr(b, tbl, tn.ToUnresolvedObjectName(), col, t.Column.String(), t.Default, tree.ColumnDefaultExprInSetDefault)

	b.Add(&scpb.ColumnDefaultExpression{
		TableID:    tbl.TableID,
		ColumnID:   colID,
		Expression: *b.WrapExpression(tbl.TableID, typedExpr),
	})
}

// panicIfInvalidNonComputedColumnExpr performs a series of checks on a DEFAULT or ON UPDATE column expression
// to ensure that the expression can be used.
func panicIfInvalidNonComputedColumnExpr(
	b BuildCtx,
	tbl *scpb.Table,
	tblName *tree.UnresolvedObjectName,
	col *scpb.Column,
	colName string,
	newExpr tree.Expr,
	schemaChange tree.SchemaExprContext,
) tree.TypedExpr {
	if col.GeneratedAsIdentityType != catpb.GeneratedAsIdentityType_NOT_IDENTITY_COLUMN {
		panic(sqlerrors.NewSyntaxErrorf("column %q is an identity column", colName))
	}

	colType := mustRetrieveColumnTypeElem(b, tbl.TableID, col.ColumnID)
	typedNewExpr, _, err := sanitizeColumnExpression(context.Background(), b.SemaCtx(), newExpr, colType, schemaChange)
	if err != nil {
		panic(err)
	}

	err = checkSequenceCrossRef(b, tblName, typedNewExpr)
	if err != nil {
		panic(err)
	}

	return typedNewExpr
}

// checkSequenceCrossRef ensures that the new sequence dependency being added
// in an ON UPDATE or DEFAULT expr does not improperly reference another DB.
func checkSequenceCrossRef(
	b BuildCtx, tblName *tree.UnresolvedObjectName, expr tree.TypedExpr,
) error {
	seqIds, err := seqexpr.GetUsedSequences(expr)
	if err != nil {
		return err
	}

	for _, seqId := range seqIds {
		tableElts := b.ResolveTable(tblName, ResolveParams{})
		_, _, tblNamespace := scpb.FindNamespace(tableElts)

		seqName, err := parser.ParseTableName(seqId.SeqName)
		if err != nil {
			return err
		}
		seqElts := b.ResolveSequence(seqName, ResolveParams{})
		_, _, seqNamespace := scpb.FindNamespace(seqElts)

		// Check if this reference is cross DB.
		if tblNamespace.DatabaseID != seqNamespace.DatabaseID {
			// TODO(annie): remove this cluster setting check after cross-database references are deprecated
			if err := b.CanCreateCrossDBSequenceRef(); err != nil {
				return err
			}
		}
	}
	return nil
}

func sanitizeColumnExpression(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	expr tree.Expr,
	col *scpb.ColumnType,
	context tree.SchemaExprContext,
) (tree.TypedExpr, string, error) {
	typedExpr, err := schemaexpr.SanitizeVarFreeExpr(ctx, expr, col.Type, context, semaCtx, volatility.Volatile, false /*allowAssignmentCast*/)
	if err != nil {
		return nil, "", pgerror.WithCandidateCode(err, pgcode.DatatypeMismatch)
	}

	typedExpr, err = schemaexpr.MaybeReplaceUDFNameWithOIDReferenceInTypedExpr(typedExpr)
	if err != nil {
		return nil, "", err
	}

	s := tree.Serialize(typedExpr)
	return typedExpr, s, nil
}

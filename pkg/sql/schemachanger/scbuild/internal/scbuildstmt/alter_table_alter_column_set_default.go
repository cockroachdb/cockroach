// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/seqexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
)

func alterTableSetDefault(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	stmt tree.Statement,
	t *tree.AlterTableSetDefault,
) {
	alterColumnPreChecks(b, tn, tbl, t.Column)
	colID := getColumnIDFromColumnName(b, tbl.TableID, t.Column, true /* required */)
	col := mustRetrieveColumnElem(b, tbl.TableID, colID)
	oldDefaultExpr := retrieveColumnDefaultExpressionElem(b, tbl.TableID, colID)
	colType := mustRetrieveColumnTypeElem(b, tbl.TableID, colID)

	// Block alters on system columns.
	panicIfSystemColumn(col, t.Column.String())

	// Block disallowed operations on computed columns.
	panicIfComputedColumn(b, tn.ObjectName, colType, t.Column.String(), t.Default)

	// For DROP DEFAULT.
	if t.Default == nil {
		if oldDefaultExpr != nil {
			b.Drop(oldDefaultExpr)
		}
		return
	}

	// If our target column already has a default expression, we want to drop it first.
	if oldDefaultExpr != nil {
		b.Drop(oldDefaultExpr)
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

// panicIfComputedColumn blocks disallowed operations on computed columns.
func panicIfComputedColumn(
	b BuildCtx, tn tree.Name, col *scpb.ColumnType, colName string, def tree.Expr,
) {
	computeExpr := retrieveColumnComputeExpression(b, col.TableID, col.ColumnID)
	// Block setting a column default if the column is computed.
	if computeExpr != nil {
		// Block dropping a computed col "default" as well.
		if def == nil {
			panic(pgerror.Newf(
				pgcode.Syntax,
				"column %q of relation %q is a computed column",
				colName,
				tn))
		}
		panic(pgerror.Newf(
			pgcode.Syntax,
			"computed column %q cannot also have a DEFAULT expression",
			colName))
	}
}

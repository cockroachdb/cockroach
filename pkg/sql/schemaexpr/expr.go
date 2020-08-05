// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemaexpr

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// DeserializeTableDescExpr takes in a serialized expression and a table, and
// returns an expression that has all user defined types resolved for
// formatting. It is intended to be used when displaying a serialized
// expression within a TableDescriptor. For example, a DEFAULT expression
// of a table containing a user defined type t with id 50 would have all
// references to t replaced with the id 50. In order to display this expression
// back to an end user, the serialized id 50 needs to be replaced back with t.
// DeserializeTableDescExpr performs this logic, but only returns a
// tree.Expr to be clear that these returned expressions are not safe to Eval.
func DeserializeTableDescExpr(
	ctx context.Context, semaCtx *tree.SemaContext, desc sqlbase.TableDescriptor, exprStr string,
) (tree.Expr, error) {
	expr, err := parser.ParseExpr(exprStr)
	if err != nil {
		return nil, err
	}
	expr, _, err = replaceColumnVars(desc, expr)
	if err != nil {
		return nil, err
	}
	typed, err := expr.TypeCheck(ctx, semaCtx, types.Any)
	if err != nil {
		return nil, err
	}
	return typed, nil
}

// DequalifyAndValidateExpr validates that an expression has the given type
// and contains no functions with a volatility greater than maxVolatility. The
// type-checked and constant-folded expression and the set of column IDs within
// the expression are returned, if valid.
// TODO(mgartner): Ideally this should return a serialized expression string on
// success. Returning a tree.TypedExpr is dangerous because variables have been
// replaced with dummyColumns which are not useful outside the context of
// type-checking the expression.
func DequalifyAndValidateExpr(
	ctx context.Context,
	desc sqlbase.TableDescriptor,
	expr tree.Expr,
	typ *types.T,
	op string,
	semaCtx *tree.SemaContext,
	maxVolatility tree.Volatility,
	tn *tree.TableName,
) (tree.TypedExpr, sqlbase.TableColSet, error) {
	var colIDs sqlbase.TableColSet
	sourceInfo := sqlbase.NewSourceInfoForSingleTable(
		*tn, sqlbase.ResultColumnsFromColDescs(
			desc.GetID(),
			desc.AllNonDropColumns(),
		),
	)
	expr, err := DequalifyColumnRefs(ctx, sourceInfo, expr)
	if err != nil {
		return nil, colIDs, err
	}

	// Replace the column variables with dummyColumns so that they can be
	// type-checked.
	replacedExpr, colIDs, err := replaceColumnVars(desc, expr)
	if err != nil {
		return nil, colIDs, err
	}

	typedExpr, err := sqlbase.SanitizeVarFreeExpr(
		ctx,
		replacedExpr,
		typ,
		op,
		semaCtx,
		maxVolatility,
	)

	if err != nil {
		return nil, colIDs, err
	}

	return typedExpr, colIDs, nil
}

// ExtractColumnIDs returns the set of column IDs within the given expression.
func ExtractColumnIDs(
	desc sqlbase.TableDescriptor, rootExpr tree.Expr,
) (sqlbase.TableColSet, error) {
	var colIDs sqlbase.TableColSet

	_, err := tree.SimpleVisit(rootExpr, func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		vBase, ok := expr.(tree.VarName)
		if !ok {
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

		col, _, err := desc.FindColumnByName(c.ColumnName)
		if err != nil {
			return false, nil, err
		}

		colIDs.Add(col.ID)
		return false, expr, nil
	})

	return colIDs, err
}

// nameResolver is used to replace unresolved names in expressions with
// IndexedVars.
type nameResolver struct {
	evalCtx    *tree.EvalContext
	tableID    descpb.ID
	source     *sqlbase.DataSourceInfo
	nrc        *nameResolverIVarContainer
	ivarHelper *tree.IndexedVarHelper
}

// newNameResolver creates and returns a nameResolver.
func newNameResolver(
	evalCtx *tree.EvalContext, tableID descpb.ID, tn *tree.TableName, cols []*descpb.ColumnDescriptor,
) *nameResolver {
	source := sqlbase.NewSourceInfoForSingleTable(
		*tn,
		sqlbase.ResultColumnsFromColDescPtrs(tableID, cols),
	)
	nrc := &nameResolverIVarContainer{cols}
	ivarHelper := tree.MakeIndexedVarHelper(nrc, len(cols))

	return &nameResolver{
		evalCtx:    evalCtx,
		tableID:    tableID,
		source:     source,
		nrc:        nrc,
		ivarHelper: &ivarHelper,
	}
}

// resolveNames returns an expression equivalent to the input expression with
// unresolved names replaced with IndexedVars.
func (nr *nameResolver) resolveNames(expr tree.Expr) (tree.Expr, error) {
	return sqlbase.ResolveNames(expr, nr.source, *nr.ivarHelper, nr.evalCtx.SessionData.SearchPath)
}

// addColumn adds a new column to the nameResolver so that it can be resolved in
// future calls to resolveNames.
func (nr *nameResolver) addColumn(col *descpb.ColumnDescriptor) {
	nr.ivarHelper.AppendSlot()
	nr.nrc.cols = append(nr.nrc.cols, col)
	newCols := sqlbase.ResultColumnsFromColDescs(nr.tableID, []descpb.ColumnDescriptor{*col})
	nr.source.SourceColumns = append(nr.source.SourceColumns, newCols...)
}

// addIVarContainerToSemaCtx assigns semaCtx's IVarContainer as the
// nameResolver's internal indexed var container.
func (nr *nameResolver) addIVarContainerToSemaCtx(semaCtx *tree.SemaContext) {
	semaCtx.IVarContainer = nr.nrc
}

// nameResolverIVarContainer is a help type that implements
// tree.IndexedVarContainer. It is used to resolve and type check columns in
// expressions. It does not support evaluation.
type nameResolverIVarContainer struct {
	cols []*descpb.ColumnDescriptor
}

// IndexedVarEval implements the tree.IndexedVarContainer interface.
// Evaluation is not support, so this function panics.
func (nrc *nameResolverIVarContainer) IndexedVarEval(
	idx int, ctx *tree.EvalContext,
) (tree.Datum, error) {
	panic("unsupported")
}

// IndexedVarResolvedType implements the tree.IndexedVarContainer interface.
func (nrc *nameResolverIVarContainer) IndexedVarResolvedType(idx int) *types.T {
	return nrc.cols[idx].Type
}

// IndexVarNodeFormatter implements the tree.IndexedVarContainer interface.
func (nrc *nameResolverIVarContainer) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	return nil
}

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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// DequalifyAndTypeCheckExpr type checks the given expression and returns the
// type-checked expression disregarding volatility. The typed expression, which contains dummyColumns,
// does not support evaluation.
func DequalifyAndTypeCheckExpr(
	ctx context.Context,
	desc catalog.TableDescriptor,
	expr tree.Expr,
	semaCtx *tree.SemaContext,
	tn *tree.TableName,
) (tree.TypedExpr, error) {
	nonDropColumns := desc.NonDropColumns()
	sourceInfo := colinfo.NewSourceInfoForSingleTable(
		*tn, colinfo.ResultColumnsFromColumns(desc.GetID(), nonDropColumns),
	)
	expr, err := dequalifyColumnRefs(ctx, sourceInfo, expr)
	if err != nil {
		return nil, err
	}

	// Replace the column variables with dummyColumns so that they can be
	// type-checked.
	replacedExpr, _, err := replaceColumnVars(desc, expr)
	if err != nil {
		return nil, err
	}

	typedExpr, err := tree.TypeCheck(ctx, replacedExpr, semaCtx, types.Any)
	if err != nil {
		return nil, err
	}

	return typedExpr, nil
}

// DequalifyAndValidateExpr validates that an expression has the given type and
// contains no functions with a volatility greater than maxVolatility. The
// type-checked and constant-folded expression, the type of the expression, and
// the set of column IDs within the expression are returned, if valid.
//
// The serialized expression is returned because returning the created
// tree.TypedExpr would be dangerous. It contains dummyColumns which do not
// support evaluation and are not useful outside the context of type-checking
// the expression.
func DequalifyAndValidateExpr(
	ctx context.Context,
	desc catalog.TableDescriptor,
	expr tree.Expr,
	typ *types.T,
	context string,
	semaCtx *tree.SemaContext,
	maxVolatility tree.Volatility,
	tn *tree.TableName,
) (string, *types.T, catalog.TableColSet, error) {
	var colIDs catalog.TableColSet
	nonDropColumns := desc.NonDropColumns()
	sourceInfo := colinfo.NewSourceInfoForSingleTable(
		*tn, colinfo.ResultColumnsFromColumns(desc.GetID(), nonDropColumns),
	)
	expr, err := dequalifyColumnRefs(ctx, sourceInfo, expr)
	if err != nil {
		return "", nil, colIDs, err
	}

	// Replace the column variables with dummyColumns so that they can be
	// type-checked.
	replacedExpr, colIDs, err := replaceColumnVars(desc, expr)
	if err != nil {
		return "", nil, colIDs, err
	}

	typedExpr, err := SanitizeVarFreeExpr(
		ctx,
		replacedExpr,
		typ,
		context,
		semaCtx,
		maxVolatility,
	)

	if err != nil {
		return "", nil, colIDs, err
	}

	return tree.Serialize(typedExpr), typedExpr.ResolvedType(), colIDs, nil
}

// ExtractColumnIDs returns the set of column IDs within the given expression.
func ExtractColumnIDs(
	desc catalog.TableDescriptor, rootExpr tree.Expr,
) (catalog.TableColSet, error) {
	var colIDs catalog.TableColSet

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

		col, err := desc.FindColumnWithName(c.ColumnName)
		if err != nil {
			return false, nil, err
		}

		colIDs.Add(col.GetID())
		return false, expr, nil
	})

	return colIDs, err
}

type returnFalse struct{}

func (returnFalse) Error() string { panic("unimplemented") }

var returnFalsePseudoError error = returnFalse{}

// HasValidColumnReferences returns true if all columns referenced in rootExpr
// exist in desc.
func HasValidColumnReferences(desc catalog.TableDescriptor, rootExpr tree.Expr) (bool, error) {
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

		_, err = desc.FindColumnWithName(c.ColumnName)
		if err != nil {
			return false, expr, returnFalsePseudoError
		}

		return false, expr, nil
	})
	if errors.Is(err, returnFalsePseudoError) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// FormatExprForDisplay formats a schema expression string for display. It
// accepts formatting flags to control things like showing type annotations or
// type casts.
func FormatExprForDisplay(
	ctx context.Context,
	desc catalog.TableDescriptor,
	exprStr string,
	semaCtx *tree.SemaContext,
	sessionData *sessiondata.SessionData,
	fmtFlags tree.FmtFlags,
) (string, error) {
	return formatExprForDisplayImpl(
		ctx,
		desc,
		exprStr,
		semaCtx,
		sessionData,
		fmtFlags,
		false, /* wrapNonFuncExprs */
	)
}

// FormatExprForExpressionIndexDisplay formats an expression index's expression
// element string for display. It is similar to FormatExprForDisplay. The only
// difference is that non-function expressions will be wrapped in parentheses to
// match the parsing requirements for expression indexes.
func FormatExprForExpressionIndexDisplay(
	ctx context.Context,
	desc catalog.TableDescriptor,
	exprStr string,
	semaCtx *tree.SemaContext,
	sessionData *sessiondata.SessionData,
	fmtFlags tree.FmtFlags,
) (string, error) {
	return formatExprForDisplayImpl(
		ctx,
		desc,
		exprStr,
		semaCtx,
		sessionData,
		fmtFlags,
		true, /* wrapNonFuncExprs */
	)
}

func formatExprForDisplayImpl(
	ctx context.Context,
	desc catalog.TableDescriptor,
	exprStr string,
	semaCtx *tree.SemaContext,
	sessionData *sessiondata.SessionData,
	fmtFlags tree.FmtFlags,
	wrapNonFuncExprs bool,
) (string, error) {
	expr, err := deserializeExprForFormatting(ctx, desc, exprStr, semaCtx, fmtFlags)
	if err != nil {
		return "", err
	}
	// Replace any IDs in the expr with their fully qualified names.
	replacedExpr, err := ReplaceIDsWithFQNames(ctx, expr, semaCtx)
	if err != nil {
		return "", err
	}
	f := tree.NewFmtCtx(fmtFlags, tree.FmtDataConversionConfig(sessionData.DataConversionConfig))
	_, isFunc := expr.(*tree.FuncExpr)
	if wrapNonFuncExprs && !isFunc {
		f.WriteByte('(')
	}
	f.FormatNode(replacedExpr)
	if wrapNonFuncExprs && !isFunc {
		f.WriteByte(')')
	}
	return f.CloseAndGetString(), nil
}

func deserializeExprForFormatting(
	ctx context.Context,
	desc catalog.TableDescriptor,
	exprStr string,
	semaCtx *tree.SemaContext,
	fmtFlags tree.FmtFlags,
) (tree.Expr, error) {
	expr, err := parser.ParseExpr(exprStr)
	if err != nil {
		return nil, err
	}

	// Replace the column variables with dummyColumns so that they can be
	// type-checked.
	replacedExpr, _, err := replaceColumnVars(desc, expr)
	if err != nil {
		return nil, err
	}

	// Type-check the expression to resolve user defined types.
	typedExpr, err := replacedExpr.TypeCheck(ctx, semaCtx, types.Any)
	if err != nil {
		return nil, err
	}

	// In pg_catalog, we need to make sure we always display constants instead of
	// expressions, when possible (e.g., turn Array expr into a DArrray). This is
	// best-effort, so if there is any error, it is safe to fallback to the
	// typedExpr.
	if fmtFlags == tree.FmtPGCatalog {
		sanitizedExpr, err := SanitizeVarFreeExpr(ctx, expr, typedExpr.ResolvedType(), "FORMAT", semaCtx,
			tree.VolatilityImmutable)
		// If the expr has no variables and has VolatilityImmutable, we can evaluate
		// it and turn it into a constant.
		if err == nil {
			// An empty EvalContext is fine here since the expression has
			// VolatilityImmutable.
			d, err := sanitizedExpr.Eval(&tree.EvalContext{})
			if err == nil {
				return d, nil
			}
		}
	}

	return typedExpr, nil
}

// nameResolver is used to replace unresolved names in expressions with
// IndexedVars.
type nameResolver struct {
	evalCtx    *tree.EvalContext
	tableID    descpb.ID
	source     *colinfo.DataSourceInfo
	nrc        *nameResolverIVarContainer
	ivarHelper *tree.IndexedVarHelper
}

// newNameResolver creates and returns a nameResolver.
func newNameResolver(
	evalCtx *tree.EvalContext, tableID descpb.ID, tn *tree.TableName, cols []catalog.Column,
) *nameResolver {
	source := colinfo.NewSourceInfoForSingleTable(
		*tn,
		colinfo.ResultColumnsFromColumns(tableID, cols),
	)
	nrc := &nameResolverIVarContainer{append(make([]catalog.Column, 0, len(cols)), cols...)}
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
	var v NameResolutionVisitor
	return ResolveNamesUsingVisitor(&v, expr, nr.source, *nr.ivarHelper, nr.evalCtx.SessionData().SearchPath)
}

// addColumn adds a new column to the nameResolver so that it can be resolved in
// future calls to resolveNames.
func (nr *nameResolver) addColumn(col catalog.Column) {
	nr.ivarHelper.AppendSlot()
	nr.nrc.cols = append(nr.nrc.cols, col)
	newCols := colinfo.ResultColumnsFromColumns(nr.tableID, []catalog.Column{col})
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
	cols []catalog.Column
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
	return nrc.cols[idx].GetType()
}

// IndexVarNodeFormatter implements the tree.IndexedVarContainer interface.
func (nrc *nameResolverIVarContainer) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	return nil
}

// SanitizeVarFreeExpr verifies that an expression is valid, has the correct
// type and contains no variable expressions. It returns the type-checked and
// constant-folded expression.
func SanitizeVarFreeExpr(
	ctx context.Context,
	expr tree.Expr,
	expectedType *types.T,
	context string,
	semaCtx *tree.SemaContext,
	maxVolatility tree.Volatility,
) (tree.TypedExpr, error) {
	if tree.ContainsVars(expr) {
		return nil, pgerror.Newf(pgcode.Syntax,
			"variable sub-expressions are not allowed in %s", context)
	}

	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called from another context
	// which uses the properties field.
	defer semaCtx.Properties.Restore(semaCtx.Properties)

	// Ensure that the expression doesn't contain special functions.
	flags := tree.RejectSpecial

	switch maxVolatility {
	case tree.VolatilityImmutable:
		flags |= tree.RejectStableOperators
		fallthrough

	case tree.VolatilityStable:
		flags |= tree.RejectVolatileFunctions

	case tree.VolatilityVolatile:
		// Allow anything (no flags needed).

	default:
		panic(errors.AssertionFailedf("maxVolatility %s not supported", maxVolatility))
	}
	semaCtx.Properties.Require(context, flags)

	typedExpr, err := tree.TypeCheck(ctx, expr, semaCtx, expectedType)
	if err != nil {
		return nil, err
	}

	actualType := typedExpr.ResolvedType()
	if !expectedType.Equivalent(actualType) && typedExpr != tree.DNull {
		// The expression must match the column type exactly unless it is a constant
		// NULL value.
		return nil, fmt.Errorf("expected %s expression to have type %s, but '%s' has type %s",
			context, expectedType, expr, actualType)
	}
	return typedExpr, nil
}

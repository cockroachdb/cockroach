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
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
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
	ctx context.Context, semaCtx *tree.SemaContext, desc catalog.TableDescriptor, exprStr string,
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

// DequalifyAndValidateExpr takes an expr de-qualifies the column names and
// validates that expression is valid, has the correct type and
// has no impure functions if allowImpure is false.
// Returns the type-checked and constant-folded expression.
func DequalifyAndValidateExpr(
	ctx context.Context,
	desc catalog.TableDescriptor,
	expr tree.Expr,
	typ *types.T,
	op string,
	semaCtx *tree.SemaContext,
	maxVolatility tree.Volatility,
	tn *tree.TableName,
) (tree.TypedExpr, TableColSet, error) {
	var colIDs TableColSet
	sourceInfo := colinfo.NewSourceInfoForSingleTable(
		*tn, colinfo.ResultColumnsFromColDescs(
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

	typedExpr, err := SanitizeVarFreeExpr(
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
func ExtractColumnIDs(desc catalog.TableDescriptor, rootExpr tree.Expr) (TableColSet, error) {
	var colIDs TableColSet

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

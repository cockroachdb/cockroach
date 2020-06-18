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
	ctx context.Context,
	semaCtx *tree.SemaContext,
	desc sqlbase.TableDescriptorInterface,
	exprStr string,
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
	desc sqlbase.TableDescriptorInterface,
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
			desc.TableDesc().AllNonDropColumns(),
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

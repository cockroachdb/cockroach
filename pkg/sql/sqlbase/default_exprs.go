// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// MakeDefaultExprs returns a slice of the default expressions for the slice
// of input column descriptors, or nil if none of the input column descriptors
// have default expressions.
// The length of the result slice matches the length of the input column descriptors.
// For every column that has no default expression, a NULL expression is reported
// as default.
func MakeDefaultExprs(
	ctx context.Context,
	cols []ColumnDescriptor,
	txCtx *transform.ExprTransformContext,
	evalCtx *tree.EvalContext,
	semaCtx *tree.SemaContext,
) ([]tree.TypedExpr, error) {
	// Check to see if any of the columns have DEFAULT expressions. If there
	// are no DEFAULT expressions, we don't bother with constructing the
	// defaults map as the defaults are all NULL.
	haveDefaults := false
	for i := range cols {
		if cols[i].DefaultExpr != nil {
			haveDefaults = true
			break
		}
	}
	if !haveDefaults {
		return nil, nil
	}

	// Build the default expressions map from the parsed SELECT statement.
	defaultExprs := make([]tree.TypedExpr, 0, len(cols))
	exprStrings := make([]string, 0, len(cols))
	for i := range cols {
		col := &cols[i]
		if col.DefaultExpr != nil {
			exprStrings = append(exprStrings, *col.DefaultExpr)
		}
	}
	exprs, err := parser.ParseExprs(exprStrings)
	if err != nil {
		return nil, err
	}

	defExprIdx := 0
	for i := range cols {
		col := &cols[i]
		if col.DefaultExpr == nil {
			defaultExprs = append(defaultExprs, tree.DNull)
			continue
		}
		expr := exprs[defExprIdx]
		typedExpr, err := tree.TypeCheck(ctx, expr, semaCtx, col.Type)
		if err != nil {
			return nil, err
		}
		if typedExpr, err = txCtx.NormalizeExpr(evalCtx, typedExpr); err != nil {
			return nil, err
		}
		defaultExprs = append(defaultExprs, typedExpr)
		defExprIdx++
	}
	return defaultExprs, nil
}

// ProcessDefaultColumns adds columns with DEFAULT to cols if not present
// and returns the defaultExprs for cols.
func ProcessDefaultColumns(
	ctx context.Context,
	cols []ColumnDescriptor,
	tableDesc *ImmutableTableDescriptor,
	txCtx *transform.ExprTransformContext,
	evalCtx *tree.EvalContext,
	semaCtx *tree.SemaContext,
) ([]ColumnDescriptor, []tree.TypedExpr, error) {
	cols = processColumnSet(cols, tableDesc, func(col *ColumnDescriptor) bool {
		return col.DefaultExpr != nil
	})
	defaultExprs, err := MakeDefaultExprs(ctx, cols, txCtx, evalCtx, semaCtx)
	return cols, defaultExprs, err
}

func processColumnSet(
	cols []ColumnDescriptor, tableDesc *ImmutableTableDescriptor, inSet func(*ColumnDescriptor) bool,
) []ColumnDescriptor {
	colIDSet := make(map[ColumnID]struct{}, len(cols))
	for i := range cols {
		colIDSet[cols[i].ID] = struct{}{}
	}

	// Add all public or columns in DELETE_AND_WRITE_ONLY state
	// that satisfy the condition.
	writable := tableDesc.WritableColumns()
	for i := range writable {
		col := &writable[i]
		if inSet(col) {
			if _, ok := colIDSet[col.ID]; !ok {
				colIDSet[col.ID] = struct{}{}
				cols = append(cols, *col)
			}
		}
	}
	return cols
}

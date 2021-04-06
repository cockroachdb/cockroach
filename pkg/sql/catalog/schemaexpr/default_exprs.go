// Copyright 2016 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
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
	cols []descpb.ColumnDescriptor,
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

// ProcessColumnSet returns columns in cols, and other writable
// columns in tableDesc that fulfills a given criteria in inSet.
func ProcessColumnSet(
	cols []descpb.ColumnDescriptor,
	tableDesc catalog.TableDescriptor,
	inSet func(*descpb.ColumnDescriptor) bool,
) []descpb.ColumnDescriptor {
	var colIDSet catalog.TableColSet
	for i := range cols {
		colIDSet.Add(cols[i].ID)
	}

	// Add all public or columns in DELETE_AND_WRITE_ONLY state
	// that satisfy the condition.
	for _, col := range tableDesc.WritableColumns() {
		if inSet(col.ColumnDesc()) {
			if !colIDSet.Contains(col.GetID()) {
				colIDSet.Add(col.GetID())
				cols = append(cols, *col.ColumnDesc())
			}
		}
	}
	return cols
}

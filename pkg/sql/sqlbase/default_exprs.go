// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sqlbase

import (
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
	cols []ColumnDescriptor, txCtx *transform.ExprTransformContext, evalCtx *tree.EvalContext,
) ([]tree.TypedExpr, error) {
	// Check to see if any of the columns have DEFAULT expressions. If there
	// are no DEFAULT expressions, we don't bother with constructing the
	// defaults map as the defaults are all NULL.
	haveDefaults := false
	for _, col := range cols {
		if col.DefaultExpr != nil {
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
	for _, col := range cols {
		if col.DefaultExpr != nil {
			exprStrings = append(exprStrings, *col.DefaultExpr)
		}
	}
	exprs, err := parser.ParseExprs(exprStrings)
	if err != nil {
		return nil, err
	}

	defExprIdx := 0
	for _, col := range cols {
		if col.DefaultExpr == nil {
			defaultExprs = append(defaultExprs, tree.DNull)
			continue
		}
		expr := exprs[defExprIdx]
		typedExpr, err := tree.TypeCheck(expr, nil, col.Type.ToDatumType())
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
	cols []ColumnDescriptor,
	tableDesc *TableDescriptor,
	txCtx *transform.ExprTransformContext,
	evalCtx *tree.EvalContext,
) ([]ColumnDescriptor, []tree.TypedExpr, error) {
	colIDSet := make(map[ColumnID]struct{}, len(cols))
	for _, col := range cols {
		colIDSet[col.ID] = struct{}{}
	}

	// Add the column if it has a DEFAULT expression.
	addIfDefault := func(col ColumnDescriptor) {
		if col.DefaultExpr != nil {
			if _, ok := colIDSet[col.ID]; !ok {
				colIDSet[col.ID] = struct{}{}
				cols = append(cols, col)
			}
		}
	}

	// Add any column that has a DEFAULT expression.
	for _, col := range tableDesc.Columns {
		addIfDefault(col)
	}
	// Also add any column in a mutation that is DELETE_AND_WRITE_ONLY and has
	// a DEFAULT expression.
	for _, m := range tableDesc.Mutations {
		if col := m.GetColumn(); col != nil &&
			m.State == DescriptorMutation_DELETE_AND_WRITE_ONLY {
			addIfDefault(*col)
		}
	}

	defaultExprs, err := MakeDefaultExprs(cols, txCtx, evalCtx)
	return cols, defaultExprs, err
}

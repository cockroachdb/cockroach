// Copyright 2018 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// RowIndexedVarContainer is used to evaluate expressions over various rows.
type RowIndexedVarContainer struct {
	CurSourceRow tree.Datums

	// Because the rows we have might not be permuted in the same way as the
	// original table, we need to store a mapping between them.

	Cols    []ColumnDescriptor
	Mapping map[ColumnID]int
}

var _ tree.IndexedVarContainer = &RowIndexedVarContainer{}

// IndexedVarEval implements tree.IndexedVarContainer.
func (r *RowIndexedVarContainer) IndexedVarEval(
	idx int, ctx *tree.EvalContext,
) (tree.Datum, error) {
	rowIdx, ok := r.Mapping[r.Cols[idx].ID]
	if !ok {
		return tree.DNull, nil
	}
	return r.CurSourceRow[rowIdx], nil
}

// IndexedVarResolvedType implements tree.IndexedVarContainer.
func (*RowIndexedVarContainer) IndexedVarResolvedType(idx int) *types.T {
	panic("unsupported")
}

// IndexedVarNodeFormatter implements tree.IndexedVarContainer.
func (*RowIndexedVarContainer) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	return nil
}

// descContainer is a helper type that implements tree.IndexedVarContainer; it
// is used to type check computed columns and does not support evaluation.
type descContainer struct {
	cols []ColumnDescriptor
}

func (j *descContainer) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	panic("unsupported")
}

func (j *descContainer) IndexedVarResolvedType(idx int) *types.T {
	return &j.cols[idx].Type
}

func (*descContainer) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	return nil
}

// CannotWriteToComputedColError constructs a write error for a computed column.
func CannotWriteToComputedColError(colName string) error {
	return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
		"cannot write directly to computed column %q", tree.ErrNameString(colName))
}

// MakeComputedExprs returns a slice of the computed expressions for the
// slice of input column descriptors, or nil if none of the input column
// descriptors have computed expressions.
// The length of the result slice matches the length of the input column
// descriptors. For every column that has no computed expression, a NULL
// expression is reported.
// addingCols indicates if the input column descriptors are being added
// and allows type checking of the compute expressions to reference
// input columns earlier in the slice.
func MakeComputedExprs(
	cols []ColumnDescriptor,
	tableDesc *ImmutableTableDescriptor,
	tn *tree.TableName,
	txCtx *transform.ExprTransformContext,
	evalCtx *tree.EvalContext,
	addingCols bool,
) ([]tree.TypedExpr, error) {
	// Check to see if any of the columns have computed expressions. If there
	// are none, we don't bother with constructing the map as the expressions
	// are all NULL.
	haveComputed := false
	for i := range cols {
		if cols[i].IsComputed() {
			haveComputed = true
			break
		}
	}
	if !haveComputed {
		return nil, nil
	}

	// Build the computed expressions map from the parsed statement.
	computedExprs := make([]tree.TypedExpr, 0, len(cols))
	exprStrings := make([]string, 0, len(cols))
	for i := range cols {
		col := &cols[i]
		if col.IsComputed() {
			exprStrings = append(exprStrings, *col.ComputeExpr)
		}
	}
	exprs, err := parser.ParseExprs(exprStrings)
	if err != nil {
		return nil, err
	}

	// We need an ivarHelper and sourceInfo, unlike DEFAULT, since computed
	// columns can reference other columns and thus need to be able to resolve
	// column names (at this stage they only need to resolve the types so that
	// the expressions can be typechecked - we have no need to evaluate them).
	iv := &descContainer{tableDesc.Columns}
	ivarHelper := tree.MakeIndexedVarHelper(iv, len(tableDesc.Columns))

	source := NewSourceInfoForSingleTable(*tn, ResultColumnsFromColDescs(tableDesc.GetID(), tableDesc.Columns))
	semaCtx := tree.MakeSemaContext()
	semaCtx.IVarContainer = iv

	addColumnInfo := func(col *ColumnDescriptor) {
		ivarHelper.AppendSlot()
		iv.cols = append(iv.cols, *col)
		newCols := ResultColumnsFromColDescs(tableDesc.GetID(), []ColumnDescriptor{*col})
		source.SourceColumns = append(source.SourceColumns, newCols...)
	}

	compExprIdx := 0
	for i := range cols {
		col := &cols[i]
		if !col.IsComputed() {
			computedExprs = append(computedExprs, tree.DNull)
			if addingCols {
				addColumnInfo(col)
			}
			continue
		}
		expr, _, err := ResolveNames(
			exprs[compExprIdx], source, ivarHelper, evalCtx.SessionData.SearchPath)
		if err != nil {
			return nil, err
		}

		typedExpr, err := tree.TypeCheck(expr, &semaCtx, &col.Type)
		if err != nil {
			return nil, err
		}
		if typedExpr, err = txCtx.NormalizeExpr(evalCtx, typedExpr); err != nil {
			return nil, err
		}
		computedExprs = append(computedExprs, typedExpr)
		compExprIdx++
		if addingCols {
			addColumnInfo(col)
		}
	}
	return computedExprs, nil
}

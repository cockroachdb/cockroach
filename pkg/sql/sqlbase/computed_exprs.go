// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package sqlbase

import (
	"context"

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

// ProcessComputedColumns adds columns which are computed to the set of columns
// being updated and returns the computation exprs for those columns.
//
// The original column descriptors are listed at the beginning of
// the first return slice, and the computed column descriptors come after that.
// The 2nd return slice is an alias for the part of the 1st return slice
// that corresponds to computed columns.
// The 3rd slice has one expression per computed column; that is, its
// length is equal to that of the 2nd return slice.
//
// TODO(justin/knz): This can be made less work intensive by only selecting
// computed columns that depend on one of the updated columns. See issue
// https://github.com/cockroachdb/cockroach/issues/23523.
func ProcessComputedColumns(
	ctx context.Context,
	cols []ColumnDescriptor,
	tn *tree.TableName,
	tableDesc *ImmutableTableDescriptor,
	txCtx *transform.ExprTransformContext,
	evalCtx *tree.EvalContext,
) ([]ColumnDescriptor, []ColumnDescriptor, []tree.TypedExpr, error) {
	computedCols := processColumnSet(nil, tableDesc, func(col *ColumnDescriptor) bool {
		return col.IsComputed()
	})
	cols = append(cols, computedCols...)

	// TODO(justin): it's unfortunate that this parses and typechecks the
	// ComputeExprs on every query.
	computedExprs, err := MakeComputedExprs(computedCols, tableDesc, tn, txCtx, evalCtx, false /* addingCols */)
	return cols, computedCols, computedExprs, err
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

	sources := []*DataSourceInfo{NewSourceInfoForSingleTable(
		*tn, ResultColumnsFromColDescs(tableDesc.Columns),
	)}

	semaCtx := tree.MakeSemaContext()
	semaCtx.IVarContainer = iv

	addColumnInfo := func(col *ColumnDescriptor) {
		ivarHelper.AppendSlot()
		iv.cols = append(iv.cols, *col)
		sources = append(sources, NewSourceInfoForSingleTable(
			*tn, ResultColumnsFromColDescs([]ColumnDescriptor{*col}),
		))
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
		expr, _, _, err := ResolveNames(exprs[compExprIdx],
			MakeMultiSourceInfo(sources...),
			ivarHelper, evalCtx.SessionData.SearchPath)
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

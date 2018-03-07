// Copyright 2018 The Cockroach Authors.
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

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// descContainer is a helper type that implements tree.IndexedVarContainer; it
// is used to type check computed columns and does not support evaluation.
type descContainer struct {
	cols []sqlbase.ColumnDescriptor
}

func (j *descContainer) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	panic("unsupported")
}

func (j *descContainer) IndexedVarResolvedType(idx int) types.T {
	return j.cols[idx].Type.ToDatumType()
}

func (*descContainer) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	return nil
}

func cannotWriteToComputedColError(col sqlbase.ColumnDescriptor) error {
	return pgerror.NewErrorf(pgerror.CodeObjectNotInPrerequisiteStateError, "cannot write directly to computed column %q",
		tree.ErrString(&tree.ColumnItem{
			ColumnName: tree.Name(col.Name),
		}))
}

func checkHasNoComputedCols(cols []sqlbase.ColumnDescriptor) error {
	for i := range cols {
		if cols[i].IsComputed() {
			return cannotWriteToComputedColError(cols[i])
		}
	}
	return nil
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
	cols []sqlbase.ColumnDescriptor,
	tn *tree.TableName,
	tableDesc *sqlbase.TableDescriptor,
	txCtx *transform.ExprTransformContext,
	evalCtx *tree.EvalContext,
) ([]sqlbase.ColumnDescriptor, []sqlbase.ColumnDescriptor, []tree.TypedExpr, error) {
	// TODO(justin): the DEFAULT version of this code also had code to handle
	// mutations. We don't support adding computed columns yet, but we will need
	// to add that back in.

	// TODO(justin): is there a way we can somehow cache this property on the
	// table descriptor so we don't have to iterate through all of these?
	haveComputed := false
	endOfNonComputed := len(cols)
	for _, col := range tableDesc.Columns {
		if col.IsComputed() {
			cols = append(cols, col)
			haveComputed = true
		}
	}

	// If this table has no computed columns, don't bother.
	if !haveComputed {
		return cols, nil, nil, nil
	}

	computedCols := cols[endOfNonComputed:]

	// TODO(justin): it's unfortunate that this parses and typechecks the
	// ComputeExprs on every query.
	exprStrings := make([]string, 0, len(cols))
	for _, col := range computedCols {
		exprStrings = append(exprStrings, *col.ComputeExpr)
	}
	exprs, err := parser.ParseExprs(exprStrings)
	if err != nil {
		return nil, nil, nil, err
	}

	// We need an ivarHelper and sourceInfo, unlike DEFAULT, since computed
	// columns can reference other columns and thus need to be able to resolve
	// column names (at this stage they only need to resolve the types so that
	// the expressions can be typechecked - we have no need to evaluate them).
	iv := &descContainer{tableDesc.Columns}
	ivarHelper := tree.MakeIndexedVarHelper(iv, len(tableDesc.Columns))

	sourceInfo := sqlbase.NewSourceInfoForSingleTable(
		*tn, sqlbase.ResultColumnsFromColDescs(tableDesc.Columns),
	)

	semaCtx := tree.MakeSemaContext(false)
	semaCtx.IVarContainer = iv

	computedExprs := make([]tree.TypedExpr, 0, len(cols))
	for i, col := range computedCols {
		expr, _, _, err := resolveNames(exprs[i],
			sqlbase.MakeMultiSourceInfo(sourceInfo),
			ivarHelper, evalCtx.SessionData.SearchPath)
		if err != nil {
			return nil, nil, nil, err
		}

		typedExpr, err := tree.TypeCheck(expr, &semaCtx, col.Type.ToDatumType())
		if err != nil {
			return nil, nil, nil, err
		}
		if typedExpr, err = txCtx.NormalizeExpr(evalCtx, typedExpr); err != nil {
			return nil, nil, nil, err
		}
		computedExprs = append(computedExprs, typedExpr)
	}

	return cols, computedCols, computedExprs, nil
}

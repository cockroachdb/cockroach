// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// joinPredicate implements the predicate logic for joins.
type joinPredicate struct {
	// This struct must be allocated on the heap and its location stay
	// stable after construction because it implements
	// IndexedVarContainer and the IndexedVar objects in sub-expressions
	// will link to it by reference after checkRenderStar / analyzeExpr.
	// Enforce this using NoCopy.
	_ util.NoCopy

	joinType descpb.JoinType

	// numLeft/RightCols are the number of columns in the left and right
	// operands.
	numLeftCols, numRightCols int

	// left/rightEqualityIndices give the position of equality columns
	// on the left and right input row arrays, respectively.
	// Only columns with the same left and right value types can be equality
	// columns.
	leftEqualityIndices  []exec.NodeColumnOrdinal
	rightEqualityIndices []exec.NodeColumnOrdinal

	// The list of names for the columns listed in leftEqualityIndices.
	// Used mainly for pretty-printing.
	leftColNames tree.NameList
	// The list of names for the columns listed in rightEqualityIndices.
	// Used mainly for pretty-printing.
	rightColNames tree.NameList

	// For ON predicates or joins with an added filter expression we need a row
	// buffer.
	curRow tree.Datums
	// The ON condition that needs to be evaluated (in addition to the
	// equality columns).
	onCond tree.TypedExpr

	leftCols  colinfo.ResultColumns
	rightCols colinfo.ResultColumns
	cols      colinfo.ResultColumns

	// If set, the left equality columns form a key in the left input. Used as a
	// hint for optimizing execution.
	leftEqKey bool
	// If set, the right equality columns form a key in the right input. Used as a
	// hint for optimizing execution.
	rightEqKey bool
}

var _ eval.IndexedVarContainer = &joinPredicate{}

// getJoinResultColumns returns the result columns of a join.
func getJoinResultColumns(
	joinType descpb.JoinType, left, right colinfo.ResultColumns,
) colinfo.ResultColumns {
	columns := make(colinfo.ResultColumns, 0, len(left)+len(right))
	if joinType.ShouldIncludeLeftColsInOutput() {
		columns = append(columns, left...)
	}
	if joinType.ShouldIncludeRightColsInOutput() {
		columns = append(columns, right...)
	}
	return columns
}

// makePredicate constructs a joinPredicate object for joins. The equality
// columns / on condition must be initialized separately.
func makePredicate(
	joinType descpb.JoinType, left, right colinfo.ResultColumns, onCond tree.TypedExpr,
) *joinPredicate {
	return &joinPredicate{
		joinType:     joinType,
		numLeftCols:  len(left),
		numRightCols: len(right),
		leftCols:     left,
		rightCols:    right,
		cols:         getJoinResultColumns(joinType, left, right),
		onCond:       onCond,
		curRow:       make(tree.Datums, len(left)+len(right)),
	}
}

// IndexedVarEval implements the eval.IndexedVarContainer interface.
func (p *joinPredicate) IndexedVarEval(idx int) (tree.Datum, error) {
	return p.curRow[idx], nil
}

// IndexedVarResolvedType implements the tree.IndexedVarContainer interface.
func (p *joinPredicate) IndexedVarResolvedType(idx int) *types.T {
	if idx < p.numLeftCols {
		return p.leftCols[idx].Typ
	}
	return p.rightCols[idx-p.numLeftCols].Typ
}

// eval for joinPredicate runs the on condition across the columns that do
// not participate in the equality (the equality columns are checked
// in the join algorithm already).
// Returns true if there is no on condition or the on condition accepts the
// row.
func (p *joinPredicate) eval(
	ctx context.Context, evalCtx *eval.Context, leftRow, rightRow tree.Datums,
) (bool, error) {
	if p.onCond != nil {
		copy(p.curRow[:len(leftRow)], leftRow)
		copy(p.curRow[len(leftRow):], rightRow)
		evalCtx.PushIVarContainer(p)
		pred, err := execinfrapb.RunFilter(ctx, p.onCond, evalCtx)
		evalCtx.PopIVarContainer()
		return pred, err
	}
	return true, nil
}

// prepareRow prepares the output row by combining values from the
// input data sources.
func (p *joinPredicate) prepareRow(result, leftRow, rightRow tree.Datums) {
	copy(result[:len(leftRow)], leftRow)
	copy(result[len(leftRow):], rightRow)
}

// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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

	joinType sqlbase.JoinType

	// numLeft/RightCols are the number of columns in the left and right
	// operands.
	numLeftCols, numRightCols int

	// left/rightEqualityIndices give the position of equality columns
	// on the left and right input row arrays, respectively.
	// Only columns with the same left and right value types can be equality
	// columns.
	leftEqualityIndices  []int
	rightEqualityIndices []int

	// The list of names for the columns listed in leftEqualityIndices.
	// Used mainly for pretty-printing.
	leftColNames tree.NameList
	// The list of names for the columns listed in rightEqualityIndices.
	// Used mainly for pretty-printing.
	rightColNames tree.NameList

	// For ON predicates or joins with an added filter expression,
	// we need an IndexedVarHelper, the DataSourceInfo, a row buffer
	// and the expression itself.
	iVarHelper tree.IndexedVarHelper
	curRow     tree.Datums
	// The ON condition that needs to be evaluated (in addition to the
	// equality columns).
	onCond tree.TypedExpr

	leftCols  sqlbase.ResultColumns
	rightCols sqlbase.ResultColumns
	cols      sqlbase.ResultColumns

	// If set, the left equality columns form a key in the left input. Used as a
	// hint for optimizing execution.
	leftEqKey bool
	// If set, the right equality columns form a key in the right input. Used as a
	// hint for optimizing execution.
	rightEqKey bool
}

// makePredicate constructs a joinPredicate object for joins. The equality
// columns / on condition must be initialized separately.
func makePredicate(joinType sqlbase.JoinType, left, right sqlbase.ResultColumns) *joinPredicate {
	// For anti and semi joins, the right columns are omitted from the output (but
	// they must be available internally for the ON condition evaluation).
	omitRightColumns := joinType == sqlbase.LeftSemiJoin || joinType == sqlbase.LeftAntiJoin

	// Prepare the metadata for the result columns.
	// The structure of the join data source results is like this:
	// - all the left columns,
	// - then all the right columns (except for anti/semi join).
	columns := make(sqlbase.ResultColumns, 0, len(left)+len(right))
	columns = append(columns, left...)
	if !omitRightColumns {
		columns = append(columns, right...)
	}

	pred := &joinPredicate{
		joinType:     joinType,
		numLeftCols:  len(left),
		numRightCols: len(right),
		leftCols:     left,
		rightCols:    right,
		cols:         columns,
	}
	// We must initialize the indexed var helper in all cases, even when
	// there is no on condition, so that getNeededColumns() does not get
	// confused.
	pred.curRow = make(tree.Datums, len(left)+len(right))
	pred.iVarHelper = tree.MakeIndexedVarHelper(pred, len(pred.curRow))

	return pred
}

// IndexedVarEval implements the tree.IndexedVarContainer interface.
func (p *joinPredicate) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	return p.curRow[idx].Eval(ctx)
}

// IndexedVarResolvedType implements the tree.IndexedVarContainer interface.
func (p *joinPredicate) IndexedVarResolvedType(idx int) *types.T {
	if idx < p.numLeftCols {
		return p.leftCols[idx].Typ
	}
	return p.rightCols[idx-p.numLeftCols].Typ
}

// IndexedVarNodeFormatter implements the tree.IndexedVarContainer interface.
func (p *joinPredicate) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	if idx < p.numLeftCols {
		return p.leftCols.NodeFormatter(idx)
	}
	return p.rightCols.NodeFormatter(idx - p.numLeftCols)
}

// eval for joinPredicate runs the on condition across the columns that do
// not participate in the equality (the equality columns are checked
// in the join algorithm already).
// Returns true if there is no on condition or the on condition accepts the
// row.
func (p *joinPredicate) eval(ctx *tree.EvalContext, leftRow, rightRow tree.Datums) (bool, error) {
	if p.onCond != nil {
		copy(p.curRow[:len(leftRow)], leftRow)
		copy(p.curRow[len(leftRow):], rightRow)
		ctx.PushIVarContainer(p.iVarHelper.Container())
		pred, err := sqlbase.RunFilter(p.onCond, ctx)
		ctx.PopIVarContainer()
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

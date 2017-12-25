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

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// joinPredicate implements the predicate logic for joins.
type joinPredicate struct {
	joinType joinType

	// numLeft/RightCols are the number of columns in the left and right
	// operands.
	numLeftCols, numRightCols int

	// The comparison function to use for each column. We need different
	// functions because each USING column (or columns in equality ON
	// expressions)  may have a different type (and they may be
	// heterogeneous between left and right).
	cmpFunctions []func(*tree.EvalContext, tree.Datum, tree.Datum) (tree.Datum, error)

	// left/rightEqualityIndices give the position of USING columns
	// on the left and right input row arrays, respectively.
	// Left/right columns that have an equality constraint in the ON
	// condition also have their indices appended when tryAddEqualityFilter
	// is invoked (see planner.addJoinFilter).
	leftEqualityIndices  []int
	rightEqualityIndices []int

	// The list of names for the columns listed in leftEqualityIndices.
	// Used mainly for pretty-printing.
	leftColNames tree.NameList
	// The list of names for the columns listed in rightEqualityIndices.
	// Used mainly for pretty-printing.
	rightColNames tree.NameList

	// For ON predicates or joins with an added filter expression,
	// we need an IndexedVarHelper, the dataSourceInfo, a row buffer
	// and the expression itself.
	iVarHelper tree.IndexedVarHelper
	info       *dataSourceInfo
	curRow     tree.Datums
	onCond     tree.TypedExpr

	// This struct must be allocated on the heap and its location stay
	// stable after construction because it implements
	// IndexedVarContainer and the IndexedVar objects in sub-expressions
	// will link to it by reference after checkRenderStar / analyzeExpr.
	// Enforce this using NoCopy.
	//
	//lint:ignore U1000 this marker prevents by-value copies.
	noCopy util.NoCopy
}

// makeCrossPredicate constructs a joinPredicate object for joins with a ON clause.
func makeCrossPredicate(
	typ joinType, left, right *dataSourceInfo,
) (*joinPredicate, *dataSourceInfo, error) {
	return makeEqualityPredicate(typ, left, right, nil /*leftColNames*/, nil /*rightColNames */)
}

// tryAddEqualityFilter attempts to turn the given filter expression into
// an equality predicate. It returns true iff the transformation succeeds.
func (p *joinPredicate) tryAddEqualityFilter(filter tree.Expr, left, right *dataSourceInfo) bool {
	c, ok := filter.(*tree.ComparisonExpr)
	if !ok || c.Operator != tree.EQ {
		return false
	}
	lhs, ok := c.Left.(*tree.IndexedVar)
	if !ok {
		return false
	}
	rhs, ok := c.Right.(*tree.IndexedVar)
	if !ok {
		return false
	}

	sourceBoundary := len(left.sourceColumns)
	if (lhs.Idx >= sourceBoundary && rhs.Idx >= sourceBoundary) ||
		(lhs.Idx < sourceBoundary && rhs.Idx < sourceBoundary) {
		// Both variables are on the same side of the join (e.g. `a JOIN b ON a.x = a.y`).
		return false
	}

	if lhs.Idx > rhs.Idx {
		lhs, rhs = rhs, lhs
	}

	// At this point we have an equality, so we can add it to the list
	// of equality columns.

	// To do this we must be a bit careful: the expression contains
	// IndexedVars, and the column indices at this point will refer to
	// the full column set of the joinPredicate, including the
	// merged columns.
	leftColIdx := lhs.Idx
	rightColIdx := rhs.Idx - len(left.sourceColumns)

	// Also, we will want to avoid redundant equality checks.
	for i := range p.leftEqualityIndices {
		if p.leftEqualityIndices[i] == leftColIdx && p.rightEqualityIndices[i] == rightColIdx {
			// The filter is already there; simply absorb it and say we succeeded.
			return true
		}
	}

	// First resolve the comparison function. We can't use the
	// ComparisonExpr's memoized comparison directly, because we may
	// have swapped the operands above.
	fn, found := tree.FindEqualComparisonFunction(lhs.ResolvedType(), rhs.ResolvedType())
	if !found {
		// This is ... unexpected. This means we have a valid ON
		// expression of the form "a = b" but the expression "b = a" is
		// invalid. We could simply avoid the optimization but this is
		// really a bug in the built-in semantics so we want to complain
		// loudly.
		panic(fmt.Errorf(
			"predicate %s is valid, but '%s = %s' cannot be type checked",
			c, lhs.ResolvedType(), rhs.ResolvedType(),
		))
	}
	p.cmpFunctions = append(p.cmpFunctions, fn)

	p.leftEqualityIndices = append(p.leftEqualityIndices, leftColIdx)
	p.rightEqualityIndices = append(p.rightEqualityIndices, rightColIdx)
	p.leftColNames = append(p.leftColNames, tree.Name(left.sourceColumns[leftColIdx].Name))
	p.rightColNames = append(p.rightColNames, tree.Name(right.sourceColumns[rightColIdx].Name))

	return true
}

// makeOnPredicate constructs a joinPredicate object for joins with a ON clause.
func (p *planner) makeOnPredicate(
	ctx context.Context, typ joinType, left, right *dataSourceInfo, expr tree.Expr,
) (*joinPredicate, *dataSourceInfo, error) {
	pred, info, err := makeEqualityPredicate(typ, left, right, nil /*leftColNames*/, nil /*rightColNames*/)
	if err != nil {
		return nil, nil, err
	}

	// Determine the on condition expression.
	onCond, err := p.analyzeExpr(ctx, expr, multiSourceInfo{info}, pred.iVarHelper, types.Bool, true, "ON")
	if err != nil {
		return nil, nil, err
	}
	pred.onCond = onCond
	return pred, info, nil
}

// makeUsingPredicate constructs a joinPredicate object for joins with
// a USING clause.
func makeUsingPredicate(
	typ joinType, left, right *dataSourceInfo, usingCols tree.NameList,
) (*joinPredicate, *dataSourceInfo, error) {
	seenNames := make(map[string]struct{})

	for _, syntaxColName := range usingCols {
		colName := string(syntaxColName)
		// Check for USING(x,x)
		if _, ok := seenNames[colName]; ok {
			return nil, nil, fmt.Errorf("column %q appears more than once in USING clause", colName)
		}
		seenNames[colName] = struct{}{}
	}

	return makeEqualityPredicate(typ, left, right, usingCols, usingCols)
}

// makeEqualityPredicate constructs a joinPredicate object for joins. The join
// condition includes equality between the columns specified by leftColNames and
// rightColNames.
func makeEqualityPredicate(
	typ joinType, left, right *dataSourceInfo, leftColNames, rightColNames tree.NameList,
) (resPred *joinPredicate, info *dataSourceInfo, err error) {
	if len(leftColNames) != len(rightColNames) {
		panic(fmt.Errorf("left columns' length %q doesn't match right columns' length %q in EqualityPredicate",
			len(leftColNames), len(rightColNames)))
	}

	// Prepare the arrays populated below.
	cmpOps := make([]func(*tree.EvalContext, tree.Datum, tree.Datum) (tree.Datum, error), len(leftColNames))
	leftEqualityIndices := make([]int, len(leftColNames))
	rightEqualityIndices := make([]int, len(rightColNames))

	// Find out which columns are involved in EqualityPredicate.
	for i := range leftColNames {
		leftColName := string(leftColNames[i])
		rightColName := string(rightColNames[i])

		// Find the column name on the left.
		leftIdx, leftType, err := pickUsingColumn(left.sourceColumns, leftColName, "left")
		if err != nil {
			return nil, nil, err
		}

		// Find the column name on the right.
		rightIdx, rightType, err := pickUsingColumn(right.sourceColumns, rightColName, "right")
		if err != nil {
			return nil, nil, err
		}

		// Remember the indices.
		leftEqualityIndices[i] = leftIdx
		rightEqualityIndices[i] = rightIdx

		// Memoize the comparison function.
		fn, found := tree.FindEqualComparisonFunction(leftType, rightType)
		if !found {
			return nil, nil, fmt.Errorf("JOIN/USING types %s for left column %s and %s for right column %s cannot be matched",
				leftType, leftColName, rightType, rightColName)
		}
		cmpOps[i] = fn
	}

	// Now, prepare/complete the metadata for the result columns.
	// The structure of the join data source results is like this:
	// - first, all the equality/USING columns;
	// - then all the left columns,
	// - then all the right columns,
	// The duplicate columns appended after the equality/USING columns
	// are hidden so that they are invisible to star expansion, but
	// not omitted so that they can still be selected separately.

	columns := make(sqlbase.ResultColumns, 0, len(left.sourceColumns)+len(right.sourceColumns))
	columns = append(columns, left.sourceColumns...)
	columns = append(columns, right.sourceColumns...)

	// Compute the mappings from table aliases to column sets from
	// both sides into a new alias-columnset mapping for the result
	// rows. We need to be extra careful about the aliases
	// for the anonymous table, which needs to be merged.
	aliases := make(sourceAliases, 0, len(left.sourceAliases)+len(right.sourceAliases))

	var anonymousCols util.FastIntSet

	collectAliases := func(sourceAliases sourceAliases, offset int) {
		for _, alias := range sourceAliases {
			newSet := alias.columnSet.Shift(offset)
			if alias.name == anonymousTable {
				anonymousCols.UnionWith(newSet)
			} else {
				aliases = append(aliases, sourceAlias{name: alias.name, columnSet: newSet})
			}
		}
	}
	collectAliases(left.sourceAliases, 0)
	collectAliases(right.sourceAliases, len(left.sourceColumns))
	if !anonymousCols.Empty() {
		aliases = append(aliases, sourceAlias{name: anonymousTable, columnSet: anonymousCols})
	}

	info = &dataSourceInfo{
		sourceColumns: columns,
		sourceAliases: aliases,
	}

	pred := &joinPredicate{
		joinType:             typ,
		numLeftCols:          len(left.sourceColumns),
		numRightCols:         len(right.sourceColumns),
		leftColNames:         leftColNames,
		rightColNames:        rightColNames,
		cmpFunctions:         cmpOps,
		leftEqualityIndices:  leftEqualityIndices,
		rightEqualityIndices: rightEqualityIndices,
		info:                 info,
	}
	// We must initialize the indexed var helper in all cases, even when
	// there is no on condition, so that getNeededColumns() does not get
	// confused.
	pred.iVarHelper = tree.MakeIndexedVarHelper(pred, len(columns))
	return pred, info, nil
}

// IndexedVarEval implements the tree.IndexedVarContainer interface.
func (p *joinPredicate) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	return p.curRow[idx].Eval(ctx)
}

// IndexedVarResolvedType implements the tree.IndexedVarContainer interface.
func (p *joinPredicate) IndexedVarResolvedType(idx int) types.T {
	return p.info.sourceColumns[idx].Typ
}

// IndexedVarNodeFormatter implements the tree.IndexedVarContainer interface.
func (p *joinPredicate) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	return p.info.NodeFormatter(idx)
}

// eval for joinPredicate runs the on condition across the columns that do
// not participate in the equality (the equality columns are checked
// in the join algorithm already).
// Returns true if there is no on condition or the on condition accepts the
// row.
func (p *joinPredicate) eval(
	ctx *tree.EvalContext, result, leftRow, rightRow tree.Datums,
) (bool, error) {
	if p.onCond != nil {
		p.curRow = result
		copy(p.curRow[:len(leftRow)], leftRow)
		copy(p.curRow[len(leftRow):], rightRow)
		ctx.IVarHelper = &p.iVarHelper
		pred, err := sqlbase.RunFilter(p.onCond, ctx)
		ctx.IVarHelper = nil
		return pred, err
	}
	return true, nil
}

// getNeededColumns figures out the columns needed for the two
// sources.  This takes into account both the equality columns and the
// predicate expression.
func (p *joinPredicate) getNeededColumns(neededJoined []bool) ([]bool, []bool) {
	// Reset the helper and rebind the variable to detect which columns
	// are effectively needed.
	p.onCond = p.iVarHelper.Rebind(p.onCond, true, false)

	// The columns that are part of the expression are always needed.
	neededJoined = append([]bool(nil), neededJoined...)
	for i := range neededJoined {
		if p.iVarHelper.IndexedVarUsed(i) {
			neededJoined[i] = true
		}
	}
	leftNeeded := neededJoined[:p.numLeftCols]
	rightNeeded := neededJoined[p.numLeftCols:]

	// The equality columns are always needed.
	for i := range p.leftEqualityIndices {
		leftNeeded[p.leftEqualityIndices[i]] = true
		rightNeeded[p.rightEqualityIndices[i]] = true
	}
	return leftNeeded, rightNeeded
}

// prepareRow prepares the output row by combining values from the
// input data sources.
func (p *joinPredicate) prepareRow(result, leftRow, rightRow tree.Datums) {
	copy(result[:len(leftRow)], leftRow)
	copy(result[len(leftRow):], rightRow)
}

// encode returns the encoding of a row from a given side (left or right),
// according to the columns specified by the equality constraints.
func (p *joinPredicate) encode(b []byte, row tree.Datums, cols []int) ([]byte, bool, error) {
	var err error
	containsNull := false
	for _, colIdx := range cols {
		if row[colIdx] == tree.DNull {
			containsNull = true
		}
		b, err = sqlbase.EncodeDatum(b, row[colIdx])
		if err != nil {
			return nil, false, err
		}
	}
	return b, containsNull, nil
}

// pickUsingColumn searches for a column whose name matches colName.
// The column index and type are returned if found, otherwise an error
// is reported.
func pickUsingColumn(
	cols sqlbase.ResultColumns, colName string, context string,
) (int, types.T, error) {
	idx := invalidColIdx
	for j, col := range cols {
		if col.Hidden {
			continue
		}
		if col.Name == colName {
			idx = j
			break
		}
	}
	if idx == invalidColIdx {
		return idx, nil, fmt.Errorf("column \"%s\" specified in USING clause does not exist in %s table", colName, context)
	}
	return idx, cols[idx].Typ, nil
}

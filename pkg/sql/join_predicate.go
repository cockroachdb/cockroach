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
//
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package sql

import (
	"bytes"
	"fmt"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// joinPredicate implements the predicate logic for joins.
type joinPredicate struct {
	// numLeft/RightCols are the number of columns in the left and right
	// operands.
	numLeftCols, numRightCols int

	// The comparison function to use for each column. We need
	// different functions because each USING column may have a different
	// type (and they may be heterogeneous between left and right).
	cmpFunctions []func(*parser.EvalContext, parser.Datum, parser.Datum) (parser.Datum, error)

	// left/rightEqualityIndices give the position of USING columns
	// on the left and right input row arrays, respectively.
	leftEqualityIndices  []int
	rightEqualityIndices []int

	// The list of names for the columns listed in leftEqualityIndices.
	// Used mainly for pretty-printing.
	leftColNames parser.NameList
	// The list of names for the columns listed in rightEqualityIndices.
	// Used mainly for pretty-printing.
	rightColNames parser.NameList

	// numMergedEqualityColumns specifies how many of the equality
	// columns must be merged at the beginning of each result row. This
	// is the desired behavior for USING and NATURAL JOIN.
	numMergedEqualityColumns int

	// For ON predicates or joins with an added filter expression,
	// we need an IndexedVarHelper, the dataSourceInfo, a row buffer
	// and the expression itself.
	iVarHelper parser.IndexedVarHelper
	info       *dataSourceInfo
	curRow     parser.Datums
	onCond     parser.TypedExpr

	// This struct must be allocated on the heap and its location stay
	// stable after construction because it implements
	// IndexedVarContainer and the IndexedVar objects in sub-expressions
	// will link to it by reference after checkRenderStar / analyzeExpr.
	// Enforce this using NoCopy.
	noCopy util.NoCopy
}

// makeCrossPredicate constructs a joinPredicate object for joins with a ON clause.
func makeCrossPredicate(left, right *dataSourceInfo) (*joinPredicate, *dataSourceInfo, error) {
	return makeEqualityPredicate(left, right, nil, nil, 0, nil)
}

// tryAddEqualityFilter attempts to turn the given filter expression into
// an equality predicate. It returns true iff the transformation succeeds.
func (p *joinPredicate) tryAddEqualityFilter(filter parser.Expr, left, right *dataSourceInfo) bool {
	c, ok := filter.(*parser.ComparisonExpr)
	if !ok || c.Operator != parser.EQ {
		return false
	}
	lhs, ok := c.Left.(*parser.IndexedVar)
	if !ok {
		return false
	}
	rhs, ok := c.Right.(*parser.IndexedVar)
	if !ok {
		return false
	}

	sourceBoundary := p.numMergedEqualityColumns + len(left.sourceColumns)
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
	leftColIdx := lhs.Idx - p.numMergedEqualityColumns
	rightColIdx := rhs.Idx - len(left.sourceColumns) - p.numMergedEqualityColumns

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
	fn, found := parser.FindEqualComparisonFunction(lhs.ResolvedType(), rhs.ResolvedType())
	if !found {
		// This is ... unexpected. This means we have a valid ON
		// expression of the form "a = b" but the expression "b = a" is
		// invalid. We could simply avoid the optimization but this is
		// really a bug in the built-in semantics so we want to complain
		// loudly.
		panic(fmt.Errorf("predicate %s is valid, but '%T = %T' cannot be type checked", c, lhs, rhs))
	}
	p.cmpFunctions = append(p.cmpFunctions, fn)

	p.leftEqualityIndices = append(p.leftEqualityIndices, leftColIdx)
	p.rightEqualityIndices = append(p.rightEqualityIndices, rightColIdx)
	p.leftColNames = append(p.leftColNames, parser.Name(left.sourceColumns[leftColIdx].Name))
	p.rightColNames = append(p.rightColNames, parser.Name(right.sourceColumns[rightColIdx].Name))

	return true
}

// makeOnPredicate constructs a joinPredicate object for joins with a ON clause.
func (p *planner) makeOnPredicate(
	ctx context.Context, left, right *dataSourceInfo, expr parser.Expr,
) (*joinPredicate, *dataSourceInfo, error) {
	pred, info, err := makeEqualityPredicate(left, right, nil, nil, 0, nil)
	if err != nil {
		return nil, nil, err
	}

	// Determine the on condition expression.
	onCond, err := p.analyzeExpr(ctx, expr, multiSourceInfo{info}, pred.iVarHelper, parser.TypeBool, true, "ON")
	if err != nil {
		return nil, nil, err
	}
	pred.onCond = onCond
	return pred, info, nil
}

// makeUsingPredicate constructs a joinPredicate object for joins with
// a USING clause.
func makeUsingPredicate(
	left, right *dataSourceInfo, colNames parser.NameList,
) (*joinPredicate, *dataSourceInfo, error) {
	seenNames := make(map[string]struct{})

	for _, unnormalizedColName := range colNames {
		colName := unnormalizedColName.Normalize()
		// Check for USING(x,x)
		if _, ok := seenNames[colName]; ok {
			return nil, nil, fmt.Errorf("column %q appears more than once in USING clause", colName)
		}
		seenNames[colName] = struct{}{}
	}

	return makeEqualityPredicate(left, right, colNames, colNames, len(colNames), nil)
}

// makeEqualityPredicate constructs a joinPredicate object for joins. The join
// condition includes equality between numMergedEqualityColumns columns,
// specified by leftColNames and rightColNames.
func makeEqualityPredicate(
	left, right *dataSourceInfo,
	leftColNames, rightColNames parser.NameList,
	numMergedEqualityColumns int,
	concatInfos *dataSourceInfo,
) (resPred *joinPredicate, info *dataSourceInfo, err error) {
	if len(leftColNames) != len(rightColNames) {
		panic(fmt.Errorf("left columns' length %q doesn't match right columns' length %q in EqualityPredicate",
			len(leftColNames), len(rightColNames)))
	}
	if len(leftColNames) < numMergedEqualityColumns {
		panic(fmt.Errorf("cannot merge %d columns, only %d columns to compare", numMergedEqualityColumns, len(leftColNames)))
	}

	// Prepare the arrays populated below.
	cmpOps := make([]func(*parser.EvalContext, parser.Datum, parser.Datum) (parser.Datum, error), len(leftColNames))
	leftEqualityIndices := make([]int, len(leftColNames))
	rightEqualityIndices := make([]int, len(rightColNames))

	// usedLeft represents the list of indices that participate in the
	// equality predicate. They are collected in order to determine
	// below which columns remain after the equality; this is used
	// only when merging result columns.
	var usedLeft, usedRight []int
	var columns ResultColumns
	if numMergedEqualityColumns > 0 {
		usedLeft = make([]int, len(left.sourceColumns))
		for i := range usedLeft {
			usedLeft[i] = invalidColIdx
		}
		usedRight = make([]int, len(right.sourceColumns))
		for i := range usedRight {
			usedRight[i] = invalidColIdx
		}
		nResultColumns := len(left.sourceColumns) + len(right.sourceColumns) - numMergedEqualityColumns
		columns = make(ResultColumns, 0, nResultColumns)
	}

	// Find out which columns are involved in EqualityPredicate.
	for i := range leftColNames {
		leftColName := leftColNames[i].Normalize()
		rightColName := rightColNames[i].Normalize()

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
		fn, found := parser.FindEqualComparisonFunction(leftType, rightType)
		if !found {
			return nil, nil, fmt.Errorf("JOIN/USING types %s for left column %s and %s for right column %s cannot be matched",
				leftType, leftColName, rightType, rightColName)
		}
		cmpOps[i] = fn

		if i < numMergedEqualityColumns {
			usedLeft[leftIdx] = i
			usedRight[rightIdx] = i

			// Merged columns come first in the results.
			columns = append(columns, left.sourceColumns[leftIdx])
		}
	}

	// Now, prepare/complete the metadata for the result columns.
	// The structure of the join data source results is like this:
	// - first, all the equality/USING columns;
	// - then all the left columns,
	// - then all the right columns,
	// The duplicate columns appended after the equality/USING columns
	// are hidden so that they are invisible to star expansion, but
	// not omitted so that they can still be selected separately.

	// Finish collecting the column definitions from the left and
	// right data sources.
	for i, c := range left.sourceColumns {
		if usedLeft != nil && usedLeft[i] != invalidColIdx {
			c.hidden = true
		}
		columns = append(columns, c)
	}
	for i, c := range right.sourceColumns {
		if usedRight != nil && usedRight[i] != invalidColIdx {
			c.hidden = true
		}
		columns = append(columns, c)
	}

	// Compute the mappings from table aliases to column sets from
	// both sides into a new alias-columnset mapping for the result
	// rows. We need to be extra careful about the aliases
	// for the anonymous table, which needs to be merged.
	aliases := make(sourceAliases, 0, len(left.sourceAliases)+len(right.sourceAliases))

	collectAliases := func(sourceAliases sourceAliases, offset int) {
		for _, alias := range sourceAliases {
			if alias.name == anonymousTable {
				continue
			}
			newRange := make([]int, len(alias.columnRange))
			for i, colIdx := range alias.columnRange {
				newRange[i] = colIdx + offset
			}
			aliases = append(aliases, sourceAlias{name: alias.name, columnRange: newRange})
		}
	}
	collectAliases(left.sourceAliases, numMergedEqualityColumns)
	collectAliases(right.sourceAliases, numMergedEqualityColumns+len(left.sourceColumns))

	anonymousAlias := sourceAlias{name: anonymousTable, columnRange: nil}
	var hiddenLeftNames, hiddenRightNames []string

	// All the merged columns at the beginning belong to the
	// anonymous data source.
	for i := 0; i < numMergedEqualityColumns; i++ {
		anonymousAlias.columnRange = append(anonymousAlias.columnRange, i)
		hiddenLeftNames = append(hiddenLeftNames, parser.ReNormalizeName(left.sourceColumns[i].Name))
		hiddenRightNames = append(hiddenRightNames, parser.ReNormalizeName(right.sourceColumns[i].Name))
	}

	// Now collect the other table-less columns into the anonymous data
	// source, but hide (skip) those that are already merged.
	collectAnonymousAliases := func(
		sourceAliases sourceAliases, hiddenNames []string, cols ResultColumns, offset int,
	) {
		for _, alias := range sourceAliases {
			if alias.name != anonymousTable {
				continue
			}
			for _, colIdx := range alias.columnRange {
				isHidden := false
				for _, hiddenName := range hiddenNames {
					if parser.ReNormalizeName(cols[colIdx].Name) == hiddenName {
						isHidden = true
						break
					}
				}
				if !isHidden {
					anonymousAlias.columnRange = append(anonymousAlias.columnRange, colIdx+offset)
				}
			}
		}
	}
	collectAnonymousAliases(left.sourceAliases, hiddenLeftNames, left.sourceColumns,
		numMergedEqualityColumns)
	collectAnonymousAliases(right.sourceAliases, hiddenRightNames, right.sourceColumns,
		numMergedEqualityColumns+len(left.sourceColumns))

	if anonymousAlias.columnRange != nil {
		aliases = append(aliases, anonymousAlias)
	}

	info = &dataSourceInfo{
		sourceColumns: columns,
		sourceAliases: aliases,
	}

	pred := &joinPredicate{
		numLeftCols:              len(left.sourceColumns),
		numRightCols:             len(right.sourceColumns),
		leftColNames:             leftColNames,
		rightColNames:            rightColNames,
		numMergedEqualityColumns: numMergedEqualityColumns,
		cmpFunctions:             cmpOps,
		leftEqualityIndices:      leftEqualityIndices,
		rightEqualityIndices:     rightEqualityIndices,
		info:                     info,
	}
	// We must initialize the indexed var helper in all cases, even when
	// there is no on condition, so that getNeededColumns() does not get
	// confused.
	pred.iVarHelper = parser.MakeIndexedVarHelper(pred, len(columns))
	return pred, info, nil
}

// IndexedVarEval implements the parser.IndexedVarContainer interface.
func (p *joinPredicate) IndexedVarEval(idx int, ctx *parser.EvalContext) (parser.Datum, error) {
	return p.curRow[idx].Eval(ctx)
}

// IndexedVarResolvedType implements the parser.IndexedVarContainer interface.
func (p *joinPredicate) IndexedVarResolvedType(idx int) parser.Type {
	return p.info.sourceColumns[idx].Typ
}

// IndexedVarFormat implements the parser.IndexedVarContainer interface.
func (p *joinPredicate) IndexedVarFormat(buf *bytes.Buffer, f parser.FmtFlags, idx int) {
	p.info.FormatVar(buf, f, idx)
}

// eval for joinPredicate runs the on condition across the columns that do
// not participate in the equality (the equality columns are checked
// in the join algorithm already).
// Returns true if there is no on condition or the on condition accepts the
// row.
func (p *joinPredicate) eval(
	ctx *parser.EvalContext, result, leftRow, rightRow parser.Datums,
) (bool, error) {
	if p.onCond != nil {
		p.curRow = result
		copy(p.curRow[p.numMergedEqualityColumns:p.numMergedEqualityColumns+len(leftRow)], leftRow)
		copy(p.curRow[p.numMergedEqualityColumns+len(leftRow):], rightRow)
		return sqlbase.RunFilter(p.onCond, ctx)
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
	leftNeeded := neededJoined[p.numMergedEqualityColumns : p.numMergedEqualityColumns+p.numLeftCols]
	rightNeeded := neededJoined[p.numMergedEqualityColumns+p.numLeftCols:]

	// The equality columns are always needed.
	for i := range p.leftEqualityIndices {
		leftNeeded[p.leftEqualityIndices[i]] = true
		rightNeeded[p.rightEqualityIndices[i]] = true
	}
	return leftNeeded, rightNeeded
}

// prepareRow prepares the output row by combining values from the
// input data sources.
func (p *joinPredicate) prepareRow(result, leftRow, rightRow parser.Datums) {
	var offset int
	for offset = 0; offset < p.numMergedEqualityColumns; offset++ {
		// The result for merged columns must be computed as per COALESCE().
		leftIdx := p.leftEqualityIndices[offset]
		if leftRow[leftIdx] != parser.DNull {
			result[offset] = leftRow[leftIdx]
		} else {
			result[offset] = rightRow[p.rightEqualityIndices[offset]]
		}
	}
	copy(result[offset:offset+len(leftRow)], leftRow)
	copy(result[offset+len(leftRow):], rightRow)
}

// encode returns the encoding of a row from a given side (left or right),
// according to the columns specified by the equality constraints.
func (p *joinPredicate) encode(b []byte, row parser.Datums, cols []int) ([]byte, bool, error) {
	var err error
	containsNull := false
	for _, colIdx := range cols {
		if row[colIdx] == parser.DNull {
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
func pickUsingColumn(cols ResultColumns, colName string, context string) (int, parser.Type, error) {
	idx := invalidColIdx
	for j, col := range cols {
		if col.hidden {
			continue
		}
		if parser.ReNormalizeName(col.Name) == colName {
			idx = j
		}
	}
	if idx == invalidColIdx {
		return idx, nil, fmt.Errorf("column \"%s\" specified in USING clause does not exist in %s table", colName, context)
	}
	return idx, cols[idx].Typ, nil
}

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

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
)

const (
	leftSide = iota
	rightSide
)

type joinPredicate interface {
	// eval tests whether the current combination of rows passes the
	// predicate. The result argument is an array pre-allocated to the
	// right size, which can be used as intermediate buffer.
	eval(result, leftRow, rightRow parser.DTuple) (bool, error)

	// getNeededColumns figures out the columns needed for the two sources.
	getNeededColumns(neededJoined []bool) (neededLeft []bool, neededRight []bool)

	// prepareRow prepares the output row by combining values from the
	// input data sources.
	prepareRow(result, leftRow, rightRow parser.DTuple)

	// encode returns the encoding of a row from a given side (left or right),
	// according to the columns specified by the equality constraints.
	encode(scratch []byte, row parser.DTuple, side int) (encoding []byte, containsNull bool, err error)

	// expand and start propagate to embedded sub-queries.
	expand() error
	start() error

	// format pretty-prints the predicate for EXPLAIN.
	format(buf *bytes.Buffer)
	// explainTypes registers the expression types for EXPLAIN.
	explainTypes(f func(string, string))
}

var _ joinPredicate = &equalityPredicate{}

// joinPredicateBase contains fields common to all joinPredicates.
type joinPredicateBase struct {
	numLeftCols, numRightCols int
}

// makeCrossPredicate constructs a joinPredicate object for joins with a ON clause.
func (p *planner) makeCrossPredicate(
	left, right *dataSourceInfo,
) (*equalityPredicate, *dataSourceInfo, error) {
	return p.makeEqualityPredicate(left, right, nil, nil, 0, nil)
}

// IndexedVarEval implements the parser.IndexedVarContainer interface.
func (p *equalityPredicate) IndexedVarEval(idx int, ctx *parser.EvalContext) (parser.Datum, error) {
	return p.curRow[idx].Eval(ctx)
}

// IndexedVarResolvedType implements the parser.IndexedVarContainer interface.
func (p *equalityPredicate) IndexedVarResolvedType(idx int) parser.Type {
	return p.info.sourceColumns[idx].Typ
}

// IndexedVarFormat implements the parser.IndexedVarContainer interface.
func (p *equalityPredicate) IndexedVarFormat(buf *bytes.Buffer, f parser.FmtFlags, idx int) {
	p.info.FormatVar(buf, f, idx)
}

func (p *equalityPredicate) expand() error {
	return p.p.expandSubqueryPlans(p.filter)
}

func (p *equalityPredicate) start() error {
	return p.p.startSubqueryPlans(p.filter)
}

func (p *equalityPredicate) explainTypes(regTypes func(string, string)) {
	if p.filter != nil {
		regTypes("filter", parser.AsStringWithFlags(p.filter, parser.FmtShowTypes))
	}
}

// optimizeOnPredicate tries to turn the filter in an onPredicate into
// equality columns in the equalityPredicate, which enables faster
// joins.  The concatInfos argument, if provided, must be a
// precomputed concatenation of the left and right dataSourceInfos.
func (p *planner) optimizeOnPredicate(
	pred *equalityPredicate, left, right *dataSourceInfo, concatInfos *dataSourceInfo,
) (*equalityPredicate, *dataSourceInfo, error) {
	c, ok := pred.filter.(*parser.ComparisonExpr)
	if !ok || c.Operator != parser.EQ {
		return pred, pred.info, nil
	}
	lhs, ok := c.Left.(*parser.IndexedVar)
	if !ok {
		return pred, pred.info, nil
	}
	rhs, ok := c.Right.(*parser.IndexedVar)
	if !ok {
		return pred, pred.info, nil
	}

	sourceBoundary := pred.numMergedEqualityColumns + len(left.sourceColumns)
	if (lhs.Idx >= sourceBoundary && rhs.Idx >= sourceBoundary) ||
		(lhs.Idx < sourceBoundary && rhs.Idx < sourceBoundary) {
		// Both variables are on the same side of the join (e.g. `a JOIN b ON a.x = a.y`).
		return pred, pred.info, nil
	}

	if lhs.Idx > rhs.Idx {
		tmp := lhs
		lhs = rhs
		rhs = tmp
	}

	// At this point we have an equality, so we can add it to the list
	// of equality columns.

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
	pred.cmpFunctions = append(pred.cmpFunctions, fn)

	// To do this we must be a bit careful: the expression contains
	// IndexedVars, and the column indices at this point will refer to
	// the full column set of the equalityPredicate, including the
	// merged columns.
	leftColIdx := lhs.Idx - pred.numMergedEqualityColumns
	rightColIdx := rhs.Idx - len(left.sourceColumns) - pred.numMergedEqualityColumns

	pred.leftEqualityIndices = append(pred.leftEqualityIndices, leftColIdx)
	pred.rightEqualityIndices = append(pred.rightEqualityIndices, rightColIdx)
	pred.leftColNames = append(pred.leftColNames, parser.Name(left.sourceColumns[leftColIdx].Name))
	pred.rightColNames = append(pred.rightColNames, parser.Name(right.sourceColumns[rightColIdx].Name))

	// The filter is optimized away now.
	pred.filter = nil

	return pred, pred.info, nil
}

// makeOnPredicate constructs a joinPredicate object for joins with a ON clause.
func (p *planner) makeOnPredicate(
	left, right *dataSourceInfo, expr parser.Expr,
) (*equalityPredicate, *dataSourceInfo, error) {
	pred, info, err := p.makeEqualityPredicate(left, right, nil, nil, 0, nil)
	if err != nil {
		return nil, nil, err
	}

	// Determine the filter expression.
	filter, err := p.analyzeExpr(expr, multiSourceInfo{info}, pred.iVarHelper, parser.TypeBool, true, "ON")
	if err != nil {
		return nil, nil, err
	}
	pred.filter = filter

	return p.optimizeOnPredicate(pred, left, right, info)
}

// equalityPredicate implements the predicate logic for joins with a USING clause.
type equalityPredicate struct {
	joinPredicateBase

	p *planner

	// The comparison function to use for each column. We need
	// different functions because each USING column may have a different
	// type (and they may be heterogeneous between left and right).
	cmpFunctions []func(*parser.EvalContext, parser.Datum, parser.Datum) (parser.DBool, error)

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
	curRow     parser.DTuple
	filter     parser.TypedExpr

	// This struct must be allocated on the heap and its location stay
	// stable after construction because it implements
	// IndexedVarContainer and the IndexedVar objects in sub-expressions
	// will link to it by reference after checkRenderStar / analyzeExpr.
	// Enforce this using NoCopy.
	noCopy util.NoCopy
}

func (p *equalityPredicate) format(buf *bytes.Buffer) {
	if p.filter != nil || len(p.leftColNames) > 0 {
		buf.WriteString(" ON ")
	}
	prefix := ""
	if len(p.leftColNames) > 0 {
		buf.WriteString("EQUALS((")
		p.leftColNames.Format(buf, parser.FmtSimple)
		buf.WriteString("),(")
		p.rightColNames.Format(buf, parser.FmtSimple)
		buf.WriteString("))")
		prefix = " AND "
	}
	if p.filter != nil {
		buf.WriteString(prefix)
		p.filter.Format(buf, parser.FmtQualify)
	}
}

// eval for equalityPredicate compares the columns that participate in
// the equality, returning true if and only if all predicate columns
// compare equal.
func (p *equalityPredicate) eval(result, leftRow, rightRow parser.DTuple) (bool, error) {
	for i := range p.leftEqualityIndices {
		leftVal := leftRow[p.leftEqualityIndices[i]]
		rightVal := rightRow[p.rightEqualityIndices[i]]
		if leftVal == parser.DNull || rightVal == parser.DNull {
			return false, nil
		}
		res, err := p.cmpFunctions[i](&p.p.evalCtx, leftVal, rightVal)
		if err != nil {
			return false, err
		}
		if res != *parser.DBoolTrue {
			return false, nil
		}
	}
	if p.filter != nil {
		p.curRow = result
		copy(p.curRow[p.numMergedEqualityColumns:p.numMergedEqualityColumns+len(leftRow)], leftRow)
		copy(p.curRow[p.numMergedEqualityColumns+len(leftRow):], rightRow)
		return sqlbase.RunFilter(p.filter, &p.p.evalCtx)
	}
	return true, nil
}

func (p *equalityPredicate) getNeededColumns(neededJoined []bool) ([]bool, []bool) {
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

// prepareRow for equalityPredicate. First, we should check if this
// equalityPredicate is generated by makeUsingPredicate,
// in this situation we'll have more work to do than for ON
// clauses and CROSS JOIN: a result row contains first the values for
// the USING columns; then the non-USING values from the left input
// row, then the non-USING values from the right input row.
func (p *equalityPredicate) prepareRow(result, leftRow, rightRow parser.DTuple) {
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

func (p *equalityPredicate) encode(b []byte, row parser.DTuple, side int) ([]byte, bool, error) {
	var cols []int
	switch side {
	case rightSide:
		cols = p.rightEqualityIndices
	case leftSide:
		cols = p.leftEqualityIndices
	default:
		panic("invalid side provided, only leftSide or rightSide applicable")
	}

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

// makeUsingPredicate constructs a joinPredicate object for joins with
// a USING clause.
func (p *planner) makeUsingPredicate(
	left, right *dataSourceInfo, colNames parser.NameList,
) (*equalityPredicate, *dataSourceInfo, error) {
	seenNames := make(map[string]struct{})

	for _, unnormalizedColName := range colNames {
		colName := unnormalizedColName.Normalize()
		// Check for USING(x,x)
		if _, ok := seenNames[colName]; ok {
			return nil, nil, fmt.Errorf("column %q appears more than once in USING clause", colName)
		}
		seenNames[colName] = struct{}{}
	}

	return p.makeEqualityPredicate(left, right, colNames, colNames, len(colNames), nil)
}

// makeEqualityPredicate constructs a joinPredicate object for joins.
func (p *planner) makeEqualityPredicate(
	left, right *dataSourceInfo,
	leftColNames, rightColNames parser.NameList,
	numMergedEqualityColumns int,
	concatInfos *dataSourceInfo,
) (resPred *equalityPredicate, info *dataSourceInfo, err error) {
	if len(leftColNames) != len(rightColNames) {
		panic(fmt.Errorf("left columns' length %q doesn't match right columns' length %q in EqualityPredicate",
			len(leftColNames), len(rightColNames)))
	}
	if len(leftColNames) < numMergedEqualityColumns {
		panic(fmt.Errorf("cannot merge %d columns, only %d columns to compare", numMergedEqualityColumns, len(leftColNames)))
	}

	// Prepare the arrays populated below.
	cmpOps := make([]func(*parser.EvalContext, parser.Datum, parser.Datum) (parser.DBool, error), len(leftColNames))
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
	// rows. This is similar to what concatDataSources() does, but
	// here we also shift the indices from both sides to accommodate
	// the merged join columns generated at the beginning.
	aliases := make(sourceAliases, 0, len(left.sourceAliases)+len(right.sourceAliases))
	for _, alias := range left.sourceAliases {
		newRange := make([]int, len(alias.columnRange))
		for i, colIdx := range alias.columnRange {
			newRange[i] = colIdx + numMergedEqualityColumns
		}
		aliases = append(aliases, sourceAlias{name: alias.name, columnRange: newRange})
	}

	for _, alias := range right.sourceAliases {
		newRange := make([]int, len(alias.columnRange))
		for i, colIdx := range alias.columnRange {
			newRange[i] = colIdx + numMergedEqualityColumns + len(left.sourceColumns)
		}
		aliases = append(aliases, sourceAlias{name: alias.name, columnRange: newRange})
	}

	if numMergedEqualityColumns > 0 {
		// All the merged columns at the beginning belong to an
		// anonymous data source.
		usingRange := make([]int, numMergedEqualityColumns)
		for i := range usingRange {
			usingRange[i] = i
		}
		aliases = append(aliases, sourceAlias{name: anonymousTable, columnRange: usingRange})
	}

	info = &dataSourceInfo{
		sourceColumns: columns,
		sourceAliases: aliases,
	}

	pred := &equalityPredicate{
		joinPredicateBase: joinPredicateBase{
			numLeftCols:  len(left.sourceColumns),
			numRightCols: len(right.sourceColumns),
		},
		p:                        p,
		leftColNames:             leftColNames,
		rightColNames:            rightColNames,
		numMergedEqualityColumns: numMergedEqualityColumns,
		cmpFunctions:             cmpOps,
		leftEqualityIndices:      leftEqualityIndices,
		rightEqualityIndices:     rightEqualityIndices,
		info:                     info,
	}
	// We must initialize the indexed var helper in all cases, even when
	// there is no filter expression, so that getNeededColumns() does
	// not get confused.
	pred.iVarHelper = parser.MakeIndexedVarHelper(pred, len(columns))
	return pred, info, nil
}

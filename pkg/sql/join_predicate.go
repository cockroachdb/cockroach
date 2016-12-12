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

var _ joinPredicate = &onPredicate{}
var _ joinPredicate = &crossPredicate{}
var _ joinPredicate = &equalityPredicate{}

// joinPredicateBase contains fields common to all joinPredicates.
type joinPredicateBase struct {
	numLeftCols, numRightCols int
}

func (p *joinPredicateBase) getNeededColumnsConcat(
	neededJoined []bool,
) (neededLeft []bool, neededRight []bool) {
	if len(neededJoined) != p.numLeftCols+p.numRightCols {
		panic(fmt.Sprintf(
			"expected %d+%d cols, got %d", p.numLeftCols, p.numRightCols, len(neededJoined),
		))
	}
	return neededJoined[:p.numLeftCols], neededJoined[p.numLeftCols:]
}

// prepareRowConcat implement the simple case of CROSS JOIN or JOIN
// with an ON clause, where the rows of the two inputs are simply
// concatenated.
func prepareRowConcat(result parser.DTuple, leftRow parser.DTuple, rightRow parser.DTuple) {
	copy(result, leftRow)
	copy(result[len(leftRow):], rightRow)
}

// crossPredicate implements the predicate logic for CROSS JOIN. The
// predicate is always true, the work done here is thus minimal.
type crossPredicate struct {
	joinPredicateBase
}

func (p *crossPredicate) eval(_, _, _ parser.DTuple) (bool, error) {
	return true, nil
}

func (p *crossPredicate) getNeededColumns(neededJoined []bool) ([]bool, []bool) {
	return p.getNeededColumnsConcat(neededJoined)
}

func (p *crossPredicate) prepareRow(result, leftRow, rightRow parser.DTuple) {
	prepareRowConcat(result, leftRow, rightRow)
}
func (p *crossPredicate) start() error                        { return nil }
func (p *crossPredicate) expand() error                       { return nil }
func (p *crossPredicate) format(_ *bytes.Buffer)              {}
func (p *crossPredicate) explainTypes(_ func(string, string)) {}
func (p *crossPredicate) encode(_ []byte, _ parser.DTuple, _ int) ([]byte, bool, error) {
	return nil, false, nil
}

// makeCrossPredicate constructs a joinPredicate object for joins with a ON clause.
func (p *planner) makeCrossPredicate(left, right *dataSourceInfo) (joinPredicate, *dataSourceInfo) {
	pred := &crossPredicate{
		joinPredicateBase: joinPredicateBase{
			numLeftCols:  len(left.sourceColumns),
			numRightCols: len(right.sourceColumns),
		},
	}
	info := concatDataSourceInfos(left, right)
	return pred, info
}

// onPredicate implements the predicate logic for joins with an ON clause.
type onPredicate struct {
	joinPredicateBase
	p          *planner
	iVarHelper parser.IndexedVarHelper
	filter     parser.TypedExpr
	info       *dataSourceInfo
	curRow     parser.DTuple

	// This struct must be allocated on the heap and its location stay
	// stable after construction because it implements
	// IndexedVarContainer and the IndexedVar objects in sub-expressions
	// will link to it by reference after checkRenderStar / analyzeExpr.
	// Enforce this using NoCopy.
	noCopy util.NoCopy
}

// IndexedVarEval implements the parser.IndexedVarContainer interface.
func (p *onPredicate) IndexedVarEval(idx int, ctx *parser.EvalContext) (parser.Datum, error) {
	return p.curRow[idx].Eval(ctx)
}

// IndexedVarResolvedType implements the parser.IndexedVarContainer interface.
func (p *onPredicate) IndexedVarResolvedType(idx int) parser.Type {
	return p.info.sourceColumns[idx].Typ
}

// IndexedVarFormat implements the parser.IndexedVarContainer interface.
func (p *onPredicate) IndexedVarFormat(buf *bytes.Buffer, f parser.FmtFlags, idx int) {
	p.info.FormatVar(buf, f, idx)
}

func (p *onPredicate) encode(_ []byte, _ parser.DTuple, _ int) ([]byte, bool, error) {
	panic("ON predicate extraction unimplemented")
}

// eval for onPredicate uses an arbitrary SQL expression to determine
// whether the left and right input row can join.
func (p *onPredicate) eval(result, leftRow, rightRow parser.DTuple) (bool, error) {
	p.curRow = result
	prepareRowConcat(p.curRow, leftRow, rightRow)
	return sqlbase.RunFilter(p.filter, &p.p.evalCtx)
}

func (p *onPredicate) getNeededColumns(neededJoined []bool) ([]bool, []bool) {
	// The columns that are part of the expression are always needed.
	neededJoined = append([]bool(nil), neededJoined...)
	for i := range neededJoined {
		if p.iVarHelper.IndexedVarUsed(i) {
			neededJoined[i] = true
		}
	}
	return p.getNeededColumnsConcat(neededJoined)
}

func (p *onPredicate) prepareRow(result, leftRow, rightRow parser.DTuple) {
	prepareRowConcat(result, leftRow, rightRow)
}

func (p *onPredicate) expand() error {
	return p.p.expandSubqueryPlans(p.filter)
}

func (p *onPredicate) start() error {
	return p.p.startSubqueryPlans(p.filter)
}

func (p *onPredicate) format(buf *bytes.Buffer) {
	buf.WriteString(" ON ")
	p.filter.Format(buf, parser.FmtQualify)
}

func (p *onPredicate) explainTypes(regTypes func(string, string)) {
	if p.filter != nil {
		regTypes("filter", parser.AsStringWithFlags(p.filter, parser.FmtShowTypes))
	}
}

// optimizeOnPredicate tries to turn an onPredicate into an
// equalityPredicate, which enables faster joins.
// The concatInfos argument, if provided, must be a precomputed
// concatenation of the left and right dataSourceInfos.
func (p *planner) optimizeOnPredicate(
	pred *onPredicate, left, right *dataSourceInfo, concatInfos *dataSourceInfo,
) (joinPredicate, *dataSourceInfo, error) {
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
	if (lhs.Idx >= len(left.sourceColumns) && rhs.Idx >= len(left.sourceColumns)) ||
		(lhs.Idx < len(left.sourceColumns) && rhs.Idx < len(left.sourceColumns)) {
		// Both variables are on the same side of the join (e.g. `a JOIN b ON a.x = a.y`).
		return pred, pred.info, nil
	}

	if lhs.Idx > rhs.Idx {
		lhs, rhs = rhs, lhs
	}

	leftColNames := parser.NameList{parser.Name(left.sourceColumns[lhs.Idx].Name)}
	rightColNames := parser.NameList{parser.Name(right.sourceColumns[rhs.Idx-len(left.sourceColumns)].Name)}

	return p.makeEqualityPredicate(left, right, leftColNames, rightColNames, false, concatInfos)
}

// makeOnPredicate constructs a joinPredicate object for joins with a ON clause.
func (p *planner) makeOnPredicate(
	left, right *dataSourceInfo, expr parser.Expr,
) (joinPredicate, *dataSourceInfo, error) {
	// Output rows are the concatenation of input rows.
	info := concatDataSourceInfos(left, right)

	pred := &onPredicate{
		joinPredicateBase: joinPredicateBase{
			numLeftCols:  len(left.sourceColumns),
			numRightCols: len(right.sourceColumns),
		},
		p:    p,
		info: info,
	}

	// Determine the filter expression.
	colInfo := multiSourceInfo{left, right}
	pred.iVarHelper = parser.MakeIndexedVarHelper(pred, len(info.sourceColumns))
	filter, err := p.analyzeExpr(expr, colInfo, pred.iVarHelper, parser.TypeBool, true, "ON")
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
	usingCmp []func(*parser.EvalContext, parser.Datum, parser.Datum) (parser.DBool, error)

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

	// mergeEqualityColumns specifies that the result row must only
	// contain one column for every pair of columns tested for equality.
	// This is the desired behavior for USING and NATURAL JOIN.
	mergeEqualityColumns bool

	// left/rightEqualityIndices give the position of non-merged columns on
	// the left and right input row arrays, respectively. This is used
	// when mergeEqualityColumns is true.
	leftRestIndices  []int
	rightRestIndices []int
}

func (p *equalityPredicate) format(buf *bytes.Buffer) {
	buf.WriteString(" ON EQUALS((")
	p.leftColNames.Format(buf, parser.FmtSimple)
	buf.WriteString("),(")
	p.rightColNames.Format(buf, parser.FmtSimple)
	buf.WriteString("))")
}
func (p *equalityPredicate) start() error                        { return nil }
func (p *equalityPredicate) expand() error                       { return nil }
func (p *equalityPredicate) explainTypes(_ func(string, string)) {}

// eval for equalityPredicate compares the columns that participate in
// the equality, returning true if and only if all predicate columns
// compare equal.
func (p *equalityPredicate) eval(_, leftRow, rightRow parser.DTuple) (bool, error) {
	for i := range p.leftEqualityIndices {
		leftVal := leftRow[p.leftEqualityIndices[i]]
		rightVal := rightRow[p.rightEqualityIndices[i]]
		if leftVal == parser.DNull || rightVal == parser.DNull {
			return false, nil
		}
		res, err := p.usingCmp[i](&p.p.evalCtx, leftVal, rightVal)
		if err != nil {
			return false, err
		}
		if res != *parser.DBoolTrue {
			return false, nil
		}
	}
	return true, nil
}

func (p *equalityPredicate) getNeededColumns(neededJoined []bool) ([]bool, []bool) {
	if !p.mergeEqualityColumns {
		leftNeeded, rightNeeded := p.getNeededColumnsConcat(neededJoined)
		// The equality columns are always needed.
		for i := range p.leftEqualityIndices {
			leftNeeded[p.leftEqualityIndices[i]] = true
			rightNeeded[p.rightEqualityIndices[i]] = true
		}
		return leftNeeded, rightNeeded
	}
	leftNeeded := make([]bool, p.numLeftCols)
	rightNeeded := make([]bool, p.numRightCols)

	// The equality columns are always needed.
	for i := range p.leftEqualityIndices {
		leftNeeded[p.leftEqualityIndices[i]] = true
		rightNeeded[p.rightEqualityIndices[i]] = true
	}

	delta := len(p.leftEqualityIndices)
	for i := range p.leftRestIndices {
		if neededJoined[delta+i] {
			leftNeeded[p.leftRestIndices[i]] = true
		}
	}
	delta += len(p.leftRestIndices)
	for i := range p.rightRestIndices {
		if neededJoined[delta+i] {
			rightNeeded[p.rightRestIndices[i]] = true
		}
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
	if !p.mergeEqualityColumns {
		// Columns are not merged, simply emit them all.
		copy(result[:len(leftRow)], leftRow)
		copy(result[len(leftRow):], rightRow)
		return
	}

	d := 0
	for k, j := range p.leftEqualityIndices {
		// The result for merged columns must be computed as per COALESCE().
		if leftRow[j] != parser.DNull {
			result[d] = leftRow[j]
		} else {
			result[d] = rightRow[p.rightEqualityIndices[k]]
		}
		d++
	}
	for _, j := range p.leftRestIndices {
		result[d] = leftRow[j]
		d++
	}
	for _, j := range p.rightRestIndices {
		result[d] = rightRow[j]
		d++
	}
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
) (joinPredicate, *dataSourceInfo, error) {
	seenNames := make(map[string]struct{})

	for _, unnormalizedColName := range colNames {
		colName := unnormalizedColName.Normalize()
		// Check for USING(x,x)
		if _, ok := seenNames[colName]; ok {
			return nil, nil, fmt.Errorf("column %q appears more than once in USING clause", colName)
		}
		seenNames[colName] = struct{}{}
	}

	return p.makeEqualityPredicate(left, right, colNames, colNames, true, nil)
}

// makeEqualityPredicate constructs a joinPredicate object for joins.
func (p *planner) makeEqualityPredicate(
	left, right *dataSourceInfo,
	leftColNames, rightColNames parser.NameList,
	mergeEqualityColumns bool,
	concatInfos *dataSourceInfo,
) (resPred joinPredicate, info *dataSourceInfo, err error) {
	if len(leftColNames) != len(rightColNames) {
		panic(fmt.Errorf("left columns' length %q doesn't match right columns' length %q in EqualityPredicate",
			len(leftColNames), len(rightColNames)))
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
	if mergeEqualityColumns {
		usedLeft = make([]int, len(left.sourceColumns))
		for i := range usedLeft {
			usedLeft[i] = invalidColIdx
		}
		usedRight = make([]int, len(right.sourceColumns))
		for i := range usedRight {
			usedRight[i] = invalidColIdx
		}
		nResultColumns := len(left.sourceColumns) + len(right.sourceColumns) - len(leftColNames)
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

		if mergeEqualityColumns {
			usedLeft[leftIdx] = i
			usedRight[rightIdx] = i

			// Merged columns come first in the results.
			columns = append(columns, left.sourceColumns[leftIdx])
		}
	}

	var leftRestIndices, rightRestIndices []int

	if mergeEqualityColumns {
		// Find out which columns are not involved in the predicate.
		leftRestIndices = make([]int, 0, len(left.sourceColumns)-1)
		for i := range left.sourceColumns {
			if usedLeft[i] == invalidColIdx {
				leftRestIndices = append(leftRestIndices, i)
				usedLeft[i] = len(columns)
				columns = append(columns, left.sourceColumns[i])
			}
		}

		rightRestIndices = make([]int, 0, len(right.sourceColumns)-1)
		for i := range right.sourceColumns {
			if usedRight[i] == invalidColIdx {
				rightRestIndices = append(rightRestIndices, i)
				usedRight[i] = len(columns)
				columns = append(columns, right.sourceColumns[i])
			}
		}

		// Merge the mappings from table aliases to column sets from both
		// sides into a new alias-columnset mapping for the result rows.
		aliases := make(sourceAliases, 0, len(left.sourceAliases)+len(right.sourceAliases))
		for _, alias := range left.sourceAliases {
			newRange := make([]int, len(alias.columnRange))
			for i, colIdx := range alias.columnRange {
				newRange[i] = usedLeft[colIdx]
			}
			aliases = append(aliases, sourceAlias{name: alias.name, columnRange: newRange})
		}

		for _, alias := range right.sourceAliases {
			newRange := make([]int, len(alias.columnRange))
			for i, colIdx := range alias.columnRange {
				newRange[i] = usedRight[colIdx]
			}
			aliases = append(aliases, sourceAlias{name: alias.name, columnRange: newRange})
		}

		info = &dataSourceInfo{
			sourceColumns: columns,
			sourceAliases: aliases,
		}
	} else {
		// Columns are not merged; result columns are simply the
		// concatenations of left and right columns.
		info = concatInfos
		if info == nil {
			info = concatDataSourceInfos(left, right)
		}
	}

	return &equalityPredicate{
		joinPredicateBase: joinPredicateBase{
			numLeftCols:  len(left.sourceColumns),
			numRightCols: len(right.sourceColumns),
		},
		p:                    p,
		leftColNames:         leftColNames,
		rightColNames:        rightColNames,
		mergeEqualityColumns: mergeEqualityColumns,
		usingCmp:             cmpOps,
		leftEqualityIndices:  leftEqualityIndices,
		rightEqualityIndices: rightEqualityIndices,
		leftRestIndices:      leftRestIndices,
		rightRestIndices:     rightRestIndices,
	}, info, nil
}

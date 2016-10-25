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
	"github.com/pkg/errors"
)

type joinType int

const (
	joinTypeInner joinType = iota
	joinTypeOuterLeft
	joinTypeOuterFull
)

// joinNode is a planNode whose rows are the result of an inner or
// left/right outer join.
type joinNode struct {
	joinType joinType

	// The data sources.
	left    planDataSource
	right   planDataSource
	swapped bool

	// pred represents the join predicate.
	pred joinPredicate

	// columns contains the metadata for the results of this node.
	columns ResultColumns

	// output contains the last generated row of results from this node.
	output parser.DTuple

	// rightRows contains a copy of all rows from the data source on the
	// right of the join.
	rightRows *valuesNode

	// rightMatched remembers which of the right rows have matched in a
	// full outer join.
	rightMatched []bool

	// rightIdx indicates the current right row.
	// In a full outer join, the value becomes negative during
	// the last pass searching for unmatched right rows.
	rightIdx int

	// passedFilter turns to true when the current left row has matched
	// at least one right row.
	passedFilter bool

	// emptyRight contain tuples of NULL values to use on the
	// right for outer joins when the filter fails.
	emptyRight parser.DTuple

	// emptyLeft contains tuples of NULL values to use
	// on the left for full outer joins when the filter fails.
	emptyLeft parser.DTuple

	// explain indicates whether this node is running on behalf of
	// EXPLAIN(DEBUG).
	explain explainMode

	// doneReadingRight is used by debugNext() and DebugValues() when
	// explain == explainDebug.
	doneReadingRight bool
}

type joinPredicate interface {
	// eval tests whether the current combination of rows passes the
	// predicate. The result argument is an array pre-allocated to the
	// right size, which can be used as intermediate buffer.
	eval(result, leftRow, rightRow parser.DTuple) (bool, error)

	// prepareRow prepares the output row by combining values from the
	// input data sources.
	prepareRow(result, leftRow, rightRow parser.DTuple)

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
var _ joinPredicate = &usingPredicate{}

// prepareRowConcat implement the simple case of CROSS JOIN or JOIN
// with an ON clause, where the rows of the two inputs are simply
// concatenated.
func prepareRowConcat(result parser.DTuple, leftRow parser.DTuple, rightRow parser.DTuple) {
	copy(result, leftRow)
	copy(result[len(leftRow):], rightRow)
}

// crossPredicate implements the predicate logic for CROSS JOIN.  The
// predicate is always true, the work done here is thus minimal.
type crossPredicate struct{}

func (p *crossPredicate) eval(_, _, _ parser.DTuple) (bool, error) {
	return true, nil
}
func (p *crossPredicate) prepareRow(result, leftRow, rightRow parser.DTuple) {
	prepareRowConcat(result, leftRow, rightRow)
}
func (p *crossPredicate) start() error                        { return nil }
func (p *crossPredicate) expand() error                       { return nil }
func (p *crossPredicate) format(_ *bytes.Buffer)              {}
func (p *crossPredicate) explainTypes(_ func(string, string)) {}

// onPredicate implements the predicate logic for joins with an ON clause.
type onPredicate struct {
	p      *planner
	filter parser.TypedExpr
	info   *dataSourceInfo
	curRow parser.DTuple

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

// eval for onPredicate uses an arbitrary SQL expression to determine
// whether the left and right input row can join.
func (p *onPredicate) eval(result, leftRow, rightRow parser.DTuple) (bool, error) {
	p.curRow = result
	prepareRowConcat(p.curRow, leftRow, rightRow)
	return sqlbase.RunFilter(p.filter, &p.p.evalCtx)
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

// makeOnPredicate constructs a joinPredicate object for joins with a
// ON clause.
func (p *planner) makeOnPredicate(
	left, right *dataSourceInfo, expr parser.Expr,
) (joinPredicate, *dataSourceInfo, error) {
	// Output rows are the concatenation of input rows.
	info, err := concatDataSourceInfos(left, right)
	if err != nil {
		return nil, nil, err
	}

	pred := &onPredicate{
		p:    p,
		info: info,
	}

	// Determine the filter expression.
	colInfo := multiSourceInfo{left, right}
	iVarHelper := parser.MakeIndexedVarHelper(pred, len(info.sourceColumns))
	filter, err := p.analyzeExpr(expr, colInfo, iVarHelper, parser.TypeBool, true, "ON")
	if err != nil {
		return nil, nil, err
	}
	pred.filter = filter

	return pred, info, nil
}

// usingPredicate implements the predicate logic for joins with a USING clause.
type usingPredicate struct {
	// The list of column names given to USING.
	colNames parser.NameList

	// The comparison function to use for each column. We need
	// different functions because each USING column may have a different
	// type (and they may be heterogeneous between left and right).
	usingCmp []func(*parser.EvalContext, parser.Datum, parser.Datum) (parser.DBool, error)
	// evalCtx is needed to evaluate the functions in usingCmp.
	evalCtx *parser.EvalContext

	// left/rightUsingIndices give the position of USING columns
	// on the left and right input row arrays, respectively.
	leftUsingIndices  []int
	rightUsingIndices []int

	// left/rightUsingIndices give the position of non-USING columns on
	// the left and right input row arrays, respectively.
	leftRestIndices  []int
	rightRestIndices []int
}

func (p *usingPredicate) format(buf *bytes.Buffer) {
	buf.WriteString(" USING(")
	p.colNames.Format(buf, parser.FmtSimple)
	buf.WriteByte(')')
}
func (p *usingPredicate) start() error                        { return nil }
func (p *usingPredicate) expand() error                       { return nil }
func (p *usingPredicate) explainTypes(_ func(string, string)) {}

// eval for usingPredicate compares the USING columns, returning true
// if and only if all USING columns are equal on both sides.
func (p *usingPredicate) eval(_, leftRow, rightRow parser.DTuple) (bool, error) {
	eq := true
	for i := range p.colNames {
		leftVal := leftRow[p.leftUsingIndices[i]]
		rightVal := rightRow[p.rightUsingIndices[i]]
		if leftVal == parser.DNull || rightVal == parser.DNull {
			eq = false
			break
		}
		res, err := p.usingCmp[i](p.evalCtx, leftVal, rightVal)
		if err != nil {
			return false, err
		}
		if res != parser.DBool(true) {
			eq = false
			break
		}
	}
	return eq, nil
}

// prepareRow for usingPredicate has more work to do than for ON
// clauses and CROSS JOIN: a result row contains first the values for
// the USING columns; then the non-USING values from the left input
// row, then the non-USING values from the right input row.
func (p *usingPredicate) prepareRow(result, leftRow, rightRow parser.DTuple) {
	d := 0
	for k, j := range p.leftUsingIndices {
		// The result for USING columns must be computed as per COALESCE().
		if leftRow[j] != parser.DNull {
			result[d] = leftRow[j]
		} else {
			result[d] = rightRow[p.rightUsingIndices[k]]
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

// pickUsingColumn searches for a column whose name matches colName.
// The column index and type are returned if found, otherwise an error
// is reported.
func pickUsingColumn(cols ResultColumns, colName string, context string) (int, parser.Type, error) {
	idx := invalidColIdx
	for j, col := range cols {
		if col.hidden {
			continue
		}
		if sqlbase.ReNormalizeName(col.Name) == colName {
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
	left *dataSourceInfo, right *dataSourceInfo, colNames parser.NameList,
) (joinPredicate, *dataSourceInfo, error) {
	cmpOps := make([]func(*parser.EvalContext, parser.Datum, parser.Datum) (parser.DBool, error), len(colNames))
	leftUsingIndices := make([]int, len(colNames))
	rightUsingIndices := make([]int, len(colNames))
	usedLeft := make([]int, len(left.sourceColumns))
	for i := range usedLeft {
		usedLeft[i] = invalidColIdx
	}
	usedRight := make([]int, len(right.sourceColumns))
	for i := range usedRight {
		usedRight[i] = invalidColIdx
	}
	seenNames := make(map[string]struct{})
	columns := make(ResultColumns, 0, len(left.sourceColumns)+len(right.sourceColumns)-len(colNames))

	// Find out which columns are involved in the USING clause.
	for i, unnormalizedColName := range colNames {
		colName := sqlbase.NormalizeName(unnormalizedColName)

		// Check for USING(x,x)
		if _, ok := seenNames[colName]; ok {
			return nil, nil, fmt.Errorf("column %q appears more than once in USING clause", colName)
		}
		seenNames[colName] = struct{}{}

		// Find the column name on the left.
		leftIdx, leftType, err := pickUsingColumn(left.sourceColumns, colName, "left")
		if err != nil {
			return nil, nil, err
		}
		usedLeft[leftIdx] = i

		// Find the column name on the right.
		rightIdx, rightType, err := pickUsingColumn(right.sourceColumns, colName, "right")
		if err != nil {
			return nil, nil, err
		}
		usedRight[rightIdx] = i

		// Remember the indices.
		leftUsingIndices[i] = leftIdx
		rightUsingIndices[i] = rightIdx

		// Memoize the comparison function.
		fn, found := parser.FindEqualComparisonFunction(leftType, rightType)
		if !found {
			return nil, nil, fmt.Errorf("JOIN/USING types %s and %s for column %s cannot be matched",
				leftType, rightType, colName)
		}
		cmpOps[i] = fn

		// Prepare the output column for USING.
		columns = append(columns, left.sourceColumns[leftIdx])
	}

	// Find out which columns are not involved in the USING clause.
	leftRestIndices := make([]int, 0, len(left.sourceColumns)-1)
	for i := range left.sourceColumns {
		if usedLeft[i] == invalidColIdx {
			leftRestIndices = append(leftRestIndices, i)
			usedLeft[i] = len(columns)
			columns = append(columns, left.sourceColumns[i])
		}
	}
	rightRestIndices := make([]int, 0, len(right.sourceColumns)-1)
	for i := range right.sourceColumns {
		if usedRight[i] == invalidColIdx {
			rightRestIndices = append(rightRestIndices, i)
			usedRight[i] = len(columns)
			columns = append(columns, right.sourceColumns[i])
		}
	}

	// Merge the mappings from table aliases to column sets from both
	// sides into a new alias-columnset mapping for the result rows.
	aliases := make(sourceAliases)
	for alias, colRange := range left.sourceAliases {
		newRange := make([]int, len(colRange))
		for i, colIdx := range colRange {
			newRange[i] = usedLeft[colIdx]
		}
		aliases[alias] = newRange
	}
	for alias, colRange := range right.sourceAliases {
		newRange := make([]int, len(colRange))
		for i, colIdx := range colRange {
			newRange[i] = usedRight[colIdx]
		}
		aliases[alias] = newRange
	}

	info := &dataSourceInfo{
		sourceColumns: columns,
		sourceAliases: aliases,
	}

	return &usingPredicate{
		evalCtx:           &p.evalCtx,
		colNames:          colNames,
		usingCmp:          cmpOps,
		leftUsingIndices:  leftUsingIndices,
		rightUsingIndices: rightUsingIndices,
		leftRestIndices:   leftRestIndices,
		rightRestIndices:  rightRestIndices,
	}, info, nil
}

// commonColumns returns the names of columns common on the
// right and left sides, for use by NATURAL JOIN.
func commonColumns(left, right *dataSourceInfo) parser.NameList {
	var res parser.NameList
	for _, cLeft := range left.sourceColumns {
		if cLeft.hidden {
			continue
		}
		for _, cRight := range right.sourceColumns {
			if cRight.hidden {
				continue
			}

			if sqlbase.ReNormalizeName(cLeft.Name) == sqlbase.ReNormalizeName(cRight.Name) {
				res = append(res, parser.Name(cLeft.Name))
			}
		}
	}
	return res
}

// makeJoin constructs a planDataSource for a JOIN node.
// The tableInfo field from the left node is taken over (overwritten)
// by the new node.
func (p *planner) makeJoin(
	astJoinType string, left planDataSource, right planDataSource, cond parser.JoinCond,
) (planDataSource, error) {
	leftInfo, rightInfo := left.info, right.info

	swapped := false
	var typ joinType
	switch astJoinType {
	case "JOIN", "INNER JOIN", "CROSS JOIN":
		typ = joinTypeInner
	case "LEFT JOIN":
		typ = joinTypeOuterLeft
	case "RIGHT JOIN":
		left, right = right, left // swap
		typ = joinTypeOuterLeft
		swapped = true
	case "FULL JOIN":
		typ = joinTypeOuterFull
	default:
		return planDataSource{}, errors.Errorf("unsupported JOIN type %T", astJoinType)
	}

	// Check that the same table name is not used on both sides.
	for alias := range right.info.sourceAliases {
		if _, ok := left.info.sourceAliases[alias]; ok {
			t := alias.Table()
			if t == "" {
				return planDataSource{}, errors.New(
					"cannot join columns from multiple anonymous sources (missing AS clause)")
			}
			return planDataSource{}, fmt.Errorf(
				"cannot join columns from the same source name %q (missing AS clause)", t)
		}
	}

	var info *dataSourceInfo
	var pred joinPredicate
	var err error
	if cond == nil {
		pred = &crossPredicate{}
		info, err = concatDataSourceInfos(leftInfo, rightInfo)
	} else {
		switch t := cond.(type) {
		case *parser.OnJoinCond:
			pred, info, err = p.makeOnPredicate(leftInfo, rightInfo, t.Expr)
		case parser.NaturalJoinCond:
			cols := commonColumns(leftInfo, rightInfo)
			pred, info, err = p.makeUsingPredicate(leftInfo, rightInfo, cols)
		case *parser.UsingJoinCond:
			pred, info, err = p.makeUsingPredicate(leftInfo, rightInfo, t.Cols)
		default:
			err = util.UnimplementedWithIssueErrorf(2970, "unsupported JOIN predicate: %T", cond)
		}
	}
	if err != nil {
		return planDataSource{}, err
	}

	return planDataSource{
		info: info,
		plan: &joinNode{
			joinType:  typ,
			left:      left,
			right:     right,
			pred:      pred,
			columns:   info.sourceColumns,
			swapped:   swapped,
			rightRows: p.newContainerValuesNode(right.plan.Columns(), 0),
		},
	}, nil
}

// ExplainTypes implements the planNode interface.
func (n *joinNode) ExplainTypes(regTypes func(string, string)) {
	n.pred.explainTypes(regTypes)
}

// SetLimitHint implements the planNode interface.
func (n *joinNode) SetLimitHint(numRows int64, soft bool) {}

// expandPlan implements the planNode interface.
func (n *joinNode) expandPlan() error {
	if err := n.pred.expand(); err != nil {
		return err
	}
	if err := n.left.plan.expandPlan(); err != nil {
		return err
	}
	return n.right.plan.expandPlan()
}

// ExplainPlan implements the planNode interface.
func (n *joinNode) ExplainPlan(v bool) (name, description string, children []planNode) {
	var buf bytes.Buffer
	switch n.joinType {
	case joinTypeInner:
		jType := "INNER"
		if _, ok := n.pred.(*crossPredicate); ok {
			jType = "CROSS"
		}
		buf.WriteString(jType)
	case joinTypeOuterLeft:
		if !n.swapped {
			buf.WriteString("LEFT OUTER")
		} else {
			buf.WriteString("RIGHT OUTER")
		}
	case joinTypeOuterFull:
		buf.WriteString("FULL OUTER")
	}

	n.pred.format(&buf)

	subplans := []planNode{n.left.plan, n.right.plan}
	if n.swapped {
		subplans[0], subplans[1] = subplans[1], subplans[0]
	}
	if p, ok := n.pred.(*onPredicate); ok {
		subplans = p.p.collectSubqueryPlans(p.filter, subplans)
	}

	return "join", buf.String(), subplans
}

// Columns implements the planNode interface.
func (n *joinNode) Columns() ResultColumns { return n.columns }

// Ordering implements the planNode interface.
func (n *joinNode) Ordering() orderingInfo { return n.left.plan.Ordering() }

// MarkDebug implements the planNode interface.
func (n *joinNode) MarkDebug(mode explainMode) {
	if mode != explainDebug {
		panic(fmt.Sprintf("unknown debug mode %d", mode))
	}
	n.explain = mode
	n.left.plan.MarkDebug(mode)
	n.right.plan.MarkDebug(mode)
}

// Start implements the planNode interface.
func (n *joinNode) Start() error {
	if err := n.pred.start(); err != nil {
		return err
	}

	if err := n.left.plan.Start(); err != nil {
		return err
	}
	if err := n.right.plan.Start(); err != nil {
		return err
	}

	if n.explain != explainDebug {
		// Load all the rows from the right side in memory.
		for {
			hasRow, err := n.right.plan.Next()
			if err != nil {
				return err
			}
			if !hasRow {
				break
			}
			row := n.right.plan.Values()
			newRow := make([]parser.Datum, len(row))
			copy(newRow, row)
			if err := n.rightRows.rows.AddRow(newRow); err != nil {
				return err
			}
		}
		if n.rightRows.Len() == 0 {
			n.rightRows.Close()
			n.rightRows = nil
		}
	}

	// Pre-allocate the space for output rows.
	n.output = make(parser.DTuple, len(n.columns))

	// If needed, pre-allocate a left row of NULL tuples for when the
	// join predicate fails to match.
	if n.joinType == joinTypeOuterLeft || n.joinType == joinTypeOuterFull {
		n.emptyRight = make(parser.DTuple, len(n.right.plan.Columns()))
		for i := range n.emptyRight {
			n.emptyRight[i] = parser.DNull
		}
	}
	// If needed, allocate an array of booleans to remember which
	// right rows have matched.
	if n.joinType == joinTypeOuterFull && n.rightRows != nil {
		n.rightMatched = make([]bool, n.rightRows.rows.Len())
		n.emptyLeft = make(parser.DTuple, len(n.left.plan.Columns()))
		for i := range n.emptyLeft {
			n.emptyLeft[i] = parser.DNull
		}
	}

	return nil
}

func (n *joinNode) debugNext() (bool, error) {
	if !n.doneReadingRight {
		hasRightRow, err := n.right.plan.Next()
		if err != nil {
			return false, err
		}
		if hasRightRow {
			return true, nil
		}
		n.doneReadingRight = true
	}

	return n.left.plan.Next()
}

// Next implements the planNode interface.
func (n *joinNode) Next() (bool, error) {
	if n.explain == explainDebug {
		return n.debugNext()
	}

	var leftRow, rightRow parser.DTuple
	var nRightRows int

	if n.rightRows == nil {
		if n.joinType != joinTypeOuterLeft && n.joinType != joinTypeOuterFull {
			// No rows on right; don't even try.
			return false, nil
		}
		nRightRows = 0
	} else {
		nRightRows = n.rightRows.Len()
	}

	// We fetch one row at a time until we find one that passes the filter.
	for {
		curRightIdx := n.rightIdx

		if curRightIdx < 0 {
			// Going through the remaining right row of a full outer join.
			curRightIdx = (-curRightIdx) - 1
			n.rightIdx--
			if curRightIdx < nRightRows {
				if n.rightMatched[curRightIdx] {
					continue
				}
				leftRow = n.emptyLeft
				rightRow = n.rightRows.rows.At(curRightIdx)
				break
			} else {
				// Both right and left exhausted.
				return false, nil
			}
		}

		if curRightIdx == 0 {
			leftHasRow, err := n.left.plan.Next()
			if err != nil {
				return false, err
			}

			if !leftHasRow && n.rightMatched != nil {
				// Go through the remaining right rows.
				n.left.plan.Close()
				n.rightIdx = -1
				continue
			}

			if !leftHasRow {
				// Both left and right are exhausted; done.
				return false, nil
			}
		}

		leftRow = n.left.plan.Values()

		if curRightIdx >= nRightRows {
			n.rightIdx = 0
			if (n.joinType == joinTypeOuterLeft || n.joinType == joinTypeOuterFull) && !n.passedFilter {
				// If nothing was emitted in the previous batch of right rows,
				// insert a tuple of NULLs on the right.
				rightRow = n.emptyRight
				if n.swapped {
					leftRow, rightRow = rightRow, leftRow
				}
				break
			}
			n.passedFilter = false
			continue
		}

		emptyRight := false
		if nRightRows > 0 {
			rightRow = n.rightRows.rows.At(curRightIdx)
			n.rightIdx = curRightIdx + 1
		} else {
			emptyRight = true
			rightRow = n.emptyRight
		}

		if n.swapped {
			leftRow, rightRow = rightRow, leftRow
		}
		passesFilter, err := n.pred.eval(n.output, leftRow, rightRow)
		if err != nil {
			return false, err
		}

		if passesFilter {
			n.passedFilter = true
			if n.rightMatched != nil && !emptyRight {
				// FULL OUTER JOIN, mark the rows as matched.
				n.rightMatched[curRightIdx] = true
			}
			break
		}
	}

	// Got a row from both right and left; prepareRow the result.
	n.pred.prepareRow(n.output, leftRow, rightRow)
	return true, nil
}

// Values implements the planNode interface.
func (n *joinNode) Values() parser.DTuple {
	return n.output
}

// DebugValues implements the planNode interface.
func (n *joinNode) DebugValues() debugValues {
	var res debugValues
	if !n.doneReadingRight {
		res = n.right.plan.DebugValues()
	} else {
		res = n.left.plan.DebugValues()
	}
	if res.output == debugValueRow {
		res.output = debugValueBuffered
	}
	return res
}

// Close implements the planNode interface.
func (n *joinNode) Close() {
	if n.rightRows != nil {
		n.rightRows.Close()
		n.rightRows = nil
	}
	n.rightMatched = nil
	n.right.plan.Close()
	n.left.plan.Close()
}

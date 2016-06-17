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

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util"
)

type joinType int

const (
	joinTypeInner joinType = iota
	joinTypeOuterLeft
)

// joinNode is a planNode whose rows are the result of an inner or
// left/right outer join.
type joinNode struct {
	p        *planner
	joinType joinType

	// The data sources.
	left    planNode
	right   planNode
	swapped bool

	// pred represents the join predicate.
	pred joinPredicate

	// columns contains the metadata for the results of this node.
	columns []ResultColumn

	// output contains the last generated row of results from this node.
	output parser.DTuple

	// rightRows contains a copy of all rows from the data source on the
	// right of the join.
	rightRows *valuesNode

	// rightIdx indicates the current right row.
	rightIdx int

	// passedFilter turns to true when the current left row has matched
	// at least one right row.
	passedFilter bool

	// emptyRight contain tuples of NULL values to use on the
	// right for outer joins when the filter fails.
	emptyRight parser.DTuple
	nRightCols int
}

type joinPredicate interface {
	// eval tests whether the current combination of rows passes the
	// predicate.
	eval(leftRow parser.DTuple, rightRow parser.DTuple) (bool, error)

	// cook prepares the output row.
	cook(result parser.DTuple, leftRow parser.DTuple, rightRow parser.DTuple)

	// expand and start propagate to embedded sub-queries.
	expand() error
	start() error

	// format pretty-prints the predicate for EXPLAIN.
	format(buf *bytes.Buffer)
	// explainTypes registers the expression types for EXPLAIN.
	explainTypes(f func(string, string))
}

var _ joinPredicate = &onPredicate{}
var _ joinPredicate = crossPredicate{}

func cookConcat(result parser.DTuple, leftRow parser.DTuple, rightRow parser.DTuple) {
	copy(result, leftRow)
	copy(result[len(leftRow):], rightRow)
}

type crossPredicate struct{}

func (p crossPredicate) eval(_, _ parser.DTuple) (bool, error) {
	return true, nil
}
func (p crossPredicate) cook(result parser.DTuple, leftRow parser.DTuple, rightRow parser.DTuple) {
	cookConcat(result, leftRow, rightRow)
}
func (p crossPredicate) start() error                        { return nil }
func (p crossPredicate) expand() error                       { return nil }
func (p crossPredicate) format(_ *bytes.Buffer)              {}
func (p crossPredicate) explainTypes(_ func(string, string)) {}

type onPredicate struct {
	p *planner

	// leftInfo/rightInfo contain the column metadata for the left and
	// right data sources.
	leftInfo  *dataSourceInfo
	rightInfo *dataSourceInfo

	// qvals is needed to resolve qvalues in the filter expression.
	// TODO(knz) perhaps this is not needed if this node ends up
	// using IndexedVar (like scanNode) instead of qvalues.
	qvals  qvalMap
	filter parser.TypedExpr
}

func (p *onPredicate) eval(leftRow parser.DTuple, rightRow parser.DTuple) (bool, error) {
	p.qvals.populateQVals(p.leftInfo, leftRow)
	p.qvals.populateQVals(p.rightInfo, rightRow)
	return sqlbase.RunFilter(p.filter, &p.p.evalCtx)
}

func (p *onPredicate) cook(result parser.DTuple, leftRow parser.DTuple, rightRow parser.DTuple) {
	cookConcat(result, leftRow, rightRow)
}

func (p *onPredicate) expand() error {
	return p.p.expandSubqueryPlans(p.filter)
}

func (p *onPredicate) start() error {
	return p.p.startSubqueryPlans(p.filter)
}

func (p *onPredicate) format(buf *bytes.Buffer) {
	buf.WriteString(" ON ")
	p.filter.Format(buf, parser.FmtSimple)
}

func (p *onPredicate) explainTypes(regTypes func(string, string)) {
	if p.filter != nil {
		regTypes("filter", parser.AsStringWithFlags(p.filter, parser.FmtShowTypes))
	}
}

type usingPredicate struct {
	evalCtx           *parser.EvalContext
	colNames          parser.NameList
	usingCmp          []func(*parser.EvalContext, parser.Datum, parser.Datum) (parser.DBool, error)
	leftUsingIndices  []int
	rightUsingIndices []int
	leftRestIndices   []int
	rightRestIndices  []int
}

func (p *usingPredicate) format(buf *bytes.Buffer) {
	buf.WriteString(" USING(")
	p.colNames.Format(buf, parser.FmtSimple)
	buf.WriteByte(')')
}
func (p *usingPredicate) start() error                        { return nil }
func (p *usingPredicate) expand() error                       { return nil }
func (p *usingPredicate) explainTypes(_ func(string, string)) {}

func (p *usingPredicate) eval(leftRow parser.DTuple, rightRow parser.DTuple) (bool, error) {
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

func (p *usingPredicate) cook(result parser.DTuple, leftRow parser.DTuple, rightRow parser.DTuple) {
	d := 0
	for _, j := range p.leftUsingIndices {
		result[d] = leftRow[j]
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

func pickUsingColumn(cols []ResultColumn, colName string, context string) (int, parser.Datum, error) {
	idx := invalidColIdx
	for j, col := range cols {
		if sqlbase.NormalizeName(col.Name) == colName {
			idx = j
		}
	}
	if idx == invalidColIdx {
		return idx, nil, fmt.Errorf("column \"%s\" specified in USING clause does not exist in %s table", colName, context)
	}
	return idx, cols[idx].Typ, nil
}

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
	columns := make([]ResultColumn, 0, len(left.sourceColumns)+len(right.sourceColumns)-len(colNames))
	//aliases := make(sourceAliases)

	for i, colName := range colNames {
		colName = sqlbase.NormalizeName(colName)

		// Check for USING(x,x)
		if _, ok := seenNames[colName]; ok {
			return nil, nil, fmt.Errorf("column \"%s\" appears more than once in USING clause", colName)
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
			return nil, nil, fmt.Errorf("JOIN/USING types %s and %s cannot be matched", leftType.Type(), rightType.Type())
		}
		cmpOps[i] = fn

		// Prepare the output column for USING.
		columns = append(columns, left.sourceColumns[leftIdx])
	}

	leftRestIndices := make([]int, 0, len(left.sourceColumns)-1)
	rightRestIndices := make([]int, 0, len(right.sourceColumns)-1)

	for i := range left.sourceColumns {
		if usedLeft[i] == invalidColIdx {
			leftRestIndices = append(leftRestIndices, i)
			usedLeft[i] = len(columns)
			columns = append(columns, left.sourceColumns[i])
		}
	}

	for i := range right.sourceColumns {
		if usedRight[i] == invalidColIdx {
			rightRestIndices = append(rightRestIndices, i)
			usedRight[i] = len(columns)
			columns = append(columns, right.sourceColumns[i])
		}
	}

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

	// FIXME
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

// makeJoin constructs a planDataSource for a JOIN node.
// The tableInfo field from the left node is taken over (overwritten)
// by the new node.
func (p *planner) makeJoin(
	astJoinType string,
	left planDataSource,
	right planDataSource,
	cond parser.JoinCond,
) (planDataSource, error) {
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
	default:
		return planDataSource{}, util.UnimplementedWithIssueErrorf(2970, "unsupported JOIN type %s", astJoinType)
	}

	resInfoLeft, resInfoRight := left.info, right.info
	if swapped {
		resInfoLeft, resInfoRight = resInfoRight, resInfoLeft
	}

	// Check that the same table name is not used on both sides.
	for alias := range right.info.sourceAliases {
		if _, ok := left.info.sourceAliases[alias]; ok {
			return planDataSource{}, fmt.Errorf("table name \"%s\" specified more than once", alias)
		}
	}

	var info *dataSourceInfo
	var pred joinPredicate
	var err error
	if cond == nil {
		pred = crossPredicate{}
		info, err = concatDataSourceInfos(resInfoLeft, resInfoRight)
		if err != nil {
			return planDataSource{}, err
		}
	} else {
		switch t := cond.(type) {
		case *parser.OnJoinCond:
			info, err = concatDataSourceInfos(resInfoLeft, resInfoRight)
			if err != nil {
				return planDataSource{}, err
			}

			// Determine the filter expression.
			qvals := make(qvalMap)
			colInfo := multiSourceInfo{left.info, right.info}
			filter, err := p.analyzeExpr(t.Expr, colInfo, qvals, parser.TypeBool, true, "ON")
			if err != nil {
				return planDataSource{}, err
			}
			pred = &onPredicate{
				p:         p,
				leftInfo:  left.info,
				rightInfo: right.info,
				qvals:     qvals,
				filter:    filter,
			}
		case *parser.UsingJoinCond:
			pred, info, err = p.makeUsingPredicate(left.info, right.info, t.Cols)
			if err != nil {
				return planDataSource{}, err
			}
		default:
			return planDataSource{}, util.UnimplementedWithIssueErrorf(2970, "unsupported JOIN predicate: %T", cond)
		}
	}

	return planDataSource{
		info: info,
		plan: &joinNode{
			joinType: typ,
			left:     left.plan,
			right:    right.plan,
			pred:     pred,
			columns:  info.sourceColumns,
			swapped:  swapped,
		},
	}, nil
}

// transformUsing changes a USING(col0, col1...) clause into (a.col0 =
// b.col0 AND a.col1 = b.col1 AND ...).
func transformUsing(left, right *dataSourceInfo, qvals qvalMap, cols []string) (parser.Expr, error) {
	var cond parser.Expr
	for i, colName := range cols {
		leftIdx, err := left.findColumnByName(colName, "left table")
		if err != nil {
			return nil, err
		}
		rightIdx, err := right.findColumnByName(colName, "right table")
		if err != nil {
			return nil, err
		}
		leftQ := qvals.getQVal(columnRef{source: left, colIdx: leftIdx})
		rightQ := qvals.getQVal(columnRef{source: right, colIdx: rightIdx})
		compare := &parser.ComparisonExpr{Operator: parser.EQ, Left: leftQ, Right: rightQ}
		if i == 0 {
			cond = compare
		} else {
			cond = &parser.AndExpr{Left: compare, Right: cond}
		}
	}
	return cond, nil
}

func (n *joinNode) ExplainTypes(regTypes func(string, string)) {
	n.pred.explainTypes(regTypes)
}

// SetLimitHint implements the planNode interface.
func (n *joinNode) SetLimitHint(numRows int64, soft bool) {
	n.left.SetLimitHint(numRows, soft)
}

// expandPlan implements the planNode interface.
func (n *joinNode) expandPlan() error {
	if err := n.pred.expand(); err != nil {
		return err
	}
	if err := n.left.expandPlan(); err != nil {
		return err
	}
	return n.right.expandPlan()
}

// ExplainPlan implements the planNode interface.
func (n *joinNode) ExplainPlan(v bool) (name, description string, children []planNode) {
	var buf bytes.Buffer
	switch n.joinType {
	case joinTypeInner:
		jType := "INNER"
		if _, ok := n.pred.(crossPredicate); ok {
			jType = "CROSS"
		}
		buf.WriteString(jType)
	case joinTypeOuterLeft:
		if !n.swapped {
			buf.WriteString("OUTER LEFT")
		} else {
			buf.WriteString("OUTER RIGHT")
		}
	}

	n.pred.format(&buf)

	subplans := []planNode{n.left, n.right}
	if n.swapped {
		subplans[0], subplans[1] = subplans[1], subplans[0]
	}
	if p, ok := n.pred.(*onPredicate); ok {
		subplans = p.p.collectSubqueryPlans(p.filter, subplans)
	}

	return "join", buf.String(), subplans
}

// Columns implements the planNode interface.
func (n *joinNode) Columns() []ResultColumn { return n.columns }

// Ordering implements the planNode interface.
func (n *joinNode) Ordering() orderingInfo { return n.left.Ordering() }

// MarkDebug implements the planNode interface.
func (n *joinNode) MarkDebug(mode explainMode) {
	n.left.MarkDebug(mode)
	n.right.MarkDebug(mode)
}

// Start implements the planNode interface.
func (n *joinNode) Start() error {
	if err := n.pred.start(); err != nil {
		return err
	}

	if err := n.left.Start(); err != nil {
		return err
	}
	if err := n.right.Start(); err != nil {
		return err
	}

	v := &valuesNode{}
	for {
		hasRow, err := n.right.Next()
		if err != nil {
			return err
		}
		if !hasRow {
			break
		}

		row := n.right.Values()
		newRow := make([]parser.Datum, len(row))
		copy(newRow, row)
		v.rows = append(v.rows, newRow)
	}
	if len(v.rows) > 0 {
		n.rightRows = v
	}
	n.output = make(parser.DTuple, len(n.columns))

	if n.joinType == joinTypeOuterLeft {
		n.emptyRight = make(parser.DTuple, len(n.right.Columns()))
		for i := range n.emptyRight {
			n.emptyRight[i] = parser.DNull
		}
	}

	return nil
}

// Next implements the planNode interface.
func (n *joinNode) Next() (bool, error) {
	var leftRow, rightRow parser.DTuple
	var nRightRows int

	if n.rightRows == nil {
		if n.joinType != joinTypeOuterLeft {
			// No rows on right; don't even try.
			return false, nil
		}
		nRightRows = 0
	} else {
		nRightRows = len(n.rightRows.rows)
	}

	// We fetch one row at a time until we find one that passes the filter.
	for {
		if n.rightIdx == 0 {
			leftHasRow, err := n.left.Next()
			if err != nil {
				return false, err
			}
			if !leftHasRow {
				// Both left and right are exhausted; done.
				return false, nil
			}
		}

		leftRow = n.left.Values()

		if n.rightIdx >= nRightRows {
			n.rightIdx = 0
			if n.joinType == joinTypeOuterLeft && !n.passedFilter {
				// If nothing was emitted in the previous batch of right rows,
				// insert a tuple of NULLs on the right.
				rightRow = n.emptyRight
				break
			}
			n.passedFilter = false
			continue
		}

		if nRightRows > 0 {
			rightRow = n.rightRows.rows[n.rightIdx]
			n.rightIdx = n.rightIdx + 1
		} else {
			rightRow = n.emptyRight
		}

		passesFilter, err := n.pred.eval(leftRow, rightRow)
		if err != nil {
			return false, err
		}

		if passesFilter {
			n.passedFilter = true
			break
		}
	}

	// Got a row from both right and left; cook the result.
	if !n.swapped {
		n.pred.cook(n.output, leftRow, rightRow)
	} else {
		n.pred.cook(n.output, rightRow, leftRow)
	}
	return true, nil
}

// Values implements the planNode interface.
func (n *joinNode) Values() parser.DTuple {
	return n.output
}

// DebugValues implements the planNode interface.
func (n *joinNode) DebugValues() debugValues {
	// FIXME: what to do here?
	return debugValues{}
}

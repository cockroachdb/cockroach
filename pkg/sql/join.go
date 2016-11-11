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

			if parser.ReNormalizeName(cLeft.Name) == parser.ReNormalizeName(cRight.Name) {
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
			if err := n.rightRows.rows.AddRow(row); err != nil {
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

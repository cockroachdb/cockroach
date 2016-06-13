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
)

type joinType int

const (
	joinTypeCross = iota
	joinTypeInner
	joinTypeOuterLeft
	joinTypeOuterRight
	joinTypeOuterFull
)

// joinNode blah
type joinNode struct {
	joinType joinType
	left     planNode
	right    planNode
	cond     parser.JoinCond
	columns  []ResultColumn

	leftRow   parser.DTuple
	rightRows *valuesNode
	rightIdx  int
	output    parser.DTuple
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
	var typ joinType
	switch astJoinType {
	case "JOIN", "INNER JOIN":
		typ = joinTypeInner
	case "FULL JOIN":
		typ = joinTypeOuterFull
	case "LEFT JOIN":
		typ = joinTypeOuterLeft
	case "RIGHT JOIN":
		typ = joinTypeOuterRight
	case "CROSS JOIN":
		typ = joinTypeCross
	default:
		return planDataSource{}, fmt.Errorf("%s is not supported", astJoinType)
	}

	info, err := concatDataSourceInfos(left.info, right.info)
	if err != nil {
		return planDataSource{}, err
	}

	return planDataSource{
		info: info,
		plan: &joinNode{
			joinType: typ,
			left:     left.plan,
			right:    right.plan,
			cond:     cond,
			columns:  info.sourceColumns,
		},
	}, nil
}

func (n *joinNode) ExplainTypes(_ func(string, string)) {
	// FIXME
}

func (n *joinNode) SetLimitHint(numRows int64, soft bool) {
	n.left.SetLimitHint(numRows, soft)
}

func (n *joinNode) expandPlan() error {
	if err := n.left.expandPlan(); err != nil {
		return err
	}
	return n.right.expandPlan()
}

func (n *joinNode) ExplainPlan(v bool) (name, description string, children []planNode) {
	var buf bytes.Buffer

	switch n.joinType {
	case joinTypeCross:
		buf.WriteString("cross")
	case joinTypeInner:
		buf.WriteString("inner")
	case joinTypeOuterLeft:
		buf.WriteString("outer-left")
	case joinTypeOuterRight:
		buf.WriteString("outer-right")
	case joinTypeOuterFull:
		buf.WriteString("outer-full")
	}
	if n.cond != nil {
		buf.WriteByte(' ')
		n.cond.Format(&buf, parser.FmtSimple)
	}
	return "join", buf.String(), []planNode{n.left, n.right}
}

func (n *joinNode) Columns() []ResultColumn { return n.columns }

func (n *joinNode) Ordering() orderingInfo { return n.left.Ordering() }

func (n *joinNode) MarkDebug(mode explainMode) {
	n.left.MarkDebug(mode)
	n.right.MarkDebug(mode)
}

func (n *joinNode) Start() error {
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
	return nil
}

func (n *joinNode) Next() (bool, error) {
	if n.rightRows == nil {
		// No rows on right; don't even try.
		return false, nil
	}

	if n.rightIdx == 0 {
		leftHasRow, err := n.left.Next()
		if err != nil {
			return false, err
		}
		if !leftHasRow {
			return false, nil
		}
		n.leftRow = n.left.Values()
	}

	copy(n.output, n.leftRow)
	copy(n.output[len(n.leftRow):], n.rightRows.rows[n.rightIdx])
	n.rightIdx = (n.rightIdx + 1) % len(n.rightRows.rows)
	return true, nil
}

func (n *joinNode) Values() parser.DTuple {
	return n.output
}

func (n *joinNode) DebugValues() debugValues {
	// FIXME
	return debugValues{}
}

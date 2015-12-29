// Copyright 2015 The Cockroach Authors.
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
// Author: Peter Mattis (peter@cockroachlabs.com)

// This code was derived from https://github.com/youtube/vitess.
//
// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file

package parser

import (
	"bytes"
	"fmt"
	"strings"
)

// SelectStatement any SELECT statement.
type SelectStatement interface {
	Statement
	selectStatement()
}

func (*ParenSelect) selectStatement() {}
func (*Select) selectStatement()      {}
func (*Union) selectStatement()       {}
func (Values) selectStatement()       {}

// ParenSelect represents a parenthesized SELECT/UNION/VALUES statement.
type ParenSelect struct {
	Select SelectStatement
}

func (node *ParenSelect) String() string {
	return fmt.Sprintf("(%s)", node.Select)
}

// Select represents a SELECT statement.
type Select struct {
	Distinct    bool
	Exprs       SelectExprs
	From        TableExprs
	Where       *Where
	GroupBy     GroupBy
	Having      *Where
	OrderBy     OrderBy
	Limit       *Limit
	Lock        string
	tableSelect bool
}

func (node *Select) String() string {
	if node.tableSelect && len(node.From) == 1 {
		return fmt.Sprintf("TABLE %s", node.From[0])
	}
	var buf bytes.Buffer
	buf.WriteString("SELECT")
	if node.Distinct {
		buf.WriteString(" DISTINCT")
	}
	if len(node.Exprs) > 0 {
		buf.WriteString(" " + node.Exprs.String())
	}
	if len(node.From) > 0 {
		// log.Infof("writing: %s", node.From.String())
		buf.WriteString(" " + node.From.String())
	}
	if node.Where != nil {
		// log.Infof("writing: %s", node.Where.String())
		buf.WriteString(" " + node.Where.String())
	}
	if len(node.GroupBy) > 0 {
		buf.WriteString(" " + node.GroupBy.String())
	}
	if node.Having != nil {
		buf.WriteString(" " + node.Having.String())
	}
	if len(node.OrderBy) > 0 {
		buf.WriteString(" " + node.OrderBy.String())
	}
	if node.Limit != nil {
		buf.WriteString(" " + node.Limit.String())
	}
	if len(node.Lock) > 0 {
		buf.WriteString(" " + node.Lock)
	}
	return buf.String()
}

// SelectExprs represents SELECT expressions.
type SelectExprs []SelectExpr

func (node SelectExprs) String() string {
	strs := make([]string, len(node))
	for i := range node {
		strs[i] = node[i].String()
	}

	return strings.Join(strs, ", ")
}

// SelectExpr represents a SELECT expression.
type SelectExpr struct {
	Expr Expr
	As   Name
}

// starSelectExpr is a convenience function that represents an unqualified "*"
// in a select expression.
func starSelectExpr() SelectExpr {
	return SelectExpr{Expr: StarExpr()}
}

func (node SelectExpr) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s", node.Expr)
	if node.As != "" {
		fmt.Fprintf(&buf, " AS %s", node.As)
	}
	return buf.String()
}

// TableExprs represents a list of table expressions.
type TableExprs []TableExpr

func (node TableExprs) String() string {
	strs := make([]string, len(node))
	for i := range node {
		strs[i] = node[i].String()
	}

	return "FROM " + strings.Join(strs, ", ")
}

// TableExpr represents a table expression.
type TableExpr interface {
	fmt.Stringer
	tableExpr()
}

func (*AliasedTableExpr) tableExpr() {}
func (*ParenTableExpr) tableExpr()   {}
func (*JoinTableExpr) tableExpr()    {}

// AliasedTableExpr represents a table expression coupled with an optional
// alias.
type AliasedTableExpr struct {
	Expr SimpleTableExpr
	As   Name
}

func (node *AliasedTableExpr) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s", node.Expr)
	if node.As != "" {
		fmt.Fprintf(&buf, " AS %s", node.As)
	}
	return buf.String()
}

// SimpleTableExpr represents a simple table expression.
type SimpleTableExpr interface {
	simpleTableExpr()
}

func (QualifiedName) simpleTableExpr() {}
func (*Subquery) simpleTableExpr()     {}

// ParenTableExpr represents a parenthesized TableExpr.
type ParenTableExpr struct {
	Expr TableExpr
}

func (node *ParenTableExpr) String() string {
	return fmt.Sprintf("(%s)", node.Expr)
}

// JoinTableExpr represents a TableExpr that's a JOIN operation.
type JoinTableExpr struct {
	Join  string
	Left  TableExpr
	Right TableExpr
	Cond  JoinCond
}

// JoinTableExpr.Join
const (
	astJoin        = "JOIN"
	astFullJoin    = "FULL JOIN"
	astLeftJoin    = "LEFT JOIN"
	astRightJoin   = "RIGHT JOIN"
	astCrossJoin   = "CROSS JOIN"
	astNaturalJoin = "NATURAL JOIN"
	astInnerJoin   = "INNER JOIN"
)

func (node *JoinTableExpr) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s %s %s", node.Left, node.Join, node.Right)
	if node.Cond != nil {
		fmt.Fprintf(&buf, " %s", node.Cond)
	}
	return buf.String()
}

// JoinCond represents a join condition.
type JoinCond interface {
	joinCond()
}

func (*OnJoinCond) joinCond()    {}
func (*UsingJoinCond) joinCond() {}

// OnJoinCond represents an ON join condition.
type OnJoinCond struct {
	Expr Expr
}

func (node *OnJoinCond) String() string {
	return fmt.Sprintf("ON %s", node.Expr)
}

// UsingJoinCond represents a USING join condition.
type UsingJoinCond struct {
	Cols NameList
}

func (node *UsingJoinCond) String() string {
	return fmt.Sprintf("USING (%s)", node.Cols)
}

// Where represents a WHERE or HAVING clause.
type Where struct {
	Type string
	Expr Expr
}

// Where.Type
const (
	astWhere  = "WHERE"
	astHaving = "HAVING"
)

// newWhere creates a WHERE or HAVING clause out of an Expr. If the expression
// is nil, it returns nil.
func newWhere(typ string, expr Expr) *Where {
	if expr == nil {
		return nil
	}
	return &Where{Type: typ, Expr: expr}
}

func (node *Where) String() string {
	return fmt.Sprintf("%s %s", node.Type, node.Expr)
}

// GroupBy represents a GROUP BY clause.
type GroupBy []Expr

func (node GroupBy) String() string {
	strs := make([]string, len(node))
	for i := range node {
		strs[i] = node[i].String()
	}

	return "GROUP BY " + strings.Join(strs, ", ")
}

// OrderBy represents an ORDER By clause.
type OrderBy []*Order

func (node OrderBy) String() string {
	strs := make([]string, len(node))
	for i := range node {
		strs[i] = node[i].String()
	}

	return "ORDER BY " + strings.Join(strs, ", ")
}

// Direction for ordering results.
type Direction int

// Direction values.
const (
	DefaultDirection Direction = iota
	Ascending
	Descending
)

var directionName = [...]string{
	DefaultDirection: "",
	Ascending:        "ASC",
	Descending:       "DESC",
}

func (d Direction) String() string {
	if d < 0 || d > Direction(len(directionName)-1) {
		return fmt.Sprintf("Direction(%d)", d)
	}
	return directionName[d]
}

// Order represents an ordering expression.
type Order struct {
	Expr      Expr
	Direction Direction
}

func (node *Order) String() string {
	if node.Direction == DefaultDirection {
		return node.Expr.String()
	}
	return fmt.Sprintf("%s %s", node.Expr, node.Direction)
}

// Limit represents a LIMIT clause.
type Limit struct {
	Offset, Count Expr
}

func (node *Limit) String() string {
	strs := make([]string, 0, 2)
	if node.Count != nil {
		strs = append(strs, "LIMIT "+node.Count.String())
	}
	if node.Offset != nil {
		strs = append(strs, "OFFSET "+node.Offset.String())
	}
	return strings.Join(strs, " ")
}

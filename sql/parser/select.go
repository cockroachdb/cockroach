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
)

// SelectStatement any SELECT statement.
type SelectStatement interface {
	Statement
	selectStatement()
}

func (*ParenSelect) selectStatement()  {}
func (*SelectClause) selectStatement() {}
func (*UnionClause) selectStatement()  {}
func (*ValuesClause) selectStatement() {}

// Select represents a SelectStatement with an ORDER and/or LIMIT.
type Select struct {
	Select  SelectStatement
	OrderBy OrderBy
	Limit   *Limit
}

func (node *Select) String() string {
	return fmt.Sprintf("%s%s%s", node.Select, node.OrderBy, node.Limit)
}

// ParenSelect represents a parenthesized SELECT/UNION/VALUES statement.
type ParenSelect struct {
	Select *Select
}

func (node *ParenSelect) String() string {
	return fmt.Sprintf("(%s)", node.Select)
}

// SelectClause represents a SELECT statement.
type SelectClause struct {
	Distinct    bool
	Exprs       SelectExprs
	From        TableExprs
	Where       *Where
	GroupBy     GroupBy
	Having      *Where
	Lock        string
	tableSelect bool
}

func (node *SelectClause) String() string {
	if node.tableSelect && len(node.From) == 1 {
		return fmt.Sprintf("TABLE %s", node.From[0])
	}
	var distinct string
	if node.Distinct {
		distinct = " DISTINCT"
	}
	return fmt.Sprintf("SELECT%s%s%s%s%s%s%s",
		distinct, node.Exprs,
		node.From, node.Where,
		node.GroupBy, node.Having, node.Lock)
}

// SelectExprs represents SELECT expressions.
type SelectExprs []SelectExpr

func (node SelectExprs) String() string {
	prefix := " "
	var buf bytes.Buffer
	for _, n := range node {
		fmt.Fprintf(&buf, "%s%s", prefix, n)
		prefix = ", "
	}
	return buf.String()
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

// AliasClause represents an alias, optionally with a column list:
// "AS name" or "AS name(col1, col2)".
type AliasClause struct {
	Alias Name
	Cols  NameList
}

func (a AliasClause) String() string {
	if len(a.Cols) == 0 {
		return a.Alias.String()
	}
	// Build a string of the form "alias (col1, col2, ...)".
	return fmt.Sprintf("%s (%s)", a.Alias, a.Cols)
}

// TableExprs represents a list of table expressions.
type TableExprs []TableExpr

func (node TableExprs) String() string {
	if len(node) == 0 {
		return ""
	}

	var prefix string
	var buf bytes.Buffer
	buf.WriteString(" FROM ")
	for _, n := range node {
		fmt.Fprintf(&buf, "%s%s", prefix, n)
		prefix = ", "
	}
	return buf.String()
}

// TableExpr represents a table expression.
type TableExpr interface {
	tableExpr()
}

func (*AliasedTableExpr) tableExpr() {}
func (*ParenTableExpr) tableExpr()   {}
func (*JoinTableExpr) tableExpr()    {}

// IndexHints represents "@<index_name>" or "@{param[,param]}" where param is
// one of:
//  - FORCE_INDEX=<index_name>
//  - NO_INDEX_JOIN
// It is used optionally after a table name in SELECT statements.
type IndexHints struct {
	Index       Name
	NoIndexJoin bool
}

func (n *IndexHints) String() string {
	if !n.NoIndexJoin {
		return fmt.Sprintf("@%s", n.Index)
	}
	if n.Index == "" {
		return "@{NO_INDEX_JOIN}"
	}
	return fmt.Sprintf("@{FORCE_INDEX=%s,NO_INDEX_JOIN}", n.Index)
}

// AliasedTableExpr represents a table expression coupled with an optional
// alias.
type AliasedTableExpr struct {
	Expr  SimpleTableExpr
	Hints *IndexHints
	As    AliasClause
}

func (node *AliasedTableExpr) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s", node.Expr)
	if node.Hints != nil {
		buf.WriteString(node.Hints.String())
	}
	if node.As.Alias != "" {
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
		fmt.Fprintf(&buf, "%s", node.Cond)
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
	return fmt.Sprintf(" ON %s", node.Expr)
}

// UsingJoinCond represents a USING join condition.
type UsingJoinCond struct {
	Cols NameList
}

func (node *UsingJoinCond) String() string {
	return fmt.Sprintf(" USING (%s)", node.Cols)
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
	if node == nil {
		return ""
	}
	return fmt.Sprintf(" %s %s", node.Type, node.Expr)
}

// GroupBy represents a GROUP BY clause.
type GroupBy []Expr

func (node GroupBy) String() string {
	prefix := " GROUP BY "
	var buf bytes.Buffer
	for _, n := range node {
		fmt.Fprintf(&buf, "%s%s", prefix, n)
		prefix = ", "
	}
	return buf.String()
}

// OrderBy represents an ORDER By clause.
type OrderBy []*Order

func (node OrderBy) String() string {
	prefix := " ORDER BY "
	var buf bytes.Buffer
	for _, n := range node {
		fmt.Fprintf(&buf, "%s%s", prefix, n)
		prefix = ", "
	}
	return buf.String()
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
	if node == nil {
		return ""
	}
	var buf bytes.Buffer
	if node.Count != nil {
		fmt.Fprintf(&buf, " LIMIT %s", node.Count)
	}
	if node.Offset != nil {
		fmt.Fprintf(&buf, " OFFSET %s", node.Offset)
	}
	return buf.String()
}

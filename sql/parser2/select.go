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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

// This code was derived from https://github.com/youtube/vitess.
//
// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file

package parser2

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

func (*Select) selectStatement() {}
func (*Union) selectStatement()  {}
func (Values) selectStatement()  {}

// Select represents a SELECT statement.
type Select struct {
	Distinct string
	Exprs    SelectExprs
	From     TableExprs
	Where    *Where
	GroupBy  GroupBy
	Having   *Where
	OrderBy  OrderBy
	Limit    *Limit
	Lock     string
}

// Select.Distinct
const (
	astDistinct = " DISTINCT"
)

// Select.Lock
const (
	astForUpdate = " FOR UPDATE"
	astShareMode = " LOCK IN SHARE MODE"
)

func (node *Select) String() string {
	return fmt.Sprintf("SELECT%s%v%v%v%v%v%v%v%s",
		node.Distinct, node.Exprs,
		node.From, node.Where,
		node.GroupBy, node.Having, node.OrderBy,
		node.Limit, node.Lock)
}

// SelectExprs represents SELECT expressions.
type SelectExprs []SelectExpr

func (node SelectExprs) String() string {
	prefix := " "
	var buf bytes.Buffer
	for _, n := range node {
		fmt.Fprintf(&buf, "%s%v", prefix, n)
		prefix = ", "
	}
	return buf.String()
}

// SelectExpr represents a SELECT expression.
type SelectExpr interface {
	selectExpr()
}

func (*StarExpr) selectExpr()    {}
func (*NonStarExpr) selectExpr() {}

// StarExpr defines a '*' or 'table.*' expression.
type StarExpr struct {
	TableName string
}

func (node *StarExpr) String() string {
	var buf bytes.Buffer
	if node.TableName != "" {
		fmt.Fprintf(&buf, "%s.", node.TableName)
	}
	fmt.Fprintf(&buf, "*")
	return buf.String()
}

// NonStarExpr defines a non-'*' select expr.
type NonStarExpr struct {
	Expr Expr
	As   string
}

func (node *NonStarExpr) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%v", node.Expr)
	if node.As != "" {
		fmt.Fprintf(&buf, " AS %s", node.As)
	}
	return buf.String()
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
		fmt.Fprintf(&buf, "%s%v", prefix, n)
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

// AliasedTableExpr represents a table expression coupled with an optional
// alias.
type AliasedTableExpr struct {
	Expr SimpleTableExpr
	As   string
}

func (node *AliasedTableExpr) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%v", node.Expr)
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
	return fmt.Sprintf("(%v)", node.Expr)
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
	fmt.Fprintf(&buf, "%v %s %v", node.Left, node.Join, node.Right)
	if node.Cond != nil {
		fmt.Fprintf(&buf, "%v", node.Cond)
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
	return fmt.Sprintf(" ON %v", node.Expr)
}

// UsingJoinCond represents a USING join condition.
type UsingJoinCond struct {
	Cols []string
}

func (node *UsingJoinCond) String() string {
	return fmt.Sprintf(" USING (%s)", strings.Join(node.Cols, ", "))
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

// newWhere creates a WHERE or HAVING clause out of a Expr. If the expression
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
	return fmt.Sprintf(" %s %v", node.Type, node.Expr)
}

// GroupBy represents a GROUP BY clause.
type GroupBy []Expr

func (node GroupBy) String() string {
	prefix := " GROUP BY "
	var buf bytes.Buffer
	for _, n := range node {
		fmt.Fprintf(&buf, "%s%v", prefix, n)
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
		fmt.Fprintf(&buf, "%s%v", prefix, n)
		prefix = ", "
	}
	return buf.String()
}

// Order represents an ordering expression.
type Order struct {
	Expr      Expr
	Direction string
}

// Order.Direction
const (
	astAsc  = " ASC"
	astDesc = " DESC"
)

func (node *Order) String() string {
	return fmt.Sprintf("%v%s", node.Expr, node.Direction)
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
		fmt.Fprintf(&buf, " LIMIT %v", node.Count)
	}
	if node.Offset != nil {
		fmt.Fprintf(&buf, " OFFSET %v", node.Offset)
	}
	return buf.String()
}

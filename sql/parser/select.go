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

// Format implements the NodeFormatter interface.
func (node *Select) Format(buf *bytes.Buffer, f FmtFlags) {
	FormatNode(buf, f, node.Select)
	FormatNode(buf, f, node.OrderBy)
	FormatNode(buf, f, node.Limit)
}

// ParenSelect represents a parenthesized SELECT/UNION/VALUES statement.
type ParenSelect struct {
	Select *Select
}

// Format implements the NodeFormatter interface.
func (node *ParenSelect) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteByte('(')
	FormatNode(buf, f, node.Select)
	buf.WriteByte(')')
}

// SelectClause represents a SELECT statement.
type SelectClause struct {
	Distinct    bool
	Exprs       SelectExprs
	From        *From
	Where       *Where
	GroupBy     GroupBy
	Having      *Where
	Lock        string
	tableSelect bool
}

// Format implements the NodeFormatter interface.
func (node *SelectClause) Format(buf *bytes.Buffer, f FmtFlags) {
	if node.tableSelect {
		buf.WriteString("TABLE ")
		FormatNode(buf, f, node.From.Tables[0])
	} else {
		buf.WriteString("SELECT ")
		if node.Distinct {
			buf.WriteString("DISTINCT ")
		}
		FormatNode(buf, f, node.Exprs)
		FormatNode(buf, f, node.From)
		FormatNode(buf, f, node.Where)
		FormatNode(buf, f, node.GroupBy)
		FormatNode(buf, f, node.Having)
		buf.WriteString(node.Lock)
	}
}

// SelectExprs represents SELECT expressions.
type SelectExprs []SelectExpr

// Format implements the NodeFormatter interface.
func (node SelectExprs) Format(buf *bytes.Buffer, f FmtFlags) {
	for i, n := range node {
		if i > 0 {
			buf.WriteString(", ")
		}
		FormatNode(buf, f, n)
	}
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

// Format implements the NodeFormatter interface.
func (node SelectExpr) Format(buf *bytes.Buffer, f FmtFlags) {
	FormatNode(buf, f, node.Expr)
	if node.As != "" {
		buf.WriteString(" AS ")
		FormatNode(buf, f, node.As)
	}
}

// AliasClause represents an alias, optionally with a column list:
// "AS name" or "AS name(col1, col2)".
type AliasClause struct {
	Alias Name
	Cols  NameList
}

// Format implements the NodeFormatter interface.
func (a AliasClause) Format(buf *bytes.Buffer, f FmtFlags) {
	FormatNode(buf, f, a.Alias)
	if len(a.Cols) != 0 {
		// Format as "alias (col1, col2, ...)".
		buf.WriteString(" (")
		FormatNode(buf, f, a.Cols)
		buf.WriteByte(')')
	}
}

// AsOfClause represents an as of time.
type AsOfClause struct {
	Expr Expr
}

// Format implements the NodeFormatter interface.
func (a AsOfClause) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("AS OF SYSTEM TIME ")
	FormatNode(buf, f, a.Expr)
}

// From represents a FROM clause.
type From struct {
	Tables TableExprs
	AsOf   AsOfClause
}

// Format implements the NodeFormatter interface.
func (node *From) Format(buf *bytes.Buffer, f FmtFlags) {
	FormatNode(buf, f, node.Tables)
	if node.AsOf.Expr != nil {
		buf.WriteByte(' ')
		FormatNode(buf, f, node.AsOf)
	}
}

// TableExprs represents a list of table expressions.
type TableExprs []TableExpr

// Format implements the NodeFormatter interface.
func (node TableExprs) Format(buf *bytes.Buffer, f FmtFlags) {
	if len(node) != 0 {
		buf.WriteString(" FROM ")
		for i, n := range node {
			if i > 0 {
				buf.WriteString(", ")
			}
			FormatNode(buf, f, n)
		}
	}
}

// TableExpr represents a table expression.
type TableExpr interface {
	NodeFormatter
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

// Format implements the NodeFormatter interface.
func (n *IndexHints) Format(buf *bytes.Buffer, f FmtFlags) {
	if !n.NoIndexJoin {
		buf.WriteByte('@')
		FormatNode(buf, f, n.Index)
	} else {
		if n.Index == "" {
			buf.WriteString("@{NO_INDEX_JOIN}")
		} else {
			buf.WriteString("@{FORCE_INDEX=")
			FormatNode(buf, f, n.Index)
			buf.WriteString(",NO_INDEX_JOIN}")
		}
	}
}

// AliasedTableExpr represents a table expression coupled with an optional
// alias.
type AliasedTableExpr struct {
	Expr  TableExpr
	Hints *IndexHints
	As    AliasClause
}

// Format implements the NodeFormatter interface.
func (node *AliasedTableExpr) Format(buf *bytes.Buffer, f FmtFlags) {
	FormatNode(buf, f, node.Expr)
	if node.Hints != nil {
		FormatNode(buf, f, node.Hints)
	}
	if node.As.Alias != "" {
		buf.WriteString(" AS ")
		FormatNode(buf, f, node.As)
	}
}

func (QualifiedName) tableExpr() {}
func (*Subquery) tableExpr()     {}

// ParenTableExpr represents a parenthesized TableExpr.
type ParenTableExpr struct {
	Expr TableExpr
}

// Format implements the NodeFormatter interface.
func (node *ParenTableExpr) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteByte('(')
	FormatNode(buf, f, node.Expr)
	buf.WriteByte(')')
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
	astJoin      = "JOIN"
	astFullJoin  = "FULL JOIN"
	astLeftJoin  = "LEFT JOIN"
	astRightJoin = "RIGHT JOIN"
	astCrossJoin = "CROSS JOIN"
	astInnerJoin = "INNER JOIN"
)

// Format implements the NodeFormatter interface.
func (node *JoinTableExpr) Format(buf *bytes.Buffer, f FmtFlags) {
	FormatNode(buf, f, node.Left)
	buf.WriteByte(' ')
	if _, isNatural := node.Cond.(NaturalJoinCond); isNatural {
		// Natural joins have a different syntax: "<a> NATURAL <join_type> <b>"
		FormatNode(buf, f, node.Cond)
		buf.WriteByte(' ')
		buf.WriteString(node.Join)
		buf.WriteByte(' ')
		FormatNode(buf, f, node.Right)
	} else {
		// General syntax: "<a> <join_type> <b> <condition>"
		buf.WriteString(node.Join)
		buf.WriteByte(' ')
		FormatNode(buf, f, node.Right)
		if node.Cond != nil {
			FormatNode(buf, f, node.Cond)
		}
	}
}

// JoinCond represents a join condition.
type JoinCond interface {
	NodeFormatter
	joinCond()
}

func (NaturalJoinCond) joinCond() {}
func (*OnJoinCond) joinCond()     {}
func (*UsingJoinCond) joinCond()  {}

// NaturalJoinCond represents a NATURAL join condition
type NaturalJoinCond struct{}

// Format implements the NodeFormatter interface.
func (NaturalJoinCond) Format(buf *bytes.Buffer, _ FmtFlags) {
	buf.WriteString("NATURAL")
}

// OnJoinCond represents an ON join condition.
type OnJoinCond struct {
	Expr Expr
}

// Format implements the NodeFormatter interface.
func (node *OnJoinCond) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString(" ON ")
	FormatNode(buf, f, node.Expr)
}

// UsingJoinCond represents a USING join condition.
type UsingJoinCond struct {
	Cols NameList
}

// Format implements the NodeFormatter interface.
func (node *UsingJoinCond) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString(" USING (")
	FormatNode(buf, f, node.Cols)
	buf.WriteByte(')')
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

// Format implements the NodeFormatter interface.
func (node *Where) Format(buf *bytes.Buffer, f FmtFlags) {
	if node != nil {
		buf.WriteByte(' ')
		buf.WriteString(node.Type)
		buf.WriteByte(' ')
		FormatNode(buf, f, node.Expr)
	}
}

// GroupBy represents a GROUP BY clause.
type GroupBy []Expr

// Format implements the NodeFormatter interface.
func (node GroupBy) Format(buf *bytes.Buffer, f FmtFlags) {
	prefix := " GROUP BY "
	for _, n := range node {
		buf.WriteString(prefix)
		FormatNode(buf, f, n)
		prefix = ", "
	}
}

// OrderBy represents an ORDER By clause.
type OrderBy []*Order

// Format implements the NodeFormatter interface.
func (node OrderBy) Format(buf *bytes.Buffer, f FmtFlags) {
	prefix := " ORDER BY "
	for _, n := range node {
		buf.WriteString(prefix)
		FormatNode(buf, f, n)
		prefix = ", "
	}
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

// Format implements the NodeFormatter interface.
func (node *Order) Format(buf *bytes.Buffer, f FmtFlags) {
	FormatNode(buf, f, node.Expr)
	if node.Direction != DefaultDirection {
		buf.WriteByte(' ')
		buf.WriteString(node.Direction.String())
	}
}

// Limit represents a LIMIT clause.
type Limit struct {
	Offset, Count Expr
}

// Format implements the NodeFormatter interface.
func (node *Limit) Format(buf *bytes.Buffer, f FmtFlags) {
	if node != nil {
		if node.Count != nil {
			buf.WriteString(" LIMIT ")
			FormatNode(buf, f, node.Count)
		}
		if node.Offset != nil {
			buf.WriteString(" OFFSET ")
			FormatNode(buf, f, node.Offset)
		}
	}
}

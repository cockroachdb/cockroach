// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-vitess.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
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

// This code was derived from https://github.com/youtube/vitess.

package tree

import (
	"fmt"
)

// SelectStatement represents any SELECT statement.
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
	With    *With
	Select  SelectStatement
	OrderBy OrderBy
	Limit   *Limit
}

// Format implements the NodeFormatter interface.
func (node *Select) Format(ctx *FmtCtx) {
	ctx.FormatNode(node.With)
	ctx.FormatNode(node.Select)
	ctx.FormatNode(&node.OrderBy)
	ctx.FormatNode(node.Limit)
}

// ParenSelect represents a parenthesized SELECT/UNION/VALUES statement.
type ParenSelect struct {
	Select *Select
}

// Format implements the NodeFormatter interface.
func (node *ParenSelect) Format(ctx *FmtCtx) {
	ctx.WriteByte('(')
	ctx.FormatNode(node.Select)
	ctx.WriteByte(')')
}

// SelectClause represents a SELECT statement.
type SelectClause struct {
	Distinct    bool
	DistinctOn  DistinctOn
	Exprs       SelectExprs
	From        *From
	Where       *Where
	GroupBy     GroupBy
	Having      *Where
	Window      Window
	TableSelect bool
}

// Format implements the NodeFormatter interface.
func (node *SelectClause) Format(ctx *FmtCtx) {
	if node.TableSelect {
		ctx.WriteString("TABLE ")
		ctx.FormatNode(node.From.Tables[0])
	} else {
		ctx.WriteString("SELECT ")
		if node.Distinct {
			ctx.WriteString("DISTINCT ")
		}
		ctx.FormatNode(&node.Exprs)
		ctx.FormatNode(node.From)
		ctx.FormatNode(node.Where)
		ctx.FormatNode(&node.GroupBy)
		ctx.FormatNode(node.Having)
		ctx.FormatNode(&node.Window)
	}
}

// SelectExprs represents SELECT expressions.
type SelectExprs []SelectExpr

// Format implements the NodeFormatter interface.
func (node *SelectExprs) Format(ctx *FmtCtx) {
	for i := range *node {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&(*node)[i])
	}
}

// SelectExpr represents a SELECT expression.
type SelectExpr struct {
	Expr Expr
	As   UnrestrictedName
}

// NormalizeTopLevelVarName preemptively expands any UnresolvedName at
// the top level of the expression into a VarName. This is meant
// to catch stars so that sql.checkRenderStar() can see it prior to
// other expression transformations.
func (node *SelectExpr) NormalizeTopLevelVarName() error {
	if vBase, ok := node.Expr.(VarName); ok {
		v, err := vBase.NormalizeVarName()
		if err != nil {
			return err
		}
		node.Expr = v
	}
	return nil
}

// StarSelectExpr is a convenience function that represents an unqualified "*"
// in a select expression.
func StarSelectExpr() SelectExpr {
	return SelectExpr{Expr: StarExpr()}
}

// Format implements the NodeFormatter interface.
func (node *SelectExpr) Format(ctx *FmtCtx) {
	ctx.FormatNode(node.Expr)
	if node.As != "" {
		ctx.WriteString(" AS ")
		ctx.FormatNode(&node.As)
	}
}

// AliasClause represents an alias, optionally with a column list:
// "AS name" or "AS name(col1, col2)".
type AliasClause struct {
	Alias Name
	Cols  NameList
}

// Format implements the NodeFormatter interface.
func (a *AliasClause) Format(ctx *FmtCtx) {
	ctx.FormatNode(&a.Alias)
	if len(a.Cols) != 0 {
		// Format as "alias (col1, col2, ...)".
		ctx.WriteString(" (")
		ctx.FormatNode(&a.Cols)
		ctx.WriteByte(')')
	}
}

// AsOfClause represents an as of time.
type AsOfClause struct {
	Expr Expr
}

// Format implements the NodeFormatter interface.
func (a *AsOfClause) Format(ctx *FmtCtx) {
	ctx.WriteString("AS OF SYSTEM TIME ")
	ctx.FormatNode(a.Expr)
}

// From represents a FROM clause.
type From struct {
	Tables TableExprs
	AsOf   AsOfClause
}

// Format implements the NodeFormatter interface.
func (node *From) Format(ctx *FmtCtx) {
	ctx.FormatNode(&node.Tables)
	if node.AsOf.Expr != nil {
		ctx.WriteByte(' ')
		ctx.FormatNode(&node.AsOf)
	}
}

// TableExprs represents a list of table expressions.
type TableExprs []TableExpr

// Format implements the NodeFormatter interface.
func (node *TableExprs) Format(ctx *FmtCtx) {
	if len(*node) != 0 {
		ctx.WriteString(" FROM ")
		for i, n := range *node {
			if i > 0 {
				ctx.WriteString(", ")
			}
			ctx.FormatNode(n)
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
func (*FuncExpr) tableExpr()         {}

// StatementSource encapsulates one of the other statements as a data source.
type StatementSource struct {
	Statement Statement
}

// Format implements the NodeFormatter interface.
func (node *StatementSource) Format(ctx *FmtCtx) {
	ctx.WriteByte('[')
	ctx.FormatNode(node.Statement)
	ctx.WriteByte(']')
}

func (*StatementSource) tableExpr() {}

// IndexID is a custom type for IndexDescriptor IDs.
type IndexID uint32

// IndexHints represents "@<index_name>" or "@{param[,param]}" where param is
// one of:
//  - FORCE_INDEX=<index_name>
//  - NO_INDEX_JOIN
// It is used optionally after a table name in SELECT statements.
type IndexHints struct {
	Index       UnrestrictedName
	IndexID     IndexID
	NoIndexJoin bool
}

// Format implements the NodeFormatter interface.
func (n *IndexHints) Format(ctx *FmtCtx) {
	if !n.NoIndexJoin {
		ctx.WriteByte('@')
		if n.Index != "" {
			ctx.FormatNode(&n.Index)
		} else {
			ctx.Printf("[%d]", n.IndexID)
		}
	} else {
		if n.Index == "" && n.IndexID == 0 {
			ctx.WriteString("@{NO_INDEX_JOIN}")
		} else {
			ctx.WriteString("@{FORCE_INDEX=")
			if n.Index != "" {
				ctx.FormatNode(&n.Index)
			} else {
				ctx.Printf("[%d]", n.IndexID)
			}
			ctx.WriteString(",NO_INDEX_JOIN}")
		}
	}
}

// AliasedTableExpr represents a table expression coupled with an optional
// alias.
type AliasedTableExpr struct {
	Expr       TableExpr
	Hints      *IndexHints
	Ordinality bool
	As         AliasClause
}

// Format implements the NodeFormatter interface.
func (node *AliasedTableExpr) Format(ctx *FmtCtx) {
	ctx.FormatNode(node.Expr)
	if node.Hints != nil {
		ctx.FormatNode(node.Hints)
	}
	if node.Ordinality {
		ctx.WriteString(" WITH ORDINALITY")
	}
	if node.As.Alias != "" {
		ctx.WriteString(" AS ")
		ctx.FormatNode(&node.As)
	}
}

func (*Subquery) tableExpr() {}

// ParenTableExpr represents a parenthesized TableExpr.
type ParenTableExpr struct {
	Expr TableExpr
}

// Format implements the NodeFormatter interface.
func (node *ParenTableExpr) Format(ctx *FmtCtx) {
	ctx.WriteByte('(')
	ctx.FormatNode(node.Expr)
	ctx.WriteByte(')')
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
	AstJoin      = "JOIN"
	AstFullJoin  = "FULL JOIN"
	AstLeftJoin  = "LEFT JOIN"
	AstRightJoin = "RIGHT JOIN"
	AstCrossJoin = "CROSS JOIN"
	AstInnerJoin = "INNER JOIN"
)

// Format implements the NodeFormatter interface.
func (node *JoinTableExpr) Format(ctx *FmtCtx) {
	ctx.FormatNode(node.Left)
	ctx.WriteByte(' ')
	if _, isNatural := node.Cond.(NaturalJoinCond); isNatural {
		// Natural joins have a different syntax: "<a> NATURAL <join_type> <b>"
		ctx.FormatNode(node.Cond)
		ctx.WriteByte(' ')
		ctx.WriteString(node.Join)
		ctx.WriteByte(' ')
		ctx.FormatNode(node.Right)
	} else {
		// General syntax: "<a> <join_type> <b> <condition>"
		ctx.WriteString(node.Join)
		ctx.WriteByte(' ')
		ctx.FormatNode(node.Right)
		if node.Cond != nil {
			ctx.FormatNode(node.Cond)
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
func (NaturalJoinCond) Format(ctx *FmtCtx) {
	ctx.WriteString("NATURAL")
}

// OnJoinCond represents an ON join condition.
type OnJoinCond struct {
	Expr Expr
}

// Format implements the NodeFormatter interface.
func (node *OnJoinCond) Format(ctx *FmtCtx) {
	ctx.WriteString(" ON ")
	ctx.FormatNode(node.Expr)
}

// UsingJoinCond represents a USING join condition.
type UsingJoinCond struct {
	Cols NameList
}

// Format implements the NodeFormatter interface.
func (node *UsingJoinCond) Format(ctx *FmtCtx) {
	ctx.WriteString(" USING (")
	ctx.FormatNode(&node.Cols)
	ctx.WriteByte(')')
}

// Where represents a WHERE or HAVING clause.
type Where struct {
	Type string
	Expr Expr
}

// Where.Type
const (
	AstWhere  = "WHERE"
	AstHaving = "HAVING"
)

// NewWhere creates a WHERE or HAVING clause out of an Expr. If the expression
// is nil, it returns nil.
func NewWhere(typ string, expr Expr) *Where {
	if expr == nil {
		return nil
	}
	return &Where{Type: typ, Expr: expr}
}

// Format implements the NodeFormatter interface.
func (node *Where) Format(ctx *FmtCtx) {
	if node != nil {
		ctx.WriteByte(' ')
		ctx.WriteString(node.Type)
		ctx.WriteByte(' ')
		ctx.FormatNode(node.Expr)
	}
}

// GroupBy represents a GROUP BY clause.
type GroupBy []Expr

// Format implements the NodeFormatter interface.
func (node *GroupBy) Format(ctx *FmtCtx) {
	prefix := " GROUP BY "
	for _, n := range *node {
		ctx.WriteString(prefix)
		ctx.FormatNode(n)
		prefix = ", "
	}
}

// DistinctOn represents a DISTINCT ON clause.
type DistinctOn []Expr

// Format implements the NodeFormatter interface.
func (node *DistinctOn) Format(ctx *FmtCtx) {
	prefix := " DISTINCT ON "
	for _, n := range *node {
		ctx.WriteString(prefix)
		ctx.FormatNode(n)
		prefix = ", "
	}
}

// OrderBy represents an ORDER By clause.
type OrderBy []*Order

// Format implements the NodeFormatter interface.
func (node *OrderBy) Format(ctx *FmtCtx) {
	prefix := " ORDER BY "
	for _, n := range *node {
		ctx.WriteString(prefix)
		ctx.FormatNode(n)
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

// OrderType indicates which type of expression is used in ORDER BY.
type OrderType int

const (
	// OrderByColumn is the regular "by expression/column" ORDER BY specification.
	OrderByColumn OrderType = iota
	// OrderByIndex enables the user to specify a given index' columns implicitly.
	OrderByIndex
)

// Order represents an ordering expression.
type Order struct {
	OrderType OrderType
	Expr      Expr
	Direction Direction
	// Table/Index replaces Expr when OrderType = OrderByIndex.
	Table NormalizableTableName
	// If Index is empty, then the order should use the primary key.
	Index UnrestrictedName
}

// Format implements the NodeFormatter interface.
func (node *Order) Format(ctx *FmtCtx) {
	if node.OrderType == OrderByColumn {
		ctx.FormatNode(node.Expr)
	} else {
		if node.Index == "" {
			ctx.WriteString("PRIMARY KEY ")
			ctx.FormatNode(&node.Table)
		} else {
			ctx.WriteString("INDEX ")
			ctx.FormatNode(&node.Table)
			ctx.WriteByte('@')
			ctx.FormatNode(&node.Index)
		}
	}
	if node.Direction != DefaultDirection {
		ctx.WriteByte(' ')
		ctx.WriteString(node.Direction.String())
	}
}

// Limit represents a LIMIT clause.
type Limit struct {
	Offset, Count Expr
}

// Format implements the NodeFormatter interface.
func (node *Limit) Format(ctx *FmtCtx) {
	if node != nil {
		if node.Count != nil {
			ctx.WriteString(" LIMIT ")
			ctx.FormatNode(node.Count)
		}
		if node.Offset != nil {
			ctx.WriteString(" OFFSET ")
			ctx.FormatNode(node.Offset)
		}
	}
}

// Window represents a WINDOW clause.
type Window []*WindowDef

// Format implements the NodeFormatter interface.
func (node *Window) Format(ctx *FmtCtx) {
	prefix := " WINDOW "
	for _, n := range *node {
		ctx.WriteString(prefix)
		ctx.FormatNode(&n.Name)
		ctx.WriteString(" AS ")
		ctx.FormatNode(n)
		prefix = ", "
	}
}

// WindowDef represents a single window definition expression.
type WindowDef struct {
	Name       Name
	RefName    Name
	Partitions Exprs
	OrderBy    OrderBy
}

// Format implements the NodeFormatter interface.
func (node *WindowDef) Format(ctx *FmtCtx) {
	ctx.WriteRune('(')
	needSpaceSeparator := false
	if node.RefName != "" {
		ctx.FormatNode(&node.RefName)
		needSpaceSeparator = true
	}
	if node.Partitions != nil {
		if needSpaceSeparator {
			ctx.WriteRune(' ')
		}
		ctx.WriteString("PARTITION BY ")
		ctx.FormatNode(&node.Partitions)
		needSpaceSeparator = true
	}
	if node.OrderBy != nil {
		if needSpaceSeparator {
			ctx.FormatNode(&node.OrderBy)
		} else {
			// We need to remove the initial space produced by OrderBy.Format.
			// TODO(knz): this code is horrendous. Figure a way to remove it.
			orderByStr := AsStringWithFlags(&node.OrderBy, ctx.flags)
			ctx.WriteString(orderByStr[1:])
		}
		needSpaceSeparator = true
		_ = needSpaceSeparator // avoid compiler warning until TODO below is addressed.
	}
	// TODO(nvanbenschoten): Support Window Frames.
	// if node.Frame != nil {}
	ctx.WriteRune(')')
}

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

package parser

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

// Expr represents an expression.
type Expr interface {
	fmt.Stringer
	expr()
}

func (*AndExpr) expr()        {}
func (*OrExpr) expr()         {}
func (*NotExpr) expr()        {}
func (*ParenExpr) expr()      {}
func (*ComparisonExpr) expr() {}
func (*RangeCond) expr()      {}
func (*NullCheck) expr()      {}
func (*ExistsExpr) expr()     {}
func (StrVal) expr()          {}
func (IntVal) expr()          {}
func (NumVal) expr()          {}
func (BoolVal) expr()         {}
func (ValArg) expr()          {}
func (NullVal) expr()         {}
func (QualifiedName) expr()   {}
func (Tuple) expr()           {}
func (*Subquery) expr()       {}
func (*BinaryExpr) expr()     {}
func (*UnaryExpr) expr()      {}
func (*FuncExpr) expr()       {}
func (*CaseExpr) expr()       {}
func (*CastExpr) expr()       {}
func (DBool) expr()           {}
func (DInt) expr()            {}
func (DFloat) expr()          {}
func (DString) expr()         {}
func (DTuple) expr()          {}
func (DNull) expr()           {}

// AndExpr represents an AND expression.
type AndExpr struct {
	Left, Right Expr
}

func (node *AndExpr) String() string {
	return fmt.Sprintf("%s AND %s", node.Left, node.Right)
}

// OrExpr represents an OR expression.
type OrExpr struct {
	Left, Right Expr
}

func (node *OrExpr) String() string {
	return fmt.Sprintf("%s OR %s", node.Left, node.Right)
}

// NotExpr represents a NOT expression.
type NotExpr struct {
	Expr Expr
}

func (node *NotExpr) String() string {
	return fmt.Sprintf("NOT %s", node.Expr)
}

// ParenExpr represents a parenthesized expression.
type ParenExpr struct {
	Expr Expr
}

func (node *ParenExpr) String() string {
	return fmt.Sprintf("(%s)", node.Expr)
}

// ComparisonOp represents a binary operator.
type ComparisonOp int

// ComparisonExpr.Operator
const (
	EQ ComparisonOp = iota
	LT
	GT
	LE
	GE
	NE
	In
	NotIn
	Like
	NotLike
)

var comparisonOpName = [...]string{
	EQ:      "=",
	LT:      "<",
	GT:      ">",
	LE:      "<=",
	GE:      ">=",
	NE:      "!=",
	In:      "IN",
	NotIn:   "NOT IN",
	Like:    "LIKE",
	NotLike: "NOT LIKE",
}

func (i ComparisonOp) String() string {
	if i < 0 || i > ComparisonOp(len(comparisonOpName)-1) {
		return fmt.Sprintf("ComparisonOp(%d)", i)
	}
	return comparisonOpName[i]
}

// ComparisonExpr represents a two-value comparison expression.
type ComparisonExpr struct {
	Operator    ComparisonOp
	Left, Right Expr
}

func (node *ComparisonExpr) String() string {
	return fmt.Sprintf("%s %s %s", node.Left, node.Operator, node.Right)
}

// RangeCond represents a BETWEEN or a NOT BETWEEN expression.
type RangeCond struct {
	Not      bool
	Left     Expr
	From, To Expr
}

// RangeCond.Operator
const (
	astBetween    = "BETWEEN"
	astNotBetween = "NOT BETWEEN"
)

func (node *RangeCond) String() string {
	if node.Not {
		return fmt.Sprintf("%s NOT BETWEEN %s AND %s", node.Left, node.From, node.To)
	}
	return fmt.Sprintf("%s BETWEEN %s AND %s", node.Left, node.From, node.To)
}

// NullCheck represents an IS NULL or an IS NOT NULL expression.
type NullCheck struct {
	Not  bool
	Expr Expr
}

func (node *NullCheck) String() string {
	if node.Not {
		return fmt.Sprintf("%s IS NOT NULL", node.Expr)
	}
	return fmt.Sprintf("%s IS NULL", node.Expr)
}

// ExistsExpr represents an EXISTS expression.
type ExistsExpr struct {
	Subquery *Subquery
}

func (node *ExistsExpr) String() string {
	return fmt.Sprintf("EXISTS %s", node.Subquery)
}

// StrVal represents a string value.
type StrVal string

func (node StrVal) String() string {
	var scratch [64]byte
	return string(encodeSQLString(scratch[0:0], []byte(node)))
}

// BytesVal represents a string of unprintable value.
type BytesVal string

func (BytesVal) expr() {}

func (node BytesVal) String() string {
	var scratch [64]byte
	return string(encodeSQLBytes(scratch[0:0], []byte(node)))
}

// IntVal represents an integer.
type IntVal int64

func (node IntVal) String() string {
	return strconv.FormatInt(int64(node), 10)
}

// NumVal represents a number.
type NumVal string

func (node NumVal) String() string {
	s := string(node)
	if l := len(s); l > 0 && s[l-1] == '-' {
		s += " "
	}
	return s
}

// BoolVal represents a boolean.
type BoolVal bool

func (node BoolVal) String() string {
	if node {
		return "true"
	}
	return "false"
}

// ValArg represents a named bind var argument.
type ValArg int

func (node ValArg) String() string {
	return fmt.Sprintf("$%d", int(node))
}

// NullVal represents a NULL value.
type NullVal struct{}

func (NullVal) String() string {
	return fmt.Sprintf("NULL")
}

// QualifiedName is a dot separated list of names.
type QualifiedName []string

// Database returns the database portion of the name.
func (n QualifiedName) Database() string {
	if len(n) > 1 {
		return n[0]
	}
	return ""
}

// Table returns the table portion of the name.
func (n QualifiedName) Table() string {
	return n[len(n)-1]
}

// Column returns the column portion of the name.
//
// TODO(pmattis) Handling of qualified names is currently very basic and we
// consider the column portion of the name to simply be the last
// component. This is identical to the table portion of the name. Perhaps it
// would be better to name this the "last" component because context matters in
// determining whether it is a table or column name. Perhaps we can have
// different types for use in the different contexts (e.g. ColumnName,
// TableName, IndexName, etc).
func (n QualifiedName) Column() string {
	return n.Table()
}

func (n QualifiedName) String() string {
	var buf bytes.Buffer
	for i, s := range n {
		if i > 0 {
			_, _ = buf.WriteString(".")
		}
		if s == "*" {
			_, _ = buf.WriteString(s)
		} else if _, ok := keywords[strings.ToUpper(s)]; ok {
			fmt.Fprintf(&buf, "\"%s\"", s)
		} else {
			encodeSQLIdent(&buf, s)
		}
	}
	return buf.String()
}

// QualifiedNames represents a command separated list (see the String method)
// of qualified names.
type QualifiedNames []QualifiedName

func (n QualifiedNames) String() string {
	var buf bytes.Buffer
	for i, e := range n {
		if i > 0 {
			_, _ = buf.WriteString(", ")
		}
		_, _ = buf.WriteString(e.String())
	}
	return buf.String()
}

// Tuple represents a tuple of actual values.
type Tuple Exprs

func (node Tuple) String() string {
	return fmt.Sprintf("(%s)", Exprs(node))
}

// Exprs represents a list of value expressions. It's not a valid expression
// because it's not parenthesized.
type Exprs []Expr

func (node Exprs) String() string {
	var prefix string
	var buf bytes.Buffer
	for _, n := range node {
		fmt.Fprintf(&buf, "%s%s", prefix, n)
		prefix = ", "
	}
	return buf.String()
}

// Subquery represents a subquery.
type Subquery struct {
	Select SelectStatement
}

func (node *Subquery) String() string {
	return fmt.Sprintf("(%s)", node.Select)
}

// BinaryOp represents a binary operator.
type BinaryOp int

// BinaryExpr.Operator
const (
	Bitand BinaryOp = iota
	Bitor
	Bitxor
	Plus
	Minus
	Mult
	Div
	Mod
	Exp
	Concat
)

var binaryOpName = [...]string{
	Bitand: "&",
	Bitor:  "|",
	Bitxor: "#",
	Plus:   "+",
	Minus:  "-",
	Mult:   "*",
	Div:    "/",
	Mod:    "%",
	Exp:    "^",
	Concat: "||",
}

func (i BinaryOp) String() string {
	if i < 0 || i > BinaryOp(len(binaryOpName)-1) {
		return fmt.Sprintf("BinaryOp(%d)", i)
	}
	return binaryOpName[i]
}

// BinaryExpr represents a binary value expression.
type BinaryExpr struct {
	Operator    BinaryOp
	Left, Right Expr
}

func (node *BinaryExpr) String() string {
	return fmt.Sprintf("%s%s%s", node.Left, node.Operator, node.Right)
}

// UnaryOp represents a unary operator.
type UnaryOp int

// UnaryExpr.Operator
const (
	UnaryPlus UnaryOp = iota
	UnaryMinus
	UnaryComplement
)

var unaryOpName = [...]string{
	UnaryPlus:       "+",
	UnaryMinus:      "-",
	UnaryComplement: "~",
}

func (i UnaryOp) String() string {
	if i < 0 || i > UnaryOp(len(unaryOpName)-1) {
		return fmt.Sprintf("UnaryOp(%d)", i)
	}
	return unaryOpName[i]
}

// UnaryExpr represents a unary value expression.
type UnaryExpr struct {
	Operator UnaryOp
	Expr     Expr
}

func (node *UnaryExpr) String() string {
	extra := ""
	if node.Operator == UnaryMinus {
		// We don't want to end up printing --5 since `--` indicates a comment.
		// Instead, space them out to "- -".
		if nNode, ok := node.Expr.(*UnaryExpr); ok && nNode.Operator == UnaryMinus {
			extra = " "
		}
	}
	return fmt.Sprintf("%s%s%s", node.Operator, extra, node.Expr)
}

// FuncExpr represents a function call.
type FuncExpr struct {
	Name     QualifiedName
	Distinct bool
	Exprs    Exprs
}

func (node *FuncExpr) String() string {
	var distinct string
	if node.Distinct {
		distinct = "DISTINCT "
	}
	return fmt.Sprintf("%s(%s%s)", node.Name, distinct, node.Exprs)
}

// CaseExpr represents a CASE expression.
type CaseExpr struct {
	Expr  Expr
	Whens []*When
	Else  Expr
}

func (node *CaseExpr) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "CASE ")
	if node.Expr != nil {
		fmt.Fprintf(&buf, "%s ", node.Expr)
	}
	for _, when := range node.Whens {
		fmt.Fprintf(&buf, "%s ", when)
	}
	if node.Else != nil {
		fmt.Fprintf(&buf, "ELSE %s ", node.Else)
	}
	fmt.Fprintf(&buf, "END")
	return buf.String()
}

// When represents a WHEN sub-expression.
type When struct {
	Cond Expr
	Val  Expr
}

func (node *When) String() string {
	return fmt.Sprintf("WHEN %s THEN %s", node.Cond, node.Val)
}

// CastExpr represents a CAST(expr AS type) expression.
type CastExpr struct {
	Expr Expr
	Type ColumnType
}

func (n *CastExpr) String() string {
	return fmt.Sprintf("CAST(%s AS %s)", n.Expr, n.Type)
}

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
	"reflect"
	"strconv"
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
func (BytesVal) expr()        {}
func (IntVal) expr()          {}
func (NumVal) expr()          {}
func (BoolVal) expr()         {}
func (ValArg) expr()          {}
func (NullVal) expr()         {}
func (*QualifiedName) expr()  {}
func (Tuple) expr()           {}
func (Row) expr()             {}
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
func (dNull) expr()           {}

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
	return string(node)
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

type nameType int

const (
	unknownName nameType = iota
	tableName
	columnName
)

// QualifiedName is a base name and an optional indirection expression.
type QualifiedName struct {
	Base       Name
	Indirect   Indirection
	normalized nameType
}

// StarExpr is a convenience variable that represents an unqualified "*".
var StarExpr = &QualifiedName{Indirect: Indirection{unqualifiedStar}}

// NormalizeTableName normalizes the qualified name to contain a database name
// as prefix, returning an error if unable to do so or if the name is not a
// valid table name (e.g. it contains an array indirection). The incoming
// qualified name should have one of the following forms:
//
//   table
//   database.table
//   table@index
//   database.table@index
//
// On successful normalization, the qualified name will have the form:
//
//   database.table@index
func (n *QualifiedName) NormalizeTableName(database string) error {
	if n == nil || n.Base == "" {
		return fmt.Errorf("empty table name: %s", n)
	}
	if n.normalized == columnName {
		return fmt.Errorf("already normalized as a column name: %s", n)
	}
	if len(n.Indirect) == 0 {
		// table -> database.table
		if database == "" {
			return fmt.Errorf("no database specified: %s", n)
		}
		n.Indirect = append(n.Indirect, NameIndirection(n.Base))
		n.Base = Name(database)
		n.normalized = tableName
		return nil
	}
	if len(n.Indirect) > 2 {
		return fmt.Errorf("invalid table name: %s", n)
	}
	// Either database.table or database.table@index.
	switch n.Indirect[0].(type) {
	case NameIndirection:
		// Nothing to do.
	case IndexIndirection:
		// table@index -> database.table@index
		//
		// Accomplished by prepending n.Base to the existing indirection and then
		// setting n.Base to the supplied database.
		if database == "" {
			return fmt.Errorf("no database specified: %s", n)
		}
		n.Indirect = append(Indirection{NameIndirection(n.Base)}, n.Indirect...)
		n.Base = Name(database)
	}
	if len(n.Indirect) == 2 {
		if _, ok := n.Indirect[1].(IndexIndirection); !ok {
			return fmt.Errorf("invalid table name: %s", n)
		}
	}
	n.normalized = tableName
	return nil
}

// NormalizeColumnName normalizes the qualified name to contain a table name as
// prefix, returning an error if unable to do so or if the name is not a valid
// column name (e.g. it contains too many indirections). If normalization
// occurred, the modified qualified name will have n.Base == "" to indicate no
// explicit table was specified. The incoming qualified name should have one of
// the following forms:
//
//   *
//   table.*
//   column
//   column[array-indirection]
//   table.column
//   table.column[array-indirection]
//
// Note that "table" may be the empty string. On successful normalization the
// qualified name will have one of the forms:
//
//   table.*
//   table.column
//   table.column[array-indirection]
func (n *QualifiedName) NormalizeColumnName() error {
	if n == nil {
		return fmt.Errorf("empty column name: %s", n)
	}
	if n.normalized == tableName {
		return fmt.Errorf("already normalized as a table name: %s", n)
	}
	if len(n.Indirect) == 0 {
		// column -> table.column
		if n.Base == "" {
			return fmt.Errorf("empty column name: %s", n)
		}
		n.Indirect = append(n.Indirect, NameIndirection(n.Base))
		n.Base = ""
		n.normalized = columnName
		return nil
	}
	if len(n.Indirect) > 2 {
		return fmt.Errorf("invalid column name: %s", n)
	}
	// Either table.column, table.*, column[array-indirection] or
	// table.column[array-indirection].
	switch n.Indirect[0].(type) {
	case NameIndirection, StarIndirection:
		// Nothing to do.
	case *ArrayIndirection:
		// column[array-indirection] -> "".column[array-indirection]
		//
		// Accomplished by prepending n.Base to the existing indirection and then
		// clearing n.Base.
		n.Indirect = append(Indirection{NameIndirection(n.Base)}, n.Indirect...)
		n.Base = ""
	default:
		return fmt.Errorf("invalid column name: %s", n)
	}
	if len(n.Indirect) == 2 {
		if _, ok := n.Indirect[1].(*ArrayIndirection); !ok {
			return fmt.Errorf("invalid column name: %s", n)
		}
	}
	n.normalized = columnName
	return nil
}

// Database returns the database portion of the name. Note that the returned
// string is not quoted even if the name is a keyword.
func (n *QualifiedName) Database() string {
	if n.normalized != tableName {
		panic(fmt.Sprintf("%s is not a table name", n))
	}
	// The database portion of the name is n.Base.
	return string(n.Base)
}

// Table returns the table portion of the name. Note that the returned string
// is not quoted even if the name is a keyword.
func (n *QualifiedName) Table() string {
	if n.normalized != tableName && n.normalized != columnName {
		panic(fmt.Sprintf("%s is not a table or column name", n))
	}
	if n.normalized == tableName {
		return string(n.Indirect[0].(NameIndirection))
	}
	return string(n.Base)
}

// Index returns the index portion of the name. Note that the returned string
// is not quoted even if the name is a keyword.
func (n *QualifiedName) Index() string {
	if n.normalized != tableName {
		panic(fmt.Sprintf("%s is not a table name", n))
	}
	if len(n.Indirect) == 2 {
		return string(n.Indirect[1].(IndexIndirection))
	}
	return ""
}

// Column returns the column portion of the name. Note that the returned string
// is not quoted even if the name is a keyword.
func (n *QualifiedName) Column() string {
	if n.normalized != columnName {
		panic(fmt.Sprintf("%s is not a column name", n))
	}
	return string(n.Indirect[0].(NameIndirection))
}

// IsStar returns true iff the qualified name contains matches "".* or table.*.
func (n *QualifiedName) IsStar() bool {
	if n.normalized != columnName {
		panic(fmt.Sprintf("%s is not a column name", n))
	}
	if len(n.Indirect) != 1 {
		return false
	}
	if _, ok := n.Indirect[0].(StarIndirection); !ok {
		return false
	}
	return true
}

func (n *QualifiedName) String() string {
	// Special handling for unqualified star indirection.
	if n.Base == "" && len(n.Indirect) == 1 && n.Indirect[0] == unqualifiedStar {
		return n.Indirect[0].String()
	}
	return fmt.Sprintf("%s%s", n.Base, n.Indirect)
}

// QualifiedNames represents a command separated list (see the String method)
// of qualified names.
type QualifiedNames []*QualifiedName

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

// Tuple represents a parenthesized list of expressions.
type Tuple Exprs

func (node Tuple) String() string {
	return fmt.Sprintf("(%s)", Exprs(node))
}

// Row represents a parenthesized list of expressions. Similar to Tuple except
// in how it is textually represented.
type Row Exprs

func (node Row) String() string {
	return fmt.Sprintf("ROW(%s)", Exprs(node))
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
	return node.Select.String()
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
	fn          func(Datum, Datum) (Datum, error)
	ltype       reflect.Type
	rtype       reflect.Type
}

func (node *BinaryExpr) String() string {
	return fmt.Sprintf("%s %s %s", node.Left, node.Operator, node.Right)
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
	fn       func(Datum) (Datum, error)
	dtype    reflect.Type
}

func (node *UnaryExpr) String() string {
	return fmt.Sprintf("%s %s", node.Operator, node.Expr)
}

// FuncExpr represents a function call.
type FuncExpr struct {
	Name     *QualifiedName
	Distinct bool
	Exprs    Exprs
	fn       builtin
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

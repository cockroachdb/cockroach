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

package parser

import (
	"bytes"
	"fmt"
	"reflect"
)

// Expr represents an expression.
type Expr interface {
	fmt.Stringer
	// Walk replaces each child of the receiver with the return of
	// `WalkExpr(v, child)`. For childless (leaf) Exprs, its
	// implementation is empty.
	Walk(Visitor)
	// TypeCheck returns the zero value of the expression's type, or an
	// error if the expression doesn't type-check. args maps bind var argument
	// names to types.
	TypeCheck(args MapArgs) (Datum, error)
	// Eval evaluates an SQL expression. Expression evaluation is a mostly
	// straightforward walk over the parse tree. The only significant complexity is
	// the handling of types and implicit conversions. See binOps and cmpOps for
	// more details. Note that expression evaluation returns an error if certain
	// node types are encountered: ValArg, QualifiedName or Subquery. These nodes
	// should be replaced prior to expression evaluation by an appropriate
	// WalkExpr. For example, ValArg should be replace by the argument passed from
	// the client.
	Eval(EvalContext) (Datum, error)
}

// VariableExpr is an Expr that may change per row. It is used to
// signal the evaluation/simplification machinery that the underlying
// Expr is not constant.
type VariableExpr interface {
	Expr
	Variable()
}

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
	Expr
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
	SimilarTo
	NotSimilarTo
	IsDistinctFrom
	IsNotDistinctFrom
	Is
	IsNot
)

var comparisonOpName = [...]string{
	EQ:                "=",
	LT:                "<",
	GT:                ">",
	LE:                "<=",
	GE:                ">=",
	NE:                "!=",
	In:                "IN",
	NotIn:             "NOT IN",
	Like:              "LIKE",
	NotLike:           "NOT LIKE",
	SimilarTo:         "SIMILAR TO",
	NotSimilarTo:      "NOT SIMILAR TO",
	IsDistinctFrom:    "IS DISTINCT FROM",
	IsNotDistinctFrom: "IS NOT DISTINCT FROM",
	Is:                "IS",
	IsNot:             "IS NOT",
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
	fn          cmpOp
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

func (node *RangeCond) String() string {
	if node.Not {
		return fmt.Sprintf("%s NOT BETWEEN %s AND %s", node.Left, node.From, node.To)
	}
	return fmt.Sprintf("%s BETWEEN %s AND %s", node.Left, node.From, node.To)
}

// IsOp represents an IS expression operator.
type IsOp int

// IsOfTypeExpr represents an IS {,NOT} OF (type_list) expression.
type IsOfTypeExpr struct {
	Not   bool
	Expr  Expr
	Types []ColumnType
}

func (node *IsOfTypeExpr) String() string {
	var buf bytes.Buffer
	buf.WriteString(node.Expr.String())
	buf.WriteString(" IS")
	if node.Not {
		buf.WriteString(" NOT")
	}
	buf.WriteString(" OF (")
	for i, t := range node.Types {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(t.String())
	}
	buf.WriteString(")")
	return buf.String()
}

// ExistsExpr represents an EXISTS expression.
type ExistsExpr struct {
	Subquery Expr
}

func (node *ExistsExpr) String() string {
	return fmt.Sprintf("EXISTS %s", node.Subquery)
}

// IfExpr represents an IF expression.
type IfExpr struct {
	Cond Expr
	True Expr
	Else Expr
}

func (node *IfExpr) String() string {
	return fmt.Sprintf("IF(%s, %s, %s)", node.Cond, node.True, node.Else)
}

// NullIfExpr represents a NULLIF expression.
type NullIfExpr struct {
	Expr1 Expr
	Expr2 Expr
}

func (node *NullIfExpr) String() string {
	return fmt.Sprintf("NULLIF(%s, %s)", node.Expr1, node.Expr2)
}

// CoalesceExpr represents a COALESCE or IFNULL expression.
type CoalesceExpr struct {
	Name  string
	Exprs Exprs
}

func (node *CoalesceExpr) String() string {
	return fmt.Sprintf("%s(%s)", node.Name, node.Exprs)
}

// IntVal represents an integer.
type IntVal struct {
	Val int64
	Str string
}

func (node *IntVal) String() string {
	return node.Str
}

// NumVal represents a number.
type NumVal string

func (node NumVal) String() string {
	return string(node)
}

// DefaultVal represents the DEFAULT expression.
type DefaultVal struct{}

func (node DefaultVal) String() string {
	return "DEFAULT"
}

var _ VariableExpr = ValArg{}

// ValArg represents a named bind var argument.
type ValArg struct {
	name string
}

// Variable implements the VariableExpr interface.
func (ValArg) Variable() {}

func (node ValArg) String() string {
	return fmt.Sprintf("$%s", node.name)
}

type nameType int

const (
	_ nameType = iota
	tableName
	columnName
)

var _ VariableExpr = &QualifiedName{}

// QualifiedName is a base name and an optional indirection expression.
type QualifiedName struct {
	Base       Name
	Indirect   Indirection
	normalized nameType
}

// Variable implements the VariableExpr interface.
func (*QualifiedName) Variable() {}

// StarExpr is a convenience function that represents an unqualified "*".
func StarExpr() *QualifiedName {
	return &QualifiedName{Indirect: Indirection{unqualifiedStar}}
}

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
	if err := n.QualifyWithDatabase(database); err != nil {
		return err
	}

	if len(n.Indirect) > 2 {
		return fmt.Errorf("invalid table name: %s", n)
	}
	if len(n.Indirect) == 2 {
		if _, ok := n.Indirect[1].(IndexIndirection); !ok {
			return fmt.Errorf("invalid table name: %s", n)
		}
	}
	n.normalized = tableName
	return nil
}

// QualifyWithDatabase adds an indirection for the database, if it's missing.
// It transforms:
// table       -> database.table
// table@index -> database.table@index
// *           -> database.*
func (n *QualifiedName) QualifyWithDatabase(database string) error {
	if len(n.Indirect) == 0 {
		if database == "" {
			return fmt.Errorf("no database specified: %s", n)
		}
		// table -> database.table
		if database == "" {
			return fmt.Errorf("no database specified: %s", n)
		}
		n.Indirect = append(n.Indirect, NameIndirection(n.Base))
		n.Base = Name(database)
		n.normalized = tableName
		return nil
	}
	switch n.Indirect[0].(type) {
	case NameIndirection:
		// Nothing to do.
	case IndexIndirection:
		// table@index -> database.table@index
		// *           -> database.*
		//
		// Accomplished by prepending n.Base to the existing indirection and then
		// setting n.Base to the supplied database.
		if database == "" {
			return fmt.Errorf("no database specified: %s", n)
		}
		n.Indirect = append(Indirection{NameIndirection(n.Base)}, n.Indirect...)
		n.Base = Name(database)
	case StarIndirection:
		// * -> database.*
		if n.Base != "" {
			// nothing to do
			return nil
		}
		if n.Base != "" {
			n.Indirect = append(Indirection{NameIndirection(n.Base)}, n.Indirect...)
		}
		if database == "" {
			return fmt.Errorf("no database specified: %s", n)
		}
		n.Base = Name(database)
	}
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
	case NameIndirection:
		// Nothing to do.
	case StarIndirection:
		n.Indirect[0] = qualifiedStar
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
			buf.WriteString(", ")
		}
		buf.WriteString(e.String())
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

// Array represents an array constructor.
type Array Exprs

func (node Array) String() string {
	return fmt.Sprintf("ARRAY[%s]", Exprs(node))
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
	Concat
	LShift
	RShift
)

var binaryOpName = [...]string{
	Bitand: "&",
	Bitor:  "|",
	Bitxor: "^",
	Plus:   "+",
	Minus:  "-",
	Mult:   "*",
	Div:    "/",
	Mod:    "%",
	Concat: "||",
	LShift: "<<",
	RShift: ">>",
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
	fn          binOp
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
	fn       unaryOp
	dtype    reflect.Type
}

func (node *UnaryExpr) String() string {
	return fmt.Sprintf("%s %s", node.Operator, node.Expr)
}

// FuncExpr represents a function call.
type FuncExpr struct {
	Name  *QualifiedName
	Type  funcType
	Exprs Exprs

	// These fields are not part of the Expr AST.
	fn      builtin
	fnFound bool
}

type funcType int

// FuncExpr.Type
const (
	_ funcType = iota
	Distinct
	All
)

var funcTypeName = [...]string{
	Distinct: "DISTINCT",
	All:      "ALL",
}

func (node *FuncExpr) String() string {
	var typ string
	if node.Type != 0 {
		typ = funcTypeName[node.Type] + " "
	}
	return fmt.Sprintf("%s(%s%s)", node.Name, typ, node.Exprs)
}

// OverlayExpr represents an overlay function call.
type OverlayExpr struct {
	FuncExpr
}

func (node *OverlayExpr) String() string {
	var f string
	if len(node.Exprs) == 4 {
		f = fmt.Sprintf(" FOR %s", node.Exprs[3])
	}
	return fmt.Sprintf("%s(%s PLACING %s FROM %s%s)", node.Name, node.Exprs[0], node.Exprs[1], node.Exprs[2], f)
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

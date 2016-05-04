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
)

// Expr represents an expression.
type Expr interface {
	fmt.Stringer
	NodeFormatter
	// Walk recursively walks all children using WalkExpr. If any children are changed, it returns a
	// copy of this node updated to point to the new children. Otherwise the receiver is returned.
	// For childless (leaf) Exprs, its implementation is empty.
	Walk(Visitor) Expr
	// TypeCheck transforms the Expr into a well-typed TypedExpr, which further permits
	// evaluation and type introspection, or an error if the expression cannot be well-typed.
	// When type checking is complete, if no error was reported, the expression and all
	// sub-expressions will be guaranteed to be well-typed, meaning that the method effectively
	// maps the Expr tree into a TypedExpr tree.
	//
	// The args parameter maps ValArg names to types inferred while type-checking.
	// The desired parameter hints the desired type that the method's caller wants from
	// the resulting TypedExpr.
	TypeCheck(args MapArgs, desired Datum) (TypedExpr, error)
}

// TypedExpr represents a well-typed expression.
type TypedExpr interface {
	Expr
	// Eval evaluates an SQL expression. Expression evaluation is a mostly
	// straightforward walk over the parse tree. The only significant complexity is
	// the handling of types and implicit conversions. See binOps and cmpOps for
	// more details. Note that expression evaluation returns an error if certain
	// node types are encountered: ValArg, QualifiedName or Subquery. These nodes
	// should be replaced prior to expression evaluation by an appropriate
	// WalkExpr. For example, ValArg should be replace by the argument passed from
	// the client.
	Eval(EvalContext) (Datum, error)
	// ReturnType provides the type of the TypedExpr, which is the type of Datum that
	// the TypedExpr will return when evaluated.
	ReturnType() Datum
}

// VariableExpr is an Expr that may change per row. It is used to
// signal the evaluation/simplification machinery that the underlying
// Expr is not constant.
type VariableExpr interface {
	Expr
	Variable()
}

// operatorExpr is used to identify expression types that involve operators;
// used by exprStrWithParen.
type operatorExpr interface {
	Expr
	operatorExpr()
}

var _ operatorExpr = &AndExpr{}
var _ operatorExpr = &OrExpr{}
var _ operatorExpr = &NotExpr{}
var _ operatorExpr = &BinaryExpr{}
var _ operatorExpr = &UnaryExpr{}
var _ operatorExpr = &ComparisonExpr{}
var _ operatorExpr = &RangeCond{}
var _ operatorExpr = &IsOfTypeExpr{}

// exprFmtWithParen is a variant of Format() which adds a set of outer parens
// if the expression involves an operator. It is used internally when the
// expression is part of another expression and we know it is preceded or
// followed by an operator.
func exprFmtWithParen(buf *bytes.Buffer, f FmtFlags, e Expr) {
	if _, ok := e.(operatorExpr); ok {
		buf.WriteByte('(')
		FormatNode(buf, f, e)
		buf.WriteByte(')')
	} else {
		FormatNode(buf, f, e)
	}
}

// typeAnnotation is an embeddable struct to provide a TypedExpr with a dynamic
// type annotation.
type typeAnnotation struct {
	typ Datum
}

func (ta typeAnnotation) ReturnType() Datum {
	ta.assertTyped()
	return ta.typ
}

func (ta typeAnnotation) assertTyped() {
	if ta.typ == nil {
		panic("ReturnType called on TypedExpr with empty typeAnnotation. " +
			"Was the underlying Expr type-checked before asserting a type of TypedExpr?")
	}
}

// AndExpr represents an AND expression.
type AndExpr struct {
	Left, Right Expr

	typeAnnotation
}

func (*AndExpr) operatorExpr() {}

func binExprFmtWithParen(buf *bytes.Buffer, f FmtFlags, e1 Expr, op string, e2 Expr) {
	exprFmtWithParen(buf, f, e1)
	buf.WriteByte(' ')
	buf.WriteString(op)
	buf.WriteByte(' ')
	exprFmtWithParen(buf, f, e2)
}

// Format implements the NodeFormatter interface.
func (node *AndExpr) Format(buf *bytes.Buffer, f FmtFlags) {
	binExprFmtWithParen(buf, f, node.Left, "AND", node.Right)
}

// NewTypedAndExpr returns a new AndExpr that is verified to be well-typed.
func NewTypedAndExpr(left, right TypedExpr) *AndExpr {
	node := &AndExpr{Left: left, Right: right}
	node.typ = DummyBool
	return node
}

// TypedLeft returns the AndExpr's left expression as a TypedExpr.
func (node *AndExpr) TypedLeft() TypedExpr {
	return node.Left.(TypedExpr)
}

// TypedRight returns the AndExpr's right expression as a TypedExpr.
func (node *AndExpr) TypedRight() TypedExpr {
	return node.Right.(TypedExpr)
}

// OrExpr represents an OR expression.
type OrExpr struct {
	Left, Right Expr

	typeAnnotation
}

func (*OrExpr) operatorExpr() {}

// Format implements the NodeFormatter interface.
func (node *OrExpr) Format(buf *bytes.Buffer, f FmtFlags) {
	binExprFmtWithParen(buf, f, node.Left, "OR", node.Right)
}

// NewTypedOrExpr returns a new OrExpr that is verified to be well-typed.
func NewTypedOrExpr(left, right TypedExpr) *OrExpr {
	node := &OrExpr{Left: left, Right: right}
	node.typ = DummyBool
	return node
}

// TypedLeft returns the OrExpr's left expression as a TypedExpr.
func (node *OrExpr) TypedLeft() TypedExpr {
	return node.Left.(TypedExpr)
}

// TypedRight returns the OrExpr's right expression as a TypedExpr.
func (node *OrExpr) TypedRight() TypedExpr {
	return node.Right.(TypedExpr)
}

// NotExpr represents a NOT expression.
type NotExpr struct {
	Expr Expr

	typeAnnotation
}

func (*NotExpr) operatorExpr() {}

// Format implements the NodeFormatter interface.
func (node *NotExpr) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("NOT ")
	exprFmtWithParen(buf, f, node.Expr)
}

// NewTypedNotExpr returns a new NotExpr that is verified to be well-typed.
func NewTypedNotExpr(expr TypedExpr) *NotExpr {
	node := &NotExpr{Expr: expr}
	node.typ = DummyBool
	return node
}

// TypedInnerExpr returns the NotExpr's inner expression as a TypedExpr.
func (node *NotExpr) TypedInnerExpr() TypedExpr {
	return node.Expr.(TypedExpr)
}

// ParenExpr represents a parenthesized expression.
type ParenExpr struct {
	Expr Expr

	typeAnnotation
}

// Format implements the NodeFormatter interface.
func (node *ParenExpr) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteByte('(')
	FormatNode(buf, f, node.Expr)
	buf.WriteByte(')')
}

// TypedInnerExpr returns the ParenExpr's inner expression as a TypedExpr.
func (node *ParenExpr) TypedInnerExpr() TypedExpr {
	return node.Expr.(TypedExpr)
}

// ComparisonOperator represents a binary operator.
type ComparisonOperator int

// ComparisonExpr.Operator
const (
	EQ ComparisonOperator = iota
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

func (i ComparisonOperator) String() string {
	if i < 0 || i > ComparisonOperator(len(comparisonOpName)-1) {
		return fmt.Sprintf("ComparisonOp(%d)", i)
	}
	return comparisonOpName[i]
}

// ComparisonExpr represents a two-value comparison expression.
type ComparisonExpr struct {
	Operator    ComparisonOperator
	Left, Right Expr

	typeAnnotation
	fn CmpOp
}

func (*ComparisonExpr) operatorExpr() {}

// Format implements the NodeFormatter interface.
func (node *ComparisonExpr) Format(buf *bytes.Buffer, f FmtFlags) {
	binExprFmtWithParen(buf, f, node.Left, node.Operator.String(), node.Right)
}

// NewTypedComparisonExpr returns a new ComparisonExpr that is verified to be well-typed.
func NewTypedComparisonExpr(op ComparisonOperator, left, right TypedExpr) *ComparisonExpr {
	node := &ComparisonExpr{Operator: op, Left: left, Right: right}
	node.typ = DummyBool
	return node
}

// TypedLeft returns the ComparisonExpr's left expression as a TypedExpr.
func (node *ComparisonExpr) TypedLeft() TypedExpr {
	return node.Left.(TypedExpr)
}

// TypedRight returns the ComparisonExpr's right expression as a TypedExpr.
func (node *ComparisonExpr) TypedRight() TypedExpr {
	return node.Right.(TypedExpr)
}

// RangeCond represents a BETWEEN or a NOT BETWEEN expression.
type RangeCond struct {
	Not      bool
	Left     Expr
	From, To Expr

	typeAnnotation
}

func (*RangeCond) operatorExpr() {}

// Format implements the NodeFormatter interface.
func (node *RangeCond) Format(buf *bytes.Buffer, f FmtFlags) {
	notStr := " BETWEEN "
	if node.Not {
		notStr = " NOT BETWEEN "
	}
	exprFmtWithParen(buf, f, node.Left)
	buf.WriteString(notStr)
	binExprFmtWithParen(buf, f, node.From, "AND", node.To)
}

// TypedLeft returns the RangeCond's left expression as a TypedExpr.
func (node *RangeCond) TypedLeft() TypedExpr {
	return node.Left.(TypedExpr)
}

// TypedFrom returns the RangeCond's from expression as a TypedExpr.
func (node *RangeCond) TypedFrom() TypedExpr {
	return node.From.(TypedExpr)
}

// TypedTo returns the RangeCond's to expression as a TypedExpr.
func (node *RangeCond) TypedTo() TypedExpr {
	return node.To.(TypedExpr)
}

// IsOfTypeExpr represents an IS {,NOT} OF (type_list) expression.
type IsOfTypeExpr struct {
	Not   bool
	Expr  Expr
	Types []ColumnType

	typeAnnotation
}

func (*IsOfTypeExpr) operatorExpr() {}

// Format implements the NodeFormatter interface.
func (node *IsOfTypeExpr) Format(buf *bytes.Buffer, f FmtFlags) {
	FormatNode(buf, f, node.Expr)
	buf.WriteString(" IS")
	if node.Not {
		buf.WriteString(" NOT")
	}
	buf.WriteString(" OF (")
	for i, t := range node.Types {
		if i > 0 {
			buf.WriteString(", ")
		}
		FormatNode(buf, f, t)
	}
	buf.WriteByte(')')
}

// ExistsExpr represents an EXISTS expression.
type ExistsExpr struct {
	Subquery Expr

	typeAnnotation
}

// Format implements the NodeFormatter interface.
func (node *ExistsExpr) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("EXISTS ")
	exprFmtWithParen(buf, f, node.Subquery)
}

// IfExpr represents an IF expression.
type IfExpr struct {
	Cond Expr
	True Expr
	Else Expr

	typeAnnotation
}

// Format implements the NodeFormatter interface.
func (node *IfExpr) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("IF(")
	FormatNode(buf, f, node.Cond)
	buf.WriteString(", ")
	FormatNode(buf, f, node.True)
	buf.WriteString(", ")
	FormatNode(buf, f, node.Else)
	buf.WriteByte(')')
}

// NullIfExpr represents a NULLIF expression.
type NullIfExpr struct {
	Expr1 Expr
	Expr2 Expr

	typeAnnotation
}

// Format implements the NodeFormatter interface.
func (node *NullIfExpr) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("NULLIF(")
	FormatNode(buf, f, node.Expr1)
	buf.WriteString(", ")
	FormatNode(buf, f, node.Expr2)
	buf.WriteByte(')')
}

// CoalesceExpr represents a COALESCE or IFNULL expression.
type CoalesceExpr struct {
	Name  string
	Exprs Exprs

	typeAnnotation
}

// Format implements the NodeFormatter interface.
func (node *CoalesceExpr) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString(node.Name)
	buf.WriteByte('(')
	FormatNode(buf, f, node.Exprs)
	buf.WriteByte(')')
}

// DefaultVal represents the DEFAULT expression.
type DefaultVal struct{}

// Format implements the NodeFormatter interface.
func (node DefaultVal) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("DEFAULT")
}

// ReturnType implements the TypedExpr interface.
func (DefaultVal) ReturnType() Datum { return nil }

var _ VariableExpr = ValArg{}

// ValArg represents a named bind var argument.
type ValArg struct {
	Name string
}

// Variable implements the VariableExpr interface.
func (ValArg) Variable() {}

// Format implements the NodeFormatter interface.
func (node ValArg) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteByte('$')
	buf.WriteString(node.Name)
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

	// We preserve the "original" string representation (before normalization).
	origString string
}

// ReturnType implements the TypedExpr interface.
func (*QualifiedName) ReturnType() Datum { return nil }

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
func (node *QualifiedName) NormalizeTableName(database string) error {
	if node == nil || node.Base == "" {
		return fmt.Errorf("empty table name: %s", node)
	}
	if node.normalized == columnName {
		return fmt.Errorf("already normalized as a column name: %s", node)
	}
	if err := node.QualifyWithDatabase(database); err != nil {
		return err
	}

	if len(node.Indirect) > 1 {
		return fmt.Errorf("invalid table name: %s", node)
	}
	node.normalized = tableName
	return nil
}

// QualifyWithDatabase adds an indirection for the database, if it's missing.
// It transforms:
// table       -> database.table
// table@index -> database.table@index
// *           -> database.*
func (node *QualifiedName) QualifyWithDatabase(database string) error {
	node.setString()
	if len(node.Indirect) == 0 {
		if database == "" {
			return fmt.Errorf("no database specified: %s", node)
		}
		// table -> database.table
		if database == "" {
			return fmt.Errorf("no database specified: %s", node)
		}
		node.Indirect = append(node.Indirect, NameIndirection(node.Base))
		node.Base = Name(database)
		node.normalized = tableName
		return nil
	}
	switch node.Indirect[0].(type) {
	case NameIndirection:
		// Nothing to do.
	case StarIndirection:
		// * -> database.*
		if node.Base != "" {
			// nothing to do
			return nil
		}
		if node.Base != "" {
			node.Indirect = append(Indirection{NameIndirection(node.Base)}, node.Indirect...)
		}
		if database == "" {
			return fmt.Errorf("no database specified: %s", node)
		}
		node.Base = Name(database)
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
func (node *QualifiedName) NormalizeColumnName() error {
	if node == nil {
		return fmt.Errorf("empty column name: %s", node)
	}
	if node.normalized == tableName {
		return fmt.Errorf("already normalized as a table name: %s", node)
	}
	node.setString()
	if len(node.Indirect) == 0 {
		// column -> table.column
		if node.Base == "" {
			return fmt.Errorf("empty column name: %s", node)
		}
		node.Indirect = append(node.Indirect, NameIndirection(node.Base))
		node.Base = ""
		node.normalized = columnName
		return nil
	}
	if len(node.Indirect) > 2 {
		return fmt.Errorf("invalid column name: %s", node)
	}
	// Either table.column, table.*, column[array-indirection] or
	// table.column[array-indirection].
	switch node.Indirect[0].(type) {
	case NameIndirection:
		// Nothing to do.
	case StarIndirection:
		node.Indirect[0] = qualifiedStar
	case *ArrayIndirection:
		// column[array-indirection] -> "".column[array-indirection]
		//
		// Accomplished by prepending node.Base to the existing indirection and then
		// clearing node.Base.
		node.Indirect = append(Indirection{NameIndirection(node.Base)}, node.Indirect...)
		node.Base = ""
	default:
		return fmt.Errorf("invalid column name: %s", node)
	}
	if len(node.Indirect) == 2 {
		if _, ok := node.Indirect[1].(*ArrayIndirection); !ok {
			return fmt.Errorf("invalid column name: %s", node)
		}
	}
	node.normalized = columnName
	return nil
}

// Database returns the database portion of the name. Note that the returned
// string is not quoted even if the name is a keyword.
func (node *QualifiedName) Database() string {
	if node.normalized != tableName {
		panic(fmt.Sprintf("%s is not a table name", node))
	}
	// The database portion of the name is n.Base.
	return string(node.Base)
}

// Table returns the table portion of the name. Note that the returned string
// is not quoted even if the name is a keyword.
func (node *QualifiedName) Table() string {
	if node.normalized != tableName && node.normalized != columnName {
		panic(fmt.Sprintf("%s is not a table or column name", node))
	}
	if node.normalized == tableName {
		return string(node.Indirect[0].(NameIndirection))
	}
	return string(node.Base)
}

// Column returns the column portion of the name. Note that the returned string
// is not quoted even if the name is a keyword.
func (node *QualifiedName) Column() string {
	if node.normalized != columnName {
		panic(fmt.Sprintf("%s is not a column name", node))
	}
	return string(node.Indirect[0].(NameIndirection))
}

// IsStar returns true iff the qualified name contains matches "".* or table.*.
func (node *QualifiedName) IsStar() bool {
	if node.normalized != columnName {
		panic(fmt.Sprintf("%s is not a column name", node))
	}
	if len(node.Indirect) != 1 {
		return false
	}
	if _, ok := node.Indirect[0].(StarIndirection); !ok {
		return false
	}
	return true
}

// ClearString causes String to return the current (possibly normalized) name instead of the
// original name (used for testing).
func (node *QualifiedName) ClearString() {
	node.origString = ""
}

func (node *QualifiedName) setString() {
	// We preserve the representation pre-normalization.
	if node.origString != "" {
		return
	}
	var buf bytes.Buffer
	if node.Base == "" && len(node.Indirect) == 1 && node.Indirect[0] == unqualifiedStar {
		FormatNode(&buf, FmtSimple, node.Indirect[0])
	} else {
		FormatNode(&buf, FmtSimple, node.Base)
		FormatNode(&buf, FmtSimple, node.Indirect)
	}
	node.origString = buf.String()
}

// Format implements the NodeFormatter interface.
func (node *QualifiedName) Format(buf *bytes.Buffer, f FmtFlags) {
	node.setString()
	buf.WriteString(node.origString)
}

// QualifiedNames represents a command separated list (see the String method)
// of qualified names.
type QualifiedNames []*QualifiedName

// Format implements the NodeFormatter interface.
func (n QualifiedNames) Format(buf *bytes.Buffer, f FmtFlags) {
	for i, e := range n {
		if i > 0 {
			buf.WriteString(", ")
		}
		FormatNode(buf, f, e)
	}
}

// TableNameWithIndex represents a "table@index", used in statements that
// specifically refer to an index.
type TableNameWithIndex struct {
	Table *QualifiedName
	Index Name
}

// Format implements the NodeFormatter interface.
func (n *TableNameWithIndex) Format(buf *bytes.Buffer, f FmtFlags) {
	FormatNode(buf, f, n.Table)
	buf.WriteByte('@')
	FormatNode(buf, f, n.Index)
}

// TableNameWithIndexList is a list of indexes.
type TableNameWithIndexList []*TableNameWithIndex

// Format implements the NodeFormatter interface.
func (n TableNameWithIndexList) Format(buf *bytes.Buffer, f FmtFlags) {
	for i, e := range n {
		if i > 0 {
			buf.WriteString(", ")
		}
		FormatNode(buf, f, e)
	}
}

// Tuple represents a parenthesized list of expressions.
type Tuple struct {
	Exprs Exprs

	types DTuple
}

// Format implements the NodeFormatter interface.
func (node *Tuple) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteByte('(')
	FormatNode(buf, f, node.Exprs)
	buf.WriteByte(')')
}

// ReturnType implements the TypedExpr interface.
func (node *Tuple) ReturnType() Datum {
	return &node.types
}

// Row represents a parenthesized list of expressions. Similar to Tuple except
// in how it is textually represented.
type Row struct {
	Exprs Exprs

	types DTuple
}

// Format implements the NodeFormatter interface.
func (node *Row) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("ROW(")
	FormatNode(buf, f, node.Exprs)
	buf.WriteByte(')')
}

// ReturnType implements the TypedExpr interface.
func (node *Row) ReturnType() Datum {
	return &node.types
}

// Array represents an array constructor.
type Array struct {
	Exprs Exprs
}

// Format implements the NodeFormatter interface.
func (node *Array) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("ARRAY[")
	FormatNode(buf, f, node.Exprs)
	buf.WriteByte(']')
}

// Exprs represents a list of value expressions. It's not a valid expression
// because it's not parenthesized.
type Exprs []Expr

// Format implements the NodeFormatter interface.
func (node Exprs) Format(buf *bytes.Buffer, f FmtFlags) {
	for i, n := range node {
		if i > 0 {
			buf.WriteString(", ")
		}
		FormatNode(buf, f, n)
	}
}

// TypedExprs represents a list of well-typed value expressions. It's not a valid expression
// because it's not parenthesized.
type TypedExprs []TypedExpr

func (node TypedExprs) String() string {
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

// Format implements the NodeFormatter interface.
func (node *Subquery) Format(buf *bytes.Buffer, f FmtFlags) {
	FormatNode(buf, f, node.Select)
}

// ReturnType implements the TypedExpr interface.
func (node *Subquery) ReturnType() Datum {
	return DNull
}

// BinaryOperator represents a binary operator.
type BinaryOperator int

// BinaryExpr.Operator
const (
	Bitand BinaryOperator = iota
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

func (i BinaryOperator) String() string {
	if i < 0 || i > BinaryOperator(len(binaryOpName)-1) {
		return fmt.Sprintf("BinaryOp(%d)", i)
	}
	return binaryOpName[i]
}

// BinaryExpr represents a binary value expression.
type BinaryExpr struct {
	Operator    BinaryOperator
	Left, Right Expr

	typeAnnotation
	fn BinOp
}

func (*BinaryExpr) operatorExpr() {}

// Format implements the NodeFormatter interface.
func (node *BinaryExpr) Format(buf *bytes.Buffer, f FmtFlags) {
	binExprFmtWithParen(buf, f, node.Left, node.Operator.String(), node.Right)
}

// UnaryOperator represents a unary operator.
type UnaryOperator int

// UnaryExpr.Operator
const (
	UnaryPlus UnaryOperator = iota
	UnaryMinus
	UnaryComplement
)

var unaryOpName = [...]string{
	UnaryPlus:       "+",
	UnaryMinus:      "-",
	UnaryComplement: "~",
}

func (i UnaryOperator) String() string {
	if i < 0 || i > UnaryOperator(len(unaryOpName)-1) {
		return fmt.Sprintf("UnaryOp(%d)", i)
	}
	return unaryOpName[i]
}

// UnaryExpr represents a unary value expression.
type UnaryExpr struct {
	Operator UnaryOperator
	Expr     Expr

	typeAnnotation
	fn UnaryOp
}

func (*UnaryExpr) operatorExpr() {}

// Format implements the NodeFormatter interface.
func (node *UnaryExpr) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString(node.Operator.String())
	buf.WriteByte(' ')
	exprFmtWithParen(buf, f, node.Expr)
}

// FuncExpr represents a function call.
type FuncExpr struct {
	Name  *QualifiedName
	Type  funcType
	Exprs Exprs

	typeAnnotation
	fn Builtin
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

// Format implements the NodeFormatter interface.
func (node *FuncExpr) Format(buf *bytes.Buffer, f FmtFlags) {
	var typ string
	if node.Type != 0 {
		typ = funcTypeName[node.Type] + " "
	}
	// TODO(nvanbenschoten) We should probably either remove
	// *QualifiedName as a TypedExpr or special case it in
	// FormatNode. The reason it's a TypedExpr in the first place is for
	// testing (I believe), but this doesn't seem like a good enough
	// reason to justify the strangeness.
	buf.WriteString(node.Name.String())
	buf.WriteByte('(')
	buf.WriteString(typ)
	FormatNode(buf, f, node.Exprs)
	buf.WriteByte(')')
}

// OverlayExpr represents an overlay function call.
type OverlayExpr struct {
	FuncExpr
}

// Format implements the NodeFormatter interface.
func (node *OverlayExpr) Format(buf *bytes.Buffer, f FmtFlags) {
	FormatNode(buf, f, node.Name)
	buf.WriteByte('(')
	FormatNode(buf, f, node.Exprs[0])
	buf.WriteString(" PLACING ")
	FormatNode(buf, f, node.Exprs[1])
	buf.WriteString(" FROM ")
	FormatNode(buf, f, node.Exprs[2])
	if len(node.Exprs) == 4 {
		buf.WriteString(" FOR ")
		FormatNode(buf, f, node.Exprs[3])
	}
	buf.WriteByte(')')
}

// CaseExpr represents a CASE expression.
type CaseExpr struct {
	Expr  Expr
	Whens []*When
	Else  Expr

	typeAnnotation
}

// Format implements the NodeFormatter interface.
func (node *CaseExpr) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("CASE ")
	if node.Expr != nil {
		FormatNode(buf, f, node.Expr)
		buf.WriteByte(' ')
	}
	for _, when := range node.Whens {
		FormatNode(buf, f, when)
		buf.WriteByte(' ')
	}
	if node.Else != nil {
		buf.WriteString("ELSE ")
		FormatNode(buf, f, node.Else)
		buf.WriteByte(' ')
	}
	buf.WriteString("END")
}

// When represents a WHEN sub-expression.
type When struct {
	Cond Expr
	Val  Expr
}

// Format implements the NodeFormatter interface.
func (node *When) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("WHEN ")
	FormatNode(buf, f, node.Cond)
	buf.WriteString(" THEN ")
	FormatNode(buf, f, node.Val)
}

// CastExpr represents a CAST(expr AS type) expression.
type CastExpr struct {
	Expr Expr
	Type ColumnType

	typeAnnotation
}

// Format implements the NodeFormatter interface.
func (node *CastExpr) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("CAST(")
	FormatNode(buf, f, node.Expr)
	buf.WriteString(" AS ")
	FormatNode(buf, f, node.Type)
	buf.WriteByte(')')
}

func (node *AliasedTableExpr) String() string { return AsString(node) }
func (node *ParenTableExpr) String() string   { return AsString(node) }
func (node *JoinTableExpr) String() string    { return AsString(node) }
func (node *AndExpr) String() string          { return AsString(node) }
func (node *Array) String() string            { return AsString(node) }
func (node *BinaryExpr) String() string       { return AsString(node) }
func (node *CaseExpr) String() string         { return AsString(node) }
func (node *CastExpr) String() string         { return AsString(node) }
func (node *CoalesceExpr) String() string     { return AsString(node) }
func (node *ComparisonExpr) String() string   { return AsString(node) }
func (node *DBool) String() string            { return AsString(node) }
func (node *DBytes) String() string           { return AsString(node) }
func (node *DDate) String() string            { return AsString(node) }
func (node *DDecimal) String() string         { return AsString(node) }
func (node *DFloat) String() string           { return AsString(node) }
func (node *DInt) String() string             { return AsString(node) }
func (node *DInterval) String() string        { return AsString(node) }
func (node *DString) String() string          { return AsString(node) }
func (node *DTimestamp) String() string       { return AsString(node) }
func (node *DTimestampTZ) String() string     { return AsString(node) }
func (node *DTuple) String() string           { return AsString(node) }
func (node *DValArg) String() string          { return AsString(node) }
func (node *ExistsExpr) String() string       { return AsString(node) }
func (node Exprs) String() string             { return AsString(node) }
func (node *FuncExpr) String() string         { return AsString(node) }
func (node *IfExpr) String() string           { return AsString(node) }
func (node *IndexedVar) String() string       { return AsString(node) }
func (node *IsOfTypeExpr) String() string     { return AsString(node) }
func (node Name) String() string              { return AsString(node) }
func (node *NotExpr) String() string          { return AsString(node) }
func (node *NullIfExpr) String() string       { return AsString(node) }
func (node *NumVal) String() string           { return AsString(node) }
func (node *OrExpr) String() string           { return AsString(node) }
func (node *OverlayExpr) String() string      { return AsString(node) }
func (node *ParenExpr) String() string        { return AsString(node) }
func (node *QualifiedName) String() string    { return AsString(node) }
func (node *RangeCond) String() string        { return AsString(node) }
func (node *Row) String() string              { return AsString(node) }
func (node *StrVal) String() string           { return AsString(node) }
func (node *Subquery) String() string         { return AsString(node) }
func (node *Tuple) String() string            { return AsString(node) }
func (node *UnaryExpr) String() string        { return AsString(node) }
func (node DefaultVal) String() string        { return AsString(node) }
func (node ValArg) String() string            { return AsString(node) }
func (node dNull) String() string             { return AsString(node) }
func (list NameList) String() string          { return AsString(list) }

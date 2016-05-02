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

// exprStrWithParen is a variant of e.String() which adds a set of outer parens
// if the expression involves an operator. It is used internally when the
// expression is part of another expression and we know it is preceded or
// followed by an operator.
func exprStrWithParen(e Expr) string {
	if _, ok := e.(operatorExpr); ok {
		return fmt.Sprintf("(%s)", e)
	}
	return e.String()
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

func (node *AndExpr) String() string {
	return fmt.Sprintf("%s AND %s", exprStrWithParen(node.Left), exprStrWithParen(node.Right))
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

func (node *OrExpr) String() string {
	return fmt.Sprintf("%s OR %s", exprStrWithParen(node.Left), exprStrWithParen(node.Right))
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

func (node *NotExpr) String() string {
	return fmt.Sprintf("NOT %s", exprStrWithParen(node.Expr))
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

func (node *ParenExpr) String() string {
	return fmt.Sprintf("(%s)", node.Expr)
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

func (node *ComparisonExpr) String() string {
	return fmt.Sprintf("%s %s %s", exprStrWithParen(node.Left), node.Operator,
		exprStrWithParen(node.Right))
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

func (node *RangeCond) String() string {
	notStr := ""
	if node.Not {
		notStr = "NOT "
	}
	return fmt.Sprintf("%s %sBETWEEN %s AND %s", exprStrWithParen(node.Left), notStr,
		exprStrWithParen(node.From), exprStrWithParen(node.To))
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

	typeAnnotation
}

func (node *ExistsExpr) String() string {
	return fmt.Sprintf("EXISTS %s", exprStrWithParen(node.Subquery))
}

// IfExpr represents an IF expression.
type IfExpr struct {
	Cond Expr
	True Expr
	Else Expr

	typeAnnotation
}

func (node *IfExpr) String() string {
	return fmt.Sprintf("IF(%s, %s, %s)", node.Cond, node.True, node.Else)
}

// NullIfExpr represents a NULLIF expression.
type NullIfExpr struct {
	Expr1 Expr
	Expr2 Expr

	typeAnnotation
}

func (node *NullIfExpr) String() string {
	return fmt.Sprintf("NULLIF(%s, %s)", node.Expr1, node.Expr2)
}

// CoalesceExpr represents a COALESCE or IFNULL expression.
type CoalesceExpr struct {
	Name  string
	Exprs Exprs

	typeAnnotation
}

func (node *CoalesceExpr) String() string {
	return fmt.Sprintf("%s(%s)", node.Name, node.Exprs)
}

// DefaultVal represents the DEFAULT expression.
type DefaultVal struct{}

func (node DefaultVal) String() string {
	return "DEFAULT"
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

func (node ValArg) String() string {
	return fmt.Sprintf("$%s", node.Name)
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

	if len(n.Indirect) > 1 {
		return fmt.Errorf("invalid table name: %s", n)
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
	n.setString()
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
	n.setString()
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

// ClearString causes String to return the current (possibly normalized) name instead of the
// original name (used for testing).
func (n *QualifiedName) ClearString() {
	n.origString = ""
}

func (n *QualifiedName) setString() {
	// We preserve the representation pre-normalization.
	if n.origString != "" {
		return
	}
	if n.Base == "" && len(n.Indirect) == 1 && n.Indirect[0] == unqualifiedStar {
		n.origString = n.Indirect[0].String()
	} else {
		n.origString = fmt.Sprintf("%s%s", n.Base, n.Indirect)
	}
}

func (n *QualifiedName) String() string {
	n.setString()
	return n.origString
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

// TableNameWithIndex represents a "table@index", used in statements that
// specifically refer to an index.
type TableNameWithIndex struct {
	Table *QualifiedName
	Index Name
}

func (n *TableNameWithIndex) String() string {
	return fmt.Sprintf("%s@%s", n.Table, n.Index)
}

// TableNameWithIndexList is a list of indexes.
type TableNameWithIndexList []*TableNameWithIndex

func (n TableNameWithIndexList) String() string {
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
type Tuple struct {
	Exprs Exprs

	types DTuple
}

func (node *Tuple) String() string {
	return fmt.Sprintf("(%s)", node.Exprs)
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

func (node *Row) String() string {
	return fmt.Sprintf("ROW(%s)", node.Exprs)
}

// ReturnType implements the TypedExpr interface.
func (node *Row) ReturnType() Datum {
	return &node.types
}

// Array represents an array constructor.
type Array struct {
	Exprs Exprs
}

func (node *Array) String() string {
	return fmt.Sprintf("ARRAY[%s]", node.Exprs)
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

func (node *Subquery) String() string {
	return node.Select.String()
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

func (node *BinaryExpr) String() string {
	return fmt.Sprintf("%s %s %s", exprStrWithParen(node.Left), node.Operator,
		exprStrWithParen(node.Right))
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

func (node *UnaryExpr) String() string {
	return fmt.Sprintf("%s %s", node.Operator, exprStrWithParen(node.Expr))
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

	typeAnnotation
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

	typeAnnotation
}

func (n *CastExpr) String() string {
	return fmt.Sprintf("CAST(%s AS %s)", n.Expr, n.Type)
}

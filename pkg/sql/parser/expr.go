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
	// The ctx parameter defines the context in which to perform type checking.
	// The desired parameter hints the desired type that the method's caller wants from
	// the resulting TypedExpr. It is not valid to call TypeCheck with a nil desired
	// type. Instead, call it with wildcard type TypeAny if no specific type is
	// desired. This restriction is also true of most methods and functions related
	// to type checking.
	TypeCheck(ctx *SemaContext, desired Type) (TypedExpr, error)
}

// TypedExpr represents a well-typed expression.
type TypedExpr interface {
	Expr
	// Eval evaluates an SQL expression. Expression evaluation is a
	// mostly straightforward walk over the parse tree. The only
	// significant complexity is the handling of types and implicit
	// conversions. See binOps and cmpOps for more details. Note that
	// expression evaluation returns an error if certain node types are
	// encountered: Placeholder, VarName (and related UnqualifiedStar,
	// UnresolvedName and AllColumnsSelector) or Subquery. These nodes
	// should be replaced prior to expression evaluation by an
	// appropriate WalkExpr. For example, Placeholder should be replace
	// by the argument passed from the client.
	Eval(*EvalContext) (Datum, error)
	// ResolvedType provides the type of the TypedExpr, which is the type of Datum
	// that the TypedExpr will return when evaluated.
	ResolvedType() Type
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

// operator is used to identify operators; used in sql.y.
type operator interface {
	operator()
}

var _ operator = UnaryOperator(0)
var _ operator = BinaryOperator(0)
var _ operator = ComparisonOperator(0)

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
	typ Type
}

func (ta typeAnnotation) ResolvedType() Type {
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
	binExprFmtWithParenAndSubOp(buf, f, e1, "", op, e2)
}

func binExprFmtWithParenAndSubOp(
	buf *bytes.Buffer, f FmtFlags, e1 Expr, subOp, op string, e2 Expr,
) {
	exprFmtWithParen(buf, f, e1)
	buf.WriteByte(' ')
	if subOp != "" {
		buf.WriteString(subOp)
		buf.WriteByte(' ')
	}
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
	node.typ = TypeBool
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
	node.typ = TypeBool
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
	node.typ = TypeBool
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

// StripParens strips any parentheses surrounding an expression and
// returns the inner expression. For instance:
//   1   -> 1
//  (1)  -> 1
// ((1)) -> 1
func StripParens(expr Expr) Expr {
	if p, ok := expr.(*ParenExpr); ok {
		return StripParens(p.Expr)
	}
	return expr
}

// ComparisonOperator represents a binary operator.
type ComparisonOperator int

func (ComparisonOperator) operator() {}

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
	ILike
	NotILike
	SimilarTo
	NotSimilarTo
	RegMatch
	NotRegMatch
	RegIMatch
	NotRegIMatch
	IsDistinctFrom
	IsNotDistinctFrom
	Is
	IsNot

	// The following operators will always be used with an associated SubOperator.
	// If Go had algebraic data types they would be defined in a self-contained
	// manner like:
	//
	// Any(ComparisonOperator)
	// Some(ComparisonOperator)
	// ...
	//
	// where the internal ComparisonOperator qualifies the behavior of the primary
	// operator. Instead, a secondary ComparisonOperator is optionally included in
	// ComparisonExpr for the cases where these operators are the primary op.
	//
	// ComparisonOperator.hasSubOperator returns true for ops in this group.
	Any
	Some
	All
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
	ILike:             "ILIKE",
	NotILike:          "NOT ILIKE",
	SimilarTo:         "SIMILAR TO",
	NotSimilarTo:      "NOT SIMILAR TO",
	RegMatch:          "~",
	NotRegMatch:       "!~",
	RegIMatch:         "~*",
	NotRegIMatch:      "!~*",
	IsDistinctFrom:    "IS DISTINCT FROM",
	IsNotDistinctFrom: "IS NOT DISTINCT FROM",
	Is:                "IS",
	IsNot:             "IS NOT",
	Any:               "ANY",
	Some:              "SOME",
	All:               "ALL",
}

func (i ComparisonOperator) String() string {
	if i < 0 || i > ComparisonOperator(len(comparisonOpName)-1) {
		return fmt.Sprintf("ComparisonOp(%d)", i)
	}
	return comparisonOpName[i]
}

// hasSubOperator returns if the ComparisonOperator is used with a sub-operator.
func (i ComparisonOperator) hasSubOperator() bool {
	switch i {
	case Any:
	case Some:
	case All:
	default:
		return false
	}
	return true
}

// ComparisonExpr represents a two-value comparison expression.
type ComparisonExpr struct {
	Operator    ComparisonOperator
	SubOperator ComparisonOperator // used for array operators (when Operator is Any, Some, or All)
	Left, Right Expr

	typeAnnotation
	fn CmpOp
}

func (*ComparisonExpr) operatorExpr() {}

// Format implements the NodeFormatter interface.
func (node *ComparisonExpr) Format(buf *bytes.Buffer, f FmtFlags) {
	opStr := node.Operator.String()
	if node.Operator.hasSubOperator() {
		binExprFmtWithParenAndSubOp(buf, f, node.Left, node.SubOperator.String(), opStr, node.Right)
	} else {
		binExprFmtWithParen(buf, f, node.Left, opStr, node.Right)
	}
}

// NewTypedComparisonExpr returns a new ComparisonExpr that is verified to be well-typed.
func NewTypedComparisonExpr(op ComparisonOperator, left, right TypedExpr) *ComparisonExpr {
	node := &ComparisonExpr{Operator: op, Left: left, Right: right}
	node.typ = TypeBool
	node.memoizeFn()
	return node
}

func (node *ComparisonExpr) memoizeFn() {
	fOp, fLeft, fRight, _, _ := foldComparisonExpr(node.Operator, node.Left, node.Right)
	leftRet, rightRet := fLeft.(TypedExpr).ResolvedType(), fRight.(TypedExpr).ResolvedType()
	switch node.Operator {
	case Is, IsNot, IsDistinctFrom, IsNotDistinctFrom:
		// Is and related operators do not memoize a CmpOp.
		return
	case Any, Some, All:
		// Array operators memoize the SubOperator's CmpOp.
		fOp, _, _, _, _ = foldComparisonExpr(node.SubOperator, nil, nil)
		rightRet = rightRet.(TArray).Typ
	}

	fn, ok := CmpOps[fOp].lookupImpl(leftRet, rightRet)
	if !ok {
		panic(fmt.Sprintf("lookup for ComparisonExpr %s's CmpOp failed",
			AsStringWithFlags(node, FmtShowTypes)))
	}
	node.fn = fn
}

// TypedLeft returns the ComparisonExpr's left expression as a TypedExpr.
func (node *ComparisonExpr) TypedLeft() TypedExpr {
	return node.Left.(TypedExpr)
}

// TypedRight returns the ComparisonExpr's right expression as a TypedExpr.
func (node *ComparisonExpr) TypedRight() TypedExpr {
	return node.Right.(TypedExpr)
}

// IsMixedTypeComparison returns true when the two sides of
// a comparison operator have different types.
func (node *ComparisonExpr) IsMixedTypeComparison() bool {
	switch node.Operator {
	case In, NotIn:
		tuple := node.TypedRight().ResolvedType().(TTuple)
		for _, typ := range tuple {
			if !sameTypeOrNull(node.TypedLeft().ResolvedType(), typ) {
				return true
			}
		}
		return false
	case Any, Some, All:
		array := node.TypedRight().ResolvedType().(TArray)
		return !sameTypeOrNull(node.TypedLeft().ResolvedType(), array.Typ)
	default:
		return !sameTypeOrNull(node.TypedLeft().ResolvedType(), node.TypedRight().ResolvedType())
	}
}

func sameTypeOrNull(left, right Type) bool {
	return left == TypeNull || right == TypeNull || left.Equivalent(right)
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
	exprFmtWithParen(buf, f, node.Expr)
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

// TypedTrueExpr returns the IfExpr's True expression as a TypedExpr.
func (node *IfExpr) TypedTrueExpr() TypedExpr {
	return node.True.(TypedExpr)
}

// TypedCondExpr returns the IfExpr's Cond expression as a TypedExpr.
func (node *IfExpr) TypedCondExpr() TypedExpr {
	return node.Cond.(TypedExpr)
}

// TypedElseExpr returns the IfExpr's Else expression as a TypedExpr.
func (node *IfExpr) TypedElseExpr() TypedExpr {
	return node.Else.(TypedExpr)
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

// TypedExprAt returns the expression at the specified index as a TypedExpr.
func (node *CoalesceExpr) TypedExprAt(idx int) TypedExpr {
	return node.Exprs[idx].(TypedExpr)
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

// ResolvedType implements the TypedExpr interface.
func (DefaultVal) ResolvedType() Type { return nil }

var _ VariableExpr = &Placeholder{}

// Placeholder represents a named placeholder.
type Placeholder struct {
	Name string

	typeAnnotation
}

// NewPlaceholder allocates a Placeholder.
func NewPlaceholder(name string) *Placeholder {
	return &Placeholder{Name: name}
}

// Variable implements the VariableExpr interface.
func (*Placeholder) Variable() {}

// Format implements the NodeFormatter interface.
func (node *Placeholder) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteByte('$')
	buf.WriteString(node.Name)
}

// ResolvedType implements the TypedExpr interface.
func (node *Placeholder) ResolvedType() Type {
	if node.typ == nil {
		node.typ = &TPlaceholder{Name: node.Name}
	}
	return node.typ
}

// Tuple represents a parenthesized list of expressions.
type Tuple struct {
	Exprs Exprs

	row   bool // indicates whether or not the tuple should be textually represented as a row.
	types TTuple
}

// Format implements the NodeFormatter interface.
func (node *Tuple) Format(buf *bytes.Buffer, f FmtFlags) {
	if node.row {
		buf.WriteString("ROW")
	}
	buf.WriteByte('(')
	FormatNode(buf, f, node.Exprs)
	buf.WriteByte(')')
}

// ResolvedType implements the TypedExpr interface.
func (node *Tuple) ResolvedType() Type {
	return node.types
}

// Array represents an array constructor.
type Array struct {
	Exprs Exprs

	typeAnnotation
}

// Format implements the NodeFormatter interface.
func (node *Array) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("ARRAY[")
	FormatNode(buf, f, node.Exprs)
	buf.WriteByte(']')
}

// ArrayFlatten represents a subquery array constructor.
type ArrayFlatten struct {
	Subquery Expr

	typeAnnotation
}

// Format implements the NodeFormatter interface.
func (node *ArrayFlatten) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("ARRAY")
	exprFmtWithParen(buf, f, node.Subquery)
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

// Variable implements the VariableExpr interface.
func (*Subquery) Variable() {}

// Format implements the NodeFormatter interface.
func (node *Subquery) Format(buf *bytes.Buffer, f FmtFlags) {
	FormatNode(buf, f, node.Select)
}

// BinaryOperator represents a binary operator.
type BinaryOperator int

func (BinaryOperator) operator() {}

// BinaryExpr.Operator
const (
	Bitand BinaryOperator = iota
	Bitor
	Bitxor
	Plus
	Minus
	Mult
	Div
	FloorDiv
	Mod
	Pow
	Concat
	LShift
	RShift
)

var binaryOpName = [...]string{
	Bitand:   "&",
	Bitor:    "|",
	Bitxor:   "#",
	Plus:     "+",
	Minus:    "-",
	Mult:     "*",
	Div:      "/",
	FloorDiv: "//",
	Mod:      "%",
	Pow:      "^",
	Concat:   "||",
	LShift:   "<<",
	RShift:   ">>",
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

// TypedLeft returns the BinaryExpr's left expression as a TypedExpr.
func (node *BinaryExpr) TypedLeft() TypedExpr {
	return node.Left.(TypedExpr)
}

// TypedRight returns the BinaryExpr's right expression as a TypedExpr.
func (node *BinaryExpr) TypedRight() TypedExpr {
	return node.Right.(TypedExpr)
}

func (*BinaryExpr) operatorExpr() {}

func (node *BinaryExpr) memoizeFn() {
	leftRet, rightRet := node.Left.(TypedExpr).ResolvedType(), node.Right.(TypedExpr).ResolvedType()
	fn, ok := BinOps[node.Operator].lookupImpl(leftRet, rightRet)
	if !ok {
		panic(fmt.Sprintf("lookup for BinaryExpr %s's BinOp failed",
			AsStringWithFlags(node, FmtShowTypes)))
	}
	node.fn = fn
}

// newBinExprIfValidOverload constructs a new BinaryExpr if and only
// if the pair of arguments have a valid implementation for the given
// BinaryOperator.
func newBinExprIfValidOverload(op BinaryOperator, left TypedExpr, right TypedExpr) *BinaryExpr {
	leftRet, rightRet := left.ResolvedType(), right.ResolvedType()
	fn, ok := BinOps[op].lookupImpl(leftRet, rightRet)
	if ok {
		expr := &BinaryExpr{
			Operator: op,
			Left:     left,
			Right:    right,
			fn:       fn,
		}
		expr.typ = returnTypeToFixedType(fn.returnType())
		return expr
	}
	return nil
}

// Format implements the NodeFormatter interface.
func (node *BinaryExpr) Format(buf *bytes.Buffer, f FmtFlags) {
	binExprFmtWithParen(buf, f, node.Left, node.Operator.String(), node.Right)
}

// UnaryOperator represents a unary operator.
type UnaryOperator int

func (UnaryOperator) operator() {}

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

// TypedInnerExpr returns the UnaryExpr's inner expression as a TypedExpr.
func (node *UnaryExpr) TypedInnerExpr() TypedExpr {
	return node.Expr.(TypedExpr)
}

// FuncExpr represents a function call.
type FuncExpr struct {
	Func  ResolvableFunctionReference
	Type  funcType
	Exprs Exprs
	// Filter is used for filters on aggregates: SUM(k) FILTER (WHERE k > 0)
	Filter    Expr
	WindowDef *WindowDef

	typeAnnotation
	fn Builtin
}

// GetAggregateConstructor exposes the AggregateFunc field for use by
// the group node in package sql.
func (node *FuncExpr) GetAggregateConstructor() func(*EvalContext) AggregateFunc {
	if node.fn.AggregateFunc == nil {
		return nil
	}
	return func(evalCtx *EvalContext) AggregateFunc {
		types := typesOfExprs(node.Exprs)
		return node.fn.AggregateFunc(types, evalCtx)
	}
}

// GetWindowConstructor returns a window function constructor if the
// FuncExpr is a built-in window function.
func (node *FuncExpr) GetWindowConstructor() func(*EvalContext) WindowFunc {
	if node.fn.WindowFunc == nil {
		return nil
	}
	return func(evalCtx *EvalContext) WindowFunc {
		types := typesOfExprs(node.Exprs)
		return node.fn.WindowFunc(types, evalCtx)
	}
}

func typesOfExprs(exprs Exprs) []Type {
	types := make([]Type, len(exprs))
	for i, expr := range exprs {
		types[i] = expr.(TypedExpr).ResolvedType()
	}
	return types
}

// IsWindowFunctionApplication returns if the function is being applied as a window function.
func (node *FuncExpr) IsWindowFunctionApplication() bool {
	return node.WindowDef != nil
}

// IsImpure returns whether the function application is impure, meaning that it
// potentially returns a different value when called in the same statement with
// the same parameters.
func (node *FuncExpr) IsImpure() bool {
	return node.fn.impure
}

// IsDistSQLBlacklist returns whether the function is not supported by DistSQL.
func (node *FuncExpr) IsDistSQLBlacklist() bool {
	return node.fn.DistSQLBlacklist()
}

type funcType int

// FuncExpr.Type
const (
	_ funcType = iota
	DistinctFuncType
	AllFuncType
)

var funcTypeName = [...]string{
	DistinctFuncType: "DISTINCT",
	AllFuncType:      "ALL",
}

// Format implements the NodeFormatter interface.
func (node *FuncExpr) Format(buf *bytes.Buffer, f FmtFlags) {
	var typ string
	if node.Type != 0 {
		typ = funcTypeName[node.Type] + " "
	}
	fmtDisableAnonymize := *f
	fmtDisableAnonymize.anonymize = false
	FormatNode(buf, &fmtDisableAnonymize, node.Func)
	buf.WriteByte('(')
	buf.WriteString(typ)
	FormatNode(buf, f, node.Exprs)
	buf.WriteByte(')')
	if window := node.WindowDef; window != nil {
		buf.WriteString(" OVER ")
		if window.Name != "" {
			FormatNode(buf, f, window.Name)
		} else {
			FormatNode(buf, f, window)
		}
	}
	if node.Filter != nil {
		buf.WriteString(" FILTER (WHERE ")
		FormatNode(buf, f, node.Filter)
		buf.WriteString(")")
	}
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

type castSyntaxMode int

const (
	castExplicit castSyntaxMode = iota
	castShort
	castPrepend
)

// CastExpr represents a CAST(expr AS type) expression.
type CastExpr struct {
	Expr Expr
	Type CastTargetType

	typeAnnotation
	syntaxMode castSyntaxMode
}

// Format implements the NodeFormatter interface.
func (node *CastExpr) Format(buf *bytes.Buffer, f FmtFlags) {
	switch node.syntaxMode {
	case castPrepend:
		// This is a special case for things like INTERVAL '1s'. These only work
		// with string constats; if the underlying expression was changed, we fall
		// back to the short syntax.
		if _, ok := node.Expr.(*StrVal); ok {
			FormatNode(buf, f, node.Type)
			buf.WriteByte(' ')
			FormatNode(buf, f, node.Expr)
			break
		}
		fallthrough
	case castShort:
		exprFmtWithParen(buf, f, node.Expr)
		buf.WriteString("::")
		FormatNode(buf, f, node.Type)
	default:
		buf.WriteString("CAST(")
		FormatNode(buf, f, node.Expr)
		buf.WriteString(" AS ")
		FormatNode(buf, f, node.Type)
		buf.WriteByte(')')
	}
}

func (node *CastExpr) castType() Type {
	return CastTargetToDatumType(node.Type)
}

var (
	boolCastTypes = []Type{TypeNull, TypeBool, TypeInt, TypeFloat, TypeDecimal, TypeString, TypeCollatedString}
	intCastTypes  = []Type{TypeNull, TypeBool, TypeInt, TypeFloat, TypeDecimal, TypeString, TypeCollatedString,
		TypeTimestamp, TypeTimestampTZ, TypeDate, TypeInterval, TypeOid}
	floatCastTypes = []Type{TypeNull, TypeBool, TypeInt, TypeFloat, TypeDecimal, TypeString, TypeCollatedString,
		TypeTimestamp, TypeTimestampTZ, TypeDate, TypeInterval}
	decimalCastTypes = []Type{TypeNull, TypeBool, TypeInt, TypeFloat, TypeDecimal, TypeString, TypeCollatedString,
		TypeTimestamp, TypeTimestampTZ, TypeDate, TypeInterval}
	stringCastTypes = []Type{TypeNull, TypeBool, TypeInt, TypeFloat, TypeDecimal, TypeString, TypeCollatedString,
		TypeBytes, TypeTimestamp, TypeTimestampTZ, TypeInterval, TypeDate, TypeOid}
	bytesCastTypes     = []Type{TypeNull, TypeString, TypeCollatedString, TypeBytes}
	dateCastTypes      = []Type{TypeNull, TypeString, TypeCollatedString, TypeDate, TypeTimestamp, TypeTimestampTZ, TypeInt}
	timestampCastTypes = []Type{TypeNull, TypeString, TypeCollatedString, TypeDate, TypeTimestamp, TypeTimestampTZ, TypeInt}
	intervalCastTypes  = []Type{TypeNull, TypeString, TypeCollatedString, TypeInt, TypeInterval}
	oidCastTypes       = []Type{TypeNull, TypeString, TypeCollatedString, TypeInt, TypeOid}
)

// validCastTypes returns a set of types that can be cast into the provided type.
func validCastTypes(t Type) []Type {
	switch UnwrapType(t) {
	case TypeBool:
		return boolCastTypes
	case TypeInt:
		return intCastTypes
	case TypeFloat:
		return floatCastTypes
	case TypeDecimal:
		return decimalCastTypes
	case TypeString:
		return stringCastTypes
	case TypeBytes:
		return bytesCastTypes
	case TypeDate:
		return dateCastTypes
	case TypeTimestamp, TypeTimestampTZ:
		return timestampCastTypes
	case TypeInterval:
		return intervalCastTypes
	case TypeOid, TypeRegClass, TypeRegNamespace, TypeRegProc, TypeRegProcedure, TypeRegType:
		return oidCastTypes
	default:
		// TODO(eisen): currently dead -- there is no syntax yet for casting
		// directly to collated string.
		if t.FamilyEqual(TypeCollatedString) {
			return stringCastTypes
		}
		return nil
	}
}

// ArraySubscripts represents a sequence of one or more array subscripts.
type ArraySubscripts []*ArraySubscript

// Format implements the NodeFormatter interface.
func (a ArraySubscripts) Format(buf *bytes.Buffer, f FmtFlags) {
	for _, s := range a {
		FormatNode(buf, f, s)
	}
}

// IndirectionExpr represents a subscript expression.
type IndirectionExpr struct {
	Expr        Expr
	Indirection ArraySubscripts

	typeAnnotation
}

// Format implements the NodeFormatter interface.
func (node *IndirectionExpr) Format(buf *bytes.Buffer, f FmtFlags) {
	exprFmtWithParen(buf, f, node.Expr)
	FormatNode(buf, f, node.Indirection)
}

type annotateSyntaxMode int

const (
	annotateExplicit annotateSyntaxMode = iota
	annotateShort
)

// AnnotateTypeExpr represents a ANNOTATE_TYPE(expr, type) expression.
type AnnotateTypeExpr struct {
	Expr Expr
	Type CastTargetType

	syntaxMode annotateSyntaxMode
}

// Format implements the NodeFormatter interface.
func (node *AnnotateTypeExpr) Format(buf *bytes.Buffer, f FmtFlags) {
	switch node.syntaxMode {
	case annotateShort:
		exprFmtWithParen(buf, f, node.Expr)
		buf.WriteString(":::")
		FormatNode(buf, f, node.Type)

	default:
		buf.WriteString("ANNOTATE_TYPE(")
		FormatNode(buf, f, node.Expr)
		buf.WriteString(", ")
		FormatNode(buf, f, node.Type)
		buf.WriteByte(')')
	}
}

// TypedInnerExpr returns the AnnotateTypeExpr's inner expression as a TypedExpr.
func (node *AnnotateTypeExpr) TypedInnerExpr() TypedExpr {
	return node.Expr.(TypedExpr)
}

func (node *AnnotateTypeExpr) annotationType() Type {
	return CastTargetToDatumType(node.Type)
}

// CollateExpr represents an (expr COLLATE locale) expression.
type CollateExpr struct {
	Expr   Expr
	Locale string

	typeAnnotation
}

// Format implements the NodeFormatter interface.
func (node *CollateExpr) Format(buf *bytes.Buffer, f FmtFlags) {
	exprFmtWithParen(buf, f, node.Expr)
	buf.WriteString(" COLLATE ")
	encodeSQLIdent(buf, node.Locale)
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
func (node *CollateExpr) String() string      { return AsString(node) }
func (node *ComparisonExpr) String() string   { return AsString(node) }
func (node *Datums) String() string           { return AsString(node) }
func (node *DBool) String() string            { return AsString(node) }
func (node *DBytes) String() string           { return AsString(node) }
func (node *DDate) String() string            { return AsString(node) }
func (node *DDecimal) String() string         { return AsString(node) }
func (node *DFloat) String() string           { return AsString(node) }
func (node *DInt) String() string             { return AsString(node) }
func (node *DInterval) String() string        { return AsString(node) }
func (node *DString) String() string          { return AsString(node) }
func (node *DCollatedString) String() string  { return AsString(node) }
func (node *DTimestamp) String() string       { return AsString(node) }
func (node *DTimestampTZ) String() string     { return AsString(node) }
func (node *DTuple) String() string           { return AsString(node) }
func (node *DArray) String() string           { return AsString(node) }
func (node *DTable) String() string           { return AsString(node) }
func (node *DOid) String() string             { return AsString(node) }
func (node *DOidWrapper) String() string      { return AsString(node) }
func (node *ExistsExpr) String() string       { return AsString(node) }
func (node Exprs) String() string             { return AsString(node) }
func (node *ArrayFlatten) String() string     { return AsString(node) }
func (node *FuncExpr) String() string         { return AsString(node) }
func (node *IfExpr) String() string           { return AsString(node) }
func (node *IndexedVar) String() string       { return AsString(node) }
func (node *IndirectionExpr) String() string  { return AsString(node) }
func (node *IsOfTypeExpr) String() string     { return AsString(node) }
func (node Name) String() string              { return AsString(node) }
func (node *NotExpr) String() string          { return AsString(node) }
func (node *NullIfExpr) String() string       { return AsString(node) }
func (node *NumVal) String() string           { return AsString(node) }
func (node *OrExpr) String() string           { return AsString(node) }
func (node *ParenExpr) String() string        { return AsString(node) }
func (node *RangeCond) String() string        { return AsString(node) }
func (node *StrVal) String() string           { return AsString(node) }
func (node *Subquery) String() string         { return AsString(node) }
func (node *Tuple) String() string            { return AsString(node) }
func (node *AnnotateTypeExpr) String() string { return AsString(node) }
func (node *UnaryExpr) String() string        { return AsString(node) }
func (node DefaultVal) String() string        { return AsString(node) }
func (node *Placeholder) String() string      { return AsString(node) }
func (node dNull) String() string             { return AsString(node) }
func (list NameList) String() string          { return AsString(list) }

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

package tree

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util"
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
	// type. Instead, call it with wildcard type types.Any if no specific type is
	// desired. This restriction is also true of most methods and functions related
	// to type checking.
	TypeCheck(ctx *SemaContext, desired types.T) (TypedExpr, error)
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
	ResolvedType() types.T
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

// Operator is used to identify Operators; used in sql.y.
type Operator interface {
	operator()
}

var _ Operator = UnaryOperator(0)
var _ Operator = BinaryOperator(0)
var _ Operator = ComparisonOperator(0)

// exprFmtWithParen is a variant of Format() which adds a set of outer parens
// if the expression involves an operator. It is used internally when the
// expression is part of another expression and we know it is preceded or
// followed by an operator.
func exprFmtWithParen(ctx *FmtCtx, e Expr) {
	if _, ok := e.(operatorExpr); ok {
		ctx.WriteByte('(')
		ctx.FormatNode(e)
		ctx.WriteByte(')')
	} else {
		ctx.FormatNode(e)
	}
}

// typeAnnotation is an embeddable struct to provide a TypedExpr with a dynamic
// type annotation.
type typeAnnotation struct {
	typ types.T
}

func (ta typeAnnotation) ResolvedType() types.T {
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

func binExprFmtWithParen(ctx *FmtCtx, e1 Expr, op string, e2 Expr, pad bool) {
	exprFmtWithParen(ctx, e1)
	if pad {
		ctx.WriteByte(' ')
	}
	ctx.WriteString(op)
	if pad {
		ctx.WriteByte(' ')
	}
	exprFmtWithParen(ctx, e2)
}

func binExprFmtWithParenAndSubOp(ctx *FmtCtx, e1 Expr, subOp, op string, e2 Expr) {
	exprFmtWithParen(ctx, e1)
	ctx.WriteByte(' ')
	if subOp != "" {
		ctx.WriteString(subOp)
		ctx.WriteByte(' ')
	}
	ctx.WriteString(op)
	ctx.WriteByte(' ')
	exprFmtWithParen(ctx, e2)
}

// Format implements the NodeFormatter interface.
func (node *AndExpr) Format(ctx *FmtCtx) {
	binExprFmtWithParen(ctx, node.Left, "AND", node.Right, true)
}

// NewTypedAndExpr returns a new AndExpr that is verified to be well-typed.
func NewTypedAndExpr(left, right TypedExpr) *AndExpr {
	node := &AndExpr{Left: left, Right: right}
	node.typ = types.Bool
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
func (node *OrExpr) Format(ctx *FmtCtx) {
	binExprFmtWithParen(ctx, node.Left, "OR", node.Right, true)
}

// NewTypedOrExpr returns a new OrExpr that is verified to be well-typed.
func NewTypedOrExpr(left, right TypedExpr) *OrExpr {
	node := &OrExpr{Left: left, Right: right}
	node.typ = types.Bool
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
func (node *NotExpr) Format(ctx *FmtCtx) {
	ctx.WriteString("NOT ")
	exprFmtWithParen(ctx, node.Expr)
}

// NewTypedNotExpr returns a new NotExpr that is verified to be well-typed.
func NewTypedNotExpr(expr TypedExpr) *NotExpr {
	node := &NotExpr{Expr: expr}
	node.typ = types.Bool
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
func (node *ParenExpr) Format(ctx *FmtCtx) {
	ctx.WriteByte('(')
	ctx.FormatNode(node.Expr)
	ctx.WriteByte(')')
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
	Contains
	ContainedBy
	JSONExists
	JSONSomeExists
	JSONAllExists

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

	NumComparisonOperators
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
	Contains:          "@>",
	ContainedBy:       "<@",
	JSONExists:        "?",
	JSONSomeExists:    "?|",
	JSONAllExists:     "?&",
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

// Inverse returns the inverse of this comparison operator if it exists. The
// second return value is true if it exists, and false otherwise.
func (i ComparisonOperator) Inverse() (ComparisonOperator, bool) {
	inverse, ok := cmpOpsInverse[i]
	return inverse, ok
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
func (node *ComparisonExpr) Format(ctx *FmtCtx) {
	opStr := node.Operator.String()
	if node.Operator == IsDistinctFrom && (node.Right == DNull || node.Right == DBoolTrue || node.Right == DBoolFalse) {
		opStr = "IS NOT"
	} else if node.Operator == IsNotDistinctFrom && (node.Right == DNull || node.Right == DBoolTrue || node.Right == DBoolFalse) {
		opStr = "IS"
	}
	if node.Operator.hasSubOperator() {
		binExprFmtWithParenAndSubOp(ctx, node.Left, node.SubOperator.String(), opStr, node.Right)
	} else {
		binExprFmtWithParen(ctx, node.Left, opStr, node.Right, true)
	}
}

// NewTypedComparisonExpr returns a new ComparisonExpr that is verified to be well-typed.
func NewTypedComparisonExpr(op ComparisonOperator, left, right TypedExpr) *ComparisonExpr {
	node := &ComparisonExpr{Operator: op, Left: left, Right: right}
	node.typ = types.Bool
	node.memoizeFn()
	return node
}

// NewTypedComparisonExprWithSubOp returns a new ComparisonExpr that is verified to be well-typed.
func NewTypedComparisonExprWithSubOp(
	op, subOp ComparisonOperator, left, right TypedExpr,
) *ComparisonExpr {
	node := &ComparisonExpr{Operator: op, SubOperator: subOp, Left: left, Right: right}
	node.typ = types.Bool
	node.memoizeFn()
	return node
}

func (node *ComparisonExpr) memoizeFn() {
	fOp, fLeft, fRight, _, _ := foldComparisonExpr(node.Operator, node.Left, node.Right)
	leftRet, rightRet := fLeft.(TypedExpr).ResolvedType(), fRight.(TypedExpr).ResolvedType()
	switch node.Operator {
	case Any, Some, All:
		// Array operators memoize the SubOperator's CmpOp.
		fOp, _, _, _, _ = foldComparisonExpr(node.SubOperator, nil, nil)
		// The right operand is either an array or a tuple/subquery.
		switch t := rightRet.(type) {
		case types.TArray:
			// For example:
			//   x = ANY(ARRAY[1,2])
			rightRet = t.Typ
		case types.TTuple:
			// For example:
			//   x = ANY(SELECT y FROM t)
			//   x = ANY(1,2)
			rightRet = t[0]
		}
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

// RangeCond represents a BETWEEN [SYMMETRIC] or a NOT BETWEEN [SYMMETRIC]
// expression.
type RangeCond struct {
	Not       bool
	Symmetric bool
	Left      Expr
	From, To  Expr

	typeAnnotation
}

func (*RangeCond) operatorExpr() {}

// Format implements the NodeFormatter interface.
func (node *RangeCond) Format(ctx *FmtCtx) {
	notStr := " BETWEEN "
	if node.Not {
		notStr = " NOT BETWEEN "
	}
	exprFmtWithParen(ctx, node.Left)
	ctx.WriteString(notStr)
	if node.Symmetric {
		ctx.WriteString("SYMMETRIC ")
	}
	binExprFmtWithParen(ctx, node.From, "AND", node.To, true)
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
	Types []coltypes.T

	typeAnnotation
}

func (*IsOfTypeExpr) operatorExpr() {}

// Format implements the NodeFormatter interface.
func (node *IsOfTypeExpr) Format(ctx *FmtCtx) {
	exprFmtWithParen(ctx, node.Expr)
	ctx.WriteString(" IS")
	if node.Not {
		ctx.WriteString(" NOT")
	}
	ctx.WriteString(" OF (")
	for i, t := range node.Types {
		if i > 0 {
			ctx.WriteString(", ")
		}
		t.Format(ctx.Buffer, ctx.flags.EncodeFlags())
	}
	ctx.WriteByte(')')
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
func (node *IfExpr) Format(ctx *FmtCtx) {
	ctx.WriteString("IF(")
	ctx.FormatNode(node.Cond)
	ctx.WriteString(", ")
	ctx.FormatNode(node.True)
	ctx.WriteString(", ")
	ctx.FormatNode(node.Else)
	ctx.WriteByte(')')
}

// NullIfExpr represents a NULLIF expression.
type NullIfExpr struct {
	Expr1 Expr
	Expr2 Expr

	typeAnnotation
}

// Format implements the NodeFormatter interface.
func (node *NullIfExpr) Format(ctx *FmtCtx) {
	ctx.WriteString("NULLIF(")
	ctx.FormatNode(node.Expr1)
	ctx.WriteString(", ")
	ctx.FormatNode(node.Expr2)
	ctx.WriteByte(')')
}

// CoalesceExpr represents a COALESCE or IFNULL expression.
type CoalesceExpr struct {
	Name  string
	Exprs Exprs

	typeAnnotation
}

// NewTypedCoalesceExpr returns a CoalesceExpr that is well-typed.
func NewTypedCoalesceExpr(typedExprs TypedExprs, typ types.T) *CoalesceExpr {
	c := &CoalesceExpr{
		Name:  "COALESCE",
		Exprs: make(Exprs, len(typedExprs)),
	}
	for i := range typedExprs {
		c.Exprs[i] = typedExprs[i]
	}
	c.typ = typ
	return c
}

// NewTypedArray returns an Array that is well-typed.
func NewTypedArray(typedExprs TypedExprs, typ types.T) *Array {
	c := &Array{
		Exprs: make(Exprs, len(typedExprs)),
	}
	for i := range typedExprs {
		c.Exprs[i] = typedExprs[i]
	}
	c.typ = typ
	return c
}

// TypedExprAt returns the expression at the specified index as a TypedExpr.
func (node *CoalesceExpr) TypedExprAt(idx int) TypedExpr {
	return node.Exprs[idx].(TypedExpr)
}

// Format implements the NodeFormatter interface.
func (node *CoalesceExpr) Format(ctx *FmtCtx) {
	ctx.WriteString(node.Name)
	ctx.WriteByte('(')
	ctx.FormatNode(&node.Exprs)
	ctx.WriteByte(')')
}

// DefaultVal represents the DEFAULT expression.
type DefaultVal struct{}

// Format implements the NodeFormatter interface.
func (node DefaultVal) Format(ctx *FmtCtx) {
	ctx.WriteString("DEFAULT")
}

// ResolvedType implements the TypedExpr interface.
func (DefaultVal) ResolvedType() types.T { return nil }

// MaxVal represents the MAXVALUE expression.
type MaxVal struct{}

// Format implements the NodeFormatter interface.
func (node MaxVal) Format(ctx *FmtCtx) {
	ctx.WriteString("MAXVALUE")
}

// MinVal represents the MINVALUE expression.
type MinVal struct{}

// Format implements the NodeFormatter interface.
func (node MinVal) Format(ctx *FmtCtx) {
	ctx.WriteString("MINVALUE")
}

// Placeholder represents a named placeholder.
type Placeholder struct {
	Name string

	typeAnnotation
}

// NewPlaceholder allocates a Placeholder.
func NewPlaceholder(name string) *Placeholder {
	return &Placeholder{Name: name}
}

// Format implements the NodeFormatter interface.
func (node *Placeholder) Format(ctx *FmtCtx) {
	if ctx.placeholderFormat != nil {
		ctx.placeholderFormat(ctx, node)
		return
	}
	ctx.WriteByte('$')
	ctx.WriteString(node.Name)
}

// ResolvedType implements the TypedExpr interface.
func (node *Placeholder) ResolvedType() types.T {
	if node.typ == nil {
		node.typ = &types.TPlaceholder{Name: node.Name}
	}
	return node.typ
}

// Tuple represents a parenthesized list of expressions.
type Tuple struct {
	Exprs Exprs
	// Row indicates whether or not the tuple should be textually represented as
	// ROW ( ... ).
	Row bool

	types types.TTuple
}

// NewTypedTuple returns a new Tuple that is verified to be well-typed.
func NewTypedTuple(typedExprs TypedExprs) *Tuple {
	node := &Tuple{
		Exprs: make(Exprs, len(typedExprs)),
		types: make(types.TTuple, len(typedExprs)),
	}
	for i := range typedExprs {
		node.Exprs[i] = typedExprs[i].(Expr)
		node.types[i] = typedExprs[i].ResolvedType()
	}
	return node
}

// Format implements the NodeFormatter interface.
func (node *Tuple) Format(ctx *FmtCtx) {
	if node.Row {
		ctx.WriteString("ROW")
	}
	ctx.WriteByte('(')
	ctx.FormatNode(&node.Exprs)
	ctx.WriteByte(')')
}

// ResolvedType implements the TypedExpr interface.
func (node *Tuple) ResolvedType() types.T {
	return node.types
}

// Truncate returns a new Tuple that contains only a prefix of the original
// expressions. E.g.
//   Tuple:       (1, 2, 3)
//   Truncate(2): (1, 2)
func (node *Tuple) Truncate(prefix int) *Tuple {
	return &Tuple{
		Exprs: append(Exprs(nil), node.Exprs[:prefix]...),
		Row:   node.Row,
		types: append(types.TTuple(nil), node.types[:prefix]...),
	}
}

// Project returns a new Tuple that contains a subset of the original
// expressions. E.g.
//  Tuple:           (1, 2, 3)
//  Project({0, 2}): (1, 3)
func (node *Tuple) Project(set util.FastIntSet) *Tuple {
	t := &Tuple{
		Exprs: make(Exprs, 0, set.Len()),
		Row:   node.Row,
		types: make(types.TTuple, 0, set.Len()),
	}
	for i, ok := set.Next(0); ok; i, ok = set.Next(i + 1) {
		t.Exprs = append(t.Exprs, node.Exprs[i])
		t.types = append(t.types, node.types[i])
	}
	return t
}

// Array represents an array constructor.
type Array struct {
	Exprs Exprs

	typeAnnotation
}

// Format implements the NodeFormatter interface.
func (node *Array) Format(ctx *FmtCtx) {
	ctx.WriteString("ARRAY[")
	ctx.FormatNode(&node.Exprs)
	ctx.WriteByte(']')
}

// ArrayFlatten represents a subquery array constructor.
type ArrayFlatten struct {
	Subquery Expr

	typeAnnotation
}

// Format implements the NodeFormatter interface.
func (node *ArrayFlatten) Format(ctx *FmtCtx) {
	ctx.WriteString("ARRAY ")
	exprFmtWithParen(ctx, node.Subquery)
}

// Exprs represents a list of value expressions. It's not a valid expression
// because it's not parenthesized.
type Exprs []Expr

// Format implements the NodeFormatter interface.
func (node *Exprs) Format(ctx *FmtCtx) {
	for i, n := range *node {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(n)
	}
}

// TypedExprs represents a list of well-typed value expressions. It's not a valid expression
// because it's not parenthesized.
type TypedExprs []TypedExpr

func (node *TypedExprs) String() string {
	var prefix string
	var buf bytes.Buffer
	for _, n := range *node {
		fmt.Fprintf(&buf, "%s%s", prefix, n)
		prefix = ", "
	}
	return buf.String()
}

// Subquery represents a subquery.
type Subquery struct {
	Select SelectStatement
	Exists bool

	// Idx is a query-unique index for the subquery.
	// Subqueries are 1-indexed to ensure that the default
	// value 0 can be used to detect uninitialized subqueries.
	Idx int

	typeAnnotation
}

// SetType forces the type annotation on the Subquery node.
func (node *Subquery) SetType(t types.T) {
	node.typ = t
}

// Variable implements the VariableExpr interface.
func (*Subquery) Variable() {}

// Format implements the NodeFormatter interface.
func (node *Subquery) Format(ctx *FmtCtx) {
	if ctx.HasFlags(FmtSymbolicSubqueries) {
		ctx.Printf("@S%d", node.Idx)
	} else {
		// Ensure that type printing is disabled during the recursion, as
		// the type annotations are not available in subqueries.
		noTypesCtx := *ctx
		noTypesCtx.flags &= ^FmtShowTypes

		if node.Exists {
			noTypesCtx.WriteString("EXISTS ")
		}
		noTypesCtx.FormatNode(node.Select)
	}
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
	JSONFetchVal
	JSONFetchText
	JSONFetchValPath
	JSONFetchTextPath

	NumBinaryOperators
)

var binaryOpName = [...]string{
	Bitand:            "&",
	Bitor:             "|",
	Bitxor:            "#",
	Plus:              "+",
	Minus:             "-",
	Mult:              "*",
	Div:               "/",
	FloorDiv:          "//",
	Mod:               "%",
	Pow:               "^",
	Concat:            "||",
	LShift:            "<<",
	RShift:            ">>",
	JSONFetchVal:      "->",
	JSONFetchText:     "->>",
	JSONFetchValPath:  "#>",
	JSONFetchTextPath: "#>>",
}

func (i BinaryOperator) isPadded() bool {
	return !(i == JSONFetchVal || i == JSONFetchText || i == JSONFetchValPath || i == JSONFetchTextPath)
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

// NewTypedBinaryExpr returns a new BinaryExpr that is well-typed.
func NewTypedBinaryExpr(op BinaryOperator, left, right TypedExpr, typ types.T) *BinaryExpr {
	node := &BinaryExpr{Operator: op, Left: left, Right: right}
	node.typ = typ
	node.memoizeFn()
	return node
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
func (node *BinaryExpr) Format(ctx *FmtCtx) {
	binExprFmtWithParen(ctx, node.Left, node.Operator.String(), node.Right, node.Operator.isPadded())
}

// UnaryOperator represents a unary operator.
type UnaryOperator int

func (UnaryOperator) operator() {}

// UnaryExpr.Operator
const (
	UnaryPlus UnaryOperator = iota
	UnaryMinus
	UnaryComplement

	NumUnaryOperators
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
func (node *UnaryExpr) Format(ctx *FmtCtx) {
	ctx.WriteString(node.Operator.String())
	exprFmtWithParen(ctx, node.Expr)
}

// TypedInnerExpr returns the UnaryExpr's inner expression as a TypedExpr.
func (node *UnaryExpr) TypedInnerExpr() TypedExpr {
	return node.Expr.(TypedExpr)
}

// NewTypedUnaryExpr returns a new UnaryExpr that is well-typed.
func NewTypedUnaryExpr(op UnaryOperator, expr TypedExpr, typ types.T) *UnaryExpr {
	node := &UnaryExpr{Operator: op, Expr: expr}
	node.typ = typ
	innerType := expr.ResolvedType()
	for _, o := range UnaryOps[op] {
		o := o.(UnaryOp)
		if innerType.Equivalent(o.Typ) && node.typ.Equivalent(o.ReturnType) {
			node.fn = o
			return node
		}
	}
	panic(fmt.Sprintf("invalid TypedExpr with unary op %d: %s", op, expr))
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
	fn *Builtin
}

// NewTypedFuncExpr returns a FuncExpr that is already well-typed and resolved.
func NewTypedFuncExpr(
	ref ResolvableFunctionReference,
	aggQualifier funcType,
	exprs TypedExprs,
	filter TypedExpr,
	windowDef *WindowDef,
	typ types.T,
	builtin *Builtin,
) *FuncExpr {
	f := &FuncExpr{
		Func:           ref,
		Type:           aggQualifier,
		Exprs:          make(Exprs, len(exprs)),
		Filter:         filter,
		WindowDef:      windowDef,
		typeAnnotation: typeAnnotation{typ: typ},
		fn:             builtin,
	}
	for i, e := range exprs {
		f.Exprs[i] = e
	}
	return f
}

// ResolvedBuiltin returns the builtin definition; can only be called after
// Resolve (which happens during TypeCheck).
func (node *FuncExpr) ResolvedBuiltin() *Builtin {
	return node.fn
}

// GetAggregateConstructor exposes the AggregateFunc field for use by
// the group node in package sql.
func (node *FuncExpr) GetAggregateConstructor() func(*EvalContext) AggregateFunc {
	if node.fn == nil || node.fn.AggregateFunc == nil {
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
	if node.fn == nil || node.fn.WindowFunc == nil {
		return nil
	}
	return func(evalCtx *EvalContext) WindowFunc {
		types := typesOfExprs(node.Exprs)
		return node.fn.WindowFunc(types, evalCtx)
	}
}

func typesOfExprs(exprs Exprs) []types.T {
	types := make([]types.T, len(exprs))
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
	return node.fn != nil && node.fn.Impure
}

// IsDistSQLBlacklist returns whether the function is not supported by DistSQL.
func (node *FuncExpr) IsDistSQLBlacklist() bool {
	return node.fn != nil && node.fn.DistsqlBlacklist
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
func (node *FuncExpr) Format(ctx *FmtCtx) {
	var typ string
	if node.Type != 0 {
		typ = funcTypeName[node.Type] + " "
	}

	// We need to remove name anonimization for the function name in
	// particular. Do this by overriding the flags.
	subCtx := ctx.CopyWithFlags(ctx.flags & ^FmtAnonymize)
	subCtx.FormatNode(&node.Func)

	ctx.WriteByte('(')
	ctx.WriteString(typ)
	ctx.FormatNode(&node.Exprs)
	ctx.WriteByte(')')
	if window := node.WindowDef; window != nil {
		ctx.WriteString(" OVER ")
		if window.Name != "" {
			ctx.FormatNode(&window.Name)
		} else {
			ctx.FormatNode(window)
		}
	}
	if node.Filter != nil {
		ctx.WriteString(" FILTER (WHERE ")
		ctx.FormatNode(node.Filter)
		ctx.WriteString(")")
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
func (node *CaseExpr) Format(ctx *FmtCtx) {
	ctx.WriteString("CASE ")
	if node.Expr != nil {
		ctx.FormatNode(node.Expr)
		ctx.WriteByte(' ')
	}
	for _, when := range node.Whens {
		ctx.FormatNode(when)
		ctx.WriteByte(' ')
	}
	if node.Else != nil {
		ctx.WriteString("ELSE ")
		ctx.FormatNode(node.Else)
		ctx.WriteByte(' ')
	}
	ctx.WriteString("END")
}

// NewTypedCaseExpr returns a new CaseExpr that is verified to be well-typed.
func NewTypedCaseExpr(
	expr TypedExpr, whens []*When, elseStmt TypedExpr, typ types.T,
) (*CaseExpr, error) {
	node := &CaseExpr{Expr: expr, Whens: whens, Else: elseStmt}
	node.typ = typ
	return node, nil
}

// When represents a WHEN sub-expression.
type When struct {
	Cond Expr
	Val  Expr
}

// Format implements the NodeFormatter interface.
func (node *When) Format(ctx *FmtCtx) {
	ctx.WriteString("WHEN ")
	ctx.FormatNode(node.Cond)
	ctx.WriteString(" THEN ")
	ctx.FormatNode(node.Val)
}

type castSyntaxMode int

// These constants separate the syntax X::Y from CAST(X AS Y).
const (
	CastExplicit castSyntaxMode = iota
	CastShort
	CastPrepend
)

// CastExpr represents a CAST(expr AS type) expression.
type CastExpr struct {
	Expr Expr
	Type coltypes.CastTargetType

	typeAnnotation
	SyntaxMode castSyntaxMode
}

// Format implements the NodeFormatter interface.
func (node *CastExpr) Format(ctx *FmtCtx) {
	buf := ctx.Buffer
	switch node.SyntaxMode {
	case CastPrepend:
		// This is a special case for things like INTERVAL '1s'. These only work
		// with string constats; if the underlying expression was changed, we fall
		// back to the short syntax.
		if _, ok := node.Expr.(*StrVal); ok {
			node.Type.Format(buf, ctx.flags.EncodeFlags())
			ctx.WriteByte(' ')
			ctx.FormatNode(node.Expr)
			break
		}
		fallthrough
	case CastShort:
		exprFmtWithParen(ctx, node.Expr)
		ctx.WriteString("::")
		node.Type.Format(buf, ctx.flags.EncodeFlags())
	default:
		ctx.WriteString("CAST(")
		ctx.FormatNode(node.Expr)
		ctx.WriteString(" AS ")
		node.Type.Format(buf, ctx.flags.EncodeFlags())
		ctx.WriteByte(')')
	}
}

// NewTypedCastExpr returns a new CastExpr that is verified to be well-typed.
func NewTypedCastExpr(expr TypedExpr, typ types.T) (*CastExpr, error) {
	colType, err := coltypes.DatumTypeToColumnType(typ)
	if err != nil {
		return nil, err
	}
	node := &CastExpr{Expr: expr, Type: colType, SyntaxMode: CastShort}
	node.typ = typ
	return node, nil
}

func (node *CastExpr) castType() types.T {
	return coltypes.CastTargetToDatumType(node.Type)
}

var (
	boolCastTypes = []types.T{types.Unknown, types.Bool, types.Int, types.Float, types.Decimal, types.String, types.FamCollatedString}
	intCastTypes  = []types.T{types.Unknown, types.Bool, types.Int, types.Float, types.Decimal, types.String, types.FamCollatedString,
		types.Timestamp, types.TimestampTZ, types.Date, types.Interval, types.Oid}
	floatCastTypes = []types.T{types.Unknown, types.Bool, types.Int, types.Float, types.Decimal, types.String, types.FamCollatedString,
		types.Timestamp, types.TimestampTZ, types.Date, types.Interval}
	decimalCastTypes = []types.T{types.Unknown, types.Bool, types.Int, types.Float, types.Decimal, types.String, types.FamCollatedString,
		types.Timestamp, types.TimestampTZ, types.Date, types.Interval}
	stringCastTypes = []types.T{types.Unknown, types.Bool, types.Int, types.Float, types.Decimal, types.String, types.FamCollatedString,
		types.Bytes, types.Timestamp, types.TimestampTZ, types.Interval, types.UUID, types.Date, types.Time, types.Oid, types.INet}
	bytesCastTypes     = []types.T{types.Unknown, types.String, types.FamCollatedString, types.Bytes, types.UUID}
	dateCastTypes      = []types.T{types.Unknown, types.String, types.FamCollatedString, types.Date, types.Timestamp, types.TimestampTZ, types.Int}
	timeCastTypes      = []types.T{types.Unknown, types.String, types.FamCollatedString, types.Time, types.Timestamp, types.TimestampTZ, types.Interval}
	timestampCastTypes = []types.T{types.Unknown, types.String, types.FamCollatedString, types.Date, types.Timestamp, types.TimestampTZ, types.Int}
	intervalCastTypes  = []types.T{types.Unknown, types.String, types.FamCollatedString, types.Int, types.Time, types.Interval}
	oidCastTypes       = []types.T{types.Unknown, types.String, types.FamCollatedString, types.Int, types.Oid}
	uuidCastTypes      = []types.T{types.Unknown, types.String, types.FamCollatedString, types.Bytes, types.UUID}
	inetCastTypes      = []types.T{types.Unknown, types.String, types.FamCollatedString, types.INet}
	arrayCastTypes     = []types.T{types.Unknown, types.String}
	jsonCastTypes      = []types.T{types.Unknown, types.String, types.JSON}
)

// validCastTypes returns a set of types that can be cast into the provided type.
func validCastTypes(t types.T) []types.T {
	switch types.UnwrapType(t) {
	case types.Bool:
		return boolCastTypes
	case types.Int:
		return intCastTypes
	case types.Float:
		return floatCastTypes
	case types.Decimal:
		return decimalCastTypes
	case types.String:
		return stringCastTypes
	case types.Bytes:
		return bytesCastTypes
	case types.Date:
		return dateCastTypes
	case types.Time:
		return timeCastTypes
	case types.Timestamp, types.TimestampTZ:
		return timestampCastTypes
	case types.Interval:
		return intervalCastTypes
	case types.JSON:
		return jsonCastTypes
	case types.UUID:
		return uuidCastTypes
	case types.INet:
		return inetCastTypes
	case types.Oid, types.RegClass, types.RegNamespace, types.RegProc, types.RegProcedure, types.RegType:
		return oidCastTypes
	default:
		// TODO(eisen): currently dead -- there is no syntax yet for casting
		// directly to collated string.
		if t.FamilyEqual(types.FamCollatedString) {
			return stringCastTypes
		} else if t.FamilyEqual(types.FamArray) {
			ret := make([]types.T, len(arrayCastTypes))
			copy(ret, arrayCastTypes)
			return ret
		}
		return nil
	}
}

// ArraySubscripts represents a sequence of one or more array subscripts.
type ArraySubscripts []*ArraySubscript

// Format implements the NodeFormatter interface.
func (a *ArraySubscripts) Format(ctx *FmtCtx) {
	for _, s := range *a {
		ctx.FormatNode(s)
	}
}

// IndirectionExpr represents a subscript expression.
type IndirectionExpr struct {
	Expr        Expr
	Indirection ArraySubscripts

	typeAnnotation
}

// Format implements the NodeFormatter interface.
func (node *IndirectionExpr) Format(ctx *FmtCtx) {
	exprFmtWithParen(ctx, node.Expr)
	ctx.FormatNode(&node.Indirection)
}

type annotateSyntaxMode int

// These constants separate the syntax X:::Y from ANNOTATE_TYPE(X, Y)
const (
	AnnotateExplicit annotateSyntaxMode = iota
	AnnotateShort
)

// AnnotateTypeExpr represents a ANNOTATE_TYPE(expr, type) expression.
type AnnotateTypeExpr struct {
	Expr Expr
	Type coltypes.CastTargetType

	SyntaxMode annotateSyntaxMode
}

// Format implements the NodeFormatter interface.
func (node *AnnotateTypeExpr) Format(ctx *FmtCtx) {
	buf := ctx.Buffer
	switch node.SyntaxMode {
	case AnnotateShort:
		exprFmtWithParen(ctx, node.Expr)
		ctx.WriteString(":::")
		node.Type.Format(buf, ctx.flags.EncodeFlags())

	default:
		ctx.WriteString("ANNOTATE_TYPE(")
		ctx.FormatNode(node.Expr)
		ctx.WriteString(", ")
		node.Type.Format(buf, ctx.flags.EncodeFlags())
		ctx.WriteByte(')')
	}
}

// TypedInnerExpr returns the AnnotateTypeExpr's inner expression as a TypedExpr.
func (node *AnnotateTypeExpr) TypedInnerExpr() TypedExpr {
	return node.Expr.(TypedExpr)
}

func (node *AnnotateTypeExpr) annotationType() types.T {
	return coltypes.CastTargetToDatumType(node.Type)
}

// CollateExpr represents an (expr COLLATE locale) expression.
type CollateExpr struct {
	Expr   Expr
	Locale string

	typeAnnotation
}

// Format implements the NodeFormatter interface.
func (node *CollateExpr) Format(ctx *FmtCtx) {
	exprFmtWithParen(ctx, node.Expr)
	ctx.WriteString(" COLLATE ")
	lex.EncodeUnrestrictedSQLIdent(ctx.Buffer, node.Locale, lex.EncNoFlags)
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
func (node *DTime) String() string            { return AsString(node) }
func (node *DDecimal) String() string         { return AsString(node) }
func (node *DFloat) String() string           { return AsString(node) }
func (node *DInt) String() string             { return AsString(node) }
func (node *DInterval) String() string        { return AsString(node) }
func (node *DJSON) String() string            { return AsString(node) }
func (node *DUuid) String() string            { return AsString(node) }
func (node *DIPAddr) String() string          { return AsString(node) }
func (node *DString) String() string          { return AsString(node) }
func (node *DCollatedString) String() string  { return AsString(node) }
func (node *DTimestamp) String() string       { return AsString(node) }
func (node *DTimestampTZ) String() string     { return AsString(node) }
func (node *DTuple) String() string           { return AsString(node) }
func (node *DArray) String() string           { return AsString(node) }
func (node *DTable) String() string           { return AsString(node) }
func (node *DOid) String() string             { return AsString(node) }
func (node *DOidWrapper) String() string      { return AsString(node) }
func (node *Exprs) String() string            { return AsString(node) }
func (node *ArrayFlatten) String() string     { return AsString(node) }
func (node *FuncExpr) String() string         { return AsString(node) }
func (node *IfExpr) String() string           { return AsString(node) }
func (node *IndexedVar) String() string       { return AsString(node) }
func (node *IndirectionExpr) String() string  { return AsString(node) }
func (node *IsOfTypeExpr) String() string     { return AsString(node) }
func (node *Name) String() string             { return AsString(node) }
func (node *UnrestrictedName) String() string { return AsString(node) }
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
func (node MaxVal) String() string            { return AsString(node) }
func (node MinVal) String() string            { return AsString(node) }
func (node *Placeholder) String() string      { return AsString(node) }
func (node dNull) String() string             { return AsString(node) }
func (list *NameList) String() string         { return AsString(list) }

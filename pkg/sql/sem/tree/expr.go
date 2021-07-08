// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"bytes"
	"context"
	"fmt"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
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
	// The semaCtx parameter defines the context in which to perform type checking.
	// The desired parameter hints the desired type that the method's caller wants from
	// the resulting TypedExpr. It is not valid to call TypeCheck with a nil desired
	// type. Instead, call it with wildcard type types.Any if no specific type is
	// desired. This restriction is also true of most methods and functions related
	// to type checking.
	TypeCheck(ctx context.Context, semaCtx *SemaContext, desired *types.T) (TypedExpr, error)
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
	ResolvedType() *types.T
}

// VariableExpr is an Expr that may change per row. It is used to
// signal the evaluation/simplification machinery that the underlying
// Expr is not constant.
type VariableExpr interface {
	Expr
	Variable()
}

var _ VariableExpr = &IndexedVar{}
var _ VariableExpr = &Subquery{}
var _ VariableExpr = UnqualifiedStar{}
var _ VariableExpr = &UnresolvedName{}
var _ VariableExpr = &AllColumnsSelector{}
var _ VariableExpr = &ColumnItem{}

// operatorExpr is used to identify expression types that involve operators;
// used by exprStrWithParen.
type operatorExpr interface {
	Expr
	operatorExpr()
}

var _ operatorExpr = &AndExpr{}
var _ operatorExpr = &OrExpr{}
var _ operatorExpr = &NotExpr{}
var _ operatorExpr = &IsNullExpr{}
var _ operatorExpr = &IsNotNullExpr{}
var _ operatorExpr = &BinaryExpr{}
var _ operatorExpr = &UnaryExpr{}
var _ operatorExpr = &ComparisonExpr{}
var _ operatorExpr = &RangeCond{}
var _ operatorExpr = &IsOfTypeExpr{}

// Operator is used to identify Operators; used in sql.y.
type Operator interface {
	operator()
}

var _ Operator = (*UnaryOperator)(nil)
var _ Operator = (*BinaryOperator)(nil)
var _ Operator = (*ComparisonOperator)(nil)

// SubqueryExpr is an interface used to identify an expression as a subquery.
// It is implemented by both tree.Subquery and optbuilder.subquery, and is
// used in TypeCheck.
type SubqueryExpr interface {
	Expr
	SubqueryExpr()
}

var _ SubqueryExpr = &Subquery{}

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
	typ *types.T
}

func (ta typeAnnotation) ResolvedType() *types.T {
	ta.assertTyped()
	return ta.typ
}

func (ta typeAnnotation) assertTyped() {
	if ta.typ == nil {
		panic(errors.AssertionFailedf(
			"ReturnType called on TypedExpr with empty typeAnnotation. " +
				"Was the underlying Expr type-checked before asserting a type of TypedExpr?"))
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

// IsNullExpr represents an IS NULL expression. This is equivalent to IS NOT
// DISTINCT FROM NULL, except when the input is a tuple.
type IsNullExpr struct {
	Expr Expr

	typeAnnotation
}

func (*IsNullExpr) operatorExpr() {}

// Format implements the NodeFormatter interface.
func (node *IsNullExpr) Format(ctx *FmtCtx) {
	exprFmtWithParen(ctx, node.Expr)
	ctx.WriteString(" IS NULL")
}

// NewTypedIsNullExpr returns a new IsNullExpr that is verified to be
// well-typed.
func NewTypedIsNullExpr(expr TypedExpr) *IsNullExpr {
	node := &IsNullExpr{Expr: expr}
	node.typ = types.Bool
	return node
}

// TypedInnerExpr returns the IsNullExpr's inner expression as a TypedExpr.
func (node *IsNullExpr) TypedInnerExpr() TypedExpr {
	return node.Expr.(TypedExpr)
}

// IsNotNullExpr represents an IS NOT NULL expression. This is equivalent to IS
// DISTINCT FROM NULL, except when the input is a tuple.
type IsNotNullExpr struct {
	Expr Expr

	typeAnnotation
}

func (*IsNotNullExpr) operatorExpr() {}

// Format implements the NodeFormatter interface.
func (node *IsNotNullExpr) Format(ctx *FmtCtx) {
	exprFmtWithParen(ctx, node.Expr)
	ctx.WriteString(" IS NOT NULL")
}

// NewTypedIsNotNullExpr returns a new IsNotNullExpr that is verified to be
// well-typed.
func NewTypedIsNotNullExpr(expr TypedExpr) *IsNotNullExpr {
	node := &IsNotNullExpr{Expr: expr}
	node.typ = types.Bool
	return node
}

// TypedInnerExpr returns the IsNotNullExpr's inner expression as a TypedExpr.
func (node *IsNotNullExpr) TypedInnerExpr() TypedExpr {
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

// ComparisonOperator represents a binary operator which returns a bool.
type ComparisonOperator struct {
	Symbol ComparisonOperatorSymbol
	// IsExplicitOperator is true if OPERATOR(symbol) is used.
	IsExplicitOperator bool
}

// MakeComparisonOperator creates a ComparisonOperator given a symbol.
func MakeComparisonOperator(symbol ComparisonOperatorSymbol) ComparisonOperator {
	return ComparisonOperator{Symbol: symbol}
}

func (o ComparisonOperator) String() string {
	if o.IsExplicitOperator {
		return fmt.Sprintf("OPERATOR(%s)", o.Symbol.String())
	}
	return o.Symbol.String()
}

func (ComparisonOperator) operator() {}

// ComparisonOperatorSymbol represents a comparison operator symbol.
type ComparisonOperatorSymbol int

// ComparisonExpr.Operator
const (
	EQ ComparisonOperatorSymbol = iota
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
	Overlaps

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
	// ComparisonOperator.HasSubOperator returns true for ops in this group.
	Any
	Some
	All

	NumComparisonOperatorSymbols
)

var _ = NumComparisonOperatorSymbols

var comparisonOpName = [...]string{
	EQ:           "=",
	LT:           "<",
	GT:           ">",
	LE:           "<=",
	GE:           ">=",
	NE:           "!=",
	In:           "IN",
	NotIn:        "NOT IN",
	Like:         "LIKE",
	NotLike:      "NOT LIKE",
	ILike:        "ILIKE",
	NotILike:     "NOT ILIKE",
	SimilarTo:    "SIMILAR TO",
	NotSimilarTo: "NOT SIMILAR TO",
	// TODO(otan): come up with a better name than RegMatch, as it also covers GeoContains.
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
	Overlaps:          "&&",
	Any:               "ANY",
	Some:              "SOME",
	All:               "ALL",
}

func (i ComparisonOperatorSymbol) String() string {
	if i < 0 || i > ComparisonOperatorSymbol(len(comparisonOpName)-1) {
		return fmt.Sprintf("ComparisonOp(%d)", i)
	}
	return comparisonOpName[i]
}

// Inverse returns the inverse of this comparison operator if it exists. The
// second return value is true if it exists, and false otherwise.
func (i ComparisonOperatorSymbol) Inverse() (ComparisonOperatorSymbol, bool) {
	inverse, ok := cmpOpsInverse[i]
	return inverse, ok
}

// HasSubOperator returns if the ComparisonOperator is used with a sub-operator.
func (i ComparisonOperatorSymbol) HasSubOperator() bool {
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
	Fn *CmpOp
}

func (*ComparisonExpr) operatorExpr() {}

// Format implements the NodeFormatter interface.
func (node *ComparisonExpr) Format(ctx *FmtCtx) {
	opStr := node.Operator.String()
	// IS and IS NOT are equivalent to IS NOT DISTINCT FROM and IS DISTINCT
	// FROM, respectively, when the RHS is true or false. We prefer the less
	// verbose IS and IS NOT in those cases.
	if node.Operator.Symbol == IsDistinctFrom && (node.Right == DBoolTrue || node.Right == DBoolFalse) {
		opStr = "IS NOT"
	} else if node.Operator.Symbol == IsNotDistinctFrom && (node.Right == DBoolTrue || node.Right == DBoolFalse) {
		opStr = "IS"
	}
	if node.Operator.Symbol.HasSubOperator() {
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

// NewTypedIndirectionExpr returns a new IndirectionExpr that is verified to be well-typed.
func NewTypedIndirectionExpr(expr, index TypedExpr, typ *types.T) *IndirectionExpr {
	node := &IndirectionExpr{
		Expr:        expr,
		Indirection: ArraySubscripts{&ArraySubscript{Begin: index}},
	}
	node.typ = typ
	return node
}

// NewTypedCollateExpr returns a new CollateExpr that is verified to be well-typed.
func NewTypedCollateExpr(expr TypedExpr, locale string) *CollateExpr {
	node := &CollateExpr{
		Expr:   expr,
		Locale: locale,
	}
	node.typ = types.MakeCollatedString(types.String, locale)
	return node
}

// NewTypedArrayFlattenExpr returns a new ArrayFlattenExpr that is verified to be well-typed.
func NewTypedArrayFlattenExpr(input Expr) *ArrayFlatten {
	inputTyp := input.(TypedExpr).ResolvedType()
	node := &ArrayFlatten{
		Subquery: input,
	}
	node.typ = types.MakeArray(inputTyp)
	return node
}

// NewTypedIfErrExpr returns a new IfErrExpr that is verified to be well-typed.
func NewTypedIfErrExpr(cond, orElse, errCode TypedExpr) *IfErrExpr {
	node := &IfErrExpr{
		Cond:    cond,
		Else:    orElse,
		ErrCode: errCode,
	}
	if orElse == nil {
		node.typ = types.Bool
	} else {
		node.typ = cond.ResolvedType()
	}
	return node
}

func (node *ComparisonExpr) memoizeFn() {
	fOp, fLeft, fRight, _, _ := FoldComparisonExpr(node.Operator, node.Left, node.Right)
	leftRet, rightRet := fLeft.(TypedExpr).ResolvedType(), fRight.(TypedExpr).ResolvedType()
	switch node.Operator.Symbol {
	case Any, Some, All:
		// Array operators memoize the SubOperator's CmpOp.
		fOp, _, _, _, _ = FoldComparisonExpr(node.SubOperator, nil, nil)
		// The right operand is either an array or a tuple/subquery.
		switch rightRet.Family() {
		case types.ArrayFamily:
			// For example:
			//   x = ANY(ARRAY[1,2])
			rightRet = rightRet.ArrayContents()
		case types.TupleFamily:
			// For example:
			//   x = ANY(SELECT y FROM t)
			//   x = ANY(1,2)
			if len(rightRet.TupleContents()) > 0 {
				rightRet = rightRet.TupleContents()[0]
			} else {
				rightRet = leftRet
			}
		}
	}

	fn, ok := CmpOps[fOp.Symbol].LookupImpl(leftRet, rightRet)
	if !ok {
		panic(errors.AssertionFailedf("lookup for ComparisonExpr %s's CmpOp failed",
			AsStringWithFlags(node, FmtShowTypes)))
	}
	node.Fn = fn
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

	// Typed version of Left for the comparison with To (where it may be
	// type-checked differently). After type-checking, Left is set to the typed
	// version for the comparison with From, and leftTo is set to the typed
	// version for the comparison with To.
	leftTo TypedExpr

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

// TypedLeftFrom returns the RangeCond's left expression as a TypedExpr, in the
// context of a comparison with TypedFrom().
func (node *RangeCond) TypedLeftFrom() TypedExpr {
	return node.Left.(TypedExpr)
}

// TypedFrom returns the RangeCond's from expression as a TypedExpr.
func (node *RangeCond) TypedFrom() TypedExpr {
	return node.From.(TypedExpr)
}

// TypedLeftTo returns the RangeCond's left expression as a TypedExpr, in the
// context of a comparison with TypedTo().
func (node *RangeCond) TypedLeftTo() TypedExpr {
	return node.leftTo
}

// TypedTo returns the RangeCond's to expression as a TypedExpr.
func (node *RangeCond) TypedTo() TypedExpr {
	return node.To.(TypedExpr)
}

// IsOfTypeExpr represents an IS {,NOT} OF (type_list) expression.
type IsOfTypeExpr struct {
	Not   bool
	Expr  Expr
	Types []ResolvableTypeReference

	resolvedTypes []*types.T

	typeAnnotation
}

func (*IsOfTypeExpr) operatorExpr() {}

// ResolvedTypes returns a slice of resolved types corresponding
// to the Types slice of unresolved types. It may only be accessed
// after typechecking.
func (node *IsOfTypeExpr) ResolvedTypes() []*types.T {
	if node.resolvedTypes == nil {
		panic("ResolvedTypes called on an IsOfTypeExpr before typechecking")
	}
	return node.resolvedTypes
}

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
		ctx.FormatTypeReference(t)
	}
	ctx.WriteByte(')')
}

// IfErrExpr represents an IFERROR expression.
type IfErrExpr struct {
	Cond    Expr
	Else    Expr
	ErrCode Expr

	typeAnnotation
}

// Format implements the NodeFormatter interface.
func (node *IfErrExpr) Format(ctx *FmtCtx) {
	if node.Else != nil {
		ctx.WriteString("IFERROR(")
	} else {
		ctx.WriteString("ISERROR(")
	}
	ctx.FormatNode(node.Cond)
	if node.Else != nil {
		ctx.WriteString(", ")
		ctx.FormatNode(node.Else)
	}
	if node.ErrCode != nil {
		ctx.WriteString(", ")
		ctx.FormatNode(node.ErrCode)
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
func NewTypedCoalesceExpr(typedExprs TypedExprs, typ *types.T) *CoalesceExpr {
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
func NewTypedArray(typedExprs TypedExprs, typ *types.T) *Array {
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
func (DefaultVal) ResolvedType() *types.T { return nil }

// PartitionMaxVal represents the MAXVALUE expression.
type PartitionMaxVal struct{}

// Format implements the NodeFormatter interface.
func (node PartitionMaxVal) Format(ctx *FmtCtx) {
	ctx.WriteString("MAXVALUE")
}

// PartitionMinVal represents the MINVALUE expression.
type PartitionMinVal struct{}

// Format implements the NodeFormatter interface.
func (node PartitionMinVal) Format(ctx *FmtCtx) {
	ctx.WriteString("MINVALUE")
}

// Placeholder represents a named placeholder.
type Placeholder struct {
	Idx PlaceholderIdx

	typeAnnotation
}

// NewPlaceholder allocates a Placeholder.
func NewPlaceholder(name string) (*Placeholder, error) {
	uval, err := strconv.ParseUint(name, 10, 64)
	if err != nil {
		return nil, err
	}
	// The string is the number that follows $ which is a 1-based index ($1, $2,
	// etc), while PlaceholderIdx is 0-based.
	if uval == 0 || uval > MaxPlaceholderIdx+1 {
		return nil, pgerror.Newf(
			pgcode.NumericValueOutOfRange,
			"placeholder index must be between 1 and %d", MaxPlaceholderIdx+1,
		)
	}
	return &Placeholder{Idx: PlaceholderIdx(uval - 1)}, nil
}

// Format implements the NodeFormatter interface.
func (node *Placeholder) Format(ctx *FmtCtx) {
	if ctx.placeholderFormat != nil {
		ctx.placeholderFormat(ctx, node)
		return
	}
	ctx.Printf("$%d", node.Idx+1)
}

// ResolvedType implements the TypedExpr interface.
func (node *Placeholder) ResolvedType() *types.T {
	if node.typ == nil {
		return types.Any
	}
	return node.typ
}

// Tuple represents a parenthesized list of expressions.
type Tuple struct {
	Exprs  Exprs
	Labels []string

	// Row indicates whether `ROW` was used in the input syntax. This is
	// used solely to generate column names automatically, see
	// col_name.go.
	Row bool

	typ *types.T
}

// NewTypedTuple returns a new Tuple that is verified to be well-typed.
func NewTypedTuple(typ *types.T, typedExprs Exprs) *Tuple {
	return &Tuple{
		Exprs:  typedExprs,
		Labels: typ.TupleLabels(),
		typ:    typ,
	}
}

// Format implements the NodeFormatter interface.
func (node *Tuple) Format(ctx *FmtCtx) {
	// If there are labels, extra parentheses are required surrounding the
	// expression.
	if len(node.Labels) > 0 {
		ctx.WriteByte('(')
	}
	ctx.WriteByte('(')
	ctx.FormatNode(&node.Exprs)
	if len(node.Exprs) == 1 {
		// Ensure the pretty-printed 1-value tuple is not ambiguous with
		// the equivalent value enclosed in grouping parentheses.
		ctx.WriteByte(',')
	}
	ctx.WriteByte(')')
	if len(node.Labels) > 0 {
		ctx.WriteString(" AS ")
		comma := ""
		for i := range node.Labels {
			ctx.WriteString(comma)
			ctx.FormatNode((*Name)(&node.Labels[i]))
			comma = ", "
		}
		ctx.WriteByte(')')
	}
}

// ResolvedType implements the TypedExpr interface.
func (node *Tuple) ResolvedType() *types.T {
	return node.typ
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
	// If the array has a type, add an annotation. Don't add it if the type is
	// UNKNOWN[], since that's not a valid annotation.
	if ctx.HasFlags(FmtParsable) && node.typ != nil {
		if node.typ.ArrayContents().Family() != types.UnknownFamily {
			ctx.WriteString(":::")
			ctx.FormatTypeReference(node.typ)
		}
	}
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
	if ctx.HasFlags(FmtParsable) {
		if t, ok := node.Subquery.(*DTuple); ok {
			if len(t.D) == 0 {
				ctx.WriteString(":::")
				ctx.Buffer.WriteString(node.typ.SQLString())
			}
		}
	}
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
func (node *Subquery) SetType(t *types.T) {
	node.typ = t
}

// Variable implements the VariableExpr interface.
func (*Subquery) Variable() {}

// SubqueryExpr implements the SubqueryExpr interface.
func (*Subquery) SubqueryExpr() {}

// Format implements the NodeFormatter interface.
func (node *Subquery) Format(ctx *FmtCtx) {
	if ctx.HasFlags(FmtSymbolicSubqueries) {
		ctx.Printf("@S%d", node.Idx)
	} else {
		// Ensure that type printing is disabled during the recursion, as
		// the type annotations are not available in subqueries.
		ctx.WithFlags(ctx.flags & ^FmtShowTypes, func() {
			if node.Exists {
				ctx.WriteString("EXISTS ")
			}
			if node.Select == nil {
				// If the subquery is generated by the optimizer, we
				// don't have an actual statement.
				ctx.WriteString("<unknown>")
			} else {
				ctx.FormatNode(node.Select)
			}
		})
	}
}

// TypedDummy is a dummy expression that represents a dummy value with
// a specified type. It can be used in situations where TypedExprs of a
// particular type are required for semantic analysis.
type TypedDummy struct {
	Typ *types.T
}

func (node *TypedDummy) String() string {
	return AsString(node)
}

// Format implements the NodeFormatter interface.
func (node *TypedDummy) Format(ctx *FmtCtx) {
	ctx.WriteString("dummyvalof(")
	ctx.FormatTypeReference(node.Typ)
	ctx.WriteString(")")
}

// ResolvedType implements the TypedExpr interface.
func (node *TypedDummy) ResolvedType() *types.T {
	return node.Typ
}

// TypeCheck implements the Expr interface.
func (node *TypedDummy) TypeCheck(context.Context, *SemaContext, *types.T) (TypedExpr, error) {
	return node, nil
}

// Walk implements the Expr interface.
func (node *TypedDummy) Walk(Visitor) Expr { return node }

// Eval implements the TypedExpr interface.
func (node *TypedDummy) Eval(*EvalContext) (Datum, error) {
	return nil, errors.AssertionFailedf("should not eval typed dummy")
}

// BinaryOperator represents a unary operator used in a BinaryExpr.
type BinaryOperator struct {
	Symbol BinaryOperatorSymbol
	// IsExplicitOperator is true if OPERATOR(symbol) is used.
	IsExplicitOperator bool
}

// MakeBinaryOperator creates a BinaryOperator given a symbol.
func MakeBinaryOperator(symbol BinaryOperatorSymbol) BinaryOperator {
	return BinaryOperator{Symbol: symbol}
}

func (o BinaryOperator) String() string {
	if o.IsExplicitOperator {
		return fmt.Sprintf("OPERATOR(%s)", o.Symbol.String())
	}
	return o.Symbol.String()
}

func (BinaryOperator) operator() {}

// BinaryOperatorSymbol is a symbol for a binary operator.
type BinaryOperatorSymbol uint8

// BinaryExpr.Operator
const (
	Bitand BinaryOperatorSymbol = iota
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

	NumBinaryOperatorSymbols
)

var _ = NumBinaryOperatorSymbols

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

// binaryOpPrio follows the precedence order in the grammar. Used for pretty-printing.
var binaryOpPrio = [...]int{
	Pow:  1,
	Mult: 2, Div: 2, FloorDiv: 2, Mod: 2,
	Plus: 3, Minus: 3,
	LShift: 4, RShift: 4,
	Bitand: 5,
	Bitxor: 6,
	Bitor:  7,
	Concat: 8, JSONFetchVal: 8, JSONFetchText: 8, JSONFetchValPath: 8, JSONFetchTextPath: 8,
}

// binaryOpFullyAssoc indicates whether an operator is fully associative.
// Reminder: an op R is fully associative if (a R b) R c == a R (b R c)
var binaryOpFullyAssoc = [...]bool{
	Pow:  false,
	Mult: true, Div: false, FloorDiv: false, Mod: false,
	Plus: true, Minus: false,
	LShift: false, RShift: false,
	Bitand: true,
	Bitxor: true,
	Bitor:  true,
	Concat: true, JSONFetchVal: false, JSONFetchText: false, JSONFetchValPath: false, JSONFetchTextPath: false,
}

func (i BinaryOperatorSymbol) isPadded() bool {
	return !(i == JSONFetchVal || i == JSONFetchText || i == JSONFetchValPath || i == JSONFetchTextPath)
}

func (i BinaryOperatorSymbol) String() string {
	if i > BinaryOperatorSymbol(len(binaryOpName)-1) {
		return fmt.Sprintf("BinaryOp(%d)", i)
	}
	return binaryOpName[i]
}

// BinaryExpr represents a binary value expression.
type BinaryExpr struct {
	Operator    BinaryOperator
	Left, Right Expr

	typeAnnotation
	Fn *BinOp
}

// TypedLeft returns the BinaryExpr's left expression as a TypedExpr.
func (node *BinaryExpr) TypedLeft() TypedExpr {
	return node.Left.(TypedExpr)
}

// TypedRight returns the BinaryExpr's right expression as a TypedExpr.
func (node *BinaryExpr) TypedRight() TypedExpr {
	return node.Right.(TypedExpr)
}

// ResolvedBinOp returns the resolved binary op overload; can only be called
// after Resolve (which happens during TypeCheck).
func (node *BinaryExpr) ResolvedBinOp() *BinOp {
	return node.Fn
}

// NewTypedBinaryExpr returns a new BinaryExpr that is well-typed.
func NewTypedBinaryExpr(op BinaryOperator, left, right TypedExpr, typ *types.T) *BinaryExpr {
	node := &BinaryExpr{Operator: op, Left: left, Right: right}
	node.typ = typ
	node.memoizeFn()
	return node
}

func (*BinaryExpr) operatorExpr() {}

func (node *BinaryExpr) memoizeFn() {
	leftRet, rightRet := node.Left.(TypedExpr).ResolvedType(), node.Right.(TypedExpr).ResolvedType()
	fn, ok := BinOps[node.Operator.Symbol].lookupImpl(leftRet, rightRet)
	if !ok {
		panic(errors.AssertionFailedf("lookup for BinaryExpr %s's BinOp failed",
			AsStringWithFlags(node, FmtShowTypes)))
	}
	node.Fn = fn
}

// newBinExprIfValidOverload constructs a new BinaryExpr if and only
// if the pair of arguments have a valid implementation for the given
// BinaryOperator.
func newBinExprIfValidOverload(op BinaryOperator, left TypedExpr, right TypedExpr) *BinaryExpr {
	leftRet, rightRet := left.ResolvedType(), right.ResolvedType()
	fn, ok := BinOps[op.Symbol].lookupImpl(leftRet, rightRet)
	if ok {
		expr := &BinaryExpr{
			Operator: op,
			Left:     left,
			Right:    right,
			Fn:       fn,
		}
		expr.typ = returnTypeToFixedType(fn.returnType(), []TypedExpr{left, right})
		return expr
	}
	return nil
}

// Format implements the NodeFormatter interface.
func (node *BinaryExpr) Format(ctx *FmtCtx) {
	binExprFmtWithParen(ctx, node.Left, node.Operator.String(), node.Right, node.Operator.Symbol.isPadded())
}

// UnaryOperator represents a unary operator used in a UnaryExpr.
type UnaryOperator struct {
	Symbol UnaryOperatorSymbol
	// IsExplicitOperator is true if OPERATOR(symbol) is used.
	IsExplicitOperator bool
}

// MakeUnaryOperator creates a UnaryOperator given a symbol.
func MakeUnaryOperator(symbol UnaryOperatorSymbol) UnaryOperator {
	return UnaryOperator{Symbol: symbol}
}

func (o UnaryOperator) String() string {
	if o.IsExplicitOperator {
		return fmt.Sprintf("OPERATOR(%s)", o.Symbol.String())
	}
	return o.Symbol.String()
}

func (UnaryOperator) operator() {}

// UnaryOperatorSymbol represents a unary operator.
type UnaryOperatorSymbol uint8

// UnaryExpr.Operator.Symbol
const (
	UnaryMinus UnaryOperatorSymbol = iota
	UnaryComplement
	UnarySqrt
	UnaryCbrt

	NumUnaryOperatorSymbols
)

var _ = NumUnaryOperatorSymbols

var unaryOpName = [...]string{
	UnaryMinus:      "-",
	UnaryComplement: "~",
	UnarySqrt:       "|/",
	UnaryCbrt:       "||/",
}

func (i UnaryOperatorSymbol) String() string {
	if i > UnaryOperatorSymbol(len(unaryOpName)-1) {
		return fmt.Sprintf("UnaryOp(%d)", i)
	}
	return unaryOpName[i]
}

// UnaryExpr represents a unary value expression.
type UnaryExpr struct {
	Operator UnaryOperator
	Expr     Expr

	typeAnnotation
	fn *UnaryOp
}

func (*UnaryExpr) operatorExpr() {}

// Format implements the NodeFormatter interface.
func (node *UnaryExpr) Format(ctx *FmtCtx) {
	ctx.WriteString(node.Operator.String())
	e := node.Expr
	_, isOp := e.(operatorExpr)
	_, isDatum := e.(Datum)
	_, isConstant := e.(Constant)
	if isOp || (node.Operator.Symbol == UnaryMinus && (isDatum || isConstant)) {
		ctx.WriteByte('(')
		ctx.FormatNode(e)
		ctx.WriteByte(')')
	} else {
		ctx.FormatNode(e)
	}
}

// TypedInnerExpr returns the UnaryExpr's inner expression as a TypedExpr.
func (node *UnaryExpr) TypedInnerExpr() TypedExpr {
	return node.Expr.(TypedExpr)
}

// NewTypedUnaryExpr returns a new UnaryExpr that is well-typed.
func NewTypedUnaryExpr(op UnaryOperator, expr TypedExpr, typ *types.T) *UnaryExpr {
	node := &UnaryExpr{Operator: op, Expr: expr}
	node.typ = typ
	innerType := expr.ResolvedType()
	for _, o := range UnaryOps[op.Symbol] {
		o := o.(*UnaryOp)
		if innerType.Equivalent(o.Typ) && node.typ.Equivalent(o.ReturnType) {
			node.fn = o
			return node
		}
	}
	panic(errors.AssertionFailedf("invalid TypedExpr with unary op %d: %s", op.Symbol, expr))
}

// FuncExpr represents a function call.
type FuncExpr struct {
	Func  ResolvableFunctionReference
	Type  funcType
	Exprs Exprs
	// Filter is used for filters on aggregates: SUM(k) FILTER (WHERE k > 0)
	Filter    Expr
	WindowDef *WindowDef

	// AggType is used to specify the type of aggregation.
	AggType AggType
	// OrderBy is used for aggregations which specify an order. This same field
	// is used for any type of aggregation.
	OrderBy OrderBy

	typeAnnotation
	fnProps *FunctionProperties
	fn      *Overload
}

// NewTypedFuncExpr returns a FuncExpr that is already well-typed and resolved.
func NewTypedFuncExpr(
	ref ResolvableFunctionReference,
	aggQualifier funcType,
	exprs TypedExprs,
	filter TypedExpr,
	windowDef *WindowDef,
	typ *types.T,
	props *FunctionProperties,
	overload *Overload,
) *FuncExpr {
	f := &FuncExpr{
		Func:           ref,
		Type:           aggQualifier,
		Exprs:          make(Exprs, len(exprs)),
		Filter:         filter,
		WindowDef:      windowDef,
		typeAnnotation: typeAnnotation{typ: typ},
		fn:             overload,
		fnProps:        props,
	}
	for i, e := range exprs {
		f.Exprs[i] = e
	}
	return f
}

// ResolvedOverload returns the builtin definition; can only be called after
// Resolve (which happens during TypeCheck).
func (node *FuncExpr) ResolvedOverload() *Overload {
	return node.fn
}

// IsGeneratorApplication returns true iff the function applied is a generator (SRF).
func (node *FuncExpr) IsGeneratorApplication() bool {
	return node.fn != nil && node.fn.Generator != nil
}

// IsWindowFunctionApplication returns true iff the function is being applied as a window function.
func (node *FuncExpr) IsWindowFunctionApplication() bool {
	return node.WindowDef != nil
}

// IsDistSQLBlocklist returns whether the function is not supported by DistSQL.
func (node *FuncExpr) IsDistSQLBlocklist() bool {
	return node.fnProps != nil && node.fnProps.DistsqlBlocklist
}

// CanHandleNulls returns whether or not the function can handle null
// arguments.
func (node *FuncExpr) CanHandleNulls() bool {
	return node.fnProps != nil && node.fnProps.NullableArgs
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

// AggType specifies the type of aggregation.
type AggType int

// FuncExpr.AggType
const (
	_ AggType = iota
	// GeneralAgg is used for general-purpose aggregate functions.
	// array_agg(col1 ORDER BY col2)
	GeneralAgg
	// OrderedSetAgg is used for ordered-set aggregate functions.
	// percentile_disc(fraction) WITHIN GROUP (ORDER BY col1)
	OrderedSetAgg
)

// Format implements the NodeFormatter interface.
func (node *FuncExpr) Format(ctx *FmtCtx) {
	var typ string
	if node.Type != 0 {
		typ = funcTypeName[node.Type] + " "
	}

	// We need to remove name anonymization for the function name in
	// particular. Do this by overriding the flags.
	ctx.WithFlags(ctx.flags&^FmtAnonymize, func() {
		ctx.FormatNode(&node.Func)
	})

	ctx.WriteByte('(')
	ctx.WriteString(typ)
	ctx.FormatNode(&node.Exprs)
	if node.AggType == GeneralAgg && len(node.OrderBy) > 0 {
		ctx.WriteByte(' ')
		ctx.FormatNode(&node.OrderBy)
	}
	ctx.WriteByte(')')
	if ctx.HasFlags(FmtParsable) && node.typ != nil {
		if node.fnProps.AmbiguousReturnType {
			// There's no type annotation available for tuples.
			// TODO(jordan,knz): clean this up. AmbiguousReturnType should be set only
			// when we should and can put an annotation here. #28579
			if node.typ.Family() != types.TupleFamily {
				ctx.WriteString(":::")
				ctx.Buffer.WriteString(node.typ.SQLString())
			}
		}
	}
	if node.AggType == OrderedSetAgg && len(node.OrderBy) > 0 {
		ctx.WriteString(" WITHIN GROUP (")
		ctx.FormatNode(&node.OrderBy)
		ctx.WriteString(")")
	}
	if node.Filter != nil {
		ctx.WriteString(" FILTER (WHERE ")
		ctx.FormatNode(node.Filter)
		ctx.WriteString(")")
	}
	if window := node.WindowDef; window != nil {
		ctx.WriteString(" OVER ")
		if window.Name != "" {
			ctx.FormatNode(&window.Name)
		} else {
			ctx.FormatNode(window)
		}
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
	expr TypedExpr, whens []*When, elseStmt TypedExpr, typ *types.T,
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
	Type ResolvableTypeReference

	typeAnnotation
	SyntaxMode castSyntaxMode
}

// Format implements the NodeFormatter interface.
func (node *CastExpr) Format(ctx *FmtCtx) {
	switch node.SyntaxMode {
	case CastPrepend:
		// This is a special case for things like INTERVAL '1s'. These only work
		// with string constants; if the underlying expression was changed, we fall
		// back to the short syntax.
		if _, ok := node.Expr.(*StrVal); ok {
			ctx.FormatTypeReference(node.Type)
			ctx.WriteByte(' ')
			ctx.FormatNode(node.Expr)
			break
		}
		fallthrough
	case CastShort:
		exprFmtWithParen(ctx, node.Expr)
		ctx.WriteString("::")
		ctx.FormatTypeReference(node.Type)
	default:
		ctx.WriteString("CAST(")
		ctx.FormatNode(node.Expr)
		ctx.WriteString(" AS ")
		if typ, ok := GetStaticallyKnownType(node.Type); ok && typ.Family() == types.CollatedStringFamily {
			// Need to write closing parentheses before COLLATE clause, so create
			// equivalent string type without the locale.
			strTyp := types.MakeScalar(
				types.StringFamily,
				typ.Oid(),
				typ.Precision(),
				typ.Width(),
				"", /* locale */
			)
			ctx.WriteString(strTyp.SQLString())
			ctx.WriteString(") COLLATE ")
			lex.EncodeLocaleName(&ctx.Buffer, typ.Locale())
		} else {
			ctx.FormatTypeReference(node.Type)
			ctx.WriteByte(')')
		}
	}
}

// NewTypedCastExpr returns a new CastExpr that is verified to be well-typed.
func NewTypedCastExpr(expr TypedExpr, typ *types.T) *CastExpr {
	node := &CastExpr{Expr: expr, Type: typ, SyntaxMode: CastShort}
	node.typ = typ
	return node
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
	// If the sub expression is a CastExpr or an Array that has a type,
	// we need to wrap it in a ParenExpr, otherwise the indirection
	// will get interpreted as part of the type.
	// Ex. ('{a}'::_typ)[1] vs. '{a}'::_typ[1].
	// Ex. (ARRAY['a'::typ]:::typ[])[1] vs. ARRAY['a'::typ]:::typ[][1].
	var annotateArray bool
	if arr, ok := node.Expr.(*Array); ctx.HasFlags(FmtParsable) && ok && arr.typ != nil {
		if arr.typ.ArrayContents().Family() != types.UnknownFamily {
			annotateArray = true
		}
	}
	if _, isCast := node.Expr.(*CastExpr); isCast || annotateArray {
		withParens := ParenExpr{Expr: node.Expr}
		exprFmtWithParen(ctx, &withParens)
	} else {
		exprFmtWithParen(ctx, node.Expr)
	}
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
	Type ResolvableTypeReference

	SyntaxMode annotateSyntaxMode
}

// Format implements the NodeFormatter interface.
func (node *AnnotateTypeExpr) Format(ctx *FmtCtx) {
	switch node.SyntaxMode {
	case AnnotateShort:
		exprFmtWithParen(ctx, node.Expr)
		ctx.WriteString(":::")
		ctx.FormatTypeReference(node.Type)

	default:
		ctx.WriteString("ANNOTATE_TYPE(")
		ctx.FormatNode(node.Expr)
		ctx.WriteString(", ")
		ctx.FormatTypeReference(node.Type)
		ctx.WriteByte(')')
	}
}

// TypedInnerExpr returns the AnnotateTypeExpr's inner expression as a TypedExpr.
func (node *AnnotateTypeExpr) TypedInnerExpr() TypedExpr {
	return node.Expr.(TypedExpr)
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
	lex.EncodeLocaleName(&ctx.Buffer, node.Locale)
}

// TupleStar represents (E).* expressions.
// It is meant to evaporate during star expansion.
type TupleStar struct {
	Expr Expr
}

// NormalizeVarName implements the VarName interface.
func (node *TupleStar) NormalizeVarName() (VarName, error) { return node, nil }

// Format implements the NodeFormatter interface.
func (node *TupleStar) Format(ctx *FmtCtx) {
	ctx.WriteByte('(')
	ctx.FormatNode(node.Expr)
	ctx.WriteString(").*")
}

// ColumnAccessExpr represents (E).x expressions. Specifically, it
// allows accessing the column(s) from a Set Returning Function.
type ColumnAccessExpr struct {
	Expr Expr

	// ByIndex, if set, indicates that the access is using a numeric
	// column reference and ColIndex below is already set.
	ByIndex bool

	// ColName is the name of the column to access. Empty if ByIndex is
	// set.
	ColName Name

	// ColIndex indicates the index of the column in the tuple. This is
	// either:
	// - set during type checking based on the label in ColName if
	//   ByIndex is false,
	// - or checked for validity during type checking if ByIndex is true.
	// The first column in the tuple is at index 0. The input
	// syntax (E).@N populates N-1 in this field.
	ColIndex int

	typeAnnotation
}

// NewTypedColumnAccessExpr creates a pre-typed ColumnAccessExpr.
// A by-index ColumnAccessExpr can be specified by passing an empty string as colName.
func NewTypedColumnAccessExpr(expr TypedExpr, colName Name, colIdx int) *ColumnAccessExpr {
	return &ColumnAccessExpr{
		Expr:           expr,
		ColName:        colName,
		ByIndex:        colName == "",
		ColIndex:       colIdx,
		typeAnnotation: typeAnnotation{typ: expr.ResolvedType().TupleContents()[colIdx]},
	}
}

// Format implements the NodeFormatter interface.
func (node *ColumnAccessExpr) Format(ctx *FmtCtx) {
	ctx.WriteByte('(')
	ctx.FormatNode(node.Expr)
	ctx.WriteString(").")
	if node.ByIndex {
		fmt.Fprintf(ctx, "@%d", node.ColIndex+1)
	} else {
		ctx.FormatNode(&node.ColName)
	}
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
func (node *ColumnAccessExpr) String() string { return AsString(node) }
func (node *CollateExpr) String() string      { return AsString(node) }
func (node *ComparisonExpr) String() string   { return AsString(node) }
func (node *Datums) String() string           { return AsString(node) }
func (node *DBitArray) String() string        { return AsString(node) }
func (node *DBool) String() string            { return AsString(node) }
func (node *DBytes) String() string           { return AsString(node) }
func (node *DDate) String() string            { return AsString(node) }
func (node *DTime) String() string            { return AsString(node) }
func (node *DTimeTZ) String() string          { return AsString(node) }
func (node *DDecimal) String() string         { return AsString(node) }
func (node *DFloat) String() string           { return AsString(node) }
func (node *DBox2D) String() string           { return AsString(node) }
func (node *DGeography) String() string       { return AsString(node) }
func (node *DGeometry) String() string        { return AsString(node) }
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
func (node *DOid) String() string             { return AsString(node) }
func (node *DOidWrapper) String() string      { return AsString(node) }
func (node *Exprs) String() string            { return AsString(node) }
func (node *ArrayFlatten) String() string     { return AsString(node) }
func (node *FuncExpr) String() string         { return AsString(node) }
func (node *IfExpr) String() string           { return AsString(node) }
func (node *IfErrExpr) String() string        { return AsString(node) }
func (node *IndexedVar) String() string       { return AsString(node) }
func (node *IndirectionExpr) String() string  { return AsString(node) }
func (node *IsOfTypeExpr) String() string     { return AsString(node) }
func (node *Name) String() string             { return AsString(node) }
func (node *UnrestrictedName) String() string { return AsString(node) }
func (node *NotExpr) String() string          { return AsString(node) }
func (node *IsNullExpr) String() string       { return AsString(node) }
func (node *IsNotNullExpr) String() string    { return AsString(node) }
func (node *NullIfExpr) String() string       { return AsString(node) }
func (node *NumVal) String() string           { return AsString(node) }
func (node *OrExpr) String() string           { return AsString(node) }
func (node *ParenExpr) String() string        { return AsString(node) }
func (node *RangeCond) String() string        { return AsString(node) }
func (node *StrVal) String() string           { return AsString(node) }
func (node *Subquery) String() string         { return AsString(node) }
func (node *Tuple) String() string            { return AsString(node) }
func (node *TupleStar) String() string        { return AsString(node) }
func (node *AnnotateTypeExpr) String() string { return AsString(node) }
func (node *UnaryExpr) String() string        { return AsString(node) }
func (node DefaultVal) String() string        { return AsString(node) }
func (node PartitionMaxVal) String() string   { return AsString(node) }
func (node PartitionMinVal) String() string   { return AsString(node) }
func (node *Placeholder) String() string      { return AsString(node) }
func (node dNull) String() string             { return AsString(node) }
func (list *NameList) String() string         { return AsString(list) }

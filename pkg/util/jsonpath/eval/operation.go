// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"strings"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/jsonpath"
	"github.com/cockroachdb/cockroach/pkg/util/jsonpath/parser"
	"github.com/cockroachdb/errors"
)

type jsonpathBool int

const (
	jsonpathBoolTrue jsonpathBool = iota
	jsonpathBoolFalse
	jsonpathBoolUnknown
)

func isBool(j json.JSON) bool {
	switch j.Type() {
	case json.TrueJSONType, json.FalseJSONType:
		return true
	default:
		return false
	}
}

func convertFromBool(b jsonpathBool) json.JSON {
	switch b {
	case jsonpathBoolTrue:
		return json.TrueJSONValue
	case jsonpathBoolFalse:
		return json.FalseJSONValue
	case jsonpathBoolUnknown:
		return json.NullJSONValue
	default:
		panic(errors.AssertionFailedf("unhandled jsonpath boolean type"))
	}
}

func (ctx *jsonpathCtx) evalOperation(
	op jsonpath.Operation, jsonValue json.JSON,
) ([]json.JSON, error) {
	switch op.Type {
	case jsonpath.OpLogicalAnd, jsonpath.OpLogicalOr, jsonpath.OpLogicalNot,
		jsonpath.OpCompEqual, jsonpath.OpCompNotEqual, jsonpath.OpCompLess,
		jsonpath.OpCompLessEqual, jsonpath.OpCompGreater, jsonpath.OpCompGreaterEqual,
		jsonpath.OpLikeRegex, jsonpath.OpExists, jsonpath.OpIsUnknown,
		jsonpath.OpStartsWith:
		b, err := ctx.evalBoolean(op, jsonValue)
		if err != nil {
			return []json.JSON{convertFromBool(jsonpathBoolUnknown)}, err
		}
		return []json.JSON{convertFromBool(b)}, nil
	case jsonpath.OpAdd, jsonpath.OpSub, jsonpath.OpMult,
		jsonpath.OpDiv, jsonpath.OpMod:
		results, err := ctx.evalArithmetic(op, jsonValue)
		if err != nil {
			return nil, err
		}
		return []json.JSON{results}, nil
	case jsonpath.OpPlus, jsonpath.OpMinus:
		return ctx.evalUnaryArithmetic(op, jsonValue)
	default:
		panic(errors.AssertionFailedf("unhandled operation type"))
	}
}

func (ctx *jsonpathCtx) evalBoolean(
	op jsonpath.Operation, jsonValue json.JSON,
) (jsonpathBool, error) {
	switch op.Type {
	case jsonpath.OpLogicalAnd, jsonpath.OpLogicalOr, jsonpath.OpLogicalNot:
		return ctx.evalLogical(op, jsonValue)
	case jsonpath.OpCompEqual, jsonpath.OpCompNotEqual,
		jsonpath.OpCompLess, jsonpath.OpCompLessEqual,
		jsonpath.OpCompGreater, jsonpath.OpCompGreaterEqual:
		return ctx.evalPredicate(op, jsonValue, evalComparisonFunc, true /* evalRight */, true /* unwrapRight */)
	case jsonpath.OpLikeRegex:
		return ctx.evalPredicate(op, jsonValue, evalRegexFunc, false /* evalRight */, false /* unwrapRight */)
	case jsonpath.OpExists:
		return ctx.evalExists(op, jsonValue)
	case jsonpath.OpIsUnknown:
		return ctx.evalIsUnknown(op, jsonValue)
	case jsonpath.OpStartsWith:
		return ctx.evalPredicate(op, jsonValue, evalStartsWithFunc, true /* evalRight */, false /* unwrapRight */)
	default:
		panic(errors.AssertionFailedf("unhandled operation type"))
	}
}

func evalStartsWithFunc(_ jsonpath.Operation, l, r json.JSON) (jsonpathBool, error) {
	if l.Type() != json.StringJSONType || r.Type() != json.StringJSONType {
		return jsonpathBoolUnknown, nil
	}
	left, err := l.AsText()
	if err != nil {
		return jsonpathBoolUnknown, err
	}
	right, err := r.AsText()
	if err != nil {
		return jsonpathBoolUnknown, err
	}
	if strings.HasPrefix(*left, *right) {
		return jsonpathBoolTrue, nil
	}
	return jsonpathBoolFalse, nil
}

func (ctx *jsonpathCtx) evalIsUnknown(
	op jsonpath.Operation, jsonValue json.JSON,
) (jsonpathBool, error) {
	leftOp, ok := op.Left.(jsonpath.Operation)
	if !ok {
		return jsonpathBoolUnknown, errors.AssertionFailedf("left is not an operation")
	}
	leftBool, err := ctx.evalBoolean(leftOp, jsonValue)
	if err != nil {
		return jsonpathBoolUnknown, errors.AssertionFailedf("left is not a boolean")
	}
	if leftBool == jsonpathBoolUnknown {
		return jsonpathBoolTrue, nil
	}
	return jsonpathBoolFalse, nil
}

func (ctx *jsonpathCtx) evalExists(
	op jsonpath.Operation, jsonValue json.JSON,
) (jsonpathBool, error) {
	// TODO(normanchenn): Only in strict mode do we need to evaluate all items.
	// We can optimize this by short-circuiting in lax mode.
	l, err := ctx.evalAndUnwrapResult(op.Left, jsonValue, false /* unwrap */)
	if err != nil {
		return jsonpathBoolUnknown, nil //nolint:returnerrcheck
	}
	if len(l) == 0 {
		return jsonpathBoolFalse, nil
	}
	return jsonpathBoolTrue, nil
}

func evalRegexFunc(op jsonpath.Operation, l, _ json.JSON) (jsonpathBool, error) {
	regexOp, ok := op.Right.(jsonpath.Regex)
	if !ok {
		return jsonpathBoolUnknown, errors.AssertionFailedf("op.Right is not a regex")
	}

	if l.Type() != json.StringJSONType {
		return jsonpathBoolUnknown, nil
	}
	// AsText() provides the correct string representation for regex pattern
	// matching by returning raw characters instead of their escaped JSON string
	// representations.
	//
	// Examples:
	// - For a JSON string with a backslash ("\\"): AsText() returns two
	//   backslashes ("\\"), while String() returns "\"\\\\\"" (two escaped
	//   backslashes enclosed in quotes).
	// - For a JSON string with a newline ("\n"): AsText() returns an actual
	//   newline character ("\n"), while String() returns "\"\\n\"" (an escaped
	//   backslash and 'n' enclosed in quotes)
	text, err := l.AsText()
	if err != nil {
		return jsonpathBoolUnknown, err
	}

	r, err := parser.ReCache.GetRegexp(regexOp)
	if err != nil {
		return jsonpathBoolUnknown, err
	}

	res := r.MatchString(*text)
	if !res {
		return jsonpathBoolFalse, nil
	}
	return jsonpathBoolTrue, nil
}

func (ctx *jsonpathCtx) evalLogical(
	op jsonpath.Operation, jsonValue json.JSON,
) (jsonpathBool, error) {
	leftOp, ok := op.Left.(jsonpath.Operation)
	if !ok {
		return jsonpathBoolUnknown, errors.AssertionFailedf("left is not an operation")
	}
	leftBool, err := ctx.evalBoolean(leftOp, jsonValue)
	if err != nil {
		return jsonpathBoolUnknown, errors.AssertionFailedf("left is not a boolean")
	}
	switch op.Type {
	case jsonpath.OpLogicalAnd:
		if leftBool == jsonpathBoolFalse {
			return jsonpathBoolFalse, nil
		}
	case jsonpath.OpLogicalOr:
		if leftBool == jsonpathBoolTrue {
			return jsonpathBoolTrue, nil
		}
	case jsonpath.OpLogicalNot:
		if leftBool == jsonpathBoolUnknown {
			return jsonpathBoolUnknown, nil
		}
		if leftBool == jsonpathBoolTrue {
			return jsonpathBoolFalse, nil
		}
		return jsonpathBoolTrue, nil
	default:
		panic(errors.AssertionFailedf("unhandled logical operation type"))
	}

	rightOp, ok := op.Right.(jsonpath.Operation)
	if !ok {
		return jsonpathBoolUnknown, errors.AssertionFailedf("right is not an operation")
	}
	rightBool, err := ctx.evalBoolean(rightOp, jsonValue)
	if err != nil {
		return jsonpathBoolUnknown, errors.AssertionFailedf("right is not a boolean")
	}
	switch op.Type {
	case jsonpath.OpLogicalAnd:
		if rightBool == jsonpathBoolTrue {
			return leftBool, nil
		}
		return rightBool, nil
	case jsonpath.OpLogicalOr:
		if rightBool == jsonpathBoolFalse {
			return leftBool, nil
		}
		return rightBool, nil
	default:
		panic(errors.AssertionFailedf("unhandled logical operation type"))
	}
}

// evalPredicate evaluates a predicate operation. Predicates have existence
// semantics. True is returned if any pair of items from the left and right
// paths satisfy the condition. In strict mode, even if a pair has been found,
// all pairs need to be checked for errors.
func (ctx *jsonpathCtx) evalPredicate(
	op jsonpath.Operation,
	jsonValue json.JSON,
	exec func(op jsonpath.Operation, l, r json.JSON) (jsonpathBool, error),
	evalRight, unwrapRight bool,
) (jsonpathBool, error) {
	// The left argument results are always auto-unwrapped.
	left, err := ctx.evalAndUnwrapResult(op.Left, jsonValue, true /* unwrap */)
	if err != nil {
		return jsonpathBoolUnknown, nil //nolint:returnerrcheck
	}
	var right []json.JSON
	if evalRight {
		// The right argument results are conditionally evaluated and unwrapped.
		right, err = ctx.evalAndUnwrapResult(op.Right, jsonValue, unwrapRight)
		if err != nil {
			return jsonpathBoolUnknown, nil //nolint:returnerrcheck
		}
	} else {
		// If we don't want to evaluate the right argument, we need to call
		// the exec function once for each item in the left argument. Currently,
		// this only includes OpLikeRegex.
		right = append(right, nil)
	}

	errored := false
	found := false
	for _, l := range left {
		for _, r := range right {
			res, err := exec(op, l, r)
			if err != nil {
				return jsonpathBoolUnknown, err
			}
			if res == jsonpathBoolUnknown {
				if ctx.strict {
					return jsonpathBoolUnknown, nil
				}
				errored = true
			} else if res == jsonpathBoolTrue {
				if !ctx.strict {
					return jsonpathBoolTrue, nil
				}
				found = true
			}
		}
	}
	if found {
		return jsonpathBoolTrue, nil
	}
	// Lax mode.
	if errored {
		return jsonpathBoolUnknown, nil
	}
	return jsonpathBoolFalse, nil
}

func evalComparisonFunc(operation jsonpath.Operation, l, r json.JSON) (jsonpathBool, error) {
	op := operation.Type
	if l.Type() != r.Type() && !(isBool(l) && isBool(r)) {
		// Inequality comparison of nulls to non-nulls is true. Everything else
		// is false.
		if l.Type() == json.NullJSONType || r.Type() == json.NullJSONType {
			if op == jsonpath.OpCompNotEqual {
				return jsonpathBoolTrue, nil
			}
			return jsonpathBoolFalse, nil
		}
		// Non-null items of different types are not comparable.
		return jsonpathBoolUnknown, nil
	}

	var cmp int
	var err error
	switch l.Type() {
	case json.NullJSONType, json.TrueJSONType, json.FalseJSONType,
		json.NumberJSONType, json.StringJSONType:
		cmp, err = l.Compare(r)
		if err != nil {
			return jsonpathBoolUnknown, err
		}
	case json.ArrayJSONType, json.ObjectJSONType:
		// Don't evaluate non-scalar types.
		return jsonpathBoolUnknown, nil
	default:
		panic(errors.AssertionFailedf("unhandled json type"))
	}

	var res bool
	switch op {
	case jsonpath.OpCompEqual:
		res = cmp == 0
	case jsonpath.OpCompNotEqual:
		res = cmp != 0
	case jsonpath.OpCompLess:
		res = cmp < 0
	case jsonpath.OpCompLessEqual:
		res = cmp <= 0
	case jsonpath.OpCompGreater:
		res = cmp > 0
	case jsonpath.OpCompGreaterEqual:
		res = cmp >= 0
	default:
		panic(errors.AssertionFailedf("unhandled jsonpath comparison type"))
	}
	if res {
		return jsonpathBoolTrue, nil
	}
	return jsonpathBoolFalse, nil
}

func (ctx *jsonpathCtx) evalUnaryArithmetic(
	op jsonpath.Operation, jsonValue json.JSON,
) ([]json.JSON, error) {
	operands, err := ctx.evalAndUnwrapResult(op.Left, jsonValue, true /* unwrap */)
	if err != nil {
		return nil, err
	}

	for i := range len(operands) {
		if operands[i].Type() != json.NumberJSONType {
			return nil, pgerror.Newf(pgcode.SingletonSQLJSONItemRequired,
				"operand of unary jsonpath operator %s is not a numeric value",
				jsonpath.OperationTypeStrings[op.Type])
		}

		if op.Type == jsonpath.OpMinus {
			leftNum, _ := operands[i].AsDecimal()
			leftNum.Neg(leftNum)
			operands[i] = json.FromDecimal(*leftNum)
		}
	}
	return operands, nil
}

func (ctx *jsonpathCtx) evalArithmetic(
	op jsonpath.Operation, jsonValue json.JSON,
) (json.JSON, error) {
	left, err := ctx.evalAndUnwrapResult(op.Left, jsonValue, true /* unwrap */)
	if err != nil {
		return nil, err
	}
	right, err := ctx.evalAndUnwrapResult(op.Right, jsonValue, true /* unwrap */)
	if err != nil {
		return nil, err
	}

	if len(left) != 1 || left[0].Type() != json.NumberJSONType {
		return nil, pgerror.Newf(pgcode.SingletonSQLJSONItemRequired,
			"left operand of jsonpath operator %s is not a single numeric value",
			jsonpath.OperationTypeStrings[op.Type])
	}
	if len(right) != 1 || right[0].Type() != json.NumberJSONType {
		return nil, pgerror.Newf(pgcode.SingletonSQLJSONItemRequired,
			"right operand of jsonpath operator %s is not a single numeric value",
			jsonpath.OperationTypeStrings[op.Type])
	}

	leftNum, _ := left[0].AsDecimal()
	rightNum, _ := right[0].AsDecimal()
	var res apd.Decimal
	var cond apd.Condition
	switch op.Type {
	case jsonpath.OpAdd:
		_, err = tree.DecimalCtx.Add(&res, leftNum, rightNum)
	case jsonpath.OpSub:
		_, err = tree.DecimalCtx.Sub(&res, leftNum, rightNum)
	case jsonpath.OpMult:
		_, err = tree.DecimalCtx.Mul(&res, leftNum, rightNum)
	case jsonpath.OpDiv:
		cond, err = tree.DecimalCtx.Quo(&res, leftNum, rightNum)
		// Division by zero or 0 / 0.
		if cond.DivisionByZero() || cond.DivisionUndefined() {
			return nil, tree.ErrDivByZero
		}
	case jsonpath.OpMod:
		// In other places where apd.Context.Rem() is called, we first check if
		// the left value is NaN and the right value is 0, then return ErrDivByZero.
		// In this case, NaN shouldn't happen because JSON numbers cannot be NaN.
		// We assert this here, and then check if the right value is 0.
		if leftNum.Form == apd.NaN || rightNum.Form == apd.NaN {
			return nil, errors.AssertionFailedf("numbers in jsonpath queries cannot be NaN")
		}
		if rightNum.IsZero() {
			return nil, tree.ErrDivByZero
		}
		_, err = tree.DecimalCtx.Rem(&res, leftNum, rightNum)
	default:
		panic(errors.AssertionFailedf("unhandled jsonpath arithmetic type"))
	}
	if err != nil {
		return nil, err
	}
	return json.FromDecimal(res), nil
}

// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/jsonpath"
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

func convertToBool(j json.JSON) jsonpathBool {
	b, ok := j.AsBool()
	if !ok {
		return jsonpathBoolUnknown
	}
	if b {
		return jsonpathBoolTrue
	}
	return jsonpathBoolFalse
}

func (ctx *jsonpathCtx) evalOperation(
	op jsonpath.Operation, jsonValue json.JSON,
) (json.JSON, error) {
	switch op.Type {
	case jsonpath.OpLogicalAnd, jsonpath.OpLogicalOr, jsonpath.OpLogicalNot:
		res, err := ctx.evalLogical(op, jsonValue)
		if err != nil {
			return convertFromBool(jsonpathBoolUnknown), err
		}
		return convertFromBool(res), nil
	case jsonpath.OpCompEqual, jsonpath.OpCompNotEqual,
		jsonpath.OpCompLess, jsonpath.OpCompLessEqual,
		jsonpath.OpCompGreater, jsonpath.OpCompGreaterEqual:
		res, err := ctx.evalComparison(op, jsonValue, true /* unwrapRight */)
		if err != nil {
			return convertFromBool(jsonpathBoolUnknown), err
		}
		return convertFromBool(res), nil
	case jsonpath.OpAdd, jsonpath.OpSub, jsonpath.OpMult,
		jsonpath.OpDiv, jsonpath.OpMod:
		return ctx.evalArithmetic(op, jsonValue)
	default:
		panic(errors.AssertionFailedf("unhandled operation type"))
	}
}

func (ctx *jsonpathCtx) evalLogical(
	op jsonpath.Operation, current json.JSON,
) (jsonpathBool, error) {
	left, err := ctx.eval(op.Left, current, !ctx.strict /* unwrap */)
	if err != nil {
		return jsonpathBoolUnknown, err
	}
	if len(left) != 1 || !isBool(left[0]) {
		return jsonpathBoolUnknown, errors.AssertionFailedf("left is not a boolean")
	}
	leftBool := convertToBool(left[0])
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

	right, err := ctx.eval(op.Right, current, !ctx.strict /* unwrap */)
	if err != nil {
		return jsonpathBoolUnknown, err
	}
	if len(right) != 1 || !isBool(right[0]) {
		return jsonpathBoolUnknown, errors.AssertionFailedf("right is not a boolean")
	}
	rightBool := convertToBool(right[0])
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

// evalComparison evaluates a comparison operation predicate. Predicates have
// existence semantics. True is returned if any pair of items from the left and
// right paths satisfy the condition. In strict mode, even if a pair has been
// found, all pairs need to be checked for errors.
func (ctx *jsonpathCtx) evalComparison(
	op jsonpath.Operation, jsonValue json.JSON, unwrapRight bool,
) (jsonpathBool, error) {
	// The left argument results are always auto-unwrapped.
	left, err := ctx.evalAndUnwrapResult(op.Left, jsonValue, true /* unwrap */)
	if err != nil {
		return jsonpathBoolUnknown, err
	}
	// The right argument results are conditionally unwrapped. Currently, it is
	// always unwrapped, but in the future for operations like like_regex, we
	// don't want to unwrap the right argument.
	right, err := ctx.evalAndUnwrapResult(op.Right, jsonValue, unwrapRight)
	if err != nil {
		return jsonpathBoolUnknown, err
	}

	errored := false
	found := false
	for _, l := range left {
		for _, r := range right {
			res, err := execComparison(l, r, op.Type)
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

func execComparison(l, r json.JSON, op jsonpath.OperationType) (jsonpathBool, error) {
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
		_, err = tree.DecimalCtx.Rem(&res, leftNum, rightNum)
	default:
		panic(errors.AssertionFailedf("unhandled jsonpath arithmetic type"))
	}
	if err != nil {
		return nil, err
	}
	return json.FromDecimal(res), nil
}

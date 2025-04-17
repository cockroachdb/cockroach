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
)

var (
	errEvalSizeNotArray = pgerror.Newf(pgcode.SQLJSONArrayNotFound, "jsonpath item method .size() can only be applied to an array")
	errEvalAbsNotNumber = pgerror.Newf(pgcode.NonNumericSQLJSONItem, "jsonpath item method .abs() can only be applied to a numeric value")
)

func (ctx *jsonpathCtx) evalMethod(
	jsonPath jsonpath.Path, method jsonpath.Method, jsonValue json.JSON, unwrap bool,
) ([]json.JSON, error) {
	switch method.Type {
	case jsonpath.SizeMethod:
		size, err := ctx.evalSize(jsonValue)
		if err != nil {
			return nil, err
		}
		return []json.JSON{json.FromInt(size)}, nil
	case jsonpath.TypeMethod:
		t := ctx.evalType(jsonValue)
		return []json.JSON{json.FromString(t)}, nil
	case jsonpath.AbsMethod:
		return ctx.evalNumericMethod(jsonPath, jsonValue, unwrap, evalAbs)
	case jsonpath.FloorMethod:
		return ctx.evalNumericMethod(jsonPath, jsonValue, unwrap, evalFloor)
	case jsonpath.CeilingMethod:
		return ctx.evalNumericMethod(jsonPath, jsonValue, unwrap, evalCeiling)
	default:
		return nil, errUnimplemented
	}
}

func (ctx *jsonpathCtx) evalSize(jsonValue json.JSON) (int, error) {
	if jsonValue.Type() != json.ArrayJSONType {
		if ctx.strict {
			return -1, errEvalSizeNotArray
		}
		return 1, nil
	}
	return jsonValue.Len(), nil
}

func (ctx *jsonpathCtx) evalType(jsonValue json.JSON) string {
	// When jsonValue is a number, json.Type.String() returns "numeric", but
	// postgres returns "number".
	if jsonValue.Type() == json.NumberJSONType {
		return "number"
	}
	return jsonValue.Type().String()
}

func (ctx *jsonpathCtx) evalNumericMethod(
	jsonPath jsonpath.Path, jsonValue json.JSON, unwrap bool, fn func(dec *apd.Decimal) error,
) ([]json.JSON, error) {
	if unwrap && jsonValue.Type() == json.ArrayJSONType {
		return ctx.unwrapCurrentTargetAndEval(jsonPath, jsonValue, unwrap)
	}
	dec, ok := jsonValue.AsDecimal()
	if !ok {
		return nil, maybeThrowError(ctx, errEvalAbsNotNumber)
	}
	if err := fn(dec); err != nil {
		return nil, err
	}
	return []json.JSON{json.FromDecimal(*dec)}, nil
}

func evalAbs(dec *apd.Decimal) error {
	_ = dec.Abs(dec)
	return nil
}

func evalFloor(dec *apd.Decimal) error {
	_, err := tree.ExactCtx.Floor(dec, dec)
	if err != nil {
		return err
	}
	return nil
}

func evalCeiling(dec *apd.Decimal) error {
	_, err := tree.ExactCtx.Ceil(dec, dec)
	if err != nil {
		return err
	}
	if dec.IsZero() {
		dec.Negative = false
	}
	return nil
}

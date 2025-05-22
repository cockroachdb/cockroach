// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/jsonpath"
	"github.com/cockroachdb/errors"
)

var (
	errEvalSizeNotArray = pgerror.Newf(pgcode.SQLJSONArrayNotFound, "jsonpath item method .size() can only be applied to an array")
)

func (ctx *jsonpathCtx) evalMethod(
	method jsonpath.Method, jsonValue json.JSON, unwrap bool,
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
	case jsonpath.AbsMethod, jsonpath.FloorMethod, jsonpath.CeilingMethod:
		return ctx.evalNumericMethod(method, jsonValue, unwrap)
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
	jsonPath jsonpath.Path, jsonValue json.JSON, unwrap bool,
) ([]json.JSON, error) {
	if unwrap && jsonValue.Type() == json.ArrayJSONType {
		return ctx.unwrapCurrentTargetAndEval(jsonPath, jsonValue, false /* unwrap */)
	}
	method, _ := jsonPath.(jsonpath.Method)
	// AsDecimal allocates a new Decimal object, so we can modify it below.
	dec, ok := jsonValue.AsDecimal()
	if !ok {
		return nil, maybeThrowError(ctx, pgerror.Newf(pgcode.NonNumericSQLJSONItem,
			"jsonpath item method .%s() can only be applied to a numeric value",
			jsonpath.MethodTypeStrings[method.Type]))
	}
	var err error
	switch method.Type {
	case jsonpath.AbsMethod:
		dec = dec.Abs(dec)
	case jsonpath.FloorMethod:
		_, err = tree.ExactCtx.Floor(dec, dec)
	case jsonpath.CeilingMethod:
		_, err = tree.ExactCtx.Ceil(dec, dec)
		if dec.IsZero() {
			dec.Negative = false
		}
	default:
		panic(errors.Newf("unimplemented method: %s", method.Type))
	}
	if err != nil {
		return nil, err
	}
	return []json.JSON{json.FromDecimal(*dec)}, nil
}
